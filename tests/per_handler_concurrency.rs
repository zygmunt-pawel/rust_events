#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DeliveryStatus, DispatchContext, DispatchOutcome, DomainEvent, EventHandler, HandlerContext,
    HandlerError, HandlerOptions, OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.per_handler_concurrency";
}

/// Handler that records the maximum observed concurrency. Each invocation
/// bumps a live counter, sleeps, and records the peak.
struct Probe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
impl EventHandler<Ev> for Probe {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A handler registered with `concurrency_limit(1)` must never run two
/// invocations concurrently, even when many events of its type are queued
/// and the worker-wide concurrency is higher than 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_one_serializes_handler() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let probe = Probe {
        live: live.clone(),
        peak: peak.clone(),
    };

    // Worker-wide concurrency 4 (pool fits 4*2+2 = 10) — high enough that,
    // without the per-key limit, several Probe tasks could overlap.
    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder().concurrency(4).build().unwrap())
        .register_handler::<Ev, _>("limited", probe, HandlerOptions::new().concurrency_limit(1))
        .build()
        .unwrap();

    // Dispatch 6 events of the limited type.
    let mut event_ids = Vec::new();
    for _ in 0..6 {
        let mut tx = pool.begin().await.unwrap();
        let ctx = DispatchContext::new("default");
        match outbox.dispatch(&mut tx, &ctx, &Ev).await.unwrap() {
            DispatchOutcome::Dispatched { event_id, .. } => event_ids.push(event_id),
            other => panic!("expected Dispatched, got {other:?}"),
        }
        tx.commit().await.unwrap();
    }

    let handle = outbox.start().await.unwrap();
    // 6 events x 300 ms serialized ~= 1.8 s; allow generous headroom.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let _ = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "concurrency_limit(1) must serialize the handler; observed peak > 1"
    );

    // Control: all 6 deliveries must have actually run to `sent` — proves the
    // `peak == 1` above is real serialization, not an artifact of under-dispatch.
    let history = outbox.history();
    for eid in &event_ids {
        let rows = history.handler_deliveries_for(*eid).await.unwrap();
        assert_eq!(rows.len(), 1, "exactly one delivery per event");
        assert_eq!(
            rows[0].status,
            DeliveryStatus::Sent,
            "every dispatched delivery must have run to completion"
        );
    }
}

/// `concurrency_limit(N)` for `N > 1` must cap the handler at *exactly* `N`
/// concurrent invocations — not collapse to serialized, not run unbounded.
/// Worker-wide `concurrency` is 4, so without the per-key cap up to 4 Probe
/// tasks could overlap; with `concurrency_limit(2)` the peak must be 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_n_caps_at_exactly_n() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let probe = Probe {
        live: live.clone(),
        peak: peak.clone(),
    };

    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder().concurrency(4).build().unwrap())
        .register_handler::<Ev, _>("limited", probe, HandlerOptions::new().concurrency_limit(2))
        .build()
        .unwrap();

    let mut event_ids = Vec::new();
    for _ in 0..8 {
        let mut tx = pool.begin().await.unwrap();
        let ctx = DispatchContext::new("default");
        match outbox.dispatch(&mut tx, &ctx, &Ev).await.unwrap() {
            DispatchOutcome::Dispatched { event_id, .. } => event_ids.push(event_id),
            other => panic!("expected Dispatched, got {other:?}"),
        }
        tx.commit().await.unwrap();
    }

    let handle = outbox.start().await.unwrap();
    // 8 events x 300 ms, 2-at-a-time ~= 1.2 s; allow generous headroom.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let _ = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        2,
        "concurrency_limit(2) must cap the handler at exactly 2 concurrent \
         invocations; observed a different peak"
    );

    // Control: all 8 deliveries ran to `sent` — proves `peak == 2` is a real
    // cap, not an artifact of under-dispatch.
    let history = outbox.history();
    for eid in &event_ids {
        let rows = history.handler_deliveries_for(*eid).await.unwrap();
        assert_eq!(rows.len(), 1, "exactly one delivery per event");
        assert_eq!(
            rows[0].status,
            DeliveryStatus::Sent,
            "every dispatched delivery must have run to completion"
        );
    }
}

/// Handler that records peak concurrency AND fails its first attempt. Used to
/// prove a `concurrency_limit` permit is released when a delivery ends via
/// retry — not only on the clean `Ok` path.
struct FlakyProbe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
impl EventHandler<Ev> for FlakyProbe {
    async fn handle(&self, _: &Ev, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        if ctx.attempt == 1 {
            Err(HandlerError::retry("induced first-attempt failure"))
        } else {
            Ok(())
        }
    }
}

/// A `concurrency_limit(1)` handler whose first attempt always fails must not
/// leak its single permit on the retry path: every event's first attempt
/// returns `HandlerError::retry`, and only the retry succeeds. If the permit
/// leaked on the error return, the handler would self-deadlock after the
/// first failure and no further delivery would ever run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_slot_released_on_failed_attempt() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let probe = FlakyProbe {
        live: live.clone(),
        peak: peak.clone(),
    };

    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder().concurrency(4).build().unwrap())
        .register_handler::<Ev, _>("limited", probe, HandlerOptions::new().concurrency_limit(1))
        .build()
        .unwrap();

    let mut event_ids = Vec::new();
    for _ in 0..3 {
        let mut tx = pool.begin().await.unwrap();
        let ctx = DispatchContext::new("default");
        match outbox.dispatch(&mut tx, &ctx, &Ev).await.unwrap() {
            DispatchOutcome::Dispatched { event_id, .. } => event_ids.push(event_id),
            other => panic!("expected Dispatched, got {other:?}"),
        }
        tx.commit().await.unwrap();
    }

    let handle = outbox.start().await.unwrap();
    // 3 events x 2 attempts, serialized, with the default retry backoff (~1 s)
    // between the failing and the succeeding attempt.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let _ = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "concurrency_limit(1) must serialize the handler across retries too"
    );

    // The real proof: every delivery reached `sent`. Had the permit leaked on
    // the first-attempt retry, the handler would have stalled and later
    // deliveries would still be `awaiting_retry` / `queued`.
    let history = outbox.history();
    for eid in &event_ids {
        let rows = history.handler_deliveries_for(*eid).await.unwrap();
        assert_eq!(rows.len(), 1, "exactly one delivery per event");
        assert_eq!(
            rows[0].status,
            DeliveryStatus::Sent,
            "delivery must reach `sent` — a leaked permit would self-deadlock \
             the concurrency_limit(1) handler"
        );
    }
}
