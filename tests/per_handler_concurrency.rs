#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder, OutboxConfig,
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

/// A handler registered with concurrency_limit(1) must never run two
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
        .register_handler::<Ev, _>(
            "limited",
            probe,
            HandlerOptions::new().concurrency_limit(1),
        )
        .build()
        .unwrap();

    // Dispatch 6 events of the limited type.
    for _ in 0..6 {
        let mut tx = pool.begin().await.unwrap();
        let ctx = DispatchContext::new("default");
        outbox.dispatch(&mut tx, &ctx, &Ev).await.unwrap();
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
}
