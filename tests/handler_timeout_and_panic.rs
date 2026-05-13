#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BackoffPolicy, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig, PanicPolicy,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.htp";
}

// ── handler_timeout ──────────────────────────────────────────────────────────

struct SleepyHandler {
    delay: Duration,
}
#[async_trait::async_trait]
impl EventHandler<Ev> for SleepyHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

/// A handler that sleeps past `handler_timeout` is cancelled by `handle_envelope`'s
/// own `tokio::time::timeout` wrap (fires `HANDLER_CLEANUP_BUDGET` before
/// pgwq's outer one), routes through `HandlerError::retry("handler_timeout")`,
/// and after `max_attempts` retries the audit row terminalizes at
/// `status='dead'` — never stays in `running` or `queued`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_timeout_terminalizes_to_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Short-ish lease and the minimum handler_timeout pgwq allows (1s).
    // Quick backoff to keep test wall-clock low.
    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(1))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("sleepy", SleepyHandler { delay: Duration::from_secs(10) })
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(&mut tx, &DispatchContext::new("t"), &Ev).await.unwrap();
    tx.commit().await.unwrap();

    // Wait up to 10s for terminal state.
    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent" | "skipped") {
            break;
        }
    }
    assert_eq!(final_status, "dead", "handler_timeout must terminalize to dead after retries");

    // No row left in 'running'.
    let running: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(running, 0, "no audit row should remain in 'running'");

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

// ── PanicPolicy::Retry ───────────────────────────────────────────────────────

struct PanicOnceHandler {
    calls: Arc<AtomicU32>,
}
#[async_trait::async_trait]
impl EventHandler<Ev> for PanicOnceHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(n != 0, "intentional panic on first attempt");
        Ok(())
    }
}

/// `PanicPolicy::Retry` (default): the first attempt panics → pgwq synthesizes
/// `JobError::Retry`, fences correctly, the second attempt succeeds and the
/// audit row goes `sent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_retry_recovers_on_next_attempt() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let calls = Arc::new(AtomicU32::new(0));
    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(3)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Retry)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", PanicOnceHandler { calls: calls.clone() })
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(&mut tx, &DispatchContext::new("t"), &Ev).await.unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "sent" | "dead") {
            break;
        }
    }
    assert_eq!(final_status, "sent", "PanicPolicy::Retry must recover, got: {final_status}");
    assert!(calls.load(Ordering::SeqCst) >= 2, "handler must be retried at least once");

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

// ── PanicPolicy::Dead ────────────────────────────────────────────────────────

struct AlwaysPanicHandler;
#[async_trait::async_trait]
impl EventHandler<Ev> for AlwaysPanicHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        panic!("permanent panic");
    }
}

/// `PanicPolicy::Dead`: a panicking handler terminalizes the audit row on the
/// FIRST attempt. Guards the fix for the `rust_events` ↔ pgwq integration —
/// previously pgwq's `flip_dead_state` bypassed our `mark_*_fenced` and the
/// row stayed `running` forever. Now `handle_envelope` catches the panic via
/// `FutureExt::catch_unwind` and routes through `mark_dead_fenced`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_dead_terminalizes_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Dead)
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", AlwaysPanicHandler)
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    let mut attempts: i32 = 0;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let row: (String, i32) = sqlx::query_as(
            "SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        final_status = row.0;
        attempts = row.1;
        if matches!(final_status.as_str(), "dead" | "sent") {
            break;
        }
    }
    assert_eq!(final_status, "dead", "PanicPolicy::Dead must terminalize on first panic");
    assert_eq!(attempts, 1, "PanicPolicy::Dead must NOT consume additional attempts");

    // No row left in 'running'.
    let running: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(running, 0);

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// `PanicPolicy::Retry` with attempts exhausted: every attempt panics, after
/// `max_attempts` the row reaches `dead` via `mark_dead_fenced`, never stuck
/// in `running`. Complements the recovery test above by exercising the
/// "panic + retry budget exhausted" terminal path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_retry_exhausted_terminalizes_to_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Retry)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", AlwaysPanicHandler)
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent") {
            break;
        }
    }
    assert_eq!(final_status, "dead");

    let running: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(running, 0);

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}
