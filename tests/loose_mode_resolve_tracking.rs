#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.loose_tracking";
}

struct H;
#[async_trait::async_trait]
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// After a worker repeatedly hits a missing handler in loose mode, the
/// per-row `resolve_attempts` counter must be > 0 (it was 0 at INSERT) and
/// `last_resolve_attempt_at` must be populated, even though the status stays
/// 'queued' and `attempts` stays 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loose_mode_claim_bumps_resolve_attempts() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Dispatcher with handler registered — produces the audit row + pgwq job.
    let dispatcher = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H)
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    dispatcher
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Worker WITHOUT the handler in loose mode. With short poll and many
    // retry attempts the worker will claim the job repeatedly.
    let worker = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(50))
                .strict_handler_lookup(false)
                .max_attempts(50)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let handle = worker.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    let (status, attempts, resolve_attempts, last_resolve): (String, i32, i32, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT status::text, attempts, resolve_attempts, last_resolve_attempt_at
         FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(status, "queued", "loose mode must keep status='queued'");
    assert_eq!(attempts, 0, "attempts must stay 0 in loose mode");
    assert!(
        resolve_attempts >= 1,
        "resolve_attempts must be bumped on loose-mode claim, got {resolve_attempts}"
    );
    assert!(
        last_resolve.is_some(),
        "last_resolve_attempt_at must be populated when resolve_attempts > 0"
    );
}

/// `History::stuck_unregistered_handlers(threshold)` returns the rows whose
/// `resolve_attempts >= threshold`, scoped to status='queued'. Verifies the
/// operator-diagnostic SELECT shape and the threshold filtering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_unregistered_handlers_filters_by_threshold() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let dispatcher = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H)
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    dispatcher
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let worker = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(50))
                .strict_handler_lookup(false)
                .max_attempts(50)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let handle = worker.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    // Threshold 1 must surface the row.
    let rows = dispatcher
        .history()
        .stuck_unregistered_handlers(1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "threshold 1 must surface the stuck row");
    assert_eq!(rows[0].handler_id, "h");
    assert!(rows[0].resolve_attempts >= 1);

    // Threshold way above observed count must return empty.
    let rows_high = dispatcher
        .history()
        .stuck_unregistered_handlers(10_000)
        .await
        .unwrap();
    assert!(
        rows_high.is_empty(),
        "very high threshold must return no rows, got {}",
        rows_high.len()
    );
}
