#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.m1";
}

struct Trip;

impl EventHandler<Ev> for Trip {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_missing_row_aborts_with_audit_missing_tracing() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .poll_interval(Duration::from_millis(200))
                .concurrency(1)
                .max_attempts(2)
                .build()
                .unwrap(),
        )
        .register_handler::<Ev, _>("h", Trip, HandlerOptions::new())
        .build()
        .unwrap();

    // Dispatch FIRST (worker not yet running).
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Delete the handler_deliveries row BEFORE starting the worker.
    // The pgwq.jobs row remains → when the worker claims it, handle_envelope
    // finds no handler_deliveries row → prev_status=NULL → returns Abort
    // → pgwq marks the job 'dead'.
    let deleted = sqlx::query("DELETE FROM outbox.handler_deliveries")
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(
        deleted, 1,
        "expected to delete exactly 1 handler_deliveries row"
    );

    // Now start the worker. It polls immediately (tokio::time::interval fires
    // on first tick), claims the pgwq.jobs row, finds no handler_deliveries →
    // aborts → pgwq job becomes 'dead'.
    let handle = outbox.start().await.unwrap();

    // Wait for pgwq job to reach 'dead'.
    for _ in 0..30 {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM pgwq.jobs LIMIT 1")
                .fetch_optional(&pool)
                .await
                .unwrap();
        if status.as_deref() == Some("dead") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let job_status: Option<String> =
        sqlx::query_scalar("SELECT status::text FROM pgwq.jobs LIMIT 1")
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(
        job_status.as_deref(),
        Some("dead"),
        "pgwq job should be dead after audit-missing Abort, got: {job_status:?}"
    );

    // No handler_deliveries rows should exist (we deleted them and no re-insert happens).
    let delivery_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(delivery_count, 0, "handler_deliveries should have 0 rows");

    let _ = handle.shutdown(Duration::from_secs(2)).await;
}
