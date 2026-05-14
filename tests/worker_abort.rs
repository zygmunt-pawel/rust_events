#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct BadEvent {
    id: i32,
}

impl DomainEvent for BadEvent {
    const EVENT_TYPE: &'static str = "test.bad_event";
}

struct AlwaysAbort;

impl EventHandler<BadEvent> for AlwaysAbort {
    async fn handle(&self, _: &BadEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Err(HandlerError::abort("permanent failure"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_marks_dead_on_first_attempt() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(std::time::Duration::from_millis(100))
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<BadEvent, _>("abort_handler", AlwaysAbort, HandlerOptions::new())
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &BadEvent { id: 42 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = outbox.start().await.unwrap();

    // Wait up to 5 s for status='dead'.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if dead >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for delivery to become dead"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    // Status is 'dead'.
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='abort_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "dead");

    // attempts = 1 (only one attempt made).
    let db_attempts: i32 = sqlx::query_scalar(
        "SELECT attempts FROM outbox.handler_deliveries WHERE handler_id='abort_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(db_attempts, 1);

    // last_error contains "permanent failure".
    let last_error: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM outbox.handler_deliveries WHERE handler_id='abort_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        last_error
            .as_deref()
            .unwrap_or("")
            .contains("permanent failure"),
        "last_error must contain 'permanent failure', got: {last_error:?}"
    );
}
