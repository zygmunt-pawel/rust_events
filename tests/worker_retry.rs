#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BackoffPolicy, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Serialize, Deserialize)]
struct Flaky {
    id: i32,
}

impl DomainEvent for Flaky {
    const EVENT_TYPE: &'static str = "test.flaky";
}

#[derive(Clone)]
struct RetryOnce(Arc<AtomicUsize>);

impl EventHandler<Flaky> for RetryOnce {
    async fn handle(&self, _: &Flaky, _: &HandlerContext) -> Result<(), HandlerError> {
        let attempt = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1 {
            Err(HandlerError::retry("transient error"))
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_once_succeeds_on_attempt_2() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));

    // Linear backoff with 100ms base so retry happens quickly in CI.
    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(std::time::Duration::from_millis(100))
        .max_attempts(5)
        .retry_backoff(BackoffPolicy::Linear {
            base: std::time::Duration::from_millis(100),
            increment: std::time::Duration::ZERO,
            cap: std::time::Duration::from_millis(100),
        })
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Flaky, _>(
            "retry_handler",
            RetryOnce(attempts.clone()),
            HandlerOptions::new(),
        )
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Flaky { id: 1 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = outbox.start().await.unwrap();

    // Wait up to 10 s for status='sent'.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if sent >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for delivery to become sent"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    // Handler was called twice.
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // Final status is 'sent'.
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='retry_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "sent");

    // attempts column = 2.
    let db_attempts: i32 = sqlx::query_scalar(
        "SELECT attempts FROM outbox.handler_deliveries WHERE handler_id='retry_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(db_attempts, 2);

    // last_error is NULL on the sent row (cleared on transition to sent).
    let last_error: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM outbox.handler_deliveries WHERE handler_id='retry_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(last_error.is_none(), "last_error must be NULL on sent row");
}
