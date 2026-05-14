#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TenantEvent {
    tenant: String,
}

impl DomainEvent for TenantEvent {
    const EVENT_TYPE: &'static str = "test.tenant_event";
}

struct SkipHandler;

impl EventHandler<TenantEvent> for SkipHandler {
    async fn handle(&self, _: &TenantEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Err(HandlerError::skip("not for this tenant"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_marks_skipped_terminal() {
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
        .register_handler::<TenantEvent, _>("skip_handler", SkipHandler, HandlerOptions::new())
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme"),
            &TenantEvent {
                tenant: "wrong".into(),
            },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = outbox.start().await.unwrap();

    // Wait up to 5 s for status='skipped'.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let skipped: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='skipped'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if skipped >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for delivery to become skipped"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    // Status is 'skipped' (terminal).
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='skip_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "skipped");

    // finished_at is set (required by status_invariants CHECK).
    let finished_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT finished_at FROM outbox.handler_deliveries WHERE handler_id='skip_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        finished_at.is_some(),
        "finished_at must be set for skipped status"
    );

    // last_error contains "not for this tenant".
    let last_error: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM outbox.handler_deliveries WHERE handler_id='skip_handler'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        last_error
            .as_deref()
            .unwrap_or("")
            .contains("not for this tenant"),
        "last_error must contain 'not for this tenant', got: {last_error:?}"
    );
}
