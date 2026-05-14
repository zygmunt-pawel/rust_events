#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DecodeStrategy, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Event with a name that legitimately contains "decode" — common in real codebases
/// (e.g. a service that processes encoded payloads).
#[derive(Serialize, Deserialize)]
struct DecodeRequest {
    blob_id: i64,
}
impl DomainEvent for DecodeRequest {
    const EVENT_TYPE: &'static str = "ingest.decode_request";
}

/// Handler that legitimately returns Abort with reason starting with "decode ".
/// Pre-fix: runtime.rs:345-355 silently converts this to Retry. The test fails
/// because attempts climbs and `last_error` never lands as permanent.
struct AbortingHandler;
impl EventHandler<DecodeRequest> for AbortingHandler {
    async fn handle(
        &self,
        _event: &DecodeRequest,
        _ctx: &HandlerContext,
    ) -> Result<(), HandlerError> {
        Err(HandlerError::abort(
            "decode dxf failed: unsupported version tag",
        ))
    }
}

/// With `DecodeStrategy::Retry` (the default), a handler-emitted Abort whose reason
/// happens to start with "decode " must STILL be honored as terminal-dead on the
/// first attempt — NOT retried as if it were a JSON decode failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_abort_with_decode_prefix_is_not_retried() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .max_attempts(5)
                .decode_error_strategy(DecodeStrategy::Retry)
                .build()
                .unwrap(),
        )
        .register_handler::<DecodeRequest, _>("h", AbortingHandler)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme"),
            &DecodeRequest { blob_id: 1 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait up to 6 s for the row to land terminal.
    for _ in 0..30 {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status IN ('dead','sent','skipped')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if n == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status::text, attempts, last_error FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        row.0, "dead",
        "handler-emitted Abort must be terminal-dead, not retried"
    );
    assert_eq!(
        row.1, 1,
        "Abort must NOT consume retry budget (attempts must be 1)"
    );
    assert!(
        row.2.unwrap_or_default().contains("dxf"),
        "last_error must preserve the handler's reason"
    );

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
