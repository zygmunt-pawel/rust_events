#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.hdr";
}
struct H;
#[async_trait::async_trait]
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Headers serialized larger than `MAX_HEADERS_BYTES` (16 KiB) must surface a
/// typed `DispatchError::HeadersTooLarge`, not an opaque DB CHECK violation.
/// Validates the Rust-side pre-check (defense-in-depth pair with the SQL
/// `events_headers_size` CHECK).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_over_16kib_rejected_typed() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H)
        .build()
        .unwrap();

    let mut headers = serde_json::Map::new();
    // Single key with a 20 KiB string value: serialized size > 16 KiB.
    headers.insert("blob".into(), serde_json::Value::String("x".repeat(20_000)));

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_headers(headers),
            &Ev,
        )
        .await
        .unwrap_err();
    tx.rollback().await.unwrap();

    assert!(
        matches!(err, DispatchError::HeadersTooLarge { .. }),
        "expected HeadersTooLarge, got {err:?}"
    );
}

/// 8 KiB headers must pass — the cap is 16 KiB. Guards against off-by-X.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_under_cap_accepted() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H)
        .build()
        .unwrap();

    let mut headers = serde_json::Map::new();
    headers.insert("ok".into(), serde_json::Value::String("y".repeat(8_000)));

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_headers(headers),
            &Ev,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
