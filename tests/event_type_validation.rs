#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmptyTypeEvent;
impl DomainEvent for EmptyTypeEvent {
    const EVENT_TYPE: &'static str = "";
}
struct NoopHandler;
#[async_trait::async_trait]
impl EventHandler<EmptyTypeEvent> for NoopHandler {
    async fn handle(&self, _: &EmptyTypeEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct LongTypeEvent;
impl DomainEvent for LongTypeEvent {
    // 129 bytes — one over the limit (12*10 + 9 = 129).
    const EVENT_TYPE: &'static str = concat!(
        "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa",
        "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaaa",
        "aaaaaaaaaa", "aaaaaaaaaa", "aaaaaaaaa",
    );
}
struct NoopHandler2;
#[async_trait::async_trait]
impl EventHandler<LongTypeEvent> for NoopHandler2 {
    async fn handle(&self, _: &LongTypeEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_empty_event_type_rejected_in_rust() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<EmptyTypeEvent, _>("h", NoopHandler)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &EmptyTypeEvent)
        .await
        .expect_err("empty EVENT_TYPE must be rejected");

    assert!(
        matches!(err, DispatchError::EventTypeInvalid { len: 0, max: 128 }),
        "expected EventTypeInvalid {{ len: 0, max: 128 }}, got: {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_oversize_event_type_rejected_in_rust() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<LongTypeEvent, _>("h", NoopHandler2)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &LongTypeEvent)
        .await
        .expect_err("oversize EVENT_TYPE must be rejected before DB CHECK");

    assert!(
        matches!(err, DispatchError::EventTypeInvalid { len: 129, max: 128 }),
        "expected EventTypeInvalid {{ len: 129, max: 128 }}, got: {err:?}",
    );
}
