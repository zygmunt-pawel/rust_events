#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Serialize, Deserialize)]
struct OrderEvent {
    order_id: i64,
}

impl DomainEvent for OrderEvent {
    const EVENT_TYPE: &'static str = "test.order_event";
    fn aggregate_key(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned(format!("order:{}", self.order_id)))
    }
}

#[derive(Serialize, Deserialize)]
struct NoAggEvent;
impl DomainEvent for NoAggEvent {
    const EVENT_TYPE: &'static str = "test.no_agg";
}

#[derive(Serialize, Deserialize)]
struct EmptyAggEvent;
impl DomainEvent for EmptyAggEvent {
    const EVENT_TYPE: &'static str = "test.empty_agg";
    fn aggregate_key(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(""))
    }
}

#[derive(Serialize, Deserialize)]
struct OversizeAggEvent;
impl DomainEvent for OversizeAggEvent {
    const EVENT_TYPE: &'static str = "test.oversize_agg";
    fn aggregate_key(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Owned("x".repeat(129)))
    }
}

struct H;
impl EventHandler<OrderEvent> for H {
    async fn handle(&self, _: &OrderEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_key_roundtrips_through_history() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<OrderEvent, _>("audit", H, HandlerOptions::new())
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("t1"),
            &OrderEvent { order_id: 42 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let rust_events::DispatchOutcome::Dispatched { event_id, .. } = outcome else {
        panic!("expected Dispatched");
    };

    let ev = outbox.history().event(event_id).await.unwrap().unwrap();
    assert_eq!(ev.aggregate_key.as_deref(), Some("order:42"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_aggregate_key_persists_as_null() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &NoAggEvent)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let event_id = match outcome {
        rust_events::DispatchOutcome::Dispatched { event_id, .. }
        | rust_events::DispatchOutcome::NoHandlers { event_id } => event_id,
        rust_events::DispatchOutcome::Duplicate { .. } => panic!("unexpected duplicate"),
        _ => panic!("unexpected DispatchOutcome variant"),
    };

    let ev = outbox.history().event(event_id).await.unwrap().unwrap();
    assert_eq!(ev.aggregate_key, None);

    // Also verify the partial index is empty for this case (DB-level sanity).
    let agg_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events WHERE aggregate_key IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(agg_rows, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_aggregate_key_rejected_typed() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &EmptyAggEvent)
        .await
        .unwrap_err();

    assert!(
        matches!(err, DispatchError::AggregateKeyInvalid { len: 0, .. }),
        "expected AggregateKeyInvalid {{ len: 0, .. }}, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversize_aggregate_key_rejected_typed() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &OversizeAggEvent)
        .await
        .unwrap_err();

    let DispatchError::AggregateKeyInvalid { len, max } = err else {
        panic!("expected AggregateKeyInvalid, got {err:?}");
    };
    assert_eq!(len, 129);
    assert_eq!(max, 128);
}
