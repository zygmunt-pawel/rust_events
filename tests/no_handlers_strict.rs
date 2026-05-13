#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{DispatchContext, DispatchError, DispatchOutcome, DomainEvent, OutboxBuilder};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Orphan {
    x: i32,
}
impl DomainEvent for Orphan {
    const EVENT_TYPE: &'static str = "test.orphan";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_default_no_handler_returns_error_no_persist() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone()).build().unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Orphan { x: 1 })
        .await
        .unwrap_err();
    tx.rollback().await.unwrap();

    assert!(matches!(
        err,
        DispatchError::NoHandlersRegistered {
            event_type: "test.orphan"
        }
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "strict mode must not persist event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_allow_no_handlers_opt_in_returns_outcome() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Orphan { x: 1 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    match outcome {
        DispatchOutcome::NoHandlers { event_id } => assert!(!event_id.is_nil()),
        other => panic!("expected NoHandlers, got {other:?}"),
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let dcount: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dcount, 0);
}
