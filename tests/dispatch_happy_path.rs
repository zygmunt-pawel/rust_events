#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchOutcome, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderCreated {
    order_id: i64,
    amount: i64,
}
impl DomainEvent for OrderCreated {
    const EVENT_TYPE: &'static str = "shop.order_created";
}

#[derive(Serialize, Deserialize)]
struct BigEvent {
    payload: String,
}
impl DomainEvent for BigEvent {
    const EVENT_TYPE: &'static str = "test.big";
}

struct Noop;

impl<E: DomainEvent> EventHandler<E> for Noop {
    async fn handle(&self, _: &E, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

async fn setup() -> (
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    sqlx::PgPool,
    rust_events::Outbox,
) {
    let (c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<OrderCreated, _>("audit", Noop, HandlerOptions::new())
        .build()
        .unwrap();
    (c, pool, outbox)
}

async fn setup_with_big() -> (
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
    sqlx::PgPool,
    rust_events::Outbox,
) {
    let (c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<BigEvent, _>("big_audit", Noop, HandlerOptions::new())
        .build()
        .unwrap();
    (c, pool, outbox)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_returns_dispatched_with_event_id_and_count() {
    let (_c, pool, outbox) = setup().await;
    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_producer_bc("shop"),
            &OrderCreated {
                order_id: 1,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    match outcome {
        DispatchOutcome::Dispatched {
            event_id,
            deliveries,
        } => {
            assert!(!event_id.is_nil());
            assert_eq!(deliveries, 1);
        }
        other => panic!("expected Dispatched, got {other:?}"),
    }

    // event row exists
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // delivery row queued
    let dcount: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='queued'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dcount, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_duplicate_returns_existing_event_id() {
    let (_c, pool, outbox) = setup().await;
    let mut tx = pool.begin().await.unwrap();
    let first = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let DispatchOutcome::Dispatched {
        event_id: first_id, ..
    } = first
    else {
        unreachable!()
    };

    let mut tx2 = pool.begin().await.unwrap();
    let second = outbox
        .dispatch(
            &mut tx2,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx2.commit().await.unwrap();

    match second {
        DispatchOutcome::Duplicate { event_id } => assert_eq!(event_id, first_id),
        other => panic!("expected Duplicate, got {other:?}"),
    }

    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 1, "second dispatch must not create new event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_too_large_rejected() {
    let (_c, pool, outbox) = setup_with_big().await;
    let big = BigEvent {
        payload: "x".repeat(2 * 1024 * 1024),
    };
    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &big)
        .await
        .unwrap_err();
    tx.rollback().await.unwrap();
    assert!(matches!(
        err,
        rust_events::DispatchError::PayloadTooLarge { .. }
    ));
}

/// Same-tx duplicate: two dispatches with identical `idempotency_key` inside one
/// transaction must collapse to a single event. The second call must observe
/// the first's `INSERT ... ON CONFLICT DO NOTHING` even before commit
/// (Postgres sees in-tx state).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_same_tx_duplicate_collapses_to_one_event() {
    let (_c, pool, outbox) = setup().await;
    let mut tx = pool.begin().await.unwrap();
    let first = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    let second = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let DispatchOutcome::Dispatched {
        event_id: first_id, ..
    } = first
    else {
        panic!("first must be Dispatched, got {first:?}");
    };
    match second {
        DispatchOutcome::Duplicate { event_id } => assert_eq!(event_id, first_id),
        other => panic!("second must be Duplicate, got {other:?}"),
    }

    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 1, "same-tx dup must not create a second event");
}

/// Cross-tenant: the same `idempotency_key` in two different tenants is NOT a
/// duplicate. Two distinct events must persist. Guards against a regression
/// where `ON CONFLICT` lost the `tenant_id` column from its predicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_cross_tenant_keys_do_not_conflict() {
    let (_c, pool, outbox) = setup().await;

    let mut tx_a = pool.begin().await.unwrap();
    let a = outbox
        .dispatch(
            &mut tx_a,
            &DispatchContext::new("tenant_a").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx_a.commit().await.unwrap();

    let mut tx_b = pool.begin().await.unwrap();
    let b = outbox
        .dispatch(
            &mut tx_b,
            &DispatchContext::new("tenant_b").with_idempotency_key("order:42"),
            &OrderCreated {
                order_id: 42,
                amount: 100,
            },
        )
        .await
        .unwrap();
    tx_b.commit().await.unwrap();

    let (
        DispatchOutcome::Dispatched { event_id: id_a, .. },
        DispatchOutcome::Dispatched { event_id: id_b, .. },
    ) = (a, b)
    else {
        panic!("both dispatches must be Dispatched (no cross-tenant collapse)");
    };
    assert_ne!(
        id_a, id_b,
        "different tenants must yield different event_ids"
    );

    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 2);
}
