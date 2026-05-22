#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DeliveryStatus, DispatchContext, DispatchOutcome, DomainEvent, EventHandler, HandlerContext,
    HandlerError, HandlerOptions, OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.handler_removed";
}

struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Single-instance handler-miss: a job is dispatched while handler "h" is
/// registered, then a worker is started WITHOUT "h" (simulating a deploy
/// that removed the handler). The delivery must be marked `dead` on first
/// claim — there is no other replica to pick it up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_removed_delivery_marked_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Dispatcher: handler "h" registered so dispatch() creates the
    // handler_deliveries row + pgwq job.
    let dispatcher = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H, HandlerOptions::new())
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let ctx = DispatchContext::new("default");
    let event_id = match dispatcher.dispatch(&mut tx, &ctx, &Ev).await.unwrap() {
        DispatchOutcome::Dispatched { event_id, .. } => event_id,
        other => panic!("expected Dispatched, got {other:?}"),
    };
    tx.commit().await.unwrap();

    // Worker WITHOUT handler "h" — allow_no_handlers(true) so build()
    // succeeds with an empty registry.
    let worker = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder().concurrency(1).build().unwrap())
        .allow_no_handlers(true)
        .build()
        .unwrap();
    let handle = worker.start().await.unwrap();

    // Poll until the delivery reaches a terminal state.
    let history = dispatcher.history();
    let mut status = None;
    for _ in 0..50 {
        let rows = history.handler_deliveries_for(event_id).await.unwrap();
        if let Some(r) = rows.first()
            && matches!(r.status, DeliveryStatus::Dead)
        {
            status = Some(r.status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        status,
        Some(DeliveryStatus::Dead),
        "handler removed across deploy must mark the delivery dead"
    );
}

/// Partial handler removal — the realistic deploy case. An event type carries
/// two handlers, `"keep"` and `"drop"`. The event is dispatched while both are
/// registered (two `handler_deliveries` rows created), then a worker is
/// started with only `"keep"`. The `"keep"` delivery must run to `sent`; the
/// `"drop"` delivery must be marked `dead` — independently, with neither
/// outcome contaminating the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_handler_removal_marks_only_missing_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Dispatcher: both handlers registered → dispatch() fans out to two
    // handler_deliveries rows + two pgwq jobs.
    let dispatcher = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("keep", H, HandlerOptions::new())
        .register_handler::<Ev, _>("drop", H, HandlerOptions::new())
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let ctx = DispatchContext::new("default");
    let event_id = match dispatcher.dispatch(&mut tx, &ctx, &Ev).await.unwrap() {
        DispatchOutcome::Dispatched { event_id, .. } => event_id,
        other => panic!("expected Dispatched, got {other:?}"),
    };
    tx.commit().await.unwrap();

    // Worker WITHOUT "drop" — simulates a deploy that retired that handler.
    let worker = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder().concurrency(1).build().unwrap())
        .register_handler::<Ev, _>("keep", H, HandlerOptions::new())
        .build()
        .unwrap();
    let handle = worker.start().await.unwrap();

    // Poll until BOTH deliveries reach a terminal state.
    let history = dispatcher.history();
    let mut keep = None;
    let mut drop = None;
    for _ in 0..50 {
        let rows = history.handler_deliveries_for(event_id).await.unwrap();
        if rows.len() == 2
            && rows
                .iter()
                .all(|r| matches!(r.status, DeliveryStatus::Sent | DeliveryStatus::Dead))
        {
            for r in rows {
                match r.handler_id.as_str() {
                    "keep" => keep = Some(r.status),
                    "drop" => drop = Some(r.status),
                    other => panic!("unexpected handler_id {other}"),
                }
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(
        keep,
        Some(DeliveryStatus::Sent),
        "the still-registered handler must run to completion"
    );
    assert_eq!(
        drop,
        Some(DeliveryStatus::Dead),
        "the removed handler's delivery must be marked dead"
    );
}
