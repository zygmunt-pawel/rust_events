#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DeliveryStatus, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev {
    x: i32,
}
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.history";
}

struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_returns_event_and_deliveries() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("audit", H, HandlerOptions::new())
        .register_handler::<Ev, _>("metrics", H, HandlerOptions::new())
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &Ev { x: 7 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let rust_events::DispatchOutcome::Dispatched { event_id, .. } = outcome else {
        unreachable!()
    };

    let ev = outbox.history().event(event_id).await.unwrap().unwrap();
    assert_eq!(ev.event_type, "test.history");
    assert_eq!(ev.tenant_id, "t1");

    let deliveries = outbox
        .history()
        .handler_deliveries_for(event_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].handler_id, "audit");
    assert_eq!(deliveries[1].handler_id, "metrics");
    assert_eq!(deliveries[0].status, DeliveryStatus::Queued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_event_returns_none_for_unknown_id() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool)
        .allow_no_handlers(true)
        .build()
        .unwrap();
    let r = outbox.history().event(uuid::Uuid::nil()).await.unwrap();
    assert!(r.is_none());
}
