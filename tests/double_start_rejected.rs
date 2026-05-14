#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder, OutboxConfig,
    StartError,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ping;
impl DomainEvent for Ping {
    const EVENT_TYPE: &'static str = "test.ping";
}

struct Noop;
impl EventHandler<Ping> for Noop {
    async fn handle(&self, _: &Ping, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_start_returns_already_started() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    // concurrency=1 requires only 1*2+2=4 connections — safe with the
    // default pool (max_connections=10).
    let cfg = OutboxConfig::builder().concurrency(1).build().unwrap();
    let outbox = OutboxBuilder::new(pool)
        .config(cfg)
        .register_handler::<Ping, _>("audit", Noop)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();
    let err = outbox.start().await.unwrap_err();
    assert!(matches!(err, StartError::AlreadyStarted));
    let _ = h.shutdown(std::time::Duration::from_secs(2)).await.unwrap();
}
