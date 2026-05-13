#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use async_trait::async_trait;
use rust_events::{
    BuildError, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct E1 {
    x: i32,
}

impl DomainEvent for E1 {
    const EVENT_TYPE: &'static str = "test.e1";
}

struct H;

#[async_trait]
impl EventHandler<E1> for H {
    async fn handle(&self, _: &E1, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("", H)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::HandlerIdEmpty));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let long = "x".repeat(129);
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>(long, H)
        .build()
        .unwrap_err();
    assert!(matches!(
        err,
        BuildError::HandlerIdTooLong { len: 129, max: 128 }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("audit", H)
        .register_handler::<E1, _>("audit", H)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::DuplicateHandlerId { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_config_concurrency_zero() {
    let (_c, pool) = common::pg_container().await;
    drop(pool);
    let cfg_err = OutboxConfig::builder().concurrency(0).build().unwrap_err();
    assert!(matches!(cfg_err, BuildError::ConfigInvalid(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_config_builds() {
    let (_c, pool) = common::pg_container().await;
    let _outbox = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("audit", H)
        .build()
        .unwrap();
}
