#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BuildError, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
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

/// `handler_timeout` at or below `2 × HANDLER_CLEANUP_BUDGET` (400 ms) leaves
/// no margin for our `mark_*_fenced` UPDATE to land before pgwq's outer
/// cancellation. Reject at `build()` time — defense-in-depth on top of pgwq's
/// own `MIN_HANDLER_TIMEOUT = 1s` so an upstream relaxation can't slip
/// through into `rust_events`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_timeout_below_cleanup_budget_rejected() {
    let err = OutboxConfig::builder()
        .handler_timeout(std::time::Duration::from_millis(400))
        .lease_timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(ref m) if m.contains("HANDLER_CLEANUP_BUDGET")),
        "expected ConfigInvalid mentioning HANDLER_CLEANUP_BUDGET, got {err:?}"
    );
}

/// Just above the threshold (401 ms) — must succeed. Confirms the boundary
/// is strict-greater-than and not strict-equals-or-greater.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_timeout_just_above_cleanup_budget_accepted() {
    let cfg = OutboxConfig::builder()
        .handler_timeout(std::time::Duration::from_millis(401))
        .lease_timeout(std::time::Duration::from_secs(5))
        .build();
    assert!(
        cfg.is_ok(),
        "401ms handler_timeout must pass validation: {cfg:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_config_builds() {
    let (_c, pool) = common::pg_container().await;
    let _outbox = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("audit", H)
        .build()
        .unwrap();
}
