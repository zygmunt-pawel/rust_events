#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BuildError, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.conc_limit_validation";
}

struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// A `concurrency_limit` of 0 is rejected at `build()` with `ConfigInvalid`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_zero_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<Ev, _>("h", H, HandlerOptions::new().concurrency_limit(0))
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(_)),
        "expected ConfigInvalid, got {err:?}"
    );
}

/// A valid `concurrency_limit` builds successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_valid_builds() {
    let (_c, pool) = common::pg_container().await;
    OutboxBuilder::new(pool)
        .register_handler::<Ev, _>("h", H, HandlerOptions::new().concurrency_limit(4))
        .build()
        .expect("valid concurrency_limit must build");
}

/// `concurrency_limit` at the inclusive upper bound `i32::MAX` builds — the
/// documented range is `1..=i32::MAX`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_i32_max_builds() {
    let (_c, pool) = common::pg_container().await;
    OutboxBuilder::new(pool)
        .register_handler::<Ev, _>(
            "h",
            H,
            HandlerOptions::new().concurrency_limit(i32::MAX as u32),
        )
        .build()
        .expect("concurrency_limit of i32::MAX must build");
}

/// `concurrency_limit` one past `i32::MAX` is rejected at `build()` with
/// `ConfigInvalid` — the value would not survive the `i32` round-trip pgwq's
/// claim SQL performs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_above_i32_max_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<Ev, _>(
            "h",
            H,
            HandlerOptions::new().concurrency_limit(i32::MAX as u32 + 1),
        )
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(_)),
        "expected ConfigInvalid, got {err:?}"
    );
}
