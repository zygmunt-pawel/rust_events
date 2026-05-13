#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder, StartError,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ping;
impl DomainEvent for Ping {
    const EVENT_TYPE: &'static str = "test.ping";
}
struct H;
#[async_trait::async_trait]
impl EventHandler<Ping> for H {
    async fn handle(&self, _: &Ping, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// After a failed `start()` (here induced by a closed pool), a second `start()`
/// must NOT return `AlreadyStarted`. The drop-guard releases the `started` flag
/// on Err paths so callers can retry once the underlying cause is resolved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_after_failure_does_not_return_already_started() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ping, _>("h", H)
        .build()
        .unwrap();

    // Close the pool to force pgwq Worker::build/start to fail.
    pool.close().await;

    let first = outbox.start().await;
    assert!(first.is_err(), "first start must fail on closed pool");
    assert!(
        !matches!(first, Err(StartError::AlreadyStarted)),
        "first start must not surface as AlreadyStarted: {first:?}",
    );

    // The bug: pre-fix, `started` is permanently true → second start returns
    // AlreadyStarted regardless of underlying state. Post-fix, the drop-guard
    // releases the flag so we get the same underlying error again (or success,
    // if pool were recovered).
    let second = outbox.start().await;
    assert!(
        !matches!(second, Err(StartError::AlreadyStarted)),
        "second start must NOT return AlreadyStarted after first failure: {second:?}",
    );
}

// Note: the success-half of the RAII contract — disarm() on success keeps
// `started == true`, so the second start returns AlreadyStarted — is covered
// by `tests/double_start_rejected.rs::second_start_returns_already_started`.
// The Err-half is covered above. Together they prove both branches of
// `StartedGuard`. A composed Err → Ok → Err(AlreadyStarted) test in a single
// instance was attempted but requires a recoverable pgwq build/start failure,
// which `_sqlx_migrations` ledger persistence makes awkward to forge.
