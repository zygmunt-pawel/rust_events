#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{OutboxBuilder, OutboxConfig, StartError};

/// `Outbox::start()` rejects a pool whose `max_connections` is below the
/// `concurrency × 2 + 2` floor. An undersized pool can starve `mark_*_fenced`
/// and strand a delivery at `status='running'`, so the worker must refuse to
/// start rather than run degraded. The pool check runs before the schema
/// probe, so no migrator is needed here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_rejects_pool_below_floor() {
    // Pool of 2 connections, concurrency 4 → floor is 4 × 2 + 2 = 10.
    let (_c, pool) = common::pg_container_with_pool(2).await;

    let outbox = OutboxBuilder::new(pool)
        .config(OutboxConfig::builder().concurrency(4).build().unwrap())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let err = outbox.start().await.unwrap_err();
    assert!(
        matches!(
            err,
            StartError::PoolTooSmall {
                max_connections: 2,
                concurrency: 4,
                required: 10,
            }
        ),
        "expected PoolTooSmall {{ max_connections: 2, concurrency: 4, required: 10 }}, \
         got {err:?}"
    );
}
