#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{purge_dispatch_keys, purge_events, purge_terminal_deliveries};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_purge_signatures_no_chunk_size_argument() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let a = purge_terminal_deliveries(&pool, Duration::ZERO).await.unwrap();
    let b = purge_dispatch_keys(&pool, Duration::ZERO).await.unwrap();
    let c = purge_events(&pool, Duration::ZERO).await.unwrap();

    assert_eq!((a, b, c), (0, 0, 0));
}
