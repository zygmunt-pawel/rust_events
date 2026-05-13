#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct SafetyEv {
    n: i32,
}
impl DomainEvent for SafetyEv {
    const EVENT_TYPE: &'static str = "test.purge_safety";
}

struct Noop;
#[async_trait::async_trait]
impl EventHandler<SafetyEv> for Noop {
    async fn handle(&self, _: &SafetyEv, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// `purge_events` MUST NOT delete an event whose delivery is still in a
/// non-terminal state (`queued`). The worker is never started so the delivery
/// row stays `queued` indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_refuses_event_with_pending_delivery() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(false)
        .register_handler::<SafetyEv, _>("audit", Noop)
        .build()
        .unwrap();

    // Dispatch the event — worker NOT started, delivery stays `queued`.
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &SafetyEv { n: 1 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Verify delivery is queued (non-terminal).
    let queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='queued'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(queued, 1, "delivery should be queued");

    // purge_events with Duration::ZERO — cutoff is now, row is older, but NOT
    // EXISTS guard prevents deletion because the delivery is still non-terminal.
    let deleted = rust_events::purge_events(&pool, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(deleted, 0, "purge_events must refuse event with pending delivery");

    // Event must still be present.
    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 1, "event must still exist after refused purge");
}

/// After a worker processes the delivery to `sent`, `purge_terminal_deliveries`
/// removes the delivery row and then `purge_events` can delete the event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m4_deletes_event_with_all_deliveries_terminal() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // concurrency=1 + fast poll: safe for a 10-conn pool (needs ~4 conns).
    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<SafetyEv, _>("audit", Noop)
        .build()
        .unwrap();

    // Dispatch the event.
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &SafetyEv { n: 2 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Start the worker and wait for the delivery to reach `sent`.
    let handle = outbox.start().await.unwrap();

    // Poll until delivery is terminal (up to 10 seconds).
    let mut attempts = 0u32;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let sent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if sent >= 1 {
            break;
        }
        attempts += 1;
        assert!(attempts < 100, "delivery never reached 'sent' after 10s");
    }

    // Graceful shutdown.
    let _ = handle.shutdown(Duration::from_secs(5)).await.unwrap();

    // Purge terminal deliveries first (removes 1 row).
    let del_deliveries = rust_events::purge_terminal_deliveries(&pool, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(del_deliveries, 1, "expected 1 terminal delivery purged");

    // Now purge events — all deliveries terminal (and already deleted), so
    // NOT EXISTS check passes.
    let del_events = rust_events::purge_events(&pool, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(del_events, 1, "expected 1 event purged");

    // Events table must be empty.
    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 0, "events table must be empty after purge");
}
