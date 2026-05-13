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
struct NewEv;
impl DomainEvent for NewEv {
    const EVENT_TYPE: &'static str = "test.m2_new";
}

struct H;
#[async_trait::async_trait]
impl EventHandler<NewEv> for H {
    async fn handle(&self, _: &NewEv, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Verifies M2 loose-mode behaviour: when a worker replica does not have a
/// handler registered for a given event type (rolling-deploy scenario), it
/// must leave `handler_deliveries.attempts = 0` — i.e. the CTE transition is
/// skipped entirely. A later replica that does have the handler will pick up
/// and process the delivery normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_loose_handler_added_later_eventually_handled() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Outbox A: dispatcher with handler "h" registered so that dispatch()
    // creates a handler_deliveries row + pgwq job. allow_no_handlers=true
    // is a belt-and-suspenders guard but not strictly required here.
    let outbox_a = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .register_handler::<NewEv, _>("h", H)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox_a
        .dispatch(&mut tx, &DispatchContext::new("t"), &NewEv)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Outbox B: worker WITHOUT "h" registered. strict_handler_lookup=false
    // (loose mode) so missing handler → retry without touching handler_deliveries.
    // max_attempts=50 ensures the pgwq job keeps retrying for the full 1s window.
    let outbox_b_no_handler = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true) // suppress build-time no-handler check
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .strict_handler_lookup(false) // default — explicit for clarity
                .max_attempts(50)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let h_b = outbox_b_no_handler.start().await.unwrap();

    // Let B run for 1 s (≈10 poll cycles at 100ms). In loose mode the wrapper
    // returns Err(retry) BEFORE executing the CTE UPDATE, so handler_deliveries
    // must remain status='queued', attempts=0 throughout.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let row: (String, i32) = sqlx::query_as(
        "SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "queued", "loose mode must leave status='queued'");
    assert_eq!(
        row.1, 0,
        "loose mode must not bump attempts when handler missing"
    );

    let _ = h_b.shutdown(Duration::from_secs(2)).await.unwrap();

    // Outbox C: with "h" registered — should pick up and mark sent.
    let outbox_c = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .build()
                .unwrap(),
        )
        .register_handler::<NewEv, _>("h", H)
        .build()
        .unwrap();
    let h_c = outbox_c.start().await.unwrap();

    for _ in 0..30 {
        let sent: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if sent == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let sent: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sent, 1, "outbox C must deliver the event to 'sent'");

    let _ = h_c.shutdown(Duration::from_secs(2)).await.unwrap();
}

/// Verifies M2 strict-mode behaviour: when `strict_handler_lookup=true` and no
/// handler is registered for the event's `handler_id`, the worker must
/// `mark_dead_fenced` immediately and abort the pgwq job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_strict_handler_missing_dead_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Dispatcher registers "h" to create handler_deliveries row.
    let dispatcher = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .register_handler::<NewEv, _>("h", H)
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    dispatcher
        .dispatch(&mut tx, &DispatchContext::new("t"), &NewEv)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Worker in strict mode WITHOUT the handler — should mark_dead on first claim.
    let strict = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true) // suppress build-time no-handler check
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .strict_handler_lookup(true)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let h = strict.start().await.unwrap();

    for _ in 0..30 {
        let dead: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if dead == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let dead: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dead, 1, "strict mode must mark delivery dead immediately");

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
