#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DecodeStrategy, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Shape with a required field. Payload `{}` fails to decode as this type.
#[derive(Serialize, Deserialize)]
struct StrictShape {
    needed: String,
}
impl DomainEvent for StrictShape {
    const EVENT_TYPE: &'static str = "test.m3";
}

struct OkHandler;
#[async_trait::async_trait]
impl EventHandler<StrictShape> for OkHandler {
    async fn handle(&self, _: &StrictShape, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Inject a malformed event (payload = `{}`) directly into the DB, bypassing the
/// type-safe dispatch path. The pgwq job payload contains a valid `HandlerEnvelope`
/// pointing at the event row. When the worker decodes the event payload as
/// `StrictShape`, it fails because the `needed` field is missing.
async fn inject_bad_payload(pool: &sqlx::PgPool) -> uuid::Uuid {
    let id = uuid::Uuid::now_v7();
    // Insert event with empty JSON payload — valid JSON bytes but wrong shape.
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, payload) VALUES ($1, $2, '{}'::text::bytea)",
    )
    .bind(id)
    .bind("test.m3")
    .execute(pool)
    .await
    .unwrap();
    // Insert handler_deliveries row (FK to events — already committed above).
    sqlx::query("INSERT INTO outbox.handler_deliveries (event_id, handler_id) VALUES ($1, 'h')")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    // Enqueue the pgwq job with a HandlerEnvelope pointing at the delivery.
    let env = serde_json::to_vec(&serde_json::json!({
        "event_id": id,
        "handler_id": "h"
    }))
    .unwrap();
    sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload) VALUES ('outbox_handler_deliveries', $1)",
    )
    .bind(env)
    .execute(pool)
    .await
    .unwrap();
    id
}

/// With `DecodeStrategy::Retry` and `max_attempts(3)`, a permanently-bad payload
/// should be retried 3 times and then marked dead. The `last_error` must contain
/// "decode" so operators can distinguish decode failures from transient errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_retry_strategy_bad_payload_eventually_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let _ = inject_bad_payload(&pool).await;

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .max_attempts(3)
                .decode_error_strategy(DecodeStrategy::Retry)
                .build()
                .unwrap(),
        )
        .register_handler::<StrictShape, _>("h", OkHandler)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();

    // Wait up to 12 s for the retry budget to be exhausted.
    for _ in 0..60 {
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

    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status::text, attempts, last_error FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "dead", "retry strategy must eventually mark dead");
    assert_eq!(row.1, 3, "should have used the full retry budget (max_attempts=3)");
    assert!(
        row.2.unwrap_or_default().contains("decode"),
        "last_error must mention 'decode'"
    );

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}

/// With `DecodeStrategy::Abort`, a decode failure must mark the delivery dead on
/// the very first attempt — no retries are consumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_abort_strategy_bad_payload_dead_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let _ = inject_bad_payload(&pool).await;

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .max_attempts(5)
                .decode_error_strategy(DecodeStrategy::Abort)
                .build()
                .unwrap(),
        )
        .register_handler::<StrictShape, _>("h", OkHandler)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();

    // Abort means dead on first claim — should happen within a few hundred ms.
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

    let row: (String, i32) = sqlx::query_as(
        "SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "dead", "abort strategy must mark dead immediately");
    assert_eq!(row.1, 1, "abort strategy must NOT retry (attempts must be 1)");

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
