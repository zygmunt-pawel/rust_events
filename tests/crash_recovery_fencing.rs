#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.b1";
}

#[derive(Clone, Copy)]
enum HandlerOutcome {
    Ok,
    Abort,
}

struct Slow {
    sleep_ms: u64,
    result: HandlerOutcome,
}

#[async_trait::async_trait]
impl EventHandler<Ev> for Slow {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        match self.result {
            HandlerOutcome::Ok => Ok(()),
            HandlerOutcome::Abort => Err(HandlerError::abort("late abort")),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_invariant_lease_token_required_iff_running() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let event_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, payload) VALUES ($1, $2, '\\x'::bytea)",
    )
    .bind(event_id)
    .bind("test")
    .execute(&pool)
    .await
    .unwrap();

    // status='running' without lease_token → CHECK violation
    let r = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
            (event_id, handler_id, status, attempts, first_attempted_at, last_attempted_at)
         VALUES ($1, 'h', 'running', 1, now(), now())",
    )
    .bind(event_id)
    .execute(&pool)
    .await;
    assert!(r.is_err(), "running without lease_token must violate CHECK");

    // status='queued' with lease_token → CHECK violation
    let r2 = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
            (event_id, handler_id, lease_token) VALUES ($1, 'h2', gen_random_uuid())",
    )
    .bind(event_id)
    .execute(&pool)
    .await;
    assert!(r2.is_err(), "queued with lease_token must violate CHECK");
}

// ── Race tests (B1 fencing scenarios) ─────────────────────────────────────────

async fn b1_two_outbox_race(
    first_result: HandlerOutcome,
    second_result: HandlerOutcome,
    expected_final_status: &str,
) {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Outbox A: slow handler (3s sleep — past lease).
    let outbox_a = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .poll_interval(Duration::from_millis(100))
                .concurrency(1)
                .lease_timeout(Duration::from_secs(1))
                .handler_timeout(Duration::from_millis(800))
                .max_attempts(5)
                .build()
                .unwrap(),
        )
        .register_handler::<Ev, _>("h", Slow { sleep_ms: 3000, result: first_result })
        .build()
        .unwrap();

    // Outbox B: fast handler.
    let outbox_b = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .poll_interval(Duration::from_millis(100))
                .concurrency(1)
                .lease_timeout(Duration::from_secs(1))
                .handler_timeout(Duration::from_millis(800))
                .max_attempts(5)
                .build()
                .unwrap(),
        )
        .register_handler::<Ev, _>("h", Slow { sleep_ms: 50, result: second_result })
        .build()
        .unwrap();

    let h_a = outbox_a.start().await.unwrap();
    let h_b = outbox_b.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox_a
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for terminal status.
    for _ in 0..100 {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .unwrap();
        if matches!(status.as_deref(), Some("sent" | "dead" | "skipped")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Give A time to wake and try its (fenced) mark.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let final_status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        final_status, expected_final_status,
        "fencing should preserve the first concurrent terminal verdict"
    );

    let _ = h_a.shutdown(Duration::from_secs(3)).await;
    let _ = h_b.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_sent_remains_sent() {
    b1_two_outbox_race(HandlerOutcome::Ok, HandlerOutcome::Ok, "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_abort_after_concurrent_sent_remains_sent() {
    b1_two_outbox_race(HandlerOutcome::Abort, HandlerOutcome::Ok, "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_dead_remains_dead() {
    b1_two_outbox_race(HandlerOutcome::Ok, HandlerOutcome::Abort, "dead").await;
}
