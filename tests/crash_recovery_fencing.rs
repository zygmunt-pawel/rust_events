#![allow(missing_docs, dead_code, unused_imports)]
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

// ── Race tests (B1 fencing scenarios) — stubbed, will be filled in next commit ─
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "race tests stubbed — implemented in next commit"]
async fn b1_stale_ok_after_concurrent_sent_remains_sent() {}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "race tests stubbed — implemented in next commit"]
async fn b1_stale_abort_after_concurrent_sent_remains_sent() {}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "race tests stubbed — implemented in next commit"]
async fn b1_stale_ok_after_concurrent_dead_remains_dead() {}
