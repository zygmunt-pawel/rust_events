#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.b1";
}

/// Handler that signals when running and waits for permission to proceed.
///
/// Used in fencing race tests: the test injects a DB update while the handler
/// is paused (simulating another worker winning the race), then unblocks it.
#[derive(Clone)]
struct Gated {
    /// Handler signals on this when it starts (so test knows to inject).
    started: Arc<Notify>,
    /// Handler waits on this before returning (so test controls timing).
    proceed: Arc<Notify>,
    /// What to return once unblocked.
    result: GateResult,
}

#[derive(Clone, Copy)]
enum GateResult {
    Ok,
    Abort,
}

#[async_trait::async_trait]
impl EventHandler<Ev> for Gated {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        self.started.notify_one();
        self.proceed.notified().await;
        match self.result {
            GateResult::Ok => Ok(()),
            GateResult::Abort => Err(HandlerError::abort("gate abort")),
        }
    }
}

// ── Invariant test ─────────────────────────────────────────────────────────────

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
//
// Strategy (mirrors pg_work_queue's mark_done_loses_to_reaper.rs):
//  1. Start outbox with a Gated handler (pauses mid-execution).
//  2. Wait for the handler to signal "I'm running" (handler_deliveries.status='running').
//  3. Inject a DB update to simulate another worker winning:
//     UPDATE handler_deliveries SET status=<X>, finished_at=now(), lease_token=NULL
//  4. Unblock the handler → it returns Ok/Abort.
//  5. handle_envelope's mark_*_fenced(stale_token) finds 0 rows → fenced out (warns).
//  6. Assert final status matches the injected terminal value (not the handler's result).
//
// This is the correct way to test the fencing invariant within pg_work_queue's
// constraint that handler_timeout + 1s <= lease_timeout (reaper can't fire while
// handler is alive; fencing must be tested via explicit DB injection).

async fn b1_gated_fencing_race(
    handler_result: GateResult,
    injected_status: &'static str,
    expected_final_status: &'static str,
) {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let started = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .poll_interval(Duration::from_millis(100))
                .concurrency(1)
                .max_attempts(5)
                .build()
                .unwrap(),
        )
        .register_handler::<Ev, _>(
            "h",
            Gated {
                started: started.clone(),
                proceed: proceed.clone(),
                result: handler_result,
            },
        )
        .build()
        .unwrap();

    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for handler to signal it is running.
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler did not start within 10s");

    // Inject: simulate a concurrent worker winning with `injected_status`.
    // This changes lease_token=NULL, so handle_envelope's mark_*_fenced
    // (called after we unblock the handler) will find 0 rows and be fenced out.
    let inject_sql = match injected_status {
        "sent" => "UPDATE outbox.handler_deliveries \
                   SET status='sent', finished_at=now(), lease_token=NULL, last_error=NULL \
                   WHERE status='running'",
        "dead" => "UPDATE outbox.handler_deliveries \
                   SET status='dead', finished_at=now(), lease_token=NULL, last_error='injected' \
                   WHERE status='running'",
        other => panic!("unknown injected_status: {other}"),
    };
    let n = sqlx::query(inject_sql)
        .execute(&pool)
        .await
        .expect("inject update")
        .rows_affected();
    assert_eq!(n, 1, "injection must hit exactly 1 running row");

    // Unblock the handler → it returns Ok or Abort.
    proceed.notify_one();

    // Give handle_envelope time to attempt its (fenced) mark_*_fenced and finish.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        final_status, expected_final_status,
        "fencing should preserve the injected terminal verdict, not the handler's result"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_sent_remains_sent() {
    // Handler returns Ok, but concurrent worker already marked 'sent' → stays 'sent'.
    b1_gated_fencing_race(GateResult::Ok, "sent", "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_abort_after_concurrent_sent_remains_sent() {
    // Handler returns Abort, but concurrent worker already marked 'sent' → stays 'sent'.
    b1_gated_fencing_race(GateResult::Abort, "sent", "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_dead_remains_dead() {
    // Handler returns Ok, but concurrent worker already marked 'dead' → stays 'dead'.
    b1_gated_fencing_race(GateResult::Ok, "dead", "dead").await;
}

/// `attempts` must NEVER regress, even when a stale worker re-claims a job
/// whose `pgwq` view of `ctx.attempt` resets. Codified the `GREATEST(hd.attempts, $3)`
/// invariant in the fenced CTE (finding #14).
///
/// Strategy: dispatch one event, force the audit row to a high `attempts`
/// value while it is `awaiting_retry`, then claim happens again with
/// `ctx.attempt < hd.attempts` (simulated by manually crafting state). After
/// the next claim transitions through the CTE, `attempts` must equal the
/// pre-existing high value, not the lower `ctx.attempt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attempts_never_regress_across_fence_out() {
    type Row = (
        Vec<u8>,
        String,
        String,
        serde_json::Value,
        Option<String>,
        Option<String>,
        bool,
    );
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Insert an event + handler_deliveries row directly in awaiting_retry
    // with attempts=7 (simulating "old worker got that far").
    let event_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, payload) VALUES ($1, 'test.b1', '\\x'::bytea)",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .unwrap();
    // Temporal CHECK requires first_attempted_at >= created_at. Pin
    // created_at to a past moment so first/last_attempted_at can also be
    // in the past while staying ordered.
    sqlx::query(
        "INSERT INTO outbox.handler_deliveries
            (event_id, handler_id, status, attempts, created_at,
             first_attempted_at, last_attempted_at)
         VALUES ($1, 'h', 'awaiting_retry', 7,
                 now() - interval '5 minutes',
                 now() - interval '1 minute',
                 now() - interval '1 second')",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .unwrap();

    // Directly run the SAME CTE that the worker uses, with ctx.attempt = 1
    // (simulating pgwq fresh claim). attempts must remain 7, not regress to 1.
    let new_lease = uuid::Uuid::now_v7();
    let _: Option<Row> = sqlx::query_as(
        r"
        WITH locked AS (
            SELECT id, status FROM outbox.handler_deliveries
            WHERE event_id = $1 AND handler_id = $2
            FOR UPDATE
        ),
        updated AS (
            UPDATE outbox.handler_deliveries hd
            SET status = 'running',
                lease_token = $4,
                attempts = GREATEST(hd.attempts, $3),
                last_attempted_at = now(),
                first_attempted_at = COALESCE(hd.first_attempted_at, now()),
                last_error = NULL
            FROM locked
            WHERE hd.id = locked.id
              AND locked.status NOT IN ('sent','dead','skipped')
            RETURNING hd.id
        )
        SELECT e.payload, e.tenant_id, e.producer_bc, e.headers,
               dk.idempotency_key,
               (SELECT status::text FROM locked),
               EXISTS(SELECT 1 FROM updated)
        FROM outbox.events e
        LEFT JOIN outbox.dispatch_keys dk ON dk.event_id = e.id
        WHERE e.id = $1
        ",
    )
    .bind(event_id)
    .bind("h")
    .bind(1_i32) // ctx.attempt regressed to 1
    .bind(new_lease)
    .fetch_optional(&pool)
    .await
    .unwrap();

    let post: i32 = sqlx::query_scalar(
        "SELECT attempts FROM outbox.handler_deliveries WHERE event_id=$1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(post, 7, "attempts must not regress (GREATEST invariant)");
}
