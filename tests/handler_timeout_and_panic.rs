#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BackoffPolicy, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder, OutboxConfig, PanicPolicy,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.htp";
}

// ── handler_timeout ──────────────────────────────────────────────────────────

struct SleepyHandler {
    delay: Duration,
}
impl EventHandler<Ev> for SleepyHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

/// A handler that sleeps past `handler_timeout` is cancelled by `handle_envelope`'s
/// own `tokio::time::timeout` wrap (fires `HANDLER_CLEANUP_BUDGET` before
/// pgwq's outer one), routes through `HandlerError::retry("handler_timeout")`,
/// and after `max_attempts` retries the audit row terminalizes at
/// `status='dead'` — never stays in `running` or `queued`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_timeout_terminalizes_to_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Short-ish lease and the minimum handler_timeout pgwq allows (1s).
    // Quick backoff to keep test wall-clock low.
    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(1))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "sleepy",
            SleepyHandler {
                delay: Duration::from_secs(10),
            },
            HandlerOptions::new(),
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

    // Wait up to 10s for terminal state.
    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent" | "skipped") {
            break;
        }
    }
    assert_eq!(
        final_status, "dead",
        "handler_timeout must terminalize to dead after retries"
    );

    // No row left in 'running'.
    let running: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(running, 0, "no audit row should remain in 'running'");

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

// ── PanicPolicy::Retry ───────────────────────────────────────────────────────

struct PanicOnceHandler {
    calls: Arc<AtomicU32>,
}
impl EventHandler<Ev> for PanicOnceHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(n != 0, "intentional panic on first attempt");
        Ok(())
    }
}

/// `PanicPolicy::Retry` (default): the first attempt panics → pgwq synthesizes
/// `JobError::Retry`, fences correctly, the second attempt succeeds and the
/// audit row goes `sent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_retry_recovers_on_next_attempt() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let calls = Arc::new(AtomicU32::new(0));
    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(3)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Retry)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "p",
            PanicOnceHandler {
                calls: calls.clone(),
            },
            HandlerOptions::new(),
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

    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "sent" | "dead") {
            break;
        }
    }
    assert_eq!(
        final_status, "sent",
        "PanicPolicy::Retry must recover, got: {final_status}"
    );
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "handler must be retried at least once"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

// ── PanicPolicy::Dead ────────────────────────────────────────────────────────

struct AlwaysPanicHandler;
impl EventHandler<Ev> for AlwaysPanicHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        panic!("permanent panic");
    }
}

/// `PanicPolicy::Dead`: a panicking handler terminalizes the audit row on the
/// FIRST attempt. Guards the fix for the `rust_events` ↔ pgwq integration —
/// previously pgwq's `flip_dead_state` bypassed our `mark_*_fenced` and the
/// row stayed `running` forever. Now `handle_envelope` catches the panic via
/// `FutureExt::catch_unwind` and routes through `mark_dead_fenced`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_dead_terminalizes_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(5)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Dead)
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", AlwaysPanicHandler, HandlerOptions::new())
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    let mut attempts: i32 = 0;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let row: (String, i32) =
            sqlx::query_as("SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        final_status = row.0;
        attempts = row.1;
        if matches!(final_status.as_str(), "dead" | "sent") {
            break;
        }
    }
    assert_eq!(
        final_status, "dead",
        "PanicPolicy::Dead must terminalize on first panic"
    );
    assert_eq!(
        attempts, 1,
        "PanicPolicy::Dead must NOT consume additional attempts"
    );

    // No row left in 'running'.
    let running: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(running, 0);

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

struct NulPanicHandler;
impl EventHandler<Ev> for NulPanicHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        panic!("nul:\0 ansi:\x1b[31m");
    }
}

/// Panic payload with NUL + ANSI escapes — must not break either our
/// `mark_dead_fenced` UPDATE (`handler_deliveries.last_error` CHECK rejects NUL
/// via Postgres 22021) NOR pgwq's `mark_dead` (`jobs.last_error` has the same
/// constraint). Audit terminalizes; both `last_error` fields contain '?'
/// replacement characters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_with_nul_and_ansi_terminalizes_safely() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(1)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Dead)
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", NulPanicHandler, HandlerOptions::new())
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut hd_err: Option<String> = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let row: (String, Option<String>) = sqlx::query_as(
            "SELECT status::text, last_error FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if row.0 == "dead" {
            hd_err = row.1;
            break;
        }
    }
    let hd_err = hd_err.expect("audit row should reach dead with last_error set");
    assert!(
        !hd_err.contains('\0'),
        "audit last_error must not contain NUL"
    );
    assert!(
        !hd_err.contains('\x1b'),
        "audit last_error must not contain ESC"
    );
    assert!(
        hd_err.contains("panic:"),
        "audit last_error should preserve panic: prefix, got {hd_err:?}"
    );

    // pgwq.jobs.last_error must also be sanitized — if our JobError::abort
    // had carried the raw NUL, pgwq's mark_dead write would have hit
    // SQLSTATE 22021 and the worker would loop.
    let pgwq_err: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM pgwq.jobs WHERE queue = 'outbox_handler_deliveries' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pgwq_err = pgwq_err.expect("pgwq.jobs.last_error should be set");
    assert!(
        !pgwq_err.contains('\0'),
        "pgwq last_error must not contain NUL"
    );
    assert!(
        !pgwq_err.contains('\x1b'),
        "pgwq last_error must not contain ESC"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

struct NulRetryHandler;
impl EventHandler<Ev> for NulRetryHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Err(HandlerError::retry("nul:\0 ansi:\x1b[31m"))
    }
}

/// Handler-emitted retry reason with NUL/ANSI — user code is untrusted, the
/// crate must sanitize at the boundary before the string reaches either
/// audit or pgwq `last_error`. Same invariants as the panic test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_retry_with_nul_reason_terminalizes_safely() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(1)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("r", NulRetryHandler, HandlerOptions::new())
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let status: String =
            sqlx::query_scalar("SELECT status::text FROM outbox.handler_deliveries LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        if status == "dead" {
            break;
        }
    }

    let pgwq_err: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM pgwq.jobs WHERE queue = 'outbox_handler_deliveries' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pgwq_err = pgwq_err.expect("pgwq.jobs.last_error should be set");
    assert!(!pgwq_err.contains('\0'));
    assert!(!pgwq_err.contains('\x1b'));

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// `PanicPolicy::Retry` with attempts exhausted: every attempt panics, after
/// `max_attempts` the row reaches `dead` via `mark_dead_fenced`, never stuck
/// in `running`. Complements the recovery test above by exercising the
/// "panic + retry budget exhausted" terminal path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_policy_retry_exhausted_terminalizes_to_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(5))
        .handler_timeout(Duration::from_secs(2))
        .panic_policy(PanicPolicy::Retry)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("p", AlwaysPanicHandler, HandlerOptions::new())
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent") {
            break;
        }
    }
    assert_eq!(final_status, "dead");

    let running: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(running, 0);

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

// ── per-handler handler_timeout override ─────────────────────────────────────

/// (a) Enforcement: a per-handler `handler_timeout` override is honored by
/// `handle_envelope`. Global timeout is 20 s; the handler gets a 1 s override
/// and sleeps 10 s. It terminalizes to `dead` in well under 7 s — proof the
/// 1 s override, not the 20 s global, drove cancellation: a single
/// global-driven attempt alone would take ~19.8 s. The elapsed-time assertion
/// IS the discriminator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_override_is_enforced() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20)) // global ceiling
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "sleepy",
            SleepyHandler {
                delay: Duration::from_secs(10),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
        )
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let started = Instant::now();
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Override path ≈ 2 × ~800 ms timeout + 100 ms backoff + claim latency
    // ≈ under 4 s. Poll up to 12 s so a slow container never false-FAILs the
    // status check; the `elapsed < 7s` assertion is what proves the override
    // (not the ~19.8 s global path) drove it.
    let mut final_status = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent" | "skipped") {
            break;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(
        final_status, "dead",
        "override-bounded handler must terminalize to dead"
    );
    assert!(
        elapsed < Duration::from_secs(7),
        "1s per-handler override must terminalize well before the ~19.8s a \
         20s-global-driven path needs; elapsed {elapsed:?}"
    );

    let running: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(running, 0, "no audit row should remain in 'running'");

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// (b) Non-interference: a handler that finishes within its tight per-handler
/// budget reaches `sent`. 1 s override ⇒ ~800 ms effective budget; the handler
/// sleeps 200 ms. Proves `effective_timeout` is wired into the success path
/// and does not wrongly cancel a handler that fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_override_allows_fast_handler() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "fast",
            SleepyHandler {
                delay: Duration::from_millis(200),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
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

    let mut final_status = String::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "sent" | "dead" | "skipped") {
            break;
        }
    }
    assert_eq!(
        final_status, "sent",
        "handler finishing within its per-handler budget must reach 'sent'"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// (c) Per-`handler_id` resolution: two handlers on the SAME event type, one
/// with a tight override (times out → dead), one with no override (uses the
/// generous global → sent). Proves the timeout is resolved per handler, not
/// once globally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_resolved_per_handler_id() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        // "tight": 1s override, sleeps 10s → must die.
        .register_handler::<Ev, _>(
            "tight",
            SleepyHandler {
                delay: Duration::from_secs(10),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
        )
        // "loose": no override, uses the 20s global, sleeps 200ms → must send.
        .register_handler::<Ev, _>(
            "loose",
            SleepyHandler {
                delay: Duration::from_millis(200),
            },
            HandlerOptions::new(),
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

    // Poll until both deliveries are terminal (up to 12s).
    let mut tight_status = String::new();
    let mut loose_status = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        tight_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='tight'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        loose_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='loose'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let tight_done = matches!(tight_status.as_str(), "dead" | "sent" | "skipped");
        let loose_done = matches!(loose_status.as_str(), "dead" | "sent" | "skipped");
        if tight_done && loose_done {
            break;
        }
    }
    assert_eq!(
        tight_status, "dead",
        "handler with 1s override must die; got {tight_status}"
    );
    assert_eq!(
        loose_status, "sent",
        "handler with no override (20s global) must send; got {loose_status}"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}
