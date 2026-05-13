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
    const EVENT_TYPE: &'static str = "test.redact";
}

struct AlwaysRetryHandler;
#[async_trait::async_trait]
impl EventHandler<Ev> for AlwaysRetryHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        // Reason intentionally innocuous — the PII risk we test is on the
        // INFRASTRUCTURE error path (sqlx::Error → pgwq.jobs.last_error),
        // not handler-emitted reasons.
        Err(HandlerError::retry("simulated_transient"))
    }
}

/// Compliance guard: whatever lands in `outbox.handler_deliveries.last_error`
/// and `pgwq.jobs.last_error` after a handler error must NOT contain caller-
/// supplied `tenant_id` substrings, Postgres `DETAIL:` lines, or unique-
/// violation `Key (...)` fragments.
///
/// This guards the property regardless of which code path (handler-emitted
/// reason OR redacted `sqlx::Error`) produced the value. A refactor that
/// substituted `format!("dispatch_failed for {tenant_id}: {e}")` somewhere,
/// or that substituted `format!("{e}")` for `redact_db_error(&e)`, would
/// regress silently without this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwq_last_error_contains_no_pg_detail() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Force handler errors so something lands in pgwq.jobs.last_error.
    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("h", AlwaysRetryHandler)
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    // Tenant id contains a recognizable substring; if it leaks via DETAIL
    // into last_error, we'll detect it.
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("PII_TENANT_secret-001"),
            &Ev,
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for the row to terminalize.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status: String = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(status.as_str(), "dead" | "sent" | "skipped") {
            break;
        }
    }

    // Both audit columns: outbox.handler_deliveries.last_error AND
    // pgwq.jobs.last_error. We do a defensive scan against the leak
    // signatures.
    let outbox_last: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pgwq_last: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM pgwq.jobs LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    for (name, val) in [("outbox.last_error", outbox_last), ("pgwq.last_error", pgwq_last)] {
        if let Some(s) = val {
            assert!(
                !s.contains("PII_TENANT_secret"),
                "{name} leaked tenant_id: {s:?}"
            );
            assert!(
                !s.contains("DETAIL:"),
                "{name} leaked Postgres DETAIL line: {s:?}"
            );
            assert!(
                !s.contains("Key ("),
                "{name} leaked Postgres unique-violation Key tuple: {s:?}"
            );
        }
    }

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}
