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
    const EVENT_TYPE: &'static str = "test.m1";
}

struct Trip;

#[async_trait::async_trait]
impl EventHandler<Ev> for Trip {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_missing_row_aborts_with_audit_missing_tracing() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .poll_interval(Duration::from_millis(200))
                .concurrency(1)
                .max_attempts(2)
                .build()
                .unwrap(),
        )
        .register_handler::<Ev, _>("h", Trip)
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Small delay so dispatch commit is visible to other connections,
    // then DELETE the row before the worker can claim it. Worker polls every 200ms;
    // immediate DELETE has a window of ~200ms before claim.
    tokio::time::sleep(Duration::from_millis(50)).await;
    sqlx::query("DELETE FROM outbox.handler_deliveries")
        .execute(&pool)
        .await
        .unwrap();

    // Wait for pgwq job to reach 'dead' (handler returned Abort).
    for _ in 0..30 {
        let dead: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pgwq.jobs WHERE status='dead'")
                .fetch_one(&pool)
                .await
                .unwrap();
        if dead == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let dead: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pgwq.jobs WHERE status='dead'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dead, 1, "pgwq job should be dead after audit-missing Abort");

    let _ = handle.shutdown(Duration::from_secs(2)).await;
}
