//! Manual purge functions. Mirror `pg_work_queue::purge` patterns:
//! chunked DELETE via CTE, no background sweeper, `PURGE_CHUNK_SIZE` const.

use crate::error::PurgeError;
use crate::limits::PURGE_CHUNK_SIZE;
use chrono::Utc;
use sqlx::PgPool;
use std::time::Duration;

/// Delete terminal delivery rows (`sent`, `dead`, `skipped`) older than
/// `older_than`. Runs in chunks of `PURGE_CHUNK_SIZE`. Returns total rows
/// deleted.
///
/// # Errors
///
/// Returns [`PurgeError::Sqlx`] if any database operation fails.
#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_terminal_deliveries(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT id FROM outbox.handler_deliveries
                WHERE status IN ('sent','dead','skipped') AND finished_at < $1
                ORDER BY finished_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.handler_deliveries
             WHERE id IN (SELECT id FROM victims)",
        )
        .bind(cutoff)
        .bind(i64::try_from(PURGE_CHUNK_SIZE).unwrap_or(i64::MAX))
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

/// Delete idempotency dispatch-key rows older than `older_than`. Runs in
/// chunks of `PURGE_CHUNK_SIZE`. Returns total rows deleted.
///
/// # Errors
///
/// Returns [`PurgeError::Sqlx`] if any database operation fails.
#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_dispatch_keys(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT tenant_id, idempotency_key FROM outbox.dispatch_keys
                WHERE created_at < $1
                ORDER BY created_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.dispatch_keys d
             USING victims v
             WHERE d.tenant_id = v.tenant_id
               AND d.idempotency_key = v.idempotency_key",
        )
        .bind(cutoff)
        .bind(i64::try_from(PURGE_CHUNK_SIZE).unwrap_or(i64::MAX))
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

/// Safe purge: only deletes events whose ALL deliveries are terminal
/// (sent/dead/skipped). Recommended ordering: call
/// `purge_terminal_deliveries` + `purge_dispatch_keys` BEFORE this so the
/// NOT EXISTS predicate can find candidates.
///
/// # Errors
///
/// Returns [`PurgeError::Sqlx`] if any database operation fails.
#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_events(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT e.id FROM outbox.events e
                WHERE e.created_at < $1
                  AND NOT EXISTS (
                      SELECT 1 FROM outbox.handler_deliveries hd
                      WHERE hd.event_id = e.id
                        AND hd.status NOT IN ('sent','dead','skipped')
                  )
                ORDER BY e.created_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.events WHERE id IN (SELECT id FROM victims)",
        )
        .bind(cutoff)
        .bind(i64::try_from(PURGE_CHUNK_SIZE).unwrap_or(i64::MAX))
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

fn cutoff_from(older_than: Duration) -> chrono::DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::zero())
}
