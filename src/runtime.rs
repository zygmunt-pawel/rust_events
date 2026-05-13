//! Worker runtime — fenced audit transitions and the `handle_envelope`
//! wrapper invoked by `pg_work_queue::Worker`.

#![allow(clippy::redundant_pub_crate)]

use crate::builder::OutboxConfig;
use crate::registry::Registry;
use crate::util::truncate_utf8;
use crate::limits;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

#[allow(dead_code)] // OutboxRuntime constructed in Phase 8 (Outbox::start)
pub(crate) struct OutboxRuntime {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
}

impl OutboxRuntime {
    /// Transition delivery to `sent` IFF `status='running'` AND `lease_token` matches.
    /// `rows_affected=0` → fenced out (stale worker); we emit warn tracing + Ok.
    #[allow(dead_code)] // called in Phase 8 via handle_envelope
    pub(crate) async fn mark_sent_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='sent', finished_at=now(), lease_token=NULL, last_error=NULL
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_sent: {e}")))?;
        log_fenced_out("sent", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `awaiting_retry` IFF `status='running'` AND `lease_token` matches.
    #[allow(dead_code)] // called in Phase 8 via handle_envelope
    pub(crate) async fn mark_awaiting_retry_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='awaiting_retry', lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_retry: {e}")))?;
        log_fenced_out("awaiting_retry", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `dead` IFF `status='running'` AND `lease_token` matches.
    #[allow(dead_code)] // called in Phase 8 via handle_envelope
    pub(crate) async fn mark_dead_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='dead', finished_at=now(), lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_dead: {e}")))?;
        log_fenced_out("dead", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `skipped` IFF `status='running'` AND `lease_token` matches.
    #[allow(dead_code)] // called in Phase 8 via handle_envelope
    pub(crate) async fn mark_skipped_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='skipped', finished_at=now(), lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_skipped: {e}")))?;
        log_fenced_out("skipped", event_id, handler_id, res.rows_affected());
        Ok(())
    }
}

/// Emit a warn-level tracing event when a fenced UPDATE affects 0 rows.
///
/// Called by all `mark_*_fenced` methods. `rows_affected=0` indicates this
/// worker's claim is stale (another worker already applied a terminal
/// transition), which is expected under concurrent claims and is NOT an error.
#[allow(dead_code)] // called in Phase 8 via mark_*_fenced methods
fn log_fenced_out(
    new_status: &str,
    event_id: Uuid,
    handler_id: &str,
    rows_affected: u64,
) {
    if rows_affected == 0 {
        tracing::warn!(
            target: "rust_events.audit.fenced_out",
            event_id = %event_id,
            handler_id = %handler_id,
            attempted_status = %new_status,
            "mark_* fenced out (stale claim or concurrent terminal verdict)"
        );
    }
}
