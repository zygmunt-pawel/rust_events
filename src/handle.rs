//! `OutboxHandle` — owned at `Outbox::start()` time. `shutdown()` drains the
//! worker, then SELECTs pending-delivery count for `OutboxStats`.

use crate::error::ShutdownError;
use crate::outcome::OutboxStats;
use sqlx::PgPool;
use std::time::Duration;

pub use pg_work_queue::Stats;

/// Handle returned by [`crate::outbox::Outbox::start`]. Drop to cancel the
/// worker; call [`OutboxHandle::shutdown`] for a graceful drain.
#[derive(Debug)]
pub struct OutboxHandle {
    inner: pg_work_queue::WorkerHandle,
    pool: PgPool,
}

impl OutboxHandle {
    pub(crate) const fn new(inner: pg_work_queue::WorkerHandle, pool: PgPool) -> Self {
        Self { inner, pool }
    }

    /// Graceful drain with a deadline. Returns `pg_work_queue` worker stats
    /// plus a count of still-non-terminal `handler_deliveries` rows.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError`] if the worker fails to drain or the
    /// pending-count query fails.
    pub async fn shutdown(
        self,
        timeout: Duration,
    ) -> Result<(Stats, OutboxStats), ShutdownError> {
        let stats = self.inner.shutdown(timeout).await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries
             WHERE status IN ('queued','running','awaiting_retry')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(ShutdownError::PendingCount)?;
        Ok((
            stats,
            OutboxStats {
                pending_deliveries: u64::try_from(pending).unwrap_or(0),
            },
        ))
    }
}
