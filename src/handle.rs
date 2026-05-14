//! `OutboxHandle` — owned at `Outbox::start()` time. `shutdown()` drains the
//! worker, then SELECTs pending-delivery count for `OutboxStats`.

use crate::error::ShutdownError;
use crate::outcome::OutboxStats;
use sqlx::PgPool;
use std::time::Duration;

pub use pg_work_queue::Stats;

/// Handle returned by [`crate::outbox::Outbox::start`]. Drop to cancel the
/// worker; call [`OutboxHandle::shutdown`] for a graceful drain.
///
/// # Drop semantics
///
/// Inherited from `pg_work_queue::WorkerHandle` (v0.1.3+): dropping a handle
/// without calling `shutdown()` triggers the inner Drop, which cancels the
/// shared shutdown token and aborts the poll + reaper tasks. There is no
/// drain and no Stats — for production use prefer the explicit
/// [`OutboxHandle::shutdown`] path. The aborting behavior just prevents the
/// classic tokio footgun where a dropped `JoinHandle` *detaches* and silently
/// leaks the background loops with their pool connections.
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
    /// The pending-count query is capped by whatever budget is left after the
    /// pgwq drain (with a 500 ms floor). On timeout or query error we surface
    /// `pending_deliveries: 0` and emit `rust_events.shutdown.pending_count_*`
    /// tracing — we never lose pgwq Stats just because the count failed.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownError::Pgwq`] only if the underlying worker drain
    /// itself fails. Count-query failures degrade gracefully.
    pub async fn shutdown(self, timeout: Duration) -> Result<(Stats, OutboxStats), ShutdownError> {
        let start = std::time::Instant::now();
        let stats = self.inner.shutdown(timeout).await?;
        let remaining = timeout
            .saturating_sub(start.elapsed())
            .max(Duration::from_millis(500));
        let count_q = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM outbox.handler_deliveries
             WHERE status IN ('queued','running','awaiting_retry')",
        )
        .fetch_one(&self.pool);
        let pending = match tokio::time::timeout(remaining, count_q).await {
            Ok(Ok(n)) => u64::try_from(n).unwrap_or(0),
            Ok(Err(e)) => {
                tracing::warn!(
                    target: "rust_events.shutdown.pending_count_err",
                    error = %crate::util::redact_db_error(&e),
                    "shutdown pending-count query failed; reporting 0"
                );
                0
            }
            Err(_) => {
                tracing::warn!(
                    target: "rust_events.shutdown.pending_count_timeout",
                    remaining_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX),
                    "shutdown pending-count query exceeded budget; reporting 0"
                );
                0
            }
        };
        Ok((
            stats,
            OutboxStats {
                pending_deliveries: pending,
            },
        ))
    }
}
