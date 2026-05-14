//! `History` — read-only queries against `outbox.events` and
//! `outbox.handler_deliveries`. Two endpoints, both bounded.

use crate::error::HistoryError;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Read-only view into the outbox event history.
pub struct History<'a> {
    pub(crate) pool: &'a PgPool,
}

/// A persisted event record from `outbox.events`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EventRecord {
    /// The event UUID.
    pub id: Uuid,
    /// The wire-name of the event type.
    pub event_type: String,
    /// The bounded context that produced this event.
    pub producer_bc: String,
    /// The tenant this event belongs to.
    pub tenant_id: String,
    /// Business-aggregate identifier, if the dispatching event opted in via
    /// `DomainEvent::aggregate_key()`. `None` for events without aggregate
    /// scoping (the default).
    pub aggregate_key: Option<String>,
    /// Raw JSON-encoded payload bytes.
    pub payload: Vec<u8>,
    /// Optional headers stored with the event.
    pub headers: serde_json::Map<String, serde_json::Value>,
    /// When the event was created.
    pub created_at: DateTime<Utc>,
}

/// A persisted handler delivery record from `outbox.handler_deliveries`.
#[derive(Debug, Clone)]
pub struct HandlerDeliveryRecord {
    /// The delivery row PK.
    pub id: i64,
    /// The event this delivery is for.
    pub event_id: Uuid,
    /// The handler identifier.
    pub handler_id: String,
    /// Current delivery status.
    pub status: DeliveryStatus,
    /// Number of delivery attempts so far.
    pub attempts: u32,
    /// The last error message, if any.
    pub last_error: Option<String>,
    /// When the first delivery attempt started.
    pub first_attempted_at: Option<DateTime<Utc>>,
    /// When the most recent delivery attempt started.
    pub last_attempted_at: Option<DateTime<Utc>>,
    /// When the delivery finished (success or permanent failure).
    pub finished_at: Option<DateTime<Utc>>,
}

/// One row returned by [`History::stuck_unregistered_handlers`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StuckHandlerRow {
    /// Event awaiting a handler.
    pub event_id: Uuid,
    /// Handler ID that no replica recognized.
    pub handler_id: String,
    /// Number of times a worker has claimed-and-retried this row in loose
    /// mode (incremented per loose miss; never reset).
    pub resolve_attempts: u32,
    /// Most recent timestamp at which a worker bumped `resolve_attempts`.
    /// `None` only on legacy rows with a default of 0 attempts.
    pub last_resolve_attempt_at: Option<DateTime<Utc>>,
}

/// Delivery status of a handler delivery row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "outbox.delivery_status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryStatus {
    /// Waiting to be picked up by the worker.
    Queued,
    /// Currently being processed.
    Running,
    /// Waiting for a retry after a transient failure.
    AwaitingRetry,
    /// Successfully delivered.
    Sent,
    /// Skipped (no handler registered at runtime).
    Skipped,
    /// Permanently failed — max attempts exhausted.
    Dead,
}

impl DeliveryStatus {
    /// Wire-name as stored in the `outbox.delivery_status` Postgres enum.
    /// Snake-case, matching the `sqlx(rename_all = "snake_case")` codec.
    ///
    /// The crate's SQL literals (`status='running'`, `status IN ('sent','dead','skipped')`)
    /// are still hand-written for readability, but `tests/schema_invariants.rs`
    /// pins them by round-tripping every variant through `as_str()` and the
    /// Postgres `pg_enum.enumlabel` of `outbox.delivery_status` — adding,
    /// renaming, or removing a variant on either side fails the build.
    ///
    /// Variants in order: `queued`, `running`, `awaiting_retry`, `sent`,
    /// `skipped`, `dead`.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::AwaitingRetry => "awaiting_retry",
            Self::Sent => "sent",
            Self::Skipped => "skipped",
            Self::Dead => "dead",
        }
    }

    /// All variants in declaration order. Used by the schema-round-trip test
    /// to enumerate without resorting to a third-party enum-iter crate.
    pub const ALL: &'static [Self] = &[
        Self::Queued,
        Self::Running,
        Self::AwaitingRetry,
        Self::Sent,
        Self::Skipped,
        Self::Dead,
    ];
}

impl History<'_> {
    /// Fetch a single event by ID, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on database errors.
    #[tracing::instrument(skip(self), target = "rust_events.history")]
    pub async fn event(&self, event_id: Uuid) -> Result<Option<EventRecord>, HistoryError> {
        type EventRow = (
            Uuid,
            String,
            String,
            String,
            Option<String>,
            Vec<u8>,
            serde_json::Value,
            DateTime<Utc>,
        );
        let row: Option<EventRow> = sqlx::query_as(
            "SELECT id, event_type, producer_bc, tenant_id, aggregate_key,
                        payload, headers, created_at
                 FROM outbox.events
                 WHERE id = $1",
        )
        .bind(event_id)
        .fetch_optional(self.pool)
        .await?;

        Ok(row.map(
            |(
                id,
                event_type,
                producer_bc,
                tenant_id,
                aggregate_key,
                payload,
                headers_val,
                created_at,
            )| {
                let headers = match headers_val {
                    serde_json::Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                };
                EventRecord {
                    id,
                    event_type,
                    producer_bc,
                    tenant_id,
                    aggregate_key,
                    payload,
                    headers,
                    created_at,
                }
            },
        ))
    }

    /// Surface deliveries stuck in loose-mode handler-lookup retries.
    ///
    /// Returns one row per `(event_id, handler_id)` whose
    /// `resolve_attempts >= threshold` and whose status is still `'queued'`
    /// (i.e. no worker has yet found a registered handler and started the
    /// row). Operators can poll this on a schedule as a single-query
    /// diagnostic for "this handler was never deployed."
    ///
    /// Ordered by `last_resolve_attempt_at DESC` so the most recent
    /// flapping rows come first.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on database errors.
    #[tracing::instrument(skip(self), target = "rust_events.history")]
    pub async fn stuck_unregistered_handlers(
        &self,
        threshold: u32,
    ) -> Result<Vec<StuckHandlerRow>, HistoryError> {
        type StuckRow = (Uuid, String, i32, Option<DateTime<Utc>>);
        let threshold_i32 = i32::try_from(threshold).unwrap_or(i32::MAX);
        let rows: Vec<StuckRow> = sqlx::query_as(
            "SELECT event_id, handler_id, resolve_attempts, last_resolve_attempt_at
             FROM outbox.handler_deliveries
             WHERE status = 'queued'
               AND resolve_attempts >= $1
             ORDER BY last_resolve_attempt_at DESC NULLS LAST",
        )
        .bind(threshold_i32)
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(event_id, handler_id, ra, last)| StuckHandlerRow {
                event_id,
                handler_id,
                resolve_attempts: u32::try_from(ra).unwrap_or(0),
                last_resolve_attempt_at: last,
            })
            .collect())
    }

    /// Fetch all handler deliveries for an event, ordered by `handler_id` ASC.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError`] on database errors.
    #[tracing::instrument(skip(self), target = "rust_events.history")]
    pub async fn handler_deliveries_for(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<HandlerDeliveryRecord>, HistoryError> {
        type DeliveryRow = (
            i64,
            Uuid,
            String,
            DeliveryStatus,
            i32,
            Option<String>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
        );
        let rows: Vec<DeliveryRow> = sqlx::query_as(
            "SELECT id, event_id, handler_id, status, attempts, last_error,
                    first_attempted_at, last_attempted_at, finished_at
             FROM outbox.handler_deliveries
             WHERE event_id = $1
             ORDER BY handler_id ASC",
        )
        .bind(event_id)
        .fetch_all(self.pool)
        .await?;

        let records = rows
            .into_iter()
            .map(
                |(
                    id,
                    ev_id,
                    handler_id,
                    status,
                    attempts_i32,
                    last_error,
                    first_attempted_at,
                    last_attempted_at,
                    finished_at,
                )| {
                    HandlerDeliveryRecord {
                        id,
                        event_id: ev_id,
                        handler_id,
                        status,
                        attempts: u32::try_from(attempts_i32).unwrap_or(0),
                        last_error,
                        first_attempted_at,
                        last_attempted_at,
                        finished_at,
                    }
                },
            )
            .collect();

        Ok(records)
    }
}
