//! Public error types. All `thiserror::Error`, `#[non_exhaustive]`,
//! `Send + Sync + 'static`. See spec §9.

use crate::util::is_sqlx_deterministic;

/// Errors returned by `OutboxBuilder::build()` for invalid configuration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// Handler ID must be non-empty.
    #[error("handler_id must be non-empty")]
    HandlerIdEmpty,

    /// Handler ID exceeds the maximum allowed byte length.
    #[error("handler_id length {len} bytes exceeds max {max}")]
    HandlerIdTooLong {
        /// Actual byte length of the supplied handler ID.
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// A handler with this ID is already registered for the given event type.
    #[error("handler_id '{handler_id}' already registered for event_type '{event_type}'")]
    DuplicateHandlerId {
        /// The event type wire-name for which the duplicate was detected.
        event_type: &'static str,
        /// The conflicting handler identifier.
        handler_id: String,
    },

    /// Generic configuration validation failure.
    #[error("config invalid: {0}")]
    ConfigInvalid(String),
}

/// Errors returned by `Outbox::dispatch()`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DispatchError {
    /// `tenant_id` byte length exceeds the allowed maximum.
    #[error("tenant_id length {len} bytes exceeds max {max}")]
    TenantIdTooLong {
        /// Actual byte length.
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// `producer_bc` byte length exceeds the allowed maximum.
    #[error("producer_bc length {len} bytes exceeds max {max}")]
    ProducerBcTooLong {
        /// Actual byte length.
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// `E::EVENT_TYPE` byte length is 0 or exceeds the allowed maximum.
    /// Pre-emptive Rust-side check; DB CHECK on `outbox.events` would
    /// otherwise surface this as an opaque `DispatchError::Constraint`.
    #[error("event_type length {len} bytes not in 1..={max}")]
    EventTypeInvalid {
        /// Actual byte length (0 means empty).
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// `idempotency_key` byte length is 0 or exceeds the allowed maximum.
    #[error("idempotency_key length {len} bytes not in 1..={max}")]
    IdempotencyKeyInvalid {
        /// Actual byte length (0 means empty).
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// `DomainEvent::aggregate_key()` returned `Some("")` or a value longer
    /// than the allowed maximum. `None` is always valid (means "no aggregate
    /// scope").
    #[error("aggregate_key length {len} bytes not in 1..={max}")]
    AggregateKeyInvalid {
        /// Actual byte length (0 means empty).
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// Encoded event payload exceeds the maximum allowed size.
    #[error("encoded payload size {size} exceeds max {max}")]
    PayloadTooLarge {
        /// Actual encoded byte size.
        size: usize,
        /// Maximum allowed byte size.
        max: usize,
    },

    /// Serialized headers JSON exceeds the maximum allowed size.
    #[error("encoded headers size {size} exceeds max {max}")]
    HeadersTooLarge {
        /// Actual encoded byte size of the serialized headers JSON.
        size: usize,
        /// Maximum allowed byte size.
        max: usize,
    },

    /// No handlers are registered for the event type.
    ///
    /// Only raised when `OutboxBuilder::allow_no_handlers(false)` (the default).
    /// When `allow_no_handlers(true)` the dispatch returns
    /// `DispatchOutcome::NoHandlers` instead.
    #[error("no handlers registered for event_type '{event_type}'")]
    NoHandlersRegistered {
        /// The wire-name of the unhandled event type.
        event_type: &'static str,
    },

    /// JSON serialization of the event payload failed.
    #[error("codec error encoding event")]
    Codec(#[source] serde_json::Error),

    /// `pg_work_queue` push failed.
    #[error("pg_work_queue push failed")]
    PgwqPush(#[from] pg_work_queue::PushError),

    /// Deterministic database error — SQLSTATE classes `22`/`23`/`28`/`42`
    /// or a deterministic `sqlx::Error` variant. Retry is unlikely to help.
    ///
    /// # Privacy
    ///
    /// The underlying `sqlx::Error` is exposed via `source()`. Its `Debug`
    /// representation MAY include the Postgres `DETAIL` line, which echoes
    /// values of `idempotency_key` / `tenant_id` from unique-constraint
    /// violations. When logging this error, prefer `Display` (`"{e}"`) over
    /// `Debug` (`"{e:?}"` or `tracing::error!(?e)`).
    #[error("database constraint violation during dispatch")]
    Constraint(#[source] sqlx::Error),

    /// Transient database error (pool starvation, deadlock, connection drop).
    #[error("transient database error during dispatch")]
    Transient(#[source] sqlx::Error),
}

impl DispatchError {
    /// Heuristic: is retrying this dispatch likely to succeed?
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        match self {
            Self::Transient(_) => true,
            Self::PgwqPush(e) => e.is_retriable(),
            _ => false,
        }
    }
}

impl From<sqlx::Error> for DispatchError {
    fn from(e: sqlx::Error) -> Self {
        if is_sqlx_deterministic(&e) {
            Self::Constraint(e)
        } else {
            Self::Transient(e)
        }
    }
}

/// Errors returned by history query operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HistoryError {
    /// Any database error raised during a history query.
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

/// Errors returned by purge operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PurgeError {
    /// Any database error raised during a purge operation.
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

/// Errors returned by `Outbox::start()`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    /// `start()` was called a second time on an already-started `Outbox`.
    #[error("outbox already started; second start() rejected")]
    AlreadyStarted,

    /// The `outbox.events` table is missing. Almost always means
    /// `rust_events::migrator()` was not run on this database. Loud-fail at
    /// startup rather than letting the worker swallow `42P01` / `3F000` as a
    /// retriable error forever.
    #[error("outbox schema missing — did you run rust_events::migrator()?")]
    SchemaMissing(#[source] sqlx::Error),

    /// The supplied `PgPool` is too small for the configured `concurrency`.
    ///
    /// `pg_work_queue`'s claim path and the crate's `mark_*_fenced` audit
    /// write each need a connection per in-flight job, plus two for poll and
    /// reaper — so `max_connections` must be at least `concurrency × 2 + 2`.
    /// An undersized pool can starve `mark_*_fenced`: if the audit UPDATE
    /// cannot acquire a connection before `pg_work_queue`'s outer
    /// `handler_timeout` cancels the wrapper, a delivery can be stranded at
    /// `status='running'`. Size the pool above this floor, plus headroom for
    /// any `dispatch`/`History`/`purge_*` traffic sharing it — see the README
    /// "Connection-pool sizing" limitation.
    #[error(
        "connection pool too small: max_connections={max_connections}, but \
         concurrency={concurrency} requires at least {required} \
         (concurrency × 2 + 2)"
    )]
    PoolTooSmall {
        /// `max_connections` configured on the supplied `PgPool`.
        max_connections: u32,
        /// The configured worker `concurrency`.
        concurrency: u32,
        /// Minimum `max_connections` required: `concurrency × 2 + 2`.
        required: u64,
    },

    /// Building the underlying `pg_work_queue` worker failed.
    #[error("pg_work_queue worker build failed")]
    PgwqBuild(#[from] pg_work_queue::BuildError),

    /// Starting the underlying `pg_work_queue` worker failed.
    #[error("pg_work_queue worker start failed")]
    PgwqStart(#[from] pg_work_queue::StartError),
}

/// Errors returned by `OutboxHandle::shutdown()`.
///
/// The pending-count query is best-effort and never produces a variant here —
/// see [`crate::handle::OutboxHandle::shutdown`] for the degradation behavior.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShutdownError {
    /// The underlying `pg_work_queue` worker shutdown failed.
    #[error("pg_work_queue worker shutdown failed")]
    Pgwq(#[from] pg_work_queue::ShutdownError),
}
