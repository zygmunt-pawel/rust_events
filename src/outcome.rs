//! Result types returned by `Outbox::dispatch()` and `OutboxHandle::shutdown()`.

use uuid::Uuid;

/// Result of a successful (non-error) `dispatch()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchOutcome {
    /// Event persisted, N delivery jobs queued.
    Dispatched {
        /// The unique identifier assigned to the persisted event.
        event_id: Uuid,
        /// Number of delivery jobs queued (one per registered handler).
        deliveries: usize,
    },
    /// `idempotency_key` matched an existing `dispatch_keys` row; the original
    /// event is returned. No new event/delivery rows created.
    Duplicate {
        /// The event ID from the original dispatch.
        event_id: Uuid,
    },
    /// Returned ONLY when `OutboxBuilder::allow_no_handlers(true)` is set and
    /// no handlers are registered for `E::EVENT_TYPE`. Event is persisted as
    /// audit-only; no delivery jobs queued. Otherwise this case surfaces as
    /// `DispatchError::NoHandlersRegistered`.
    NoHandlers {
        /// The unique identifier assigned to the persisted event.
        event_id: Uuid,
    },
}

/// Outbox-level stats at `shutdown()` time. Separate query on
/// `outbox.handler_deliveries` for `pending_deliveries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutboxStats {
    /// Count of rows with `status IN ('queued','running','awaiting_retry')`
    /// at shutdown time. May be > 0 if shutdown timeout was reached before
    /// in-flight deliveries terminated.
    pub pending_deliveries: u64,
}
