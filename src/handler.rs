//! Public traits and types for domain events and handlers.

use chrono::{DateTime, Utc};
use std::borrow::Cow;
use std::time::Duration;
use uuid::Uuid;

/// A user-defined domain event type.
///
/// `EVENT_TYPE` is the **stable wire-name** stored in `outbox.events.event_type`
/// and used as the dispatch key in the in-memory registry. Independent of
/// Rust path: renaming a module or restructuring crates will not break
/// dispatch as long as `EVENT_TYPE` is preserved.
pub trait DomainEvent:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// Stable wire-name. Convention: `"<bc>.<event_name>"`,
    /// e.g. `"shop.order_created"`.
    const EVENT_TYPE: &'static str;

    /// Optional business-aggregate identifier for this event.
    ///
    /// Returned value is stored in `outbox.events.aggregate_key` and indexed
    /// (partial, `WHERE aggregate_key IS NOT NULL`) under
    /// `(tenant_id, aggregate_key, created_at DESC)`. Use for "all events
    /// for order/customer/account X" lookups.
    ///
    /// Default returns `None` — zero overhead for callers that don't need
    /// aggregate scoping (the partial index stays empty).
    ///
    /// Must be ≤ [`crate::limits::MAX_AGGREGATE_KEY_BYTES`] bytes (128).
    /// Empty string is rejected at dispatch time.
    fn aggregate_key(&self) -> Option<Cow<'_, str>> {
        None
    }
}

/// Handler outcome. Mirror of `pg_work_queue::JobError` plus `Skip` for the
/// "this event doesn't apply to me" pattern (filtered tenant, opted-out user).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerError {
    /// Transient failure; `pg_work_queue` will retry with backoff.
    #[error("retry: {reason}")]
    Retry {
        /// Operator-visible reason string (no PII).
        reason: String,
        /// Optional override of the per-worker default retry delay.
        retry_in: Option<Duration>,
    },
    /// Terminal: this delivery does not apply (filtered tenant, wrong env, ...).
    /// Audit shows `status='skipped'`, distinct from `'sent'` (success) and
    /// `'dead'` (failure).
    #[error("skip: {reason}")]
    Skip {
        /// Operator-visible reason string (no PII).
        reason: String,
    },
    /// Permanent failure; bypass retry budget, mark dead immediately.
    #[error("abort: {reason}")]
    Abort {
        /// Operator-visible reason string (no PII).
        reason: String,
    },
}

impl HandlerError {
    /// Construct a `Retry` with default backoff (decided by Worker's policy).
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: None,
        }
    }

    /// Construct a `Retry` with explicit delay.
    pub fn retry_in(reason: impl Into<String>, retry_in: Duration) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: Some(retry_in),
        }
    }

    /// Construct a `Retry` whose delay is the wall-clock distance from now
    /// to `when`. A timestamp in the past (or exactly now) becomes
    /// `Duration::ZERO` — retry immediately rather than panic on negative
    /// conversion.
    ///
    /// Useful when the handler knows when retrying *will* succeed (e.g. a
    /// `Retry-After` header from a 429, a scheduled-window restart, a
    /// rate-limit reset).
    pub fn retry_at(reason: impl Into<String>, when: DateTime<Utc>) -> Self {
        let delay = (when - Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        Self::Retry {
            reason: reason.into(),
            retry_in: Some(delay),
        }
    }

    /// Construct a `Skip` variant.
    pub fn skip(reason: impl Into<String>) -> Self {
        Self::Skip {
            reason: reason.into(),
        }
    }

    /// Construct an `Abort` variant.
    pub fn abort(reason: impl Into<String>) -> Self {
        Self::Abort {
            reason: reason.into(),
        }
    }
}

/// Read-only context passed to every handler invocation.
///
/// `delivery_key` is the per-handler UUID assigned by `pg_work_queue` at
/// `Pusher::push_batch` time. **Stable across retries** of THIS delivery
/// (so external APIs using `Idempotency-Key` headers can dedupe retries).
/// **NOT** the same as the dispatch-time idempotency key passed by the
/// caller — for that see `dispatch_idempotency_key`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HandlerContext {
    /// The event's unique identifier.
    pub event_id: Uuid,
    /// The tenant this event belongs to.
    pub tenant_id: String,
    /// The bounded context that produced this event.
    pub producer_bc: String,
    /// 1-indexed current attempt; from `pg_work_queue::JobContext::attempt`.
    pub attempt: u32,
    /// Per-row stamped `max_attempts`; from `pg_work_queue::JobContext::max_attempts`.
    pub max_attempts: u32,
    /// Per-delivery UUID, stable across retries. Use as `Idempotency-Key`.
    pub delivery_key: Uuid,
    /// User-supplied dispatch-level idempotency key (if any). The same string
    /// the caller passed to `DispatchContext::with_idempotency_key`. Useful
    /// when external API dedup should align with domain-level deduplication
    /// (the same business action repeated → same external key).
    pub dispatch_idempotency_key: Option<String>,
    /// Arbitrary key-value metadata attached at dispatch time.
    pub headers: serde_json::Map<String, serde_json::Value>,
}

/// User-implementable async handler for a domain event.
///
/// Implementations must be `Send + Sync + 'static` (stored in `Arc<dyn ...>`
/// in the registry). Use `async_trait` macro for dyn-compatibility.
#[async_trait::async_trait]
pub trait EventHandler<E: DomainEvent>: Send + Sync + 'static {
    /// Handle the given event with the provided delivery context.
    async fn handle(
        &self,
        event: &E,
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn retry_at_future_yields_positive_delay() {
        let when = Utc::now() + chrono::Duration::seconds(30);
        let HandlerError::Retry { retry_in, .. } = HandlerError::retry_at("after window", when)
        else {
            panic!("expected Retry");
        };
        let delay = retry_in.expect("retry_at must set retry_in");
        assert!(
            delay >= Duration::from_secs(28) && delay <= Duration::from_secs(31),
            "expected ~30s delay, got {delay:?}"
        );
    }

    #[test]
    fn retry_at_past_yields_zero_delay() {
        let when = Utc::now() - chrono::Duration::seconds(10);
        let HandlerError::Retry { retry_in, .. } = HandlerError::retry_at("late", when)
        else {
            panic!("expected Retry");
        };
        assert_eq!(retry_in, Some(Duration::ZERO));
    }

    #[test]
    fn retry_at_carries_reason() {
        let HandlerError::Retry { reason, .. } =
            HandlerError::retry_at("rate_limited", Utc::now())
        else {
            panic!("expected Retry");
        };
        assert_eq!(reason, "rate_limited");
    }
}
