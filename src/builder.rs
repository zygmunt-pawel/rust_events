//! `OutboxConfig`, `OutboxConfigBuilder`, `OutboxBuilder`. Fail-late validation
//! in `build()` — mirrors `pg_work_queue::WorkerBuilder` conventions.

use crate::error::BuildError;
use crate::handler::{DomainEvent, EventHandler};
use crate::registry::{ErasedHandler, RegisteredHandler, Registry, TypedHandler};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

pub use pg_work_queue::{BackoffPolicy, PanicPolicy};

/// What to do when payload bytes fail to deserialize as `E`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecodeStrategy {
    /// Default. Return `JobError::retry` — gives a window for rollback if a
    /// schema-incompatible event payload was deployed accidentally. After
    /// `max_attempts` retries the job goes dead via `pg_work_queue`'s own
    /// path (and our wrapper marks `handler_deliveries.status='dead'`).
    #[default]
    Retry,
    /// Decode error is a permanent fault → `mark_dead` on first claim.
    /// Use only when payload schema is strictly versioned and decode errors
    /// must surface immediately.
    Abort,
}

/// Per-handler registration options. Every field is optional; an unset field
/// falls back to the corresponding global [`OutboxConfig`] value.
///
/// This is a plain options value-bag, not a validating builder like
/// [`OutboxConfigBuilder`] — it has no `build()` and no cross-field rules
/// (per-handler bounds are checked against the global config at
/// [`OutboxBuilder::build`], which is the only place both values are known).
/// It still follows the crate's `const fn` setter / `#[must_use]` convention.
/// The knobs are [`handler_timeout`](HandlerOptions::handler_timeout) and
/// [`concurrency_limit`](HandlerOptions::concurrency_limit).
///
/// Not `Copy` on purpose: this type is an extension point, and a future
/// non-`Copy` field should not be a breaking change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandlerOptions {
    /// Per-handler `handler_timeout` override; `None` ⇒ use the global value.
    /// Private — only read inside this module (`register_handler`, `build`).
    handler_timeout: Option<Duration>,
    /// Per-handler concurrency cap — at most this many invocations of this
    /// handler run at once. `None` ⇒ unbounded (only the global
    /// `OutboxConfig::concurrency` applies). Private — read in
    /// `register_handler` and `build`.
    concurrency_limit: Option<u32>,
}

impl HandlerOptions {
    /// Options with every field unset — behaves identically to the global config.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            handler_timeout: None,
            concurrency_limit: None,
        }
    }

    /// Override the wall-clock budget for a single invocation of *this*
    /// handler.
    ///
    /// Must be `> 2 × HANDLER_CLEANUP_BUDGET` (i.e. `> 400 ms`) and
    /// `<= OutboxConfig::handler_timeout`: the global timeout is a hard
    /// ceiling because it is the single value `pg_work_queue`'s worker-wide
    /// outer cancellation (and lease math) uses, so a per-handler value may
    /// only *match or tighten* the global budget. Validated at
    /// [`OutboxBuilder::build`]; a violation is [`BuildError::ConfigInvalid`].
    ///
    /// Note the effective handler budget is `d - HANDLER_CLEANUP_BUDGET`
    /// (≈ `d - 200 ms`): the crate reserves the tail of the window for its own
    /// `mark_*_fenced` audit write. A `d` near the 400 ms floor leaves a very
    /// small actual budget.
    ///
    /// Multi-replica: the override is resolved from the registry of whichever
    /// replica claims the job. Keep `HandlerOptions` consistent across replicas
    /// — if the same `handler_id` carries different overrides on different
    /// replicas, delivery stays at-least-once-safe but the effective timeout
    /// for a given attempt is whichever replica won the claim.
    #[must_use]
    pub const fn handler_timeout(mut self, d: Duration) -> Self {
        self.handler_timeout = Some(d);
        self
    }

    /// Cap the number of concurrent invocations of *this* handler.
    ///
    /// At most `n` tasks for this handler run at once, gated at job-claim
    /// time by `pg_work_queue` (a saturated handler's jobs are simply not
    /// claimed — no head-of-line blocking). `None` (the default) leaves the
    /// handler bounded only by the global [`OutboxConfig`] `concurrency`.
    ///
    /// `n` must be `1..=i32::MAX`; `0` is rejected at [`OutboxBuilder::build`]
    /// with [`BuildError::ConfigInvalid`]. There is no cross-knob constraint
    /// with `OutboxConfig::concurrency` — the two are independent axes.
    ///
    /// Single-instance: the cap is enforced by an in-process counter, correct
    /// because the service runs as exactly one worker process.
    #[must_use]
    pub const fn concurrency_limit(mut self, n: u32) -> Self {
        self.concurrency_limit = Some(n);
        self
    }
}

/// Configuration for the `Outbox` runtime. Build with [`OutboxConfig::builder()`].
#[derive(Debug, Clone)]
pub struct OutboxConfig {
    pub(crate) poll_interval: Duration,
    pub(crate) concurrency: u32,
    pub(crate) max_attempts: u32,
    pub(crate) lease_timeout: Duration,
    pub(crate) handler_timeout: Duration,
    pub(crate) retry_backoff: BackoffPolicy,
    pub(crate) panic_policy: PanicPolicy,
    pub(crate) decode_error_strategy: DecodeStrategy,
}

impl OutboxConfig {
    /// Return a builder pre-populated with the library defaults.
    #[must_use]
    pub fn builder() -> OutboxConfigBuilder {
        OutboxConfigBuilder::default()
    }
}

impl Default for OutboxConfig {
    fn default() -> Self {
        // Mirror pg_work_queue's WorkerBuilder defaults.
        let lease_timeout = Duration::from_secs(300);
        Self {
            poll_interval: Duration::from_millis(500),
            concurrency: 16,
            max_attempts: 5,
            lease_timeout,
            handler_timeout: Duration::from_secs(240), // 80% of 300s lease
            retry_backoff: BackoffPolicy::default(),
            panic_policy: PanicPolicy::default(),
            decode_error_strategy: DecodeStrategy::Retry,
        }
    }
}

/// Builder for [`OutboxConfig`].
#[derive(Debug, Default)]
pub struct OutboxConfigBuilder {
    cfg: OutboxConfig,
}

impl OutboxConfigBuilder {
    /// Set the poll interval (how often the worker wakes to claim jobs).
    #[must_use]
    pub const fn poll_interval(mut self, d: Duration) -> Self {
        self.cfg.poll_interval = d;
        self
    }

    /// Set the maximum number of concurrently running handlers.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.cfg.concurrency = n;
        self
    }

    /// Set the default maximum delivery attempts for each handler.
    #[must_use]
    pub const fn max_attempts(mut self, n: u32) -> Self {
        self.cfg.max_attempts = n;
        self
    }

    /// Set the lease timeout (how long a claimed job is owned before reaper reclaims it).
    #[must_use]
    pub const fn lease_timeout(mut self, d: Duration) -> Self {
        self.cfg.lease_timeout = d;
        self
    }

    /// Set the handler timeout (wall-clock budget for a single handler invocation).
    #[must_use]
    pub const fn handler_timeout(mut self, d: Duration) -> Self {
        self.cfg.handler_timeout = d;
        self
    }

    /// Set the retry backoff policy.
    #[must_use]
    pub const fn retry_backoff(mut self, p: BackoffPolicy) -> Self {
        self.cfg.retry_backoff = p;
        self
    }

    /// Set the panic policy (what happens when a handler panics).
    #[must_use]
    pub const fn panic_policy(mut self, p: PanicPolicy) -> Self {
        self.cfg.panic_policy = p;
        self
    }

    /// Set what happens when an event payload fails to deserialize.
    #[must_use]
    pub const fn decode_error_strategy(mut self, s: DecodeStrategy) -> Self {
        self.cfg.decode_error_strategy = s;
        self
    }

    /// Validate configuration and return an [`OutboxConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError::ConfigInvalid`] when:
    /// - `concurrency == 0`
    /// - `max_attempts == 0`
    /// - `handler_timeout >= lease_timeout`
    /// - `handler_timeout <= 2 × HANDLER_CLEANUP_BUDGET` (currently 400 ms) —
    ///   our wrap reserves `HANDLER_CLEANUP_BUDGET` at the tail of
    ///   `handler_timeout` for `mark_*_fenced` to land before pgwq's outer
    ///   cancellation. A `handler_timeout` near that budget would either
    ///   collapse to the 100 ms floor (where pgwq could cancel us mid-mark)
    ///   or leave no headroom at all. Belt-and-braces — `pg_work_queue`
    ///   already enforces a 1 s minimum on its side, this guards against
    ///   pgwq lowering that floor in the future.
    pub fn build(self) -> Result<OutboxConfig, BuildError> {
        if self.cfg.concurrency == 0 {
            return Err(BuildError::ConfigInvalid("concurrency must be >= 1".into()));
        }
        if self.cfg.max_attempts == 0 {
            return Err(BuildError::ConfigInvalid(
                "max_attempts must be >= 1".into(),
            ));
        }
        if self.cfg.handler_timeout >= self.cfg.lease_timeout {
            return Err(BuildError::ConfigInvalid(
                "handler_timeout must be < lease_timeout".into(),
            ));
        }
        handler_timeout_floor_check(self.cfg.handler_timeout, "OutboxConfig")?;
        Ok(self.cfg)
    }
}

/// Shared lower-bound check for any `handler_timeout` (global or per-handler):
/// it must exceed `2 × HANDLER_CLEANUP_BUDGET` so the crate's internal
/// `tokio::time::timeout` never collapses onto its 100 ms floor and always
/// reserves room for the `mark_*_fenced` audit write. `label` identifies the
/// source (`"OutboxConfig"` or a specific handler) in the error message.
fn handler_timeout_floor_check(d: Duration, label: &str) -> Result<(), BuildError> {
    let min = crate::runtime::HANDLER_CLEANUP_BUDGET * 2;
    if d <= min {
        return Err(BuildError::ConfigInvalid(format!(
            "{label}: handler_timeout {d:?} must be > {min:?} \
             (2× HANDLER_CLEANUP_BUDGET) so mark_*_fenced has headroom before \
             pgwq's outer cancellation"
        )));
    }
    Ok(())
}

/// Builder for [`crate::outbox::Outbox`]. Collects pool, config, and handler
/// registrations; validates at `build()` time (fail-late convention from
/// `pg_work_queue`).
pub struct OutboxBuilder {
    pool: PgPool,
    config: Option<OutboxConfig>,
    /// Pending handler entries — validated and folded into `Registry` in `build()`.
    pending: Vec<PendingHandler>,
    allow_no_handlers: bool,
}

struct PendingHandler {
    event_type: &'static str,
    handler_id: String,
    handler: Arc<dyn ErasedHandler>,
    handler_timeout: Option<Duration>,
    concurrency_limit: Option<u32>,
}

impl OutboxBuilder {
    /// Create a builder backed by the given connection pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self {
            pool,
            config: None,
            pending: Vec::new(),
            allow_no_handlers: false,
        }
    }

    /// Override the default [`OutboxConfig`].
    #[must_use]
    pub const fn config(mut self, cfg: OutboxConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Register a handler. Takes ownership of `handler` and wraps it in an
    /// `Arc<TypedHandler<E, H>>` internally — callers must **not** pre-wrap.
    ///
    /// `options` carries per-handler overrides (see [`HandlerOptions`]); pass
    /// `HandlerOptions::new()` for a handler that should use the global
    /// [`OutboxConfig`] verbatim.
    // `options` is taken by value (not `&HandlerOptions`) so the ergonomic
    // call site reads `HandlerOptions::new().handler_timeout(..)`; the type is
    // intentionally not `Copy`, so clippy's needless_pass_by_value fires here.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn register_handler<E, H>(
        mut self,
        handler_id: impl Into<String>,
        handler: H,
        options: HandlerOptions,
    ) -> Self
    where
        E: DomainEvent,
        H: EventHandler<E>,
    {
        let inner = Arc::new(handler);
        let erased: Arc<dyn ErasedHandler> = Arc::new(TypedHandler {
            inner,
            _e: PhantomData::<fn() -> E>,
        });
        self.pending.push(PendingHandler {
            event_type: E::EVENT_TYPE,
            handler_id: handler_id.into(),
            handler: erased,
            handler_timeout: options.handler_timeout,
            concurrency_limit: options.concurrency_limit,
        });
        self
    }

    /// When `true`, `dispatch()` for an event type with no registered handlers
    /// returns `Ok(DispatchOutcome::NoHandlers { event_id })`. When `false`
    /// (default), returns `Err(DispatchError::NoHandlersRegistered)`.
    #[must_use]
    pub const fn allow_no_handlers(mut self, allow: bool) -> Self {
        self.allow_no_handlers = allow;
        self
    }

    /// Validate all registrations and build an [`crate::outbox::Outbox`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] when any handler ID is empty, too long, or
    /// duplicated within the registry, or [`BuildError::ConfigInvalid`] when a
    /// per-handler [`HandlerOptions::handler_timeout`] is out of range
    /// (`<= 2 × HANDLER_CLEANUP_BUDGET` or `> OutboxConfig::handler_timeout`).
    pub fn build(self) -> Result<crate::outbox::Outbox, BuildError> {
        let config = self.config.unwrap_or_default();

        // Validate handler entries; build Registry.
        let mut handlers: HashMap<String, RegisteredHandler> = HashMap::new();
        let mut by_type: HashMap<&'static str, Vec<String>> = HashMap::new();

        for entry in self.pending {
            if entry.handler_id.is_empty() {
                return Err(BuildError::HandlerIdEmpty);
            }
            if entry.handler_id.len() > crate::limits::MAX_HANDLER_ID_BYTES {
                return Err(BuildError::HandlerIdTooLong {
                    len: entry.handler_id.len(),
                    max: crate::limits::MAX_HANDLER_ID_BYTES,
                });
            }
            if handlers.contains_key(&entry.handler_id) {
                return Err(BuildError::DuplicateHandlerId {
                    event_type: entry.event_type,
                    handler_id: entry.handler_id,
                });
            }
            if let Some(ht) = entry.handler_timeout {
                handler_timeout_floor_check(ht, &format!("handler '{}'", entry.handler_id))?;
                if ht > config.handler_timeout {
                    return Err(BuildError::ConfigInvalid(format!(
                        "handler '{}': a per-handler handler_timeout may only \
                         match or tighten the global budget, never exceed it — \
                         {ht:?} is larger than the global OutboxConfig \
                         handler_timeout {:?} (which is the default 240s when \
                         .config(...) was not called). The global value is the \
                         hard ceiling: pg_work_queue's worker-wide outer \
                         cancellation enforces it. Fix: lower this override, or \
                         raise the global handler_timeout.",
                        entry.handler_id, config.handler_timeout
                    )));
                }
            }
            if let Some(limit) = entry.concurrency_limit
                && (limit == 0 || limit > i32::MAX as u32)
            {
                return Err(BuildError::ConfigInvalid(format!(
                    "handler '{}': concurrency_limit must be in 1..=2147483647, \
                     got {limit}",
                    entry.handler_id
                )));
            }
            by_type
                .entry(entry.event_type)
                .or_default()
                .push(entry.handler_id.clone());
            handlers.insert(
                entry.handler_id,
                RegisteredHandler {
                    handler: entry.handler,
                    handler_timeout: entry.handler_timeout,
                    concurrency_limit: entry.concurrency_limit,
                },
            );
        }

        let registry = Arc::new(Registry { handlers, by_type });

        Ok(crate::outbox::Outbox::new(
            self.pool,
            config,
            registry,
            self.allow_no_handlers,
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::HandlerOptions;
    use std::time::Duration;

    #[test]
    fn handler_options_records_timeout_override() {
        let o = HandlerOptions::new().handler_timeout(Duration::from_secs(180));
        assert_eq!(o.handler_timeout, Some(Duration::from_secs(180)));
    }

    #[test]
    fn handler_options_default_has_no_override() {
        assert_eq!(HandlerOptions::default().handler_timeout, None);
        assert_eq!(HandlerOptions::new().handler_timeout, None);
    }

    #[test]
    fn handler_options_last_timeout_wins() {
        let o = HandlerOptions::new()
            .handler_timeout(Duration::from_secs(1))
            .handler_timeout(Duration::from_secs(2));
        assert_eq!(o.handler_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn handler_options_records_concurrency_limit() {
        let o = HandlerOptions::new().concurrency_limit(4);
        assert_eq!(o.concurrency_limit, Some(4));
    }

    #[test]
    fn handler_options_default_has_no_concurrency_limit() {
        assert_eq!(HandlerOptions::default().concurrency_limit, None);
        assert_eq!(HandlerOptions::new().concurrency_limit, None);
    }

    #[test]
    fn handler_options_last_concurrency_limit_wins() {
        let o = HandlerOptions::new()
            .concurrency_limit(1)
            .concurrency_limit(8);
        assert_eq!(o.concurrency_limit, Some(8));
    }
}
