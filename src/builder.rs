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
    pub(crate) strict_handler_lookup: bool,
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
            strict_handler_lookup: false,
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

    /// When `true`, missing handler IDs in the registry cause an error at dispatch time.
    #[must_use]
    pub const fn strict_handler_lookup(mut self, strict: bool) -> Self {
        self.cfg.strict_handler_lookup = strict;
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
        let min_handler_timeout = crate::runtime::HANDLER_CLEANUP_BUDGET * 2;
        if self.cfg.handler_timeout <= min_handler_timeout {
            return Err(BuildError::ConfigInvalid(format!(
                "handler_timeout must be > {min_handler_timeout:?} (2× HANDLER_CLEANUP_BUDGET) \
                 so mark_*_fenced has headroom before pgwq's outer cancellation"
            )));
        }
        Ok(self.cfg)
    }
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
    #[must_use]
    pub fn register_handler<E, H>(mut self, handler_id: impl Into<String>, handler: H) -> Self
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
    /// duplicated within the registry.
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
            by_type
                .entry(entry.event_type)
                .or_default()
                .push(entry.handler_id.clone());
            handlers.insert(
                entry.handler_id,
                RegisteredHandler {
                    handler: entry.handler,
                    handler_timeout: None,
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
