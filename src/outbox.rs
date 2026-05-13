//! `Outbox` runtime — public entry point for emitting events in user tx.

/// `pg_work_queue` queue name for handler delivery jobs.
const PGWQ_QUEUE: &str = "outbox_handler_deliveries";

use crate::builder::OutboxConfig;
use crate::dispatch_context::DispatchContext;
use crate::envelope::HandlerEnvelope;
use crate::error::{DispatchError, StartError};
use crate::handle::OutboxHandle;
use crate::handler::DomainEvent;
use crate::limits;
use crate::outcome::DispatchOutcome;
use crate::registry::Registry;
use crate::runtime::OutboxRuntime;
use sqlx::{PgConnection, PgPool};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

/// The transactional outbox runtime. Created via [`crate::builder::OutboxBuilder`].
pub struct Outbox {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
    pub(crate) allow_no_handlers: bool,
    pub(crate) started: AtomicBool,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox")
            .field("allow_no_handlers", &self.allow_no_handlers)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl Outbox {
    pub(crate) const fn new(
        pool: PgPool,
        config: OutboxConfig,
        registry: Arc<Registry>,
        allow_no_handlers: bool,
    ) -> Self {
        Self {
            pool,
            config,
            registry,
            allow_no_handlers,
            started: AtomicBool::new(false),
        }
    }

    /// Dispatch `event` within the caller-owned transaction `tx`.
    ///
    /// Validates inputs, optionally enforces idempotency, persists the event
    /// to `outbox.events`, fans out to `outbox.handler_deliveries`, and
    /// enqueues one `pg_work_queue` job per handler — all in `tx`.
    ///
    /// # Transaction discipline
    ///
    /// On `Err`, the caller MUST roll back `tx`. Committing despite an `Err`
    /// return MAY leak `outbox.handler_deliveries` rows in `queued` state
    /// without corresponding `pg_work_queue` jobs — they will never be
    /// delivered. The idiomatic Rust pattern is `outbox.dispatch(...).await?;`
    /// inside a function whose `Result` exit drops `tx` without commit, which
    /// rolls back automatically.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError`] on validation failures, DB errors, or when no
    /// handlers are registered for `E::EVENT_TYPE` (strict mode).
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        skip(self, tx, event),
        target = "rust_events.dispatch",
        fields(
            event_type = E::EVENT_TYPE,
            tenant_id = ctx.tenant_id(),
            producer_bc = ctx.producer_bc(),
            idempotency_key_set = ctx.idempotency_key().is_some(),
        )
    )]
    pub async fn dispatch<E: DomainEvent>(
        &self,
        tx: &mut PgConnection,
        ctx: &DispatchContext<'_>,
        event: &E,
    ) -> Result<DispatchOutcome, DispatchError> {
        // 1. Validate inputs (early, no I/O).
        if E::EVENT_TYPE.is_empty() || E::EVENT_TYPE.len() > limits::MAX_EVENT_TYPE_BYTES {
            return Err(DispatchError::EventTypeInvalid {
                len: E::EVENT_TYPE.len(),
                max: limits::MAX_EVENT_TYPE_BYTES,
            });
        }
        if ctx.tenant_id().len() > limits::MAX_TENANT_BYTES {
            return Err(DispatchError::TenantIdTooLong {
                len: ctx.tenant_id().len(),
                max: limits::MAX_TENANT_BYTES,
            });
        }
        if ctx.producer_bc().len() > limits::MAX_BC_BYTES {
            return Err(DispatchError::ProducerBcTooLong {
                len: ctx.producer_bc().len(),
                max: limits::MAX_BC_BYTES,
            });
        }
        if let Some(k) = ctx.idempotency_key()
            && (k.is_empty() || k.len() > limits::MAX_IDEMPOTENCY_KEY_BYTES)
        {
            return Err(DispatchError::IdempotencyKeyInvalid {
                len: k.len(),
                max: limits::MAX_IDEMPOTENCY_KEY_BYTES,
            });
        }

        // 2. Encode payload + payload-size check.
        let payload = serde_json::to_vec(event).map_err(DispatchError::Codec)?;
        if payload.len() > limits::MAX_PAYLOAD_BYTES {
            return Err(DispatchError::PayloadTooLarge {
                size: payload.len(),
                max: limits::MAX_PAYLOAD_BYTES,
            });
        }

        // 3. Generate event_id client-side (Type B1 + DEFERRABLE FK pattern).
        let event_id = Uuid::now_v7();

        // 4. Handler lookup BEFORE any DB write — strict mode fails fast.
        let handler_ids = self.registry.handler_ids_for(E::EVENT_TYPE);
        if handler_ids.is_empty() && !self.allow_no_handlers {
            return Err(DispatchError::NoHandlersRegistered {
                event_type: E::EVENT_TYPE,
            });
        }

        // 5. Idempotency reservation (atomic; DEFERRABLE FK lets us write keys
        //    before events).
        if let Some(key) = ctx.idempotency_key() {
            let inserted: Option<(Uuid,)> = sqlx::query_as(
                "INSERT INTO outbox.dispatch_keys (tenant_id, idempotency_key, event_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT DO NOTHING
                 RETURNING event_id",
            )
            .bind(ctx.tenant_id())
            .bind(key)
            .bind(event_id)
            .fetch_optional(&mut *tx)
            .await?;

            if inserted.is_none() {
                let (existing,): (Uuid,) = sqlx::query_as(
                    "SELECT event_id FROM outbox.dispatch_keys
                     WHERE tenant_id = $1 AND idempotency_key = $2",
                )
                .bind(ctx.tenant_id())
                .bind(key)
                .fetch_one(&mut *tx)
                .await?;
                tracing::info!(
                    target: "rust_events.dispatch.dup",
                    event_id = %existing,
                    "duplicate dispatch returned existing event_id"
                );
                return Ok(DispatchOutcome::Duplicate { event_id: existing });
            }
        }

        // 6. INSERT outbox.events. Serialize headers once and pre-check size
        //    to surface a typed error rather than an opaque DB CHECK violation.
        let headers_json = serde_json::Value::Object(
            ctx.headers().cloned().unwrap_or_default(),
        );
        let headers_text = serde_json::to_string(&headers_json)
            .map_err(DispatchError::Codec)?;
        if headers_text.len() > limits::MAX_HEADERS_BYTES {
            return Err(DispatchError::HeadersTooLarge {
                size: headers_text.len(),
                max: limits::MAX_HEADERS_BYTES,
            });
        }
        sqlx::query(
            "INSERT INTO outbox.events
                (id, event_type, producer_bc, tenant_id, payload, headers)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(event_id)
        .bind(E::EVENT_TYPE)
        .bind(ctx.producer_bc())
        .bind(ctx.tenant_id())
        .bind(&payload)
        .bind(headers_json)
        .execute(&mut *tx)
        .await?;

        // 7. No handlers + allow_no_handlers: persist event only.
        if handler_ids.is_empty() {
            tracing::info!(
                target: "rust_events.dispatch.empty",
                event_id = %event_id,
                event_type = E::EVENT_TYPE,
                "event persisted with no handlers (allow_no_handlers=true)"
            );
            return Ok(DispatchOutcome::NoHandlers { event_id });
        }

        // 8. Multi-row INSERT handler_deliveries.
        let handler_id_array: Vec<&str> =
            handler_ids.iter().map(String::as_str).collect();
        sqlx::query(
            "INSERT INTO outbox.handler_deliveries (event_id, handler_id)
             SELECT $1, unnest($2::text[])",
        )
        .bind(event_id)
        .bind(&handler_id_array)
        .execute(&mut *tx)
        .await?;

        // 9. Push N jobs to pg_work_queue.
        let envelopes: Vec<HandlerEnvelope> = handler_ids
            .iter()
            .map(|hid| HandlerEnvelope {
                event_id,
                handler_id: hid.clone(),
            })
            .collect();
        pg_work_queue::Pusher::new(PGWQ_QUEUE)
            .push_batch(&mut *tx, &envelopes)
            .await?;

        tracing::debug!(
            target: "rust_events.dispatch",
            event_id = %event_id,
            deliveries = handler_ids.len(),
            "dispatched"
        );
        Ok(DispatchOutcome::Dispatched {
            event_id,
            deliveries: handler_ids.len(),
        })
    }
}

impl Outbox {
    /// Return a [`crate::history::History`] accessor bound to this outbox's pool.
    #[must_use]
    pub const fn history(&self) -> crate::history::History<'_> {
        crate::history::History { pool: &self.pool }
    }
}

impl Outbox {
    /// First call starts the worker; subsequent calls return
    /// [`StartError::AlreadyStarted`].
    ///
    /// # Retry semantics
    ///
    /// If `start()` returns an `Err` (e.g. a transient DB outage during
    /// `pg_work_queue::Worker::build` or `start`), the internal `started`
    /// flag is released via an RAII guard. Callers — including supervising
    /// restart loops — may call `start()` again to retry.
    ///
    /// # Intended usage
    ///
    /// `Outbox` is designed for build-once, start-once-per-process semantics.
    /// Running multiple `Outbox` instances against the same database (e.g.,
    /// across replicas) IS supported — `pg_work_queue`'s
    /// `FOR UPDATE SKIP LOCKED` claim and fencing tokens make concurrent
    /// workers safe.
    ///
    /// # Errors
    ///
    /// Returns [`StartError`] if `pg_work_queue`'s Worker build/start fails,
    /// or [`StartError::AlreadyStarted`] when a previous `start()` succeeded.
    pub async fn start(&self) -> Result<OutboxHandle, StartError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(StartError::AlreadyStarted);
        }

        // RAII: if any `?` below unwinds the stack, drop releases `started`
        // so the caller (or a supervising restart loop) can retry. On
        // success we `disarm()` and `started` stays `true` for the
        // process lifetime.
        let mut guard = StartedGuard::new(&self.started);

        // Schema probe: no-op SELECT against outbox.events. If migrator was
        // not run, SQLSTATE 42P01 (undefined_table) or 3F000 (invalid_schema)
        // surfaces as StartError::SchemaMissing — caller fails fast instead
        // of watching the worker retry-loop forever. Other probe errors fall
        // through to pgwq's build/start path, which has its own diagnostics
        // (connection failures, missing pgwq.jobs, etc.).
        if let Err(e) = sqlx::query("SELECT 1 FROM outbox.events LIMIT 0")
            .execute(&self.pool)
            .await
            && let sqlx::Error::Database(db) = &e
            && matches!(db.code().as_deref(), Some("42P01" | "3F000"))
        {
            return Err(StartError::SchemaMissing(e));
        }
        // Non-42P01/3F000 probe failures fall through: pgwq's own Worker
        // build/start path will surface the same condition with its richer
        // error variants (connection failures, missing pgwq.jobs, etc.).

        let runtime = Arc::new(OutboxRuntime {
            pool: self.pool.clone(),
            config: self.config.clone(),
            registry: self.registry.clone(),
        });

        let runtime_for_handler = runtime.clone();
        let inner = pg_work_queue::Worker::<HandlerEnvelope>::builder()
            .pool(self.pool.clone())
            .queue(PGWQ_QUEUE)
            .poll_interval(self.config.poll_interval)
            .concurrency(usize::try_from(self.config.concurrency).unwrap_or(usize::MAX))
            .max_attempts(self.config.max_attempts)
            .lease_timeout(self.config.lease_timeout)
            .handler_timeout(self.config.handler_timeout)
            .retry_backoff(self.config.retry_backoff)
            .panic_policy(self.config.panic_policy)
            .handler(move |env: HandlerEnvelope, ctx: pg_work_queue::JobContext| {
                let runtime = runtime_for_handler.clone();
                async move { runtime.handle_envelope(env, ctx).await }
            })
            .build()?
            .start()
            .await?;

        guard.disarm();
        Ok(OutboxHandle::new(inner, self.pool.clone()))
    }
}

/// RAII guard that releases [`Outbox::started`] to `false` on Drop unless
/// explicitly disarmed before the success path. Used in [`Outbox::start`] to
/// keep the flag honest under fallible Worker build/start.
struct StartedGuard<'a> {
    flag: &'a AtomicBool,
    armed: bool,
}

impl<'a> StartedGuard<'a> {
    const fn new(flag: &'a AtomicBool) -> Self {
        Self { flag, armed: true }
    }
    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartedGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(false, Ordering::SeqCst);
        }
    }
}
