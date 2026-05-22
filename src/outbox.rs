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
use sqlx::PgPool;
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
    /// `tx` is typed as `&mut sqlx::Transaction<'_, sqlx::Postgres>` rather
    /// than `&mut PgConnection`. The narrower type is what makes
    /// "transactional outbox" actually transactional: with `&mut PgConnection`
    /// a caller could legally pass `&mut pool.acquire().await?`, every
    /// statement would autocommit, and the contract would silently break
    /// without any compile-time signal. Requiring a `Transaction` lifts that
    /// invariant into the type system.
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
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
        let aggregate_key = event.aggregate_key();
        if let Some(ref ak) = aggregate_key
            && (ak.is_empty() || ak.len() > limits::MAX_AGGREGATE_KEY_BYTES)
        {
            return Err(DispatchError::AggregateKeyInvalid {
                len: ak.len(),
                max: limits::MAX_AGGREGATE_KEY_BYTES,
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
        //
        // Single-statement no-op upsert: `DO UPDATE SET event_id =
        // dispatch_keys.event_id` always returns the row whether we inserted
        // or hit the conflict. `xmax = 0` distinguishes the two — true for a
        // freshly inserted row, false when the conflict path took over.
        //
        // Why not "DO NOTHING + follow-up SELECT": the follow-up window was
        // visible to concurrent `purge_dispatch_keys` (DO NOTHING does not
        // lock the conflicting row), so a purge between the two statements
        // could surface as RowNotFound → caller retry → fresh INSERT under a
        // *different* event_id, silently breaking the idempotency contract.
        // The single statement closes the window: the upsert either inserts
        // or holds the existing row's lock for the duration of `tx`.
        if let Some(key) = ctx.idempotency_key() {
            let (existing_event_id, inserted): (Uuid, bool) = sqlx::query_as(
                "INSERT INTO outbox.dispatch_keys (tenant_id, idempotency_key, event_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (tenant_id, idempotency_key)
                 DO UPDATE SET event_id = dispatch_keys.event_id
                 RETURNING event_id, (xmax = 0) AS inserted",
            )
            .bind(ctx.tenant_id())
            .bind(key)
            .bind(event_id)
            .fetch_one(&mut **tx)
            .await?;

            if !inserted {
                tracing::info!(
                    target: "rust_events.dispatch.dup",
                    event_id = %existing_event_id,
                    "duplicate dispatch returned existing event_id"
                );
                return Ok(DispatchOutcome::Duplicate {
                    event_id: existing_event_id,
                });
            }
        }

        // 6. INSERT outbox.events. Serialize headers ONCE and pre-check size
        //    to surface a typed error rather than an opaque DB CHECK violation.
        //    Serialize the map directly (skip the redundant Value::Object
        //    wrap that was being copied to bind a serde_json::Value, which
        //    sqlx would then re-serialize on its way to the JSONB column).
        //    Bind as TEXT with `$7::jsonb` PG-side cast — same wire shape,
        //    one serialization pass.
        let empty_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        let headers_ref = ctx.headers().unwrap_or(&empty_map);
        let headers_text = serde_json::to_string(headers_ref).map_err(DispatchError::Codec)?;
        if headers_text.len() > limits::MAX_HEADERS_BYTES {
            return Err(DispatchError::HeadersTooLarge {
                size: headers_text.len(),
                max: limits::MAX_HEADERS_BYTES,
            });
        }
        // 7. INSERT events (+ handler_deliveries fan-out in the same CTE when
        //    we have handlers — saves one round-trip in the user's tx).
        //    `allow_no_handlers=true` with zero handlers takes the simple
        //    INSERT path and returns NoHandlers; the common path merges.
        if handler_ids.is_empty() {
            sqlx::query(
                "INSERT INTO outbox.events
                    (id, event_type, producer_bc, tenant_id, aggregate_key, payload, headers)
                 VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)",
            )
            .bind(event_id)
            .bind(E::EVENT_TYPE)
            .bind(ctx.producer_bc())
            .bind(ctx.tenant_id())
            .bind(aggregate_key.as_deref())
            .bind(&payload)
            .bind(&headers_text)
            .execute(&mut **tx)
            .await?;
            tracing::info!(
                target: "rust_events.dispatch.empty",
                event_id = %event_id,
                event_type = E::EVENT_TYPE,
                "event persisted with no handlers (allow_no_handlers=true)"
            );
            return Ok(DispatchOutcome::NoHandlers { event_id });
        }

        // 8. CTE merge: INSERT events + multi-row INSERT handler_deliveries
        //    in a single statement. PG's "WITH ... RETURNING" sees the
        //    modifying CTE's row in the main INSERT — handler_deliveries.event_id
        //    references the events row that was just created. The non-deferrable
        //    FK is satisfied because the events row is visible inside the
        //    same statement.
        let handler_id_array: Vec<&str> = handler_ids.iter().map(String::as_str).collect();
        sqlx::query(
            "WITH ev AS (
                INSERT INTO outbox.events
                    (id, event_type, producer_bc, tenant_id, aggregate_key, payload, headers)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                RETURNING id
            )
            INSERT INTO outbox.handler_deliveries (event_id, handler_id)
            SELECT ev.id, unnest($8::text[]) FROM ev",
        )
        .bind(event_id)
        .bind(E::EVENT_TYPE)
        .bind(ctx.producer_bc())
        .bind(ctx.tenant_id())
        .bind(aggregate_key.as_deref())
        .bind(&payload)
        .bind(&headers_text)
        .bind(&handler_id_array)
        .execute(&mut **tx)
        .await?;

        // 9. Push N jobs to pg_work_queue. The concurrency key is the
        //    handler_id, but stamped ONLY for handlers that have a
        //    concurrency_limit configured — keying every job would double
        //    pgwq's claim-index write churn for no benefit.
        let envelopes: Vec<(HandlerEnvelope, Option<String>)> = handler_ids
            .iter()
            .map(|hid| {
                let key = self
                    .registry
                    .lookup(hid)
                    .and_then(|h| h.concurrency_limit)
                    .map(|_| hid.clone());
                (
                    HandlerEnvelope {
                        event_id,
                        handler_id: hid.clone(),
                    },
                    key,
                )
            })
            .collect();
        pg_work_queue::Pusher::new(PGWQ_QUEUE)
            .push_batch(tx, &envelopes)
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

        // Current-thread tokio runtime + concurrency > 1 means all worker
        // handlers, the poll loop, the reaper, and the timer wheel share a
        // single OS thread. CPU-bound or sync-blocking handlers will starve
        // each other and the rest of the runtime. Most outbox handlers are
        // I/O-bound (DB writes, HTTP) so this still works, but it is rarely
        // what the operator intended — warn loudly so the misconfiguration
        // surfaces in logs.
        if self.config.concurrency > 1
            && matches!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread
            )
        {
            tracing::warn!(
                target: "rust_events.start.current_thread_runtime",
                concurrency = self.config.concurrency,
                "Outbox::start invoked on a current_thread tokio runtime with concurrency > 1; \
                 all handlers will share one OS thread — switch to a multi_thread runtime \
                 (e.g. #[tokio::main(flavor = \"multi_thread\")]) or set concurrency(1)"
            );
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
        // Per-handler concurrency limits → pgwq's per-key concurrency.
        // Key = handler_id; only handlers with a configured limit appear.
        let concurrency_limits: Vec<(String, u32)> = self
            .registry
            .handlers
            .iter()
            .filter_map(|(id, h)| h.concurrency_limit.map(|n| (id.clone(), n)))
            .collect();
        let inner = pg_work_queue::Worker::<HandlerEnvelope>::builder()
            .pool(self.pool.clone())
            .queue(PGWQ_QUEUE)
            .poll_interval(self.config.poll_interval)
            .concurrency(usize::try_from(self.config.concurrency).unwrap_or(usize::MAX))
            .concurrency_limits(concurrency_limits)
            .max_attempts(self.config.max_attempts)
            .lease_timeout(self.config.lease_timeout)
            .handler_timeout(self.config.handler_timeout)
            .retry_backoff(self.config.retry_backoff)
            .panic_policy(self.config.panic_policy)
            .handler(
                move |env: HandlerEnvelope, ctx: pg_work_queue::JobContext| {
                    let runtime = runtime_for_handler.clone();
                    async move { runtime.handle_envelope(env, ctx).await }
                },
            )
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
