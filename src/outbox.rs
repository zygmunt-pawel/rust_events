//! `Outbox` runtime — public entry point for emitting events in user tx.

use crate::builder::OutboxConfig;
use crate::dispatch_context::DispatchContext;
use crate::envelope::HandlerEnvelope;
use crate::error::DispatchError;
use crate::handler::DomainEvent;
use crate::limits;
use crate::outcome::DispatchOutcome;
use crate::registry::Registry;
use sqlx::{PgConnection, PgPool};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use uuid::Uuid;

/// The transactional outbox runtime. Created via [`crate::builder::OutboxBuilder`].
pub struct Outbox {
    #[allow(dead_code)] // used in Phase 8 (start / worker)
    pub(crate) pool: PgPool,
    #[allow(dead_code)] // used in Phase 8 (start / worker)
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

        // 6. INSERT outbox.events.
        let headers_json = serde_json::Value::Object(
            ctx.headers().cloned().unwrap_or_default(),
        );
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
        pg_work_queue::Pusher::new("outbox_handler_deliveries")
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
