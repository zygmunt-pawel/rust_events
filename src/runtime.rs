//! Worker runtime — fenced audit transitions and the `handle_envelope`
//! wrapper invoked by `pg_work_queue::Worker`.

#![allow(clippy::redundant_pub_crate)]

use crate::builder::OutboxConfig;
use crate::registry::Registry;
use crate::util::truncate_utf8;
use crate::limits;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct OutboxRuntime {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
}

impl OutboxRuntime {
    /// Transition delivery to `sent` IFF `status='running'` AND `lease_token` matches.
    /// `rows_affected=0` → fenced out (stale worker); we emit warn tracing + Ok.
    pub(crate) async fn mark_sent_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='sent', finished_at=now(), lease_token=NULL, last_error=NULL
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_sent: {e}")))?;
        log_fenced_out("sent", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `awaiting_retry` IFF `status='running'` AND `lease_token` matches.
    pub(crate) async fn mark_awaiting_retry_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='awaiting_retry', lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_retry: {e}")))?;
        log_fenced_out("awaiting_retry", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `dead` IFF `status='running'` AND `lease_token` matches.
    pub(crate) async fn mark_dead_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='dead', finished_at=now(), lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_dead: {e}")))?;
        log_fenced_out("dead", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    /// Transition delivery to `skipped` IFF `status='running'` AND `lease_token` matches.
    pub(crate) async fn mark_skipped_fenced(
        &self,
        event_id: Uuid,
        handler_id: &str,
        reason: &str,
        lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError> {
        let trimmed = truncate_utf8(reason, limits::MAX_LAST_ERROR_BYTES);
        let res = sqlx::query(
            "UPDATE outbox.handler_deliveries
             SET status='skipped', finished_at=now(), lease_token=NULL, last_error=$4
             WHERE event_id=$1 AND handler_id=$2
               AND status='running' AND lease_token=$3",
        )
        .bind(event_id)
        .bind(handler_id)
        .bind(lease_token)
        .bind(trimmed)
        .execute(&self.pool)
        .await
        .map_err(|e| pg_work_queue::JobError::retry(format!("mark_skipped: {e}")))?;
        log_fenced_out("skipped", event_id, handler_id, res.rows_affected());
        Ok(())
    }
}

/// Emit a warn-level tracing event when a fenced UPDATE affects 0 rows.
///
/// Called by all `mark_*_fenced` methods. `rows_affected=0` indicates this
/// worker's claim is stale (another worker already applied a terminal
/// transition), which is expected under concurrent claims and is NOT an error.
fn log_fenced_out(
    new_status: &str,
    event_id: Uuid,
    handler_id: &str,
    rows_affected: u64,
) {
    if rows_affected == 0 {
        tracing::warn!(
            target: "rust_events.audit.fenced_out",
            event_id = %event_id,
            handler_id = %handler_id,
            attempted_status = %new_status,
            "mark_* fenced out (stale claim or concurrent terminal verdict)"
        );
    }
}

use crate::builder::DecodeStrategy;
use crate::envelope::HandlerEnvelope;
use crate::handler::{HandlerContext, HandlerError};
use crate::util::{is_pg_constraint_violation, parse_headers};

impl OutboxRuntime {
    /// Handle a single job envelope delivered by `pg_work_queue::Worker`.
    ///
    /// Performs an atomic fenced CTE transition on `outbox.handler_deliveries`,
    /// dispatches to the registered handler, and transitions the row to its
    /// terminal state (`sent`, `awaiting_retry`, `dead`, or `skipped`).
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        skip(self, env, ctx),
        target = "rust_events.worker",
        fields(
            event_id = %env.event_id,
            handler_id = %env.handler_id,
            attempt = ctx.attempt,
            max_attempts = ctx.max_attempts,
        )
    )]
    pub(crate) async fn handle_envelope(
        self: Arc<Self>,
        env: HandlerEnvelope,
        ctx: pg_work_queue::JobContext,
    ) -> Result<(), pg_work_queue::JobError> {
        // ① Registry lookup BEFORE touching the audit row — but ONLY for loose
        // mode. Loose mode must leave `handler_deliveries` completely untouched
        // (status stays 'queued', attempts stays 0) when a handler is absent, so
        // we return early here without executing the CTE.
        //
        // Strict mode defers the lookup until AFTER the CTE transition (step ③)
        // so that the row is already in 'running' state when we call
        // `mark_dead_fenced` — the status invariant requires the row to be
        // 'running' before it can be marked 'dead'.
        if self.registry.lookup(&env.handler_id).is_none()
            && !self.config.strict_handler_lookup
        {
            // Loose mode: leave the row untouched, return retry so pgwq will
            // redeliver when a replica with the handler comes online.
            tracing::warn!(
                target: "rust_events.worker.handler_missing",
                handler_id = %env.handler_id,
                "handler not in this replica's registry; retrying"
            );
            return Err(pg_work_queue::JobError::retry(
                "handler not registered in this replica",
            ));
        }

        // ② Atomic transition + event/dispatch_key fetch via fenced CTE.
        struct Row {
            payload: Vec<u8>,
            tenant_id: String,
            producer_bc: String,
            headers: serde_json::Value,
            dispatch_idempotency_key: Option<String>,
            prev_status: Option<String>,
            did_update: bool,
        }

        let row: Option<Row> = sqlx::query_as::<_, (
            Vec<u8>,             // payload
            String,              // tenant_id
            String,              // producer_bc
            serde_json::Value,   // headers
            Option<String>,      // dispatch_idempotency_key
            Option<String>,      // prev_status
            bool,                // did_update
        )>(
            r"
            WITH locked AS (
                SELECT id, status FROM outbox.handler_deliveries
                WHERE event_id = $1 AND handler_id = $2
                FOR UPDATE
            ),
            updated AS (
                UPDATE outbox.handler_deliveries hd
                SET status = 'running',
                    lease_token = $4,
                    attempts = $3,
                    last_attempted_at = now(),
                    first_attempted_at = COALESCE(hd.first_attempted_at, now()),
                    last_error = NULL
                FROM locked
                WHERE hd.id = locked.id
                  AND locked.status NOT IN ('sent','dead','skipped')
                RETURNING hd.id
            )
            SELECT e.payload,
                   e.tenant_id,
                   e.producer_bc,
                   e.headers,
                   dk.idempotency_key,
                   (SELECT status::text FROM locked),
                   EXISTS(SELECT 1 FROM updated)
            FROM outbox.events e
            LEFT JOIN outbox.dispatch_keys dk ON dk.event_id = e.id
            WHERE e.id = $1
            ",
        )
        .bind(env.event_id)
        .bind(&env.handler_id)
        .bind(i32::try_from(ctx.attempt).unwrap_or(i32::MAX))
        .bind(ctx.lease_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| map_sql(&e, "fetch delivery"))?
        .map(|(p, t, b, h, dk, prev, du)| Row {
            payload: p,
            tenant_id: t,
            producer_bc: b,
            headers: h,
            dispatch_idempotency_key: dk,
            prev_status: prev,
            did_update: du,
        });

        // ③ Discriminate the four CTE result states.
        let Some(row) = row else {
            return Err(pg_work_queue::JobError::abort("event row missing"));
        };
        match (row.prev_status.as_deref(), row.did_update) {
            (None, _) => {
                tracing::error!(
                    target: "rust_events.worker.audit_missing",
                    event_id = %env.event_id,
                    handler_id = %env.handler_id,
                    "handler_deliveries row missing"
                );
                return Err(pg_work_queue::JobError::abort(
                    "handler_delivery row not found",
                ));
            }
            (Some(prev), false) if matches!(prev, "sent" | "dead" | "skipped") => {
                tracing::info!(
                    target: "rust_events.worker.skip",
                    prev_status = %prev,
                    "delivery already terminal — skipping handler"
                );
                return Ok(());
            }
            (Some(other), false) => {
                tracing::error!(
                    target: "rust_events.worker.audit_inconsistent",
                    prev_status = %other,
                    "non-terminal row failed to UPDATE — unexpected"
                );
                return Err(pg_work_queue::JobError::retry(
                    "audit row UPDATE collision",
                ));
            }
            (Some(_), true) => { /* normal path */ }
        }

        // ③b Deferred strict-mode registry check. The row is now 'running'
        //     (CTE updated it in step ②), so mark_dead_fenced's WHERE
        //     `status='running' AND lease_token=$token` can match.
        let handler = if let Some(h) = self.registry.lookup(&env.handler_id) {
            h.clone()
        } else {
            // strict_handler_lookup must be true here — loose mode returned
            // early in step ①.
            tracing::error!(
                target: "rust_events.worker.handler_not_registered",
                handler_id = %env.handler_id,
                "handler not in registry (strict mode) → mark_dead"
            );
            self.mark_dead_fenced(
                env.event_id,
                &env.handler_id,
                "handler not in registry (strict mode)",
                ctx.lease_token,
            )
            .await?;
            return Err(pg_work_queue::JobError::abort(
                "handler not registered (strict mode)",
            ));
        };

        // ④ Build HandlerContext.
        let hctx = HandlerContext {
            event_id: env.event_id,
            tenant_id: row.tenant_id,
            producer_bc: row.producer_bc,
            attempt: ctx.attempt,
            max_attempts: ctx.max_attempts,
            delivery_key: ctx.idempotency_key,
            dispatch_idempotency_key: row.dispatch_idempotency_key,
            headers: parse_headers(row.headers),
        };

        // ⑤ Handler call via type-erased dispatch.
        let result = handler.handle_erased(&row.payload, &hctx).await;

        // ⑥ Translate decode aborts based on decode_error_strategy.
        // TypedHandler::handle_erased returns Abort("decode ...") on JSON decode failure;
        // when strategy=Retry we convert it so the wrapper retries instead.
        let result = match (result, self.config.decode_error_strategy) {
            (Err(HandlerError::Abort { reason }), DecodeStrategy::Retry)
                if reason.starts_with("decode ") =>
            {
                Err(HandlerError::Retry {
                    reason,
                    retry_in: None,
                })
            }
            (other, _) => other,
        };

        // ⑦ Terminal transition based on handler result.
        match result {
            Ok(()) => {
                self.mark_sent_fenced(env.event_id, &env.handler_id, ctx.lease_token)
                    .await?;
                Ok(())
            }
            Err(HandlerError::Retry { reason, retry_in }) => {
                if ctx.attempt >= ctx.max_attempts {
                    self.mark_dead_fenced(
                        env.event_id,
                        &env.handler_id,
                        &reason,
                        ctx.lease_token,
                    )
                    .await?;
                } else {
                    self.mark_awaiting_retry_fenced(
                        env.event_id,
                        &env.handler_id,
                        &reason,
                        ctx.lease_token,
                    )
                    .await?;
                }
                match retry_in {
                    Some(d) => Err(pg_work_queue::JobError::retry_in(reason, d)),
                    None => Err(pg_work_queue::JobError::retry(reason)),
                }
            }
            Err(HandlerError::Skip { reason }) => {
                tracing::info!(
                    target: "rust_events.worker.skipped",
                    reason = %reason,
                    "delivery skipped by handler"
                );
                self.mark_skipped_fenced(
                    env.event_id,
                    &env.handler_id,
                    &reason,
                    ctx.lease_token,
                )
                .await?;
                // Skip is terminal in our audit (status='skipped') but pg_work_queue
                // has no "skipped" — we map to abort so pgwq marks its job dead and
                // does not retry. Operators monitoring pgwq.jobs WHERE status='dead'
                // will see "skipped: <reason>" in last_error; distinguish from real
                // failures by the "skipped: " prefix or by joining outbox.handler_deliveries.status.
                Err(pg_work_queue::JobError::abort(format!("skipped: {reason}")))
            }
            Err(HandlerError::Abort { reason }) => {
                self.mark_dead_fenced(
                    env.event_id,
                    &env.handler_id,
                    &reason,
                    ctx.lease_token,
                )
                .await?;
                Err(pg_work_queue::JobError::abort(reason))
            }
        }
    }
}

/// Map a [`sqlx::Error`] from the fenced-CTE fetch to a [`pg_work_queue::JobError`].
///
/// Constraint violations are treated as permanent (`abort`) since they indicate
/// a data invariant was broken; all other errors are transient (`retry`).
fn map_sql(e: &sqlx::Error, ctx: &str) -> pg_work_queue::JobError {
    if is_pg_constraint_violation(e) {
        pg_work_queue::JobError::abort(format!("{ctx}: constraint violation: {e}"))
    } else {
        pg_work_queue::JobError::retry(format!("{ctx}: {e}"))
    }
}
