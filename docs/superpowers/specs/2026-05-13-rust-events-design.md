# rust_events — design spec

**Date:** 2026-05-13 (revised after critical review)
**Status:** Design approved post-review, ready for implementation plan
**Targets:** Rust 1.85+, Postgres 18+, Tokio
**Depends on:** `pg_work_queue >= 0.1` (with commit `34c137d` — `JobContext.max_attempts` + `#[non_exhaustive]`)

## 1. Purpose

`rust_events` is a transactional outbox library for Rust services running on Postgres. It solves the **dual-write problem** in modular monoliths: in one domain transaction, atomically persist business state AND a guarantee of event delivery to downstream consumers. Without an outbox, services choose between dual-write (commit DB, then crash before downstream side-effect → lost event) or distributed transactions (expensive, fragile).

The library does **one thing well**: cross-bounded-context in-process pub/sub of domain events with at-least-once delivery, idempotent dispatch, and a durable audit trail. It is a thin layer over `pg_work_queue`'s polling-based job queue.

### What this crate IS

- A typed event bus where domain code registers handlers (`EventHandler<E>`) and dispatches events inside its own transaction.
- A durable audit log (`outbox.events` immutable, `outbox.handler_deliveries` mutable per-handler state with fencing tokens mirroring `pg_work_queue`).
- Eager fanout at dispatch time: one event in user's tx → N delivery rows + N `pg_work_queue` jobs in the same tx.
- An at-least-once delivery system. Combined with `JobContext.idempotency_key`, callers can achieve exactly-once side-effects against idempotent downstreams.

### What this crate IS NOT

- **Not** a notification/subscription engine. Channels (email/slack/webhook), DB-driven subscriptions per-user, and condition predicates are explicit non-goals for v1 — these are orthogonal responsibilities ("notification engine" built on top of outbox) and belong in a separate crate.
- **Not** a multi-backend abstraction. Postgres-only, built on `pg_work_queue` schema and CTE semantics.
- **Not** a framework. The crate owns the `outbox` schema and one Postgres queue inside `pg_work_queue`. Users own their `PgPool`, async runtime, migration tooling, and operational schedule.
- **Not** an admin dashboard or metrics endpoint. Observability is `tracing` events; metrics are user-side via `tracing::Layer`.
- **Not** an auto-retention sweeper. Operators invoke `purge_*` on a schedule of their choice. Mirror of `pg_work_queue`'s position.
- **Not** an exactly-once system. No polling queue can be. Handlers MUST use `HandlerContext.delivery_key` (or `dispatch_idempotency_key`) for external dedup.

## 2. Scope and Non-Goals (v1)

### In scope

1. `OutboxBuilder` / `Outbox` runtime with config knobs (poll_interval, concurrency, max_attempts, lease_timeout, handler_timeout, retry_backoff, panic_policy, strict_handler_lookup, decode_error_strategy, allow_no_handlers).
2. `DomainEvent` + `EventHandler<E>` traits, type-erased registry.
3. `outbox.dispatch(&mut tx, ctx, &event)` API persisting event + N deliveries + pushing N jobs to one `pg_work_queue` queue.
4. Idempotency keys per (tenant_id, key) with `outbox.dispatch_keys`.
5. Worker wrapper with **fencing tokens** mirroring `pg_work_queue`. Pre-checks terminal/missing state, transitions audit through running/sent/awaiting_retry/skipped/dead, mirrors `pg_work_queue`'s per-row retry budget verdict.
6. `History` API: `event(id)`, `handler_deliveries_for(event_id)`.
7. Manual purge: `purge_terminal_deliveries`, `purge_dispatch_keys`, **`purge_events`** (with NOT EXISTS guard); re-export `pg_work_queue::{purge_done, purge_dead}`.
8. `migrator()` returning `sqlx::Migrator` configured with `set_ignore_missing(true)` to coexist with `pg_work_queue` migrator on shared `_sqlx_migrations` table.

### Explicit non-goals for v1

| Cut | Reason |
|---|---|
| DB-driven per-user channel subscriptions (email/slack/webhook) | Separate responsibility — "notification engine" belongs in its own crate. |
| Subscription condition language (`Condition::eq/Gte/...`) | Eliminated together with channels. |
| `events_for_tenant(paginate)` / generic event listing | Schema is public knowledge; power users SQL. Pagination design is its own surface. |
| Auto background sweeper | Operators schedule purges themselves. |
| Multi-process worker registry / leader election | `pg_work_queue` fencing tokens already make multi-worker safe at row level. We mirror them in our audit. |
| Metrics endpoint | `tracing` only. User builds `tracing::Layer` if Prometheus is needed. |
| Codec swap (`Codec` trait) | `serde_json` only. Users serialize manually if they need binary. |
| `dispatch_global()` / Default DispatchContext | `tenant_id` is required at every dispatch — explicit constructor, no silent multi-tenant data leaks. |

## 3. Constraints

- **C1: Use only `pg_work_queue`'s public API.** No JOINs against `pgwq.jobs`, no reaching into internal modules. Schema of `pg_work_queue` is opaque, mediated solely by `Pusher`, `Worker`, `JobContext`, public errors, `purge_done`/`purge_dead`, and `migrator()`.
- **C2: Minimalist surface.** Cut features rather than feature-flag. No `cfg`-gated subsystems.
- **C3: Postgres 18+.** Uses `uuidv7()` native — same gate as `pg_work_queue`, loud-fail in init migration.
- **C4: Schema namespace `outbox.*`.** No collision with `pgwq.*` or app schemas. Helpers (`set_updated_at`, `deny_update`) live in `outbox.*` too, not `public.*` — same rationale as `pg_work_queue`'s `pgwq.set_updated_at` (avoid colliding with consumer-app helpers).
- **C5: Mirror `pg_work_queue`'s fencing token discipline.** Every UPDATE to `handler_deliveries` (after first claim) MUST include the lease_token guard in its WHERE clause. Concurrent claims of the same row are serialized at the audit-log level, not just at the queue level.

## 4. Architecture

```
┌──────────────────── user tx (domain command) ─────────────────────┐
│                                                                   │
│   business state writes (e.g. INSERT orders ...)                  │
│                                                                   │
│   outbox.dispatch(&mut tx, ctx, &OrderCreated{..})                │
│     │                                                             │
│     ├── [if idempotency_key]                                      │
│     │     INSERT outbox.dispatch_keys ON CONFLICT DO NOTHING      │
│     │     RETURNING event_id                                      │
│     │       0 rows → SELECT existing → return Duplicate           │
│     │                                                             │
│     ├── INSERT outbox.events (id = client-gen uuidv7)             │
│     │                                                             │
│     ├── registry.handler_ids_for(E::EVENT_TYPE) → ["audit",...]   │
│     │     empty → if !allow_no_handlers:                          │
│     │              Err(NoHandlersRegistered { event_type })       │
│     │            else: return NoHandlers { event_id }             │
│     │                                                             │
│     ├── INSERT outbox.handler_deliveries × N (multi-row, unnest)  │
│     │                                                             │
│     └── pg_work_queue::Pusher("outbox_handler_deliveries")        │
│           .push_batch(&mut tx, &envelopes) × 1                    │
│                                                                   │
│   tx.commit()                                                     │
└───────────────────────────────┬───────────────────────────────────┘
                                │
            ┌───────────────────▼───────────────────┐
            │   pg_work_queue::Worker (poll loop)   │
            │   queue: "outbox_handler_deliveries"  │
            │   handler: OutboxRuntime::handle_envelope
            └───────────────────┬───────────────────┘
                                │  per claimed job:
                                ▼
┌──────────────── OutboxRuntime::handle_envelope ────────────────────┐
│  ① Lookup handler in registry BEFORE touching audit row.           │
│     missing → strict_handler_lookup:                               │
│       false → return Err(retry "handler not registered yet")       │
│               (NO wrapper CTE, NO attempts bump in our table —     │
│                handler_deliveries stays `queued` for next claim)   │
│       true  → mark_dead via fenced UPDATE + Err(abort)             │
│                                                                    │
│  ② Atomic transition: WITH CTE                                     │
│     locked  = SELECT id, status, lease_token FROM ... FOR UPDATE   │
│     updated = UPDATE SET status='running', lease_token=$ctx_token, │
│                      attempts=$ctx_attempt, last_attempted_at=...  │
│                WHERE status NOT IN ('sent','dead','skipped')       │
│                RETURNING id                                        │
│     event_lookup = SELECT event + LEFT JOIN dispatch_keys          │
│     return (event_row, prev_status: Option<String>, did_update)    │
│                                                                    │
│  ③ Match on (event_exists, prev_status, did_update):               │
│     (Some, Some(s_sent|s_dead|s_skipped), false) → skip terminal   │
│     (Some, None,                            false) → row missing → │
│                                          Err(abort + audit_missing)│
│     (None,                          _,     _    ) → Err(abort      │
│                                            "event row missing")    │
│     (Some, Some(_),                       true ) → continue        │
│                                                                    │
│  ④ Decode payload as E:                                            │
│     Err → decode_error_strategy:                                   │
│             Retry → mark_awaiting_retry (fenced) + Err(retry)      │
│             Abort → mark_dead (fenced) + Err(abort)                │
│                                                                    │
│  ⑤ handler.handle_erased(payload, &HandlerContext)                 │
│                                                                    │
│  ⑥ match result:                                                   │
│      Ok            → mark_sent (fenced) → Ok                       │
│      Err(Retry)    → if attempt >= max_attempts:                   │
│                        mark_dead (fenced) → Err(retry)             │
│                      else:                                         │
│                        mark_awaiting_retry (fenced) → Err(retry)   │
│      Err(Skip)     → mark_skipped (fenced) → Err(abort "skipped")  │
│      Err(Abort)    → mark_dead (fenced) → Err(abort)               │
│                                                                    │
│  ⑦ mark_* with rows_affected=0 → fenced_out path:                  │
│       tracing::warn!(target="rust_events.audit.fenced_out")        │
│       return Ok (mirror pg_work_queue Stats::fenced_out)           │
└────────────────────────────────────────────────────────────────────┘
```

### Data ownership

- `pg_work_queue` owns: `pgwq.jobs`, job lifecycle (claim/mark/retry/dead), lease + fencing tokens, reaper, backoff scheduling.
- `rust_events` owns: `outbox.events` (immutable), `outbox.handler_deliveries` (mutable mirror of delivery state, **fenced via its own lease_token column copied from JobContext at each claim**), `outbox.dispatch_keys` (idempotency reservations), in-memory handler registry.

### Cross-process / cross-worker semantics

Multiple processes can call `dispatch()` (push side). Multiple processes can run `Outbox::start()` (worker side). `pg_work_queue::Worker::claim_batch` uses `FOR UPDATE SKIP LOCKED` for concurrent claim safety. Our wrapper's `SELECT ... FOR UPDATE` on `handler_deliveries` adds row-level lock for the audit transition window; **fencing tokens prevent stale-worker writes from clobbering newer-worker terminal verdicts** (the critical safety property identified in the post-review fix to original draft).

## 5. Schema (`migrations/20260513000000_v01_outbox_init.sql`)

Classification per postgres-schema-design skill:

| Table | Type | Why |
|---|---|---|
| `outbox.events` | **B1** External Event Log | `event_id` exposed in `HandlerContext`, history queries, public API. Append-only + `deny_update` trigger. App-generated UUIDv7 PK for pre-insert correlation. |
| `outbox.dispatch_keys` | **C** with composite natural PK | Internal idempotency reservation. Only INSERT + DELETE (retention). No UPDATE → no `updated_at`. PK is `(tenant_id, idempotency_key)`. |
| `outbox.handler_deliveries` | **C** | Internal mutable audit state with fencing token mirroring `pgwq.jobs`. Hard delete on retention. No `public_id` (no API CRUD by id — handler accesses via `HandlerContext`). |

### Why UUID PK on `outbox.events` (not the `id BIGINT + public_id UUID` split)

All three roles of `event_id` in `rust_events` use the SAME value:

1. FK target in `handler_deliveries` and `dispatch_keys`.
2. Value in `HandlerContext.event_id` consumed by user handler code.
3. External lookup key in `history()` API.

A hybrid `id BIGINT + public_id UUID` (the `pgwq.jobs` shape) is correct when internal mechanics (reaper, claim_batch CTE, fencing) USE the compact BIGINT while only logs/correlation expose the UUID. In `outbox.events` there is no such internal-vs-external boundary — every reference is an external one. Maintaining two identifiers would require consistent rules about which to use in SQL JOINs and in Rust API at every site, with no offsetting performance win (UUIDv7 has time-ordered insert locality, comparable to BIGINT IDENTITY for our volume).

**Cost paid:** handler_deliveries.event_id is UUID (16B) not BIGINT (8B) → +8B per row × 3 indexes containing event_id (PK index, handler_deliveries_event_idx, dispatch_keys_event_idx). At 100M events × 5 handlers = ~3 GB heap+index overhead. Outbox is lower-volume than the job queue itself; storage scales linearly not quadratically.

### DDL

```sql
DO $$ BEGIN
  IF current_setting('server_version_num')::int < 180000 THEN
    RAISE EXCEPTION 'rust_events requires PostgreSQL 18+ (uuidv7() native), got %',
      current_setting('server_version');
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS outbox;

CREATE FUNCTION outbox.set_updated_at() RETURNS TRIGGER LANGUAGE plpgsql AS $$
  BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

CREATE FUNCTION outbox.deny_update() RETURNS TRIGGER LANGUAGE plpgsql AS $$
  BEGIN RAISE EXCEPTION 'updates are not allowed on table "%"', TG_TABLE_NAME; END $$;

-- ============================================================================
-- (1) outbox.events — Type B1, immutable.
-- ============================================================================
CREATE TABLE outbox.events (
    id           UUID        PRIMARY KEY,
    event_type   TEXT        COLLATE "C" NOT NULL,
    producer_bc  TEXT        COLLATE "C" NOT NULL DEFAULT '',
    tenant_id    TEXT        COLLATE "C" NOT NULL DEFAULT '',
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Byte limits everywhere (not character length) — storage-bound, predictable
    -- across multi-byte UTF-8 inputs. Rust validates input.len() (bytes) too.
    CONSTRAINT events_event_type_bytes   CHECK (octet_length(event_type) BETWEEN 1 AND 128),
    CONSTRAINT events_producer_bc_bytes  CHECK (octet_length(producer_bc) <= 64),
    CONSTRAINT events_tenant_id_bytes    CHECK (octet_length(tenant_id) <= 64),
    CONSTRAINT events_payload_size       CHECK (octet_length(payload) <= 1048576),
    CONSTRAINT events_headers_object     CHECK (jsonb_typeof(headers) = 'object')
);

-- No listing index in initial migration. Operators add their own for their query
-- patterns (most common: tenant + event_type + recency). Keeping initial migration
-- write-cheap; documented in README.

CREATE TRIGGER deny_update_events
    BEFORE UPDATE ON outbox.events
    FOR EACH ROW EXECUTE FUNCTION outbox.deny_update();

-- ============================================================================
-- (2) outbox.dispatch_keys — composite PK, DEFERRABLE FK.
-- ============================================================================
CREATE TABLE outbox.dispatch_keys (
    tenant_id        TEXT        COLLATE "C" NOT NULL,
    idempotency_key  TEXT        COLLATE "C" NOT NULL,
    event_id         UUID        NOT NULL
                     REFERENCES outbox.events(id) ON DELETE CASCADE
                     DEFERRABLE INITIALLY DEFERRED,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, idempotency_key),

    CONSTRAINT dispatch_keys_tenant_bytes  CHECK (octet_length(tenant_id) <= 64),
    CONSTRAINT dispatch_keys_key_bytes     CHECK (octet_length(idempotency_key) BETWEEN 1 AND 128)
);

CREATE INDEX dispatch_keys_event_idx ON outbox.dispatch_keys (event_id);

-- Purge sweeps by created_at; without this index, full table scan per chunk.
CREATE INDEX dispatch_keys_created_at_idx ON outbox.dispatch_keys (created_at);

-- ============================================================================
-- (3) Delivery status enum + handler_deliveries.
--   lease_token mirrors pg_work_queue's fencing discipline:
--     after the first claim, all UPDATEs MUST match the stamped lease_token,
--     or they are silently rejected (mark_* with rows_affected=0 → fenced_out).
-- ============================================================================
CREATE TYPE outbox.delivery_status AS ENUM (
    'queued', 'running', 'awaiting_retry', 'sent', 'skipped', 'dead'
);

CREATE TABLE outbox.handler_deliveries (
    id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id           UUID        NOT NULL REFERENCES outbox.events(id) ON DELETE CASCADE,
    handler_id         TEXT        COLLATE "C" NOT NULL,
    status             outbox.delivery_status NOT NULL DEFAULT 'queued',
    attempts           INTEGER     NOT NULL DEFAULT 0,
    last_error         TEXT,
    -- Fencing token: NULL when not running, set to JobContext.lease_token while
    -- in 'running'. Cleared on every transition out of 'running'. All mark_*
    -- helpers WHERE lease_token = $token; mismatched (stale-worker) UPDATE
    -- returns rows_affected=0 and the wrapper emits fenced_out tracing.
    lease_token        UUID,
    first_attempted_at TIMESTAMPTZ,
    last_attempted_at  TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT handler_deliveries_handler_bytes
        CHECK (octet_length(handler_id) BETWEEN 1 AND 128),
    CONSTRAINT handler_deliveries_attempts_nonneg
        CHECK (attempts >= 0),
    CONSTRAINT handler_deliveries_last_error_bytes
        CHECK (last_error IS NULL OR octet_length(last_error) <= 8192),
    CONSTRAINT handler_deliveries_temporal CHECK (
        (first_attempted_at IS NULL OR first_attempted_at >= created_at)
        AND (last_attempted_at IS NULL OR last_attempted_at >= COALESCE(first_attempted_at, created_at))
        AND (finished_at IS NULL OR finished_at >= COALESCE(last_attempted_at, created_at))
        AND updated_at >= created_at
    ),
    -- State machine invariant: lease_token NOT NULL iff status='running'.
    -- This is what makes the fencing-token guard in mark_* meaningful — any
    -- code path producing a logically impossible state fails loudly here.
    CONSTRAINT handler_deliveries_status_invariants CHECK (
        (status = 'queued'
            AND attempts = 0
            AND first_attempted_at IS NULL
            AND last_attempted_at IS NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status = 'running'
            AND attempts > 0
            AND first_attempted_at IS NOT NULL
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NOT NULL)
        OR (status = 'awaiting_retry'
            AND attempts > 0
            AND first_attempted_at IS NOT NULL
            AND last_attempted_at IS NOT NULL
            AND finished_at IS NULL
            AND lease_token IS NULL)
        OR (status IN ('sent','dead','skipped')
            AND finished_at IS NOT NULL
            AND lease_token IS NULL)
    ),

    UNIQUE (event_id, handler_id)
);

CREATE INDEX handler_deliveries_event_idx
    ON outbox.handler_deliveries (event_id);

CREATE INDEX handler_deliveries_pending_idx
    ON outbox.handler_deliveries (status, created_at)
    WHERE status IN ('queued','running','awaiting_retry');

CREATE INDEX handler_deliveries_terminal_idx
    ON outbox.handler_deliveries (finished_at)
    WHERE status IN ('sent','dead','skipped');

CREATE TRIGGER touch_handler_deliveries
    BEFORE UPDATE ON outbox.handler_deliveries
    FOR EACH ROW EXECUTE FUNCTION outbox.set_updated_at();

ALTER TABLE outbox.handler_deliveries SET (
    fillfactor = 90,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);
```

### Limits module

```rust
pub mod limits {
    // Byte counts (not characters). UTF-8 multi-byte chars consume 2-4 bytes each.
    pub const MAX_EVENT_TYPE_BYTES: usize = 128;
    pub const MAX_HANDLER_ID_BYTES: usize = 128;
    pub const MAX_TENANT_BYTES: usize = 64;
    pub const MAX_BC_BYTES: usize = 64;
    pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
    pub const MAX_PAYLOAD_BYTES: usize = 1_048_576; // = pg_work_queue MAX_PAYLOAD_BYTES
    pub const MAX_LAST_ERROR_BYTES: usize = 8192;   // UTF-8-boundary safe truncate at edge
    pub const PURGE_CHUNK_SIZE: usize = 10_000;     // matches pg_work_queue purge chunk
}
```

## 6. Public Rust API

### `DomainEvent` trait

```rust
pub trait DomainEvent: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Stable wire-name, independent of Rust path (renaming a module won't break dispatch).
    const EVENT_TYPE: &'static str;
}
```

### `EventHandler<E>` trait

```rust
#[async_trait::async_trait]
pub trait EventHandler<E: DomainEvent>: Send + Sync + 'static {
    async fn handle(&self, event: &E, ctx: &HandlerContext) -> Result<(), HandlerError>;
}
```

Native `async fn in trait` is not used: registry needs `Arc<dyn ErasedHandler>` trait objects, which require `BoxFuture` wrapping anyway. `async_trait` is the stable, ecosystem-standard choice.

### `HandlerContext`

```rust
pub struct HandlerContext {
    pub event_id: Uuid,
    pub tenant_id: String,
    pub producer_bc: String,
    pub attempt: u32,            // 1-indexed, from pg_work_queue
    pub max_attempts: u32,       // per-row stamped value, from pg_work_queue
    /// UUID of the per-handler pg_work_queue job. Stable across retries of THIS
    /// delivery (each handler gets its own job UUID at dispatch time via push_batch).
    /// Suitable as `Idempotency-Key` header on external API calls — different per
    /// handler, NOT tied to `DispatchContext.idempotency_key`.
    pub delivery_key: Uuid,
    /// User-supplied dispatch-level idempotency key (if any) from the call to
    /// `outbox.dispatch()` that emitted this event. Use this when external dedup
    /// must align with domain-level deduplication ("the order with this id was
    /// already processed"), not just retry-deduplication. NOT a UUID — original
    /// string the caller passed.
    pub dispatch_idempotency_key: Option<String>,
    pub headers: serde_json::Map<String, serde_json::Value>,
}
```

The renaming from `idempotency_key: Uuid` to `delivery_key: Uuid` resolves the trap where two completely different values shared a name. Pre-review draft used `idempotency_key: Uuid` for the per-delivery UUID, conflating it with `DispatchContext.idempotency_key: Option<&str>` (different semantics, different value).

### `HandlerError`

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerError {
    #[error("retry: {reason}")]
    Retry { reason: String, retry_in: Option<Duration> },
    #[error("skip: {reason}")]
    Skip { reason: String },
    #[error("abort: {reason}")]
    Abort { reason: String },
}

impl HandlerError {
    pub fn retry(reason: impl Into<String>) -> Self;
    pub fn retry_in(reason: impl Into<String>, retry_in: Duration) -> Self;
    pub fn skip(reason: impl Into<String>) -> Self;
    pub fn abort(reason: impl Into<String>) -> Self;
}
```

`Skip` is a third terminal outcome for "this event doesn't apply to me" (wrong env, wrong tenant filter, opted-out user, feature flag off). Audit shows `status='skipped'` distinct from `'sent'` (lying success) or `'dead'` (lying failure).

### `OutboxBuilder` / `OutboxConfig`

```rust
pub struct OutboxBuilder { /* pool, config, registry, allow_no_handlers */ }

impl OutboxBuilder {
    pub fn new(pool: PgPool) -> Self;
    pub fn config(self, cfg: OutboxConfig) -> Self;

    /// Register a handler. Takes ownership of `handler` and wraps internally
    /// in `Arc<TypedHandler<E, H>>` — no need for callers to wrap themselves.
    /// `options` carries per-handler overrides; pass `HandlerOptions::new()`
    /// for a handler that should use the global `OutboxConfig` verbatim.
    pub fn register_handler<E, H>(
        self,
        handler_id: impl Into<String>,
        handler: H,
        options: HandlerOptions,
    ) -> Self
    where E: DomainEvent, H: EventHandler<E>;

    /// When true, dispatch() for an event_type with 0 registered handlers
    /// returns `Ok(DispatchOutcome::NoHandlers { event_id })` and the event is
    /// persisted as an audit-only row. When false (default), it returns
    /// `Err(DispatchError::NoHandlersRegistered { event_type })` to prevent
    /// silent-success when a deployer forgets to register_handler for a new
    /// event_type.
    pub fn allow_no_handlers(self, allow: bool) -> Self;

    pub fn build(self) -> Result<Outbox, BuildError>;
}

pub struct OutboxConfig { /* poll_interval, concurrency, max_attempts, lease_timeout,
                             handler_timeout, retry_backoff, panic_policy,
                             strict_handler_lookup, decode_error_strategy */ }
impl OutboxConfig {
    pub fn builder() -> OutboxConfigBuilder;
}
pub struct OutboxConfigBuilder { /* fluent setters mirroring pg_work_queue WorkerBuilder
                                    plus rust_events-specific knobs below */ }

impl OutboxConfigBuilder {
    pub fn poll_interval(self, d: Duration) -> Self;
    pub fn concurrency(self, n: u32) -> Self;
    pub fn max_attempts(self, n: u32) -> Self;
    pub fn lease_timeout(self, d: Duration) -> Self;
    pub fn handler_timeout(self, d: Duration) -> Self;
    pub fn retry_backoff(self, p: BackoffPolicy) -> Self;
    pub fn panic_policy(self, p: PanicPolicy) -> Self;

    /// false (default): worker hitting a handler_id that is not in this process's
    /// registry returns Err(retry) WITHOUT touching the handler_deliveries row
    /// — leaving it `queued` so a different replica (with the new handler) can
    /// pick it up after a rolling deploy. After max_attempts wraps via pgwq's
    /// dead-letter path.
    /// true: missing handler is permanent fault → mark_dead + Err(abort) on first
    /// claim. Use only when handler registration is strictly stable across all
    /// running replicas.
    pub fn strict_handler_lookup(self, strict: bool) -> Self;

    /// Default `Retry`: when payload fails to deserialize as E, return Err(retry)
    /// — gives a window for rollback if schema breakage was deployed accidentally.
    /// `Abort`: decode error is permanent fault → mark_dead immediately.
    pub fn decode_error_strategy(self, s: DecodeStrategy) -> Self;

    pub fn build(self) -> Result<OutboxConfig, BuildError>;
}

#[derive(Debug, Clone, Copy)]
pub enum DecodeStrategy { Retry, Abort }

/// Per-handler registration overrides, passed to `register_handler`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HandlerOptions { /* handler_timeout: Option<Duration> */ }
impl HandlerOptions {
    pub const fn new() -> Self;
    pub const fn handler_timeout(self, d: Duration) -> Self;
}

pub use pg_work_queue::{BackoffPolicy, PanicPolicy};
```

Defaults mirror `pg_work_queue`'s WorkerBuilder defaults: poll 500ms, concurrency 16, max_attempts 5, lease 300s, handler_timeout = 80% lease, exponential backoff base=1s factor=2 cap=5min jitter=0.2. Plus: `strict_handler_lookup=false`, `decode_error_strategy=Retry`, `allow_no_handlers=false`.

**Builder semantics:** `OutboxBuilder::config()` called twice — last wins (mirror pg_work_queue `WorkerBuilder`). `register_handler` called twice with same `(E::EVENT_TYPE, handler_id)` — recorded; surfaces as `BuildError::DuplicateHandlerId` at `build()` time (fail-late). No silent override.

**Per-handler `handler_timeout`:** `register_handler` takes a `HandlerOptions` argument. `HandlerOptions::handler_timeout` overrides the global `OutboxConfig::handler_timeout` for that one handler. The override is **match-or-tighten only** — validated at `build()` to be `> 2 × HANDLER_CLEANUP_BUDGET` and `<= OutboxConfig::handler_timeout`. The global value is a hard ceiling because pgwq's worker-wide outer cancellation (and lease math) is configured with it; rust_events resolves the effective per-attempt budget in `handle_envelope` from the registry-stored override, falling back to the global when unset.

### `Outbox`

```rust
pub struct Outbox { /* pool, config, registry: Arc<Registry>, started: AtomicBool */ }

impl Outbox {
    pub async fn dispatch<E: DomainEvent>(
        &self,
        tx: &mut PgConnection,
        ctx: &DispatchContext<'_>,
        event: &E,
    ) -> Result<DispatchOutcome, DispatchError>;

    pub fn history(&self) -> History<'_>;

    /// First call starts the worker; subsequent calls return
    /// `Err(StartError::AlreadyStarted)` to prevent accidental duplicate workers
    /// on the same queue from a single Outbox instance. (Multiple Outbox instances
    /// in separate processes are fine — pg_work_queue handles concurrent claims.)
    pub async fn start(&self) -> Result<OutboxHandle, StartError>;
}
```

### `DispatchContext`

```rust
/// NO Default impl: tenant_id is required to prevent silent multi-tenant data
/// leaks via `..Default::default()`. Use the constructor.
pub struct DispatchContext<'a> {
    tenant_id: &'a str,
    producer_bc: &'a str,
    idempotency_key: Option<&'a str>,
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

impl<'a> DispatchContext<'a> {
    /// Single-arg constructor: caller MUST decide tenant explicitly. For
    /// single-tenant deployments pass `"default"` or the application name.
    pub fn new(tenant_id: &'a str) -> Self;

    pub fn with_producer_bc(self, bc: &'a str) -> Self;
    pub fn with_idempotency_key(self, key: &'a str) -> Self;
    pub fn with_headers(self, headers: serde_json::Map<String, serde_json::Value>) -> Self;

    // Accessors for internal use:
    pub fn tenant_id(&self) -> &str;
    pub fn producer_bc(&self) -> &str;
    pub fn idempotency_key(&self) -> Option<&str>;
}
```

### `DispatchOutcome`

```rust
pub enum DispatchOutcome {
    Dispatched { event_id: Uuid, deliveries: usize },
    Duplicate  { event_id: Uuid },
    /// Returned ONLY when `OutboxBuilder::allow_no_handlers(true)` is set.
    /// Otherwise this case surfaces as `DispatchError::NoHandlersRegistered`.
    NoHandlers { event_id: Uuid },
}
```

### `OutboxHandle`

```rust
pub struct OutboxHandle { /* wraps pg_work_queue::WorkerHandle + pool ref */ }

impl OutboxHandle {
    /// Returns pg_work_queue's job-level stats plus our delivery-level stats
    /// (count of non-terminal handler_deliveries rows at shutdown time).
    pub async fn shutdown(self, timeout: Duration)
        -> Result<(pg_work_queue::Stats, OutboxStats), ShutdownError>;
}

/// Outbox-level shutdown stats (separate one extra query at shutdown to
/// count non-terminal deliveries — operators want to know what's left).
pub struct OutboxStats {
    /// COUNT(*) FROM handler_deliveries WHERE status IN ('queued','running','awaiting_retry')
    pub pending_deliveries: u64,
}

pub use pg_work_queue::Stats;
```

### `History`

```rust
pub struct History<'a> { /* pool ref */ }

impl<'a> History<'a> {
    pub async fn event(&self, event_id: Uuid) -> Result<Option<EventRecord>, HistoryError>;
    pub async fn handler_deliveries_for(&self, event_id: Uuid)
        -> Result<Vec<HandlerDeliveryRecord>, HistoryError>;
}

pub struct EventRecord {
    pub id: Uuid,
    pub event_type: String,
    pub producer_bc: String,
    pub tenant_id: String,
    pub payload: Vec<u8>,
    pub headers: serde_json::Map<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

pub struct HandlerDeliveryRecord {
    pub id: i64,
    pub event_id: Uuid,
    pub handler_id: String,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub first_attempted_at: Option<DateTime<Utc>>,
    pub last_attempted_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "outbox.delivery_status", rename_all = "snake_case")]
pub enum DeliveryStatus { Queued, Running, AwaitingRetry, Sent, Skipped, Dead }
```

### Maintenance

```rust
/// Returns a migrator configured with `set_ignore_missing(true)` so it
/// coexists with pg_work_queue's migrator on the shared `_sqlx_migrations`
/// table — neither library treats the other's VERSION rows as missing.
pub fn migrator() -> sqlx::Migrator;

/// Deletes handler_deliveries rows in terminal status older than `older_than`.
/// Chunked at `limits::PURGE_CHUNK_SIZE` (10_000). Mirrors pg_work_queue's purge
/// pattern — no chunk_size argument (constant matches pgwq's).
pub async fn purge_terminal_deliveries(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

pub async fn purge_dispatch_keys(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

/// Deletes outbox.events rows older than `older_than` WHERE all deliveries are
/// terminal (sent/dead/skipped). CASCADE removes the terminal handler_deliveries
/// and any dispatch_keys. Safe ordering: call purge_terminal_deliveries +
/// purge_dispatch_keys BEFORE this (otherwise the NOT EXISTS predicate may not
/// find any candidates).
pub async fn purge_events(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

pub use pg_work_queue::{purge_done, purge_dead};
```

## 7. Dispatch flow (in user's tx)

### Steps

1. **Validate inputs** (early, no I/O):
   - `tenant_id.len() <= MAX_TENANT_BYTES`
   - `producer_bc.len() <= MAX_BC_BYTES`
   - `idempotency_key`: `Some(k)` → `1..=MAX_IDEMPOTENCY_KEY_BYTES`
2. **Encode payload**: `serde_json::to_vec(event)?`, check `len <= MAX_PAYLOAD_BYTES`.
3. **Generate `event_id`**: `Uuid::now_v7()` client-side.
4. **Look up handlers**: `registry.handler_ids_for(E::EVENT_TYPE)`.
   - Empty AND `!allow_no_handlers` → return `DispatchError::NoHandlersRegistered { event_type }` BEFORE any DB write (cheaper to bail).
5. **(if idempotency_key)** Atomic check + reserve:
   ```sql
   INSERT INTO outbox.dispatch_keys (tenant_id, idempotency_key, event_id)
   VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING event_id
   ```
   - 1 row returned → continue
   - 0 rows → `SELECT event_id FROM outbox.dispatch_keys WHERE tenant_id=$1 AND idempotency_key=$2` → return `Duplicate { event_id: existing }`
6. **INSERT event**:
   ```sql
   INSERT INTO outbox.events (id, event_type, producer_bc, tenant_id, payload, headers)
   VALUES ($1, $2, $3, $4, $5, $6)
   ```
7. **If handlers empty AND `allow_no_handlers`**: return `NoHandlers { event_id }`. Skip steps 8–9.
8. **INSERT handler_deliveries** (multi-row):
   ```sql
   INSERT INTO outbox.handler_deliveries (event_id, handler_id)
   SELECT $1, unnest($2::text[])
   ```
9. **Push pg_work_queue jobs**:
   ```rust
   pg_work_queue::Pusher::new("outbox_handler_deliveries")
       .push_batch(&mut *tx, &envelopes).await?;
   ```
10. Return `Dispatched { event_id, deliveries: N }`.

### Round-trip cost

- With idempotency_key, no duplicate: 4 round trips (steps 5, 6, 8, 9).
- Without idempotency_key: 3 round trips (steps 6, 8, 9).
- Duplicate path: 2 round trips (step 5 ON CONFLICT + SELECT existing).
- NoHandlers (allow_no_handlers=true): 1–2 round trips (steps 6, optionally 5).
- NoHandlersRegistered error (allow_no_handlers=false): 0 round trips — error before any INSERT.

### `HandlerEnvelope` (pg_work_queue job payload)

```rust
#[derive(Serialize, Deserialize)]
struct HandlerEnvelope {
    event_id: Uuid,
    handler_id: String,
}
```

Worker fetches event payload from `outbox.events` at handle time — envelope stays minimal.

### Edge cases

| Scenario | Behavior |
|---|---|
| 0 handlers registered for event_type, `allow_no_handlers=false` (default) | `Err(DispatchError::NoHandlersRegistered { event_type })` — no DB write. |
| 0 handlers, `allow_no_handlers=true` | Event persisted; `NoHandlers { event_id }` returned. |
| `idempotency_key` matches existing (same tenant) | Existing `event_id` returned in `Duplicate`. No new event. |
| `idempotency_key` matches in DIFFERENT tenant | Treated as new (keys scoped per tenant). |
| Payload > 1 MiB encoded | `DispatchError::PayloadTooLarge` before any INSERT. |
| Same dispatch called twice in one tx with different keys | Two events, two delivery sets. Normal. |
| User rollback after dispatch | All inserts vanish (events, deliveries, dispatch_keys, pgwq jobs). |

## 8. Worker flow (post-commit, in worker process)

Single `pg_work_queue::Worker<HandlerEnvelope>` for queue `"outbox_handler_deliveries"`. Handler closure is our wrapper: `OutboxRuntime::handle_envelope`.

### `Outbox::start()` with single-start guard

```rust
pub async fn start(&self) -> Result<OutboxHandle, StartError> {
    if self.started.swap(true, Ordering::SeqCst) {
        return Err(StartError::AlreadyStarted);
    }
    let runtime = self.runtime.clone();
    let cfg = &self.config;

    let handle = pg_work_queue::Worker::<HandlerEnvelope>::builder()
        .pool(self.pool.clone())
        .queue("outbox_handler_deliveries")
        .poll_interval(cfg.poll_interval)
        .concurrency(cfg.concurrency)
        .max_attempts(cfg.max_attempts)
        .lease_timeout(cfg.lease_timeout)
        .handler_timeout(cfg.handler_timeout)
        .retry_backoff(cfg.retry_backoff.clone())
        .panic_policy(cfg.panic_policy.clone())
        .handler(move |env: HandlerEnvelope, ctx: pg_work_queue::JobContext| {
            let runtime = runtime.clone();
            async move { runtime.handle_envelope(env, ctx).await }
        })
        .build().map_err(StartError::from)?
        .start().await.map_err(StartError::from)?;

    Ok(OutboxHandle::new(handle, self.pool.clone()))
}
```

### `handle_envelope` wrapper — fenced + decode_error_strategy + strict_handler_lookup

```rust
async fn handle_envelope(
    self: &Arc<OutboxRuntime>,
    env: HandlerEnvelope,
    ctx: pg_work_queue::JobContext,
) -> Result<(), pg_work_queue::JobError> {

    // ① Registry lookup BEFORE touching audit row — rolling-deploy friendly.
    let handler = match self.registry.lookup(&env.handler_id) {
        Some(h) => h,
        None => {
            if self.config.strict_handler_lookup {
                // Strict: treat as permanent fault.
                self.mark_dead_fenced(env.event_id, &env.handler_id,
                    "handler not in registry (strict mode)", ctx.lease_token).await?;
                return Err(pg_work_queue::JobError::abort(
                    "handler not registered (strict mode)"));
            } else {
                // Loose (default): leave handler_deliveries untouched, retry.
                // No mark_running, no attempts bump in our table. Next claim
                // may run in a replica that has the handler registered.
                tracing::warn!(
                    target: "rust_events.worker.handler_missing",
                    handler_id = %env.handler_id,
                    event_id = %env.event_id,
                    "handler not in this replica's registry, retrying"
                );
                return Err(pg_work_queue::JobError::retry(
                    "handler not registered in this replica"));
            }
        }
    };

    // ② Atomic transition: fenced UPDATE + fetch event metadata + dispatch_idem.
    let row = sqlx::query!(
        r#"
        WITH locked AS (
            SELECT id, status, lease_token FROM outbox.handler_deliveries
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
        SELECT e.payload, e.event_type, e.producer_bc, e.tenant_id, e.headers,
               dk.idempotency_key AS dispatch_idempotency_key,
               (SELECT status::text FROM locked) AS prev_status,
               EXISTS(SELECT 1 FROM updated) AS did_update
        FROM outbox.events e
        LEFT JOIN outbox.dispatch_keys dk ON dk.event_id = e.id
        WHERE e.id = $1
        "#,
        env.event_id, env.handler_id, ctx.attempt as i32, ctx.lease_token
    )
    .fetch_optional(&self.pool)
    .await
    .map_err(|e| map_internal_sql_err(e, "fetch delivery"))?;

    // ③ Discriminate the three states.
    let Some(row) = row else {
        // Event row absent — purged or never existed. Permanent fault.
        return Err(pg_work_queue::JobError::abort("event row missing"));
    };

    match (row.prev_status.as_deref(), row.did_update) {
        // Row missing in handler_deliveries — silent failure mode prevention.
        (None, _) => {
            tracing::error!(
                target: "rust_events.worker.audit_missing",
                event_id = %env.event_id,
                handler_id = %env.handler_id,
                "handler_deliveries row not found — audit corruption"
            );
            return Err(pg_work_queue::JobError::abort(
                "handler_delivery row not found"));
        }
        // Already terminal (crash recovery path: previous attempt marked
        // sent/skipped/dead before pgwq mark_done). Skip handler — at-least-once
        // contract already satisfied.
        (Some(prev), false) if matches!(prev, "sent" | "dead" | "skipped") => {
            tracing::info!(
                target: "rust_events.worker.skip",
                event_id = %env.event_id,
                handler_id = %env.handler_id,
                prev_status = %prev,
                "skipping; delivery already terminal"
            );
            return Ok(());
        }
        // Non-terminal but did_update=false should be impossible (CTE updates
        // any non-terminal row). Treat as unexpected race.
        (Some(other), false) => {
            tracing::error!(
                target: "rust_events.worker.audit_inconsistent",
                event_id = %env.event_id,
                handler_id = %env.handler_id,
                prev_status = %other,
                "non-terminal row failed to update — unexpected"
            );
            return Err(pg_work_queue::JobError::retry(
                "audit row UPDATE collision"));
        }
        // Normal path: we successfully transitioned to running.
        (Some(_), true) => { /* continue */ }
    }

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

    // ⑤ Decode payload. decode_error_strategy decides handler.
    let event: E = match serde_json::from_slice::<E>(&row.payload) {
        Ok(e) => e,
        Err(e) => {
            let reason = format!("decode {}: {e}", E::EVENT_TYPE);
            match self.config.decode_error_strategy {
                DecodeStrategy::Retry => {
                    self.mark_awaiting_retry_fenced(
                        env.event_id, &env.handler_id, &reason, ctx.lease_token
                    ).await?;
                    return Err(pg_work_queue::JobError::retry(reason));
                }
                DecodeStrategy::Abort => {
                    self.mark_dead_fenced(
                        env.event_id, &env.handler_id, &reason, ctx.lease_token
                    ).await?;
                    return Err(pg_work_queue::JobError::abort(reason));
                }
            }
        }
    };

    // ⑥ Call typed handler.
    let result = handler.handle_with_event(&event, &hctx).await;

    // ⑦ Terminal transition mirroring pg_work_queue's per-row verdict, fenced.
    match result {
        Ok(()) => {
            self.mark_sent_fenced(env.event_id, &env.handler_id, ctx.lease_token).await?;
            Ok(())
        }
        Err(HandlerError::Retry { reason, retry_in }) => {
            if ctx.attempt >= ctx.max_attempts {
                self.mark_dead_fenced(env.event_id, &env.handler_id, &reason, ctx.lease_token).await?;
            } else {
                self.mark_awaiting_retry_fenced(env.event_id, &env.handler_id, &reason, ctx.lease_token).await?;
            }
            match retry_in {
                Some(d) => Err(pg_work_queue::JobError::retry_in(reason, d)),
                None    => Err(pg_work_queue::JobError::retry(reason)),
            }
        }
        Err(HandlerError::Skip { reason }) => {
            self.mark_skipped_fenced(env.event_id, &env.handler_id, &reason, ctx.lease_token).await?;
            // Skip is terminal but not a "failure" — pg_work_queue still needs to
            // mark the job done. We pass abort (suppresses retry) with a Skip-prefix
            // reason so logs distinguish; tracing target is rust_events.worker.skipped.
            tracing::info!(
                target: "rust_events.worker.skipped",
                event_id = %env.event_id,
                handler_id = %env.handler_id,
                reason = %reason,
                "delivery skipped by handler"
            );
            Err(pg_work_queue::JobError::abort(format!("skipped: {reason}")))
        }
        Err(HandlerError::Abort { reason }) => {
            self.mark_dead_fenced(env.event_id, &env.handler_id, &reason, ctx.lease_token).await?;
            Err(pg_work_queue::JobError::abort(reason))
        }
    }
}
```

### Worker round-trip cost

Per claimed job:
1. Registry lookup (in-process, no IO).
2. CTE atomic transition + event fetch + dispatch_key JOIN — 1 round trip.
3. Handler execution (user code; external IO outside our purview).
4. Terminal mark_*_fenced UPDATE — 1 round trip.
5. pg_work_queue's mark_done/retry/dead inside its own connection — 1 round trip from pgwq's side.

Total: **3 round trips per job + handler-side IO**. At pool=8 and 10ms RTT, baseline ≈ 200 jobs/s before handler IO dominates. Sufficient for typical modular-monolith outbox volume (10–100 events/s).

### Registry — type-erased handler dispatch

```rust
pub(crate) struct Registry {
    handlers: HashMap<String, Arc<dyn ErasedHandler>>,  // key = handler_id
    by_type: HashMap<&'static str, Vec<String>>,        // event_type → handler_ids
}

impl Registry {
    pub fn lookup(&self, handler_id: &str) -> Option<&Arc<dyn ErasedHandler>>;
    pub fn handler_ids_for(&self, event_type: &'static str) -> &[String];
}

#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    /// Untyped entry point used by the worker wrapper.
    async fn handle_erased(&self, payload: &[u8], ctx: &HandlerContext)
        -> Result<(), HandlerError>;

    /// Typed entry point — called after wrapper has decoded the event (so we can
    /// branch decode-strategy outside the handler). Default impl decodes then
    /// dispatches; specialized impls bypass the decode step.
    async fn handle_with_event<E: DomainEvent>(&self, event: &E, ctx: &HandlerContext)
        -> Result<(), HandlerError>;
}

pub(crate) struct TypedHandler<E, H> {
    inner: Arc<H>,
    _e: PhantomData<E>,
}

#[async_trait::async_trait]
impl<E, H> ErasedHandler for TypedHandler<E, H>
where E: DomainEvent, H: EventHandler<E>
{
    async fn handle_erased(&self, payload: &[u8], ctx: &HandlerContext)
        -> Result<(), HandlerError>
    {
        let event: E = serde_json::from_slice(payload)
            .map_err(|e| HandlerError::abort(format!("decode {}: {e}", E::EVENT_TYPE)))?;
        self.inner.handle(&event, ctx).await
    }
    // handle_with_event: type-narrowed dispatch — implementation calls
    // self.inner.handle(event, ctx) when E matches the registered type.
}
```

Note: the wrapper calls `handler.handle_with_event(&event, &hctx)` directly after decoding. The fallback `handle_erased` exists for paths where bytes are easier (e.g., future tooling); not used in the main wrapper.

### Crash recovery (with fencing)

| Crash window | pg_work_queue behavior | handler_deliveries state | Next claim |
|---|---|---|---|
| Between pgwq claim and wrapper start | Reaper re-queues at `lease_expires_at` | Still `queued` or `running` with stale `lease_token = T_old` | Worker B claims, ctx.lease_token = T_B. Wrapper ② UPDATE sets lease_token=T_B (CTE updates any non-terminal row). T_old's mark_* (if Worker A wakes up) → rows_affected=0 due to fencing → fenced_out, return Ok. |
| During handler execution (handler hangs past lease) | Reaper re-queues | `running` with T_old | Same as above. Worker A's eventual mark_* is fenced out by Worker B's stamping. |
| Between handler Ok return and mark_sent_fenced | Reaper re-queues | `running` with T_old | Worker B re-runs handler; T_old's mark_sent fails fencing. |
| Between mark_sent_fenced and pgwq mark_done | Reaper re-queues | `sent` (terminal, lease_token=NULL) | Worker B's wrapper ② sees `prev_status='sent'` and `did_update=false` → skip handler, return Ok. Audit-consistent. |
| Between mark_dead_fenced and pgwq mark_done | Reaper re-queues | `dead` (terminal) | Same path as `sent`. Slight pgwq.jobs drift (`done` vs our `dead`) — acceptable, both terminal. |

The fix versus the pre-review draft: in the row labeled "Between mark_sent and pgwq mark_done", the OLD design without fencing would have let a stale Worker A overwrite `sent → dead` (or vice versa) when its handler returned later. With lease_token enforcement, A's UPDATE is silently fenced; audit stays correct.

### `mark_*_fenced` helpers

```rust
impl OutboxRuntime {
    /// All mark_* helpers carry the fencing-token guard. rows_affected=0 means
    /// our claim was stolen (reaper, sibling worker) — we emit
    /// rust_events.audit.fenced_out and return Ok to let pgwq's own mark_*
    /// see the same fence-out and bump fenced_out counter.
    async fn mark_sent_fenced(
        &self, event_id: Uuid, handler_id: &str, lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError>;

    async fn mark_awaiting_retry_fenced(
        &self, event_id: Uuid, handler_id: &str, reason: &str, lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError>;

    async fn mark_dead_fenced(
        &self, event_id: Uuid, handler_id: &str, reason: &str, lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError>;

    async fn mark_skipped_fenced(
        &self, event_id: Uuid, handler_id: &str, reason: &str, lease_token: Uuid,
    ) -> Result<(), pg_work_queue::JobError>;
}
```

SQL template (mark_sent shown; others symmetric):

```sql
UPDATE outbox.handler_deliveries
SET status = 'sent',
    finished_at = now(),
    lease_token = NULL,
    last_error = NULL
WHERE event_id = $1
  AND handler_id = $2
  AND status = 'running'
  AND lease_token = $3
```

`mark_*` returning `rows_affected == 0` → `tracing::warn!(target: "rust_events.audit.fenced_out", ...)` + `return Ok(())`. Mirror `pg_work_queue::worker`'s fenced_out path.

### UTF-8-safe error truncation

```rust
pub(crate) fn truncate_utf8(s: &str, max: usize) -> &str {
    &s[..s.floor_char_boundary(max)]
}
```

`floor_char_boundary` is stable since Rust 1.73; we target 1.85+. Skill `rust-safe-string-truncation`: this is the canonical UTF-8-safe slice.

### SQL error mapping inside wrapper

```rust
fn map_internal_sql_err(e: sqlx::Error, ctx: &str) -> pg_work_queue::JobError {
    if is_pg_constraint_violation(&e) {
        pg_work_queue::JobError::abort(format!("{ctx}: constraint violation: {e}"))
    } else {
        pg_work_queue::JobError::retry(format!("{ctx}: {e}"))
    }
}
```

`is_pg_constraint_violation`: SQLSTATE class `23` (integrity constraint violation). Pattern lifted from `pg_work_queue::worker::is_fatal_sqlx`.

## 9. Error model

All public error types: `thiserror::Error` enum, `#[non_exhaustive]`, `Send + Sync + 'static`.

```rust
#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum BuildError {
    #[error("handler_id must be non-empty")]
    HandlerIdEmpty,
    #[error("handler_id length {len} bytes exceeds max {max}")]
    HandlerIdTooLong { len: usize, max: usize },
    #[error("handler_id '{handler_id}' already registered for event_type '{event_type}'")]
    DuplicateHandlerId { event_type: &'static str, handler_id: String },
    #[error("config invalid: {0}")]
    ConfigInvalid(String),
}

#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum DispatchError {
    #[error("tenant_id length {len} bytes exceeds max {max}")]
    TenantIdTooLong { len: usize, max: usize },
    #[error("producer_bc length {len} bytes exceeds max {max}")]
    ProducerBcTooLong { len: usize, max: usize },
    #[error("idempotency_key length {len} bytes not in 1..={max}")]
    IdempotencyKeyInvalid { len: usize, max: usize },
    #[error("encoded payload size {size} exceeds max {max}")]
    PayloadTooLarge { size: usize, max: usize },
    #[error("no handlers registered for event_type '{event_type}'")]
    NoHandlersRegistered { event_type: &'static str },
    #[error("codec error encoding event")]
    Codec(#[source] serde_json::Error),
    #[error("pg_work_queue push failed")]
    PgwqPush(#[from] pg_work_queue::PushError),
    /// SQLSTATE class 23 (integrity constraint violation) or similar deterministic
    /// database error — retrying is unlikely to help.
    #[error("database constraint violation during dispatch")]
    Constraint(#[source] sqlx::Error),
    /// Transient database error (connection drop, pool starvation, deadlock).
    /// Retrying the whole dispatch is reasonable.
    #[error("transient database error during dispatch")]
    Transient(#[source] sqlx::Error),
}

impl DispatchError {
    /// Heuristic: is retrying this error likely to succeed?
    pub fn is_retriable(&self) -> bool {
        matches!(self, DispatchError::Transient(_)
                     | DispatchError::PgwqPush(e) if e.is_retriable())
    }
}

#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum HistoryError {
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum PurgeError {
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum StartError {
    #[error("outbox already started; second start() rejected")]
    AlreadyStarted,
    #[error("pg_work_queue worker build failed")]
    PgwqBuild(#[from] pg_work_queue::BuildError),
    #[error("pg_work_queue worker start failed")]
    PgwqStart(#[from] pg_work_queue::StartError),
}

#[derive(Debug, thiserror::Error)] #[non_exhaustive]
pub enum ShutdownError {
    #[error("pg_work_queue worker shutdown failed")]
    Pgwq(#[from] pg_work_queue::ShutdownError),
    #[error("could not count pending deliveries at shutdown")]
    PendingCount(#[source] sqlx::Error),
}
```

### Conversions

- `DispatchError`: explicit constructors for `Constraint` / `Transient` via internal `From<sqlx::Error>` that inspects SQLSTATE.
- `From<pg_work_queue::PushError>` for `DispatchError::PgwqPush` (passthrough — `PushError` already classifies by retriability).
- `HistoryError`, `PurgeError`: `From<sqlx::Error>`.
- `StartError`: `From<pg_work_queue::BuildError>`, `From<pg_work_queue::StartError>`.
- `ShutdownError`: `From<pg_work_queue::ShutdownError>`.

`DispatchError::Codec` has `#[source]`, no `From` — because `serde_json::Error` can appear in multiple sites and we want explicit `map_err`.

## 10. History queries (`History` impl)

Two read-only queries, both bounded:

- `event(event_id)`: 0 or 1 row.
- `handler_deliveries_for(event_id)`: bounded by registered-handler count for the event's type. ORDER BY `handler_id ASC`.

No pagination, streaming, broader queries. Power users SELECT directly — schema is the public contract.

## 11. Maintenance / retention

```rust
pub async fn purge_terminal_deliveries(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

pub async fn purge_dispatch_keys(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

/// Safe purge: only deletes events with all deliveries terminal.
/// Recommended ordering: purge_terminal_deliveries → purge_dispatch_keys → purge_events.
pub async fn purge_events(pool: &PgPool, older_than: Duration)
    -> Result<u64, PurgeError>;

pub use pg_work_queue::{purge_done, purge_dead};
```

Chunked DELETE pattern (mirror `pg_work_queue::purge`), `PURGE_CHUNK_SIZE = 10_000`:

```sql
-- purge_terminal_deliveries
WITH victims AS (
    SELECT id FROM outbox.handler_deliveries
    WHERE status IN ('sent','dead','skipped') AND finished_at < $1
    ORDER BY finished_at ASC
    LIMIT 10000
)
DELETE FROM outbox.handler_deliveries WHERE id IN (SELECT id FROM victims)
```

```sql
-- purge_events — safe guard: every related delivery must be terminal.
WITH victims AS (
    SELECT e.id FROM outbox.events e
    WHERE e.created_at < $1
      AND NOT EXISTS (
          SELECT 1 FROM outbox.handler_deliveries hd
          WHERE hd.event_id = e.id
            AND hd.status NOT IN ('sent','dead','skipped')
      )
    ORDER BY e.created_at ASC
    LIMIT 10000
)
DELETE FROM outbox.events WHERE id IN (SELECT id FROM victims)
```

CASCADE removes any remaining terminal `handler_deliveries` and `dispatch_keys` along with the event.

Caller invokes in a loop or schedule. No background sweeper.

## 12. Testing strategy

PG 18 via `testcontainers` per pg_work_queue's pattern. ~140–170 integration tests (bumped from initial 100 estimate after adding tests for the post-review fixes). Each public builder knob has paired tests at two distinct values.

### Test categories

- **Schema invariants**: CHECK violations (status without finished_at, lease_token mismatch with status, byte-length over limits); `deny_update` trigger; FK CASCADE; DEFERRABLE FK on dispatch_keys; `skipped` status invariant (finished_at NOT NULL, lease_token NULL).
- **Dispatch happy paths**: with/without idempotency_key, allow_no_handlers true/false.
- **Dispatch idempotency**: same key → Duplicate; concurrent dispatchers with same key (proptest).
- **NoHandlers behavior**: default returns DispatchError; opt-in returns DispatchOutcome.
- **Worker happy/retry/last-attempt-dead/abort/decode-error-retry/decode-error-abort/skip/handler-missing-strict/handler-missing-loose**.
- **Crash recovery + fencing**: simulate stale worker writes after lease expiry, verify mark_* returns rows_affected=0 and audit stays consistent with successful concurrent claim.
- **Builder validation**: DuplicateHandlerId, HandlerIdEmpty, etc.
- **Purge correctness**: purge_terminal_deliveries / purge_dispatch_keys / purge_events; purge_events NOT EXISTS guard refuses to delete events with in-flight deliveries.
- **History queries**.
- **Concurrency**: two workers, fencing safety; second `start()` returns AlreadyStarted.
- **Migrator coexistence**: pg_work_queue migrator + rust_events migrator both run on the same pool against shared `_sqlx_migrations` table.

### Critical tests (per post-review finding)

Each numbered fix gets at least one dedicated integration test file. Names reflect the finding for traceability in test logs.

#### B1 — Fencing prevents stale worker overwrites (`crash_recovery_fencing.rs`)

Three paired tests for the scenario the original design missed:

1. `b1_stale_worker_ok_after_concurrent_sent__audit_preserves_concurrent_verdict`:
   - Worker A claims, lease_token=T_A, handler sleeps past `handler_timeout` (configure to 1s test-side).
   - Reaper re-queues at lease_expires_at.
   - Worker B claims, lease_token=T_B, handler returns Ok in <100ms.
   - B's `mark_sent_fenced` with T_B succeeds → row: status='sent', lease_token=NULL.
   - A wakes up, handler returns Ok → A's `mark_sent_fenced` with T_A returns `rows_affected=0`.
   - **Assert:** `handler_deliveries.status == 'sent'`, `finished_at` matches B's transition; `tracing` captured `audit.fenced_out` with handler_id+event_id.

2. `b1_stale_worker_abort_after_concurrent_sent__sent_preserved`:
   - Same as above but A's handler returns `HandlerError::Abort` instead of Ok.
   - **Assert:** status remains 'sent' (NOT overwritten with 'dead'); fenced_out tracing fired.

3. `b1_stale_worker_ok_after_concurrent_dead__dead_preserved`:
   - Same flow inverted: B aborts first (mark_dead), then A returns Ok.
   - **Assert:** status remains 'dead'; A's `mark_sent_fenced` fenced out.

Schema invariant check (same file):
4. `b1_invariant_lease_token_required_iff_running`:
   - Direct SQL: INSERT row with status='running' AND lease_token IS NULL → CHECK violation.
   - INSERT row with status='queued' AND lease_token NOT NULL → CHECK violation.

#### B2 — `delivery_key` vs `dispatch_idempotency_key` are distinct values (`handler_context_keys.rs`)

1. `b2_handler_sees_distinct_keys__both_propagated_correctly`:
   - Dispatch with `idempotency_key="order:42"`.
   - Register 2 handlers H1, H2 for the same event_type.
   - Each handler captures `(ctx.delivery_key, ctx.dispatch_idempotency_key)` to a shared Vec.
   - **Assert:**
     - H1's `delivery_key` != H2's `delivery_key` (per-pgwq-job UUIDs).
     - Both delivery_keys are valid UUIDv7 (versions field == 7).
     - H1's and H2's `dispatch_idempotency_key == Some("order:42".into())`.

2. `b2_dispatch_without_idempotency_key__handler_sees_none`:
   - Dispatch without idempotency_key.
   - **Assert:** `ctx.dispatch_idempotency_key == None` in handler, `ctx.delivery_key` still valid Uuid.

3. `b2_delivery_key_stable_across_retries__retry_sees_same_uuid`:
   - Handler returns Retry on attempt 1, Ok on attempt 2.
   - Capture delivery_key from both attempts.
   - **Assert:** attempt_1.delivery_key == attempt_2.delivery_key (pg_work_queue contract).

#### M1 — Missing handler_deliveries row → Abort with audit_missing (`audit_row_missing.rs`)

1. `m1_missing_row__wrapper_aborts_with_audit_missing_tracing`:
   - Dispatch event; before worker tick, manually `DELETE FROM outbox.handler_deliveries WHERE event_id=$1`.
   - Wait one poll cycle.
   - **Assert:** pgwq job ends in `dead` (handler returned Abort); tracing captured `worker.audit_missing` with event_id+handler_id; subsequent SELECT shows row stays deleted.

2. `m1_missing_row_distinct_from_terminal__do_not_treat_as_success`:
   - Compare: terminal `sent` row + same wrapper path → returns Ok with `worker.skip` tracing.
   - Missing row + same wrapper path → returns Err(Abort) with `worker.audit_missing` tracing.
   - **Assert:** distinct tracing events emitted; distinct pgwq outcomes (done vs dead).

#### M2 — Loose handler-lookup retries; strict mode dead-letters (`rolling_deploy_handler_miss.rs`)

1. `m2_loose__handler_added_after_dispatch__eventually_handled`:
   - Build Outbox A with handler set {audit}. Dispatch event of type "new.event" (no handler registered).
   - Wait 2 poll cycles. Worker A's wrapper returns retry without touching handler_deliveries.
   - **Assert:** `handler_deliveries.status == 'queued'`, `attempts == 0` (we didn't bump), `last_attempted_at IS NULL`.
   - Now build Outbox B with handler set {new.event handler}, start.
   - **Assert:** Outbox B's worker picks the job up, handler runs, status='sent'.

2. `m2_strict__handler_missing__dead_immediately`:
   - Outbox built with `strict_handler_lookup(true)`. Dispatch event with no registered handler matching.
   - **Assert:** First claim → `mark_dead_fenced` → status='dead', last_error contains "strict mode".

3. `m2_loose__exhausts_max_attempts__dead_eventually`:
   - Loose mode + handler never registered. After `max_attempts` retries:
   - **Assert:** pgwq job marked dead; our row eventually transitions to 'dead' via wrapper's last-attempt mark_dead path.

#### M3 — decode_error_strategy switching (`decode_error_strategy.rs`)

1. `m3_retry__bad_payload__retries_until_dead`:
   - Default Retry strategy. Manually INSERT outbox.events with payload not matching event struct (e.g., `b"{}"` for `OrderCreated { order_id, amount }`).
   - Push a HandlerEnvelope referencing it.
   - **Assert:** wrapper marks `awaiting_retry` first 4 attempts, mark_dead on 5th; status='dead' after max_attempts; last_error contains "decode".

2. `m3_abort__bad_payload__dead_immediately`:
   - `decode_error_strategy(DecodeStrategy::Abort)`.
   - **Assert:** status='dead' after first claim; `attempts == 1`.

3. `m3_retry_schema_recovery__fix_payload_mid_flight__succeeds`:
   - Default Retry; bad payload; on attempt 2, manually UPDATE outbox.events.payload to valid bytes (simulates ops fix; not normally possible due to deny_update — test bypasses by direct SQL).
   - **Assert:** attempt 2 (or 3) decodes successfully, handler runs, status='sent'.

#### M4 — `purge_events` with NOT EXISTS guard (`purge_events_safety.rs`)

1. `m4_purge_events__refuses_event_with_pending_delivery`:
   - Dispatch event; force handler to retry forever (always returns Retry). After delivery in `awaiting_retry`:
   - Call `purge_events(pool, Duration::from_secs(0))` (any age).
   - **Assert:** returns 0; event still present; delivery still present.

2. `m4_purge_events__deletes_event_with_all_deliveries_terminal`:
   - Dispatch event; let all deliveries complete (status sent or dead or skipped).
   - Call `purge_terminal_deliveries(pool, Duration::from_secs(0))` → cleared deliveries.
   - Call `purge_events(pool, Duration::from_secs(0))` → returns 1.
   - **Assert:** events table empty; dispatch_keys also empty (CASCADE).

3. `m4_purge_events__chunked__respects_PURGE_CHUNK_SIZE`:
   - Dispatch 15_000 events, all terminal. Run purge_events once.
   - **Assert:** returns 10_000 (one chunk); second call returns 5_000; third call returns 0.

#### M5 — `_sqlx_migrations` coexistence (`migrator_coexistence.rs`)

1. `m5_both_migrators__run_in_either_order__success`:
   - Fresh DB. Run `pg_work_queue::migrator().run(&pool).await` first, then `rust_events::migrator().run(&pool).await`.
   - **Assert:** both schemas exist; `SELECT version FROM _sqlx_migrations` returns rows from both migration sets.
   - Tear down. Repeat with reversed order.
   - **Assert:** same outcome.

2. `m5_idempotent_reruns__no_duplicate_apply`:
   - Run `rust_events::migrator().run(&pool)` twice in a row.
   - **Assert:** second call no-op; no errors; row count in `_sqlx_migrations` unchanged after second.

#### M6 — Strict NoHandlers by default (`no_handlers_strict.rs`)

1. `m6_default__no_handler__returns_NoHandlersRegistered_error`:
   - Build Outbox without `allow_no_handlers`. Dispatch event with no registered handler.
   - **Assert:** `Err(DispatchError::NoHandlersRegistered { event_type })`; no row in outbox.events; no row in dispatch_keys.

2. `m6_allow_no_handlers__opt_in_returns_outcome`:
   - Build Outbox with `allow_no_handlers(true)`. Dispatch event with no registered handler.
   - **Assert:** `Ok(DispatchOutcome::NoHandlers { event_id })`; event_id row present in outbox.events; no handler_deliveries.

3. `m6_strict_does_not_persist_event__rollback_clean`:
   - Same as test 1 but inside a tx with other domain writes; assert error propagates and tx remains valid for rollback decision; manually rollback.
   - **Assert:** no orphan event row; domain writes also rolled back.

#### M7 — Purge API consistency (`purge_api_signature.rs`)

This is mostly a compile-time API check; the dedicated test asserts the signature explicitly.

1. `m7_purge_signatures__no_chunk_size_argument`:
   - Trait test: `purge_terminal_deliveries(&pool, Duration::ZERO)` compiles (no third arg).
   - `purge_dispatch_keys(&pool, Duration::ZERO)` compiles.
   - `purge_events(&pool, Duration::ZERO)` compiles.
   - Sanity behavioral test: all three return `Ok(0)` on empty schema.

#### M8 — Lint config + test exception (CI-level, not test-file)

Verified by `cargo clippy --all-targets -- -D warnings` in CI being clean. The acceptance criteria in §18 cover this; no per-test file. Implementor adds `#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]` in `tests/common/mod.rs` and per-test-file where needed; the `cargo clippy` invocation in CI is the test.

### Other critical tests already in the suite (carried from initial spec)

- **Idempotency race (proptest, `proptest_idempotency.rs`)**: N=10 concurrent dispatchers × M=30 idempotency_keys drawn from a pool of size M/3=10 → invariant: `COUNT(*) FROM outbox.events == COUNT(DISTINCT key) across the workload`. Proptest shrinks counterexamples.

- **HandlerError::Skip terminates with status='skipped'**: `worker_skip.rs` — handler returns Skip; assert status='skipped', finished_at set, last_error captures reason, pgwq job marked done (not retried).

- **Concurrency two workers + fencing safety**: two Outbox instances against the same DB; dispatch 100 events; assert each handler delivery happens exactly once across both workers (handler captures counter).

- **`start()` called twice on same Outbox**: assert `Err(StartError::AlreadyStarted)`.

## 13. Observability

`tracing` only, no metrics endpoint. Operators bridge to Prometheus/OpenTelemetry via `tracing::Layer`.

### Targets

- `rust_events.dispatch` — span for `dispatch()`, event `dispatch.complete`
- `rust_events.dispatch.dup` — event when `DispatchOutcome::Duplicate`
- `rust_events.dispatch.empty` — event when `DispatchOutcome::NoHandlers` (loose mode)
- `rust_events.worker` — span for `handle_envelope`
- `rust_events.worker.skip` — event when delivery already terminal
- `rust_events.worker.skipped` — event when handler returned Skip
- `rust_events.worker.handler_missing` — event when handler not in registry (loose retry path)
- `rust_events.worker.audit_missing` — event when handler_deliveries row missing
- `rust_events.worker.audit_inconsistent` — event when non-terminal row failed UPDATE
- `rust_events.audit.fenced_out` — event when mark_* returned rows_affected=0
- `rust_events.history` — span (debug) around history queries
- `rust_events.purge` — span around purge functions

### Span fields

`event_id` (Uuid), `event_type` (&'static str), `handler_id` (&str), `tenant_id` (&str), `producer_bc` (&str when non-empty), `attempt` (u32), `max_attempts` (u32), `idempotency_key_set` (bool — NOT the value), `outcome` (&'static str), `deliveries` (usize), `prev_status` (&str on skip/audit events).

### Levels

| Level | Event |
|---|---|
| INFO | `dispatch.complete`, `dispatch.duplicate`, `dispatch.empty`, `worker.skip`, `worker.skipped` |
| DEBUG | `worker.delivery_start`, `worker.delivery_sent`, `purge.complete` |
| WARN | `worker.handler_missing`, `audit.fenced_out`, `worker.delivery_retry` |
| ERROR | `worker.audit_missing`, `worker.audit_inconsistent`, `worker.delivery_dead`, `worker.handler_not_registered` (strict mode) |

`#[tracing::instrument(skip(self, tx, event), target = "rust_events.dispatch", fields(...))]` on `dispatch()`. Similar on `start`, `shutdown`, purge functions, and the wrapper.

## 14. Module layout

```
src/
├── lib.rs              # public re-exports, crate-level docs
├── builder.rs          # OutboxBuilder, OutboxConfig, OutboxConfigBuilder, DecodeStrategy
├── outbox.rs           # Outbox struct + dispatch()
├── runtime.rs          # OutboxRuntime (handle_envelope, mark_*_fenced)
├── registry.rs         # Registry, ErasedHandler, TypedHandler
├── handler.rs          # DomainEvent, EventHandler, HandlerContext, HandlerError
├── envelope.rs         # HandlerEnvelope (serde)
├── history.rs          # History, EventRecord, HandlerDeliveryRecord, DeliveryStatus
├── purge.rs            # purge_terminal_deliveries, purge_dispatch_keys, purge_events
├── error.rs            # BuildError, DispatchError, HistoryError, PurgeError, StartError, ShutdownError
├── limits.rs           # MAX_*_BYTES constants, PURGE_CHUNK_SIZE
├── migrator.rs         # migrator() returning sqlx::Migrator with set_ignore_missing(true)
├── outcome.rs          # DispatchOutcome, OutboxStats
├── dispatch_context.rs # DispatchContext + builder methods
├── handle.rs           # OutboxHandle (Stats re-exported from pg_work_queue)
└── util.rs             # truncate_utf8, is_pg_constraint_violation, parse_headers

migrations/
└── 20260513000000_v01_outbox_init.sql

tests/
├── common/
│   └── mod.rs                       # testcontainers fixture; #![cfg_attr(test, allow(...))]
├── schema_invariants.rs
├── dispatch_happy_path.rs
├── dispatch_idempotency.rs
├── dispatch_validation.rs
├── worker_happy_path.rs
├── worker_retry.rs
├── worker_last_attempt.rs
├── worker_abort.rs
├── worker_skip.rs                   # HandlerError::Skip → 'skipped' status
├── crash_recovery_fencing.rs        # B1: 4 tests (fencing + invariant)
├── handler_context_keys.rs          # B2: 3 tests (delivery_key vs dispatch_idem_key)
├── audit_row_missing.rs             # M1: 2 tests (missing != terminal)
├── rolling_deploy_handler_miss.rs   # M2: 3 tests (loose/strict modes)
├── decode_error_strategy.rs         # M3: 3 tests (Retry vs Abort + recovery)
├── purge_events_safety.rs           # M4: 3 tests (NOT EXISTS guard + chunking)
├── migrator_coexistence.rs          # M5: 2 tests (both migrators)
├── no_handlers_strict.rs            # M6: 3 tests (default error vs opt-in outcome)
├── purge_api_signature.rs           # M7: signature compile + behavioral
├── builder_validation.rs
├── history_queries.rs
├── concurrency.rs
└── proptest_idempotency.rs
```

## 15. Dependencies (Cargo.toml)

License: MIT (matches `pg_work_queue`). Rust 1.85+, edition 2024.

```toml
[package]
name = "rust_events"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT"
description = "Transactional outbox for Rust services on Postgres, built on pg_work_queue"

[dependencies]
# During pre-publish, this is a path/git dep; switches to crates.io version on publish.
pg_work_queue = "0.1"
sqlx = { version = "=0.8.6", default-features = false, features = [
    "postgres", "runtime-tokio-rustls", "uuid", "chrono", "macros", "migrate",
] }
tokio = { version = "=1.52.3", features = ["rt", "macros", "time", "sync"] }
tracing = "=0.1.44"
serde = { version = "=1.0.228", features = ["derive"] }
serde_json = "=1.0.149"
thiserror = "=2.0.18"
uuid = { version = "=1.23.1", features = ["v4", "v7", "serde"] }
chrono = { version = "=0.4.44", default-features = false, features = ["serde", "clock"] }
async-trait = "=0.1.83"

[dev-dependencies]
tokio = { version = "=1.52.3", features = ["full", "test-util"] }
testcontainers = "=0.27.3"
testcontainers-modules = { version = "=0.15.0", features = ["postgres"] }
tracing-subscriber = { version = "=0.3.23", features = ["env-filter"] }
proptest = "=1.5.0"

[lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
unreachable_pub = "warn"

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
nursery = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
```

**Test files** must allow `unwrap`/`expect`/`panic` since they're banned at crate level. Add at top of `tests/common/mod.rs` (mirror pg_work_queue convention):

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
```

And per-test-file as needed.

## 16. Known limitations

1. **`pg_work_queue` and `rust_events` share the `_sqlx_migrations` table.** Both migrators are configured with `set_ignore_missing(true)` to coexist (each treats unknown VERSION rows as belonging to the other). Order of `migrator().run()` calls doesn't matter for correctness, but both must be called before any DB write. Documented limitation inherited from `pg_work_queue` (commit `814dc15` in its repo).

2. **Schema evolution requires `#[serde(default)]` discipline.** Adding a non-optional field to an event struct without `#[serde(default)]` will fail to decode old in-flight payloads. Default `decode_error_strategy = Retry` gives a window for rollback (events stay in `awaiting_retry` until `max_attempts` then dead), but the right answer is: always add new event fields with `#[serde(default)]`, or version event types.

3. **Tenant/producer_bc empty string is conflated with "unset".** Default values are `""`; absence is not distinguishable from explicit empty. Mitigated by the `DispatchContext::new(tenant_id)` constructor that forces the caller to choose, but storage layer still allows `""`. Sufficient for v1 modular monolith use.

4. **`pgwq.jobs.status='done'` vs `outbox.handler_deliveries.status='dead'` minor drift on crash between mark_dead_fenced and pgwq mark_done.** Both terminal; semantically equivalent for at-least-once contract. Fenced from the other direction (sent overwriting dead) by the lease_token guard.

5. **Loose handler-lookup mode silently retries on missing registry entry.** If the new handler is never deployed, jobs eventually exhaust `max_attempts` and become dead. Detection: handler_deliveries rows stuck at `queued`/`awaiting_retry` with low `attempts` and recent `last_attempted_at` from non-strict workers. Switch to `strict_handler_lookup=true` once the deploy is stable.

## 17. Follow-ups (v1.1 candidates)

- `events_for_tenant(tenant, paginate, status_filter)` paginated query.
- Optional codec swap (`Codec` trait) for binary payloads.
- A separate `rust_events_channels` crate for DB-driven per-user notification subscriptions (the cut from v1 scope).
- Replay API: `replay_to_handler(event_id, handler_id)` — useful after deploying a new handler that needs backfill.
- Listing index `events_tenant_type_listing_idx (tenant_id, event_type, created_at DESC)` — defer to v1.1 or operator-added as needed; not in initial migration.

## 18. Acceptance criteria for implementation

- All ~140–170 integration tests pass against PG 18 via testcontainers, including the proptest idempotency race, the fencing crash-recovery tests (B1), the `delivery_key` vs `dispatch_idempotency_key` propagation tests (B2), and dedicated tests for each post-review fix M1–M7.
- `cargo clippy --all-targets -- -D warnings` with pedantic + nursery clean (with the `#![cfg_attr(test, allow(...))]` exception in test files).
- `cargo test --doc` passes for all public-API doctests.
- `cargo doc --no-deps` builds without warnings.
- README documents: quick start, schema, API reference, delivery semantics (at-least-once + fencing), known limitations (especially the loose handler-lookup behavior on rolling deploys), testing instructions.
- Versions pinned exactly per pg_work_queue convention.
- No `unsafe` code, no `unwrap`/`expect`/`panic` in non-test code.
