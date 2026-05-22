# rust_events

Transactional outbox for Rust services on Postgres, built on `pg_work_queue`.

Full design rationale: `docs/superpowers/specs/2026-05-13-rust-events-design.md`

---

## Status

- Version: `0.4.0` (pre-publish)
- Requires: PostgreSQL 18+ (uses `uuidv7()` native), Rust 1.88+
- License: MIT
- Deployment model: a single worker process per database (single-instance — see [Known limitations](#known-limitations))
- Depends on: `pg_work_queue` v0.1.4 (tag-pinned via git; `set_ignore_missing(true)` migrator coexistence is load-bearing; v0.1.4 adds the per-key concurrency limiting used by `HandlerOptions::concurrency_limit`)

This crate is production-ready for modular monolith workloads on Postgres 18. Neither `rust_events` nor `pg_work_queue` is yet published to crates.io; reference both via git dependency.

---

## Table of Contents

1. [What this crate is (and is not)](#what-this-crate-is-and-is-not)
2. [Quick start](#quick-start)
3. [Architecture](#architecture)
4. [Delivery semantics](#delivery-semantics)
5. [State machine and schema](#state-machine-and-schema)
6. [Limits](#limits)
7. [API reference](#api-reference)
8. [Tracing and observability](#tracing-and-observability)
9. [Design decisions](#design-decisions)
10. [Known limitations](#known-limitations)
11. [Testing](#testing)
12. [License](#license)

---

## What this crate is (and is not)

### What it IS

- A typed event bus where domain code registers handlers (`EventHandler<E>`) and dispatches events inside its own transaction.
- A durable audit log: `outbox.events` is immutable; `outbox.handler_deliveries` tracks per-handler delivery state with fencing tokens mirroring `pg_work_queue`.
- Eager fanout at dispatch time: one event in the user's transaction produces N delivery rows and N `pg_work_queue` jobs in the same transaction.
- An at-least-once delivery system. Combined with `HandlerContext.delivery_key`, callers can achieve exactly-once side-effects against idempotent downstreams.

### What it IS NOT

- **Not** a notification or subscription engine. Channels (email, Slack, webhook), DB-driven subscriptions per user, and condition predicates are explicit non-goals for v1. These belong in a separate crate built on top of this one.
- **Not** a multi-backend abstraction. Postgres-only, built on `pg_work_queue` schema and CTE semantics.
- **Not** a framework. The crate owns the `outbox` schema and one Postgres queue inside `pg_work_queue`. You own your `PgPool`, async runtime, migration tooling, and operational schedule.
- **Not** an admin dashboard or metrics endpoint. Observability is `tracing` events; build your `tracing::Layer` if you need Prometheus.
- **Not** an auto-retention sweeper. Invoke `purge_*` on a schedule of your choice (mirror of `pg_work_queue`'s position).
- **Not** an exactly-once system. No polling queue can be. Handlers MUST use `HandlerContext.delivery_key` (or `dispatch_idempotency_key`) for external dedup.

---

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
rust_events = { git = "https://github.com/zygmunt-pawel/rust_events.git", tag = "v0.4.0" }
pg_work_queue = { git = "https://github.com/zygmunt-pawel/pg_work_queue.git", tag = "v0.1.4" }
sqlx = { version = "=0.8.6", features = ["postgres", "runtime-tokio-rustls", "migrate"] }
serde = { version = "=1.0.228", features = ["derive"] }
```

Define an event type:

```rust
use rust_events::DomainEvent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderCreated {
    order_id: i64,
    amount: i64,
}

impl DomainEvent for OrderCreated {
    // This string is the stable wire name. It must not change after events are
    // in flight; renaming a Rust module or struct won't break dispatch.
    const EVENT_TYPE: &'static str = "shop.order_created";

    // Optional: a business aggregate identifier. Default returns `None`. When
    // set, lets you ask "all events for this order" via the partial index
    // `(tenant_id, aggregate_key, created_at DESC) WHERE aggregate_key IS NOT NULL`.
    fn aggregate_key(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(std::borrow::Cow::Owned(format!("order:{}", self.order_id)))
    }
}
```

Implement a handler:

```rust
use rust_events::{EventHandler, HandlerContext, HandlerError};

struct Auditor;

impl EventHandler<OrderCreated> for Auditor {
    async fn handle(
        &self,
        event: &OrderCreated,
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError> {
        // Use ctx.delivery_key as an idempotency key for external calls.
        // Use ctx.dispatch_idempotency_key when dedup must align with domain-level
        // dispatch idempotency ("the order with this ID was already processed").
        println!("order {} arrived (attempt {})", event.order_id, ctx.attempt);
        Ok(())
    }
}
```

Wire it all together:

```rust
use rust_events::{DispatchContext, OutboxBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::PgPool::connect("postgres://...").await?;

    // Run both migrators once at startup. Order does not matter.
    pg_work_queue::migrator().run(&pool).await?;
    rust_events::migrator().run(&pool).await?;

    // Build and start the outbox worker.
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<OrderCreated, _>("audit", Auditor, HandlerOptions::new())
        .build()?;

    let handle = outbox.start().await?;

    // Dispatch an event inside your domain transaction.
    let mut tx = pool.begin().await?;
    // ... your domain writes ...
    outbox.dispatch(
        &mut tx,
        &DispatchContext::new("acme")
            .with_producer_bc("shop")
            // Idempotency keys are scoped per `tenant_id`, NOT per
            // (tenant_id, event_type). Reusing "order:42" for a different
            // DomainEvent in the same tenant would collapse the second
            // dispatch into the first. Encode any per-type dimension into
            // the key yourself, e.g. format!("{}:{}", E::EVENT_TYPE, id).
            .with_idempotency_key("order_created:42"),
        &OrderCreated { order_id: 42, amount: 100 },
    ).await?;
    tx.commit().await?;

    // Graceful shutdown.
    let (_pgwq_stats, outbox_stats) = handle.shutdown(Duration::from_secs(10)).await?;
    println!("pending deliveries at shutdown: {}", outbox_stats.pending_deliveries);
    Ok(())
}
```

**Single-tenant deployments:** pass `"default"` or your app name as `tenant_id`. The constructor is explicit: there is no `Default` impl on `DispatchContext` so multi-tenant data leaks via `..Default::default()` are impossible.

---

## Architecture

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
│     ├── registry.handler_ids_for(E::EVENT_TYPE) → ["audit",...]  │
│     │     empty → if !allow_no_handlers:                          │
│     │              Err(NoHandlersRegistered { event_type })       │
│     │            else: return NoHandlers { event_id }             │
│     │                                                             │
│     ├── INSERT outbox.handler_deliveries × N (unnest)             │
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
│  ① Lookup handler in registry                                      │
│     missing → mark_dead + abort (handler not deployed)             │
│                                                                    │
│  ② Atomic CTE: transition handler_deliveries to 'running',         │
│     stamp lease_token, fetch event row + dispatch_idempotency_key  │
│                                                                    │
│  ③ Already terminal? → return Ok (idempotent skip)                 │
│     Row missing? → abort with audit_missing tracing                │
│                                                                    │
│  ④ Decode payload; on error apply decode_error_strategy            │
│     Retry (default) → mark awaiting_retry                          │
│     Abort           → mark dead                                    │
│                                                                    │
│  ⑤ Call typed handler — wrapped in tokio::time::timeout +          │
│     futures::FutureExt::catch_unwind                               │
│       Timeout  → HandlerError::retry("handler_timeout")            │
│       Panic    → HandlerError::{retry,abort} per panic_policy      │
│                                                                    │
│  ⑥ Ok → mark_sent (fenced)                                         │
│     HandlerError::Retry → awaiting_retry or dead at max_attempts   │
│     HandlerError::Skip  → mark_skipped (terminal, not failure)     │
│     HandlerError::Abort → mark_dead                                │
│                                                                    │
│  ⑦ mark_* rows_affected=0 → fenced_out: stale worker, return Ok   │
└────────────────────────────────────────────────────────────────────┘
```

**Data ownership:**
- `pg_work_queue` owns: `pgwq.jobs`, job lifecycle, lease and fencing tokens, reaper, backoff scheduling.
- `rust_events` owns: `outbox.events` (immutable), `outbox.handler_deliveries` (mutable, fenced via `lease_token` copied from `JobContext`), `outbox.dispatch_keys` (idempotency reservations), in-memory handler registry.

---

## Delivery semantics

**At-least-once delivery.** `pg_work_queue` is a polling queue; polling queues cannot guarantee exactly-once execution. The lease + reaper mechanism means a job can be re-claimed if a worker crashes mid-execution. Your handler may run more than once for the same delivery.

A non-exhaustive list of re-invocation paths:
- **Handler explicitly retries.** `HandlerError::Retry` / `Retry { retry_in }` — the standard, intended case.
- **Handler panics or exceeds `handler_timeout`.** Our `handle_envelope` wrap catches both via `tokio::time::timeout` + `FutureExt::catch_unwind` and routes through `mark_awaiting_retry_fenced` (panic policy `Retry`) or `mark_dead_fenced` (panic policy `Dead`).
- **Worker crashes mid-handler.** Lease expires after `lease_timeout`; pgwq's reaper requeues the job. The new worker stamps a fresh `lease_token`, re-stamps the audit row at `status='running'`, and re-invokes the handler.
- **Handler returned `Ok(())` but `mark_sent_fenced` failed transiently.** The audit row stays at `status='running'` and pgwq retries the job. The CTE in step ② accepts re-claim of `'running'` rows (`status NOT IN ('sent','dead','skipped')`) and re-stamps the lease — the handler runs again. Failure modes that hit this: pool exhaustion (`PoolTimedOut`/`PoolClosed`) after the handler returned, connectivity blips on the mark UPDATE, transient PG errors classified non-deterministic by `is_sqlx_deterministic`. Symptom: `rust_events.audit.mark_pool_starved` tracing (when the trigger is pool starvation) preceding a duplicate handler invocation.

The takeaway: handler idempotency must hold even when the handler "succeeded last time" from its own POV. Use:
- `ctx.delivery_key` (a stable UUID across retries of the same delivery) as an `Idempotency-Key` header on outbound HTTP calls.
- `ctx.dispatch_idempotency_key` when dedup must align with the domain-level concept ("the order with this business key was already processed"), not just retry dedup.

The two values are distinct:
- `delivery_key`: a per-handler UUID assigned by `pg_work_queue` at push time. Stable across retries. Different per handler (H1 and H2 for the same event each get their own `delivery_key`).
- `dispatch_idempotency_key`: the raw string you passed to `DispatchContext::with_idempotency_key()`, if any. The same across all handlers for the same event dispatch.

**Fencing tokens.** Every UPDATE to `handler_deliveries` (after the initial claim) includes a `WHERE lease_token = $current_token` guard. If a stale worker wakes up after its lease expired and a new worker has already stamped a fresh token, the stale worker's `mark_*` query returns `rows_affected=0`. The wrapper emits `rust_events.audit.fenced_out` tracing and returns `Ok` (mirroring `pg_work_queue`'s own fenced-out path). The audit row retains the verdict of the successful concurrent worker.

---

## State machine and schema

Handler deliveries go through these states:

```
queued → running → sent       (success)
               → awaiting_retry → running (retry loop)
                               → dead    (last attempt)
               → dead          (abort or strict mode)
               → skipped       (handler returned HandlerError::Skip)
```

The `outbox.delivery_status` enum in Postgres enforces these transitions, with CHECK constraints ensuring internal consistency (e.g., `lease_token NOT NULL iff status='running'`, `finished_at NOT NULL iff terminal`).

**Schema: three tables in the `outbox` schema**

`outbox.events` — immutable audit log. One row per dispatch (or per de-duplicated dispatch). Write-once: protected by a `deny_update` trigger. UUID PK using UUIDv7 (time-ordered, Postgres 18 native).

`outbox.dispatch_keys` — idempotency reservations. Composite PK `(tenant_id, idempotency_key)`. Scoped per tenant. DEFERRABLE FK to `outbox.events` to allow dispatch_key and event to be inserted in the same transaction.

`outbox.handler_deliveries` — per-handler mutable state. Mirrors `pg_work_queue`'s fencing discipline. One row per (event, handler) pair. BIGINT identity PK (internal mechanics only; external identity is the event UUID).

Both `pg_work_queue` and `rust_events` share the `_sqlx_migrations` table. Both migrators use `set_ignore_missing(true)`. Call both migrators at startup; order does not matter.

The migration also checks the PG version at runtime (`current_setting('server_version_num')::int < 180000`) and raises an exception on unsupported versions.

**Indexes on `outbox.events` are minimal by design.** The only index is `events_created_at_idx` on `(created_at)`, which `purge_events` requires (it filters and orders by `created_at`, and is a library-owned public API — the cost cannot be left to operators). Listing indexes for application queries (most common: `(tenant_id, event_type, created_at DESC)`) are deliberately left to operators so the initial migration stays write-cheap.

---

## Limits

Public byte limits, enforced both at Rust input-validation time and at DB-level CHECK constraints (defense in depth). All lengths are measured in **bytes**, not Unicode characters. Truncation, where applied, is UTF-8-codepoint-safe — a multi-byte codepoint crossing the boundary is dropped, not split.

| Constant | Value | Field | Error |
|----------|-------|-------|-------|
| `MAX_EVENT_TYPE_BYTES`       | 128    | `events.event_type`                  | `DispatchError::EventTypeInvalid` |
| `MAX_HANDLER_ID_BYTES`       | 128    | registration string                  | `BuildError::HandlerIdTooLong`    |
| `MAX_TENANT_BYTES`           | 64     | `events.tenant_id`                   | `DispatchError::TenantIdTooLong`  |
| `MAX_BC_BYTES`               | 64     | `events.producer_bc`                 | `DispatchError::ProducerBcTooLong`|
| `MAX_IDEMPOTENCY_KEY_BYTES`  | 128    | `dispatch_keys.idempotency_key`      | `DispatchError::IdempotencyKeyInvalid` |
| `MAX_AGGREGATE_KEY_BYTES`    | 128    | `events.aggregate_key`               | `DispatchError::AggregateKeyInvalid` |
| `MAX_PAYLOAD_BYTES`          | 1 MiB  | `events.payload` (encoded JSON)      | `DispatchError::PayloadTooLarge`  |
| `MAX_HEADERS_BYTES`          | 16 KiB | `events.headers` (encoded JSON)      | `DispatchError::HeadersTooLarge`  |
| `MAX_LAST_ERROR_BYTES`       | 8 KiB  | `handler_deliveries.last_error`      | truncated via `truncate_utf8`     |

`last_error` is additionally sanitized for control characters (NUL → `?`, ANSI escapes, CR/LF, BEL/DEL, C1 range) before storage to avoid Postgres `22021` `TEXT` rejection and ANSI log-injection in operator consoles.

---

## API reference

Detailed rustdoc at [docs.rs/rust_events](https://docs.rs/rust_events). Brief module summary:

### `OutboxBuilder`

```rust
OutboxBuilder::new(pool)
    .register_handler::<MyEvent, _>("handler_id", MyHandler, HandlerOptions::new())
    .allow_no_handlers(false)   // default: false → error if no handler registered
    .config(OutboxConfig::builder()
        .poll_interval(Duration::from_millis(500))  // default
        .concurrency(16)                             // default
        .max_attempts(5)                             // default
        .lease_timeout(Duration::from_secs(300))     // default
        .handler_timeout(Duration::from_secs(240))   // default (80% of lease)
        .decode_error_strategy(DecodeStrategy::Retry)// default
        .retry_backoff(BackoffPolicy::exponential(
            Duration::from_secs(1), 2.0,
            Duration::from_secs(300), 0.2))         // default
        .panic_policy(PanicPolicy::Retry)            // default
        .build()?)
    .build()?
```

`register_handler` called twice with the same `(EVENT_TYPE, handler_id)` pair surfaces as `BuildError::DuplicateHandlerId` at `build()` time. No silent override.

### Per-handler timeout

`OutboxConfig::handler_timeout` is the global wall-clock budget for every handler invocation. A handler may tighten that budget for itself via `HandlerOptions`:

```rust
.register_handler::<LlmClassify, _>(
    "bc2_llm",
    LlmClassifier,
    HandlerOptions::new().handler_timeout(Duration::from_secs(180)),
)
```

The per-handler value may only **match or tighten** the global budget: it must be `> 400 ms` and `<= OutboxConfig::handler_timeout`. The global value is a hard ceiling because `pg_work_queue`'s worker-wide outer cancellation (and the lease math) is configured with it — `rust_events` cannot extend a handler's budget past what pgwq itself enforces. Set the global `handler_timeout` to your *slowest* handler's needs and use per-handler overrides to hold faster handlers to a tighter bound. A handler registered with `HandlerOptions::new()` (no override) uses the global value unchanged. A violation is `BuildError::ConfigInvalid` at `build()` time.

### Per-handler concurrency

`HandlerOptions::concurrency_limit` caps how many invocations of one handler run at once — e.g. a handler calling a rate-limited external API, or a heavy handler that must not be flooded:

```rust
.register_handler::<ChargeCard, _>(
    "billing",
    BillingHandler,
    HandlerOptions::new().concurrency_limit(2),
)
```

At most `n` tasks for that handler run concurrently. The cap is gated at job-claim time: a saturated handler's jobs are simply not claimed, so they neither occupy worker slots nor block other handlers (no head-of-line blocking). `n` must be `1..=i32::MAX`; `0` is rejected at `build()` with `BuildError::ConfigInvalid`. There is no cross-knob constraint with `OutboxConfig::concurrency` — the two are independent axes; a handler with no `concurrency_limit` is bounded only by the global `concurrency`. The cap is enforced by an in-process counter, correct because `rust_events` runs as a single worker process (see [Known limitations](#known-limitations)).

### `Outbox`

```rust
// Dispatch inside a user transaction:
let outcome = outbox.dispatch(&mut tx, &ctx, &my_event).await?;

// Start the background worker (once per process per Outbox instance):
let handle = outbox.start().await?;

// Query delivery history:
let record = outbox.history().event(event_id).await?;
let deliveries = outbox.history().handler_deliveries_for(event_id).await?;
```

`start()` is guarded: calling it a second time on the same `Outbox` returns `Err(StartError::AlreadyStarted)`. `rust_events` is single-instance — run exactly one worker process per database (see [Known limitations](#known-limitations)).

### `DispatchOutcome`

```rust
match outcome {
    DispatchOutcome::Dispatched { event_id, deliveries } => { /* N handlers enqueued */ }
    DispatchOutcome::Duplicate { event_id } => { /* idempotency_key already used */ }
    DispatchOutcome::NoHandlers { event_id } => { /* only when allow_no_handlers=true */ }
}
```

### `HandlerError`

```rust
// From inside a handler:
return Err(HandlerError::retry("transient upstream error"));
return Err(HandlerError::retry_in("rate limited", Duration::from_secs(60)));
return Err(HandlerError::retry_at("after window", reset_at));  // chrono::DateTime<Utc>
return Err(HandlerError::skip("feature flag off for this tenant"));
return Err(HandlerError::abort("permanent domain error — do not retry"));
```

`retry_at` converts the wall-clock distance from now to `when` into the backoff override; a past timestamp becomes `Duration::ZERO` (retry immediately).

`Skip` is a distinct terminal state (`status='skipped'`) — not a lying success, not a lying failure. Use it for "this event does not apply to me."

### Maintenance

```rust
// Call on a schedule (e.g., daily). Recommended order:
let n1 = purge_terminal_deliveries(&pool, Duration::from_days(30)).await?;
let n2 = purge_dispatch_keys(&pool, Duration::from_days(30)).await?;
let n3 = purge_events(&pool, Duration::from_days(30)).await?;

// Also re-exported from pg_work_queue:
let n4 = purge_done(&pool, Duration::from_days(30)).await?;
let n5 = purge_dead(&pool, Duration::from_days(30)).await?;
```

`purge_events` has a safety guard: it only deletes events where all handler deliveries are terminal (sent/dead/skipped). Events with in-flight deliveries are left untouched regardless of age. Call `purge_terminal_deliveries` first to avoid leaving phantom events behind.

All purge functions chunk at 10,000 rows per call. Loop until the return value is 0 for a full sweep.

---

## Tracing and observability

All instrumentation is via `tracing`. No metrics endpoint. Build a `tracing::Layer` (e.g., `tracing-opentelemetry`, `metrics-tracing-context`) to bridge to Prometheus or OTLP.

**Targets emitted:**

| Target | Level | When |
|--------|-------|------|
| `rust_events.dispatch` | INFO span | Around each `dispatch()` call |
| `rust_events.dispatch.dup` | INFO | `DispatchOutcome::Duplicate` |
| `rust_events.dispatch.empty` | INFO | `DispatchOutcome::NoHandlers` |
| `rust_events.worker` | DEBUG span | Around each `handle_envelope` invocation |
| `rust_events.worker.skip` | INFO | Delivery already terminal, handler skipped |
| `rust_events.worker.skipped` | INFO | Handler returned `HandlerError::Skip` |
| `rust_events.worker.handler_not_registered` | ERROR | Handler not in registry — delivery marked dead |
| `rust_events.worker.audit_missing` | ERROR | `handler_deliveries` row missing — audit corruption |
| `rust_events.worker.audit_inconsistent` | ERROR | Non-terminal row failed UPDATE unexpectedly |
| `rust_events.audit.fenced_out` | WARN | `mark_*` returned `rows_affected=0` — stale worker |
| `rust_events.audit.mark_pool_starved` | WARN | `mark_*_fenced` failed on `PoolTimedOut`/`PoolClosed`/`Io` — see Known Limitations #7 |
| `rust_events.start.current_thread_runtime` | WARN | `Outbox::start` on a `current_thread` runtime with `concurrency > 1` |
| `rust_events.history` | DEBUG span | Around history queries |
| `rust_events.purge` | DEBUG span | Around purge functions |

**Worker self-shutdown.** `pg_work_queue` v0.1.3+ tracks consecutive non-fatal `claim_batch` errors (TLS, network, pool config) and after `MAX_CONSECUTIVE_CLAIM_ERRORS = 30` ticks in a row escalates the most recent error to `last_fatal`. `OutboxHandle::shutdown` then surfaces `ShutdownError::Pgwq(pg_work_queue::ShutdownError::Fatal(_))` — supervising restart loops can react instead of watching `warn!` forever. The counter resets on any successful claim (including empty results).

**Common span fields:** `event_id`, `event_type`, `handler_id`, `tenant_id`, `producer_bc`, `attempt`, `max_attempts`, `idempotency_key_set` (bool, not the value), `outcome`, `deliveries`, `prev_status`.

**Observability contract.** The span-field names and types listed above are part of the public API and follow SemVer:

- **Removing or renaming** a field is a **major** bump.
- **Adding** a new field is a **minor** bump.
- Field **types** (`Uuid`, `&str`, `u32`, `bool`) will not change without a **major** bump.
- Target names (`rust_events.<area>.<kind>`) follow the same rules.

External tracing layers (Prometheus exporters, OTLP pipelines) and alerting rules can be authored against this contract without breaking on patch / minor releases.

---

## Design decisions

**UUID PK on `outbox.events` (not `id BIGINT + public_id UUID`).**
All three roles of `event_id` in this crate — FK target in `handler_deliveries`, value in `HandlerContext.event_id`, and key in the `History` API — use the same value. The `pgwq.jobs` split (BIGINT for internal mechanics, UUID for logs/correlation) is correct when internal query paths primarily use the compact integer. In `outbox.events` there is no such boundary: every reference is external. Maintaining two identifiers would add complexity with no offsetting performance win. UUIDv7 has time-ordered insert locality comparable to `BIGINT IDENTITY`. Cost: `handler_deliveries.event_id` is 16B not 8B; at 100M events × 5 handlers this is ~3 GB overhead. Acceptable for outbox volumes.

**Eager fanout at dispatch time.**
N handlers produce N `pg_work_queue` jobs in the user's transaction. The alternative (lazy fanout: one job, worker fans out) would require a coordinator job and complicate the delivery audit. Eager fanout keeps the dispatch fast (one multi-row INSERT + one `push_batch`), makes the audit straightforward, and lets each handler's retry budget be independent.

**`decode_error_strategy` defaults to `Retry`.**
When you accidentally deploy a breaking payload schema change, the default behavior is to leave jobs in `awaiting_retry` and give you a window for rollback. `Abort` mode dead-letters immediately. Choose `Abort` only when you are certain a decode failure is a permanent fault, not a rollout mistake.

**`allow_no_handlers` defaults to `false`.**
Dispatching an event with no registered handler is most often a configuration error: you forgot to call `register_handler` for a new event type. The default `Err(DispatchError::NoHandlersRegistered)` surfaces this loudly, and no DB write is performed (cheaper to bail). Opt into `true` only when you intentionally want to persist events for audit without routing them anywhere.

**Single-instance by design.**
`rust_events` runs as exactly one worker process per database. A handler that is not in the registry at claim time is a permanent fault (the handler was removed by a deploy) and the delivery is marked `dead` on first claim — there is no other replica that might still have it. Per-handler `concurrency_limit` is likewise enforced by an in-process counter, which is correct precisely because there is only one worker process. Crash-recovery machinery (fencing tokens, the `pg_work_queue` reaper, lease timeouts) is unchanged and still required — a single process still crashes and restarts.

**User handler is wrapped inside `handle_envelope`, even though `pg_work_queue` already provides `handler_timeout` and `panic_policy`.**
Both pgwq mechanisms cancel / flip `pgwq.jobs` *without* calling our handler closure again, which would leave `outbox.handler_deliveries` stuck at `status='running'`. We wrap `handler.handle_erased(...)` in `tokio::time::timeout` (firing `HANDLER_CLEANUP_BUDGET = 200ms` before pgwq's outer timer) and `futures::FutureExt::catch_unwind`, then route both branches through `HandlerError::{retry,abort}`. The existing step ⑦ machinery then calls `mark_*_fenced` and returns `JobError` to pgwq — pgwq applies its own scheduling on `pgwq.jobs` consistently. Net effect: both audit rows reach terminal states together, panic_policy semantics are preserved, and the worker's `handler_timeout` setting still bounds wall-clock per attempt.

---

## Known limitations

1. **Shared `_sqlx_migrations` table.** Both `pg_work_queue::migrator()` and `rust_events::migrator()` write to the same `_sqlx_migrations` table. Both are configured with `set_ignore_missing(true)` so they treat each other's VERSION rows as unknown (not as errors). Call both migrators at startup. Limitation inherited from `pg_work_queue`.

2. **Schema evolution requires `#[serde(default)]` discipline.** Adding a non-optional field to an event struct without `#[serde(default)]` will fail to deserialize old in-flight payloads. The default `DecodeStrategy::Retry` gives a window for rollback (jobs stay `awaiting_retry` until `max_attempts` then go `dead`), but the correct practice is: always add new event fields with `#[serde(default)]`, or version your event type names (`EVENT_TYPE = "shop.order_created.v2"`).

3. **Empty string `tenant_id` and `producer_bc` are conflated with "unset".** The schema allows `""` (it is the default). For single-tenant deployments pass `"default"` or your application name. The explicit constructor `DispatchContext::new(tenant_id)` prevents accidental omission; the storage layer cannot distinguish explicit empty from unset.

4. **Minor status drift between `pgwq.jobs` and `handler_deliveries` after a crash.** If a worker crashes between `mark_dead_fenced` and `pg_work_queue`'s own `mark_done`, the reaper re-queues the job. The next worker sees `handler_deliveries.status='dead'` (terminal), returns `Ok`, and `pg_work_queue` marks the job done. The `pgwq.jobs` row ends up `done` while our row is `dead`. Both are terminal; the at-least-once contract is satisfied. The reverse (pgwq `dead`, our `sent`) is prevented by fencing.

5. **`HandlerError::{Retry,Skip,Abort}::reason` is durable and operator-visible.** Reason strings are persisted in `outbox.handler_deliveries.last_error` AND `pgwq.jobs.last_error`, and surface in operator logs and tracing. Do NOT format payload values, user PII (emails, names, tokens), or other sensitive data into the reason. Prefer enum-like categories — `"upstream_5xx"`, `"rate_limited"`, `"feature_flag_off"`. For detail, use `tracing::error!()` from inside the handler instead; tracing has its own retention policy distinct from the audit columns. Reason strings are sanitized for control characters and ANSI escapes (NUL → `?`, etc.) before storage and truncated to 8 KiB.

6. **Connection-pool sizing.** `pg_work_queue` enforces `pool.options().get_max_connections() >= concurrency × 2 + 2` at `start()`. That covers the worker's own use: one connection for the user handler (claim path) and one for `mark_*_fenced` per concurrent job, plus two for poll and reaper. Anything else hitting the same pool — `Outbox::dispatch` inside user request-handling code, `History::*` queries, `purge_*` sweepers, the `OutboxHandle::shutdown` pending-count query — competes for whatever slack you size above that minimum. When the pool is exhausted, `mark_*_fenced` can fail with `sqlx::Error::PoolTimedOut` BEFORE pgwq's outer `handler_timeout` cancels the wrapper; the audit row then stays `'running'` with our `lease_token` set until pgwq's reaper reclaims it (default `lease_timeout = 300s`). Symptom: a spike of `rust_events.audit.mark_pool_starved` warn-tracing events. Mitigation: size `max_connections >= concurrency × 2 + 2 + (peak concurrent dispatch/history/purge connections on this pool)`. If you have unrelated traffic on the pool, an even higher floor is appropriate.

7. **The `pg_work_queue` v0.1.4 `concurrency_key` migration locks `pgwq.jobs`.** The `20260521000000_v01_concurrency_key.sql` migration builds two indexes non-`CONCURRENTLY`, so it holds `ACCESS EXCLUSIVE` on `pgwq.jobs` for its whole duration — blocking reads and writes (claims, marks, the reaper, pushes). On a queue table kept small by purging this is sub-second; on a large unpurged table it is a full read+write stall proportional to row count. Purge before migrating a hot, large queue table.

---

## Testing

Tests require Docker (for testcontainers with PostgreSQL 18).

```bash
# Run all integration and unit tests:
cargo test

# Run only doctests:
cargo test --doc

# Run a specific test file:
cargo test --test crash_recovery_fencing

# Run with tracing output:
RUST_LOG=rust_events=debug cargo test -- --nocapture

# Check lints:
cargo clippy --all-targets -- -D warnings

# Build docs:
cargo doc --no-deps --open
```

Tests spin up a PostgreSQL 18 container per test via `testcontainers`. Both migrators run on each container before the test. Test containers are shut down after each test.

**Test scope (~73 tests across 1 unit binary + 27 integration binaries):**

- Schema invariants: CHECK constraint violations, `deny_update` trigger, FK CASCADE, state machine consistency
- Dispatch happy paths: with/without idempotency key, multi-handler fanout
- Dispatch idempotency: same key returns `Duplicate`; concurrent dispatchers race (proptest)
- Worker outcomes: sent, retry, last-attempt dead, abort, skip, decode error (Retry and Abort strategies)
- Handler missing: delivery marked dead on first claim (handler removed by a deploy)
- Crash recovery with fencing: simulates stale worker writes, verifies `fenced_out` tracing and audit consistency (B1 tests)
- `delivery_key` vs `dispatch_idempotency_key` propagation (B2 tests)
- Missing `handler_deliveries` row detection (M1 tests)
- Per-handler `concurrency_limit`: build-time validation and end-to-end serialization
- `decode_error_strategy` switching and schema-recovery path (M3 tests)
- `purge_events` NOT EXISTS safety guard and chunking (M4 tests)
- Migrator coexistence: both migrators in either order, idempotent reruns (M5 tests)
- `allow_no_handlers` behavior (M6 tests)
- Purge API signature and behavioral checks (M7 tests)
- Builder validation: `DuplicateHandlerId`, `HandlerIdEmpty`, etc.
- History queries
- Concurrency: two workers with fencing safety; second `start()` returns `AlreadyStarted`
- Proptest idempotency race: N concurrent dispatchers with overlapping keys

---

## License

MIT. See [LICENSE](LICENSE).

Copyright 2026 the rust_events contributors.
