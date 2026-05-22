# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this crate is

`rust_events` is a **transactional outbox for Rust services on Postgres**, built as a thin layer on top of `pg_work_queue` (pinned in `Cargo.toml` via `git = "https://github.com/zygmunt-pawel/pg_work_queue.git", tag = "v0.1.4"`; sibling checkout at `../pg_work_queue` is for reference reading only, not as the cargo source). Pre-publish (v0.4.0), MIT, Rust 1.88+, Postgres 18+ (uses native `uuidv7()`).

**Single-instance by design.** The service runs as exactly one worker process per database — never a cluster, never multiple replicas. Handler-lookup (a missing handler is a permanent fault → dead-letter) and per-handler `concurrency_limit` (a process-local counter gating `pg_work_queue`'s job claim) both depend on this. Crash-recovery machinery (fencing tokens, the `pg_work_queue` reaper, lease timeouts) is **not** clustering and stays — a single process still crashes and restarts.

The full design rationale lives in `docs/superpowers/specs/2026-05-13-rust-events-design.md`. **Before reviewing or auditing this crate, read `docs/REVIEW_SCOPE.md`** — it lists deliberate design decisions (single migration file edited in place, no cross-version queue state, no multi-process guard) that are settled and must not be re-flagged as bugs. The README is unusually detailed — read it before doing non-trivial work. It explicitly states what this crate is **not** (notification engine, multi-backend abstraction, exactly-once system, auto-retention sweeper, admin dashboard).

## Commands

```bash
# All tests (require Docker — testcontainers spins up PG 18 per test):
cargo test

# Single test file:
cargo test --test crash_recovery_fencing

# Doctests only:
cargo test --doc

# With tracing:
RUST_LOG=rust_events=debug cargo test -- --nocapture

# Lints (CI-level strictness):
cargo clippy --all-targets -- -D warnings

# Docs:
cargo doc --no-deps --open
```

Dependency versions in `Cargo.toml` are **fully pinned** (`=x.y.z`). Do not relax pins without explicit reason. `unsafe_code` is `forbid`; `unwrap_used`, `expect_used`, and `panic` are `deny` (production code only — tests opt out via `#![cfg_attr(test, allow(...))]` in `tests/common/mod.rs`).

## Architecture, in one screen

```
user tx ──► outbox.dispatch(&mut tx, ctx, &Event)
              ├─ (idempotency_key?) INSERT outbox.dispatch_keys ON CONFLICT → Duplicate
              ├─ INSERT outbox.events  (UUIDv7 client-gen)
              ├─ registry.handler_ids_for(EVENT_TYPE) → [h1, h2, ...]
              ├─ INSERT outbox.handler_deliveries × N (unnest)
              └─ pg_work_queue Pusher.push_batch on queue "outbox_handler_deliveries"
            tx.commit()  ◄── all of the above is atomic with user's domain writes

pg_work_queue::Worker (poll loop, FOR UPDATE SKIP LOCKED) ──► OutboxRuntime::handle_envelope
   ② atomic CTE: handler_deliveries → 'running', stamp lease_token, fetch payload
   ③ already terminal? Ok (idempotent skip)
   ③b registry lookup: miss → mark_dead + abort (handler removed by a deploy)
   ④ decode payload (DecodeStrategy::{Retry,Abort})
   ⑤ user handler wrapped in tokio::time::timeout + futures::FutureExt::catch_unwind
      ├─ Ok(Ok(o))      → o (normal)
      ├─ Ok(Err(panic)) → HandlerError::{retry|abort} per panic_policy
      └─ Err(elapsed)   → HandlerError::retry("handler_timeout")
   ⑥ verdict → mark_sent_fenced / mark_awaiting_retry_fenced / mark_dead_fenced / mark_skipped_fenced
   ⑦ rows_affected=0 on mark_* ⇒ stale worker fenced out, emit tracing, return Ok
```

**Why we wrap user handler ourselves.** pgwq has its own `handler_timeout` and `PanicPolicy`, but on either trigger pgwq cancels the future / flips `pgwq.jobs` to dead *without* re-invoking our handler closure — which would leave `outbox.handler_deliveries` stuck at `status='running'` forever. We pre-empt by wrapping in our own `tokio::time::timeout` (with `HANDLER_CLEANUP_BUDGET = 200ms` reserved before pgwq's outer timer) and `FutureExt::catch_unwind`, so our `mark_*_fenced` runs before pgwq's outer cancellation. pgwq then sees a normal `JobError::{Retry,Abort}` return and applies its scheduling on `pgwq.jobs` consistently. The 200 ms reservation only buys time for the `mark_*` UPDATE — it does **not** guarantee the UPDATE lands if the connection pool is starved (the UPDATE may not acquire a connection in time). Size the pool per the README's `concurrency × 2 + 2 + headroom` rule; `Outbox::start` rejects pools below the `× 2 + 2` floor with `StartError::PoolTooSmall`.

**Data ownership boundary:**
- `pg_work_queue` owns `pgwq.jobs`, leases, fencing, reaper, backoff scheduling.
- `rust_events` owns three tables in the `outbox` schema: `events` (immutable, `deny_update` trigger), `handler_deliveries` (mutable, fenced by `lease_token` copied from `JobContext`), `dispatch_keys` (idempotency).

**One Postgres queue.** All handler deliveries flow through the single queue name `outbox_handler_deliveries` (constant `PGWQ_QUEUE` in `src/outbox.rs`). This is intentional.

**Both migrators share `_sqlx_migrations`** with `set_ignore_missing(true)`. Call both at startup; order does not matter.

## Module map

`src/lib.rs` is the canonical re-export surface — start there. Internal-only modules: `envelope`, `registry`, `runtime`, `util`.

| Concern | File | Notes |
|--|--|--|
| Public entry: dispatch + start | `outbox.rs` | Owns `dispatch()` (the big function), `start()` guarded by `AtomicBool` |
| Worker loop, fenced UPDATEs | `runtime.rs` | The `mark_*_fenced` family — every UPDATE matches `lease_token` |
| Handler trait, `HandlerError`, `HandlerContext` | `handler.rs` | `EventHandler<E>` is async-trait. `HandlerError::{retry, retry_in, skip, abort}` |
| In-memory handler registry | `registry.rs` | `(EVENT_TYPE, handler_id)` keyed; duplicate IDs → `BuildError::DuplicateHandlerId` |
| `OutboxBuilder`, `OutboxConfig`, `DecodeStrategy` | `builder.rs` | Re-exports `BackoffPolicy`/`PanicPolicy` from `pg_work_queue` |
| `DispatchContext` | `dispatch_context.rs` | **No `Default` impl** — tenant_id must be explicit, prevents multi-tenant leaks |
| `OutboxHandle` (shutdown) | `handle.rs` | |
| History queries | `history.rs` | `DeliveryStatus`, `EventRecord`, `HandlerDeliveryRecord` |
| Purge functions | `purge.rs` | `purge_events` has NOT EXISTS safety; chunks 10k rows |
| Byte limits (event_type 128, payload 1 MiB, last_error 8 KiB) | `limits.rs` | Mirrored in SQL CHECK constraints |
| Single migration | `migrations/20260513000001_v01_outbox_init.sql` | PG version check, schema, enum, triggers |

## Conventions to preserve

- **Byte limits, not char limits.** All length constraints (event_type, tenant_id, producer_bc, idempotency_key, aggregate_key, payload, headers, last_error) are measured in bytes, both in Rust (`limits.rs`) and in SQL (`octet_length(...)` CHECKs). Match this when adding new bounded fields.
- **UTF-8-safe truncation.** Use `util::truncate_utf8`, never naive byte slicing — the `rust-safe-string-truncation` skill applies. Truncation is used to bound `last_error` before it lands in the DB.
- **Redact `sqlx::Error` before persisting.** `util::redact_db_error` strips connection details. Apply when an error string is going into `pgwq.jobs.last_error` or `outbox.handler_deliveries.last_error`.
- **Fenced UPDATEs are non-negotiable.** Every mutation of `handler_deliveries` after the initial claim must include `WHERE lease_token = $stamped_token`. `rows_affected=0` ⇒ stale worker — emit `rust_events.audit.fenced_out` and return `Ok`. See `runtime.rs::mark_*_fenced`.
- **User handler must run through our wrap.** `handle_envelope` step ⑤ wraps `handler.handle_erased(...)` in `tokio::time::timeout` + `FutureExt::catch_unwind` so timeouts and panics route through our `mark_*_fenced` before pgwq's own cancellation/`PanicPolicy` fires. Do NOT remove the wrap thinking "pgwq has its own handler_timeout" — pgwq's path bypasses our audit cleanup. The 200 ms `HANDLER_CLEANUP_BUDGET` constant in `runtime.rs` reserves room for the mark UPDATE before pgwq's outer cancel.
- **Tracing targets are namespaced.** All emitted events use `rust_events.<area>.<kind>` (see the README table). Span fields are stable: `event_id, event_type, handler_id, tenant_id, producer_bc, attempt, max_attempts, idempotency_key_set` (the **bool**, never the value).
- **Schema CHECK constraints encode the state machine.** `handler_deliveries_status_invariants` enforces `lease_token NOT NULL iff status='running'`, terminal states have `finished_at NOT NULL`, etc. When changing transitions in `runtime.rs`, change the CHECK in tandem and add a `schema_invariants` test.
- **No silent overrides.** Re-registering the same `(EVENT_TYPE, handler_id)` errors at `build()` time. Calling `start()` twice on the same `Outbox` returns `StartError::AlreadyStarted`.
- **Defaults are conservative.** `allow_no_handlers=false`, `DecodeStrategy::Retry` (window for rollback on schema mistakes). Don't flip these without thinking through the deploy-rollback story.
- **Per-handler `concurrency_limit`.** `HandlerOptions::concurrency_limit(u32)` caps concurrent invocations of one handler. `dispatch()` stamps `pg_work_queue`'s `concurrency_key` with the `handler_id` — but only for handlers that have a limit configured (keying every job doubles pgwq's claim-index churn). `start()` feeds the limits to `WorkerBuilder::concurrency_limits`. Validated `1..=i32::MAX` at `build()`.

## Testing notes

- ~140–170 integration tests live in `tests/`. Each test spins its own PG 18 container via `tests/common/mod.rs::pg_container()`; container is dropped (stopped) at test end. Tests are heavy — Docker must be running.
- Naming maps directly to scenarios: `crash_recovery_fencing`, `handler_removed_marks_dead`, `decode_error_strategy`, `purge_events_safety`, `proptest_idempotency`, `migrator_coexistence`, `per_handler_concurrency`. When adding behavior, find the closest existing test file before creating a new one.
- The `proptest_idempotency` test races N concurrent dispatchers with overlapping keys — keep it deterministic about *invariants*, not exact counts.
- `cargo audit` ignores `RUSTSEC-2023-0071` (reachable only via `sqlx-mysql`, which we don't compile). Rationale documented in `.cargo/audit.toml`.

## Things that look weird but are intentional

- **`outbox.events.id` is UUID (not BIGINT + public_id UUID).** Every reference to `event_id` is external (FK, `HandlerContext`, History API) — the dual-identifier split that `pgwq.jobs` uses would add cost with no benefit here. UUIDv7 keeps insert locality.
- **`outbox.events` has no listing index in the initial migration.** Operators add their own (`(tenant_id, event_type, created_at DESC)` is the common pattern). Keeps initial migration write-cheap.
- **`fillfactor=90` + tight autovacuum on `handler_deliveries`.** It's an update-heavy table (every state transition is an UPDATE). Don't drop these settings.
- **Empty-string `tenant_id`/`producer_bc` are allowed and treated as "unset"** — they're the DEFAULT. The Rust API forces an explicit `DispatchContext::new(tenant_id)` so this only happens when the caller passes `""` deliberately. For single-tenant deployments, pass `"default"` or your app name.
- **Status drift after crash:** if a worker crashes between `mark_dead_fenced` and `pg_work_queue`'s `mark_done`, you can end up with `pgwq.jobs` `done` and `handler_deliveries` `dead`. Both terminal, at-least-once contract holds. The reverse direction (`pgwq.jobs` terminal, `handler_deliveries` still `running`) is normally prevented by the handle-envelope wrap, which catches user-handler timeouts/panics before pgwq fires its own cancellation. The one residual hole is pool starvation: if `mark_*_fenced` cannot acquire a connection within `HANDLER_CLEANUP_BUDGET`, the UPDATE is cancelled. On a non-final attempt the row self-heals — pgwq re-delivers and the claim CTE re-claims the still-`running` row. On the **final** attempt pgwq marks its job `dead` with no re-delivery, leaving the audit row stuck at `running`. `Outbox::start` enforces the `concurrency × 2 + 2` pool floor (`StartError::PoolTooSmall`); a pool sized with headroom per the README never reaches this.
- **Handler not in registry → dead-letter on first claim.** A job whose `handler_id` is absent from the registry (the handler was removed by a deploy) is marked `dead` at step ③b in `runtime.rs::handle_envelope` — after the CTE has moved the row to `'running'`, so the `running → dead` transition satisfies `handler_deliveries_status_invariants`. There is no retry-and-wait: single-instance means no other replica will ever have the handler.
