# Single-instance rework + per-key concurrency — design

Date: 2026-05-22
Status: approved (brainstorming) — pending implementation plan
Supersedes the multi-replica assumptions in `2026-05-13-rust-events-design.md`.

## Context

`rust_events` was designed assuming a multi-replica deployment (rolling
deploys, several worker replicas claiming from one queue). That assumption
no longer holds: **the service runs on a single machine, as exactly one
process instance, always.** The crate must be reworked to match.

Separately, `pg_work_queue` shipped **v0.1.4**, which adds per-key
concurrency limiting. Its per-key counter is in-process and explicitly
single-instance-only — which is now exactly the right fit. v0.1.4 also
changed the `Pusher` API (breaking), so adopting it is mandatory and the
current build is broken until the call site is updated.

This spec covers three intertwined pieces of work, all touching the same
files (`builder.rs`, `runtime.rs`, `outbox.rs`), delivered as one change:

1. Remove the multi-replica machinery (single-instance simplification).
2. Adapt to the `pg_work_queue` v0.1.4 API.
3. Add the per-handler `concurrency_limit` knob.

## Scope boundary: clustering vs crash-recovery

"Single-instance" removes **multi-replica coordination** machinery only. It
does **not** touch **crash-recovery** machinery — a single process still
crashes (panic, OOM, kill) and restarts on every deploy, and a single
process still has the lease-expiry → reaper → re-claim race that fencing
guards against.

**Removed (multi-replica):** loose handler-lookup mode and everything
downstream of it.

**Kept (crash-recovery — unaffected):** fencing (`lease_token`, the
`mark_*_fenced` family), the `pg_work_queue` reaper, lease timeout, the
handler `tokio::time::timeout` + `catch_unwind` wrap, `SKIP LOCKED`.

**Kept (not multi-replica, out of scope):** `DecodeStrategy::Retry` default
(a deploy-rollback window, not a clustering feature), `allow_no_handlers`,
multi-tenancy (`DispatchContext`/`tenant_id`), `OutboxConfig::concurrency`.

## Part 1 — Remove the multi-replica machinery

The only multi-replica subsystem in the crate is **loose handler-lookup
mode**. Loose mode exists so that, during a rolling deploy, a replica
missing a handler leaves the audit row untouched for another replica that
*has* the handler to claim. With one instance there is no other replica;
a missing handler is a plain configuration/deploy fault.

### Removed surface

- **`builder.rs`** — `OutboxConfig::strict_handler_lookup` field, the
  `OutboxConfigBuilder::strict_handler_lookup()` setter, and the field in
  the `Default` impl. The crate no longer has a lookup-mode knob; strict
  is the only behavior.
- **`runtime.rs`** — step ① of `handle_envelope` (the
  `registry.lookup().is_none() && !strict_handler_lookup` branch, with its
  `UPDATE … resolve_attempts` and tracing) is deleted in full. The deferred
  registry check at step ③b (registry-miss → `mark_dead_fenced` +
  `JobError::abort`) is **unchanged** — it is already the strict behavior;
  only its now-stale comment referencing "loose mode returned early in
  step ①" is updated.
- **`migrations/20260513000001_v01_outbox_init.sql`** — edited **in place**
  (no new migration; the crate is pre-publish, no deployed databases,
  tests spin fresh containers): drop the `resolve_attempts` and
  `last_resolve_attempt_at` columns from `handler_deliveries`, the comment
  above them, and `CONSTRAINT handler_deliveries_resolve_attempts_nonneg`.
- **`history.rs`** — remove `History::stuck_unregistered_handlers()` and
  the `StuckHandlerRow` struct (and any re-export in `lib.rs`).

### Behavioral consequence

A job whose handler was removed across a deploy (job persisted by older
code; new code no longer registers that handler) is marked `dead` on first
claim, instead of looping forever at `queued` as loose mode did. This is
correct under single-instance: no replica with the handler will ever
arrive. The operator sees the row via the normal History API (status
`dead`); the dedicated `stuck_unregistered_handlers` accessor is no longer
needed.

## Part 2 — Per-handler `concurrency_limit`

A new per-handler knob caps how many handler invocations of a given
`handler_id` run concurrently — e.g. a handler hitting a rate-limited
external API, or a heavy handler that must not be flooded.

### API

`HandlerOptions::concurrency_limit(u32)` — mirrors the existing
`handler_timeout` knob: a private `Option<u32>` field, a `const fn`
setter, `#[must_use]`. `None` ⇒ unlimited (the handler is bounded only by
the global `OutboxConfig::concurrency`).

`RegisteredHandler` (`registry.rs`) and `PendingHandler` (`builder.rs`)
each gain a `concurrency_limit: Option<u32>` field, threaded through
`OutboxBuilder::register_handler`.

The `HandlerOptions` docstring is updated to describe both knobs.

### Validation (`OutboxBuilder::build`)

A configured limit must be `1..=i32::MAX`; `0` ⇒
`BuildError::ConfigInvalid` ("concurrency_limit must be >= 1"). There is
**no** cross-knob constraint against `OutboxConfig::concurrency` — `pgwq`
documents the two as independent axes (a per-key limit larger than the
worker-wide cap merely means the worker-wide cap binds first). This is
deliberately unlike `handler_timeout`, which has a ceiling against the
global value.

### Dispatch — stamping the concurrency key

`pg_work_queue` v0.1.4's `Pusher::push_batch` takes
`&[(T, Option<String>)]` — a `(payload, concurrency_key)` pair per item.
In `dispatch()`, for each `handler_id` in the fan-out the key is:

- `Some(handler_id.clone())` — if that handler has a `concurrency_limit`
  configured;
- `None` — otherwise.

Stamping a key **only** for limited handlers is deliberate: `pgwq`
maintains a second claim index (`jobs_claim_conc_idx`) that every
non-NULL-key job must update — roughly 2× claim-index write churn.
Unlimited handlers keep `None` and stay on the cheap single-index path.
`dispatch()` already iterates `handler_id`s and holds the `Registry`, so
the per-handler limit is available via `registry.lookup(handler_id)`.

### Start — wiring limits to the `pgwq` Worker

In `start()`, the `concurrency_limits` map for `WorkerBuilder` is built
from the registry: for every registered handler with
`concurrency_limit: Some(n)`, emit `(handler_id, n)`. `pgwq`'s
`WorkerBuilder::concurrency_limits` accepts any
`IntoIterator<Item = (String, u32)>`.

### Key identity

`pgwq`'s `concurrency_key` is the `rust_events` `handler_id`, 1:1.
`MAX_HANDLER_ID_BYTES = 128`; `pgwq`'s key bound is 128 *characters*. A
string of ≤128 bytes always has ≤128 characters, so a valid `handler_id`
always satisfies `pgwq`'s bound — no additional length validation is
needed.

### Inherited semantics (`pgwq` v0.1.4)

The limit caps concurrent handler **tasks in this process**, gated at
claim time (a saturated key's jobs are simply not claimed — no
head-of-line blocking, no wasted lease). The counter is in-memory and
starts at zero after a crash; `running` rows left by a crashed previous
process are ghosts and are not counted. All of this is correct under the
single-instance model — the documented `N × limit` multi-instance caveat
does not apply.

## Part 3 — `pg_work_queue` v0.1.4 adaptation

`Cargo.toml` and `Cargo.lock` are already pinned to `v0.1.4`. The
remaining adaptation is fully covered by Part 2: the `push_batch` call
site in `outbox.rs` (now `&[(HandlerEnvelope, Option<String>)]`) and the
`.concurrency_limits(...)` call on the Worker builder in `start()`.
Nothing else in the v0.1.4 diff touches the API surface `rust_events`
consumes — reaper, fencing, and lease behavior are unchanged.

**Operational note (docs, not code):** the new `pgwq` migration
`20260521000000_v01_concurrency_key.sql` takes `ACCESS EXCLUSIVE` on
`pgwq.jobs` for its full duration (it builds two indexes
non-`CONCURRENTLY`). `rust_events` calls both migrators at startup. On a
queue table kept small by purging this is sub-second; on a large unpurged
table it is a read+write stall. Worth a line in the README/CLAUDE.md ops
notes.

## Testing

- `tests/rolling_deploy_handler_miss.rs` — repurposed to the
  single-instance handler-miss case: a job whose handler is no longer
  registered is marked `dead` on first claim.
- Tests referencing `strict_handler_lookup`, `resolve_attempts`, or
  `stuck_unregistered_handlers` are removed or adapted. The
  `schema_invariants` test is updated for the dropped columns.
- New test for per-key concurrency: register a handler with
  `concurrency_limit(1)`, dispatch several events of its type, assert the
  handler never runs two invocations concurrently (a shared barrier or
  overlap-detecting counter). `pgwq` tests the mechanism itself; this test
  verifies the `rust_events` wiring — key stamped on dispatch, limits
  passed to the Worker.
- `migrator_coexistence` is unaffected — `pgwq`'s new migration coexists
  via the shared `_sqlx_migrations` table with `set_ignore_missing(true)`.

## Documentation

- **README** — remove the multi-replica sections, the loose-mode
  explanation, and the "loose mode + exhausted attempts → stuck at
  `queued`" note; document `HandlerOptions::concurrency_limit`; state the
  single-instance deployment model explicitly.
- **CLAUDE.md** — remove the loose-mode bullet under "Things that look
  weird", the `strict_handler_lookup=false` default note; update step ①
  of the architecture diagram (loose retries gone); add per-key
  concurrency; record the single-instance model.
- **`builder.rs`** — drop the "Multi-replica" paragraph from the
  `HandlerOptions::handler_timeout` docstring.

## Versioning

Breaking changes: removed `strict_handler_lookup`, removed
`History::stuck_unregistered_handlers` / `StuckHandlerRow`, changed
handler-miss behavior. Pre-1.0, so permitted. Bump `0.3.0 → 0.4.0`
following the release checklist (version strings in README, `Cargo.lock`).
The merge/tag/push is a separate, user-driven step.
