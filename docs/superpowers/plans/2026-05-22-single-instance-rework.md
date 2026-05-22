# Single-instance rework + per-key concurrency — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework `rust_events` from a multi-replica design to single-instance-only, adapt to `pg_work_queue` v0.1.4, and add a per-handler `concurrency_limit` knob.

**Architecture:** Remove the loose handler-lookup subsystem (the crate's only multi-replica machinery); keep all crash-recovery machinery (fencing, reaper, lease, handler wrap). Adopt the v0.1.4 `Pusher`/`WorkerBuilder` API. Add `HandlerOptions::concurrency_limit`, stamp the `handler_id` as `pgwq`'s `concurrency_key` for limited handlers only, and feed the limits to the `pgwq` Worker.

**Tech Stack:** Rust 1.88+, `sqlx` (Postgres), `pg_work_queue` v0.1.4, `tokio`, `testcontainers` (PG 18 per test).

**Spec:** `docs/superpowers/specs/2026-05-22-single-instance-rework-design.md`

**Preconditions:** `Cargo.toml` and `Cargo.lock` are already pinned to `pg_work_queue` v0.1.4. The build is currently **broken** (the `push_batch` signature changed); Task 1 restores it. Work happens on branch `feat/single-instance-rework`.

**Conventions:** `cargo clippy --all-targets -- -D warnings` must stay clean. Integration tests under `tests/` require Docker (PG 18 container per test). Unit tests (`cargo test --lib`) do not.

---

### Task 1: Restore the build — adapt `push_batch` to v0.1.4

`pg_work_queue` v0.1.4 changed `Pusher::push_batch` from `&[T]` to
`&[(T, Option<String>)]` (payload + optional per-job `concurrency_key`).
This task restores compilation by passing `None` for every job; Task 7
replaces `None` with the real key.

**Files:**
- Modify: `src/outbox.rs` (the push step in `dispatch`, currently around lines 269-279)

- [ ] **Step 1: Verify the build currently fails**

Run: `CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --lib`
Expected: FAIL — `error[E0308]: mismatched types` at `src/outbox.rs`, `expected &[(_, Option<String>)]`.

- [ ] **Step 2: Change the push step to the tuple shape**

In `src/outbox.rs`, replace the push block in `dispatch`:

```rust
        // 9. Push N jobs to pg_work_queue. The per-job concurrency key is
        //    `None` here; Task 7 stamps handler_id for limited handlers.
        let envelopes: Vec<(HandlerEnvelope, Option<String>)> = handler_ids
            .iter()
            .map(|hid| {
                (
                    HandlerEnvelope {
                        event_id,
                        handler_id: hid.clone(),
                    },
                    None,
                )
            })
            .collect();
        pg_work_queue::Pusher::new(PGWQ_QUEUE)
            .push_batch(tx, &envelopes)
            .await?;
```

- [ ] **Step 3: Verify the build passes**

Run: `cargo build --all-targets`
Expected: PASS (warnings about unused `strict_handler_lookup` etc. are acceptable; clippy is checked later).

- [ ] **Step 4: Commit**

```bash
git add src/outbox.rs
git commit -m "fix: adapt push_batch call to pg_work_queue v0.1.4 tuple API"
```

---

### Task 2: Remove loose handler-lookup mode

Loose mode is the crate's only multi-replica subsystem. Remove the
`strict_handler_lookup` knob and the loose branch in the worker; the
existing strict path (registry-miss → `mark_dead_fenced`) becomes the only
behavior. Delete the loose-mode test and rewrite the rolling-deploy test.

**Files:**
- Modify: `src/runtime.rs` (step ① of `handle_envelope`, around lines 179-225; the step ③b comment around line 339)
- Modify: `src/builder.rs` (`OutboxConfig` struct, `Default` impl, `strict_handler_lookup` setter)
- Delete: `tests/loose_mode_resolve_tracking.rs`
- Replace: `tests/rolling_deploy_handler_miss.rs` → `tests/handler_removed_marks_dead.rs`

- [ ] **Step 1: Delete the loose branch in `runtime.rs`**

In `src/runtime.rs::handle_envelope`, delete the entire step ① block — the
comment paragraph beginning `// ① Registry lookup BEFORE touching the
audit row` through the closing `}` of the
`if self.registry.lookup(&env.handler_id).is_none() && !self.config.strict_handler_lookup { ... }`
statement (including the `UPDATE … resolve_attempts` query, both
`tracing::warn!` calls, and the `return Err(JobError::retry(...))`). The
next code after deletion is the step ② comment (`// ② Atomic transition …`).

- [ ] **Step 2: Update the step ③b comment in `runtime.rs`**

The deferred registry check (`let Some(registered) = self.registry.lookup(&env.handler_id) else { … }`) is unchanged. Replace its stale inner comment:

```rust
        // ③b Registry miss. The job's handler_id is not registered in this
        //     process — a permanent fault (the handler was removed by a
        //     deploy). The row is now 'running' (CTE updated it in step ②),
        //     so mark_dead_fenced's WHERE `status='running' AND
        //     lease_token=$token` can match. Mark it dead.
        let Some(registered) = self.registry.lookup(&env.handler_id) else {
```

Also update the `tracing::error!` message inside that block from `"handler not in registry (strict mode) → mark_dead"` to `"handler not in registry → mark_dead"`, and the `mark_dead_fenced` reason / `JobError::abort` strings from `"handler not in registry (strict mode)"` / `"handler not registered (strict mode)"` to `"handler not registered"`.

- [ ] **Step 3: Remove `strict_handler_lookup` from `builder.rs`**

In `src/builder.rs`:
- Delete the field `pub(crate) strict_handler_lookup: bool,` from `struct OutboxConfig`.
- Delete the line `strict_handler_lookup: false,` from the `Default for OutboxConfig` impl.
- Delete the entire `strict_handler_lookup` setter method on `OutboxConfigBuilder` (the doc comment, `#[must_use]`, and `pub const fn strict_handler_lookup(...)`).

- [ ] **Step 4: Verify the build passes**

Run: `cargo build --all-targets`
Expected: PASS. If a compile error names `strict_handler_lookup` elsewhere, that reference must also be removed.

- [ ] **Step 5: Delete the loose-mode test**

```bash
git rm tests/loose_mode_resolve_tracking.rs
```

- [ ] **Step 6: Replace the rolling-deploy test with a handler-removed test**

```bash
git rm tests/rolling_deploy_handler_miss.rs
```

Create `tests/handler_removed_marks_dead.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DeliveryStatus, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.handler_removed";
}

struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Single-instance handler-miss: a job is dispatched while handler "h" is
/// registered, then a worker is started WITHOUT "h" (simulating a deploy
/// that removed the handler). The delivery must be marked `dead` on first
/// claim — there is no other replica to pick it up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_removed_delivery_marked_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Dispatcher: handler "h" registered so dispatch() creates the
    // handler_deliveries row + pgwq job.
    let dispatcher = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H, HandlerOptions::new())
        .build()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    let ctx = DispatchContext::new("default");
    let event_id = match dispatcher
        .dispatch(&mut tx, &ctx, &Ev)
        .await
        .unwrap()
    {
        rust_events::DispatchOutcome::Dispatched { event_id, .. } => event_id,
        other => panic!("expected Dispatched, got {other:?}"),
    };
    tx.commit().await.unwrap();

    // Worker WITHOUT handler "h" — allow_no_handlers(true) so build()
    // succeeds with an empty registry.
    let worker = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();
    let handle = worker.start().await.unwrap();

    // Poll until the delivery reaches a terminal state.
    let history = dispatcher.history();
    let mut status = None;
    for _ in 0..50 {
        let rows = history.deliveries_for_event(event_id).await.unwrap();
        if let Some(r) = rows.first() {
            if matches!(r.status, DeliveryStatus::Dead) {
                status = Some(r.status);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    handle.shutdown().await;

    assert_eq!(
        status,
        Some(DeliveryStatus::Dead),
        "handler removed across deploy must mark the delivery dead"
    );
}
```

> Note: confirm the exact `History` accessor name for per-event delivery rows during execution (`deliveries_for_event` is the expected name — adjust to the real method on `History` if it differs) and the `DispatchOutcome` variant shape.

- [ ] **Step 7: Run the new test**

Run: `cargo test --test handler_removed_marks_dead`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: remove loose handler-lookup mode (single-instance)"
```

---

### Task 3: Remove `History::stuck_unregistered_handlers`

The `stuck_unregistered_handlers` accessor exists only to surface
loose-mode stuck rows. With loose mode gone it has no purpose, and it
queries the `resolve_attempts` column that Task 4 drops — so it must be
removed first.

**Files:**
- Modify: `src/history.rs` (remove `StuckHandlerRow` struct and `stuck_unregistered_handlers` method)
- Modify: `src/lib.rs` (only if `StuckHandlerRow` is re-exported there)

- [ ] **Step 1: Remove the struct and method from `history.rs`**

In `src/history.rs`:
- Delete the `StuckHandlerRow` struct (the doc comment, `#[derive(...)]`, `#[non_exhaustive]`, and the full `pub struct StuckHandlerRow { ... }`).
- Delete the `stuck_unregistered_handlers` method on `History` in full (doc comment, `#[tracing::instrument(...)]`, and the `pub async fn stuck_unregistered_handlers(...) -> Result<Vec<StuckHandlerRow>, HistoryError> { ... }` body).

- [ ] **Step 2: Remove the re-export if present**

In `src/lib.rs`, the line `pub use crate::history::{DeliveryStatus, EventRecord, HandlerDeliveryRecord, History};` does not currently list `StuckHandlerRow`. If a `StuckHandlerRow` re-export exists anywhere in `lib.rs`, remove it. Otherwise no change.

- [ ] **Step 3: Verify the build passes**

Run: `cargo build --all-targets`
Expected: PASS. Any remaining reference to `StuckHandlerRow` or `stuck_unregistered_handlers` (e.g. a doctest) must be removed.

- [ ] **Step 4: Commit**

```bash
git add src/history.rs src/lib.rs
git commit -m "refactor: remove History::stuck_unregistered_handlers (loose-mode only)"
```

---

### Task 4: Drop the `resolve_attempts` columns from the migration

Edit the single init migration **in place** (no new migration — the crate
is pre-publish, has no deployed databases, and tests spin fresh
containers).

**Files:**
- Modify: `migrations/20260513000001_v01_outbox_init.sql` (the `handler_deliveries` table)

- [ ] **Step 1: Remove the columns and comment**

In `migrations/20260513000001_v01_outbox_init.sql`, inside `CREATE TABLE outbox.handler_deliveries`, delete this comment-and-columns block:

```sql
    -- Loose-mode handler-lookup counter. Bumped each time a worker claims a
    -- job whose handler_id is not in its in-memory registry (loose mode only;
    -- strict mode dead-letters and never touches this column). Operators can
    -- alert on rows where resolve_attempts > N to detect undeployed handlers
    -- without depending on tracing-level retention.
    resolve_attempts        INTEGER     NOT NULL DEFAULT 0,
    last_resolve_attempt_at TIMESTAMPTZ,
```

- [ ] **Step 2: Remove the CHECK constraint**

In the same `CREATE TABLE`, delete the constraint:

```sql
    CONSTRAINT handler_deliveries_resolve_attempts_nonneg
        CHECK (resolve_attempts >= 0),
```

- [ ] **Step 3: Verify no SQL still references the dropped columns**

Run: `grep -rn "resolve_attempt" src/ migrations/`
Expected: no output. If anything matches, that SQL must be fixed (Tasks 2 and 3 should already have removed every reference).

- [ ] **Step 4: Run a migration-touching test to confirm the schema still builds**

Run: `cargo test --test schema_invariants`
Expected: PASS. If `schema_invariants.rs` asserts on the dropped columns, update those assertions (a `grep -n "resolve" tests/schema_invariants.rs` currently returns nothing, so no change is expected).

- [ ] **Step 5: Commit**

```bash
git add migrations/20260513000001_v01_outbox_init.sql
git commit -m "refactor: drop resolve_attempts columns from outbox schema"
```

---

### Task 5: Add the `HandlerOptions::concurrency_limit` knob

Add the per-handler `concurrency_limit` setter, mirroring the existing
`handler_timeout` knob. This task only adds the field and setter on
`HandlerOptions`; threading it into the registry is Task 6.

**Files:**
- Modify: `src/builder.rs` (`HandlerOptions` struct, `new`, new setter, docstring, unit tests)

- [ ] **Step 1: Write the failing unit tests**

In `src/builder.rs`, inside `#[cfg(test)] mod tests`, add:

```rust
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
        let o = HandlerOptions::new().concurrency_limit(1).concurrency_limit(8);
        assert_eq!(o.concurrency_limit, Some(8));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib builder::tests::handler_options_records_concurrency_limit`
Expected: FAIL — `no field concurrency_limit` / `no method named concurrency_limit`.

- [ ] **Step 3: Add the field and setter**

In `src/builder.rs`, add the field to `struct HandlerOptions`:

```rust
    /// Per-handler `handler_timeout` override; `None` ⇒ use the global value.
    /// Private — only read inside this module (`register_handler`, `build`).
    handler_timeout: Option<Duration>,
    /// Per-handler concurrency cap — at most this many invocations of this
    /// handler run at once. `None` ⇒ unbounded (only the global
    /// `OutboxConfig::concurrency` applies). Private — read in
    /// `register_handler` and `build`.
    concurrency_limit: Option<u32>,
```

Update `HandlerOptions::new()` to initialize it:

```rust
    #[must_use]
    pub const fn new() -> Self {
        Self {
            handler_timeout: None,
            concurrency_limit: None,
        }
    }
```

Add the setter (after the `handler_timeout` setter):

```rust
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
```

Update the `HandlerOptions` type-level docstring: replace "Currently the only knob is [`handler_timeout`](HandlerOptions::handler_timeout)." with a sentence naming both `handler_timeout` and `concurrency_limit`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --lib builder::tests`
Expected: PASS (all `handler_options_*` tests).

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs
git commit -m "feat: add HandlerOptions::concurrency_limit knob"
```

---

### Task 6: Thread `concurrency_limit` through the registry and `build()`

Carry the limit from `HandlerOptions` into `RegisteredHandler` via
`PendingHandler`, and validate it in `OutboxBuilder::build`.

**Files:**
- Modify: `src/registry.rs` (`RegisteredHandler` struct)
- Modify: `src/builder.rs` (`PendingHandler`, `register_handler`, `build`)
- Test: `tests/concurrency_limit_validation.rs` (new)

- [ ] **Step 1: Add the field to `RegisteredHandler`**

In `src/registry.rs`, add to `struct RegisteredHandler`:

```rust
pub(crate) struct RegisteredHandler {
    /// The type-erased handler.
    pub(crate) handler: Arc<dyn ErasedHandler>,
    /// Per-handler `handler_timeout` override; `None` ⇒ use the global value.
    pub(crate) handler_timeout: Option<Duration>,
    /// Per-handler concurrency cap; `None` ⇒ unbounded. Fed to
    /// `pg_work_queue`'s `WorkerBuilder::concurrency_limits` at `start()`.
    pub(crate) concurrency_limit: Option<u32>,
}
```

- [ ] **Step 2: Thread it through `builder.rs`**

In `src/builder.rs`:

Add the field to `struct PendingHandler`:

```rust
struct PendingHandler {
    event_type: &'static str,
    handler_id: String,
    handler: Arc<dyn ErasedHandler>,
    handler_timeout: Option<Duration>,
    concurrency_limit: Option<u32>,
}
```

In `register_handler`, set it when pushing the `PendingHandler`:

```rust
        self.pending.push(PendingHandler {
            event_type: E::EVENT_TYPE,
            handler_id: handler_id.into(),
            handler: erased,
            handler_timeout: options.handler_timeout,
            concurrency_limit: options.concurrency_limit,
        });
```

In `build`, where `RegisteredHandler` is constructed, add the field:

```rust
            handlers.insert(
                entry.handler_id,
                RegisteredHandler {
                    handler: entry.handler,
                    handler_timeout: entry.handler_timeout,
                    concurrency_limit: entry.concurrency_limit,
                },
            );
```

- [ ] **Step 3: Add `build()` validation**

In `build`, inside the per-entry loop, after the existing
`handler_timeout` validation block, add:

```rust
            if let Some(limit) = entry.concurrency_limit
                && (limit == 0 || limit > i32::MAX as u32)
            {
                return Err(BuildError::ConfigInvalid(format!(
                    "handler '{}': concurrency_limit must be in 1..=2147483647, \
                     got {limit}",
                    entry.handler_id
                )));
            }
```

- [ ] **Step 4: Write the validation test**

Create `tests/concurrency_limit_validation.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BuildError, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.conc_limit_validation";
}

struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// concurrency_limit(0) is rejected at build() with ConfigInvalid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_zero_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<Ev, _>("h", H, HandlerOptions::new().concurrency_limit(0))
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(_)),
        "expected ConfigInvalid, got {err:?}"
    );
}

/// A valid concurrency_limit builds successfully.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrency_limit_valid_builds() {
    let (_c, pool) = common::pg_container().await;
    OutboxBuilder::new(pool)
        .register_handler::<Ev, _>("h", H, HandlerOptions::new().concurrency_limit(4))
        .build()
        .expect("valid concurrency_limit must build");
}
```

> Note: confirm `BuildError` is re-exported from the crate root (`src/lib.rs`) — it is expected to be among the `error` re-exports. If `OutboxBuilder::build` does not need a live DB connection, the container is still used here to match the crate's test conventions.

- [ ] **Step 5: Run the tests**

Run: `cargo build --all-targets` then `cargo test --test concurrency_limit_validation`
Expected: build PASS; both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/registry.rs src/builder.rs tests/concurrency_limit_validation.rs
git commit -m "feat: thread concurrency_limit through registry and build() validation"
```

---

### Task 7: Stamp the concurrency key on dispatch and wire limits to the Worker

Replace the `None` placeholder from Task 1 with the real key
(`handler_id`, but only for handlers that have a `concurrency_limit`), and
pass the limits map to the `pg_work_queue` Worker in `start()`.

**Files:**
- Modify: `src/outbox.rs` (the push step in `dispatch`; the Worker builder chain in `start`)

- [ ] **Step 1: Stamp the key in `dispatch`**

In `src/outbox.rs`, replace the push block from Task 1 with:

```rust
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
```

- [ ] **Step 2: Build the limits map and pass it to the Worker in `start`**

In `src/outbox.rs::start`, before the `pg_work_queue::Worker::...::builder()`
chain, build the limits map:

```rust
        // Per-handler concurrency limits → pgwq's per-key concurrency.
        // Key = handler_id; only handlers with a configured limit appear.
        let concurrency_limits: Vec<(String, u32)> = self
            .registry
            .handlers
            .iter()
            .filter_map(|(id, h)| h.concurrency_limit.map(|n| (id.clone(), n)))
            .collect();
```

Add `.concurrency_limits(concurrency_limits)` to the builder chain (place
it next to `.concurrency(...)`):

```rust
            .concurrency(usize::try_from(self.config.concurrency).unwrap_or(usize::MAX))
            .concurrency_limits(concurrency_limits)
```

- [ ] **Step 3: Verify the build passes**

Run: `cargo build --all-targets`
Expected: PASS. If `WorkerBuilder::concurrency_limits` rejects the
argument type, adjust to the exact `IntoIterator<Item = (String, u32)>`
shape `pg_work_queue` v0.1.4 expects (confirm against the v0.1.4 docs).

- [ ] **Step 4: Run the existing dispatch/worker tests to confirm no regression**

Run: `cargo test --test handler_removed_marks_dead`
Expected: PASS (a handler with no limit still dispatches with a `None` key).

- [ ] **Step 5: Commit**

```bash
git add src/outbox.rs
git commit -m "feat: stamp per-key concurrency key and wire limits to the worker"
```

---

### Task 8: Per-key concurrency integration test

Verify the end-to-end wiring: a handler registered with
`concurrency_limit(1)` never runs two invocations at once.

**Files:**
- Test: `tests/per_handler_concurrency.rs` (new)

- [ ] **Step 1: Write the integration test**

Create `tests/per_handler_concurrency.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, HandlerOptions,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.per_handler_concurrency";
}

/// Handler that records the maximum observed concurrency. Each invocation
/// bumps a live counter, sleeps, and records the peak.
struct Probe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}
impl EventHandler<Ev> for Probe {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A handler registered with concurrency_limit(1) must never run two
/// invocations concurrently, even when many events of its type are queued
/// and the worker-wide concurrency is high.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_one_serializes_handler() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let probe = Probe {
        live: live.clone(),
        peak: peak.clone(),
    };

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>(
            "limited",
            probe,
            HandlerOptions::new().concurrency_limit(1),
        )
        .build()
        .unwrap();

    // Dispatch 6 events of the limited type.
    for _ in 0..6 {
        let mut tx = pool.begin().await.unwrap();
        let ctx = DispatchContext::new("default");
        outbox.dispatch(&mut tx, &ctx, &Ev).await.unwrap();
        tx.commit().await.unwrap();
    }

    let handle = outbox.start().await.unwrap();
    // 6 events × 300 ms serialized ≈ 1.8 s; allow generous headroom.
    tokio::time::sleep(Duration::from_secs(4)).await;
    handle.shutdown().await;

    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "concurrency_limit(1) must serialize the handler; observed peak > 1"
    );
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test --test per_handler_concurrency`
Expected: PASS — observed peak concurrency is exactly 1.

- [ ] **Step 3: Commit**

```bash
git add tests/per_handler_concurrency.rs
git commit -m "test: per-handler concurrency_limit end-to-end coverage"
```

---

### Task 9: Documentation and version bump

Update prose docs to the single-instance model, document the new knob,
and bump the version.

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `src/builder.rs` (`HandlerOptions::handler_timeout` docstring)
- Modify: `Cargo.toml`, `Cargo.lock`

- [ ] **Step 1: Update `README.md`**

- Remove the multi-replica sections, the loose-mode explanation, and the
  "loose mode + exhausted attempts → stuck at `queued`" note.
- Document `HandlerOptions::concurrency_limit` alongside `handler_timeout`.
- State the single-instance deployment model explicitly (one worker
  process, always).
- Add an operational note: `pg_work_queue`'s `20260521000000_v01_concurrency_key.sql`
  migration takes `ACCESS EXCLUSIVE` on `pgwq.jobs` for its duration;
  purge before migrating a large queue table.
- Update any `0.3.0` version string to `0.4.0`.

- [ ] **Step 2: Update `CLAUDE.md`**

- Remove the loose-mode bullet under "Things that look weird".
- Remove the `strict_handler_lookup=false` mention from "Defaults are
  conservative".
- Update step ① of the architecture diagram (loose retries are gone;
  registry miss now dead-letters in strict-only fashion).
- Add `concurrency_limit` to the `HandlerOptions` description.
- Record the single-instance deployment model.
- Update the `pg_work_queue` pin reference from the old tag to `v0.1.4`.

- [ ] **Step 3: Update the `handler_timeout` docstring**

In `src/builder.rs`, delete the "Multi-replica:" paragraph from the
`HandlerOptions::handler_timeout` doc comment.

- [ ] **Step 4: Bump the version**

In `Cargo.toml`, change `version = "0.3.0"` to `version = "0.4.0"`.

Run: `cargo build --lib`
Expected: PASS — this refreshes the `rust_events` entry in `Cargo.lock` to `0.4.0`.

- [ ] **Step 5: Verify the whole suite and clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS — no warnings.

Run: `cargo test`
Expected: PASS — full integration suite (Docker required).

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md src/builder.rs Cargo.toml Cargo.lock
git commit -m "docs: single-instance model + concurrency_limit; bump to 0.4.0"
```

---

## Self-review notes

- **Spec coverage:** Part 1 (remove multi-replica) → Tasks 2, 3, 4, 9.
  Part 2 (`concurrency_limit`) → Tasks 5, 6, 7, 8. Part 3 (v0.1.4
  adaptation) → Tasks 1, 7. Testing section → Tasks 2, 6, 8. Docs →
  Task 9. Versioning → Task 9.
- **Build stays green** after every task: Task 1 restores compilation
  before any other change; Tasks 2-4 remove loose-mode surface in
  dependency order (History accessor removed before its column is
  dropped).
- **Open items to confirm during execution** (flagged inline): the exact
  `History` accessor for per-event delivery rows; the `DispatchOutcome`
  variant shape; that `BuildError` is re-exported from the crate root;
  the precise `WorkerBuilder::concurrency_limits` argument type in
  `pg_work_queue` v0.1.4.
