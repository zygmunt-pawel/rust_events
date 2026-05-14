# Per-Handler `handler_timeout` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let each handler optionally override the global `handler_timeout` with its own (tighter) wall-clock budget, registered through a single, strongly-typed `register_handler` method.

**Architecture:** Add a `HandlerOptions` value type carrying optional per-handler overrides. `OutboxBuilder::register_handler` takes it as a required argument (the old 2-arg form is **removed** — breaking change). The override is threaded `PendingHandler → Registry::RegisteredHandler`, validated at `build()` (must be `> 2× HANDLER_CLEANUP_BUDGET` and `<= global handler_timeout` — the global is the hard ceiling because `pg_work_queue`'s single worker-wide outer cancellation uses it). At delivery time `runtime.rs::handle_envelope` reads `registered.handler_timeout.unwrap_or(config.handler_timeout)` when computing its internal `tokio::time::timeout`.

**Tech Stack:** Rust 1.88+, sqlx, tokio, `pg_work_queue`, testcontainers (PG 18). Lints: `unsafe_code` forbid; `unwrap_used`/`expect_used`/`panic` deny **crate-wide** (the `tests/common/mod.rs` opt-out only covers the integration-test crate — inline `#[cfg(test)]` modules in `src/` must carry their own `#[allow(...)]`, as `src/util.rs`, `src/handler.rs`, `src/dispatch_context.rs` already do). CI runs `cargo clippy --all-targets -- -D warnings`.

**Breaking change:** removing the 2-arg `register_handler` bumps the crate `0.2.0 → 0.3.0` (Task 5). No `CHANGELOG.md` exists (pre-publish crate); the migration recipe goes in the README and the Task 2 commit message.

**Task ordering rationale:** the registry refactor (Task 1) lands first so `RegisteredHandler` exists before `register_handler` threads a value into it (Task 2). `HandlerOptions` is introduced *together with* its only consumer (`register_handler`) in Task 2 — never committed as a type with an unread field, which would trip `dead_code` under `clippy -D warnings`.

---

## File Structure

| File | Change |
|--|--|
| `src/registry.rs` | New `RegisteredHandler` struct; `Registry.handlers` value type; `lookup` return type |
| `src/runtime.rs` | `handle_envelope` resolves `effective_timeout` and uses it for `our_timeout` |
| `src/builder.rs` | New `HandlerOptions` type; `PendingHandler` gains a field; `register_handler` signature; shared `handler_timeout_floor_check` helper; per-handler validation in `build()`; inline unit tests |
| `src/lib.rs` | Re-export `HandlerOptions`; fix module doctest call site |
| `tests/*.rs` (23 files) | Mechanical sweep: every `register_handler` call gains `HandlerOptions::new()` |
| `tests/builder_validation.rs` | New: 4 validation tests |
| `tests/handler_timeout_and_panic.rs` | New: 3 integration tests |
| `README.md` | Update 2 call sites; new "Per-handler timeout" subsection incl. multi-replica caveat + migration note |
| `docs/superpowers/specs/2026-05-13-rust-events-design.md` | Update the `register_handler` signature block + short prose |
| `Cargo.toml` | Version bump `0.2.0 → 0.3.0` |

---

## Task 1: Registry stores handler + override; runtime reads it

Internal refactor — no public API change, no behavior change. `RegisteredHandler` groups the handler with its (initially always `None`) timeout override; `runtime.rs` is wired to *read* it now (so the field is never dead code). Because every override is `None`, `effective_timeout` always equals the global value — the existing suite is the regression gate.

**Files:**
- Modify: `src/registry.rs` (new struct, `Registry.handlers` type, `lookup` signature)
- Modify: `src/builder.rs:255-284` (construct `RegisteredHandler` in `build()`)
- Modify: `src/runtime.rs:338-358` and `:380-384` (use `RegisteredHandler`, derive `effective_timeout`)

- [ ] **Step 1: Add `RegisteredHandler` and change `Registry` storage**

In `src/registry.rs`, change the imports line (line 4) to include `Duration`:

```rust
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};
```

Immediately before `pub(crate) struct Registry {` (~line 69), insert:

```rust
/// A registered handler together with its per-handler option overrides.
/// Stored as the value type of [`Registry::handlers`].
pub(crate) struct RegisteredHandler {
    /// The type-erased handler.
    pub(crate) handler: Arc<dyn ErasedHandler>,
    /// Per-handler `handler_timeout` override; `None` ⇒ use the global
    /// [`crate::builder::OutboxConfig`] `handler_timeout`.
    pub(crate) handler_timeout: Option<Duration>,
}
```

Change the `handlers` field type in `Registry`:

```rust
pub(crate) struct Registry {
    /// Primary lookup: `handler_id` → registered handler.
    pub(crate) handlers: HashMap<String, RegisteredHandler>,
    /// Secondary index: `event_type` → list of registered `handler_id`s.
    pub(crate) by_type: HashMap<&'static str, Vec<String>>,
}
```

Change the `lookup` return type (the body is unchanged — `HashMap::get` now yields `&RegisteredHandler`):

```rust
    /// Look up a handler by its stable `handler_id`.
    pub(crate) fn lookup(&self, handler_id: &str) -> Option<&RegisteredHandler> {
        self.handlers.get(handler_id)
    }
```

`Registry::new()` is unchanged (still constructs two empty `HashMap`s).

- [ ] **Step 2: Update `build()` to construct `RegisteredHandler`**

In `src/builder.rs`, change the import on line 6 to add `RegisteredHandler`:

```rust
use crate::registry::{ErasedHandler, RegisteredHandler, Registry, TypedHandler};
```

In `build()`, change the `handlers` map type (line 258):

```rust
        let mut handlers: HashMap<String, RegisteredHandler> = HashMap::new();
```

Change the insert (line 281) — `handler_timeout` is hard-coded `None` here (Task 2 threads the real value; this is a deliberate one-line stub for this TDD checkpoint):

```rust
            handlers.insert(
                entry.handler_id,
                RegisteredHandler {
                    handler: entry.handler,
                    handler_timeout: None,
                },
            );
```

- [ ] **Step 3: Update `runtime.rs` — use `RegisteredHandler`, derive `effective_timeout`**

In `src/runtime.rs`, replace the handler-resolution block (lines 338-358). **The `None`-arm body below is byte-for-byte the current `else` block — copy it verbatim, do not rewrite it.** Only the surrounding shape and the two new `let` lines are new:

```rust
        let registered = match self.registry.lookup(&env.handler_id) {
            Some(h) => h,
            None => {
                // strict_handler_lookup must be true here — loose mode returned
                // early in step ①. (Body unchanged from the previous `else`.)
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
            }
        };
        // Copy out owned values immediately. `registered` borrows
        // `self.registry`; do NOT reference it past these two lines.
        let handler = registered.handler.clone();
        // Per-handler override tightens (or matches) the global budget; `None`
        // ⇒ global. Validated `<= config.handler_timeout` at build(), so our
        // internal timeout always fires before pgwq's single worker-wide outer
        // timeout — which stays at the global value. Do NOT try to make pgwq's
        // outer timer per-handler: pgwq has one `handler_timeout` per Worker.
        let effective_timeout = registered
            .handler_timeout
            .unwrap_or(self.config.handler_timeout);
```

Then replace the `our_timeout` computation (lines 380-384):

```rust
        let our_timeout = effective_timeout
            .saturating_sub(HANDLER_CLEANUP_BUDGET)
            .max(Duration::from_millis(100));
```

Add `effective_timeout` to the timeout-warn tracing event (the `Err(_elapsed)` arm, ~lines 420-429) — insert one field (this is a log-event field, not a span field, so it does not touch the stable-span-fields contract):

```rust
            Err(_elapsed) => {
                tracing::warn!(
                    target: "rust_events.worker.handler_timeout",
                    event_id = %env.event_id,
                    handler_id = %env.handler_id,
                    attempt = ctx.attempt,
                    max_attempts = ctx.max_attempts,
                    effective_timeout = ?effective_timeout,
                    "handler exceeded handler_timeout; routing through mark_*_fenced"
                );
                HandlerOutcome::Handler(HandlerError::retry("handler_timeout"))
            }
```

- [ ] **Step 4: Verify compile, lints, and full existing suite (regression gate)**

Run: `cargo build --all-targets`
Expected: compiles, no errors.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean — no warnings (the `RegisteredHandler.handler_timeout` field IS read by `runtime.rs`, so no `dead_code`).

Run: `cargo test`
Expected: entire existing suite PASSES unchanged — every override is `None`, so `effective_timeout` always equals `config.handler_timeout`. Pay particular attention to `worker_happy_path`, `worker_retry`, `worker_abort`, `crash_recovery_fencing` (exercise the `Some(h)` resolution arm) and `rolling_deploy_handler_miss`, `no_handlers_strict` (exercise the `None` arm) — these cover the block you just restructured.

- [ ] **Step 5: Commit**

```bash
git add src/registry.rs src/builder.rs src/runtime.rs
git commit -m "refactor: registry stores RegisteredHandler with optional handler_timeout"
```

---

## Task 2: `HandlerOptions` type + new `register_handler` signature + sweep call sites

Introduce `HandlerOptions` **together with its only consumer** — the changed `register_handler` — so no commit ever lands a type with an unread field. `register_handler` gains a required `options: HandlerOptions` parameter; the old 2-arg form is **removed**. This breaks every call site in `tests/` and the `lib.rs` doctest; this task fixes them all in one sweep so the tree compiles again.

**Files:**
- Modify: `src/builder.rs` (`HandlerOptions` type + inline tests, `PendingHandler` struct, `register_handler` method)
- Modify: `src/lib.rs:7-11,39,83-85` (re-export + doctest import + doctest call site)
- Modify: all 23 test files listed below

- [ ] **Step 1: Write the failing inline unit tests**

Append to the end of `src/builder.rs` (the `#[allow(...)]` is **required** — the crate's deny-lints are crate-wide and `assert_eq!` expands to `panic!`; this mirrors `src/util.rs`/`src/handler.rs`/`src/dispatch_context.rs`):

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::HandlerOptions;
    use std::time::Duration;

    #[test]
    fn handler_options_records_timeout_override() {
        let o = HandlerOptions::new().handler_timeout(Duration::from_secs(180));
        assert_eq!(o.handler_timeout, Some(Duration::from_secs(180)));
    }

    #[test]
    fn handler_options_default_has_no_override() {
        assert_eq!(HandlerOptions::default().handler_timeout, None);
        assert_eq!(HandlerOptions::new().handler_timeout, None);
    }

    #[test]
    fn handler_options_last_timeout_wins() {
        let o = HandlerOptions::new()
            .handler_timeout(Duration::from_secs(1))
            .handler_timeout(Duration::from_secs(2));
        assert_eq!(o.handler_timeout, Some(Duration::from_secs(2)));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib builder::tests`
Expected: FAIL — `cannot find type/struct HandlerOptions`.

- [ ] **Step 3: Add the `HandlerOptions` type**

In `src/builder.rs`, immediately after the `DecodeStrategy` enum (the `}` at ~line 29), insert:

```rust
/// Per-handler registration options. Every field is optional; an unset field
/// falls back to the corresponding global [`OutboxConfig`] value.
///
/// This is a plain options value-bag, not a validating builder like
/// [`OutboxConfigBuilder`] — it has no `build()` and no cross-field rules
/// (per-handler bounds are checked against the global config at
/// [`OutboxBuilder::build`], which is the only place both values are known).
/// It still follows the crate's `const fn` setter / `#[must_use]` convention.
/// Currently the only knob is
/// [`handler_timeout`](HandlerOptions::handler_timeout).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandlerOptions {
    /// Per-handler `handler_timeout` override; `None` ⇒ use the global value.
    /// Private — only read inside this module (`register_handler`, `build`).
    handler_timeout: Option<Duration>,
}

impl HandlerOptions {
    /// Options with every field unset — behaves identically to the global config.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            handler_timeout: None,
        }
    }

    /// Override the wall-clock budget for a single invocation of *this*
    /// handler.
    ///
    /// Must be `> 2 × HANDLER_CLEANUP_BUDGET` (i.e. `> 400 ms`) and
    /// `<= OutboxConfig::handler_timeout`: the global timeout is a hard
    /// ceiling because it is the single value `pg_work_queue`'s worker-wide
    /// outer cancellation (and lease math) uses, so a per-handler value may
    /// only *match or tighten* the global budget. Validated at
    /// [`OutboxBuilder::build`]; a violation is [`BuildError::ConfigInvalid`].
    ///
    /// Note the effective handler budget is `d - HANDLER_CLEANUP_BUDGET`
    /// (≈ `d - 200 ms`): the crate reserves the tail of the window for its own
    /// `mark_*_fenced` audit write. A `d` near the 400 ms floor leaves a very
    /// small actual budget.
    #[must_use]
    pub const fn handler_timeout(mut self, d: Duration) -> Self {
        self.handler_timeout = Some(d);
        self
    }
}
```

In `src/lib.rs`, change the builder re-export (lines 83-85) to add `HandlerOptions`:

```rust
pub use crate::builder::{
    BackoffPolicy, DecodeStrategy, HandlerOptions, OutboxBuilder, OutboxConfig,
    OutboxConfigBuilder, PanicPolicy,
};
```

- [ ] **Step 4: Run the inline tests to verify they pass**

Run: `cargo test --lib builder::tests`
Expected: PASS (3 tests).

- [ ] **Step 5: Change `PendingHandler` and `register_handler`**

In `src/builder.rs`, add a field to the private `PendingHandler` struct (~lines 193-197):

```rust
struct PendingHandler {
    event_type: &'static str,
    handler_id: String,
    handler: Arc<dyn ErasedHandler>,
    handler_timeout: Option<Duration>,
}
```

Replace the `register_handler` method (~lines 218-237) with:

```rust
    /// Register a handler. Takes ownership of `handler` and wraps it in an
    /// `Arc<TypedHandler<E, H>>` internally — callers must **not** pre-wrap.
    ///
    /// `options` carries per-handler overrides (see [`HandlerOptions`]); pass
    /// `HandlerOptions::new()` for a handler that should use the global
    /// [`OutboxConfig`] verbatim.
    #[must_use]
    pub fn register_handler<E, H>(
        mut self,
        handler_id: impl Into<String>,
        handler: H,
        options: HandlerOptions,
    ) -> Self
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
            handler_timeout: options.handler_timeout,
        });
        self
    }
```

In `build()`, change the `RegisteredHandler` construction from Task 1 (`handler_timeout: None`) to thread the real value:

```rust
                    handler_timeout: entry.handler_timeout,
```

- [ ] **Step 6: Fix the `lib.rs` module doctest**

In `src/lib.rs`, add `HandlerOptions` to the doctest import block (lines 8-11):

```rust
//! use rust_events::{
//!     DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
//!     HandlerOptions, OutboxBuilder,
//! };
```

Change the call site (line 39):

```rust
//!     .register_handler::<OrderCreated, _>("audit", Auditor, HandlerOptions::new())
```

- [ ] **Step 7: Sweep all 23 test-file call sites**

For **each** file below: (a) add `HandlerOptions` to the `use rust_events::{...}` import list, (b) append `, HandlerOptions::new()` as the final argument of **every** `register_handler::<...>(...)` call in that file.

Example transformation:

```rust
// before
.register_handler::<Ev, _>("sleepy", SleepyHandler { delay: d })
// after
.register_handler::<Ev, _>("sleepy", SleepyHandler { delay: d }, HandlerOptions::new())
```

Multi-line calls get the new argument before the closing `)`:

```rust
// after
.register_handler::<Ev, _>(
    "sleepy",
    SleepyHandler { delay: d },
    HandlerOptions::new(),
)
```

Files (occurrence counts in parentheses):
`tests/start_retry_after_failure.rs` (1), `tests/purge_events_safety.rs` (2), `tests/builder_validation.rs` (5), `tests/dispatch_constraint_classification.rs` (1), `tests/handler_timeout_and_panic.rs` (6), `tests/decode_abort_not_swallowed.rs` (1), `tests/decode_error_strategy.rs` (2), `tests/handler_context_keys.rs` (3), `tests/loose_mode_resolve_tracking.rs` (2), `tests/double_start_rejected.rs` (1), `tests/audit_row_missing.rs` (1), `tests/aggregate_key.rs` (1), `tests/worker_retry.rs` (1), `tests/headers_too_large.rs` (2), `tests/event_type_validation.rs` (2), `tests/history_queries.rs` (2), `tests/rolling_deploy_handler_miss.rs` (3), `tests/dispatch_happy_path.rs` (2), `tests/worker_abort.rs` (1), `tests/worker_happy_path.rs` (1), `tests/worker_skip.rs` (1), `tests/redact_pii_in_last_error.rs` (1), `tests/crash_recovery_fencing.rs` (1).

- [ ] **Step 8: Verify the sweep is complete and the tree compiles**

Run: `cargo build --all-targets`
Expected: compiles, zero errors. **This is the authoritative completeness gate** — the 2-arg `register_handler` no longer exists, so any un-swept call site is a hard compile error.

Run (import smoke check — confirms each swept file references the symbol; not a substitute for the build gate above): `bash -c 'for f in $(grep -rl "register_handler" tests/); do grep -q "HandlerOptions" "$f" || echo "MISSING import?: $f"; done'`
Expected: no output.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

Run: `cargo test --doc`
Expected: PASS — the `lib.rs` module doctest compiles and runs.

- [ ] **Step 9: Run the full suite to confirm no behavior regression**

Run: `cargo test`
Expected: entire suite PASSES — `HandlerOptions::new()` carries no override, so behavior is identical to before.

- [ ] **Step 10: Commit**

```bash
git add src/builder.rs src/lib.rs tests/
git commit -m "$(cat <<'EOF'
feat!: register_handler takes HandlerOptions; remove 2-arg form

BREAKING CHANGE: `OutboxBuilder::register_handler` now requires a third
argument, `HandlerOptions`. Migration: pass `HandlerOptions::new()` to keep
the previous behavior, or `HandlerOptions::new().handler_timeout(d)` to give
a handler its own (tighter) timeout budget.
EOF
)"
```

---

## Task 3: `build()`-time validation of per-handler `handler_timeout`

A per-handler override must be `> 2 × HANDLER_CLEANUP_BUDGET` (the same floor as the global timeout — extracted into a shared helper to avoid duplicating the check) and `<= config.handler_timeout` (the global is the hard ceiling). Violations are `BuildError::ConfigInvalid` — consistent with how the *global* `handler_timeout` validation already reports.

**Files:**
- Modify: `src/builder.rs` (new `handler_timeout_floor_check` helper; `OutboxConfigBuilder::build()` uses it; `OutboxBuilder::build()` loop validation)
- Test: `tests/builder_validation.rs`

- [ ] **Step 1: Write the failing tests**

In `tests/builder_validation.rs`, add `HandlerOptions` to the `use rust_events::{...}` import list and add `use std::time::Duration;`. Append these four tests:

```rust
/// A per-handler `handler_timeout` larger than the global `OutboxConfig`
/// `handler_timeout` is rejected at `build()` — the global is the hard
/// ceiling (pgwq's worker-wide outer cancellation uses it), so a per-handler
/// value may only match or tighten it, never exceed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_handler_timeout_exceeding_global_rejected() {
    let (_c, pool) = common::pg_container().await;
    let cfg = OutboxConfig::builder()
        .handler_timeout(Duration::from_secs(10))
        .lease_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let err = OutboxBuilder::new(pool)
        .config(cfg)
        .register_handler::<E1, _>(
            "slow",
            H,
            HandlerOptions::new().handler_timeout(Duration::from_secs(20)),
        )
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(ref m) if m.contains("exceeds the global")),
        "expected ConfigInvalid about exceeding global, got {err:?}"
    );
}

/// A per-handler `handler_timeout` at or below `2 × HANDLER_CLEANUP_BUDGET`
/// (400 ms) is rejected — same floor as the global timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_handler_timeout_below_cleanup_budget_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>(
            "tiny",
            H,
            HandlerOptions::new().handler_timeout(Duration::from_millis(400)),
        )
        .build()
        .unwrap_err();
    assert!(
        matches!(err, BuildError::ConfigInvalid(ref m) if m.contains("HANDLER_CLEANUP_BUDGET")),
        "expected ConfigInvalid about cleanup budget, got {err:?}"
    );
}

/// A per-handler `handler_timeout` strictly inside `(400 ms, global)` builds fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_handler_timeout_within_global_accepted() {
    let (_c, pool) = common::pg_container().await;
    let cfg = OutboxConfig::builder()
        .handler_timeout(Duration::from_secs(10))
        .lease_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool)
        .config(cfg)
        .register_handler::<E1, _>(
            "fast",
            H,
            HandlerOptions::new().handler_timeout(Duration::from_secs(2)),
        )
        .build();
    assert!(
        outbox.is_ok(),
        "valid per-handler timeout must build: {outbox:?}"
    );
}

/// Boundary: a per-handler `handler_timeout` exactly EQUAL to the global is
/// accepted — `<=` is the correct ceiling (it is byte-identical to the
/// no-override path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_handler_timeout_equal_to_global_accepted() {
    let (_c, pool) = common::pg_container().await;
    let cfg = OutboxConfig::builder()
        .handler_timeout(Duration::from_secs(10))
        .lease_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool)
        .config(cfg)
        .register_handler::<E1, _>(
            "exact",
            H,
            HandlerOptions::new().handler_timeout(Duration::from_secs(10)),
        )
        .build();
    assert!(
        outbox.is_ok(),
        "per-handler timeout equal to global must build: {outbox:?}"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test builder_validation per_handler_timeout`
Expected: `per_handler_timeout_exceeding_global_rejected` and `per_handler_timeout_below_cleanup_budget_rejected` FAIL (no validation yet — `build()` returns `Ok`, `unwrap_err()` panics). The two `_accepted` tests already pass.

- [ ] **Step 3: Add the shared floor helper and the per-handler validation**

In `src/builder.rs`, add a free function (place it near the bottom of the file, before the `#[cfg(test)]` module):

```rust
/// Shared lower-bound check for any `handler_timeout` (global or per-handler):
/// it must exceed `2 × HANDLER_CLEANUP_BUDGET` so the crate's internal
/// `tokio::time::timeout` never collapses onto its 100 ms floor and always
/// reserves room for the `mark_*_fenced` audit write. `label` identifies the
/// source ("OutboxConfig" or a specific handler) in the error message.
fn handler_timeout_floor_check(d: Duration, label: &str) -> Result<(), BuildError> {
    let min = crate::runtime::HANDLER_CLEANUP_BUDGET * 2;
    if d <= min {
        return Err(BuildError::ConfigInvalid(format!(
            "{label}: handler_timeout {d:?} must be > {min:?} \
             (2× HANDLER_CLEANUP_BUDGET)"
        )));
    }
    Ok(())
}
```

In `OutboxConfigBuilder::build()`, replace the inline floor check (lines 171-177) with a call to the helper:

```rust
        handler_timeout_floor_check(self.cfg.handler_timeout, "OutboxConfig")?;
```

(The existing `concurrency`, `max_attempts`, and `handler_timeout >= lease_timeout` checks stay as-is. The existing tests `handler_timeout_below_cleanup_budget_rejected` / `handler_timeout_just_above_cleanup_budget_accepted` still pass — the helper's message still contains the `HANDLER_CLEANUP_BUDGET` substring and the `> 400 ms` boundary is unchanged.)

In `OutboxBuilder::build()`, inside the `for entry in self.pending` loop, immediately after the duplicate-id check block (the `if handlers.contains_key(...) { return Err(...); }`, ~line 276) and before the `by_type.entry(...)` push, insert:

```rust
            if let Some(ht) = entry.handler_timeout {
                handler_timeout_floor_check(ht, &format!("handler '{}'", entry.handler_id))?;
                if ht > config.handler_timeout {
                    return Err(BuildError::ConfigInvalid(format!(
                        "handler '{}': handler_timeout {ht:?} exceeds the global \
                         OutboxConfig handler_timeout {:?} — a per-handler timeout \
                         may only match or tighten the global budget, never exceed \
                         it (the global value, default 240s when .config(...) is \
                         not set, is what pg_work_queue's worker-wide outer \
                         cancellation enforces)",
                        entry.handler_id, config.handler_timeout
                    )));
                }
            }
```

(`config` is bound at the top of `build()` — `let config = self.config.unwrap_or_default();` at line 255, before the loop at line 261 — so it is in scope here and is the same binding.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test builder_validation`
Expected: all tests in the file PASS — the 4 new ones plus the pre-existing global-timeout ones.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/builder.rs tests/builder_validation.rs
git commit -m "feat: validate per-handler handler_timeout bounds at build()"
```

---

## Task 4: Integration tests — per-handler override behavior

Three container tests proving the feature end-to-end: (a) the override is **enforced** when exceeded, (b) it **does not interfere** with a handler that fits its tight budget, (c) it is resolved **per `handler_id`**, not globally. The implementation already landed (Tasks 1-3); these are acceptance tests. Test (a) discriminates the override path from the global path purely by *elapsed time* — no manual mutate-and-revert.

**Files:**
- Test: `tests/handler_timeout_and_panic.rs` (`SleepyHandler` and `Ev` already exist there)

- [ ] **Step 1: Write the three tests**

In `tests/handler_timeout_and_panic.rs`, add `HandlerOptions` to the `use rust_events::{...}` import list, and change `use std::time::Duration;` to `use std::time::{Duration, Instant};`. Append:

```rust
// ── per-handler handler_timeout override ─────────────────────────────────────

/// (a) Enforcement: a per-handler `handler_timeout` override is honored by
/// `handle_envelope`. Global timeout is 20 s; the handler gets a 1 s override
/// and sleeps 10 s. It terminalizes to `dead` in well under 7 s — proof the
/// 1 s override, not the 20 s global, drove cancellation: a single
/// global-driven attempt alone would take ~19.8 s. The elapsed-time assertion
/// IS the discriminator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_override_is_enforced() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20)) // global ceiling
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "sleepy",
            SleepyHandler {
                delay: Duration::from_secs(10),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
        )
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let started = Instant::now();
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Override path ≈ 2 × ~800 ms timeout + 100 ms backoff + claim latency
    // ≈ under 4 s. Poll up to 12 s so a slow container never false-FAILs the
    // status check; the `elapsed < 7s` assertion is what proves the override
    // (not the ~19.8 s global path) drove it.
    let mut final_status = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "dead" | "sent" | "skipped") {
            break;
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(final_status, "dead", "override-bounded handler must terminalize to dead");
    assert!(
        elapsed < Duration::from_secs(7),
        "1s per-handler override must terminalize well before the ~19.8s a \
         20s-global-driven path needs; elapsed {elapsed:?}"
    );

    let running: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='running'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(running, 0, "no audit row should remain in 'running'");

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// (b) Non-interference: a handler that finishes within its tight per-handler
/// budget reaches `sent`. 1 s override ⇒ ~800 ms effective budget; the handler
/// sleeps 200 ms. Proves `effective_timeout` is wired into the success path
/// and does not wrongly cancel a handler that fits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_override_allows_fast_handler() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>(
            "fast",
            SleepyHandler {
                delay: Duration::from_millis(200),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
        )
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut final_status = String::new();
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        final_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if matches!(final_status.as_str(), "sent" | "dead" | "skipped") {
            break;
        }
    }
    assert_eq!(
        final_status, "sent",
        "handler finishing within its per-handler budget must reach 'sent'"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}

/// (c) Per-`handler_id` resolution: two handlers on the SAME event type, one
/// with a tight override (times out → dead), one with no override (uses the
/// generous global → sent). Proves the timeout is resolved per handler, not
/// once globally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_handler_timeout_resolved_per_handler_id() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cfg = OutboxConfig::builder()
        .poll_interval(Duration::from_millis(100))
        .concurrency(1)
        .max_attempts(2)
        .lease_timeout(Duration::from_secs(30))
        .handler_timeout(Duration::from_secs(20))
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .build()
        .unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        // "tight": 1s override, sleeps 10s → must die.
        .register_handler::<Ev, _>(
            "tight",
            SleepyHandler {
                delay: Duration::from_secs(10),
            },
            HandlerOptions::new().handler_timeout(Duration::from_secs(1)),
        )
        // "loose": no override, uses the 20s global, sleeps 200ms → must send.
        .register_handler::<Ev, _>(
            "loose",
            SleepyHandler {
                delay: Duration::from_millis(200),
            },
            HandlerOptions::new(),
        )
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("t"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Poll until both deliveries are terminal (up to 12s).
    let mut tight_status = String::new();
    let mut loose_status = String::new();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        tight_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='tight'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        loose_status = sqlx::query_scalar::<_, String>(
            "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='loose'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let tight_done = matches!(tight_status.as_str(), "dead" | "sent" | "skipped");
        let loose_done = matches!(loose_status.as_str(), "dead" | "sent" | "skipped");
        if tight_done && loose_done {
            break;
        }
    }
    assert_eq!(
        tight_status, "dead",
        "handler with 1s override must die; got {tight_status}"
    );
    assert_eq!(
        loose_status, "sent",
        "handler with no override (20s global) must send; got {loose_status}"
    );

    let _ = handle.shutdown(Duration::from_secs(3)).await;
}
```

- [ ] **Step 2: Run the three tests to verify they pass**

Run: `cargo test --test handler_timeout_and_panic per_handler_timeout`
Expected: all 3 PASS.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/handler_timeout_and_panic.rs
git commit -m "test: per-handler handler_timeout override integration coverage"
```

---

## Task 5: Docs, version bump, final gates

**Files:**
- Modify: `README.md` (2 call sites + new subsection)
- Modify: `docs/superpowers/specs/2026-05-13-rust-events-design.md` (signature block + prose)
- Modify: `Cargo.toml` (version), `src/lib.rs:57` (`html_root_url`)

- [ ] **Step 1: Update `README.md`**

Read `README.md`. It has exactly **2** `register_handler` occurrences (around lines 133 and 317). For each, apply the Task 2 sweep transformation (add `HandlerOptions` to that example's imports if shown; append `, HandlerOptions::new()` to the call).

Then add a subsection near the existing `handler_timeout` / configuration discussion:

```markdown
### Per-handler timeout

`OutboxConfig::handler_timeout` is the global wall-clock budget for every
handler invocation. A handler may tighten that budget for itself by passing
`HandlerOptions::handler_timeout` at registration:

    .register_handler::<LlmClassify, _>(
        "bc2_llm",
        LlmClassifier,
        HandlerOptions::new().handler_timeout(Duration::from_secs(180)),
    )

The per-handler value may only **match or tighten** the global budget: it must
be `> 400 ms` and `<= OutboxConfig::handler_timeout`. The global value is a
hard ceiling because `pg_work_queue`'s worker-wide outer cancellation (and the
lease math) is configured with it — `rust_events` cannot extend a handler's
budget past what pgwq itself enforces. Set the global `handler_timeout` to your
*slowest* handler's needs and use per-handler overrides to hold faster handlers
to a tighter bound. A handler registered with `HandlerOptions::new()` (no
override) uses the global value unchanged.

**Multi-replica note:** the per-handler timeout is resolved from the registry
of whichever replica claims the job. If two replicas register the same
`handler_id` with *different* per-handler timeouts (a deployment skew /
misconfiguration), delivery behavior is at-least-once-safe but the effective
timeout for a given attempt is non-deterministic — whichever replica wins the
`FOR UPDATE SKIP LOCKED` claim decides. Keep `HandlerOptions` consistent across
replicas, the same way you keep `OutboxConfig` consistent.

**Migration from 0.2.x:** `register_handler` now takes a third argument. Pass
`HandlerOptions::new()` to preserve the previous behavior.
```

- [ ] **Step 2: Update the design spec**

In `docs/superpowers/specs/2026-05-13-rust-events-design.md`, locate the `register_handler<E, H>` **signature block** (~line 438) — it shows the old 2-arg signature and will be stale. Update that block to the new 3-arg signature, and add 2-3 sentences noting that `handler_timeout` can be overridden per handler via `HandlerOptions`, that the override is match-or-tighten only (bounded by the global, which is pgwq's worker-wide ceiling), and that it is validated at `build()`.

- [ ] **Step 3: Version bump**

In `Cargo.toml`, change `version = "0.2.0"` to `version = "0.3.0"`.
In `src/lib.rs` line 57, change `html_root_url = "https://docs.rs/rust_events/0.2.0"` to `.../0.3.0`.

- [ ] **Step 4: Final verification gates**

Run: `cargo build --all-targets`
Expected: compiles, zero warnings/errors.

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean — no warnings.

Run: `cargo test --doc`
Expected: PASS.

Run: `cargo test`
Expected: entire suite PASSES, including the 7 new tests (3 inline unit + 4 builder validation + 3 integration — note the inline `builder::tests` run under `cargo test --lib`).

- [ ] **Step 5: Commit**

```bash
git add README.md docs/superpowers/specs/2026-05-13-rust-events-design.md Cargo.toml src/lib.rs
git commit -m "docs: per-handler timeout README + spec; bump to 0.3.0"
```

---

## Self-Review

**Spec coverage:**
- Single strongly-typed registration method, old form removed → Task 2.
- `HandlerOptions` type, can't pass an ambiguous bare `Duration` → Task 2.
- Per-handler `handler_timeout` threaded to the runtime → Tasks 1-2.
- Match-or-tighten bound (`400 ms < d <= global`) enforced at build, with a shared floor helper → Task 3.
- Behavior proven end-to-end: enforcement, non-interference, per-`handler_id` resolution → Task 4.
- Docs (incl. multi-replica caveat + migration note) + version bump → Task 5.

**Placeholder scan:** none — every code step has complete code; the Task 2 sweep is a precise mechanical rule with an explicit file list, and `cargo build --all-targets` (not the grep) is its authoritative completeness gate.

**Type consistency:**
- `HandlerOptions { handler_timeout: Option<Duration> }` — private field, `derive(Debug, Clone, Default, PartialEq, Eq)`, `const fn new()`, `const fn handler_timeout()`. Introduced and consumed in Task 2.
- `RegisteredHandler { handler: Arc<dyn ErasedHandler>, handler_timeout: Option<Duration> }` — defined Task 1, constructed in `build()` (Task 1 with `None`, Task 2 with `entry.handler_timeout`), read in `runtime.rs` (Task 1).
- `Registry::lookup` returns `Option<&RegisteredHandler>` (Task 1) — both call sites updated: `runtime.rs:188` (`.is_none()`, unchanged) and the resolution block (`match`, Task 1).
- `register_handler(handler_id, handler, options)` arg order identical across Task 2, Task 3, Task 4 and all swept call sites.
- `handler_timeout_floor_check(d, label)` — defined Task 3, called by both `OutboxConfigBuilder::build()` and `OutboxBuilder::build()`.
- `effective_timeout` named consistently in `runtime.rs` (Task 1).

### Plan review

Validated against 9 parallel review agents (failure/rollback, race conditions, reinventing wheels, verbose design, API design, test coverage, correctness, completeness, lints). Caught and fixed before finalizing:

- **(High)** Inline `#[cfg(test)] mod tests` in `src/builder.rs` was missing `#[allow(clippy::unwrap_used, expect_used, panic)]` — the crate's deny-lints are crate-wide, `assert_eq!` expands to `panic!`, so `clippy -D warnings` would have failed. Added (matches existing inline test modules).
- **(High)** No success-path coverage for `effective_timeout` — a fast handler completing within its tight budget. Added integration test (b) `per_handler_timeout_override_allows_fast_handler`.
- **(Med)** `HandlerOptions` was committed in its own task with an unread private field → `dead_code` under `clippy -D warnings`, undiscovered for several tasks. Restructured: the type is now introduced *with* its consumer (`register_handler`) in one task.
- **(Med)** Task 2's runtime change rewrote the strict-mode `else` branch — transcription risk in a fenced-audit path. Plan now mandates the `None`-arm body be copied verbatim, and names the strict/loose-mode tests in the regression gate.
- **(Med)** Task 5's old "manually flip the override, confirm FAIL, revert" discrimination step risked committing a mutated test. Replaced with an `elapsed < 7s` time assertion that is self-discriminating — no manual edits.
- **(Med)** Per-handler floor check duplicated the global one — extracted `handler_timeout_floor_check`.
- **(Med)** Integration-test timing margins were thin and prose-asserted. Widened the global timeout to 20 s (bad path ~19.8 s vs. `elapsed < 7s` assertion) and added per-`handler_id` resolution coverage.
- **(Med)** Breaking change had no migration recipe — added to the README and the Task 2 commit message (no `CHANGELOG.md` exists in this pre-publish crate).
- **(Low)** `new()` made `const fn`; `HandlerOptions` derives `PartialEq, Eq`; doc/error wording aligned to "match or tighten" (the `<=` ceiling allows equality); error message no longer claims the misleading "same pgwq-headroom rationale" for the per-handler floor and notes the global may be the 240 s default; clippy added to every task's gate; spec signature block (not just prose) flagged for update; `==global` boundary test added.

Confirmed sound by the agents (no change needed): pgwq genuinely has no per-job timeout (the per-handler mechanism must live in `rust_events`); leaving pgwq's `Worker` config unchanged is correct; the `<=` ceiling (not `<`) is right; the `registered` borrow does not span an `.await`; `RegisteredHandler` is the right abstraction over a parallel map; task ordering has no forward dependencies.
