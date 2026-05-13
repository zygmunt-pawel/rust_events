# Post-review fixes (Critical + High) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the accepted Critical / High findings from the v0.1 code review: separate decode errors from `HandlerError::Abort`, make `Outbox::start()` retryable after failure, validate `EVENT_TYPE`, redact DB errors before they reach `pgwq.jobs.last_error`, document transaction discipline on `dispatch`, and silence the documented-as-unreachable `rsa` advisory.

**Architecture:** Six surgical fixes, one per finding. No public-API breakage: `HandlerError` stays the same (decode becomes a crate-internal variant of `HandlerOutcome`); `DispatchError` gains one new `#[non_exhaustive]` arm; `Outbox::start()` becomes idempotent under failure via an RAII drop-guard; worker DB error formatters route through a single `redact_db_error()` helper. Each task is TDD: failing test → fix → green → commit.

**Tech Stack:** Rust 2024, sqlx 0.8.6, pg_work_queue 0.1 (path), tokio 1.52, testcontainers PG18, proptest. No new dependencies.

**Source of truth:** Code-review analysis from session 2026-05-13 (findings #1, #2, #3, #4, #6, #8 accepted; #5, #7 rejected as premature / YAGNI).

---

## File structure

| File | Change | Why |
|---|---|---|
| `src/registry.rs` | Modify | Add `HandlerOutcome` enum; change `ErasedHandler::handle_erased` return type. |
| `src/runtime.rs` | Modify | Match on `HandlerOutcome` instead of string-prefix; route all `format!("{e}")` of `sqlx::Error` through `redact_db_error`. |
| `src/util.rs` | Modify | Add `redact_db_error` helper + unit test. |
| `src/outbox.rs` | Modify | Drop-guard for `started`; `EVENT_TYPE` validation; doc-warning on `dispatch`. |
| `src/error.rs` | Modify | Add `DispatchError::EventTypeInvalid { len, max }`. |
| `tests/decode_abort_not_swallowed.rs` | Create | TDD regression for #3 (handler-emitted `abort("decode-…")` must NOT be retried). |
| `tests/start_retry_after_failure.rs` | Create | TDD regression for #1 (`start()` after Err must not return `AlreadyStarted`). |
| `tests/event_type_validation.rs` | Create | TDD regression for #6 (empty / >128-byte `EVENT_TYPE` returns `EventTypeInvalid`). |
| `.cargo/audit.toml` | Create | Ignore `RUSTSEC-2023-0071` with reasoning (unreachable via mysql feature). |

No migrations. No new public types except one `DispatchError` arm.

---

## Task 1: #3 — Separate decode errors from `HandlerError::Abort`

**Why this first:** Highest-priority finding. Tight string coupling between `TypedHandler` and `OutboxRuntime` silently converts user-emitted `HandlerError::abort("decode …")` into retry. We refactor the internal contract; public `HandlerError` is unchanged.

**Files:**
- Create: `tests/decode_abort_not_swallowed.rs`
- Modify: `src/registry.rs`
- Modify: `src/runtime.rs:340-355`

- [ ] **Step 1.1: Write the failing test**

Create `tests/decode_abort_not_swallowed.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DecodeStrategy, DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Event with a name that legitimately contains "decode" — common in real codebases
/// (e.g. a service that processes encoded payloads).
#[derive(Serialize, Deserialize)]
struct DecodeRequest {
    blob_id: i64,
}
impl DomainEvent for DecodeRequest {
    const EVENT_TYPE: &'static str = "ingest.decode_request";
}

/// Handler that legitimately returns Abort with reason starting with "decode ".
/// Pre-fix: runtime.rs:345-355 silently converts this to Retry. The test fails
/// because attempts climbs and last_error never lands as permanent.
struct AbortingHandler;
#[async_trait::async_trait]
impl EventHandler<DecodeRequest> for AbortingHandler {
    async fn handle(
        &self,
        _event: &DecodeRequest,
        _ctx: &HandlerContext,
    ) -> Result<(), HandlerError> {
        Err(HandlerError::abort(
            "decode dxf failed: unsupported version tag",
        ))
    }
}

/// With DecodeStrategy::Retry (the default), a handler-emitted Abort whose reason
/// happens to start with "decode " must STILL be honored as terminal-dead on the
/// first attempt — NOT retried as if it were a JSON decode failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_abort_with_decode_prefix_is_not_retried() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(
            OutboxConfig::builder()
                .concurrency(1)
                .poll_interval(Duration::from_millis(100))
                .max_attempts(5)
                .decode_error_strategy(DecodeStrategy::Retry) // <-- the trigger
                .build()
                .unwrap(),
        )
        .register_handler::<DecodeRequest, _>("h", AbortingHandler)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme"),
            &DecodeRequest { blob_id: 1 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait up to 6 s for the row to land terminal.
    for _ in 0..30 {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status IN ('dead','sent','skipped')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if n == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status::text, attempts, last_error FROM outbox.handler_deliveries LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, "dead", "handler-emitted Abort must be terminal-dead, not retried");
    assert_eq!(row.1, 1, "Abort must NOT consume retry budget (attempts must be 1)");
    assert!(
        row.2.unwrap_or_default().contains("dxf"),
        "last_error must preserve the handler's reason"
    );

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 1.2: Run the test, verify it fails (current bug)**

```bash
cargo test --test decode_abort_not_swallowed -- --nocapture
```

Expected: FAIL. `status='dead'` but `attempts=5` (full budget consumed) — Abort was silently converted to Retry.

- [ ] **Step 1.3: Add `HandlerOutcome` enum in `src/registry.rs`**

Replace the `ErasedHandler` trait and its `TypedHandler` impl. Find this block in `src/registry.rs:12-49`:

```rust
#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError>;
}
```

Replace with:

```rust
/// Outcome of one type-erased handler invocation. `DecodeFailed` is a
/// crate-internal third state — it does NOT escape as a `HandlerError`,
/// so runtime can dispatch decode-strategy without string-matching reasons.
#[derive(Debug)]
pub(crate) enum HandlerOutcome {
    /// Handler returned `Ok(())`.
    Ok,
    /// Handler returned `Err(HandlerError::{Retry,Skip,Abort})`.
    Handler(HandlerError),
    /// Payload bytes failed to deserialize as the target event type. The
    /// string is the formatted `serde_json::Error` (no PII risk — payload
    /// bytes are user-controlled but the error message reflects parser
    /// position/expected tokens, not values).
    DecodeFailed(String),
}

#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    /// Deserialize `payload` as the concrete event type and invoke the
    /// underlying [`EventHandler`]. Returns a [`HandlerOutcome`] that lets
    /// the runtime distinguish decode failures from handler-emitted errors.
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> HandlerOutcome;
}
```

And replace the `TypedHandler` impl (currently lines 33-49):

```rust
#[async_trait::async_trait]
impl<E, H> ErasedHandler for TypedHandler<E, H>
where
    E: DomainEvent,
    H: EventHandler<E>,
{
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> HandlerOutcome {
        let event: E = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => {
                return HandlerOutcome::DecodeFailed(format!(
                    "decode {}: {e}",
                    E::EVENT_TYPE
                ));
            }
        };
        match self.inner.handle(&event, ctx).await {
            Ok(()) => HandlerOutcome::Ok,
            Err(err) => HandlerOutcome::Handler(err),
        }
    }
}
```

- [ ] **Step 1.4: Export `HandlerOutcome` for runtime use**

In `src/registry.rs` (no change to `pub` — `HandlerOutcome` stays crate-private).
In `src/runtime.rs`, find the import of `ErasedHandler` / `TypedHandler` (currently implicit via `self.registry.lookup`) and add `use crate::registry::HandlerOutcome;` at the top of the file alongside the existing imports.

- [ ] **Step 1.5: Rewrite the decision block in `src/runtime.rs`**

Find lines 340-355:

```rust
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
```

Replace with:

```rust
        // ⑤ Handler call via type-erased dispatch.
        let outcome = handler.handle_erased(&row.payload, &hctx).await;

        // ⑥ Translate decode failures via decode_error_strategy. Decode is a
        // crate-internal signal (HandlerOutcome::DecodeFailed) — never a
        // HandlerError variant — so handler-emitted Abort("decode-anything")
        // is honored verbatim as terminal-dead.
        let result: Result<(), HandlerError> = match (outcome, self.config.decode_error_strategy) {
            (HandlerOutcome::Ok, _) => Ok(()),
            (HandlerOutcome::Handler(err), _) => Err(err),
            (HandlerOutcome::DecodeFailed(reason), DecodeStrategy::Retry) => {
                Err(HandlerError::Retry {
                    reason,
                    retry_in: None,
                })
            }
            (HandlerOutcome::DecodeFailed(reason), DecodeStrategy::Abort) => {
                Err(HandlerError::Abort { reason })
            }
        };
```

- [ ] **Step 1.6: Run targeted tests**

```bash
cargo test --test decode_abort_not_swallowed
cargo test --test decode_error_strategy
```

Expected: BOTH pass. `decode_abort_not_swallowed::handler_abort_with_decode_prefix_is_not_retried` proves Abort survives. `decode_error_strategy::*` proves real decode failures still route through Retry/Abort strategy.

- [ ] **Step 1.7: Run the full worker / dispatch test set as a smoke check**

```bash
cargo test --test worker_happy_path --test worker_retry --test worker_abort --test worker_skip --test dispatch_happy_path
```

Expected: all pass.

- [ ] **Step 1.8: Commit**

```bash
git add tests/decode_abort_not_swallowed.rs src/registry.rs src/runtime.rs
git commit -m "$(cat <<'EOF'
post-review #3: separate decode failures from HandlerError::Abort

Introduce crate-private HandlerOutcome::{Ok, Handler, DecodeFailed}.
Runtime decides DecodeStrategy on the dedicated variant — no more
string-prefix matching on HandlerError::Abort::reason. Handler-emitted
Abort("decode …") is honored verbatim.

Regression test: tests/decode_abort_not_swallowed.rs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: #1 — Drop-guard for `Outbox::start()` flag

**Why second:** Standalone, no dependency on other tasks. After the fix `start()` is retry-safe across pgwq build/start failures.

**Files:**
- Create: `tests/start_retry_after_failure.rs`
- Modify: `src/outbox.rs:223-283`

- [ ] **Step 2.1: Write the failing test**

Create `tests/start_retry_after_failure.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder, StartError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ping;
impl DomainEvent for Ping {
    const EVENT_TYPE: &'static str = "test.ping";
}
struct H;
#[async_trait::async_trait]
impl EventHandler<Ping> for H {
    async fn handle(&self, _: &Ping, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// After a failed `start()` (here induced by a closed pool), a second `start()`
/// must NOT return `AlreadyStarted`. The drop-guard releases the `started` flag
/// on Err paths so callers can retry once the underlying cause is resolved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_after_failure_does_not_return_already_started() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ping, _>("h", H)
        .build()
        .unwrap();

    // Close the pool to force pgwq Worker::build/start to fail.
    pool.close().await;

    let first = outbox.start().await;
    assert!(first.is_err(), "first start must fail on closed pool");
    assert!(
        !matches!(first, Err(StartError::AlreadyStarted)),
        "first start must not surface as AlreadyStarted: {first:?}",
    );

    // The bug: pre-fix, `started` is permanently true → second start returns
    // AlreadyStarted regardless of underlying state. Post-fix, the drop-guard
    // releases the flag so we get the same underlying error again (or success,
    // if pool were recovered).
    let second = outbox.start().await;
    assert!(
        !matches!(second, Err(StartError::AlreadyStarted)),
        "second start must NOT return AlreadyStarted after first failure: {second:?}",
    );
}
```

- [ ] **Step 2.2: Run the failing test**

```bash
cargo test --test start_retry_after_failure -- --nocapture
```

Expected: FAIL — `second` is `Err(AlreadyStarted)`.

- [ ] **Step 2.3: Apply the drop-guard in `src/outbox.rs`**

Find the `start()` method (currently `src/outbox.rs:251-282`). Replace the body:

```rust
    pub async fn start(&self) -> Result<OutboxHandle, StartError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(StartError::AlreadyStarted);
        }

        // RAII: if any `?` below unwinds the stack, drop releases `started`
        // so the caller (or a supervising restart loop) can retry. On
        // success we `disarm()` and `started` stays `true` for the
        // process lifetime.
        let mut guard = StartedGuard::new(&self.started);

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

/// RAII guard that releases `Outbox::started` to `false` on Drop unless
/// explicitly disarmed before the success path. Used in `Outbox::start` to
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
```

Also update the existing doc-comment on `start()` (the lines starting with `/// First call starts the worker;`). The "TOCTOU" caveat about concurrent `start()` calls becomes false — callers can retry safely. Replace the doc-comment block immediately above `pub async fn start` with:

```rust
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
```

- [ ] **Step 2.4: Run the test**

```bash
cargo test --test start_retry_after_failure
```

Expected: PASS.

- [ ] **Step 2.5: Run the broader lifecycle tests as a smoke check**

```bash
cargo test --test concurrency
```

Expected: PASS (this test exercises a second `start()` after a first successful one — must still return `AlreadyStarted`).

- [ ] **Step 2.6: Commit**

```bash
git add tests/start_retry_after_failure.rs src/outbox.rs
git commit -m "$(cat <<'EOF'
post-review #1: drop-guard releases Outbox.started on start() failure

Pre-fix, swap(true) ran before fallible Worker::build/start. A failure
left started=true permanently, locking out retries with a misleading
AlreadyStarted on the next call. RAII StartedGuard now releases the
flag on unwinding; success path explicitly disarms.

Doc updated: drop the TOCTOU caveat, document retry semantics.

Regression test: tests/start_retry_after_failure.rs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: #6 — Validate `EVENT_TYPE` in `Outbox::dispatch()`

**Files:**
- Create: `tests/event_type_validation.rs`
- Modify: `src/error.rs` (add `DispatchError::EventTypeInvalid`)
- Modify: `src/outbox.rs:82-122` (insert validation step)

- [ ] **Step 3.1: Write the failing test**

Create `tests/event_type_validation.rs`:

```rust
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EmptyTypeEvent;
impl DomainEvent for EmptyTypeEvent {
    const EVENT_TYPE: &'static str = "";
}
struct NoopHandler;
#[async_trait::async_trait]
impl EventHandler<EmptyTypeEvent> for NoopHandler {
    async fn handle(&self, _: &EmptyTypeEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct LongTypeEvent;
impl DomainEvent for LongTypeEvent {
    // 129 bytes — one over the limit.
    const EVENT_TYPE: &'static str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
}
struct NoopHandler2;
#[async_trait::async_trait]
impl EventHandler<LongTypeEvent> for NoopHandler2 {
    async fn handle(&self, _: &LongTypeEvent, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_empty_event_type_rejected_in_rust() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<EmptyTypeEvent, _>("h", NoopHandler)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &EmptyTypeEvent)
        .await
        .expect_err("empty EVENT_TYPE must be rejected");

    assert!(
        matches!(
            err,
            DispatchError::EventTypeInvalid { len: 0, max: 128 }
        ),
        "expected EventTypeInvalid {{ len: 0, max: 128 }}, got: {err:?}",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_oversize_event_type_rejected_in_rust() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<LongTypeEvent, _>("h", NoopHandler2)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &LongTypeEvent)
        .await
        .expect_err("oversize EVENT_TYPE must be rejected before DB CHECK");

    assert!(
        matches!(
            err,
            DispatchError::EventTypeInvalid { len: 129, max: 128 }
        ),
        "expected EventTypeInvalid {{ len: 129, max: 128 }}, got: {err:?}",
    );
}
```

- [ ] **Step 3.2: Run the test, verify both fail**

```bash
cargo test --test event_type_validation
```

Expected: compilation FAIL — `DispatchError::EventTypeInvalid` does not exist yet.

- [ ] **Step 3.3: Add the error variant in `src/error.rs`**

Find `DispatchError` (currently `src/error.rs:38-104`). Insert a new variant between `ProducerBcTooLong` and `IdempotencyKeyInvalid` (keep the file ordered by where the validation runs in `dispatch()`):

```rust
    /// `E::EVENT_TYPE` byte length is 0 or exceeds the allowed maximum.
    /// Pre-emptive Rust-side check; DB CHECK on `outbox.events` would
    /// otherwise surface this as an opaque `DispatchError::Constraint`.
    #[error("event_type length {len} bytes not in 1..={max}")]
    EventTypeInvalid {
        /// Actual byte length (0 means empty).
        len: usize,
        /// Maximum allowed byte length.
        max: usize,
    },
```

- [ ] **Step 3.4: Add validation in `Outbox::dispatch()`**

In `src/outbox.rs`, find step 1 of `dispatch()` (lines 82-102) and insert a new check at the very top of the validation block (BEFORE the `tenant_id` check — `EVENT_TYPE` is the most identifying field, fail fastest):

```rust
        // 1. Validate inputs (early, no I/O).
        if E::EVENT_TYPE.is_empty() || E::EVENT_TYPE.len() > limits::MAX_EVENT_TYPE_BYTES {
            return Err(DispatchError::EventTypeInvalid {
                len: E::EVENT_TYPE.len(),
                max: limits::MAX_EVENT_TYPE_BYTES,
            });
        }
        if ctx.tenant_id().len() > limits::MAX_TENANT_BYTES {
            return Err(DispatchError::TenantIdTooLong {
                // … (existing check, unchanged)
```

Make sure `limits::MAX_EVENT_TYPE_BYTES` is in scope — it's already exported from `src/limits.rs` and `outbox.rs` already does `use crate::limits;`.

- [ ] **Step 3.5: Run the tests**

```bash
cargo test --test event_type_validation
```

Expected: both PASS.

- [ ] **Step 3.6: Run the broader dispatch tests as smoke check**

```bash
cargo test --test dispatch_happy_path --test builder_validation
```

Expected: PASS.

- [ ] **Step 3.7: Commit**

```bash
git add tests/event_type_validation.rs src/error.rs src/outbox.rs
git commit -m "$(cat <<'EOF'
post-review #6: validate EVENT_TYPE byte length in dispatch()

MAX_EVENT_TYPE_BYTES was a dead constant — the DB CHECK on
outbox.events was the only enforcement, surfacing as opaque
DispatchError::Constraint. Add a Rust-side check at dispatch entry
with a dedicated DispatchError::EventTypeInvalid arm.

#[non_exhaustive] on DispatchError makes the new variant additive
(no caller breakage). Empty tenant_id is intentionally left allowed
per README §Known-limitations §3.

Regression test: tests/event_type_validation.rs (empty + 129-byte).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: #4 — Redact DB errors before they reach `pgwq.jobs.last_error`

**Files:**
- Modify: `src/util.rs` (add `redact_db_error` + unit test)
- Modify: `src/runtime.rs:40, 66, 92, 118, 425-431` (apply the helper)

- [ ] **Step 4.1: Add the helper + unit test in `src/util.rs`**

In `src/util.rs`, after `is_pg_constraint_violation` (currently lines 27-35), add:

```rust
/// Format a `sqlx::Error` for inclusion in `pg_work_queue::JobError` messages
/// (which land in `pgwq.jobs.last_error` and operator logs). Strips Postgres
/// DETAIL lines so user-supplied PII (e.g. values of unique-constraint columns
/// like `idempotency_key`, `tenant_id`) cannot leak via fenced/transient error
/// paths. Pairs with `is_pg_constraint_violation` — same SQLSTATE classification.
#[allow(dead_code)]
pub fn redact_db_error(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Database(db) => {
            let code = db.code();
            let code = code.as_deref().unwrap_or("unknown");
            format!("db_error code={code}")
        }
        sqlx::Error::Io(_) => "db_error kind=io".into(),
        sqlx::Error::PoolTimedOut => "db_error kind=pool_timed_out".into(),
        sqlx::Error::PoolClosed => "db_error kind=pool_closed".into(),
        sqlx::Error::RowNotFound => "db_error kind=row_not_found".into(),
        sqlx::Error::Tls(_) => "db_error kind=tls".into(),
        sqlx::Error::Configuration(_) => "db_error kind=configuration".into(),
        _ => "db_error kind=other".into(),
    }
}
```

In the same file, add a unit test in the `#[cfg(test)] mod tests` block (currently around lines 52-101). Insert before the closing `}` of `mod tests`:

```rust
    #[test]
    fn redact_db_error_pool_closed() {
        let e = sqlx::Error::PoolClosed;
        let msg = redact_db_error(&e);
        assert_eq!(msg, "db_error kind=pool_closed");
        // The Display impl is unaffected; we just don't expose its contents.
        assert!(!msg.contains(&e.to_string()) || e.to_string().is_empty());
    }

    #[test]
    fn redact_db_error_pool_timed_out() {
        let msg = redact_db_error(&sqlx::Error::PoolTimedOut);
        assert_eq!(msg, "db_error kind=pool_timed_out");
    }

    #[test]
    fn redact_db_error_row_not_found() {
        let msg = redact_db_error(&sqlx::Error::RowNotFound);
        assert_eq!(msg, "db_error kind=row_not_found");
    }

    #[test]
    fn redact_db_error_no_pii_in_format() {
        // We can't construct sqlx::Error::Database synthetically without a backend,
        // but we can pin the contract: every variant we route through returns a
        // string that begins with "db_error " — operators can rely on this prefix
        // to distinguish redacted-DB-errors from handler-emitted reasons.
        for e in &[
            sqlx::Error::PoolClosed,
            sqlx::Error::PoolTimedOut,
            sqlx::Error::RowNotFound,
        ] {
            assert!(redact_db_error(e).starts_with("db_error "));
        }
    }
```

- [ ] **Step 4.2: Run the unit tests**

```bash
cargo test --lib util::tests
```

Expected: PASS.

- [ ] **Step 4.3: Apply the helper in `src/runtime.rs`**

Add the import at the top of `src/runtime.rs` (near the existing `use crate::util::truncate_utf8;`):

```rust
use crate::util::{is_pg_constraint_violation, parse_headers, redact_db_error, truncate_utf8};
```

(There's already a `use crate::util::{is_pg_constraint_violation, parse_headers};` lower in the file — merge them or just add `redact_db_error` where it lives. Either is fine.)

Then replace five `format!(... {e})` sites:

**Site 1** — `mark_sent_fenced` (currently line 40):

```rust
// before:
.map_err(|e| pg_work_queue::JobError::retry(format!("mark_sent: {e}")))?;
// after:
.map_err(|e| pg_work_queue::JobError::retry(format!("mark_sent: {}", redact_db_error(&e))))?;
```

**Site 2** — `mark_awaiting_retry_fenced` (currently line 66):

```rust
.map_err(|e| pg_work_queue::JobError::retry(format!("mark_retry: {}", redact_db_error(&e))))?;
```

**Site 3** — `mark_dead_fenced` (currently line 92):

```rust
.map_err(|e| pg_work_queue::JobError::retry(format!("mark_dead: {}", redact_db_error(&e))))?;
```

**Site 4** — `mark_skipped_fenced` (currently line 118):

```rust
.map_err(|e| pg_work_queue::JobError::retry(format!("mark_skipped: {}", redact_db_error(&e))))?;
```

**Site 5** — `map_sql` (currently lines 425-431):

```rust
fn map_sql(e: &sqlx::Error, ctx: &str) -> pg_work_queue::JobError {
    if is_pg_constraint_violation(e) {
        pg_work_queue::JobError::abort(format!("{ctx}: constraint violation: {}", redact_db_error(e)))
    } else {
        pg_work_queue::JobError::retry(format!("{ctx}: {}", redact_db_error(e)))
    }
}
```

- [ ] **Step 4.4: Verify with grep that no raw `{e}` formats remain in runtime.rs paths that feed `JobError`**

```bash
grep -nE 'JobError::(retry|abort)\(.*\{e\}' src/runtime.rs
```

Expected: NO MATCHES (empty output).

- [ ] **Step 4.5: Run the worker test suite as smoke check**

```bash
cargo test --test worker_happy_path --test worker_retry --test worker_abort --test worker_skip --test crash_recovery_fencing --test decode_error_strategy --test decode_abort_not_swallowed
```

Expected: ALL PASS. (`decode_error_strategy` includes the `last_error` assertion `contains("decode")` — that still passes because `HandlerError::Abort { reason }` reason is set by `mark_dead_fenced`'s `reason` parameter, NOT by `redact_db_error`. Verify by re-reading the assertion if test fails.)

- [ ] **Step 4.6: Commit**

```bash
git add src/util.rs src/runtime.rs
git commit -m "$(cat <<'EOF'
post-review #4: redact sqlx::Error before it lands in pgwq.jobs.last_error

Worker DB error paths formatted the raw sqlx::Error via Display, which
includes Postgres DETAIL lines containing constraint-column values
(idempotency_key, tenant_id). Add redact_db_error() that emits
"db_error code=<sqlstate>" / "db_error kind=<variant>" only.

Applied to mark_sent_fenced, mark_awaiting_retry_fenced,
mark_dead_fenced, mark_skipped_fenced, and map_sql — every site
whose error string feeds JobError → pgwq.jobs.last_error.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: #2 — Doc warning: rollback on Err

**Files:**
- Modify: `src/outbox.rs:55-65` (the `dispatch()` doc-comment)

- [ ] **Step 5.1: Replace the doc-comment on `Outbox::dispatch`**

Find the existing doc-comment immediately above `pub async fn dispatch` (currently `src/outbox.rs:55-65`):

```rust
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
```

Replace with:

```rust
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
```

- [ ] **Step 5.2: Verify the docs build cleanly**

```bash
cargo doc --no-deps 2>&1 | tail -20
```

Expected: no warnings about broken intra-doc links.

- [ ] **Step 5.3: Commit**

```bash
git add src/outbox.rs
git commit -m "$(cat <<'EOF'
post-review #2: doc transaction-discipline on Outbox::dispatch Err path

On Err, caller must roll back tx. Committing anyway can leak queued
handler_deliveries rows without a corresponding pgwq job — they never
deliver. Documented; no helper API added (operators can SELECT
WHERE status='queued' AND created_at < cutoff for reconciliation).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: #8 — Ignore RUSTSEC-2023-0071 (`rsa` unreachable via sqlx-mysql)

**Why last:** Pure config, no test. Verified pre-plan that `rsa 0.9.10` is in `Cargo.lock` via `sqlx-mysql 0.8.6` (which is built by `sqlx-macros-core` even when the `mysql` feature is disabled, because cargo features are additive at the workspace level).

**Files:**
- Create: `.cargo/audit.toml`

- [ ] **Step 6.1: Verify the current state**

```bash
cargo audit 2>&1 | grep -E '^(Crate|Title|ID|error)'
```

Expected: `Crate: rsa`, `ID: RUSTSEC-2023-0071`, `error: 1 vulnerability found!`.

- [ ] **Step 6.2: Create `.cargo/audit.toml`**

```toml
# cargo-audit configuration for rust_events.
# Re-evaluate quarterly; remove entries once upstream fixes are available.

[advisories]
ignore = [
    # RUSTSEC-2023-0071: Marvin Attack (timing sidechannel in `rsa` crate).
    # Reachable only via `sqlx-mysql`, which is pulled into the dependency
    # graph by `sqlx-macros-core` (proc-macro crate) but NOT compiled into
    # our binary — we enable only the `postgres` feature of `sqlx`:
    #
    #     sqlx = { version = "=0.8.6", default-features = false,
    #              features = ["postgres", ...], ... }
    #
    # The vulnerable RSA code path is unreachable from rust_events. No
    # upstream fix is published yet (verified 2026-05-13). Track the
    # `rsa` crate for a release with a constant-time implementation.
    "RUSTSEC-2023-0071",
]
```

- [ ] **Step 6.3: Verify `cargo audit` now passes**

```bash
cargo audit 2>&1 | tail -10
```

Expected: `Success` or `0 vulnerabilities` (no `error:` line). The advisory should appear under "Ignored Warnings" with the comment context.

- [ ] **Step 6.4: Commit**

```bash
git add .cargo/audit.toml
git commit -m "$(cat <<'EOF'
post-review #8: ignore RUSTSEC-2023-0071 (rsa unreachable via sqlx-mysql)

cargo audit flags rsa 0.9.10 via sqlx-mysql → sqlx-macros-core. We
enable only the postgres feature of sqlx; the RSA timing-sidechannel
code is unreachable from our binary. Document the reasoning in
.cargo/audit.toml; re-evaluate quarterly.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Final acceptance

- [ ] **Step 7.1: Run the full test suite**

```bash
cargo test --all-targets
```

Expected: all tests PASS (existing + 3 new integration tests + new unit tests). Note: full suite uses testcontainers — runs ~3-10 min.

- [ ] **Step 7.2: Run clippy with deny-warnings**

```bash
cargo clippy --all-targets -- -D warnings
```

Expected: clean (no warnings, no errors). The crate-level lint config in `Cargo.toml` already denies `unwrap_used`, `expect_used`, `panic` outside `#[cfg(test)]`.

- [ ] **Step 7.3: Run doctests**

```bash
cargo test --doc
```

Expected: PASS.

- [ ] **Step 7.4: Run `cargo audit` one more time**

```bash
cargo audit
```

Expected: 0 vulnerabilities (RUSTSEC-2023-0071 listed under ignored).

- [ ] **Step 7.5: Summary commit (optional)**

If anything was missed and amended above, no extra commit needed. Otherwise this is the end state — six review-driven commits on top of `feat/v0.1-impl`.

---

## Self-review notes

Coverage check against accepted findings:

| Finding | Task | Verified |
|---|---|---|
| #1 start() locked | Task 2 | ✓ test asserts second start() != AlreadyStarted |
| #2 orphaned deliveries on commit-on-Err | Task 5 | ✓ doc-only as agreed (no helper API) |
| #3 decode → retry magic string | Task 1 | ✓ test asserts handler Abort("decode-…") → dead, attempts=1 |
| #4 PII leak in last_error | Task 4 | ✓ helper + unit test + 5 call-sites converted |
| #6 EVENT_TYPE validation | Task 3 | ✓ test asserts empty + 129-byte → EventTypeInvalid (Rust-side, not DB CHECK) |
| #8 RUSTSEC-2023-0071 | Task 6 | ✓ `.cargo/audit.toml` with doc-comment, verified silenced |

Rejected findings:
- #5 (3 RTT in dispatch): premature optimization for v0.1; defer to v0.2 with benchmark.
- #7 (HandlerEnvelope clones): micro-optimization, YAGNI.

Out of scope (separate Medium-priority follow-up plan if you want):
- #9–#22 (Średnie). Several are valid (#11 race with purge, #14 headers cap, #16 partial index, #19 sanity caps); some are doc-only (#12, #18); some debatable (#9 take-over warn, #13 charset).
