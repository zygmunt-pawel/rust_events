# rust_events Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `rust_events` v0.1 — transactional outbox library for Rust services on Postgres, built on `pg_work_queue`'s public API.

**Architecture:** Thin layer over `pg_work_queue::{Pusher, Worker, JobContext, migrator}`. Three tables in `outbox.*` schema (immutable events log, mutable handler_deliveries audit with fencing-token mirroring, dispatch_keys idempotency reservations). One pg_work_queue queue (`outbox_handler_deliveries`). Type-erased handler registry, eager fanout at dispatch time in user's tx.

**Tech Stack:** Rust 1.85+, edition 2024, sqlx 0.8.6, tokio 1.52.3, pg_work_queue 0.1+ (commit `34c137d` or later), async-trait, thiserror, tracing. PG 18+ (uuidv7 native).

**Source of truth:** `docs/superpowers/specs/2026-05-13-rust-events-design.md` — every task references this spec; deviations require updating the spec first.

---

## File structure

### Source files (`src/`)

| File | Responsibility |
|---|---|
| `lib.rs` | Public re-exports, crate-level docs, doctest. |
| `limits.rs` | `MAX_*_BYTES` constants + `PURGE_CHUNK_SIZE`. |
| `util.rs` | `truncate_utf8`, `is_pg_constraint_violation`, `parse_headers`. |
| `error.rs` | All public error enums (`BuildError`, `DispatchError`, `HistoryError`, `PurgeError`, `StartError`, `ShutdownError`). |
| `handler.rs` | `DomainEvent` trait, `EventHandler<E>` trait, `HandlerContext`, `HandlerError`. |
| `dispatch_context.rs` | `DispatchContext<'a>` with builder methods (no `Default`). |
| `outcome.rs` | `DispatchOutcome`, `OutboxStats`. |
| `envelope.rs` | `HandlerEnvelope` (Serialize/Deserialize, pg_work_queue job payload). |
| `registry.rs` | `Registry`, `ErasedHandler` trait, `TypedHandler<E, H>`. |
| `builder.rs` | `OutboxBuilder`, `OutboxConfig`, `OutboxConfigBuilder`, `DecodeStrategy`. |
| `outbox.rs` | `Outbox` struct + `dispatch()`. |
| `runtime.rs` | `OutboxRuntime` (worker wrapper `handle_envelope` + `mark_*_fenced` helpers). |
| `handle.rs` | `OutboxHandle` (wraps pg_work_queue `WorkerHandle`). |
| `history.rs` | `History`, `EventRecord`, `HandlerDeliveryRecord`, `DeliveryStatus`. |
| `purge.rs` | `purge_terminal_deliveries`, `purge_dispatch_keys`, `purge_events`. |
| `migrator.rs` | `migrator()` returning `sqlx::Migrator` with `set_ignore_missing(true)`. |

### Migrations

| File | Responsibility |
|---|---|
| `migrations/20260513000000_v01_outbox_init.sql` | All schema DDL: PG18 guard, helpers, 3 tables, ENUM, indexes, triggers. |

### Tests (`tests/`)

Per spec §14 module layout: 21 test files. Each test file at top has `#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]` so test code can use unwrap freely while crate-level lints stay strict.

---

## Phase 0: Repo skeleton

### Task 0.1: Cargo.toml + lint config + .gitignore

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`

- [ ] **Step 1: Write Cargo.toml**

```toml
[package]
name = "rust_events"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT"
description = "Transactional outbox for Rust services on Postgres, built on pg_work_queue"
repository = "https://github.com/pawel/rust_events"
documentation = "https://docs.rs/rust_events"
readme = "README.md"
keywords = ["outbox", "postgres", "event", "transactional", "queue"]
categories = ["database", "asynchronous"]

[dependencies]
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

- [ ] **Step 2: Write .gitignore**

```gitignore
/target
Cargo.lock.bak
.idea/
.vscode/
.DS_Store
*.swp
```

(Note: Cargo.lock IS committed since this is a binary-adjacent library. Mirror pg_work_queue.)

- [ ] **Step 3: Run `cargo check` — must fail with "no targets"**

Run: `cargo check`
Expected: error E0601 ("`main` function not found") OR "no library targets" — confirms Cargo found the manifest.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml .gitignore
git commit -m "faza 0: cargo manifest + lint config + gitignore"
```

---

### Task 0.2: Empty lib.rs + minimal compile

**Files:**
- Create: `src/lib.rs`

- [ ] **Step 1: Write minimal lib.rs**

```rust
//! Transactional outbox library for Rust services on Postgres.
//!
//! See `docs/superpowers/specs/2026-05-13-rust-events-design.md` for design.
#![doc(html_root_url = "https://docs.rs/rust_events/0.1.0")]
```

- [ ] **Step 2: Run `cargo check` — must succeed**

Run: `cargo check`
Expected: PASS (warnings about missing_docs OK at this stage; will be addressed as modules land)

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "faza 0: empty lib.rs (compiles, no public surface yet)"
```

---

## Phase 1: Schema migration + migrator()

### Task 1.1: Initial migration SQL

**Files:**
- Create: `migrations/20260513000000_v01_outbox_init.sql`

- [ ] **Step 1: Write migration**

Copy DDL verbatim from spec §5 "DDL" subsection. Includes:
- PG18 version guard
- `outbox` schema
- `outbox.set_updated_at()` + `outbox.deny_update()` functions
- `outbox.events` table (B1, UUID PK, deny_update trigger, byte-length CHECKs)
- `outbox.dispatch_keys` table (composite PK, DEFERRABLE FK)
- `dispatch_keys_event_idx`, `dispatch_keys_created_at_idx`
- `outbox.delivery_status` ENUM
- `outbox.handler_deliveries` table (BIGINT IDENTITY PK, lease_token + status_invariants)
- 3 indexes on handler_deliveries (event, pending, terminal)
- `touch_handler_deliveries` trigger
- `ALTER TABLE outbox.handler_deliveries SET (fillfactor=90, ...)`

Exact SQL is in spec §5 — no transcription here (would duplicate ~150 lines). Implementer reads spec §5 and pastes.

- [ ] **Step 2: Sanity check SQL syntactically — no DB needed**

Run: `psql --version`  (just to confirm psql available; if not, `cat migrations/20260513000000_v01_outbox_init.sql | head -5` to inspect)
Expected: file present, well-formed.

- [ ] **Step 3: Commit**

```bash
git add migrations/20260513000000_v01_outbox_init.sql
git commit -m "faza 1: schema migration — outbox.{events, dispatch_keys, handler_deliveries}"
```

---

### Task 1.2: migrator() function

**Files:**
- Create: `src/migrator.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write migrator.rs**

```rust
//! Embedded schema migrations for `rust_events`.
//!
//! Coexists with `pg_work_queue::migrator()` on the shared `_sqlx_migrations`
//! table via `set_ignore_missing(true)` — neither library treats the other's
//! VERSION rows as missing/erroneous.

/// Returns the embedded migrator for `outbox.*` schema.
///
/// Call `.run(&pool)` to apply. See README for ordering — either migrator
/// can run first; both must run before any DB write.
///
/// ```ignore
/// pg_work_queue::migrator().run(&pool).await?;
/// rust_events::migrator().run(&pool).await?;
/// ```
#[must_use]
pub fn migrator() -> sqlx::migrate::Migrator {
    let mut m = sqlx::migrate!("./migrations");
    m.set_ignore_missing(true);
    m
}
```

- [ ] **Step 2: Re-export from lib.rs**

Add to `src/lib.rs`:

```rust
pub mod migrator;
pub use crate::migrator::migrator;
```

- [ ] **Step 3: Run `cargo check`**

Run: `cargo check`
Expected: PASS. `sqlx::migrate!` macro embeds the migration at build time.

- [ ] **Step 4: Commit**

```bash
git add src/migrator.rs src/lib.rs
git commit -m "faza 1: migrator() z set_ignore_missing(true) dla coexistence z pgwq"
```

---

### Task 1.3: Test — migrator coexistence (M5)

**Files:**
- Create: `tests/common/mod.rs`
- Create: `tests/migrator_coexistence.rs`

- [ ] **Step 1: Write test fixture in `tests/common/mod.rs`**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

/// Spin up a fresh PG18 container and return a connected pool.
/// Container handle MUST be held by the test (drop = stop container).
pub async fn pg_container() -> (ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    (container, pool)
}
```

- [ ] **Step 2: Write migrator_coexistence test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m5_both_migrators_run_in_either_order_success() {
    let (_c, pool) = common::pg_container().await;

    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Both schemas exist:
    let pgwq_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_schema='pgwq' AND table_name='jobs')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(pgwq_exists, "pgwq.jobs should exist");

    let outbox_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_schema='outbox' AND table_name='events')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(outbox_exists, "outbox.events should exist");

    let migration_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _sqlx_migrations",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(migration_count >= 2, "should have rows from both migrators");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m5_reverse_order_also_works() {
    let (_c, pool) = common::pg_container().await;

    rust_events::migrator().run(&pool).await.unwrap();
    pg_work_queue::migrator().run(&pool).await.unwrap();

    let outbox_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata \
         WHERE schema_name='outbox')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(outbox_exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m5_idempotent_reruns_no_duplicate_apply() {
    let (_c, pool) = common::pg_container().await;

    rust_events::migrator().run(&pool).await.unwrap();
    let count_after_first: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();

    rust_events::migrator().run(&pool).await.unwrap();
    let count_after_second: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(count_after_first, count_after_second, "no duplicate rows");
}
```

- [ ] **Step 3: Run the test (Docker required)**

Run: `cargo test --test migrator_coexistence`
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add tests/common/mod.rs tests/migrator_coexistence.rs
git commit -m "faza 1: migrator coexistence test (M5)"
```

---

## Phase 2: Limits + util helpers

### Task 2.1: limits module

**Files:**
- Create: `src/limits.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write limits.rs**

```rust
//! Resource limits — `pub const` bounds enforced both at Rust input validation
//! and at DB-level CHECK constraints (defense in depth).

/// Max byte length of event_type (`DomainEvent::EVENT_TYPE`).
pub const MAX_EVENT_TYPE_BYTES: usize = 128;
/// Max byte length of handler_id (registration string).
pub const MAX_HANDLER_ID_BYTES: usize = 128;
/// Max byte length of tenant_id.
pub const MAX_TENANT_BYTES: usize = 64;
/// Max byte length of producer_bc (bounded context name).
pub const MAX_BC_BYTES: usize = 64;
/// Max byte length of idempotency_key (per dispatch).
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
/// Max encoded payload size — matches `pg_work_queue::MAX_PAYLOAD_BYTES`.
pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;
/// Max length of stored `last_error` after UTF-8-safe truncation.
pub const MAX_LAST_ERROR_BYTES: usize = 8192;
/// Chunk size for purge functions. Mirrors `pg_work_queue` purge constant.
pub const PURGE_CHUNK_SIZE: usize = 10_000;
```

- [ ] **Step 2: Add module to lib.rs**

```rust
pub mod limits;
```

- [ ] **Step 3: `cargo check` passes**

Run: `cargo check`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/limits.rs src/lib.rs
git commit -m "faza 2: limits module (MAX_*_BYTES + PURGE_CHUNK_SIZE)"
```

---

### Task 2.2: util module — truncate_utf8

**Files:**
- Create: `src/util.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing unit test**

In `src/util.rs`:

```rust
//! Internal helpers.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_utf8_ascii() {
        assert_eq!(truncate_utf8("abcdef", 3), "abc");
    }

    #[test]
    fn truncate_utf8_multibyte_does_not_split_codepoint() {
        // "🦀" is 4 bytes in UTF-8.
        let s = "ab🦀cd";
        let t = truncate_utf8(s, 3);
        assert_eq!(t, "ab", "should drop the crab rather than slice it");
    }

    #[test]
    fn truncate_utf8_max_larger_than_len_returns_whole() {
        assert_eq!(truncate_utf8("abc", 100), "abc");
    }

    #[test]
    fn truncate_utf8_zero_max() {
        assert_eq!(truncate_utf8("abc", 0), "");
    }
}
```

- [ ] **Step 2: Run test — must fail (function not defined)**

Run: `cargo test --lib util::tests`
Expected: FAIL with E0425 ("cannot find function `truncate_utf8`").

- [ ] **Step 3: Implement truncate_utf8**

Prepend to `src/util.rs`:

```rust
/// UTF-8-boundary-safe truncation. Returns the longest prefix of `s` whose
/// byte length is `<= max`. Multi-byte codepoints crossing the boundary are
/// dropped, not split. Stable Rust 1.73+.
#[must_use]
pub(crate) fn truncate_utf8(s: &str, max: usize) -> &str {
    if max >= s.len() {
        s
    } else {
        &s[..s.floor_char_boundary(max)]
    }
}
```

Add `pub(crate) mod util;` to `src/lib.rs`.

- [ ] **Step 4: Run test — passes**

Run: `cargo test --lib util::tests`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/util.rs src/lib.rs
git commit -m "faza 2: util::truncate_utf8 (UTF-8-boundary safe via floor_char_boundary)"
```

---

### Task 2.3: util — is_pg_constraint_violation + parse_headers

**Files:**
- Modify: `src/util.rs`

- [ ] **Step 1: Add failing tests**

Append to `mod tests`:

```rust
#[test]
fn is_constraint_violation_recognizes_sqlstate_23() {
    // Construct a synthetic sqlx::Error::Database with SQLSTATE 23505.
    // Easier path: trigger one via a real query in an integration test;
    // for now this lives in tests/schema_invariants.rs. Here just exercise
    // the type signature compile-time.
    fn _types_compile(e: &sqlx::Error) -> bool {
        is_pg_constraint_violation(e)
    }
}

#[test]
fn parse_headers_object_passthrough() {
    let v = serde_json::json!({"a": 1, "b": "x"});
    let m = parse_headers(v);
    assert_eq!(m.get("a"), Some(&serde_json::json!(1)));
}

#[test]
fn parse_headers_non_object_returns_empty() {
    let v = serde_json::json!([1, 2, 3]);
    let m = parse_headers(v);
    assert!(m.is_empty());
}
```

- [ ] **Step 2: Run test — must fail (functions missing)**

Run: `cargo test --lib util::tests`
Expected: FAIL with E0425 for `is_pg_constraint_violation` and `parse_headers`.

- [ ] **Step 3: Implement helpers**

Append to `src/util.rs` (before `#[cfg(test)]`):

```rust
/// True iff `e` is a Postgres database error with SQLSTATE class `23`
/// (integrity constraint violation). Mirrors pg_work_queue's classification
/// for retry-vs-abort decisions in the worker wrapper.
pub(crate) fn is_pg_constraint_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = e {
        if let Some(code) = db.code() {
            return code.starts_with("23");
        }
    }
    false
}

/// Convert a `serde_json::Value` into a header `Map`. Non-object inputs
/// (arrays, scalars, null) collapse to an empty map — the DB CHECK on
/// `outbox.events.headers` should prevent these reaching us, but defense
/// in depth.
pub(crate) fn parse_headers(
    v: serde_json::Value,
) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}
```

- [ ] **Step 4: Run test — passes**

Run: `cargo test --lib util::tests`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/util.rs
git commit -m "faza 2: util::is_pg_constraint_violation + parse_headers"
```

---

## Phase 3: Core public types

### Task 3.1: handler.rs — DomainEvent + HandlerError

**Files:**
- Create: `src/handler.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write handler.rs (initial — DomainEvent trait + HandlerError enum)**

```rust
//! Public traits and types for domain events and handlers.

use std::time::Duration;
use uuid::Uuid;

/// A user-defined domain event type.
///
/// `EVENT_TYPE` is the **stable wire-name** stored in `outbox.events.event_type`
/// and used as the dispatch key in the in-memory registry. Independent of
/// Rust path: renaming a module or restructuring crates will not break
/// dispatch as long as `EVENT_TYPE` is preserved.
pub trait DomainEvent:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    /// Stable wire-name. Convention: `"<bc>.<event_name>"`,
    /// e.g. `"shop.order_created"`.
    const EVENT_TYPE: &'static str;
}

/// Handler outcome. Mirror of `pg_work_queue::JobError` plus `Skip` for the
/// "this event doesn't apply to me" pattern (filtered tenant, opted-out user).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerError {
    /// Transient failure; pg_work_queue will retry with backoff.
    #[error("retry: {reason}")]
    Retry {
        reason: String,
        retry_in: Option<Duration>,
    },
    /// Terminal: this delivery does not apply (filtered tenant, wrong env, ...).
    /// Audit shows `status='skipped'`, distinct from `'sent'` (success) and
    /// `'dead'` (failure).
    #[error("skip: {reason}")]
    Skip { reason: String },
    /// Permanent failure; bypass retry budget, mark dead immediately.
    #[error("abort: {reason}")]
    Abort { reason: String },
}

impl HandlerError {
    /// Construct a `Retry` with default backoff (decided by Worker's policy).
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: None,
        }
    }

    /// Construct a `Retry` with explicit delay.
    pub fn retry_in(reason: impl Into<String>, retry_in: Duration) -> Self {
        Self::Retry {
            reason: reason.into(),
            retry_in: Some(retry_in),
        }
    }

    pub fn skip(reason: impl Into<String>) -> Self {
        Self::Skip {
            reason: reason.into(),
        }
    }

    pub fn abort(reason: impl Into<String>) -> Self {
        Self::Abort {
            reason: reason.into(),
        }
    }
}

/// Read-only context passed to every handler invocation.
///
/// `delivery_key` is the per-handler UUID assigned by `pg_work_queue` at
/// `Pusher::push_batch` time. **Stable across retries** of THIS delivery
/// (so external APIs using `Idempotency-Key` headers can dedupe retries).
/// **NOT** the same as the dispatch-time idempotency key passed by the
/// caller — for that see `dispatch_idempotency_key`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HandlerContext {
    pub event_id: Uuid,
    pub tenant_id: String,
    pub producer_bc: String,
    /// 1-indexed current attempt; from `pg_work_queue::JobContext::attempt`.
    pub attempt: u32,
    /// Per-row stamped max_attempts; from `pg_work_queue::JobContext::max_attempts`.
    pub max_attempts: u32,
    /// Per-delivery UUID, stable across retries. Use as `Idempotency-Key`.
    pub delivery_key: Uuid,
    /// User-supplied dispatch-level idempotency key (if any). The same string
    /// the caller passed to `DispatchContext::with_idempotency_key`. Useful
    /// when external API dedup should align with domain-level deduplication
    /// (the same business action repeated → same external key).
    pub dispatch_idempotency_key: Option<String>,
    pub headers: serde_json::Map<String, serde_json::Value>,
}
```

- [ ] **Step 2: Wire into lib.rs**

```rust
pub mod handler;
pub use crate::handler::{DomainEvent, HandlerContext, HandlerError};
```

- [ ] **Step 3: `cargo check` + `cargo test --lib`**

Run: `cargo check && cargo test --lib`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/handler.rs src/lib.rs
git commit -m "faza 3: DomainEvent trait + HandlerError + HandlerContext"
```

---

### Task 3.2: handler.rs — EventHandler trait (async_trait)

**Files:**
- Modify: `src/handler.rs`

- [ ] **Step 1: Append `EventHandler<E>` trait**

```rust
/// User-implementable async handler for a domain event.
///
/// Implementations must be `Send + Sync + 'static` (stored in `Arc<dyn ...>`
/// in the registry). Use `async_trait` macro for dyn-compatibility.
#[async_trait::async_trait]
pub trait EventHandler<E: DomainEvent>: Send + Sync + 'static {
    async fn handle(
        &self,
        event: &E,
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError>;
}
```

Update lib.rs re-export:

```rust
pub use crate::handler::{DomainEvent, EventHandler, HandlerContext, HandlerError};
```

- [ ] **Step 2: `cargo check`**

Run: `cargo check`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/handler.rs src/lib.rs
git commit -m "faza 3: EventHandler<E> trait (async_trait, dyn-compatible)"
```

---

### Task 3.3: dispatch_context.rs — DispatchContext (no Default)

**Files:**
- Create: `src/dispatch_context.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write unit tests first**

```rust
//! Per-dispatch caller context. NO `Default` impl — tenant_id must be set
//! explicitly to prevent silent multi-tenant data leaks via `..Default::default()`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_tenant() {
        let ctx = DispatchContext::new("acme");
        assert_eq!(ctx.tenant_id(), "acme");
        assert_eq!(ctx.producer_bc(), "");
        assert!(ctx.idempotency_key().is_none());
    }

    #[test]
    fn with_producer_bc_chains() {
        let ctx = DispatchContext::new("acme").with_producer_bc("shop");
        assert_eq!(ctx.producer_bc(), "shop");
    }

    #[test]
    fn with_idempotency_key_chains() {
        let ctx = DispatchContext::new("acme").with_idempotency_key("order:42");
        assert_eq!(ctx.idempotency_key(), Some("order:42"));
    }

    #[test]
    fn with_headers_chains() {
        let mut h = serde_json::Map::new();
        h.insert("trace".into(), serde_json::json!("abc"));
        let ctx = DispatchContext::new("acme").with_headers(h);
        assert!(ctx.headers().is_some());
    }
}
```

- [ ] **Step 2: Run — must fail (type missing)**

Run: `cargo test --lib dispatch_context::tests`
Expected: FAIL with E0433.

- [ ] **Step 3: Implement DispatchContext**

Prepend to `src/dispatch_context.rs`:

```rust
/// Caller-supplied context for a single `Outbox::dispatch()` call.
///
/// Constructed via `DispatchContext::new(tenant_id)` and refined via chainable
/// `with_*` methods. NO `Default` impl by design — every dispatch must
/// explicitly choose a tenant_id (use `"default"` for single-tenant apps).
#[derive(Debug, Clone)]
pub struct DispatchContext<'a> {
    tenant_id: &'a str,
    producer_bc: &'a str,
    idempotency_key: Option<&'a str>,
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

impl<'a> DispatchContext<'a> {
    /// Construct with required `tenant_id`. For single-tenant deployments
    /// pass `"default"` or your application slug.
    #[must_use]
    pub fn new(tenant_id: &'a str) -> Self {
        Self {
            tenant_id,
            producer_bc: "",
            idempotency_key: None,
            headers: None,
        }
    }

    #[must_use]
    pub fn with_producer_bc(mut self, bc: &'a str) -> Self {
        self.producer_bc = bc;
        self
    }

    #[must_use]
    pub fn with_idempotency_key(mut self, key: &'a str) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    #[must_use]
    pub fn with_headers(
        mut self,
        headers: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.headers = Some(headers);
        self
    }

    #[must_use]
    pub fn tenant_id(&self) -> &str {
        self.tenant_id
    }

    #[must_use]
    pub fn producer_bc(&self) -> &str {
        self.producer_bc
    }

    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key
    }

    #[must_use]
    pub fn headers(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.headers.as_ref()
    }
}
```

Wire into lib.rs:

```rust
pub mod dispatch_context;
pub use crate::dispatch_context::DispatchContext;
```

- [ ] **Step 4: Run tests — pass**

Run: `cargo test --lib dispatch_context::tests`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/dispatch_context.rs src/lib.rs
git commit -m "faza 3: DispatchContext (no Default, builder-style construction)"
```

---

### Task 3.4: outcome.rs — DispatchOutcome + OutboxStats

**Files:**
- Create: `src/outcome.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write outcome.rs**

```rust
//! Result types returned by `Outbox::dispatch()` and `OutboxHandle::shutdown()`.

use uuid::Uuid;

/// Result of a successful (non-error) `dispatch()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchOutcome {
    /// Event persisted, N delivery jobs queued.
    Dispatched { event_id: Uuid, deliveries: usize },
    /// `idempotency_key` matched an existing dispatch_keys row; the original
    /// event is returned. No new event/delivery rows created.
    Duplicate { event_id: Uuid },
    /// Returned ONLY when `OutboxBuilder::allow_no_handlers(true)` is set and
    /// no handlers are registered for `E::EVENT_TYPE`. Event is persisted as
    /// audit-only; no delivery jobs queued. Otherwise this case surfaces as
    /// `DispatchError::NoHandlersRegistered`.
    NoHandlers { event_id: Uuid },
}

/// Outbox-level stats at `shutdown()` time. Separate query on
/// `outbox.handler_deliveries` for `pending_deliveries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OutboxStats {
    /// Count of rows with `status IN ('queued','running','awaiting_retry')`
    /// at shutdown time. May be > 0 if shutdown timeout was reached before
    /// in-flight deliveries terminated.
    pub pending_deliveries: u64,
}
```

Wire into lib.rs:

```rust
pub mod outcome;
pub use crate::outcome::{DispatchOutcome, OutboxStats};
```

- [ ] **Step 2: `cargo check`**

Run: `cargo check`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/outcome.rs src/lib.rs
git commit -m "faza 3: DispatchOutcome + OutboxStats"
```

---

### Task 3.5: error.rs — all public error enums

**Files:**
- Create: `src/error.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write error.rs**

```rust
//! Public error types. All `thiserror::Error`, `#[non_exhaustive]`,
//! `Send + Sync + 'static`. See spec §9.

use crate::util::is_pg_constraint_violation;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    #[error("handler_id must be non-empty")]
    HandlerIdEmpty,

    #[error("handler_id length {len} bytes exceeds max {max}")]
    HandlerIdTooLong { len: usize, max: usize },

    #[error("handler_id '{handler_id}' already registered for event_type '{event_type}'")]
    DuplicateHandlerId {
        event_type: &'static str,
        handler_id: String,
    },

    #[error("config invalid: {0}")]
    ConfigInvalid(String),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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

    /// Deterministic database error (constraint violation, etc.) — retry
    /// unlikely to help.
    #[error("database constraint violation during dispatch")]
    Constraint(#[source] sqlx::Error),

    /// Transient database error (pool starvation, deadlock, connection drop).
    #[error("transient database error during dispatch")]
    Transient(#[source] sqlx::Error),
}

impl DispatchError {
    /// Heuristic: is retrying this dispatch likely to succeed?
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Transient(_) => true,
            Self::PgwqPush(e) => matches!(e, pg_work_queue::PushError::Transient(_)),
            _ => false,
        }
    }
}

impl From<sqlx::Error> for DispatchError {
    fn from(e: sqlx::Error) -> Self {
        if is_pg_constraint_violation(&e) {
            Self::Constraint(e)
        } else {
            Self::Transient(e)
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HistoryError {
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PurgeError {
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartError {
    #[error("outbox already started; second start() rejected")]
    AlreadyStarted,

    #[error("pg_work_queue worker build failed")]
    PgwqBuild(#[from] pg_work_queue::BuildError),

    #[error("pg_work_queue worker start failed")]
    PgwqStart(#[from] pg_work_queue::StartError),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShutdownError {
    #[error("pg_work_queue worker shutdown failed")]
    Pgwq(#[from] pg_work_queue::ShutdownError),

    #[error("could not count pending deliveries at shutdown")]
    PendingCount(#[source] sqlx::Error),
}
```

Wire into lib.rs:

```rust
pub mod error;
pub use crate::error::{
    BuildError, DispatchError, HistoryError, PurgeError, ShutdownError, StartError,
};
```

- [ ] **Step 2: `cargo check`**

Run: `cargo check`
Expected: PASS. If `pg_work_queue::PushError::Transient` does not exist with this name, inspect the actual variant via `cargo doc -p pg_work_queue --open` and adjust the matches!. If pgwq's `PushError` exposes its own `is_retriable()`, prefer calling that.

- [ ] **Step 3: Commit**

```bash
git add src/error.rs src/lib.rs
git commit -m "faza 3: public error enums (BuildError, DispatchError, HistoryError, PurgeError, StartError, ShutdownError)"
```

---

## Phase 4: Envelope + Registry

### Task 4.1: envelope.rs — HandlerEnvelope

**Files:**
- Create: `src/envelope.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write envelope.rs**

```rust
//! Internal: payload of pg_work_queue jobs in the `outbox_handler_deliveries`
//! queue. Worker decodes this, then fetches event payload from outbox.events.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandlerEnvelope {
    pub(crate) event_id: Uuid,
    pub(crate) handler_id: String,
}
```

Add to lib.rs:

```rust
pub(crate) mod envelope;
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/envelope.rs src/lib.rs
git commit -m "faza 4: HandlerEnvelope (pgwq job payload)"
```

---

### Task 4.2: registry.rs — ErasedHandler + TypedHandler + Registry

**Files:**
- Create: `src/registry.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write registry.rs**

```rust
//! Type-erased handler registry. Maps `(event_type, handler_id) → Arc<dyn ErasedHandler>`
//! and exposes a fast `event_type → Vec<handler_id>` lookup for the dispatch hot path.

use crate::handler::{DomainEvent, EventHandler, HandlerContext, HandlerError};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

/// Internal trait erased over event type so heterogeneous handlers can live
/// in one `HashMap`. Implementations decode bytes → `E`, then call the user's
/// typed handler.
#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    /// Untyped entry point: decode bytes, dispatch to typed handler.
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError>;
}

/// Concrete adapter wrapping a user's `Arc<H: EventHandler<E>>` so it can be
/// stored as `Arc<dyn ErasedHandler>`.
pub(crate) struct TypedHandler<E, H> {
    pub(crate) inner: Arc<H>,
    pub(crate) _e: PhantomData<fn() -> E>,
}

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
    ) -> Result<(), HandlerError> {
        let event: E = serde_json::from_slice(payload).map_err(|e| {
            HandlerError::abort(format!("decode {}: {e}", E::EVENT_TYPE))
        })?;
        self.inner.handle(&event, ctx).await
    }
}

/// In-memory registry built at `OutboxBuilder::build()` time. Immutable after
/// construction (no runtime register/unregister).
pub(crate) struct Registry {
    /// Keyed by `handler_id`. Worker side: `lookup(handler_id)`.
    pub(crate) handlers: HashMap<String, Arc<dyn ErasedHandler>>,
    /// Dispatch side: `event_type → handler_ids` for fast fanout.
    pub(crate) by_type: HashMap<&'static str, Vec<String>>,
}

impl Registry {
    pub(crate) fn lookup(&self, handler_id: &str) -> Option<&Arc<dyn ErasedHandler>> {
        self.handlers.get(handler_id)
    }

    pub(crate) fn handler_ids_for(&self, event_type: &'static str) -> &[String] {
        self.by_type
            .get(event_type)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}
```

Add to lib.rs:

```rust
pub(crate) mod registry;
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/registry.rs src/lib.rs
git commit -m "faza 4: Registry + ErasedHandler + TypedHandler"
```

---

## Phase 5: OutboxConfig + OutboxBuilder

### Task 5.1: builder.rs — OutboxConfig + OutboxConfigBuilder + DecodeStrategy

**Files:**
- Create: `src/builder.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write OutboxConfig + OutboxConfigBuilder + DecodeStrategy**

```rust
//! `OutboxConfig`, `OutboxConfigBuilder`, `OutboxBuilder`. Fail-late validation
//! in `build()` — mirrors `pg_work_queue::WorkerBuilder` conventions.

use crate::error::BuildError;
use crate::handler::{DomainEvent, EventHandler};
use crate::registry::{ErasedHandler, Registry, TypedHandler};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

pub use pg_work_queue::{BackoffPolicy, PanicPolicy};

/// What to do when payload bytes fail to deserialize as `E`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeStrategy {
    /// Default. Return `JobError::retry` — gives a window for rollback if a
    /// schema-incompatible event payload was deployed accidentally. After
    /// `max_attempts` retries the job goes dead via `pg_work_queue`'s own
    /// path (and our wrapper marks `handler_deliveries.status='dead'`).
    Retry,
    /// Decode error is a permanent fault → `mark_dead` on first claim.
    /// Use only when payload schema is strictly versioned and decode errors
    /// must surface immediately.
    Abort,
}

impl Default for DecodeStrategy {
    fn default() -> Self {
        Self::Retry
    }
}

#[derive(Debug, Clone)]
pub struct OutboxConfig {
    pub(crate) poll_interval: Duration,
    pub(crate) concurrency: u32,
    pub(crate) max_attempts: u32,
    pub(crate) lease_timeout: Duration,
    pub(crate) handler_timeout: Duration,
    pub(crate) retry_backoff: BackoffPolicy,
    pub(crate) panic_policy: PanicPolicy,
    pub(crate) strict_handler_lookup: bool,
    pub(crate) decode_error_strategy: DecodeStrategy,
}

impl OutboxConfig {
    #[must_use]
    pub fn builder() -> OutboxConfigBuilder {
        OutboxConfigBuilder::default()
    }
}

impl Default for OutboxConfig {
    fn default() -> Self {
        // Mirror pg_work_queue's WorkerBuilder defaults.
        let lease_timeout = Duration::from_secs(300);
        Self {
            poll_interval: Duration::from_millis(500),
            concurrency: 16,
            max_attempts: 5,
            lease_timeout,
            handler_timeout: Duration::from_secs(240), // 80% of 300s lease
            retry_backoff: BackoffPolicy::default(),
            panic_policy: PanicPolicy::default(),
            strict_handler_lookup: false,
            decode_error_strategy: DecodeStrategy::Retry,
        }
    }
}

#[derive(Debug, Default)]
pub struct OutboxConfigBuilder {
    cfg: OutboxConfig,
}

impl OutboxConfigBuilder {
    #[must_use]
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.cfg.poll_interval = d;
        self
    }
    #[must_use]
    pub fn concurrency(mut self, n: u32) -> Self {
        self.cfg.concurrency = n;
        self
    }
    #[must_use]
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.cfg.max_attempts = n;
        self
    }
    #[must_use]
    pub fn lease_timeout(mut self, d: Duration) -> Self {
        self.cfg.lease_timeout = d;
        self
    }
    #[must_use]
    pub fn handler_timeout(mut self, d: Duration) -> Self {
        self.cfg.handler_timeout = d;
        self
    }
    #[must_use]
    pub fn retry_backoff(mut self, p: BackoffPolicy) -> Self {
        self.cfg.retry_backoff = p;
        self
    }
    #[must_use]
    pub fn panic_policy(mut self, p: PanicPolicy) -> Self {
        self.cfg.panic_policy = p;
        self
    }
    #[must_use]
    pub fn strict_handler_lookup(mut self, strict: bool) -> Self {
        self.cfg.strict_handler_lookup = strict;
        self
    }
    #[must_use]
    pub fn decode_error_strategy(mut self, s: DecodeStrategy) -> Self {
        self.cfg.decode_error_strategy = s;
        self
    }

    /// Validates basic invariants. pg_work_queue's own validation runs at
    /// `Outbox::start()` and surfaces via `StartError::PgwqBuild`.
    pub fn build(self) -> Result<OutboxConfig, BuildError> {
        if self.cfg.concurrency == 0 {
            return Err(BuildError::ConfigInvalid("concurrency must be >= 1".into()));
        }
        if self.cfg.max_attempts == 0 {
            return Err(BuildError::ConfigInvalid("max_attempts must be >= 1".into()));
        }
        if self.cfg.handler_timeout >= self.cfg.lease_timeout {
            return Err(BuildError::ConfigInvalid(
                "handler_timeout must be < lease_timeout".into(),
            ));
        }
        Ok(self.cfg)
    }
}
```

Wire into lib.rs:

```rust
pub mod builder;
pub use crate::builder::{
    BackoffPolicy, DecodeStrategy, OutboxConfig, OutboxConfigBuilder, PanicPolicy,
};
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/builder.rs src/lib.rs
git commit -m "faza 5: OutboxConfig + OutboxConfigBuilder + DecodeStrategy"
```

---

### Task 5.2: builder.rs — OutboxBuilder with register_handler + allow_no_handlers

**Files:**
- Modify: `src/builder.rs`

- [ ] **Step 1: Add OutboxBuilder to existing builder.rs**

Append after the OutboxConfigBuilder block:

```rust
use sqlx::PgPool;

/// Builder for `Outbox`. Collects pool, config, and handler registrations;
/// validates at `build()` time (fail-late convention from pg_work_queue).
pub struct OutboxBuilder {
    pool: PgPool,
    config: Option<OutboxConfig>,
    /// Pending handler entries — validated and folded into `Registry` in `build()`.
    pending: Vec<PendingHandler>,
    allow_no_handlers: bool,
}

struct PendingHandler {
    event_type: &'static str,
    handler_id: String,
    handler: Arc<dyn ErasedHandler>,
}

impl OutboxBuilder {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            config: None,
            pending: Vec::new(),
            allow_no_handlers: false,
        }
    }

    #[must_use]
    pub fn config(mut self, cfg: OutboxConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Register a handler. Takes ownership of `handler` and wraps in
    /// `Arc<TypedHandler<E, H>>` internally — caller does NOT pre-wrap.
    #[must_use]
    pub fn register_handler<E, H>(
        mut self,
        handler_id: impl Into<String>,
        handler: H,
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
        });
        self
    }

    /// When true, `dispatch()` for an event_type with no registered handlers
    /// returns `Ok(DispatchOutcome::NoHandlers { event_id })`. When false
    /// (default), returns `Err(DispatchError::NoHandlersRegistered)`.
    #[must_use]
    pub fn allow_no_handlers(mut self, allow: bool) -> Self {
        self.allow_no_handlers = allow;
        self
    }

    pub fn build(self) -> Result<crate::outbox::Outbox, BuildError> {
        let config = self.config.unwrap_or_default();

        // Validate handler entries; build Registry.
        let mut handlers: HashMap<String, Arc<dyn ErasedHandler>> = HashMap::new();
        let mut by_type: HashMap<&'static str, Vec<String>> = HashMap::new();

        for entry in self.pending {
            if entry.handler_id.is_empty() {
                return Err(BuildError::HandlerIdEmpty);
            }
            if entry.handler_id.len() > crate::limits::MAX_HANDLER_ID_BYTES {
                return Err(BuildError::HandlerIdTooLong {
                    len: entry.handler_id.len(),
                    max: crate::limits::MAX_HANDLER_ID_BYTES,
                });
            }
            if handlers.contains_key(&entry.handler_id) {
                return Err(BuildError::DuplicateHandlerId {
                    event_type: entry.event_type,
                    handler_id: entry.handler_id,
                });
            }
            by_type
                .entry(entry.event_type)
                .or_default()
                .push(entry.handler_id.clone());
            handlers.insert(entry.handler_id, entry.handler);
        }

        let registry = Arc::new(Registry { handlers, by_type });

        Ok(crate::outbox::Outbox::new(
            self.pool,
            config,
            registry,
            self.allow_no_handlers,
        ))
    }
}
```

Re-export from lib.rs:

```rust
pub use crate::builder::OutboxBuilder;
```

- [ ] **Step 2: `cargo check`**

Expected: FAIL — `crate::outbox::Outbox` does not exist yet. This is intentional — defer to Phase 6. For now, comment out the body of `build()` and return placeholder error, or stub `outbox` module with skeleton. Pragmatic: stub the module now to keep build green.

Add minimal stub `src/outbox.rs`:

```rust
//! Outbox runtime — populated in Phase 6.

use crate::builder::OutboxConfig;
use crate::registry::Registry;
use sqlx::PgPool;
use std::sync::Arc;

pub struct Outbox {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
    pub(crate) allow_no_handlers: bool,
}

impl Outbox {
    pub(crate) fn new(
        pool: PgPool,
        config: OutboxConfig,
        registry: Arc<Registry>,
        allow_no_handlers: bool,
    ) -> Self {
        Self { pool, config, registry, allow_no_handlers }
    }
}
```

Wire `pub mod outbox;` into lib.rs.

- [ ] **Step 3: `cargo check` — passes**

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/builder.rs src/outbox.rs src/lib.rs
git commit -m "faza 5: OutboxBuilder + register_handler + allow_no_handlers + Outbox skeleton"
```

---

### Task 5.3: Builder validation tests

**Files:**
- Create: `tests/builder_validation.rs`

- [ ] **Step 1: Write failing tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    BuildError, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct E1 { x: i32 }
impl DomainEvent for E1 {
    const EVENT_TYPE: &'static str = "test.e1";
}

struct H;
#[async_trait::async_trait]
impl EventHandler<E1> for H {
    async fn handle(&self, _: &E1, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("", H)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::HandlerIdEmpty));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let long = "x".repeat(129);
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>(long, H)
        .build()
        .unwrap_err();
    assert!(matches!(
        err,
        BuildError::HandlerIdTooLong { len: 129, max: 128 }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_handler_id_rejected() {
    let (_c, pool) = common::pg_container().await;
    let err = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("audit", H)
        .register_handler::<E1, _>("audit", H)
        .build()
        .unwrap_err();
    assert!(matches!(err, BuildError::DuplicateHandlerId { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_config_concurrency_zero() {
    let (_c, pool) = common::pg_container().await;
    let cfg_err = OutboxConfig::builder().concurrency(0).build().unwrap_err();
    assert!(matches!(cfg_err, BuildError::ConfigInvalid(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_config_builds() {
    let (_c, pool) = common::pg_container().await;
    let _outbox = OutboxBuilder::new(pool)
        .register_handler::<E1, _>("audit", H)
        .build()
        .unwrap();
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test builder_validation`
Expected: 5 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/builder_validation.rs
git commit -m "faza 5: builder validation tests"
```

---

## Phase 6: Dispatch flow (`Outbox::dispatch`)

### Task 6.1: Outbox::dispatch — validation + idempotency check + events INSERT

**Files:**
- Modify: `src/outbox.rs`

- [ ] **Step 1: Implement dispatch() with input validation**

Replace the skeleton `outbox.rs` body with the full struct + dispatch:

```rust
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

pub struct Outbox {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
    pub(crate) allow_no_handlers: bool,
    pub(crate) started: AtomicBool,
}

impl Outbox {
    pub(crate) fn new(
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
        if let Some(k) = ctx.idempotency_key() {
            if k.is_empty() || k.len() > limits::MAX_IDEMPOTENCY_KEY_BYTES {
                return Err(DispatchError::IdempotencyKeyInvalid {
                    len: k.len(),
                    max: limits::MAX_IDEMPOTENCY_KEY_BYTES,
                });
            }
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
```

- [ ] **Step 2: `cargo check`**

Expected: PASS. If pg_work_queue's Pusher signature differs (e.g., `push_batch` expects `impl Iterator` not `&[T]`), adjust the call.

- [ ] **Step 3: Commit**

```bash
git add src/outbox.rs
git commit -m "faza 6: Outbox::dispatch (validation + idempotency + events INSERT + push_batch)"
```

---

### Task 6.2: Dispatch happy-path integration tests

**Files:**
- Create: `tests/dispatch_happy_path.rs`

- [ ] **Step 1: Write tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchOutcome, DomainEvent, EventHandler, HandlerContext,
    HandlerError, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderCreated {
    order_id: i64,
    amount: i64,
}
impl DomainEvent for OrderCreated {
    const EVENT_TYPE: &'static str = "shop.order_created";
}

struct Noop;
#[async_trait::async_trait]
impl EventHandler<OrderCreated> for Noop {
    async fn handle(
        &self,
        _: &OrderCreated,
        _: &HandlerContext,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

async fn setup() -> (testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>, sqlx::PgPool, rust_events::Outbox) {
    let (c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<OrderCreated, _>("audit", Noop)
        .build()
        .unwrap();
    (c, pool, outbox)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_returns_dispatched_with_event_id_and_count() {
    let (_c, pool, outbox) = setup().await;
    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_producer_bc("shop"),
            &OrderCreated { order_id: 1, amount: 100 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    match outcome {
        DispatchOutcome::Dispatched { event_id, deliveries } => {
            assert!(!event_id.is_nil());
            assert_eq!(deliveries, 1);
        }
        other => panic!("expected Dispatched, got {other:?}"),
    }

    // event row exists
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // delivery row queued
    let dcount: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='queued'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dcount, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_duplicate_returns_existing_event_id() {
    let (_c, pool, outbox) = setup().await;
    let mut tx = pool.begin().await.unwrap();
    let first = outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated { order_id: 42, amount: 100 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let first_id = match first {
        DispatchOutcome::Dispatched { event_id, .. } => event_id,
        _ => unreachable!(),
    };

    let mut tx2 = pool.begin().await.unwrap();
    let second = outbox
        .dispatch(
            &mut tx2,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &OrderCreated { order_id: 42, amount: 100 },
        )
        .await
        .unwrap();
    tx2.commit().await.unwrap();

    match second {
        DispatchOutcome::Duplicate { event_id } => assert_eq!(event_id, first_id),
        other => panic!("expected Duplicate, got {other:?}"),
    }

    let ec: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ec, 1, "second dispatch must not create new event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payload_too_large_rejected() {
    let (_c, pool, outbox) = setup().await;
    let huge = OrderCreated {
        order_id: 1,
        amount: 0,
    };
    // OrderCreated is small — instead use a struct with a big String field:
    // we'll just synthesize via a different event type later. For now, exercise
    // the validation path with a manually-encoded huge payload by directly
    // calling serde_json::to_vec to estimate. Skip if not over the limit.
    let _ = (huge, outbox);
}
```

(The `payload_too_large_rejected` test is a placeholder; full version uses a `String` field of size > 1 MiB. Implementer adds when concrete event struct lands.)

- [ ] **Step 2: Run tests**

Run: `cargo test --test dispatch_happy_path`
Expected: 2 PASS (third placeholder essentially is a no-op).

- [ ] **Step 3: Commit**

```bash
git add tests/dispatch_happy_path.rs
git commit -m "faza 6: dispatch happy-path tests (Dispatched + Duplicate)"
```

---

### Task 6.3: NoHandlers strict-mode test (M6)

**Files:**
- Create: `tests/no_handlers_strict.rs`

- [ ] **Step 1: Write tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DispatchOutcome, DomainEvent, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Orphan {
    x: i32,
}
impl DomainEvent for Orphan {
    const EVENT_TYPE: &'static str = "test.orphan";
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_default_no_handler_returns_error_no_persist() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone()).build().unwrap();

    let mut tx = pool.begin().await.unwrap();
    let err = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Orphan { x: 1 })
        .await
        .unwrap_err();
    tx.rollback().await.unwrap();

    assert!(matches!(
        err,
        DispatchError::NoHandlersRegistered {
            event_type: "test.orphan"
        }
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "strict mode must not persist event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m6_allow_no_handlers_opt_in_returns_outcome() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Orphan { x: 1 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    match outcome {
        DispatchOutcome::NoHandlers { event_id } => assert!(!event_id.is_nil()),
        other => panic!("expected NoHandlers, got {other:?}"),
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let dcount: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.handler_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(dcount, 0);
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test no_handlers_strict`
Expected: 2 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/no_handlers_strict.rs
git commit -m "faza 6: NoHandlers strict-mode tests (M6)"
```

---

## Phase 7: Worker runtime (`OutboxRuntime` + handle_envelope + mark_*_fenced)

### Task 7.1: runtime.rs — OutboxRuntime + mark_*_fenced helpers

**Files:**
- Create: `src/runtime.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write runtime.rs (helpers only, wrapper next task)**

```rust
//! Worker runtime — fenced audit transitions and the `handle_envelope`
//! wrapper invoked by `pg_work_queue::Worker`.

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
    /// Transition delivery to `sent` IFF status='running' AND lease_token matches.
    /// rows_affected=0 → fenced out (stale worker); we emit warn tracing + Ok.
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
        self.log_fenced_out("sent", event_id, handler_id, res.rows_affected());
        Ok(())
    }

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
        self.log_fenced_out("awaiting_retry", event_id, handler_id, res.rows_affected());
        Ok(())
    }

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
        self.log_fenced_out("dead", event_id, handler_id, res.rows_affected());
        Ok(())
    }

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
        self.log_fenced_out("skipped", event_id, handler_id, res.rows_affected());
        Ok(())
    }

    fn log_fenced_out(
        &self,
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
}
```

Wire into lib.rs:

```rust
pub(crate) mod runtime;
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/runtime.rs src/lib.rs
git commit -m "faza 7: OutboxRuntime + mark_*_fenced helpers"
```

---

### Task 7.2: handle_envelope wrapper

**Files:**
- Modify: `src/runtime.rs`

- [ ] **Step 1: Append handle_envelope to runtime.rs**

```rust
use crate::builder::DecodeStrategy;
use crate::envelope::HandlerEnvelope;
use crate::handler::{HandlerContext, HandlerError};
use crate::util::{is_pg_constraint_violation, parse_headers};

impl OutboxRuntime {
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
        // ① Registry lookup BEFORE touching the audit row.
        let handler = match self.registry.lookup(&env.handler_id) {
            Some(h) => h.clone(),
            None => {
                if self.config.strict_handler_lookup {
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
                // Loose: leave the row untouched, return retry.
                tracing::warn!(
                    target: "rust_events.worker.handler_missing",
                    handler_id = %env.handler_id,
                    "handler not in this replica's registry; retrying"
                );
                return Err(pg_work_queue::JobError::retry(
                    "handler not registered in this replica",
                ));
            }
        };

        // ② Atomic transition + event/dispatch_key fetch.
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
            Vec<u8>,         // payload
            String,          // tenant_id
            String,          // producer_bc
            serde_json::Value, // headers
            Option<String>,    // dispatch_idempotency_key
            Option<String>,    // prev_status
            bool,              // did_update
        )>(
            r#"
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
            "#,
        )
        .bind(env.event_id)
        .bind(&env.handler_id)
        .bind(i32::try_from(ctx.attempt).unwrap_or(i32::MAX))
        .bind(ctx.lease_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| map_sql(e, "fetch delivery"))?
        .map(|(p, t, b, h, dk, prev, du)| Row {
            payload: p,
            tenant_id: t,
            producer_bc: b,
            headers: h,
            dispatch_idempotency_key: dk,
            prev_status: prev,
            did_update: du,
        });

        // ③ Discriminate the three states.
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

        // ⑤ Handler call via type-erased dispatch + decode_error_strategy.
        let result = handler.handle_erased(&row.payload, &hctx).await;

        // Translate decode aborts wrapping based on strategy.
        // (TypedHandler::handle_erased already returns Abort on decode error;
        //  but we need to honor decode_error_strategy=Retry by converting.)
        let result = match (result, self.config.decode_error_strategy) {
            (Err(HandlerError::Abort { reason }), DecodeStrategy::Retry)
                if reason.starts_with("decode ") =>
            {
                Err(HandlerError::Retry { reason, retry_in: None })
            }
            (other, _) => other,
        };

        // ⑥ Terminal transition.
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

fn map_sql(e: sqlx::Error, ctx: &str) -> pg_work_queue::JobError {
    if is_pg_constraint_violation(&e) {
        pg_work_queue::JobError::abort(format!("{ctx}: constraint violation: {e}"))
    } else {
        pg_work_queue::JobError::retry(format!("{ctx}: {e}"))
    }
}
```

- [ ] **Step 2: `cargo check`**

Expected: PASS. NOTE: the SQL query_as uses positional tuple destructuring; depending on sqlx version may need named struct binding or `#[derive(FromRow)]`. Implementer adjusts to match sqlx 0.8.6 API.

- [ ] **Step 3: Commit**

```bash
git add src/runtime.rs
git commit -m "faza 7: handle_envelope wrapper (fenced CTE + decode strategy + Skip)"
```

---

## Phase 8: Lifecycle — Outbox::start + OutboxHandle + shutdown

### Task 8.1: handle.rs — OutboxHandle wrapping pg_work_queue::WorkerHandle

**Files:**
- Create: `src/handle.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write handle.rs**

```rust
//! `OutboxHandle` — owned at `Outbox::start()` time. `shutdown()` drains the
//! worker, then SELECTs pending-delivery count for `OutboxStats`.

use crate::error::ShutdownError;
use crate::outcome::OutboxStats;
use sqlx::PgPool;
use std::time::Duration;

pub use pg_work_queue::Stats;

pub struct OutboxHandle {
    inner: pg_work_queue::WorkerHandle,
    pool: PgPool,
}

impl OutboxHandle {
    pub(crate) fn new(inner: pg_work_queue::WorkerHandle, pool: PgPool) -> Self {
        Self { inner, pool }
    }

    /// Graceful drain with a deadline. Returns pg_work_queue worker stats
    /// plus a count of still-non-terminal handler_deliveries rows.
    pub async fn shutdown(
        self,
        timeout: Duration,
    ) -> Result<(Stats, OutboxStats), ShutdownError> {
        let stats = self.inner.shutdown(timeout).await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries
             WHERE status IN ('queued','running','awaiting_retry')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(ShutdownError::PendingCount)?;
        Ok((
            stats,
            OutboxStats {
                pending_deliveries: u64::try_from(pending).unwrap_or(0),
            },
        ))
    }
}
```

Wire into lib.rs:

```rust
pub mod handle;
pub use crate::handle::{OutboxHandle, Stats};
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/handle.rs src/lib.rs
git commit -m "faza 8: OutboxHandle (shutdown returns Stats + OutboxStats)"
```

---

### Task 8.2: Outbox::start with AtomicBool guard

**Files:**
- Modify: `src/outbox.rs`

- [ ] **Step 1: Append start() to Outbox impl**

```rust
use crate::envelope::HandlerEnvelope;
use crate::error::StartError;
use crate::handle::OutboxHandle;
use crate::runtime::OutboxRuntime;
use std::sync::atomic::Ordering;

impl Outbox {
    pub async fn start(&self) -> Result<OutboxHandle, StartError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(StartError::AlreadyStarted);
        }

        let runtime = Arc::new(OutboxRuntime {
            pool: self.pool.clone(),
            config: self.config.clone(),
            registry: self.registry.clone(),
        });

        let runtime_for_handler = runtime.clone();
        let inner = pg_work_queue::Worker::<HandlerEnvelope>::builder()
            .pool(self.pool.clone())
            .queue("outbox_handler_deliveries")
            .poll_interval(self.config.poll_interval)
            .concurrency(self.config.concurrency)
            .max_attempts(self.config.max_attempts)
            .lease_timeout(self.config.lease_timeout)
            .handler_timeout(self.config.handler_timeout)
            .retry_backoff(self.config.retry_backoff.clone())
            .panic_policy(self.config.panic_policy.clone())
            .handler(move |env: HandlerEnvelope, ctx: pg_work_queue::JobContext| {
                let runtime = runtime_for_handler.clone();
                async move { runtime.handle_envelope(env, ctx).await }
            })
            .build()?
            .start()
            .await?;

        Ok(OutboxHandle::new(inner, self.pool.clone()))
    }
}
```

- [ ] **Step 2: `cargo check`**

Expected: PASS. Note `OutboxConfig: Clone` may need adding (`#[derive(Clone)]` on the struct). pg_work_queue's `BackoffPolicy`/`PanicPolicy` already implement Clone per pgwq spec.

- [ ] **Step 3: Commit**

```bash
git add src/outbox.rs
git commit -m "faza 8: Outbox::start (AtomicBool guard + pgwq Worker wiring)"
```

---

### Task 8.3: start_already_started test + concurrency test

**Files:**
- Create: `tests/concurrency.rs`

- [ ] **Step 1: Tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder, StartError,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ping;
impl DomainEvent for Ping {
    const EVENT_TYPE: &'static str = "test.ping";
}

struct Noop;
#[async_trait::async_trait]
impl EventHandler<Ping> for Noop {
    async fn handle(&self, _: &Ping, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_start_returns_already_started() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool)
        .register_handler::<Ping, _>("audit", Noop)
        .build()
        .unwrap();
    let h = outbox.start().await.unwrap();
    let err = outbox.start().await.unwrap_err();
    assert!(matches!(err, StartError::AlreadyStarted));
    let _ = h.shutdown(std::time::Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test concurrency`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/concurrency.rs
git commit -m "faza 8: second start() returns AlreadyStarted test"
```

---

## Phase 9: History API

### Task 9.1: history.rs

**Files:**
- Create: `src/history.rs`
- Modify: `src/outbox.rs` (add `history()` accessor)
- Modify: `src/lib.rs`

- [ ] **Step 1: Write history.rs**

```rust
//! `History` — read-only queries against `outbox.events` and
//! `outbox.handler_deliveries`. Two endpoints, both bounded.

use crate::error::HistoryError;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

pub struct History<'a> {
    pub(crate) pool: &'a PgPool,
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub id: Uuid,
    pub event_type: String,
    pub producer_bc: String,
    pub tenant_id: String,
    pub payload: Vec<u8>,
    pub headers: serde_json::Map<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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
pub enum DeliveryStatus {
    Queued,
    Running,
    AwaitingRetry,
    Sent,
    Skipped,
    Dead,
}

impl<'a> History<'a> {
    #[tracing::instrument(skip(self), target = "rust_events.history")]
    pub async fn event(
        &self,
        event_id: Uuid,
    ) -> Result<Option<EventRecord>, HistoryError> {
        let row: Option<(
            Uuid,
            String,
            String,
            String,
            Vec<u8>,
            serde_json::Value,
            DateTime<Utc>,
        )> = sqlx::query_as(
            "SELECT id, event_type, producer_bc, tenant_id, payload, headers, created_at
             FROM outbox.events WHERE id = $1",
        )
        .bind(event_id)
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(|(id, et, bc, tid, p, h, c)| EventRecord {
            id,
            event_type: et,
            producer_bc: bc,
            tenant_id: tid,
            payload: p,
            headers: match h {
                serde_json::Value::Object(m) => m,
                _ => serde_json::Map::new(),
            },
            created_at: c,
        }))
    }

    #[tracing::instrument(skip(self), target = "rust_events.history")]
    pub async fn handler_deliveries_for(
        &self,
        event_id: Uuid,
    ) -> Result<Vec<HandlerDeliveryRecord>, HistoryError> {
        let rows: Vec<(
            i64,
            Uuid,
            String,
            DeliveryStatus,
            i32,
            Option<String>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
            Option<DateTime<Utc>>,
        )> = sqlx::query_as(
            "SELECT id, event_id, handler_id, status, attempts, last_error,
                    first_attempted_at, last_attempted_at, finished_at
             FROM outbox.handler_deliveries
             WHERE event_id = $1
             ORDER BY handler_id ASC",
        )
        .bind(event_id)
        .fetch_all(self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, eid, hid, st, a, le, fa, la, fin)| HandlerDeliveryRecord {
                id,
                event_id: eid,
                handler_id: hid,
                status: st,
                attempts: u32::try_from(a).unwrap_or(0),
                last_error: le,
                first_attempted_at: fa,
                last_attempted_at: la,
                finished_at: fin,
            })
            .collect())
    }
}
```

Add to lib.rs:

```rust
pub mod history;
pub use crate::history::{DeliveryStatus, EventRecord, HandlerDeliveryRecord, History};
```

- [ ] **Step 2: Add `history()` accessor on Outbox**

In `src/outbox.rs`:

```rust
impl Outbox {
    #[must_use]
    pub fn history(&self) -> crate::history::History<'_> {
        crate::history::History { pool: &self.pool }
    }
}
```

- [ ] **Step 3: `cargo check`**

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/history.rs src/outbox.rs src/lib.rs
git commit -m "faza 9: History API (event + handler_deliveries_for)"
```

---

### Task 9.2: History integration test

**Files:**
- Create: `tests/history_queries.rs`

- [ ] **Step 1: Test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DeliveryStatus, DispatchContext, DomainEvent, EventHandler, HandlerContext,
    HandlerError, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev {
    x: i32,
}
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.history";
}

struct H;
#[async_trait::async_trait]
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_returns_event_and_deliveries() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("audit", H)
        .register_handler::<Ev, _>("metrics", H)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("t1"), &Ev { x: 7 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let event_id = match outcome {
        rust_events::DispatchOutcome::Dispatched { event_id, .. } => event_id,
        _ => unreachable!(),
    };

    let ev = outbox.history().event(event_id).await.unwrap().unwrap();
    assert_eq!(ev.event_type, "test.history");
    assert_eq!(ev.tenant_id, "t1");

    let deliveries = outbox
        .history()
        .handler_deliveries_for(event_id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].handler_id, "audit"); // alphabetical
    assert_eq!(deliveries[1].handler_id, "metrics");
    assert_eq!(deliveries[0].status, DeliveryStatus::Queued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_event_returns_none_for_unknown_id() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool).build().unwrap();
    let r = outbox.history().event(uuid::Uuid::nil()).await.unwrap();
    assert!(r.is_none());
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test history_queries`
Expected: 2 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/history_queries.rs
git commit -m "faza 9: history integration tests"
```

---

## Phase 10: Purge maintenance

### Task 10.1: purge.rs — three purge functions

**Files:**
- Create: `src/purge.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write purge.rs**

```rust
//! Manual purge functions. Mirror `pg_work_queue::purge` patterns:
//! chunked DELETE via CTE, no background sweeper, `PURGE_CHUNK_SIZE` const.

use crate::error::PurgeError;
use crate::limits::PURGE_CHUNK_SIZE;
use chrono::Utc;
use sqlx::PgPool;
use std::time::Duration;

#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_terminal_deliveries(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT id FROM outbox.handler_deliveries
                WHERE status IN ('sent','dead','skipped') AND finished_at < $1
                ORDER BY finished_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.handler_deliveries
             WHERE id IN (SELECT id FROM victims)",
        )
        .bind(cutoff)
        .bind(PURGE_CHUNK_SIZE as i64)
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_dispatch_keys(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT tenant_id, idempotency_key FROM outbox.dispatch_keys
                WHERE created_at < $1
                ORDER BY created_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.dispatch_keys d
             USING victims v
             WHERE d.tenant_id = v.tenant_id
               AND d.idempotency_key = v.idempotency_key",
        )
        .bind(cutoff)
        .bind(PURGE_CHUNK_SIZE as i64)
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

/// Safe purge: only deletes events with ALL deliveries terminal. CASCADE
/// removes those deliveries + dispatch_keys.
#[tracing::instrument(skip(pool), target = "rust_events.purge")]
pub async fn purge_events(
    pool: &PgPool,
    older_than: Duration,
) -> Result<u64, PurgeError> {
    let cutoff = cutoff_from(older_than);
    let mut total = 0u64;
    loop {
        let n = sqlx::query(
            "WITH victims AS (
                SELECT e.id FROM outbox.events e
                WHERE e.created_at < $1
                  AND NOT EXISTS (
                      SELECT 1 FROM outbox.handler_deliveries hd
                      WHERE hd.event_id = e.id
                        AND hd.status NOT IN ('sent','dead','skipped')
                  )
                ORDER BY e.created_at ASC
                LIMIT $2
             )
             DELETE FROM outbox.events WHERE id IN (SELECT id FROM victims)",
        )
        .bind(cutoff)
        .bind(PURGE_CHUNK_SIZE as i64)
        .execute(pool)
        .await?
        .rows_affected();

        total += n;
        if usize::try_from(n).unwrap_or(0) < PURGE_CHUNK_SIZE {
            break;
        }
    }
    Ok(total)
}

fn cutoff_from(older_than: Duration) -> chrono::DateTime<Utc> {
    Utc::now() - chrono::Duration::from_std(older_than).unwrap_or(chrono::Duration::zero())
}
```

Add to lib.rs:

```rust
pub mod purge;
pub use crate::purge::{purge_dispatch_keys, purge_events, purge_terminal_deliveries};
pub use pg_work_queue::{purge_dead, purge_done};
```

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/purge.rs src/lib.rs
git commit -m "faza 10: purge_terminal_deliveries + purge_dispatch_keys + purge_events"
```

---

### Task 10.2: M4 — purge_events safety tests

**Files:**
- Create: `tests/purge_events_safety.rs`

- [ ] **Step 1: Write tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, purge_events, purge_terminal_deliveries,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.purge";
}

struct Retrying;
#[async_trait::async_trait]
impl EventHandler<Ev> for Retrying {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Err(HandlerError::retry("never going to succeed"))
    }
}

struct OkHandler;
#[async_trait::async_trait]
impl EventHandler<Ev> for OkHandler {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_refuses_event_with_pending_delivery() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("retrying", Retrying)
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Don't even start a worker — delivery is `queued` (non-terminal).
    let deleted = purge_events(&pool, Duration::ZERO).await.unwrap();
    assert_eq!(deleted, 0, "must refuse to purge event with non-terminal delivery");

    let evcount: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(evcount, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_deletes_event_with_all_deliveries_terminal() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("ok", OkHandler)
        .build()
        .unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Wait for delivery to reach 'sent'.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if n == 1 {
            break;
        }
    }

    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    // Now terminal — purge_terminal_deliveries first, then purge_events.
    let d = purge_terminal_deliveries(&pool, Duration::ZERO).await.unwrap();
    assert_eq!(d, 1);

    let e = purge_events(&pool, Duration::ZERO).await.unwrap();
    assert_eq!(e, 1);

    let evcount: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(evcount, 0);
}
```

- [ ] **Step 2: Run**

Run: `cargo test --test purge_events_safety`
Expected: 2 PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/purge_events_safety.rs
git commit -m "faza 10: purge_events NOT EXISTS guard tests (M4)"
```

---

### Task 10.3: M7 — purge API signature test + purge_terminal/keys behavior

**Files:**
- Create: `tests/purge_api_signature.rs`

- [ ] **Step 1: Test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{purge_dispatch_keys, purge_events, purge_terminal_deliveries};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_purge_signatures_no_chunk_size_argument() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Two-arg signature (pool, Duration). If anyone tries to add a third arg
    // the compile fails here.
    let a = purge_terminal_deliveries(&pool, Duration::ZERO).await.unwrap();
    let b = purge_dispatch_keys(&pool, Duration::ZERO).await.unwrap();
    let c = purge_events(&pool, Duration::ZERO).await.unwrap();

    assert_eq!((a, b, c), (0, 0, 0));
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test purge_api_signature`
Expected: PASS.

```bash
git add tests/purge_api_signature.rs
git commit -m "faza 10: purge API signature test (M7)"
```

---

## Phase 11: Critical-fix tests (B1, B2, M1, M2, M3) + worker happy path

### Task 11.1: Worker happy-path test

**Files:**
- Create: `tests/worker_happy_path.rs`

- [ ] **Step 1: Test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev { v: i32 }
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.happy";
}

struct Counting { count: Arc<AtomicUsize> }
#[async_trait::async_trait]
impl EventHandler<Ev> for Counting {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_called_exactly_once_audit_marked_sent() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .build().unwrap())
        .register_handler::<Ev, _>("c", Counting { count: count.clone() })
        .build().unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(&mut tx, &DispatchContext::new("t"), &Ev { v: 1 }).await.unwrap();
    tx.commit().await.unwrap();

    for _ in 0..50 {
        if count.load(Ordering::SeqCst) == 1 { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);

    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    let sent: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(sent, 1);
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test worker_happy_path`
Expected: PASS.

```bash
git add tests/worker_happy_path.rs
git commit -m "faza 11: worker happy-path test"
```

---

### Task 11.2: B2 — delivery_key vs dispatch_idempotency_key

**Files:**
- Create: `tests/handler_context_keys.rs`

- [ ] **Step 1: Test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct E;
impl DomainEvent for E { const EVENT_TYPE: &'static str = "test.b2"; }

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<(Uuid, Option<String>, String)>>>);
#[async_trait::async_trait]
impl EventHandler<E> for Capture {
    async fn handle(&self, _: &E, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let hid = "?".to_string(); // handler_id is not on ctx; capture by closure
        self.0.lock().unwrap().push((
            ctx.delivery_key,
            ctx.dispatch_idempotency_key.clone(),
            hid,
        ));
        Ok(())
    }
}

async fn drain(pool: &sqlx::PgPool, target: i64) {
    for _ in 0..80 {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'"
        ).fetch_one(pool).await.unwrap();
        if n == target { return; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timeout waiting for {target} sent deliveries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b2_two_handlers_distinct_delivery_keys_same_dispatch_key() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cap = Capture(Arc::new(Mutex::new(Vec::new())));
    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100)).build().unwrap())
        .register_handler::<E, _>("h1", cap.clone())
        .register_handler::<E, _>("h2", cap.clone())
        .build().unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(
        &mut tx,
        &DispatchContext::new("acme").with_idempotency_key("order:42"),
        &E,
    ).await.unwrap();
    tx.commit().await.unwrap();

    drain(&pool, 2).await;
    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    let captured = cap.0.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    // Both handlers see the same dispatch_idempotency_key:
    assert_eq!(captured[0].1.as_deref(), Some("order:42"));
    assert_eq!(captured[1].1.as_deref(), Some("order:42"));
    // But distinct delivery_keys:
    assert_ne!(captured[0].0, captured[1].0);
    // Both are valid UUIDv7:
    assert_eq!(captured[0].0.get_version_num(), 7);
    assert_eq!(captured[1].0.get_version_num(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b2_no_idempotency_key_dispatch_yields_none() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cap = Capture(Arc::new(Mutex::new(Vec::new())));
    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100)).build().unwrap())
        .register_handler::<E, _>("h", cap.clone())
        .build().unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(&mut tx, &DispatchContext::new("acme"), &E).await.unwrap();
    tx.commit().await.unwrap();

    drain(&pool, 1).await;
    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();

    let captured = cap.0.lock().unwrap().clone();
    assert_eq!(captured[0].1, None);
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test handler_context_keys`
Expected: 2 PASS.

```bash
git add tests/handler_context_keys.rs
git commit -m "faza 11: B2 delivery_key vs dispatch_idempotency_key distinctness"
```

---

### Task 11.3: B1 — Fencing tests (crash recovery)

**Files:**
- Create: `tests/crash_recovery_fencing.rs`

- [ ] **Step 1: Test scaffolding + one scenario**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev { const EVENT_TYPE: &'static str = "test.b1"; }

/// Sleeps `sleep_ms` then returns the configured `result`.
struct Slow {
    sleep_ms: u64,
    result: HandlerOutcome,
}
#[derive(Clone, Copy)]
enum HandlerOutcome { Ok, Abort }

#[async_trait::async_trait]
impl EventHandler<Ev> for Slow {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        match self.result {
            HandlerOutcome::Ok => Ok(()),
            HandlerOutcome::Abort => Err(HandlerError::abort("late abort")),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_invariant_lease_token_required_iff_running() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Set up an event so FK is satisfied.
    let event_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, payload) VALUES ($1, $2, '\\x'::bytea)"
    ).bind(event_id).bind("test").execute(&pool).await.unwrap();

    // status='running' without lease_token must fail.
    let r = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
            (event_id, handler_id, status, attempts, first_attempted_at, last_attempted_at)
         VALUES ($1, 'h', 'running', 1, now(), now())"
    ).bind(event_id).execute(&pool).await;
    assert!(r.is_err(), "running without lease_token must violate CHECK");

    // status='queued' with lease_token must fail.
    let r2 = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
            (event_id, handler_id, lease_token) VALUES ($1, 'h2', gen_random_uuid())"
    ).bind(event_id).execute(&pool).await;
    assert!(r2.is_err(), "queued with lease_token must violate CHECK");
}

// Full race tests b1_stale_worker_* require precise lease_timeout control and
// a way to start two Outbox instances against the same DB. Pattern:
//   - lease_timeout=1s, handler_timeout=800ms
//   - Outbox A with Slow{sleep_ms: 2000, result: Ok}
//   - Outbox B with normal handler
//   - After ~1.2s, B claims the same job (reaper re-queues)
//   - B finishes fast (status='sent'); when A wakes, mark_sent fenced out.
// The exact orchestration is involved; implementer fleshes out using
// pg_work_queue's reaper behavior. Spec §12 has the recipe.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires careful lease orchestration; flesh out before merging"]
async fn b1_stale_worker_ok_after_concurrent_sent__audit_preserves_concurrent_verdict() {
    // Two Outbox instances against the same pool.
    // Outbox A: Slow handler returning Ok with sleep 2s.
    // Outbox B: same handler_id, normal fast Ok handler.
    // Both started. Dispatch one event. After A claims and starts sleeping,
    // reaper re-queues at lease_expires_at. B claims with new lease_token T_B.
    // B finishes; mark_sent_fenced succeeds (T_B matches).
    // A wakes up; mark_sent_fenced returns rows_affected=0 (T_A no longer in row).
    // Final: status='sent', no overwrite.
    todo!("implement after pgwq lease semantics are confirmed in this repo")
}
```

- [ ] **Step 2: Run (the invariant test must pass; ignored tests pending)**

Run: `cargo test --test crash_recovery_fencing`
Expected: invariant test PASS; one #[ignore]'d test reported.

- [ ] **Step 3: Commit**

```bash
git add tests/crash_recovery_fencing.rs
git commit -m "faza 11: B1 lease_token invariant test (race tests stubbed)"
```

---

### Task 11.4: B1 — fenced_out race tests (implementation pass)

**Files:**
- Modify: `tests/crash_recovery_fencing.rs`

- [ ] **Step 1: Replace the `#[ignore]` tests with working scenarios**

This is the most subtle test in the suite. The pattern: short `lease_timeout` (1s) + short `handler_timeout` (800ms) + handler sleep slightly above lease → reaper re-queues → second claim succeeds → first handler's wake-up mark_* is fenced out.

```rust
async fn b1_two_outbox_race(
    first_result: HandlerOutcome,
    second_result: HandlerOutcome,
    expected_final_status: &str,
) {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Outbox A — slow handler.
    let outbox_a = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .lease_timeout(Duration::from_secs(1))
            .handler_timeout(Duration::from_millis(800))
            .max_attempts(5)
            .build().unwrap())
        .register_handler::<Ev, _>("h", Slow { sleep_ms: 2000, result: first_result })
        .build().unwrap();

    // Outbox B — fast handler with the configured second_result.
    let outbox_b = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .lease_timeout(Duration::from_secs(1))
            .handler_timeout(Duration::from_millis(800))
            .max_attempts(5)
            .build().unwrap())
        .register_handler::<Ev, _>("h", Slow { sleep_ms: 50, result: second_result })
        .build().unwrap();

    let h_a = outbox_a.start().await.unwrap();
    let h_b = outbox_b.start().await.unwrap();

    // Dispatch a single event.
    let mut tx = pool.begin().await.unwrap();
    outbox_a.dispatch(&mut tx, &DispatchContext::new("t"), &Ev).await.unwrap();
    tx.commit().await.unwrap();

    // Wait until status reaches terminal.
    for _ in 0..50 {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM outbox.handler_deliveries LIMIT 1"
        ).fetch_optional(&pool).await.unwrap();
        if matches!(status.as_deref(), Some("sent" | "dead" | "skipped")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Give A time to wake up and try its (fenced) mark.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let final_status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries LIMIT 1"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(final_status, expected_final_status,
        "fencing should preserve the first concurrent terminal verdict");

    let _ = h_a.shutdown(Duration::from_secs(3)).await;
    let _ = h_b.shutdown(Duration::from_secs(3)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_sent_remains_sent() {
    // B finishes Ok first (sent). A wakes up later with Ok — fenced.
    b1_two_outbox_race(HandlerOutcome::Ok, HandlerOutcome::Ok, "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_abort_after_concurrent_sent_remains_sent() {
    // B finishes Ok first. A wakes Abort — fenced.
    b1_two_outbox_race(HandlerOutcome::Abort, HandlerOutcome::Ok, "sent").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_stale_ok_after_concurrent_dead_remains_dead() {
    // B aborts first (dead). A wakes Ok — fenced.
    b1_two_outbox_race(HandlerOutcome::Ok, HandlerOutcome::Abort, "dead").await;
}
```

Remove the `#[ignore]`'d stub.

- [ ] **Step 2: Run**

Run: `cargo test --test crash_recovery_fencing`
Expected: 4 PASS (invariant + 3 race).

NOTE: These tests are timing-sensitive. Run in CI with `--test-threads=1` if flakiness appears. Tweak handler sleep/lease values if necessary.

- [ ] **Step 3: Commit**

```bash
git add tests/crash_recovery_fencing.rs
git commit -m "faza 11: B1 fencing race tests (3 scenarios)"
```

---

### Task 11.5: M1 — Audit row missing test

**Files:**
- Create: `tests/audit_row_missing.rs`

- [ ] **Step 1: Test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev { const EVENT_TYPE: &'static str = "test.m1"; }

struct Trip;
#[async_trait::async_trait]
impl EventHandler<Ev> for Trip {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_missing_row_aborts_with_audit_missing_tracing() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(200))
            .max_attempts(2)
            .build().unwrap())
        .register_handler::<Ev, _>("h", Trip)
        .build().unwrap();
    let handle = outbox.start().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox.dispatch(&mut tx, &DispatchContext::new("t"), &Ev).await.unwrap();
    tx.commit().await.unwrap();

    // Race: delete the row before the worker can pick it up. Polling at 200ms
    // gives us a few ms of headroom right after commit.
    sqlx::query("DELETE FROM outbox.handler_deliveries").execute(&pool).await.unwrap();

    // Wait until pgwq's job is dead (handler-side Abort propagates).
    for _ in 0..30 {
        let dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pgwq.jobs WHERE status='dead'"
        ).fetch_one(&pool).await.unwrap();
        if dead == 1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let dead: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pgwq.jobs WHERE status='dead'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(dead, 1, "pgwq job should be dead after Abort");

    let _ = handle.shutdown(Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test audit_row_missing`
Expected: PASS.

```bash
git add tests/audit_row_missing.rs
git commit -m "faza 11: M1 audit row missing → Abort test"
```

---

### Task 11.6: M2 — Rolling deploy handler-miss tests

**Files:**
- Create: `tests/rolling_deploy_handler_miss.rs`

- [ ] **Step 1: Tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
    OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct NewEv;
impl DomainEvent for NewEv { const EVENT_TYPE: &'static str = "test.m2_new"; }

struct H;
#[async_trait::async_trait]
impl EventHandler<NewEv> for H {
    async fn handle(&self, _: &NewEv, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_loose_handler_added_later_eventually_handled() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // Outbox A: dispatcher only — has the handler so dispatch passes the strict
    // gate. (allow_no_handlers=true alternative also works.)
    let outbox_a = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build().unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox_a.dispatch(&mut tx, &DispatchContext::new("t"), &NewEv).await.unwrap();
    tx.commit().await.unwrap();

    // Outbox B: another worker WITHOUT the handler. Should leave the queued row alone.
    let outbox_b_no_handler = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true) // we don't dispatch from B, only worker
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .strict_handler_lookup(false) // default
            .max_attempts(50)
            .build().unwrap())
        .build().unwrap();
    let h_b = outbox_b_no_handler.start().await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // After 1s of B retrying: handler_deliveries should still be queued, attempts=0.
    let row: (String, i32) = sqlx::query_as(
        "SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, "queued");
    assert_eq!(row.1, 0, "loose mode must not bump attempts when handler missing");

    let _ = h_b.shutdown(Duration::from_secs(2)).await.unwrap();

    // Outbox C: with the handler registered.
    let outbox_c = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .build().unwrap())
        .register_handler::<NewEv, _>("h", H)
        .build().unwrap();
    let h_c = outbox_c.start().await.unwrap();

    for _ in 0..30 {
        let sent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'"
        ).fetch_one(&pool).await.unwrap();
        if sent == 1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let sent: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(sent, 1);

    let _ = h_c.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_strict_handler_missing_dead_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let dispatcher = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .build().unwrap();
    let mut tx = pool.begin().await.unwrap();
    dispatcher.dispatch(&mut tx, &DispatchContext::new("t"), &NewEv).await.unwrap();
    tx.commit().await.unwrap();

    // Worker in strict mode without the handler.
    let strict = OutboxBuilder::new(pool.clone())
        .allow_no_handlers(true)
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .strict_handler_lookup(true)
            .build().unwrap())
        .build().unwrap();
    let h = strict.start().await.unwrap();

    for _ in 0..30 {
        let dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'"
        ).fetch_one(&pool).await.unwrap();
        if dead == 1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let dead: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(dead, 1);

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test rolling_deploy_handler_miss`
Expected: 2 PASS.

```bash
git add tests/rolling_deploy_handler_miss.rs
git commit -m "faza 11: M2 rolling-deploy handler-lookup tests (loose vs strict)"
```

---

### Task 11.7: M3 — decode_error_strategy tests

**Files:**
- Create: `tests/decode_error_strategy.rs`

- [ ] **Step 1: Tests**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DecodeStrategy, DispatchContext, DomainEvent, EventHandler, HandlerContext,
    HandlerError, OutboxBuilder, OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct StrictShape { needed: String }
impl DomainEvent for StrictShape { const EVENT_TYPE: &'static str = "test.m3"; }

struct OkHandler;
#[async_trait::async_trait]
impl EventHandler<StrictShape> for OkHandler {
    async fn handle(&self, _: &StrictShape, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

async fn inject_bad_payload(pool: &sqlx::PgPool) -> uuid::Uuid {
    // Bypass the type-safe dispatch by writing an empty object directly.
    let id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, payload) VALUES ($1, $2, '{}'::text::bytea)"
    ).bind(id).bind("test.m3").execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO outbox.handler_deliveries (event_id, handler_id) VALUES ($1, 'h')"
    ).bind(id).execute(pool).await.unwrap();
    let env = serde_json::to_vec(&serde_json::json!({
        "event_id": id, "handler_id": "h"
    })).unwrap();
    sqlx::query(
        "INSERT INTO pgwq.jobs (queue, payload) VALUES ('outbox_handler_deliveries', $1)"
    ).bind(env).execute(pool).await.unwrap();
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_retry_strategy_bad_payload_eventually_dead() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let _ = inject_bad_payload(&pool).await;

    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .max_attempts(3)
            .decode_error_strategy(DecodeStrategy::Retry)
            .build().unwrap())
        .register_handler::<StrictShape, _>("h", OkHandler)
        .build().unwrap();
    let h = outbox.start().await.unwrap();

    for _ in 0..60 {
        let dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'"
        ).fetch_one(&pool).await.unwrap();
        if dead == 1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status::text, attempts, last_error FROM outbox.handler_deliveries LIMIT 1"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, "dead");
    assert_eq!(row.1, 3, "should have used full retry budget");
    assert!(row.2.unwrap_or_default().contains("decode"));

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_abort_strategy_bad_payload_dead_immediately() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let _ = inject_bad_payload(&pool).await;

    let outbox = OutboxBuilder::new(pool.clone())
        .config(OutboxConfig::builder()
            .poll_interval(Duration::from_millis(100))
            .max_attempts(5)
            .decode_error_strategy(DecodeStrategy::Abort)
            .build().unwrap())
        .register_handler::<StrictShape, _>("h", OkHandler)
        .build().unwrap();
    let h = outbox.start().await.unwrap();

    for _ in 0..30 {
        let dead: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='dead'"
        ).fetch_one(&pool).await.unwrap();
        if dead == 1 { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let row: (String, i32) = sqlx::query_as(
        "SELECT status::text, attempts FROM outbox.handler_deliveries LIMIT 1"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(row.0, "dead");
    assert_eq!(row.1, 1, "abort strategy must NOT retry");

    let _ = h.shutdown(Duration::from_secs(2)).await.unwrap();
}
```

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test decode_error_strategy`
Expected: 2 PASS.

```bash
git add tests/decode_error_strategy.rs
git commit -m "faza 11: M3 decode_error_strategy tests (Retry vs Abort)"
```

---

### Task 11.8: Worker variant tests (retry, abort, skip, schema invariants)

**Files:**
- Create: `tests/worker_retry.rs`, `tests/worker_abort.rs`, `tests/worker_skip.rs`, `tests/schema_invariants.rs`

- [ ] **Step 1: Implement four small test files**

Each file is ~50–80 lines, following the same setup pattern as `worker_happy_path.rs`. Test code is similar to existing patterns; full templates per spec §12.

- `worker_retry.rs`: handler returns `Err(Retry)` on attempt 1, `Ok` on attempt 2. Assert status transitions `queued→running→awaiting_retry→running→sent`, attempts=2.
- `worker_abort.rs`: handler returns `Err(Abort)` on first attempt. Assert status='dead' with attempts=1, last_error captures reason.
- `worker_skip.rs`: handler returns `Err(Skip)`. Assert status='skipped' (terminal), finished_at set, last_error captures reason.
- `schema_invariants.rs`: directly INSERT/UPDATE bad combinations (status='sent' without finished_at, octet_length over limits) — assert CHECK violations.

For each file, follow this skeleton:

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
mod common;
// ... event + handler with the specific behavior ...
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn <descriptive_name>() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    // ... dispatch + start + wait for terminal status + assert ...
}
```

- [ ] **Step 2: Run all four**

Run: `cargo test --test worker_retry --test worker_abort --test worker_skip --test schema_invariants`
Expected: PASS each.

- [ ] **Step 3: Commit**

```bash
git add tests/worker_retry.rs tests/worker_abort.rs tests/worker_skip.rs tests/schema_invariants.rs
git commit -m "faza 11: worker retry/abort/skip + schema invariant tests"
```

---

### Task 11.9: Idempotency race proptest

**Files:**
- Create: `tests/proptest_idempotency.rs`

- [ ] **Step 1: Property-based test**

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use proptest::prelude::*;
use rust_events::{
    DispatchContext, DispatchOutcome, DomainEvent, EventHandler, HandlerContext,
    HandlerError, OutboxBuilder,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
struct E { n: u32 }
impl DomainEvent for E { const EVENT_TYPE: &'static str = "test.proptest"; }

struct H;
#[async_trait::async_trait]
impl EventHandler<E> for H {
    async fn handle(&self, _: &E, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn invariant_unique_events_equals_unique_keys(
        keys in proptest::collection::vec(0u32..10u32, 30)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (_c, pool) = common::pg_container().await;
            pg_work_queue::migrator().run(&pool).await.unwrap();
            rust_events::migrator().run(&pool).await.unwrap();
            let outbox = OutboxBuilder::new(pool.clone())
                .allow_no_handlers(true)
                .build().unwrap();

            let unique: HashSet<_> = keys.iter().copied().collect();
            let mut tasks = Vec::new();
            for k in &keys {
                let key = format!("k:{k}");
                let p = pool.clone();
                let o = &outbox;
                tasks.push(async move {
                    let mut tx = p.begin().await.unwrap();
                    let r = o.dispatch(
                        &mut tx,
                        &DispatchContext::new("t").with_idempotency_key(&key),
                        &E { n: 0 },
                    ).await.unwrap();
                    tx.commit().await.unwrap();
                    r
                });
            }
            let _outcomes: Vec<DispatchOutcome> = futures::future::join_all(tasks).await;

            let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
                .fetch_one(&pool).await.unwrap();
            prop_assert_eq!(events as usize, unique.len());
            Ok(())
        }).unwrap()
    }
}
```

Add `futures = "=0.3.31"` to dev-dependencies if not already present (for `join_all`).

- [ ] **Step 2: Run + Commit**

Run: `cargo test --test proptest_idempotency`
Expected: PASS (8 cases). Slow due to container per case; expected.

```bash
git add tests/proptest_idempotency.rs Cargo.toml
git commit -m "faza 11: proptest idempotency invariant (unique events == unique keys)"
```

---

## Phase 12: lib.rs polish + README + acceptance

### Task 12.1: lib.rs crate-level docs + doctest

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the canonical doctest**

Replace `src/lib.rs` with a full crate-level intro:

```rust
//! Transactional outbox for Rust services on Postgres.
//!
//! See `docs/superpowers/specs/2026-05-13-rust-events-design.md` for design.
//!
//! # Quick start
//!
//! ```no_run
//! use rust_events::{
//!     DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError,
//!     OutboxBuilder,
//! };
//! use serde::{Deserialize, Serialize};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize)]
//! struct OrderCreated { order_id: i64, amount: i64 }
//!
//! impl DomainEvent for OrderCreated {
//!     const EVENT_TYPE: &'static str = "shop.order_created";
//! }
//!
//! struct Auditor;
//!
//! #[async_trait::async_trait]
//! impl EventHandler<OrderCreated> for Auditor {
//!     async fn handle(
//!         &self,
//!         _event: &OrderCreated,
//!         _ctx: &HandlerContext,
//!     ) -> Result<(), HandlerError> {
//!         Ok(())
//!     }
//! }
//!
//! # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! pg_work_queue::migrator().run(&pool).await?;
//! rust_events::migrator().run(&pool).await?;
//!
//! let outbox = OutboxBuilder::new(pool.clone())
//!     .register_handler::<OrderCreated, _>("audit", Auditor)
//!     .build()?;
//!
//! let handle = outbox.start().await?;
//!
//! let mut tx = pool.begin().await?;
//! outbox.dispatch(
//!     &mut tx,
//!     &DispatchContext::new("acme")
//!         .with_producer_bc("shop")
//!         .with_idempotency_key("order:42"),
//!     &OrderCreated { order_id: 42, amount: 100 },
//! ).await?;
//! tx.commit().await?;
//!
//! let (_pgwq_stats, _outbox_stats) = handle.shutdown(Duration::from_secs(10)).await?;
//! # Ok(()) }
//! ```
#![doc(html_root_url = "https://docs.rs/rust_events/0.1.0")]

pub mod builder;
pub mod dispatch_context;
pub(crate) mod envelope;
pub mod error;
pub mod handle;
pub mod handler;
pub mod history;
pub mod limits;
pub mod migrator;
pub mod outbox;
pub mod outcome;
pub mod purge;
pub(crate) mod registry;
pub(crate) mod runtime;
pub(crate) mod util;

pub use crate::builder::{
    BackoffPolicy, DecodeStrategy, OutboxBuilder, OutboxConfig, OutboxConfigBuilder, PanicPolicy,
};
pub use crate::dispatch_context::DispatchContext;
pub use crate::error::{
    BuildError, DispatchError, HistoryError, PurgeError, ShutdownError, StartError,
};
pub use crate::handle::{OutboxHandle, Stats};
pub use crate::handler::{DomainEvent, EventHandler, HandlerContext, HandlerError};
pub use crate::history::{DeliveryStatus, EventRecord, HandlerDeliveryRecord, History};
pub use crate::migrator::migrator;
pub use crate::outbox::Outbox;
pub use crate::outcome::{DispatchOutcome, OutboxStats};
pub use crate::purge::{purge_dispatch_keys, purge_events, purge_terminal_deliveries};

pub use pg_work_queue::{purge_dead, purge_done};
```

- [ ] **Step 2: `cargo test --doc`**

Run: `cargo test --doc`
Expected: PASS.

- [ ] **Step 3: `cargo doc --no-deps`**

Run: `cargo doc --no-deps`
Expected: zero warnings.

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs
git commit -m "faza 12: lib.rs crate-level docs + doctest"
```

---

### Task 12.2: README

**Files:**
- Create/Replace: `README.md`

- [ ] **Step 1: Write README**

Structure (~300–500 lines, mirror pg_work_queue's README format):

1. Header + tagline
2. Status (v0.1 pre-publish, PG18+, Rust 1.85+, MIT)
3. Table of contents
4. "What this crate is" / "is not" (from spec §1)
5. Quick start (the doctest from lib.rs, slightly expanded)
6. Architecture diagram (from spec §4)
7. Delivery semantics: at-least-once + fencing tokens
8. State machine and schema (from spec §5)
9. API reference (link to docs.rs; brief summary of each module)
10. Tracing / observability (from spec §13)
11. Design decisions (key choices: B1 UUID PK, eager fanout, decode_error_strategy default, allow_no_handlers default)
12. Known limitations (from spec §16)
13. Testing (how to run tests; Docker requirement)
14. License (MIT)

Reference the spec at the top: "Full design rationale in `docs/superpowers/specs/2026-05-13-rust-events-design.md`."

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "faza 12: README"
```

---

### Task 12.3: Final CI checks + acceptance gate

- [ ] **Step 1: Run full clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: zero warnings.

- [ ] **Step 2: Run full test suite**

Run: `cargo test`
Expected: ALL PASS (~140–170 tests; takes 5–15 minutes due to per-test containers).

- [ ] **Step 3: Run doctest**

Run: `cargo test --doc`
Expected: PASS.

- [ ] **Step 4: Verify acceptance criteria (spec §18)**

Manually check each bullet:
- `cargo clippy --all-targets -- -D warnings` ✓
- `cargo test` all pass ✓
- `cargo test --doc` ✓
- `cargo doc --no-deps` no warnings ✓
- No `unsafe` in non-test code: `rg 'unsafe' src/` → empty
- No `unwrap` in non-test code: `rg '\.unwrap\(\)' src/` → empty
- Versions pinned exactly (check Cargo.toml)
- README covers required sections

- [ ] **Step 5: Tag v0.1.0 release candidate**

```bash
git tag -a v0.1.0-rc1 -m "rust_events v0.1.0-rc1"
```

(Don't push the tag until external review.)

---

## Self-Review (pre-handoff)

**Spec coverage check:**

| Spec section | Implemented in phase/task |
|---|---|
| §3 Constraints | Throughout — only-public-API enforced per task |
| §5 Schema | Phase 1 (migration) + Phase 11 (invariant tests) |
| §6 API | Phases 3–10 |
| §7 Dispatch | Phase 6 |
| §8 Worker | Phase 7 |
| §9 Errors | Task 3.5 + per-phase additions |
| §10 History | Phase 9 |
| §11 Purge | Phase 10 |
| §12 Testing | Phases 5, 6, 8, 9, 10, 11 |
| §13 Observability | Per-phase tracing instrumentation |
| §14 Module layout | Tasks 12.1 (lib.rs ordering) + per-phase file creation |
| §15 Deps | Task 0.1 |
| §16 Known limitations | Task 12.2 (README) |
| §18 Acceptance | Task 12.3 |

**Type-consistency check:**

- `DispatchContext` constructors match across §6 spec and `dispatch_context.rs` task: `new(tenant_id)` + chainable `with_*`.
- `HandlerContext.delivery_key` and `dispatch_idempotency_key` consistent across handler.rs, runtime.rs CTE SELECT, and tests in Task 11.2.
- `mark_*_fenced` SQL signatures: all UPDATE templates use the same `(event_id, handler_id, lease_token)` WHERE shape.
- `DispatchOutcome::NoHandlers` shape (one field `event_id`) consistent between outcome.rs and tests in Task 6.3.
- `delivery_status` ENUM values match Rust `DeliveryStatus` variants (5 variants — `Queued`, `Running`, `AwaitingRetry`, `Sent`, `Skipped`, `Dead` — 6 with `Skipped` added per spec revision; verify the enum in `history.rs` has all 6).

If implementer notices a mismatch with the spec while executing, treat it as a spec ambiguity → pause, surface, update spec, then continue.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-13-rust-events.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**

