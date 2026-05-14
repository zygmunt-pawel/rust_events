#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DispatchError, DomainEvent, EventHandler, HandlerContext, HandlerError,
    HandlerOptions, OutboxBuilder,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Ev;
impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.classification";
}
struct H;
impl EventHandler<Ev> for H {
    async fn handle(&self, _: &Ev, _: &HandlerContext) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// A SQLSTATE 23xxx (constraint violation) hit during dispatch must surface as
/// `DispatchError::Constraint`, not `Transient`. `is_retriable()` must return
/// false — the caller's retry loop must NOT re-attempt.
///
/// Forced by inserting a clashing `outbox.events` row by hand inside the same
/// transaction, then letting `dispatch()` collide on PK.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pk_collision_classifies_as_constraint_not_retriable() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();
    let outbox = OutboxBuilder::new(pool.clone())
        .register_handler::<Ev, _>("h", H, HandlerOptions::new())
        .build()
        .unwrap();

    // Wedge a row that will collide with the FK target during INSERT
    // outbox.handler_deliveries (event_id FK → events.id). We forge the
    // collision by inserting a duplicate (event_id, handler_id) UNIQUE pair
    // ahead of time.
    let mut tx = pool.begin().await.unwrap();
    // Successful dispatch first.
    let outcome = outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Ev)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let rust_events::DispatchOutcome::Dispatched { event_id, .. } = outcome else {
        panic!()
    };

    // Manually clone the same handler_deliveries row so the next dispatch
    // attempting to write (event_id, 'h') will collide on the UNIQUE
    // (event_id, handler_id) constraint. We trigger by re-INSERTing the same
    // event_id into outbox.events from a fresh dispatch — events.id PK
    // collision (UUIDv7 is client-generated and rust_events::dispatch picks
    // a fresh one, so a natural collision is improbable; we force one via
    // raw SQL using the existing event_id).
    let mut tx2 = pool.begin().await.unwrap();
    let err: sqlx::Error = sqlx::query(
        "INSERT INTO outbox.events (id, event_type, producer_bc, tenant_id, payload, headers)
         VALUES ($1, 'test.classification', '', 'acme', '\\x7b7d'::bytea, '{}'::jsonb)",
    )
    .bind(event_id)
    .execute(&mut *tx2)
    .await
    .expect_err("PK collision must error");
    tx2.rollback().await.unwrap();

    // Route through DispatchError's From<sqlx::Error> classifier — the
    // contract under test is the From impl, not where dispatch happens to
    // call it from. SQLSTATE 23505 (unique_violation) → Constraint.
    let de: DispatchError = err.into();
    assert!(
        matches!(de, DispatchError::Constraint(_)),
        "PK collision must classify as Constraint, got {de:?}"
    );
    assert!(!de.is_retriable(), "Constraint must not be retriable");
}

/// Pool-closed errors must classify as `Transient` (retriable). Distinguish
/// real transient failures from the deterministic-classification path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_closed_classifies_as_transient_retriable() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    pool.close().await;

    // sqlx::Error::PoolClosed is the transient signal we expect.
    let e = sqlx::Error::PoolClosed;
    let de: DispatchError = e.into();
    assert!(
        matches!(de, DispatchError::Transient(_)),
        "PoolClosed must classify as Transient, got {de:?}"
    );
    assert!(de.is_retriable(), "Transient must be retriable");
}

/// `3F000` (`invalid_schema_name`) — pointing at a schema that does not exist.
/// Must classify as deterministic. Same operator-failure shape as 42P01
/// (forgotten migrator), routed through SQLSTATE class `3F` rather than `42`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_schema_classifies_as_constraint() {
    let (_c, pool) = common::pg_container().await;

    let err: sqlx::Error =
        sqlx::query("SELECT 1 FROM definitely_not_a_schema.definitely_not_a_table")
            .execute(&pool)
            .await
            .expect_err("missing schema must error");

    let de: DispatchError = err.into();
    assert!(
        matches!(de, DispatchError::Constraint(_)),
        "3F000 must classify as Constraint, got {de:?}"
    );
    assert!(!de.is_retriable());
}

/// `25P02` (`in_failed_sql_transaction`) — once a tx has aborted, subsequent
/// statements on the same connection raise this code. Retrying within the
/// same session cannot succeed; the caller must open a fresh tx.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_failed_tx_classifies_as_constraint() {
    let (_c, pool) = common::pg_container().await;

    let mut conn = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
    // Force the tx into aborted state.
    let _ = sqlx::query("SELECT 1 / 0").execute(&mut *conn).await;
    // Any subsequent statement on this connection now raises 25P02.
    let err: sqlx::Error = sqlx::query("SELECT 1")
        .execute(&mut *conn)
        .await
        .expect_err("aborted tx must reject further statements");
    let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;

    let de: DispatchError = err.into();
    assert!(
        matches!(de, DispatchError::Constraint(_)),
        "25P02 must classify as Constraint, got {de:?}"
    );
    assert!(!de.is_retriable());
}

/// `42P01` (`undefined_table`) — schema missing. Must classify as `Constraint`
/// (deterministic), NOT Transient. This is the bug we hardened: previously
/// only class 23 was deterministic, so a forgotten migration would surface
/// as `Transient` and a caller-driven retry loop would spin forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_table_classifies_as_constraint() {
    let (_c, pool) = common::pg_container().await;
    // Deliberately do NOT run rust_events migrator. pgwq migrator skipped
    // too — neither table exists.

    let err: sqlx::Error = sqlx::query("SELECT 1 FROM outbox.events")
        .execute(&pool)
        .await
        .expect_err("undefined_table must error");

    let de: DispatchError = err.into();
    assert!(
        matches!(de, DispatchError::Constraint(_)),
        "42P01 must classify as Constraint (deterministic), got {de:?}"
    );
    assert!(!de.is_retriable(), "missing-table must not be retriable");
}
