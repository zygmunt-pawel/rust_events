#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use uuid::Uuid;

/// Extract the 5-character SQLSTATE code from a `sqlx::Error`, if available.
fn sqlstate(e: &sqlx::Error) -> Option<String> {
    if let sqlx::Error::Database(db) = e {
        db.code().map(|c| c.to_string())
    } else {
        None
    }
}

/// Insert a minimal valid `outbox.events` row and return its UUID.
async fn insert_valid_event(pool: &sqlx::PgPool) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox.events (id, event_type, producer_bc, tenant_id, payload, headers)
         VALUES ($1, 'test.inv', 'bc', 'ten', '\\x7b7d', '{}'::jsonb)",
    )
    .bind(id)
    .execute(pool)
    .await
    .unwrap();
    id
}

// ────────────────────────────────────────────────────────────────────────────
// 1. sent + finished_at IS NULL → CHECK violation (status_invariants)
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sent_without_finished_at_violates_check() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let event_id = insert_valid_event(&pool).await;

    let err = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
             (event_id, handler_id, status, attempts, finished_at)
         VALUES ($1, 'h', 'sent', 1, NULL)",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .unwrap_err();

    let code = sqlstate(&err).unwrap_or_default();
    // SQLSTATE 23514 = CHECK violation
    assert!(
        code.starts_with("23"),
        "expected SQLSTATE 23xxx (constraint violation), got: {code} — error: {err}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 2. running + lease_token IS NULL → CHECK violation (status_invariants)
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_without_lease_token_violates_check() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let event_id = insert_valid_event(&pool).await;

    let err = sqlx::query(
        "INSERT INTO outbox.handler_deliveries
             (event_id, handler_id, status, attempts, first_attempted_at,
              last_attempted_at, lease_token)
         VALUES ($1, 'h', 'running', 1, now(), now(), NULL)",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .unwrap_err();

    let code = sqlstate(&err).unwrap_or_default();
    assert!(
        code.starts_with("23"),
        "expected SQLSTATE 23xxx (constraint violation), got: {code} — error: {err}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 3. event_type longer than 128 bytes → CHECK violation
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_type_over_128_bytes_violates_check() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    // 200 ASCII characters → 200 bytes > 128.
    let long_type = "x".repeat(200);

    let err = sqlx::query(
        "INSERT INTO outbox.events (id, event_type, producer_bc, tenant_id, payload, headers)
         VALUES ($1, $2, '', '', '\\x7b7d', '{}'::jsonb)",
    )
    .bind(Uuid::now_v7())
    .bind(&long_type)
    .execute(&pool)
    .await
    .unwrap_err();

    let code = sqlstate(&err).unwrap_or_default();
    assert!(
        code.starts_with("23"),
        "expected SQLSTATE 23xxx (constraint violation), got: {code} — error: {err}"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 4. Postgres `outbox.delivery_status` enum ↔ Rust `DeliveryStatus` variants
//    are exactly the same set. Catches any drift where a status string
//    appears in a SQL literal in src/ but no matching enum variant exists,
//    or vice-versa.
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_status_enum_round_trip() {
    use std::collections::BTreeSet;

    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let pg_labels: Vec<String> = sqlx::query_scalar(
        "SELECT e.enumlabel
           FROM pg_type t
           JOIN pg_enum e ON e.enumtypid = t.oid
           JOIN pg_namespace n ON n.oid = t.typnamespace
          WHERE n.nspname = 'outbox' AND t.typname = 'delivery_status'
          ORDER BY e.enumsortorder",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    let pg_set: BTreeSet<String> = pg_labels.iter().cloned().collect();
    let rust_set: BTreeSet<String> = rust_events::DeliveryStatus::ALL
        .iter()
        .map(|s| s.as_str().to_owned())
        .collect();

    assert_eq!(
        pg_set,
        rust_set,
        "Postgres outbox.delivery_status labels and Rust DeliveryStatus variants drifted.\n\
         pg only: {:?}\n\
         rust only: {:?}",
        pg_set.difference(&rust_set).collect::<Vec<_>>(),
        rust_set.difference(&pg_set).collect::<Vec<_>>(),
    );
}

// ────────────────────────────────────────────────────────────────────────────
// 5. UPDATE outbox.events → denied by deny_update_events trigger (P0001)
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_events_blocked_by_trigger() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let event_id = insert_valid_event(&pool).await;

    let err = sqlx::query("UPDATE outbox.events SET event_type='x' WHERE id=$1")
        .bind(event_id)
        .execute(&pool)
        .await
        .unwrap_err();

    // deny_update() raises EXCEPTION → SQLSTATE P0001.
    let code = sqlstate(&err).unwrap_or_default();
    assert_eq!(
        code, "P0001",
        "expected SQLSTATE P0001 (raise_exception), got: {code} — error: {err}"
    );
}
