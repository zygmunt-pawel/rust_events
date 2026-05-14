#![allow(missing_docs)]
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

    let migration_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
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
