#![allow(missing_docs, unreachable_pub)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Spin up a fresh PG18 container and return a connected pool.
/// Container handle MUST be held by the test (drop = stop container).
// `common` is compiled independently into every test binary; a binary that
// only needs `pg_container_with_pool` would otherwise see this as dead code.
#[allow(dead_code)]
pub async fn pg_container() -> (ContainerAsync<Postgres>, PgPool) {
    let container: ContainerAsync<Postgres> = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    (container, pool)
}

/// Like [`pg_container`] but the pool is built with an explicit
/// `max_connections`. Used to exercise pool-size validation paths.
// Not used by every test binary that links `common` — each binary compiles
// the module independently, so binaries that don't call this see dead code.
#[allow(dead_code)]
pub async fn pg_container_with_pool(max_connections: u32) -> (ContainerAsync<Postgres>, PgPool) {
    let container: ContainerAsync<Postgres> = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .unwrap();
    (container, pool)
}
