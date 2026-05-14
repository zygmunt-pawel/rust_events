#![allow(missing_docs, unreachable_pub)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use sqlx::PgPool;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Spin up a fresh PG18 container and return a connected pool.
/// Container handle MUST be held by the test (drop = stop container).
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
