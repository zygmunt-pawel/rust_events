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
