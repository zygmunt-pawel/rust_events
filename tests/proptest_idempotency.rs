#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use proptest::prelude::*;
use rust_events::{DispatchContext, DispatchOutcome, DomainEvent, OutboxBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
struct E {
    n: u32,
}
impl DomainEvent for E {
    const EVENT_TYPE: &'static str = "test.proptest";
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    /// For N dispatches drawing keys from a pool of size M/3, the number of
    /// persisted events must equal the number of distinct keys used. This is
    /// the core idempotency invariant: duplicate keys deduplicate to one event.
    ///
    /// NOTE: Each test case spins up a fresh Postgres container (~10 s). With
    /// 8 cases, total runtime is approximately 80 s. This test is intentionally
    /// slow — it verifies the invariant under a variety of generated key sets.
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
                .build()
                .unwrap();

            let unique: HashSet<_> = keys.iter().copied().collect();
            let mut tasks = Vec::new();
            for k in &keys {
                let key = format!("k:{k}");
                let p = pool.clone();
                let o = &outbox;
                tasks.push(async move {
                    let mut tx = p.begin().await.unwrap();
                    let r = o
                        .dispatch(
                            &mut tx,
                            &DispatchContext::new("t").with_idempotency_key(&key),
                            &E { n: 0 },
                        )
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                    r
                });
            }
            let _outcomes: Vec<DispatchOutcome> = futures::future::join_all(tasks).await;

            let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox.events")
                .fetch_one(&pool)
                .await
                .unwrap();
            prop_assert_eq!(usize::try_from(events).unwrap_or(0), unique.len());
            Ok(())
        }).unwrap();
    }
}
