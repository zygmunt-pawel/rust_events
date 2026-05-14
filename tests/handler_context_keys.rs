#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Ev {
    v: i32,
}

impl DomainEvent for Ev {
    const EVENT_TYPE: &'static str = "test.ctx_keys";
}

type Captured = Arc<Mutex<Vec<(Uuid, Option<String>)>>>;

#[derive(Clone)]
struct Capture(Captured);

impl EventHandler<Ev> for Capture {
    async fn handle(&self, _: &Ev, ctx: &HandlerContext) -> Result<(), HandlerError> {
        self.0
            .lock()
            .unwrap()
            .push((ctx.delivery_key, ctx.dispatch_idempotency_key.clone()));
        Ok(())
    }
}

/// Poll `handler_deliveries` until `target` rows have `status='sent'`.
async fn drain(pool: &sqlx::PgPool, target: i64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if sent >= target {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {target} sent deliveries (got {sent})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

const fn is_uuidv7(u: Uuid) -> bool {
    // Version bits are in the high nibble of byte 6 (0x70 = version 7).
    u.get_version_num() == 7
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b2_two_handlers_distinct_delivery_keys_same_dispatch_key() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cap1 = Capture(Arc::new(Mutex::new(Vec::new())));
    let cap2 = Capture(Arc::new(Mutex::new(Vec::new())));

    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(std::time::Duration::from_millis(100))
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("h1", cap1.clone())
        .register_handler::<Ev, _>("h2", cap2.clone())
        .build()
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme").with_idempotency_key("order:42"),
            &Ev { v: 1 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = outbox.start().await.unwrap();
    drain(&pool, 2).await;
    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let data1 = cap1.0.lock().unwrap().clone();
    let data2 = cap2.0.lock().unwrap().clone();

    assert_eq!(data1.len(), 1, "h1 should be called exactly once");
    assert_eq!(data2.len(), 1, "h2 should be called exactly once");

    let (key1, disp1) = &data1[0];
    let (key2, disp2) = &data2[0];

    // Distinct delivery keys — each handler gets its own UUIDv7.
    assert_ne!(key1, key2, "delivery_keys must differ between handlers");

    // Same dispatch_idempotency_key for both.
    assert_eq!(
        disp1,
        &Some("order:42".to_string()),
        "h1 must see dispatch_idempotency_key=Some('order:42')"
    );
    assert_eq!(
        disp2,
        &Some("order:42".to_string()),
        "h2 must see dispatch_idempotency_key=Some('order:42')"
    );

    // Both delivery keys are UUIDv7.
    assert!(is_uuidv7(*key1), "h1 delivery_key must be UUIDv7");
    assert!(is_uuidv7(*key2), "h2 delivery_key must be UUIDv7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b2_no_idempotency_key_dispatch_yields_none() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let cap = Capture(Arc::new(Mutex::new(Vec::new())));

    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(std::time::Duration::from_millis(100))
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<Ev, _>("h_nokey", cap.clone())
        .build()
        .unwrap();

    // Dispatch WITHOUT idempotency_key.
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(&mut tx, &DispatchContext::new("acme"), &Ev { v: 2 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let handle = outbox.start().await.unwrap();
    drain(&pool, 1).await;
    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    let data = cap.0.lock().unwrap().clone();
    assert_eq!(data.len(), 1);

    let (delivery_key, disp) = &data[0];

    // No idempotency key → dispatch_idempotency_key is None.
    assert_eq!(disp, &None, "dispatch_idempotency_key must be None");

    // delivery_key is still a valid non-nil UUIDv7.
    assert!(!delivery_key.is_nil(), "delivery_key must not be nil");
    assert!(is_uuidv7(*delivery_key), "delivery_key must be UUIDv7");
}
