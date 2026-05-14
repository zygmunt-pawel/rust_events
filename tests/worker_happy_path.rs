#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod common;

use rust_events::{
    DispatchContext, DomainEvent, EventHandler, HandlerContext, HandlerError, OutboxBuilder,
    OutboxConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Serialize, Deserialize)]
struct OrderShipped {
    order_id: i64,
}

impl DomainEvent for OrderShipped {
    const EVENT_TYPE: &'static str = "shop.order_shipped";
}

#[derive(Clone)]
struct Counter(Arc<AtomicUsize>);

impl EventHandler<OrderShipped> for Counter {
    async fn handle(&self, _: &OrderShipped, _: &HandlerContext) -> Result<(), HandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_called_exactly_once_audit_marked_sent() {
    let (_c, pool) = common::pg_container().await;
    pg_work_queue::migrator().run(&pool).await.unwrap();
    rust_events::migrator().run(&pool).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let cfg = OutboxConfig::builder()
        .concurrency(1)
        .poll_interval(std::time::Duration::from_millis(100))
        .build()
        .unwrap();

    let outbox = OutboxBuilder::new(pool.clone())
        .config(cfg)
        .register_handler::<OrderShipped, _>("audit", Counter(counter.clone()))
        .build()
        .unwrap();

    // Dispatch event in its own transaction.
    let mut tx = pool.begin().await.unwrap();
    outbox
        .dispatch(
            &mut tx,
            &DispatchContext::new("acme"),
            &OrderShipped { order_id: 1 },
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Start worker and let it process the delivery.
    let handle = outbox.start().await.unwrap();

    // Poll until handler_deliveries has status='sent' (up to 5 s).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let sent: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox.handler_deliveries WHERE status='sent'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if sent >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for delivery to become sent"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    handle
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();

    // Handler called exactly once.
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Audit row has status='sent'.
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM outbox.handler_deliveries WHERE handler_id='audit'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "sent");
}
