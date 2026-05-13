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

pub mod limits;
pub(crate) mod util;
pub mod migrator;
pub use crate::migrator::migrator;

pub mod handler;
pub use crate::handler::{DomainEvent, EventHandler, HandlerContext, HandlerError};

pub mod dispatch_context;
pub use crate::dispatch_context::DispatchContext;

pub mod outcome;
pub use crate::outcome::{DispatchOutcome, OutboxStats};

pub mod error;
pub use crate::error::{
    BuildError, DispatchError, HistoryError, PurgeError, ShutdownError, StartError,
};

pub(crate) mod envelope;
pub(crate) mod registry;
pub(crate) mod runtime;

pub mod builder;
pub use crate::builder::{
    BackoffPolicy, DecodeStrategy, OutboxBuilder, OutboxConfig, OutboxConfigBuilder, PanicPolicy,
};

pub mod handle;
pub use crate::handle::{OutboxHandle, Stats};

pub mod history;
pub use crate::history::{DeliveryStatus, EventRecord, HandlerDeliveryRecord, History};

pub mod outbox;
pub use crate::outbox::Outbox;

pub mod purge;
pub use crate::purge::{purge_dispatch_keys, purge_events, purge_terminal_deliveries};
pub use pg_work_queue::{purge_dead, purge_done};
