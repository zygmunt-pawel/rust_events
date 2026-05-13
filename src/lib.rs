//! Transactional outbox library for Rust services on Postgres.
//!
//! See `docs/superpowers/specs/2026-05-13-rust-events-design.md` for design.
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

pub mod builder;
pub use crate::builder::{
    BackoffPolicy, DecodeStrategy, OutboxBuilder, OutboxConfig, OutboxConfigBuilder, PanicPolicy,
};

pub mod outbox;
pub use crate::outbox::Outbox;
