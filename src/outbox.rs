//! `Outbox` runtime — populated fully in Phase 6.

use crate::builder::OutboxConfig;
use crate::registry::Registry;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// The transactional outbox runtime. Created via [`crate::builder::OutboxBuilder`].
#[allow(dead_code)] // pool/config/registry used in Phase 6 (dispatch) + Phase 8 (start)
pub struct Outbox {
    pub(crate) pool: PgPool,
    pub(crate) config: OutboxConfig,
    pub(crate) registry: Arc<Registry>,
    pub(crate) allow_no_handlers: bool,
    pub(crate) started: AtomicBool,
}

impl std::fmt::Debug for Outbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox")
            .field("allow_no_handlers", &self.allow_no_handlers)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl Outbox {
    pub(crate) const fn new(
        pool: PgPool,
        config: OutboxConfig,
        registry: Arc<Registry>,
        allow_no_handlers: bool,
    ) -> Self {
        Self {
            pool,
            config,
            registry,
            allow_no_handlers,
            started: AtomicBool::new(false),
        }
    }
}
