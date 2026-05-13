//! Type-erased handler registry for runtime dispatch.
#![allow(clippy::redundant_pub_crate)]

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use crate::handler::{DomainEvent, EventHandler, HandlerContext, HandlerError};

/// Object-safe async handler that operates on raw bytes.
///
/// Each concrete `TypedHandler<E, H>` implements this trait, allowing the
/// registry to store heterogeneous handler types behind a single pointer.
#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync + 'static {
    /// Deserialize `payload` as the concrete event type and invoke the
    /// underlying [`EventHandler`].
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError>;
}

/// A concrete handler `H` for event type `E`, stored type-erased in the
/// [`Registry`].
///
/// `PhantomData<fn() -> E>` gives covariance in `E` without imposing
/// `Send`/`Sync` constraints through ownership of an `E` value.
pub(crate) struct TypedHandler<E, H> {
    pub(crate) inner: Arc<H>,
    pub(crate) _e: PhantomData<fn() -> E>,
}

#[async_trait::async_trait]
impl<E, H> ErasedHandler for TypedHandler<E, H>
where
    E: DomainEvent,
    H: EventHandler<E>,
{
    async fn handle_erased(
        &self,
        payload: &[u8],
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError> {
        let event: E = serde_json::from_slice(payload).map_err(|e| {
            HandlerError::abort(format!("decode {}: {e}", E::EVENT_TYPE))
        })?;
        self.inner.handle(&event, ctx).await
    }
}

/// In-memory registry mapping handler IDs to type-erased handlers.
///
/// Populated at `Worker` build time; immutable during dispatch.
/// Held behind an `Arc<Registry>` by the worker.
pub(crate) struct Registry {
    /// Primary lookup: `handler_id` → erased handler.
    pub(crate) handlers: HashMap<String, Arc<dyn ErasedHandler>>,
    /// Secondary index: `event_type` → list of registered `handler_id`s.
    pub(crate) by_type: HashMap<&'static str, Vec<String>>,
}

impl Registry {
    /// Creates an empty registry.
    pub(crate) fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            by_type: HashMap::new(),
        }
    }

    /// Look up a handler by its stable `handler_id`.
    pub(crate) fn lookup(&self, handler_id: &str) -> Option<&Arc<dyn ErasedHandler>> {
        self.handlers.get(handler_id)
    }

    /// Return the list of handler IDs registered for the given `event_type`.
    ///
    /// Returns an empty slice when no handlers are registered for that type.
    pub(crate) fn handler_ids_for(&self, event_type: &'static str) -> &[String] {
        self.by_type
            .get(event_type)
            .map_or(&[], Vec::as_slice)
    }
}
