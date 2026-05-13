//! Per-dispatch caller context. NO `Default` impl — `tenant_id` must be set
//! explicitly to prevent silent multi-tenant data leaks via `..Default::default()`.

/// Caller-supplied context for a single `Outbox::dispatch()` call.
///
/// Constructed via `DispatchContext::new(tenant_id)` and refined via chainable
/// `with_*` methods. NO `Default` impl by design — every dispatch must
/// explicitly choose a `tenant_id` (use `"default"` for single-tenant apps).
#[derive(Debug, Clone)]
pub struct DispatchContext<'a> {
    tenant_id: &'a str,
    producer_bc: &'a str,
    idempotency_key: Option<&'a str>,
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

impl<'a> DispatchContext<'a> {
    /// Construct with required `tenant_id`. For single-tenant deployments
    /// pass `"default"` or your application slug.
    #[must_use]
    pub const fn new(tenant_id: &'a str) -> Self {
        Self {
            tenant_id,
            producer_bc: "",
            idempotency_key: None,
            headers: None,
        }
    }

    /// Set the bounded context that produced this event.
    #[must_use]
    pub const fn with_producer_bc(mut self, bc: &'a str) -> Self {
        self.producer_bc = bc;
        self
    }

    /// Set a caller-supplied idempotency key for this dispatch.
    ///
    /// # Scope
    ///
    /// The key is **per-`tenant_id`, not per-event-type**. Reusing the same
    /// key across different `DomainEvent` types within one tenant collapses
    /// them to a single event — the second dispatch returns
    /// [`crate::outcome::DispatchOutcome::Duplicate`] pointing at the first
    /// event, regardless of the Rust type. This is intentional (the outbox
    /// keys on business-level identity, not on Rust type), but it means
    /// callers MUST encode any per-type dimension into the key themselves:
    ///
    /// ```ignore
    /// let key = format!("{}:{}", E::EVENT_TYPE, business_id);
    /// ctx.with_idempotency_key(&key)
    /// ```
    #[must_use]
    pub const fn with_idempotency_key(mut self, key: &'a str) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Attach arbitrary key-value metadata headers to the dispatched event.
    #[must_use]
    pub fn with_headers(
        mut self,
        headers: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.headers = Some(headers);
        self
    }

    /// The tenant identifier for this dispatch.
    #[must_use]
    pub const fn tenant_id(&self) -> &str {
        self.tenant_id
    }

    /// The bounded context that produced this event.
    #[must_use]
    pub const fn producer_bc(&self) -> &str {
        self.producer_bc
    }

    /// The caller-supplied idempotency key, if any.
    #[must_use]
    pub const fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key
    }

    /// The headers attached to this dispatch, if any.
    #[must_use]
    pub const fn headers(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.headers.as_ref()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_tenant() {
        let ctx = DispatchContext::new("acme");
        assert_eq!(ctx.tenant_id(), "acme");
        assert_eq!(ctx.producer_bc(), "");
        assert!(ctx.idempotency_key().is_none());
    }

    #[test]
    fn with_producer_bc_chains() {
        let ctx = DispatchContext::new("acme").with_producer_bc("shop");
        assert_eq!(ctx.producer_bc(), "shop");
    }

    #[test]
    fn with_idempotency_key_chains() {
        let ctx = DispatchContext::new("acme").with_idempotency_key("order:42");
        assert_eq!(ctx.idempotency_key(), Some("order:42"));
    }

    #[test]
    fn with_headers_chains() {
        let mut h = serde_json::Map::new();
        h.insert("trace".into(), serde_json::json!("abc"));
        let ctx = DispatchContext::new("acme").with_headers(h);
        assert!(ctx.headers().is_some());
    }
}
