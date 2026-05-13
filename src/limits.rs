//! Resource limits — `pub const` bounds enforced both at Rust input validation
//! and at DB-level CHECK constraints (defense in depth).

/// Max byte length of `event_type` (`DomainEvent::EVENT_TYPE`).
pub const MAX_EVENT_TYPE_BYTES: usize = 128;
/// Max byte length of `handler_id` (registration string).
pub const MAX_HANDLER_ID_BYTES: usize = 128;
/// Max byte length of `tenant_id`.
pub const MAX_TENANT_BYTES: usize = 64;
/// Max byte length of `producer_bc` (bounded context name).
pub const MAX_BC_BYTES: usize = 64;
/// Max byte length of `idempotency_key` (per dispatch).
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
/// Max encoded payload size — matches `pg_work_queue::MAX_PAYLOAD_BYTES`.
pub const MAX_PAYLOAD_BYTES: usize = 1_048_576;
/// Max length of stored `last_error` after UTF-8-safe truncation.
pub const MAX_LAST_ERROR_BYTES: usize = 8192;
/// Chunk size for purge functions. Mirrors `pg_work_queue` purge constant.
pub const PURGE_CHUNK_SIZE: usize = 10_000;
