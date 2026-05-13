//! Internal job payload envelope for `pg_work_queue` jobs.
#![allow(clippy::redundant_pub_crate)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Payload stored in each `pg_work_queue` job row.
///
/// The worker deserializes this to find which handler to invoke and which
/// event row to load from the outbox.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandlerEnvelope {
    /// UUID of the outbox event row.
    pub(crate) event_id: Uuid,
    /// Registry key identifying the handler to invoke.
    pub(crate) handler_id: String,
}
