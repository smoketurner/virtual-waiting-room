//! Identifier newtypes and the event phase enum.

use serde::{Deserialize, Serialize};

/// A waiting-room event id — the partition key of the `Counters` table and the
/// tenant boundary for every other key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub String);

impl EventId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A client-supplied request id (a `UUIDv7`), unique per join attempt and the
/// key of `PreQueue`, `Positions`, and `Tokens`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub String);

impl RequestId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The event lifecycle phase, stored on the `Counters` item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Idle,
    PreQueue,
    Active,
    PostEvent,
    Maintenance,
}
