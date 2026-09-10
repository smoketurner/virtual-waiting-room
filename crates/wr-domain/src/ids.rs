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

impl Phase {
    /// The stored `snake_case` wire string, matching the `serde` representation.
    /// The exhaustive `match` returns `&'static str` so callers building a
    /// `DynamoDB` value or an HTML form option allocate at most one `String`.
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::PreQueue => "pre_queue",
            Phase::Active => "active",
            Phase::PostEvent => "post_event",
            Phase::Maintenance => "maintenance",
        }
    }
}

impl std::str::FromStr for Phase {
    type Err = UnknownPhase;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "idle" => Ok(Phase::Idle),
            "pre_queue" => Ok(Phase::PreQueue),
            "active" => Ok(Phase::Active),
            "post_event" => Ok(Phase::PostEvent),
            "maintenance" => Ok(Phase::Maintenance),
            other => Err(UnknownPhase(other.to_owned())),
        }
    }
}

/// A string that names no known [`Phase`]. Callers that read a stored phase
/// resolve this to [`Phase::Idle`]; callers parsing operator input surface it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown phase: {0}")]
pub struct UnknownPhase(pub String);

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn phase_wire_string_round_trips() {
        for phase in [
            Phase::Idle,
            Phase::PreQueue,
            Phase::Active,
            Phase::PostEvent,
            Phase::Maintenance,
        ] {
            assert_eq!(phase.as_wire_str().parse(), Ok(phase));
        }
    }

    #[test]
    fn from_str_rejects_unknown() {
        assert_eq!(
            "nonsense".parse::<Phase>(),
            Err(UnknownPhase("nonsense".to_owned()))
        );
        assert_eq!("".parse::<Phase>(), Err(UnknownPhase(String::new())));
    }

    #[test]
    fn wire_string_matches_serde_representation() {
        // as_wire_str must equal the serde snake_case form the item shapes use.
        for phase in [Phase::PreQueue, Phase::PostEvent] {
            let json = serde_json::to_string(&phase).unwrap();
            assert_eq!(json, format!("\"{}\"", phase.as_wire_str()));
        }
    }
}
