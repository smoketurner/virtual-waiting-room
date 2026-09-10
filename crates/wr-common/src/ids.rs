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

/// The operator's live admission override, a small state machine (ADR-0019).
/// This replaces the former `admission_paused` / `fail_open` boolean pair so an
/// illegal combination (e.g. paused AND fail-open) is unrepresentable. Stored on
/// the `Counters` item as one attribute; transitions are the methods below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionControl {
    /// Admission proceeds normally (the default).
    #[default]
    Open,
    /// Admission is held; the queue and positions are intact (andon cord).
    Paused,
    /// Break-glass: the waiting room is bypassed and arrivals go straight to the
    /// origin (ADR-0009 fail-open). Enforcement is post-MVP.
    FailOpen,
}

/// An admission-control transition that is not legal from the current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cannot {action} from {from:?}")]
pub struct IllegalControl {
    pub from: AdmissionControl,
    pub action: &'static str,
}

impl AdmissionControl {
    /// Pause admission. Legal only from `Open` (pausing when paused is a no-op
    /// the caller treats as idempotent; from `FailOpen` it is refused).
    ///
    /// # Errors
    /// [`IllegalControl`] if not currently `Open`.
    pub fn pause(self) -> Result<Self, IllegalControl> {
        match self {
            Self::Open => Ok(Self::Paused),
            Self::Paused | Self::FailOpen => Err(IllegalControl {
                from: self,
                action: "pause",
            }),
        }
    }

    /// Resume admission. Legal only from `Paused`.
    ///
    /// # Errors
    /// [`IllegalControl`] if not currently `Paused`.
    pub fn resume(self) -> Result<Self, IllegalControl> {
        match self {
            Self::Paused => Ok(Self::Open),
            Self::Open | Self::FailOpen => Err(IllegalControl {
                from: self,
                action: "resume",
            }),
        }
    }

    /// Engage fail-open. Break-glass: legal from any state, always applies.
    #[must_use]
    pub fn fail_open(self) -> Self {
        Self::FailOpen
    }

    /// Clear fail-open back to normal admission. Legal from any state.
    #[must_use]
    pub fn recover(self) -> Self {
        Self::Open
    }

    /// The stored wire string, matching the `serde` `snake_case` representation.
    /// Single source of truth so no caller hand-writes "open"/"paused"/etc.
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Paused => "paused",
            Self::FailOpen => "fail_open",
        }
    }
}

impl std::str::FromStr for AdmissionControl {
    type Err = UnknownControl;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "paused" => Ok(Self::Paused),
            "fail_open" => Ok(Self::FailOpen),
            other => Err(UnknownControl(other.to_owned())),
        }
    }
}

/// A string that names no known [`AdmissionControl`]. A stored value that fails
/// to parse resolves to [`AdmissionControl::Open`] (the safe default).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown admission control: {0}")]
pub struct UnknownControl(pub String);

/// What a visitor arriving right now experiences (ADR-0019). The visitor-facing
/// projection published by `/status`, distinct from [`Phase`] (the timeline). It
/// is derived from `(Phase, AdmissionControl)` by [`serving_state`], never
/// stored, so it cannot drift. Steady states only: `Pausing`/`Resuming` are
/// deferred until the outflow controller introduces a real drain interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServingState {
    /// Arrivals queue and are admitted at the target rate.
    Running,
    /// Arrivals queue but are held; already-queued visitors keep their place.
    Paused,
    /// Arrivals are told the event is not open; the queue is frozen, not lost.
    Closed,
    /// The waiting room is bypassed: arrivals go straight to the origin
    /// (ADR-0009 fail-open). Enforcement is post-MVP.
    FailOpen,
}

/// Projects the timeline and the operator override onto the single
/// visitor-facing [`ServingState`] (ADR-0019). Total over every input, so a
/// visitor never sees an undefined state.
#[must_use]
pub fn serving_state(phase: Phase, control: AdmissionControl) -> ServingState {
    match control {
        AdmissionControl::FailOpen => ServingState::FailOpen,
        AdmissionControl::Paused if phase == Phase::Active => ServingState::Paused,
        // Paused override while not active still reads as Closed to a visitor
        // (there is nothing to admit yet, or the event is over/down).
        AdmissionControl::Open | AdmissionControl::Paused => match phase {
            Phase::Active => ServingState::Running,
            Phase::Idle | Phase::PreQueue | Phase::PostEvent | Phase::Maintenance => {
                ServingState::Closed
            }
        },
    }
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
    fn serving_state_projects_the_visitor_experience() {
        use AdmissionControl::{Open, Paused};
        use Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
        use ServingState::Paused as SPaused;
        use ServingState::{Closed, Running};
        // Active + open = Running; Active + paused = Paused.
        assert_eq!(serving_state(Active, Open), Running);
        assert_eq!(serving_state(Active, Paused), SPaused);
        // Nothing to admit yet, over, or taken down: all Closed to a visitor.
        assert_eq!(serving_state(Idle, Open), Closed);
        assert_eq!(serving_state(PreQueue, Open), Closed);
        assert_eq!(serving_state(PostEvent, Open), Closed);
        assert_eq!(serving_state(Maintenance, Open), Closed);
        // A paused override while not active still reads Closed (not Paused).
        assert_eq!(serving_state(Idle, Paused), Closed);
    }

    #[test]
    fn fail_open_overrides_every_phase() {
        use AdmissionControl::FailOpen;
        use Phase::{Active, Idle, Maintenance};
        for phase in [Active, Idle, Maintenance] {
            assert_eq!(serving_state(phase, FailOpen), ServingState::FailOpen);
        }
    }

    #[test]
    fn admission_control_transitions_are_legal_only_where_defined() {
        use AdmissionControl::{FailOpen, Open, Paused};
        // Open <-> Paused.
        assert_eq!(Open.pause().unwrap(), Paused);
        assert_eq!(Paused.resume().unwrap(), Open);
        // Pausing when paused, or resuming when open, is refused.
        assert!(Paused.pause().is_err());
        assert!(Open.resume().is_err());
        // Fail-open is break-glass from anywhere; recover clears it.
        assert_eq!(Open.fail_open(), FailOpen);
        assert_eq!(Paused.fail_open(), FailOpen);
        assert_eq!(FailOpen.recover(), Open);
        // Cannot pause/resume out of fail-open; recover first.
        assert!(FailOpen.pause().is_err());
        assert!(FailOpen.resume().is_err());
    }

    #[test]
    fn admission_control_wire_string_round_trips() {
        for c in [
            AdmissionControl::Open,
            AdmissionControl::Paused,
            AdmissionControl::FailOpen,
        ] {
            assert_eq!(c.as_wire_str().parse::<AdmissionControl>().unwrap(), c);
            // Wire string matches the serde representation.
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_wire_str()));
        }
        assert!("bogus".parse::<AdmissionControl>().is_err());
    }

    #[test]
    fn serving_state_wire_string_round_trips() {
        for st in [
            ServingState::Running,
            ServingState::Paused,
            ServingState::Closed,
            ServingState::FailOpen,
        ] {
            let json = serde_json::to_string(&st).unwrap();
            let back: ServingState = serde_json::from_str(&json).unwrap();
            assert_eq!(st, back);
        }
    }

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
