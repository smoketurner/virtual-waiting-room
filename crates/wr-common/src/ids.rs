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

/// Checks the canonical `8-4-4-4-12` hex shape, rejecting a malformed or
/// truncated id before it reaches a claim.
///
/// No version or variant nibble check: nothing here depends on a `request_id`
/// being time-ordered, since no consumer sorts by it and no index exists over
/// it.
#[must_use]
pub fn is_uuid_shape(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
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

/// What the `Counters` item's `admission_control` attribute can hold (issue
/// #71). Only two values — this is the storage codec, and it is what makes
/// [`AdmissionControl::FailOpen`] unstorable as a string: nothing on the write
/// path can produce a value that reads back "`fail_open`". Fail-open is instead
/// an epoch (`Counters.fail_open_until`) evaluated against a clock by
/// [`resolve`], so a control plane that dies after engaging it cannot leave the
/// deployment open indefinitely — the flag self-expires whether or not anything
/// ever clears it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StoredControl {
    /// Admission proceeds normally (the default).
    #[default]
    Open,
    /// Admission is held; the queue and positions are intact (andon cord).
    Paused,
}

/// The operator's live admission override, resolved for display and for every
/// consumer that decides on it (ADR-0019). Three values, one more than
/// [`StoredControl`]: [`resolve`] is the only thing that produces `FailOpen`,
/// from the epoch rather than from a stored string, so the asymmetry between
/// this type (three values, display-only, no [`std::str::FromStr`]) and
/// `StoredControl` (two values, the storage codec) is the invariant — not an
/// oversight to be narrowed away. `Serialize`/`Deserialize` are kept only for
/// `controller::PassOutcome`'s durable-execution checkpoint (an internal,
/// ephemeral round-trip, not the `Counters.admission_control` storage
/// boundary `StoredControl` alone governs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionControl {
    /// Admission proceeds normally (the default).
    #[default]
    Open,
    /// Admission is held; the queue and positions are intact (andon cord).
    Paused,
    /// Break-glass: the waiting room is bypassed and arrivals go straight to
    /// the origin (ADR-0009 fail-open), until `fail_open_until` passes.
    FailOpen,
}

/// A `StoredControl` transition that is not legal from the current state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cannot {action} from {from:?}")]
pub struct IllegalControl {
    pub from: StoredControl,
    pub action: &'static str,
}

impl StoredControl {
    /// Pause admission. Legal only from `Open` (pausing when paused is a no-op
    /// the caller treats as idempotent). Legal even while fail-open is active:
    /// the two are orthogonal, so a queued pause takes effect the moment the
    /// fail-open epoch lapses.
    ///
    /// # Errors
    /// [`IllegalControl`] if not currently `Open`.
    pub fn pause(self) -> Result<Self, IllegalControl> {
        match self {
            Self::Open => Ok(Self::Paused),
            Self::Paused => Err(IllegalControl {
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
            Self::Open => Err(IllegalControl {
                from: self,
                action: "resume",
            }),
        }
    }

    /// The stored wire string, matching the `serde` `snake_case` representation.
    /// Single source of truth so no caller hand-writes "open"/"paused".
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Paused => "paused",
        }
    }
}

impl std::str::FromStr for StoredControl {
    type Err = UnknownControl;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "paused" => Ok(Self::Paused),
            other => Err(UnknownControl(other.to_owned())),
        }
    }
}

impl AdmissionControl {
    /// The display wire string. Three values, unlike [`StoredControl`]'s two —
    /// `admin/src/templates.rs` and `controller/src/lib.rs` render `"fail_open"`
    /// from the *resolved* control, never from a stored one.
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Paused => "paused",
            Self::FailOpen => "fail_open",
        }
    }
}

/// Resolves the stored control and the fail-open epoch into what a consumer
/// acts on right now. The only place [`AdmissionControl::FailOpen`] is
/// produced: `now < fail_open_until` overrides whatever is stored, and once
/// the epoch has passed the stored value is authoritative again with no write
/// required on either side — natural expiry is simultaneous by construction.
#[must_use]
pub fn resolve(stored: StoredControl, fail_open_until: u64, now: u64) -> AdmissionControl {
    if now < fail_open_until {
        return AdmissionControl::FailOpen;
    }
    match stored {
        StoredControl::Open => AdmissionControl::Open,
        StoredControl::Paused => AdmissionControl::Paused,
    }
}

/// A string that names no known [`StoredControl`]. A stored value that fails to
/// parse resolves to [`StoredControl::Open`] (the safe default) — including a
/// legacy `"fail_open"` string left by a table written before issue #71, which
/// decays to `Open` rather than sticking: the epoch is the sole authority for
/// fail-open now, so a stale string carries no window to reopen.
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
    fn uuid_shape_validation() {
        assert!(is_uuid_shape("018f3a2b-7c9d-7e1f-abcd-0123456789ab"));
        // A v4-shaped id passes too: the check pins the hyphen/hex layout, not
        // a version.
        assert!(is_uuid_shape("018f3a2b-7c9d-4e1f-abcd-0123456789ab"));
        assert!(!is_uuid_shape("not-a-uuid"));
        assert!(!is_uuid_shape("018f3a2b7c9d7e1fabcd0123456789ab"));
        assert!(!is_uuid_shape(""));
        assert!(!is_uuid_shape("018f3a2b-7c9d-7e1f-abcd-0123456789abcd"));
        assert!(!is_uuid_shape("018f3a2b-7c9d-7e1f-abcd-0123456789ag"));
    }

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
    fn stored_control_transitions_are_legal_only_where_defined() {
        use StoredControl::{Open, Paused};
        assert_eq!(Open.pause().unwrap(), Paused);
        assert_eq!(Paused.resume().unwrap(), Open);
        // Pausing when paused, or resuming when open, is refused.
        assert!(Paused.pause().is_err());
        assert!(Open.resume().is_err());
    }

    #[test]
    fn pause_is_legal_even_while_fail_open_is_active() {
        // The split's point: StoredControl and the fail-open epoch are
        // orthogonal, so an operator can queue a pause during a fail-open
        // window — it lands the moment the epoch lapses, rather than being
        // refused because the resolved control currently reads FailOpen.
        assert!(StoredControl::Open.pause().is_ok());
    }

    #[test]
    fn stored_control_wire_string_round_trips() {
        for c in [StoredControl::Open, StoredControl::Paused] {
            assert_eq!(c.as_wire_str().parse::<StoredControl>().unwrap(), c);
            // Wire string matches the serde representation.
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(json, format!("\"{}\"", c.as_wire_str()));
        }
        assert!("bogus".parse::<StoredControl>().is_err());
    }

    #[test]
    fn a_legacy_fail_open_string_decays_to_open() {
        // Before issue #71 "fail_open" was a legal stored value. It no longer
        // parses as StoredControl; a table written by the old code must decay
        // to Open (the unwrap_or_default() every reader falls back to) rather
        // than error out or, worse, be given special-cased meaning.
        assert_eq!(
            "fail_open".parse::<StoredControl>(),
            Err(UnknownControl("fail_open".to_owned()))
        );
    }

    #[test]
    fn resolve_prefers_the_epoch_over_the_stored_value() {
        use StoredControl::{Open, Paused};
        assert_eq!(resolve(Open, 100, 50), AdmissionControl::FailOpen);
        assert_eq!(resolve(Paused, 100, 50), AdmissionControl::FailOpen);
        // Once now reaches the epoch, the stored value is authoritative again,
        // with no write required on either side.
        assert_eq!(resolve(Open, 100, 100), AdmissionControl::Open);
        assert_eq!(resolve(Paused, 100, 100), AdmissionControl::Paused);
        // No epoch in force: stored value passes through unchanged.
        assert_eq!(resolve(Open, 0, 1000), AdmissionControl::Open);
        assert_eq!(resolve(Paused, 0, 1000), AdmissionControl::Paused);
    }

    #[test]
    fn admission_control_wire_string_is_three_valued() {
        for (c, s) in [
            (AdmissionControl::Open, "open"),
            (AdmissionControl::Paused, "paused"),
            (AdmissionControl::FailOpen, "fail_open"),
        ] {
            assert_eq!(c.as_wire_str(), s);
        }
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
