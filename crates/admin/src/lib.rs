//! Pure control-plane logic for the `admin` Lambda: phase-transition rules and
//! the operator actions, each expressed as a change to the `Counters` item.
//! Generic over a [`Store`] port so the logic runs without AWS; the SDK-backed
//! implementation lives in `dynamo`, the Axum wiring in `main`.

use std::future::Future;

use jiff::{SignedDuration, Timestamp};

use wr_common::{IllegalControl, Phase, ProtectionRule, RuleFieldError, StoredControl};

pub mod dynamo;
pub mod edge;
pub mod oidc;
pub mod scheduler;
pub mod security;
pub mod sessions;
pub mod templates;

/// The upper sanity bound on a fail-open duration (minutes): long enough for
/// a real break-glass window, short enough that a fat-fingered value cannot
/// bypass the waiting room for days.
pub const MAX_FAIL_OPEN_MINUTES: u32 = 1440;

/// The gate's `KeyValueStore` config document (`c`), issue #71. `v` is a schema
/// version the gate refuses to run against if it does not recognise it;
/// `enforce_from` is written by Terraform and carried through read-modify-write
/// unchanged by every action this crate performs. `rules` is authoritative
/// here — the `KeyValueStore`, not `Counters`, is where a ruleset lives; storing
/// a second copy in `DynamoDB` would let an operator save a rule `DynamoDB`
/// accepts (400 KB) that the `KeyValueStore` refuses (950 B): saved, but never
/// received by the gate, with nothing to say so. One store means nothing can
/// diverge. `fail_open_until` mirrors `Counters.fail_open_until`, which is a different
/// fact from a second copy: the epoch self-resolves on both sides, so there is
/// nothing to reconcile.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GateConfig {
    pub v: u8,
    #[serde(rename = "s")]
    pub enforce_from: u64,
    #[serde(rename = "f")]
    pub fail_open_until: u64,
    #[serde(rename = "r")]
    pub rules: Vec<ProtectionRule>,
}

/// The 1 KB `KeyValueStore` value ceiling (ADR-0021 §2) leaves room for roughly
/// 45 rules at the spike's measured 23 bytes/rule; enforced here at a rounder
/// 35 so a ruleset this crate encodes never risks the edge, and enforced by
/// the writer rather than merely documented (ADR-0021 §6).
pub const MAX_RULES: usize = 35;
/// 950, not the store's documented 1,024: the `KeyValueStore` API accounts in
/// bytes (`PutKey` returns `TotalSizeInBytes`, worth logging once this runs
/// for real) and the documented examples show roughly a byte of per-pair
/// overhead beyond key plus value length, so this margin is evidenced rather
/// than superstition.
const MAX_CONFIG_BYTES: usize = 950;

/// Why a [`GateConfig`] could not be encoded for the edge.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GateConfigError {
    #[error("ruleset has {actual} rules, over the {max} limit")]
    TooManyRules { actual: usize, max: usize },
    #[error("encoded config is {actual} bytes, over the {max}-byte KeyValueStore value limit")]
    TooLarge { actual: usize, max: usize },
    #[error("could not serialize the config: {0}")]
    Serialize(String),
    #[error(transparent)]
    Field(#[from] RuleFieldError),
}

/// Encodes a [`GateConfig`] to the compact JSON the gate reads, enforcing
/// per-field input bounds, the rule-count ceiling, and the byte ceiling so an
/// invalid or oversized document never reaches the edge writer. This is the
/// single function that returns the exact string a caller is about to
/// `PutKey` — there is deliberately no other path that validates one thing
/// and writes another. AWS-free — the risky part of the writer, so it is
/// tested (and runs) in every build, including one built without AWS SDK
/// features enabled.
///
/// # Errors
///
/// [`GateConfigError`] if a rule field fails input validation, the ruleset or
/// the encoded document is too large, or serialization itself fails
/// (unreachable for this shape in practice).
pub fn encode_gate_config(cfg: &GateConfig) -> Result<String, GateConfigError> {
    if cfg.rules.len() > MAX_RULES {
        return Err(GateConfigError::TooManyRules {
            actual: cfg.rules.len(),
            max: MAX_RULES,
        });
    }
    wr_common::validate_rule_fields(&cfg.rules)?;
    let json = serde_json::to_string(cfg).map_err(|e| GateConfigError::Serialize(e.to_string()))?;
    if json.len() > MAX_CONFIG_BYTES {
        return Err(GateConfigError::TooLarge {
            actual: json.len(),
            max: MAX_CONFIG_BYTES,
        });
    }
    Ok(json)
}

/// The port to the gate's `KeyValueStore`. A trait seam so the fail-open mirror
/// runs without AWS; the SDK-backed implementation lives in `edge`.
pub trait EdgeConfigStore {
    /// Reads the current gate config document (`c`).
    fn read_config(&self) -> impl Future<Output = Result<GateConfig, EdgeStoreError>> + Send;

    /// Writes the gate config document (`c`), describe-then-put under the
    /// store's `ETag`.
    fn write_config(
        &self,
        cfg: &GateConfig,
    ) -> impl Future<Output = Result<(), EdgeStoreError>> + Send;
}

/// An edge `KeyValueStore` failure.
#[derive(Debug, thiserror::Error)]
#[error("edge config store error: {0}")]
pub struct EdgeStoreError(pub String);

/// The port to the one-time seal schedule (issue #128). A trait seam so the
/// start-time actions run without AWS; the SDK-backed implementation lives in
/// `scheduler`.
///
/// There is deliberately no delete operation. Terraform owns whether the
/// schedule exists and the admin's IAM policy grants no
/// `scheduler:DeleteSchedule`, so clearing a start time can only ever disable
/// it — an invariant held by the shape of this trait rather than by a
/// convention a later edit could quietly drop.
pub trait SealSchedule {
    /// Arms the schedule at a bare `YYYY-MM-DDTHH:MM:SS` read in the given
    /// IANA zone, or disables it when `None`.
    ///
    /// The zone goes to the scheduler rather than being folded into a UTC
    /// timestamp, so that a schedule set months ahead still fires at the local
    /// hour the operator chose after a daylight-saving change.
    ///
    /// Implementations MUST read the current schedule first and resend the
    /// whole definition: `UpdateSchedule` replaces rather than patches.
    fn set_start_time(
        &self,
        at: Option<(&str, &str)>,
    ) -> impl Future<Output = Result<(), ScheduleError>> + Send;
}

/// A seal-schedule failure.
#[derive(Debug, thiserror::Error)]
#[error("seal schedule error: {0}")]
pub struct ScheduleError(pub String);

/// The persistence port the admin actions drive. Reading the current `Counters`
/// state and applying one guarded mutation to it.
pub trait Store {
    /// Loads the event's control state, or `None` if the event does not exist.
    fn load(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<Option<ControlState>, StoreError>> + Send;

    /// Transitions the phase with a guard on the expected current phase, so two
    /// operators cannot race a transition, and stamps the audit fields
    /// (`last_action` = `action`, `last_action_by`, `last_action_at`,
    /// `last_action_epoch_ms`) atomically in the same `UpdateItem`. Returns
    /// `Conflict` if the stored phase no longer matches `from`. NOT debounced —
    /// like `force_maintenance`, the operator's lifecycle/recovery move must
    /// always apply.
    fn set_phase(
        &self,
        event_id: &str,
        from: Phase,
        to: Phase,
        action: AdminAction,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the admission target rate, guarded (ADR-0017 defensive controls):
    /// the write applies only if the stored rate still equals `expected` and the
    /// last mutation is older than the debounce window (`now - DEBOUNCE`).
    /// It also stamps the audit fields and `last_action_epoch_ms` atomically.
    /// Returns `Conflict` if the guard fails (lost race or too-fast).
    fn set_rate(
        &self,
        event_id: &str,
        expected: Option<u32>,
        rate: u32,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the operator broadcast message, guarded on the debounce window and
    /// stamping the audit fields atomically.
    fn set_message(
        &self,
        event_id: &str,
        message: &str,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Writes or clears the scheduled event start (issue #128), guarded on the
    /// debounce window and stamping the audit fields atomically. `None`
    /// removes the attribute rather than zeroing it, because the read path
    /// distinguishes "never scheduled" from a start that has passed.
    ///
    /// One method for both directions on purpose: a separate clear could drift
    /// from the write in which audit fields it stamps or which guard it holds.
    fn set_starts_at(
        &self,
        event_id: &str,
        starts_at: Option<(u64, &str)>,
        action: AdminAction,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Moves the stored control from `from` to `to`, guarded on the stored
    /// value still being `from` (so a transition that already happened is a
    /// no-op `Conflict`, making the action idempotent) and on the debounce
    /// window; stamps `action` and the audit fields atomically. `target_rate` is
    /// untouched, so resuming restores the configured rate. Never touches
    /// `fail_open_until` — the two are orthogonal (issue #71).
    fn set_stored_control(
        &self,
        event_id: &str,
        from: StoredControl,
        to: StoredControl,
        action: AdminAction,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the fail-open epoch unconditionally (break-glass: not guarded on
    /// the prior value, not debounced — mirroring `force_maintenance`, the
    /// operator must always be able to engage or clear it). Stamps `action`
    /// and the audit fields atomically.
    fn set_fail_open_until(
        &self,
        event_id: &str,
        until: u64,
        action: AdminAction,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Stamps the audit fields for a ruleset change (issue #71), plus
    /// `rules_digest` (first 16 hex characters of SHA-256 over the encoded
    /// `KeyValueStore` value) and `rules_count`. Unconditional, and called
    /// *after* the `KeyValueStore` write lands — the `KeyValueStore` is
    /// authoritative for the ruleset itself, so this record only ever
    /// describes a change that actually happened; nothing reads it back to
    /// make a decision, so a failure here is logged and swallowed rather than
    /// failing the operator's action.
    fn set_rules_audit(
        &self,
        event_id: &str,
        rules_digest: &str,
        rules_count: usize,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Forces the maintenance phase, guarded on the expected current phase and
    /// stamping audit. NOT debounced — the emergency full-stop must always apply.
    fn force_maintenance(
        &self,
        event_id: &str,
        from: Phase,
        actor: &str,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The operator-facing control state rendered on the dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlState {
    pub event_id: String,
    pub phase: Phase,
    pub serving_counter: u64,
    pub queue_counter: u64,
    pub participant_count: Option<u64>,
    pub target_rate: Option<u32>,
    pub message: Option<String>,
    /// The operator's stored admission override (issue #71: never `FailOpen`,
    /// which is resolved from `fail_open_until` for display). While it is
    /// anything but `Open` the controller admits nobody and the queue and
    /// positions stay intact; `target_rate` is preserved across a hold.
    pub stored_control: StoredControl,
    /// Epoch-seconds fail-open deadline; `0` means no window is in force.
    /// Orthogonal to `stored_control`: an operator can queue a pause during a
    /// fail-open window, and it takes effect the moment this epoch lapses.
    pub fail_open_until: u64,
    /// The last mutating action, who performed it (OIDC email), and when
    /// (RFC3339). Surfaced as "last changed by X at T".
    pub last_action: Option<String>,
    pub last_action_by: Option<String>,
    pub last_action_at: Option<String>,
    /// When the last mutation happened, for the debounce guard. `None` when
    /// the event has never been mutated, and also when the stored epoch is
    /// not a representable instant — an unusable stamp is the same as no
    /// stamp, and treating it as one would reject every later action.
    pub last_action_time: Option<Timestamp>,
    /// The scheduled event start, epoch seconds; `None` when unscheduled
    /// (issue #128).
    pub starts_at: Option<u64>,
    /// The IANA zone the operator chose the start time in, so the form
    /// re-renders in the zone they typed rather than snapping to UTC.
    /// `None` when unscheduled.
    pub starts_at_timezone: Option<String>,
}

/// The upper sanity bound on the admission target rate (ADR-0017 §4): a
/// per-second admission ceiling that stops a fat-fingered runaway value from
/// being written. Chosen well above any realistic origin capacity.
pub const MAX_ADMISSION_RATE: u32 = 100_000;

/// Debounce window for control-plane mutations (ADR-0017 defensive controls):
/// two mutations to the same event closer together than this are rejected as
/// [`ActionError::TooFast`], defeating double-clicks and fast toggling. The
/// emergency full-stop (force maintenance) is deliberately NOT debounced.
pub const DEBOUNCE: SignedDuration = SignedDuration::from_millis(DEBOUNCE_MILLIS);

/// [`DEBOUNCE`] in the unit the stored stamp is written in, for the `DynamoDB`
/// condition expression — which compares numbers, not instants.
pub(crate) const DEBOUNCE_MILLIS: i64 = 2_000;

/// The mutating operator actions, recorded verbatim in the `last_action` audit
/// field (ADR-0017). An enum rather than scattered string literals so the audit
/// vocabulary has a single source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAction {
    SetRate,
    SetMessage,
    Pause,
    Resume,
    ForceMaintenance,
    SetPhase,
    /// Engages the fail-open break-glass epoch (issue #71). New with the
    /// `StoredControl`/epoch split — before it, nothing in this crate could
    /// engage fail-open at all.
    FailOpen,
    /// Clears the fail-open epoch. Not "resume": under the split this only
    /// clears `fail_open_until`, so an operator who queued a pause during the
    /// window lands in `Paused`, not `Open`.
    Recover,
    /// Replaces the edge gate's ruleset (issue #71). The ruleset itself lives
    /// only in the `KeyValueStore` — this label and the audit fields it stamps
    /// on `Counters` are what makes the change visible on the dashboard.
    SetRules,
    /// Schedules the event start (issue #128), arming the seal schedule.
    SetStartTime,
    /// Clears the scheduled start, disabling the seal schedule.
    ClearStartTime,
}

impl AdminAction {
    /// The stored audit label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SetRate => "set_rate",
            Self::SetMessage => "set_message",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::ForceMaintenance => "force_maintenance",
            Self::SetPhase => "set_phase",
            Self::FailOpen => "fail_open",
            Self::Recover => "recover",
            Self::SetRules => "set_rules",
            Self::SetStartTime => "set_start_time",
            Self::ClearStartTime => "clear_start_time",
        }
    }
}

/// A store failure or a lost transition race.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The guarded phase transition lost a race (stored phase changed).
    #[error("phase transition conflict")]
    Conflict,
    /// The underlying store failed.
    #[error("store error: {0}")]
    Backend(String),
}

/// An operator action rejected before it touches the store.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActionError {
    /// The requested phase string is not a known phase.
    #[error("unknown phase")]
    UnknownPhase,
    /// The requested transition is not legal from the current phase.
    #[error("illegal transition from {from:?} to {to:?}")]
    IllegalTransition { from: Phase, to: Phase },
    /// The rate value could not be parsed as a positive integer.
    #[error("invalid rate")]
    InvalidRate,
    /// The rate exceeds the admission ceiling (ADR-0017 §4).
    #[error("rate exceeds the maximum of {max}")]
    RateTooHigh { max: u32 },
    /// The fail-open duration could not be parsed as a positive integer, or
    /// exceeds [`MAX_FAIL_OPEN_MINUTES`].
    #[error("invalid fail-open duration")]
    InvalidDuration,
    /// The submitted ruleset failed input validation, was too large to
    /// encode, or a line could not be parsed as a rule. Carries the exact
    /// reason so the form can be re-shown with it.
    #[error("invalid rules: {0}")]
    InvalidRules(String),
    /// A control-plane mutation arrived within the debounce window (ADR-0017
    /// defensive controls) — a double-click or fast toggle.
    #[error("action rejected: too soon after the previous change")]
    TooFast,
    /// The action would not change state (e.g. pause when already paused), or
    /// lost a race with a concurrent operator.
    #[error("no change or concurrent update")]
    Conflict,
    /// The event does not exist.
    #[error("event not found")]
    NotFound,
    /// The start time is not a `YYYY-MM-DDTHH:MM[:SS]` UTC timestamp.
    #[error("invalid start time")]
    InvalidStartTime,
    /// The start time is in the past. Rejected rather than clamped: an
    /// operator who mistypes a date wants to be told, not to have the event
    /// seal immediately.
    #[error("start time must be in the future")]
    StartTimeInPast,
    /// The timezone is not an IANA name in the bundled database. Validated
    /// server-side because the form is a plain POST and its value is whatever
    /// the client sent, not necessarily one of the offered options.
    #[error("unknown timezone")]
    UnknownTimezone,
}

/// A validated operator-supplied start time (issue #128), carrying every form
/// the writers need: the UTC epoch for `Counters.starts_at`, and the bare
/// wall-clock timestamp plus IANA zone for the schedule's `at()` expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartTime {
    /// The absolute instant, so the waiting page counts down to the same
    /// moment regardless of where the visitor is.
    pub epoch_secs: u64,
    /// `YYYY-MM-DDTHH:MM:SS`, no zone suffix. `EventBridge` Scheduler takes the
    /// zone separately and rejects an expression that carries one itself.
    pub expression: String,
    /// The IANA zone the expression is read in, e.g. `America/New_York`.
    pub timezone: String,
}

/// The zone an unspecified start time is read in.
pub const DEFAULT_TIMEZONE: &str = "UTC";

/// Parses the dashboard's wall-clock field plus its IANA timezone into a
/// [`StartTime`].
///
/// Accepts `YYYY-MM-DDTHH:MM` (what a `datetime-local` field submits) and
/// `YYYY-MM-DDTHH:MM:SS`, read in `timezone` rather than assumed to be UTC —
/// an operator scheduling a 10am onsale should enter 10am, not convert it,
/// and should not have to know whether their event falls on the far side of a
/// daylight-saving change. The zone is what makes that safe: an offset would
/// be correct on the day it was chosen and wrong after the transition.
///
/// The `Z` juggling below is not incidental. Smithy's RFC-3339 codec requires
/// a zone suffix on input and always writes one on output, while `at()`
/// forbids one either way, so the suffix is appended before parsing and
/// stripped after formatting.
///
/// # Errors
///
/// [`ActionError::UnknownTimezone`] if the zone is not in the bundled IANA
/// database; [`ActionError::InvalidStartTime`] if the timestamp is not one of
/// the two accepted shapes, carries an offset or fractional seconds, or names
/// a wall-clock time that does not exist in that zone (the hour skipped by a
/// daylight-saving change); [`ActionError::StartTimeInPast`] if it is not
/// strictly in the future.
pub fn parse_start_time(
    input: &str,
    timezone: &str,
    now_secs: u64,
) -> Result<StartTime, ActionError> {
    let tz = jiff::tz::TimeZone::get(timezone).map_err(|_| ActionError::UnknownTimezone)?;

    let trimmed = input.trim();
    // Exactly the two accepted lengths, so an offset ("...+01:00"), a trailing
    // 'Z', or fractional seconds are rejected by shape before the parser sees
    // them and silently reinterprets the instant.
    let normalized = match trimmed.len() {
        16 => format!("{trimmed}:00"),
        19 => trimmed.to_owned(),
        _ => return Err(ActionError::InvalidStartTime),
    };

    let civil: jiff::civil::DateTime = normalized
        .parse()
        .map_err(|_| ActionError::InvalidStartTime)?;

    // A wall-clock time inside a spring-forward gap never happens, so it is
    // rejected rather than silently nudged: an operator told "18:30" is
    // scheduled deserves to know their zone has no 18:30 that day. An
    // ambiguous time in a fall-back overlap does happen, twice, and takes the
    // earlier of the two -- the event opens at the first 18:30, not the
    // second.
    let zoned = tz
        .to_ambiguous_zoned(civil)
        .compatible()
        .map_err(|_| ActionError::InvalidStartTime)?;

    // `compatible` resolves a spring-forward gap by shifting forward rather
    // than failing, so the only way to tell a nonexistent wall-clock time from
    // a real one is that the resolved civil time is not the one asked for. An
    // ambiguous fall-back time survives this check, because both of its two
    // instants carry the civil time that was requested.
    if zoned.datetime() != civil {
        return Err(ActionError::InvalidStartTime);
    }

    let epoch_secs =
        u64::try_from(zoned.timestamp().as_second()).map_err(|_| ActionError::InvalidStartTime)?;
    if epoch_secs <= now_secs {
        return Err(ActionError::StartTimeInPast);
    }

    Ok(StartTime {
        epoch_secs,
        expression: normalized,
        timezone: timezone.to_owned(),
    })
}

/// Renders a stored UTC epoch back into the `YYYY-MM-DDTHH:MM` wall-clock form
/// in `timezone`, for pre-filling the dashboard field with what the operator
/// originally typed.
///
/// Returns `None` for an unknown zone or an epoch outside the representable
/// range, neither of which any value this crate writes can be.
#[must_use]
pub fn format_start_time(epoch_secs: u64, timezone: &str) -> Option<String> {
    let tz = jiff::tz::TimeZone::get(timezone).ok()?;
    let secs = i64::try_from(epoch_secs).ok()?;
    let zoned = jiff::Timestamp::from_second(secs).ok()?.to_zoned(tz);
    // datetime-local round-trips minutes, not seconds.
    Some(format!("{}", zoned.datetime().strftime("%Y-%m-%dT%H:%M")))
}

/// Parses an operator-supplied phase string to a [`Phase`].
///
/// # Errors
///
/// [`ActionError::UnknownPhase`] for any string outside the known set.
pub fn parse_phase(s: &str) -> Result<Phase, ActionError> {
    s.parse().map_err(|_| ActionError::UnknownPhase)
}

/// Whether a phase transition is legal.
///
/// The lifecycle is a forward chain `idle → pre_queue → active → post_event`;
/// `maintenance` is reachable from any phase (the operator-forced override) and
/// can be left back to any phase so the operator can recover. A no-op
/// transition to the same phase is allowed (idempotent). No backward jumps
/// along the chain.
#[must_use]
pub fn transition_allowed(from: Phase, to: Phase) -> bool {
    use Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
    if from == to || to == Maintenance || from == Maintenance {
        return true;
    }
    matches!(
        (from, to),
        (Idle, PreQueue) | (PreQueue, Active) | (Active, PostEvent)
    )
}

/// The legal forward phase transitions from `from`, excluding the same-phase
/// no-op and Maintenance (which is the separate Force-maintenance control). The
/// dropdown offers exactly these, so an operator cannot select an illegal jump
/// like `idle -> active`.
#[must_use]
pub fn next_phases(from: Phase) -> Vec<Phase> {
    use Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
    match from {
        Idle => vec![PreQueue],
        PreQueue => vec![Active],
        Active => vec![PostEvent],
        // Event over: no forward step; start a new event.
        PostEvent => vec![],
        // Maintenance is a recoverable stop, not a dead-end: offer a return to
        // the running event or a reset to idle. `transition_allowed` permits
        // maintenance -> anything, so both are legal.
        Maintenance => vec![Active, Idle],
    }
}

/// Transitions the event to the requested phase, stamping the operator and
/// time on `Counters` (ADR-0017 §6) so the dashboard's "last changed by X at
/// T" line reflects the transition — including the recovery *out* of
/// `Maintenance`, which the phase dropdown routes through here.
///
/// Entering `Maintenance` is rejected here even though `transition_allowed`
/// permits it from any phase: the emergency stop must go through `/admin/reset`
/// (`force_maintenance`), so it is audited under its own label and a crafted
/// `POST /admin/phase?phase=maintenance` cannot write `phase = maintenance`
/// unaudited. The phase dropdown never offers `Maintenance` (`next_phases`
/// excludes it), so this only catches a hand-crafted request.
///
/// # Errors
///
/// [`ActionError::UnknownPhase`] if the phase string is not a known phase;
/// [`ActionError::NotFound`] if the event is missing;
/// [`ActionError::IllegalTransition`] if the transition is illegal (including
/// any target of `Maintenance`); the store's [`StoreError`] is mapped to
/// [`ActionError`] on a lost race.
pub async fn apply_phase<S: Store>(
    store: &S,
    event_id: &str,
    to: &str,
    actor: &str,
    now: Timestamp,
) -> Result<Phase, ApplyError> {
    let to = parse_phase(to)?;
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    if to == Phase::Maintenance {
        return Err(ActionError::IllegalTransition {
            from: state.phase,
            to,
        }
        .into());
    }
    if !transition_allowed(state.phase, to) {
        return Err(ActionError::IllegalTransition {
            from: state.phase,
            to,
        }
        .into());
    }
    store
        .set_phase(event_id, state.phase, to, AdminAction::SetPhase, actor, now)
        .await?;
    Ok(to)
}

/// Forces the maintenance phase — the emergency full-stop, legal from any phase.
/// NOT debounced: the stop must always apply.
///
/// # Errors
///
/// [`ApplyError`] if the event is missing or the store write fails.
pub async fn apply_reset<S: Store>(
    store: &S,
    event_id: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    store
        .force_maintenance(event_id, state.phase, actor, now)
        .await?;
    Ok(())
}

/// Returns `Err(TooFast)` if the last mutation is within the debounce window.
///
/// The elapsed time is signed, so a stamp later than `now` measures negative
/// and falls outside the window rather than reading as "no time has passed".
/// A stamp in the future describes no prior action, and nothing an operator
/// does would move it back into the past, so treating it as recent would
/// reject every later action for good.
fn debounce_check(state: &ControlState, now: Timestamp) -> Result<(), ActionError> {
    if let Some(last) = state.last_action_time
        && (SignedDuration::ZERO..DEBOUNCE).contains(&now.duration_since(last))
    {
        return Err(ActionError::TooFast);
    }
    Ok(())
}

/// Epoch seconds, for the two places that work in them rather than in
/// instants: the fail-open deadline the edge gate compares against its own
/// clock, and the start-time validation.
///
/// A pre-epoch instant clamps to 0, which both read as already past — the
/// safe direction for a deadline, since it withholds a fail-open window
/// rather than granting an unbounded one.
#[must_use]
pub fn epoch_seconds(now: Timestamp) -> u64 {
    u64::try_from(now.as_second()).unwrap_or(0)
}

/// Sets the admission target rate (guarded + debounced).
///
/// # Errors
///
/// [`ActionError::InvalidRate`] if not a positive integer;
/// [`ActionError::RateTooHigh`] over the ceiling; [`ActionError::TooFast`] inside
/// the debounce window; store errors otherwise.
pub async fn apply_rate<S: Store>(
    store: &S,
    event_id: &str,
    rate: &str,
    actor: &str,
    now: Timestamp,
) -> Result<u32, ApplyError> {
    let rate: u32 = rate.parse().map_err(|_| ActionError::InvalidRate)?;
    if rate == 0 {
        return Err(ActionError::InvalidRate.into());
    }
    if rate > MAX_ADMISSION_RATE {
        return Err(ActionError::RateTooHigh {
            max: MAX_ADMISSION_RATE,
        }
        .into());
    }
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now)?;
    store
        .set_rate(event_id, state.target_rate, rate, actor, now)
        .await?;
    Ok(rate)
}

/// Holds admission: the queue keeps forming and nobody is admitted. Legal only
/// from `Open`, and debounced.
///
/// # Errors
///
/// [`ActionError::NotFound`] / [`ActionError::TooFast`] / [`ActionError::Conflict`]
/// when the control is not `Open` / store errors.
pub async fn apply_pause<S: Store>(
    store: &S,
    event_id: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    apply_control(
        store,
        event_id,
        StoredControl::pause,
        AdminAction::Pause,
        actor,
        now,
    )
    .await
}

/// Releases a hold, restoring the configured `target_rate`. Legal only from
/// `Paused`, and debounced.
///
/// # Errors
///
/// [`ActionError::NotFound`] / [`ActionError::TooFast`] / [`ActionError::Conflict`]
/// when the control is not `Paused` / store errors.
pub async fn apply_resume<S: Store>(
    store: &S,
    event_id: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    apply_control(
        store,
        event_id,
        StoredControl::resume,
        AdminAction::Resume,
        actor,
        now,
    )
    .await
}

/// Applies one stored-control transition. `transition` is the state machine's
/// own method, so a move it refuses is rejected here before any write and the
/// store's conditional update is left to catch only a lost race. Legal
/// regardless of whether fail-open is currently active (issue #71): the two
/// are orthogonal, so pausing during a fail-open window is not refused.
async fn apply_control<S, T>(
    store: &S,
    event_id: &str,
    transition: T,
    action: AdminAction,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError>
where
    S: Store,
    T: Fn(StoredControl) -> Result<StoredControl, IllegalControl>,
{
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now)?;
    let from = state.stored_control;
    let to = transition(from).map_err(|_| ActionError::Conflict)?;
    store
        .set_stored_control(event_id, from, to, action, actor, now)
        .await?;
    Ok(())
}

/// Reserves headroom in a *ruleset* write for `s` and `f` growing to their
/// worst-case realistic width (a 10-digit epoch each: `apply_fail_open`
/// bounds `f` to `now + MAX_FAIL_OPEN_MINUTES` minutes, comfortably under
/// 10 digits for the foreseeable future). `apply_fail_open` only grows `f`
/// on a document that otherwise already validated, so if a ruleset alone
/// were allowed to consume the full [`MAX_CONFIG_BYTES`] ceiling, engaging
/// break-glass could fail to write at exactly the moment it is needed
/// (review finding: fail-open blocked by ruleset size). Not applied inside
/// [`encode_gate_config`] itself, which `apply_fail_open`/`apply_recover`
/// call directly and which must be allowed the full ceiling.
const RULES_WRITE_CEILING: usize = MAX_CONFIG_BYTES - 20;

/// Runs a read-modify-write against the edge `KeyValueStore` with one retry
/// against a **fresh** read: `mutate` is applied to whatever `read_config`
/// currently returns, written, and — if that write is rejected (a
/// concurrent writer's change landed first, so the precondition this write
/// assumed is stale) — the whole read-modify-write is redone once against a
/// fresh read rather than retrying the same now-stale document, which would
/// silently clobber whichever change landed first (review finding: the
/// `KeyValueStore` write retried with a stale document instead of re-reading).
/// This still retries on any write failure, not narrowly a precondition
/// conflict — acceptable for a low-frequency, operator-triggered write with
/// a bounded retry count of one, and simpler than threading a
/// conflict-vs-other-error distinction through [`EdgeConfigStore`].
async fn edge_read_modify_write<E, F>(edge: &E, mutate: F) -> Result<GateConfig, EdgeStoreError>
where
    E: EdgeConfigStore,
    F: Fn(&mut GateConfig),
{
    let mut cfg = edge.read_config().await?;
    mutate(&mut cfg);
    if edge.write_config(&cfg).await.is_ok() {
        return Ok(cfg);
    }
    let mut cfg = edge.read_config().await?;
    mutate(&mut cfg);
    edge.write_config(&cfg).await?;
    Ok(cfg)
}

/// Engages fail-open (issue #71): the break-glass bypass, until
/// `now_secs + minutes * 60`. Legal from any `StoredControl` — pausing and
/// fail-open are orthogonal, so a paused event can still be fail-opened.
/// Writes the edge's `KeyValueStore` mirror first: a crash between the two
/// writes then leaves the edge open with the machinery still minting and
/// counting, whereas `DynamoDB`-first would stop `generate_token` while the
/// edge still enforced.
///
/// # Errors
///
/// [`ActionError::InvalidDuration`] if `minutes` does not parse as `1..=
/// MAX_FAIL_OPEN_MINUTES`; [`ActionError::NotFound`] if the event is missing;
/// store and edge-store errors otherwise.
pub async fn apply_fail_open<S: Store, E: EdgeConfigStore>(
    store: &S,
    edge: &E,
    event_id: &str,
    minutes: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    let minutes: u32 = minutes.parse().map_err(|_| ActionError::InvalidDuration)?;
    if minutes == 0 || minutes > MAX_FAIL_OPEN_MINUTES {
        return Err(ActionError::InvalidDuration.into());
    }
    if store.load(event_id).await?.is_none() {
        return Err(ActionError::NotFound.into());
    }
    let now_secs = epoch_seconds(now);
    let until = now_secs.saturating_add(u64::from(minutes).saturating_mul(60));

    edge_read_modify_write(edge, |cfg| cfg.fail_open_until = until)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;

    store
        .set_fail_open_until(event_id, until, AdminAction::FailOpen, actor, now)
        .await?;
    Ok(())
}

/// Clears the fail-open epoch (issue #71). This is not "resume": it only
/// clears `fail_open_until`, so an operator who queued a pause during the
/// window lands in `Paused`, not `Open` — `apply_resume` is the separate
/// action for that. Writes `DynamoDB` first: a crash between the two writes
/// then leaves the machinery running and the edge open only until the
/// already-stamped expiry, which is self-consistent.
///
/// # Errors
///
/// [`ActionError::NotFound`] if the event is missing; store and edge-store
/// errors otherwise.
pub async fn apply_recover<S: Store, E: EdgeConfigStore>(
    store: &S,
    edge: &E,
    event_id: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    if store.load(event_id).await?.is_none() {
        return Err(ActionError::NotFound.into());
    }
    store
        .set_fail_open_until(event_id, 0, AdminAction::Recover, actor, now)
        .await?;

    edge_read_modify_write(edge, |cfg| cfg.fail_open_until = 0)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    Ok(())
}

/// Sets or clears the event's scheduled start (issue #128): writes
/// `Counters.starts_at`, which the waiting page counts down to, and arms or
/// disables the one-time seal schedule, which is what actually fires the seal.
/// An empty `at` clears, so one action covers both directions.
///
/// Two stores describe one fact, so the order is fixed to make the residue of
/// a half-completed change the harmless one. **The schedule may be armed
/// without an announcement; an announcement must never outlive its schedule.**
/// Arming first when setting leaves, at worst, a schedule that fires with no
/// countdown shown — which is exactly how the system behaved before this
/// existed. Clearing `starts_at` first leaves, at worst, a countdown already
/// removed from a page whose schedule is still armed, and the operator sees
/// the error and retries.
///
/// The debounce is evaluated here, before either write. Leaving it to the
/// store's own guard would let a double-submit move the schedule and then be
/// rejected by `DynamoDB`, diverging the two for the rest of the window.
///
/// # Errors
///
/// [`ActionError::InvalidStartTime`] or [`ActionError::StartTimeInPast`] for a
/// rejected value, in which case neither store is touched;
/// [`ActionError::NotFound`] if the event is missing; [`ActionError::TooFast`]
/// inside the debounce window; store and schedule errors otherwise.
pub async fn apply_start_time<S: Store, K: SealSchedule>(
    store: &S,
    schedule: &K,
    event_id: &str,
    at: &str,
    timezone: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now)?;

    if at.trim().is_empty() {
        store
            .set_starts_at(event_id, None, AdminAction::ClearStartTime, actor, now)
            .await?;
        schedule
            .set_start_time(None)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        return Ok(());
    }

    let start = parse_start_time(at, timezone, epoch_seconds(now))?;
    schedule
        .set_start_time(Some((&start.expression, &start.timezone)))
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    store
        .set_starts_at(
            event_id,
            Some((start.epoch_secs, &start.timezone)),
            AdminAction::SetStartTime,
            actor,
            now,
        )
        .await?;
    Ok(())
}

/// Replaces the edge gate's ruleset (issue #71). The `KeyValueStore` is the
/// sole store for `rules` — this reads the current config, keeps
/// `enforce_from` and `fail_open_until` exactly as they were, replaces only
/// `rules`, and validates a snapshot through [`encode_gate_config`] against
/// [`RULES_WRITE_CEILING`] (not the full ceiling — see its doc comment)
/// before writing anything, so a rejected ruleset never partially lands and
/// a later `apply_fail_open` can always still write.
///
/// Write order: the `KeyValueStore` first, then the audit stamp — the store
/// is authoritative, so the record only ever describes a change that
/// actually happened.
///
/// # Errors
///
/// [`ActionError::InvalidRules`] if a field fails validation or the encoded
/// document is too large; [`ActionError::NotFound`] if the event is missing;
/// store and edge-store errors otherwise.
pub async fn apply_set_rules<S: Store, E: EdgeConfigStore>(
    store: &S,
    edge: &E,
    event_id: &str,
    rules: Vec<ProtectionRule>,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    if store.load(event_id).await?.is_none() {
        return Err(ActionError::NotFound.into());
    }

    // Validate against a snapshot first, so a bad ruleset is rejected before
    // any write is attempted. encode_gate_config runs again inside every
    // write_config call too, so a config that drifted invalid between this
    // read and the real write (s/f changing size) is still caught — just
    // with a less specific error, and only in the vanishingly unlikely case
    // of another admin action landing in between.
    let mut probe = edge
        .read_config()
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    probe.rules = rules.clone();
    let encoded =
        encode_gate_config(&probe).map_err(|e| ActionError::InvalidRules(e.to_string()))?;
    if encoded.len() > RULES_WRITE_CEILING {
        return Err(ActionError::InvalidRules(format!(
            "ruleset is {} bytes, over the {RULES_WRITE_CEILING}-byte limit reserved so a \
             later fail-open can always still be written",
            encoded.len()
        ))
        .into());
    }

    let cfg = edge_read_modify_write(edge, |cfg| cfg.rules.clone_from(&rules))
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;

    // cfg was just written successfully via this exact encoder, so
    // re-encoding it here cannot fail; the fallback is defensive, not a
    // realistic path.
    let digest = match encode_gate_config(&cfg) {
        Ok(encoded) => rules_digest(&encoded),
        Err(_) => String::new(),
    };
    if let Err(e) = store
        .set_rules_audit(event_id, &digest, cfg.rules.len(), actor, now)
        .await
    {
        // Non-fatal: the KeyValueStore write already landed and is what the
        // gate reads. A missed audit stamp costs the dashboard's "last
        // changed by X at T" line, not correctness. cloudfront-keyvaluestore
        // writes are data-plane and outside CloudTrail management events, so
        // this log line — under a stable event name a metric filter can
        // alarm on — is the only trail a failed stamp leaves.
        tracing::error!(
            error = %e,
            event = "rules_audit_failed",
            "ruleset was written but the audit stamp failed"
        );
    }
    Ok(())
}

/// The first 16 hex characters of SHA-256 over the exact string that was
/// `PutKey`'d, for the audit trail's `rules_digest` field.
fn rules_digest(encoded: &str) -> String {
    use std::fmt::Write as _;

    use aws_lc_rs::digest::{SHA256, digest};
    let hash = digest(&SHA256, encoded.as_bytes());
    let mut out = String::with_capacity(16);
    for byte in &hash.as_ref()[..8] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Parses the operator-facing ruleset form, one rule per line: `p <prefix>`,
/// `c <name>`, `u <substring>`, `h <name> <value>`. Blank lines and lines
/// starting with `#` are ignored, so an operator can leave the form
/// human-readable. Field-level and count/size validation is
/// [`apply_set_rules`]'s job via [`encode_gate_config`]; this only turns text
/// into rules or names the line that would not parse.
///
/// # Errors
///
/// A message naming the offending line (1-indexed as the operator sees it)
/// and why it did not parse.
pub fn parse_rules(text: &str) -> Result<Vec<ProtectionRule>, String> {
    let mut rules = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((tag, rest)) = line.split_once(char::is_whitespace) else {
            return Err(format!(
                "line {}: expected \"<tag> <value...>\", got {line:?}",
                i + 1
            ));
        };
        let rest = rest.trim();
        let rule = match tag {
            "p" => ProtectionRule::PathPrefix(rest.to_owned()),
            "c" => ProtectionRule::Cookie(rest.to_owned()),
            "u" => ProtectionRule::UserAgent(rest.to_owned()),
            "h" => {
                let Some((name, value)) = rest.split_once(char::is_whitespace) else {
                    return Err(format!(
                        "line {}: \"h\" needs a name and a value, got {rest:?}",
                        i + 1
                    ));
                };
                ProtectionRule::Header {
                    name: name.to_owned(),
                    value: value.trim().to_owned(),
                }
            }
            other => {
                return Err(format!(
                    "line {}: unknown rule tag {other:?} (expected p, c, u, or h)",
                    i + 1
                ));
            }
        };
        rules.push(rule);
    }
    Ok(rules)
}

/// The inverse of [`parse_rules`], for pre-filling the form with the current
/// ruleset.
#[must_use]
pub fn format_rules(rules: &[ProtectionRule]) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for rule in rules {
        let _ = match rule {
            ProtectionRule::PathPrefix(p) => writeln!(out, "p {p}"),
            ProtectionRule::Cookie(name) => writeln!(out, "c {name}"),
            ProtectionRule::UserAgent(ua) => writeln!(out, "u {ua}"),
            ProtectionRule::Header { name, value } => writeln!(out, "h {name} {value}"),
        };
    }
    out
}

/// Sets the operator broadcast message (debounced, audit-stamped).
///
/// # Errors
///
/// [`ActionError::NotFound`] / [`ActionError::TooFast`] / store errors. Any
/// string (including empty, which clears it) is allowed.
pub async fn apply_message<S: Store>(
    store: &S,
    event_id: &str,
    message: &str,
    actor: &str,
    now: Timestamp,
) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now)?;
    store.set_message(event_id, message, actor, now).await?;
    Ok(())
}

/// The union of a rejected action and a store failure.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error(transparent)]
    Action(#[from] ActionError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Whether an `If-None-Match` header value matches `etag`, so a caller can
/// answer 304 rather than resend the body.
///
/// RFC 9110: the value is a comma-separated list, `*` matches anything, and a
/// `W/` prefix marks a weak validator. Weak comparison is the right one here —
/// the assets are byte-identical embedded files, so a weak match is a match.
#[must_use]
pub fn if_none_match(header_value: &str, etag: &str) -> bool {
    let strip = |s: &str| s.trim().trim_start_matches("W/").to_owned();
    let wanted = strip(etag);
    header_value
        .split(',')
        .any(|candidate| candidate.trim() == "*" || strip(candidate) == wanted)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::{Arc, Mutex};

    use super::*;

    /// The instant `ms` milliseconds after the epoch. The tests below talk in
    /// small round numbers because only the intervals between them matter.
    fn ts(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap()
    }

    struct FakeStore {
        phase: Mutex<Phase>,
        rate: Mutex<Option<u32>>,
        message: Mutex<Option<String>>,
        control: Mutex<StoredControl>,
        fail_open_until: Mutex<u64>,
        /// The audit label the last control change recorded.
        control_action: Mutex<Option<AdminAction>>,
        /// The four ADR-0017 §6 audit fields, mirroring `apply_audit_values` in
        /// `dynamo.rs` so the fake faithfully exposes the dashboard-facing
        /// attribution like the real store.
        last_action: Mutex<Option<String>>,
        last_action_by: Mutex<Option<String>>,
        last_action_at: Mutex<Option<String>>,
        last_time: Mutex<Option<Timestamp>>,
        /// The most recent `set_rules_audit` call's digest + count, if any.
        rules_audit: Mutex<Option<(String, usize)>>,
        /// The scheduled start (issue #128), `None` when unscheduled.
        starts_at: Mutex<Option<u64>>,
        starts_at_timezone: Mutex<Option<String>>,
        /// Ordered log of every write to either store, so a test can assert
        /// which one moved first rather than only that both did. Shared with
        /// [`FakeSealSchedule`], which appends to the same log.
        writes: Arc<Mutex<Vec<&'static str>>>,
        /// When set, the `DynamoDB` write of the start time fails, standing in
        /// for a crash between the two stores.
        starts_at_write_fails: bool,
        missing: bool,
        /// When set, the next guarded write reports a lost race.
        conflict: bool,
    }

    impl Default for FakeStore {
        fn default() -> Self {
            Self {
                phase: Mutex::new(Phase::Idle),
                rate: Mutex::new(None),
                message: Mutex::new(None),
                control: Mutex::new(StoredControl::Open),
                fail_open_until: Mutex::new(0),
                starts_at: Mutex::new(None),
                starts_at_timezone: Mutex::new(None),
                writes: Arc::new(Mutex::new(Vec::new())),
                starts_at_write_fails: false,
                control_action: Mutex::new(None),
                last_action: Mutex::new(None),
                last_action_by: Mutex::new(None),
                last_action_at: Mutex::new(None),
                last_time: Mutex::new(None),
                rules_audit: Mutex::new(None),
                missing: false,
                conflict: false,
            }
        }
    }

    /// An in-memory [`EdgeConfigStore`], mirroring the `KeyValueStore`'s
    /// read-modify-write shape closely enough to exercise the write ordering
    /// `apply_fail_open`/`apply_recover` depend on.
    /// A [`SealSchedule`] that records what it was told, sharing the store's
    /// write log so a test can assert which of the two moved first.
    struct FakeSealSchedule {
        armed: Mutex<Option<(String, String)>>,
        writes: Arc<Mutex<Vec<&'static str>>>,
        fails: bool,
    }

    impl FakeSealSchedule {
        fn new(writes: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                armed: Mutex::new(None),
                writes,
                fails: false,
            }
        }

        fn failing(writes: Arc<Mutex<Vec<&'static str>>>) -> Self {
            Self {
                armed: Mutex::new(None),
                writes,
                fails: true,
            }
        }
    }

    impl SealSchedule for FakeSealSchedule {
        fn set_start_time(
            &self,
            at: Option<(&str, &str)>,
        ) -> impl Future<Output = Result<(), ScheduleError>> + Send {
            self.writes.lock().unwrap().push("schedule");
            let result = if self.fails {
                Err(ScheduleError("update_schedule failed".to_owned()))
            } else {
                *self.armed.lock().unwrap() = at.map(|(a, tz)| (a.to_owned(), tz.to_owned()));
                Ok(())
            };
            std::future::ready(result)
        }
    }

    struct FakeEdgeStore {
        cfg: Mutex<GateConfig>,
        writes: Mutex<Vec<u64>>,
        /// Armed before the call under test: the *next* `write_config`
        /// fails once and moves this value into `pending_injection`.
        fail_next_write_then_inject: Mutex<Option<GateConfig>>,
        /// Set by a failing write above; spliced into `cfg` on the very next
        /// `read_config`, simulating a concurrent writer's change landing
        /// between this write's failure and this call's retry-read.
        pending_injection: Mutex<Option<GateConfig>>,
    }

    impl Default for FakeEdgeStore {
        fn default() -> Self {
            Self {
                cfg: Mutex::new(GateConfig {
                    v: 1,
                    enforce_from: 0,
                    fail_open_until: 0,
                    rules: Vec::new(),
                }),
                writes: Mutex::new(Vec::new()),
                fail_next_write_then_inject: Mutex::new(None),
                pending_injection: Mutex::new(None),
            }
        }
    }

    impl EdgeConfigStore for FakeEdgeStore {
        fn read_config(&self) -> impl Future<Output = Result<GateConfig, EdgeStoreError>> + Send {
            if let Some(injected) = self.pending_injection.lock().unwrap().take() {
                *self.cfg.lock().unwrap() = injected;
            }
            std::future::ready(Ok(self.cfg.lock().unwrap().clone()))
        }

        fn write_config(
            &self,
            cfg: &GateConfig,
        ) -> impl Future<Output = Result<(), EdgeStoreError>> + Send {
            if let Some(injected) = self.fail_next_write_then_inject.lock().unwrap().take() {
                *self.pending_injection.lock().unwrap() = Some(injected);
                return std::future::ready(Err(EdgeStoreError("simulated conflict".to_owned())));
            }
            *self.cfg.lock().unwrap() = cfg.clone();
            self.writes.lock().unwrap().push(cfg.fail_open_until);
            std::future::ready(Ok(()))
        }
    }

    impl FakeStore {
        fn with_phase(phase: Phase) -> Self {
            Self {
                phase: Mutex::new(phase),
                ..Self::default()
            }
        }

        /// Stamps all four audit fields, mirroring `apply_audit_values` in the
        /// real store; every mutating method calls this.
        fn stamp_audit(&self, action: AdminAction, actor: &str, now: Timestamp) {
            *self.last_action.lock().unwrap() = Some(action.as_str().to_owned());
            *self.last_action_by.lock().unwrap() = Some(actor.to_owned());
            *self.last_action_at.lock().unwrap() = Some(now.to_string());
            *self.last_time.lock().unwrap() = Some(now);
        }
    }

    impl Store for FakeStore {
        fn load(
            &self,
            event_id: &str,
        ) -> impl Future<Output = Result<Option<ControlState>, StoreError>> + Send {
            let result = if self.missing {
                Ok(None)
            } else {
                Ok(Some(ControlState {
                    event_id: event_id.to_owned(),
                    phase: *self.phase.lock().unwrap(),
                    serving_counter: 0,
                    queue_counter: 0,
                    participant_count: None,
                    target_rate: *self.rate.lock().unwrap(),
                    message: self.message.lock().unwrap().clone(),
                    stored_control: *self.control.lock().unwrap(),
                    fail_open_until: *self.fail_open_until.lock().unwrap(),
                    last_action: self.last_action.lock().unwrap().clone(),
                    last_action_by: self.last_action_by.lock().unwrap().clone(),
                    last_action_at: self.last_action_at.lock().unwrap().clone(),
                    last_action_time: *self.last_time.lock().unwrap(),
                    starts_at: *self.starts_at.lock().unwrap(),
                    starts_at_timezone: self.starts_at_timezone.lock().unwrap().clone(),
                }))
            };
            std::future::ready(result)
        }

        fn set_phase(
            &self,
            _event_id: &str,
            _from: Phase,
            to: Phase,
            action: AdminAction,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.phase.lock().unwrap() = to;
                self.stamp_audit(action, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_rate(
            &self,
            _event_id: &str,
            _expected: Option<u32>,
            rate: u32,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.rate.lock().unwrap() = Some(rate);
                self.stamp_audit(AdminAction::SetRate, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_message(
            &self,
            _event_id: &str,
            message: &str,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.message.lock().unwrap() = Some(message.to_owned());
            self.stamp_audit(AdminAction::SetMessage, actor, now);
            std::future::ready(Ok(()))
        }

        fn set_starts_at(
            &self,
            _event_id: &str,
            starts_at: Option<(u64, &str)>,
            action: AdminAction,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.writes.lock().unwrap().push("dynamo");
            let result = if self.starts_at_write_fails {
                Err(StoreError::Backend("starts_at write failed".to_owned()))
            } else {
                *self.starts_at.lock().unwrap() = starts_at.map(|(secs, _)| secs);
                *self.starts_at_timezone.lock().unwrap() = starts_at.map(|(_, tz)| tz.to_owned());
                self.stamp_audit(action, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_stored_control(
            &self,
            _event_id: &str,
            from: StoredControl,
            to: StoredControl,
            action: AdminAction,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            // Mirrors the conditional write: the move applies only if the stored
            // value is still the one the caller read.
            let mut guard = self.control.lock().unwrap();
            let result = if self.conflict || *guard != from {
                Err(StoreError::Conflict)
            } else {
                *guard = to;
                *self.control_action.lock().unwrap() = Some(action);
                self.stamp_audit(action, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_fail_open_until(
            &self,
            _event_id: &str,
            until: u64,
            action: AdminAction,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.fail_open_until.lock().unwrap() = until;
                self.stamp_audit(action, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_rules_audit(
            &self,
            _event_id: &str,
            rules_digest: &str,
            rules_count: usize,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.rules_audit.lock().unwrap() = Some((rules_digest.to_owned(), rules_count));
                self.stamp_audit(AdminAction::SetRules, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }

        fn force_maintenance(
            &self,
            _event_id: &str,
            _from: Phase,
            actor: &str,
            now: Timestamp,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.phase.lock().unwrap() = Phase::Maintenance;
                self.stamp_audit(AdminAction::ForceMaintenance, actor, now);
                Ok(())
            };
            std::future::ready(result)
        }
    }

    #[test]
    fn parse_phase_rejects_garbage() {
        assert_eq!(parse_phase("active"), Ok(Phase::Active));
        assert_eq!(parse_phase("nonsense"), Err(ActionError::UnknownPhase));
        assert_eq!(parse_phase(""), Err(ActionError::UnknownPhase));
    }

    #[test]
    fn transition_rules_match_the_lifecycle() {
        use Phase::{Active, Idle, Maintenance, PostEvent, PreQueue};
        // Forward chain is allowed.
        assert!(transition_allowed(Idle, PreQueue));
        assert!(transition_allowed(PreQueue, Active));
        assert!(transition_allowed(Active, PostEvent));
        // Backward / skipping jumps are not.
        assert!(!transition_allowed(Active, Idle));
        assert!(!transition_allowed(Idle, Active));
        assert!(!transition_allowed(PostEvent, Active));
        // Maintenance is reachable from and to anything; same-phase is a no-op.
        assert!(transition_allowed(Active, Maintenance));
        assert!(transition_allowed(Maintenance, Idle));
        assert!(transition_allowed(Active, Active));
    }

    #[test]
    fn next_phases_offers_only_the_single_forward_step() {
        use Phase::{Active, Idle, PostEvent, PreQueue};
        assert_eq!(next_phases(Idle), vec![PreQueue]);
        assert_eq!(next_phases(PreQueue), vec![Active]);
        assert_eq!(next_phases(Active), vec![PostEvent]);
        // Only the finished event is a dead end.
        assert_eq!(next_phases(PostEvent), Vec::<Phase>::new());
    }

    #[test]
    fn maintenance_is_recoverable_not_a_dead_end() {
        use Phase::{Active, Idle, Maintenance};
        // Force maintenance must be reversible from the UI: offer resume (active)
        // and reset (idle), both of which transition_allowed permits.
        assert_eq!(next_phases(Maintenance), vec![Active, Idle]);
        assert!(transition_allowed(Maintenance, Active));
        assert!(transition_allowed(Maintenance, Idle));
    }

    // --- start time (issue #128) ---------------------------------------------

    /// Well past any `now` the tests use, so a valid time is never rejected
    /// for being in the past.
    const FUTURE: &str = "2030-06-15T10:00";

    fn start_time_fixture() -> (FakeStore, FakeSealSchedule) {
        let store = FakeStore::default();
        let schedule = FakeSealSchedule::new(Arc::clone(&store.writes));
        (store, schedule)
    }

    #[test]
    fn a_start_time_is_read_in_the_operators_zone_not_utc() {
        // 10:00 in New York on a summer date is 14:00 UTC. Reading the field as
        // UTC would open the event four hours early.
        let start = parse_start_time("2030-06-15T10:00", "America/New_York", 0).unwrap();
        let utc = parse_start_time("2030-06-15T14:00", "UTC", 0).unwrap();
        assert_eq!(start.epoch_secs, utc.epoch_secs);
        // The expression keeps the operator's wall clock; the zone travels
        // beside it, so the schedule fires at 10:00 local whatever the offset
        // is by then.
        assert_eq!(start.expression, "2030-06-15T10:00:00");
        assert_eq!(start.timezone, "America/New_York");
    }

    #[test]
    fn the_same_wall_clock_is_a_different_instant_across_a_dst_boundary() {
        // The point of storing a zone rather than an offset: 10:00 New York is
        // UTC-4 in June and UTC-5 in January, so a fixed offset chosen at
        // scheduling time would be an hour wrong on one side of the change.
        let summer = parse_start_time("2030-06-15T10:00", "America/New_York", 0).unwrap();
        let winter = parse_start_time("2030-01-15T10:00", "America/New_York", 0).unwrap();
        let summer_utc = parse_start_time("2030-06-15T14:00", "UTC", 0).unwrap();
        let winter_utc = parse_start_time("2030-01-15T15:00", "UTC", 0).unwrap();
        assert_eq!(summer.epoch_secs, summer_utc.epoch_secs);
        assert_eq!(winter.epoch_secs, winter_utc.epoch_secs);
    }

    #[test]
    fn a_wall_clock_time_that_does_not_exist_is_rejected() {
        // 02:30 never happens in New York on the spring-forward date; an
        // operator told it is scheduled should be told otherwise instead.
        assert_eq!(
            parse_start_time("2030-03-10T02:30", "America/New_York", 0),
            Err(ActionError::InvalidStartTime)
        );
    }

    #[test]
    fn start_time_rejects_bad_shapes_and_zones() {
        for bad in [
            "2030-06-15",
            "2030-06-15T10:00:00Z",
            "2030-06-15T10:00:00+01:00",
            "2030-06-15T10:00:00.5",
            "not-a-time",
            "",
        ] {
            assert!(parse_start_time(bad, "UTC", 0).is_err(), "accepted {bad:?}");
        }
        assert_eq!(
            parse_start_time(FUTURE, "Mars/Olympus_Mons", 0),
            Err(ActionError::UnknownTimezone)
        );
    }

    #[test]
    fn a_start_time_in_the_past_is_rejected_rather_than_clamped() {
        let start = parse_start_time(FUTURE, "UTC", 0).unwrap();
        assert_eq!(
            parse_start_time(FUTURE, "UTC", start.epoch_secs),
            Err(ActionError::StartTimeInPast)
        );
    }

    #[test]
    fn start_time_round_trips_through_the_form_value() {
        for tz in ["UTC", "America/New_York", "Australia/Sydney"] {
            let start = parse_start_time(FUTURE, tz, 0).unwrap();
            assert_eq!(
                format_start_time(start.epoch_secs, tz).as_deref(),
                Some(FUTURE),
                "{tz} did not round trip"
            );
        }
    }

    #[tokio::test]
    async fn setting_a_start_time_arms_the_schedule_before_announcing_it() {
        // The schedule may be armed without an announcement; an announcement
        // must never outlive its schedule. A crash between the two writes
        // therefore leaves a seal that still fires with no countdown shown,
        // which is how the system behaved before start times existed.
        let (store, schedule) = start_time_fixture();
        apply_start_time(
            &store,
            &schedule,
            "evt",
            FUTURE,
            "UTC",
            "op@example.com",
            ts(1000),
        )
        .await
        .unwrap();

        assert_eq!(*store.writes.lock().unwrap(), vec!["schedule", "dynamo"]);
        assert_eq!(
            *schedule.armed.lock().unwrap(),
            Some(("2030-06-15T10:00:00".to_owned(), "UTC".to_owned()))
        );
        assert!(store.starts_at.lock().unwrap().is_some());
        assert_eq!(
            store.last_action.lock().unwrap().as_deref(),
            Some("set_start_time")
        );
    }

    #[tokio::test]
    async fn clearing_a_start_time_withdraws_the_announcement_first() {
        let (store, schedule) = start_time_fixture();
        apply_start_time(
            &store,
            &schedule,
            "evt",
            FUTURE,
            "UTC",
            "op@example.com",
            ts(1000),
        )
        .await
        .unwrap();
        store.writes.lock().unwrap().clear();

        apply_start_time(
            &store,
            &schedule,
            "evt",
            "",
            "UTC",
            "op@example.com",
            ts(9999),
        )
        .await
        .unwrap();

        assert_eq!(*store.writes.lock().unwrap(), vec!["dynamo", "schedule"]);
        // Disabled, not deleted — and the port has no delete to call.
        assert!(schedule.armed.lock().unwrap().is_none());
        assert!(store.starts_at.lock().unwrap().is_none());
        assert!(store.starts_at_timezone.lock().unwrap().is_none());
        assert_eq!(
            store.last_action.lock().unwrap().as_deref(),
            Some("clear_start_time")
        );
    }

    #[tokio::test]
    async fn a_rejected_start_time_touches_neither_store() {
        for (at, tz) in [("nonsense", "UTC"), (FUTURE, "Nowhere/Anywhere")] {
            let (store, schedule) = start_time_fixture();
            assert!(
                apply_start_time(&store, &schedule, "evt", at, tz, "op@example.com", ts(1000))
                    .await
                    .is_err()
            );
            assert!(store.writes.lock().unwrap().is_empty());
            assert!(schedule.armed.lock().unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn a_failed_announcement_leaves_the_schedule_armed_and_reports_it() {
        // The residue of a half-applied set: armed but unannounced. The
        // operator sees the error and retries; nobody is counting down to a
        // moment that will not arrive.
        let store = FakeStore {
            starts_at_write_fails: true,
            ..FakeStore::default()
        };
        let schedule = FakeSealSchedule::new(Arc::clone(&store.writes));

        assert!(
            apply_start_time(
                &store,
                &schedule,
                "evt",
                FUTURE,
                "UTC",
                "op@example.com",
                ts(1000)
            )
            .await
            .is_err()
        );
        assert!(schedule.armed.lock().unwrap().is_some());
        assert!(store.starts_at.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_failed_schedule_write_does_not_announce_a_start() {
        let store = FakeStore::default();
        let schedule = FakeSealSchedule::failing(Arc::clone(&store.writes));

        assert!(
            apply_start_time(
                &store,
                &schedule,
                "evt",
                FUTURE,
                "UTC",
                "op@example.com",
                ts(1000)
            )
            .await
            .is_err()
        );
        assert!(store.starts_at.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_double_submitted_start_time_is_rejected_before_the_schedule_moves() {
        // The debounce is evaluated here rather than left to the store's own
        // guard: a rejection after the schedule had already moved would leave
        // the two describing different times for the rest of the window.
        let (store, schedule) = start_time_fixture();
        apply_start_time(
            &store,
            &schedule,
            "evt",
            FUTURE,
            "UTC",
            "op@example.com",
            ts(1000),
        )
        .await
        .unwrap();
        let armed = schedule.armed.lock().unwrap().clone();
        store.writes.lock().unwrap().clear();

        let err = apply_start_time(
            &store,
            &schedule,
            "evt",
            "2030-07-20T09:00",
            "UTC",
            "op@example.com",
            ts(1500),
        )
        .await;
        assert!(matches!(err, Err(ApplyError::Action(ActionError::TooFast))));
        assert!(store.writes.lock().unwrap().is_empty());
        assert_eq!(*schedule.armed.lock().unwrap(), armed);
    }

    #[tokio::test]
    async fn a_start_time_for_a_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..FakeStore::default()
        };
        let schedule = FakeSealSchedule::new(Arc::clone(&store.writes));
        assert!(matches!(
            apply_start_time(
                &store,
                &schedule,
                "evt",
                FUTURE,
                "UTC",
                "op@example.com",
                ts(1000)
            )
            .await,
            Err(ApplyError::Action(ActionError::NotFound))
        ));
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn apply_phase_advances_and_persists() {
        let store = FakeStore::with_phase(Phase::Idle);
        let to = apply_phase(&store, "evt", "pre_queue", "op@x", ts(1_000))
            .await
            .unwrap();
        assert_eq!(to, Phase::PreQueue);
        assert_eq!(*store.phase.lock().unwrap(), Phase::PreQueue);
        // Audit is stamped (ADR-0017 §6).
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_time, Some(ts(1_000)));
    }

    #[tokio::test]
    async fn apply_phase_rejects_illegal_transition() {
        let store = FakeStore::with_phase(Phase::Idle);
        let err = apply_phase(&store, "evt", "active", "op@x", ts(1_000))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ApplyError::Action(ActionError::IllegalTransition {
                from: Phase::Idle,
                to: Phase::Active
            })
        ));
        // Store is untouched.
        assert_eq!(*store.phase.lock().unwrap(), Phase::Idle);
    }

    #[tokio::test]
    async fn apply_phase_rejects_unknown_phase() {
        let store = FakeStore::with_phase(Phase::Idle);
        let err = apply_phase(&store, "evt", "bogus", "op@x", ts(1_000))
            .await
            .unwrap_err();
        assert!(matches!(err, ApplyError::Action(ActionError::UnknownPhase)));
    }

    #[tokio::test]
    async fn apply_phase_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..FakeStore::default()
        };
        let err = apply_phase(&store, "evt", "pre_queue", "op@x", ts(1_000))
            .await
            .unwrap_err();
        assert!(matches!(err, ApplyError::Action(ActionError::NotFound)));
    }

    #[tokio::test]
    async fn apply_phase_surfaces_a_lost_race() {
        let store = FakeStore {
            phase: Mutex::new(Phase::Idle),
            conflict: true,
            ..FakeStore::default()
        };
        let err = apply_phase(&store, "evt", "pre_queue", "op@x", ts(1_000))
            .await
            .unwrap_err();
        assert!(matches!(err, ApplyError::Store(StoreError::Conflict)));
    }

    #[tokio::test]
    async fn apply_phase_rejects_maintenance_target() {
        // Entering maintenance must go through /admin/reset (force_maintenance)
        // so it is audited under its own label; a crafted POST to /admin/phase
        // with phase=maintenance is rejected here, before any write.
        for from in [
            Phase::Idle,
            Phase::PreQueue,
            Phase::Active,
            Phase::PostEvent,
        ] {
            let store = FakeStore::with_phase(from);
            let err = apply_phase(&store, "evt", "maintenance", "op@x", ts(1_000))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    ApplyError::Action(ActionError::IllegalTransition {
                        from: _,
                        to: Phase::Maintenance
                    })
                ),
                "maintenance must be rejected from {from:?}"
            );
            // Store is untouched.
            assert_eq!(*store.phase.lock().unwrap(), from);
        }
        // Maintenance -> Maintenance is also rejected (no same-phase no-op for
        // maintenance through this route).
        let store = FakeStore::with_phase(Phase::Maintenance);
        let err = apply_phase(&store, "evt", "maintenance", "op@x", ts(1_000))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ApplyError::Action(ActionError::IllegalTransition {
                from: Phase::Maintenance,
                to: Phase::Maintenance
            })
        ));
    }

    #[tokio::test]
    async fn reset_forces_maintenance_from_any_phase() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_reset(&store, "evt", "op@x", ts(1000)).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
        // The emergency stop stamps audit (ADR-0017 §6).
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("force_maintenance"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_time, Some(ts(1000)));
    }

    #[tokio::test]
    async fn recovery_from_maintenance_via_phase_stamps_audit() {
        // The full emergency-stop + recovery lifecycle, mirroring the
        // dashboard dropdown path: force_maintenance (audited) then
        // apply_phase(Maintenance -> Active) — the ADR-0017 Revision's
        // designated recovery route, which must also be audited.
        let store = FakeStore::with_phase(Phase::Active);

        // Audited emergency stop.
        apply_reset(&store, "evt", "op@x", ts(5_000)).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("force_maintenance"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_time, Some(ts(5_000)));

        // The dropdown's offered recovery path (next_phases(Maintenance)[0]).
        assert_eq!(
            next_phases(Phase::Maintenance),
            vec![Phase::Active, Phase::Idle]
        );
        apply_phase(&store, "evt", "active", "op@y", ts(6_000))
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Active);

        // FIXED: the recovery now stamps audit — the dashboard-facing
        // "last changed by X at T" line names the recovery (set_phase by
        // op@y at T6_000), not the entry into maintenance.
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@y"));
        assert_eq!(state.last_action_time, Some(ts(6_000)));
    }

    #[tokio::test]
    async fn recovery_from_maintenance_to_idle_stamps_audit() {
        // The other dropdown recovery option: Maintenance -> Idle (reset).
        let store = FakeStore::with_phase(Phase::Maintenance);
        apply_phase(&store, "evt", "idle", "op@z", ts(7_000))
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Idle);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@z"));
        assert_eq!(state.last_action_time, Some(ts(7_000)));
    }

    #[tokio::test]
    async fn apply_phase_stamps_audit_on_each_lifecycle_transition() {
        // Every lifecycle transition records the actor and time, not just the
        // recovery-from-maintenance path.
        let store = FakeStore::with_phase(Phase::Idle);
        apply_phase(&store, "evt", "pre_queue", "alice", ts(1_000))
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::PreQueue);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("alice"));
        assert_eq!(state.last_action_time, Some(ts(1_000)));

        apply_phase(&store, "evt", "active", "bob", ts(2_000))
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("bob"));
        assert_eq!(state.last_action_time, Some(ts(2_000)));

        apply_phase(&store, "evt", "post_event", "carol", ts(3_000))
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("carol"));
        assert_eq!(state.last_action_time, Some(ts(3_000)));
    }

    #[tokio::test]
    async fn apply_phase_overwrites_prior_audit_from_a_different_action() {
        // A phase transition after a set_rate must update the audit line to
        // set_phase, proving set_phase does not leave the prior action's
        // stamp in place (the bug).
        let store = FakeStore::with_phase(Phase::Active);
        apply_rate(&store, "evt", "500", "rate-op", ts(1_000))
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_rate"));
        assert_eq!(state.last_action_by.as_deref(), Some("rate-op"));

        apply_phase(&store, "evt", "post_event", "phase-op", ts(2_000))
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("phase-op"));
        assert_eq!(state.last_action_time, Some(ts(2_000)));
    }

    #[tokio::test]
    async fn rate_rejects_zero_and_nonnumeric() {
        let store = FakeStore::with_phase(Phase::Active);
        assert!(matches!(
            apply_rate(&store, "evt", "0", "op@x", ts(1000))
                .await
                .unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert!(matches!(
            apply_rate(&store, "evt", "fast", "op@x", ts(1000))
                .await
                .unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert_eq!(
            apply_rate(&store, "evt", "500", "op@x", ts(1000))
                .await
                .unwrap(),
            500
        );
        assert_eq!(*store.rate.lock().unwrap(), Some(500));
    }

    #[tokio::test]
    async fn message_sets_and_allows_empty() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_message(&store, "evt", "Doors open at noon", "op@x", ts(1000))
            .await
            .unwrap();
        assert_eq!(
            store.message.lock().unwrap().as_deref(),
            Some("Doors open at noon")
        );
        // Second call is spaced beyond the debounce window.
        apply_message(
            &store,
            "evt",
            "",
            "op@x",
            ts(1000).checked_add(DEBOUNCE).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(store.message.lock().unwrap().as_deref(), Some(""));
    }

    #[tokio::test]
    async fn rate_over_ceiling_is_rejected() {
        let store = FakeStore::with_phase(Phase::Active);
        let over = (u64::from(MAX_ADMISSION_RATE) + 1).to_string();
        assert!(matches!(
            apply_rate(&store, "evt", &over, "op@x", ts(1000)).await,
            Err(ApplyError::Action(ActionError::RateTooHigh { .. }))
        ));
        assert!(
            apply_rate(
                &store,
                "evt",
                &MAX_ADMISSION_RATE.to_string(),
                "op@x",
                ts(1000)
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn pause_and_resume_move_the_control_between_open_and_paused() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_pause(&store, "evt", "op@x", ts(1000)).await.unwrap();
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Paused);
        assert_eq!(
            *store.control_action.lock().unwrap(),
            Some(AdminAction::Pause)
        );
        // Resume spaced beyond the debounce window.
        apply_resume(
            &store,
            "evt",
            "op@x",
            ts(1000).checked_add(DEBOUNCE).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Open);
        assert_eq!(
            *store.control_action.lock().unwrap(),
            Some(AdminAction::Resume)
        );
    }

    #[tokio::test]
    async fn a_stored_fail_open_epoch_resolves_to_fail_open() {
        // A stale "fail_open" string in admission_control is no longer how
        // this is represented at all — the epoch is (issue #71). A bool for
        // stored_control would still collapse Paused/Open together, so the
        // resolved AdmissionControl is what the operator-facing read proves.
        let store = FakeStore {
            fail_open_until: Mutex::new(1000),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        let state = store.load("evt").await.unwrap().unwrap();
        let resolved = wr_common::resolve(state.stored_control, state.fail_open_until, 500);
        assert_eq!(resolved, wr_common::AdmissionControl::FailOpen);
        assert_eq!(
            wr_common::serving_state(state.phase, resolved),
            wr_common::ServingState::FailOpen
        );
    }

    #[tokio::test]
    async fn pause_and_resume_are_legal_during_fail_open() {
        // The point of the split (issue #71, v4): StoredControl and the
        // fail-open epoch are orthogonal, so pausing while fail-open is
        // active is not refused — it lands the moment the epoch lapses,
        // rather than sitting blocked behind a state machine that used to
        // treat FailOpen as a fourth value pause/resume had to route around.
        let store = FakeStore {
            fail_open_until: Mutex::new(1_000_000),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        apply_pause(&store, "evt", "op@x", ts(1000)).await.unwrap();
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Paused);
        // The epoch is untouched by a pause.
        assert_eq!(*store.fail_open_until.lock().unwrap(), 1_000_000);
    }

    #[tokio::test]
    async fn pause_on_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..Default::default()
        };
        assert!(matches!(
            apply_pause(&store, "evt", "op@x", ts(1000)).await,
            Err(ApplyError::Action(ActionError::NotFound))
        ));
    }

    #[tokio::test]
    async fn second_mutation_within_debounce_window_is_rejected() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_rate(&store, "evt", "500", "op@x", ts(1000))
            .await
            .unwrap();
        // A second mutation 100ms later (< DEBOUNCE) is rejected.
        assert!(matches!(
            apply_rate(&store, "evt", "600", "op@x", ts(1100)).await,
            Err(ApplyError::Action(ActionError::TooFast))
        ));
        // The rate did not change.
        assert_eq!(*store.rate.lock().unwrap(), Some(500));
        // After the window, it succeeds.
        assert!(
            apply_rate(
                &store,
                "evt",
                "600",
                "op@x",
                ts(1000).checked_add(DEBOUNCE).unwrap()
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn a_stamp_from_the_future_does_not_lock_the_control_plane() {
        // A stamp later than now cannot describe a prior action, so it does
        // not hold the debounce window open. Measuring it as unsigned would
        // read it as "no time has passed" and refuse every rate, message,
        // pause, resume, rules and start-time change for good, because nothing
        // an operator can do moves the stamp back into the past.
        let store = FakeStore::with_phase(Phase::Active);
        let ahead = ts(60_000);
        apply_rate(&store, "evt", "500", "op@x", ahead)
            .await
            .unwrap();
        assert_eq!(*store.last_time.lock().unwrap(), Some(ahead));

        apply_rate(&store, "evt", "600", "op@x", ts(1_000))
            .await
            .unwrap();
        assert_eq!(*store.rate.lock().unwrap(), Some(600));
        // And the debounce itself still works from the recovered stamp.
        assert!(matches!(
            apply_rate(&store, "evt", "700", "op@x", ts(1_100)).await,
            Err(ApplyError::Action(ActionError::TooFast))
        ));
    }

    #[tokio::test]
    async fn pause_when_already_paused_is_a_conflict() {
        let store = FakeStore {
            control: Mutex::new(StoredControl::Paused),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        // The transition is illegal from Paused, so it is refused before any
        // write is attempted.
        assert!(matches!(
            apply_pause(&store, "evt", "op@x", ts(1000)).await,
            Err(ApplyError::Action(ActionError::Conflict))
        ));
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Paused);
    }

    #[tokio::test]
    async fn force_maintenance_is_not_debounced() {
        let store = FakeStore::with_phase(Phase::Active);
        // A recent mutation sets the debounce clock.
        apply_rate(&store, "evt", "500", "op@x", ts(1000))
            .await
            .unwrap();
        // Force maintenance immediately after still applies (emergency stop).
        apply_reset(&store, "evt", "op@x", ts(1100)).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
    }

    // --- fail-open / recover (issue #71) --------------------------------

    #[tokio::test]
    async fn fail_open_writes_the_edge_before_dynamodb() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        apply_fail_open(&store, &edge, "evt", "30", "op@x", ts(1_000_000))
            .await
            .unwrap();
        let until = 1_000 + 30 * 60; // now/1000 + minutes*60
        assert_eq!(*store.fail_open_until.lock().unwrap(), until);
        assert_eq!(edge.cfg.lock().unwrap().fail_open_until, until);
        // Order: the edge write happened, and it is the only write recorded
        // (DynamoDB has no equivalent "writes" log here, but the edge fake
        // proves at least that its own write landed before this call
        // returned, which is all a synchronous fake can distinguish).
        assert_eq!(*edge.writes.lock().unwrap(), vec![until]);
    }

    #[tokio::test]
    async fn fail_open_preserves_the_rest_of_the_edge_config() {
        // The writer must read-modify-write: s and r are owned by Terraform
        // and a future rules action respectively, and must round-trip
        // unchanged through a fail-open write.
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        edge.cfg.lock().unwrap().enforce_from = 42;
        edge.cfg.lock().unwrap().rules = vec![ProtectionRule::PathPrefix("/checkout".to_owned())];
        apply_fail_open(&store, &edge, "evt", "5", "op@x", ts(0))
            .await
            .unwrap();
        let cfg = edge.cfg.lock().unwrap();
        assert_eq!(cfg.enforce_from, 42);
        assert_eq!(
            cfg.rules,
            vec![ProtectionRule::PathPrefix("/checkout".to_owned())]
        );
    }

    #[tokio::test]
    async fn fail_open_retry_re_reads_instead_of_clobbering_a_concurrent_write() {
        // Review finding: the retry re-put the same stale document instead
        // of re-reading, so a concurrent operator's change (here, a rules
        // update) landing between this write's failure and its retry would
        // be silently lost. The fix re-reads before retrying.
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        let concurrent = GateConfig {
            v: 1,
            enforce_from: 0,
            fail_open_until: 0,
            rules: vec![ProtectionRule::PathPrefix("/concurrent".to_owned())],
        };
        *edge.fail_next_write_then_inject.lock().unwrap() = Some(concurrent.clone());

        apply_fail_open(&store, &edge, "evt", "10", "op@x", ts(0))
            .await
            .unwrap();

        let final_cfg = edge.cfg.lock().unwrap().clone();
        assert_eq!(
            final_cfg.rules, concurrent.rules,
            "the concurrent writer's rules must survive the retry"
        );
        assert!(
            final_cfg.fail_open_until > 0,
            "this action's own mutation must still apply on top of the concurrent change"
        );
    }

    #[tokio::test]
    async fn fail_open_is_legal_while_paused() {
        let store = FakeStore {
            control: Mutex::new(StoredControl::Paused),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        let edge = FakeEdgeStore::default();
        apply_fail_open(&store, &edge, "evt", "10", "op@x", ts(0))
            .await
            .unwrap();
        assert!(*store.fail_open_until.lock().unwrap() > 0);
        // Pausing is untouched by engaging fail-open.
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Paused);
    }

    #[tokio::test]
    async fn fail_open_rejects_a_zero_or_oversized_duration() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        assert!(matches!(
            apply_fail_open(&store, &edge, "evt", "0", "op@x", ts(0)).await,
            Err(ApplyError::Action(ActionError::InvalidDuration))
        ));
        assert!(matches!(
            apply_fail_open(&store, &edge, "evt", "not-a-number", "op@x", ts(0)).await,
            Err(ApplyError::Action(ActionError::InvalidDuration))
        ));
        let over = (MAX_FAIL_OPEN_MINUTES + 1).to_string();
        assert!(matches!(
            apply_fail_open(&store, &edge, "evt", &over, "op@x", ts(0)).await,
            Err(ApplyError::Action(ActionError::InvalidDuration))
        ));
    }

    /// A ruleset of 7 path-prefix rules at 122 bytes each: legal under
    /// `MAX_RULES`/`MAX_PATH_PREFIX_BYTES`, and its encoded document (942
    /// bytes) fits under the full `MAX_CONFIG_BYTES` ceiling (950) but not
    /// under `RULES_WRITE_CEILING` (930) — the case `apply_set_rules` must
    /// reject even though `encode_gate_config` alone would accept it.
    fn ruleset_between_the_two_ceilings() -> Vec<ProtectionRule> {
        (0..7)
            .map(|_| ProtectionRule::PathPrefix(format!("/{}", "a".repeat(121))))
            .collect()
    }

    #[tokio::test]
    async fn set_rules_rejects_a_ruleset_that_would_block_a_later_fail_open() {
        // Review finding: apply_fail_open re-encodes the whole document
        // through encode_gate_config with `f` at its current width, so a
        // ruleset that fits the full ceiling today can still overflow once
        // `f` grows to a real epoch — break-glass would then fail to write
        // at exactly the moment it is needed. apply_set_rules must refuse
        // this ruleset up front, before either write.
        let rules = ruleset_between_the_two_ceilings();
        let probe_cfg = GateConfig {
            v: 1,
            enforce_from: 0,
            fail_open_until: 0,
            rules: rules.clone(),
        };
        let encoded = encode_gate_config(&probe_cfg).unwrap();
        assert!(
            encoded.len() > RULES_WRITE_CEILING && encoded.len() <= MAX_CONFIG_BYTES,
            "fixture must sit strictly between the two ceilings, got {} bytes",
            encoded.len()
        );

        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        let err = apply_set_rules(&store, &edge, "evt", rules, "op@x", ts(0))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ApplyError::Action(ActionError::InvalidRules(_))
        ));
        assert!(
            edge.cfg.lock().unwrap().rules.is_empty(),
            "a rejected ruleset must not be written"
        );
    }

    #[tokio::test]
    async fn a_ruleset_accepted_by_set_rules_can_always_still_be_fail_opened() {
        // The other half of the same guarantee: a ruleset apply_set_rules
        // does accept must never later block apply_fail_open, however wide
        // `f` grows within the range apply_fail_open can produce.
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        // One fewer rule than the rejected fixture: comfortably under
        // RULES_WRITE_CEILING.
        let rules: Vec<ProtectionRule> = (0..6)
            .map(|_| ProtectionRule::PathPrefix(format!("/{}", "a".repeat(121))))
            .collect();
        apply_set_rules(&store, &edge, "evt", rules, "op@x", ts(0))
            .await
            .unwrap();

        // now picked so now_secs is a realistic 10-digit epoch and the
        // engaged window is the maximum the form allows.
        apply_fail_open(
            &store,
            &edge,
            "evt",
            &MAX_FAIL_OPEN_MINUTES.to_string(),
            "op@x",
            ts(9_999_999_999_000),
        )
        .await
        .unwrap();
        assert!(edge.cfg.lock().unwrap().fail_open_until > 0);
    }

    #[tokio::test]
    async fn recover_clears_the_epoch_on_both_stores_dynamodb_first() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        apply_fail_open(&store, &edge, "evt", "10", "op@x", ts(0))
            .await
            .unwrap();
        assert!(*store.fail_open_until.lock().unwrap() > 0);

        apply_recover(&store, &edge, "evt", "op@y", ts(5_000))
            .await
            .unwrap();
        assert_eq!(*store.fail_open_until.lock().unwrap(), 0);
        assert_eq!(edge.cfg.lock().unwrap().fail_open_until, 0);
        // Last recorded edge write is the clearing 0.
        assert_eq!(edge.writes.lock().unwrap().last(), Some(&0));
    }

    #[tokio::test]
    async fn recover_is_not_resume_a_queued_pause_survives_it() {
        // v4's declared behaviour change: recovering from fail-open only
        // clears the epoch. An operator who paused during the window lands
        // in Paused, not Open.
        let store = FakeStore {
            control: Mutex::new(StoredControl::Paused),
            fail_open_until: Mutex::new(1_000_000),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        let edge = FakeEdgeStore::default();
        apply_recover(&store, &edge, "evt", "op@x", ts(0))
            .await
            .unwrap();
        assert_eq!(*store.fail_open_until.lock().unwrap(), 0);
        assert_eq!(*store.control.lock().unwrap(), StoredControl::Paused);
    }

    #[tokio::test]
    async fn recover_on_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..Default::default()
        };
        let edge = FakeEdgeStore::default();
        assert!(matches!(
            apply_recover(&store, &edge, "evt", "op@x", ts(0)).await,
            Err(ApplyError::Action(ActionError::NotFound))
        ));
    }

    // --- set_rules (issue #71) --------------------------------------------

    #[tokio::test]
    async fn set_rules_writes_the_edge_and_then_the_audit_record() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        let rules = vec![
            ProtectionRule::PathPrefix("/checkout".to_owned()),
            ProtectionRule::Cookie("loyalty_member".to_owned()),
        ];
        apply_set_rules(&store, &edge, "evt", rules.clone(), "op@x", ts(1000))
            .await
            .unwrap();
        assert_eq!(edge.cfg.lock().unwrap().rules, rules);
        let (digest, count) = store.rules_audit.lock().unwrap().clone().unwrap();
        assert_eq!(count, 2);
        assert_eq!(digest.len(), 16, "digest must be the 16-hex-char prefix");
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn set_rules_preserves_enforce_from_and_fail_open_until() {
        // The KeyValueStore write is a read-modify-write of the whole
        // document: a ruleset change must not clobber fields it does not own.
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        edge.cfg.lock().unwrap().enforce_from = 42;
        edge.cfg.lock().unwrap().fail_open_until = 99;
        apply_set_rules(
            &store,
            &edge,
            "evt",
            vec![ProtectionRule::PathPrefix("/x".to_owned())],
            "op@x",
            ts(0),
        )
        .await
        .unwrap();
        let cfg = edge.cfg.lock().unwrap();
        assert_eq!(cfg.enforce_from, 42);
        assert_eq!(cfg.fail_open_until, 99);
    }

    #[tokio::test]
    async fn set_rules_rejects_an_invalid_field_before_writing_anything() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        let bad = vec![ProtectionRule::Cookie("bad\nname".to_owned())];
        let err = apply_set_rules(&store, &edge, "evt", bad, "op@x", ts(0))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ApplyError::Action(ActionError::InvalidRules(_))
        ));
        // Nothing written: the store's config is still the default empty one.
        assert!(edge.cfg.lock().unwrap().rules.is_empty());
        assert!(store.rules_audit.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn set_rules_rejects_too_many_rules() {
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        let too_many: Vec<ProtectionRule> = (0..=MAX_RULES)
            .map(|i| ProtectionRule::PathPrefix(format!("/p{i}")))
            .collect();
        let err = apply_set_rules(&store, &edge, "evt", too_many, "op@x", ts(0))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ApplyError::Action(ActionError::InvalidRules(_))
        ));
    }

    #[tokio::test]
    async fn set_rules_on_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..Default::default()
        };
        let edge = FakeEdgeStore::default();
        assert!(matches!(
            apply_set_rules(&store, &edge, "evt", vec![], "op@x", ts(0)).await,
            Err(ApplyError::Action(ActionError::NotFound))
        ));
    }

    #[tokio::test]
    async fn set_rules_accepts_an_empty_ruleset_dormancy() {
        // [] is dormancy (#60): the operator must be able to clear a ruleset
        // back to passing everything through.
        let store = FakeStore::with_phase(Phase::Active);
        let edge = FakeEdgeStore::default();
        edge.cfg.lock().unwrap().rules = vec![ProtectionRule::PathPrefix("/x".to_owned())];
        apply_set_rules(&store, &edge, "evt", vec![], "op@x", ts(0))
            .await
            .unwrap();
        assert!(edge.cfg.lock().unwrap().rules.is_empty());
    }

    // --- rules form parsing (issue #71) -----------------------------------

    #[test]
    fn parse_rules_reads_every_tag() {
        let text = "p /checkout\nc loyalty_member\nu HeadlessChrome\nh x-internal-monitor true\n";
        let rules = parse_rules(text).unwrap();
        assert_eq!(
            rules,
            vec![
                ProtectionRule::PathPrefix("/checkout".to_owned()),
                ProtectionRule::Cookie("loyalty_member".to_owned()),
                ProtectionRule::UserAgent("HeadlessChrome".to_owned()),
                ProtectionRule::Header {
                    name: "x-internal-monitor".to_owned(),
                    value: "true".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn parse_rules_skips_blank_lines_and_comments() {
        let text = "\n# a comment\np /checkout\n\n";
        assert_eq!(
            parse_rules(text).unwrap(),
            vec![ProtectionRule::PathPrefix("/checkout".to_owned())]
        );
    }

    #[test]
    fn parse_rules_header_value_may_contain_spaces() {
        let rules = parse_rules("h user-agent some bot 1.0").unwrap();
        assert_eq!(
            rules,
            vec![ProtectionRule::Header {
                name: "user-agent".to_owned(),
                value: "some bot 1.0".to_owned(),
            }]
        );
    }

    #[test]
    fn parse_rules_rejects_an_unknown_tag_naming_the_line() {
        let err = parse_rules("p /ok\nz bogus").unwrap_err();
        assert!(err.contains("line 2"), "{err}");
        assert!(err.contains('z'), "{err}");
    }

    #[test]
    fn parse_rules_rejects_a_header_missing_its_value() {
        let err = parse_rules("h x-only-a-name").unwrap_err();
        assert!(err.contains("line 1"), "{err}");
    }

    #[test]
    fn parse_rules_rejects_a_tag_with_no_value() {
        let err = parse_rules("p").unwrap_err();
        assert!(err.contains("line 1"), "{err}");
    }

    #[test]
    fn parse_rules_of_an_empty_string_is_an_empty_dormant_ruleset() {
        assert_eq!(parse_rules("").unwrap(), Vec::new());
        assert_eq!(parse_rules("\n\n  \n").unwrap(), Vec::new());
    }

    #[test]
    fn format_rules_round_trips_through_parse_rules() {
        let rules = vec![
            ProtectionRule::PathPrefix("/checkout".to_owned()),
            ProtectionRule::Cookie("loyalty_member".to_owned()),
            ProtectionRule::UserAgent("HeadlessChrome".to_owned()),
            ProtectionRule::Header {
                name: "x-internal-monitor".to_owned(),
                value: "true".to_owned(),
            },
        ];
        let text = format_rules(&rules);
        assert_eq!(parse_rules(&text).unwrap(), rules);
    }

    #[test]
    fn format_rules_of_an_empty_slice_is_an_empty_string() {
        assert_eq!(format_rules(&[]), "");
    }

    // --- gate config encoding --------------------------------------------

    fn empty_config() -> GateConfig {
        GateConfig {
            v: 1,
            enforce_from: 0,
            fail_open_until: 0,
            rules: Vec::new(),
        }
    }

    #[test]
    fn empty_config_encodes_as_the_dormant_placeholder() {
        // Must match the literal Terraform seeds aws_cloudfrontkeyvaluestore_key.config
        // with, or a fresh stack's "dormant" reading disagrees with what this
        // crate would write.
        assert_eq!(
            encode_gate_config(&empty_config()).unwrap(),
            r#"{"v":1,"s":0,"f":0,"r":[]}"#
        );
    }

    #[test]
    fn encoder_rejects_a_ruleset_over_the_limit() {
        let mut cfg = empty_config();
        cfg.rules = (0..=MAX_RULES)
            .map(|i| ProtectionRule::PathPrefix(format!("/p{i}")))
            .collect();
        assert_eq!(
            encode_gate_config(&cfg),
            Err(GateConfigError::TooManyRules {
                actual: MAX_RULES + 1,
                max: MAX_RULES,
            })
        );
    }

    #[test]
    fn encoder_accepts_exactly_the_rule_limit() {
        let mut cfg = empty_config();
        cfg.rules = (0..MAX_RULES)
            .map(|i| ProtectionRule::PathPrefix(format!("/p{i}")))
            .collect();
        assert!(encode_gate_config(&cfg).is_ok());
    }

    #[test]
    fn encoder_rejects_a_config_over_the_byte_ceiling() {
        // 8 path-prefix rules at 122 bytes each (legal under MAX_RULES and
        // MAX_PATH_PREFIX_BYTES individually) encode to well over
        // MAX_CONFIG_BYTES — the direct case testing.md flagged as
        // uncovered: TooLarge from too many bytes, not too many rules.
        let mut cfg = empty_config();
        cfg.rules = (0..8)
            .map(|_| ProtectionRule::PathPrefix(format!("/{}", "a".repeat(121))))
            .collect();
        assert!(matches!(
            encode_gate_config(&cfg),
            Err(GateConfigError::TooLarge {
                max: MAX_CONFIG_BYTES,
                ..
            })
        ));
    }

    #[test]
    fn encoder_uses_the_compact_rule_wire_form() {
        let mut cfg = empty_config();
        cfg.rules = vec![ProtectionRule::PathPrefix("/checkout".to_owned())];
        assert_eq!(
            encode_gate_config(&cfg).unwrap(),
            r#"{"v":1,"s":0,"f":0,"r":[["p","/checkout"]]}"#
        );
    }

    #[test]
    fn if_none_match_handles_the_shapes_a_browser_sends() {
        let tag = "\"abc123\"";
        assert!(if_none_match(tag, tag));
        assert!(if_none_match("*", tag), "* matches anything");
        assert!(
            if_none_match("\"other\", \"abc123\"", tag),
            "a comma-separated list matches on any member"
        );
        assert!(
            if_none_match("W/\"abc123\"", tag),
            "a weak validator matches a byte-identical asset"
        );
        assert!(!if_none_match("\"stale\"", tag));
        assert!(!if_none_match("", tag));
    }
}
