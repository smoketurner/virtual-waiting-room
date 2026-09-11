//! Pure control-plane logic for the `admin` Lambda: phase-transition rules and
//! the operator actions, each expressed as a change to the `Counters` item.
//! Generic over a [`Store`] port so the logic runs without AWS; the SDK-backed
//! implementation lives in `dynamo`, the Axum wiring in `main`.

use std::future::Future;

use wr_common::{AdmissionControl, IllegalControl, Phase};

pub mod dynamo;
pub mod oidc;
pub mod security;
pub mod sessions;
pub mod templates;

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
        now_ms: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the admission target rate, guarded (ADR-0017 defensive controls):
    /// the write applies only if the stored rate still equals `expected` and the
    /// last mutation is older than the debounce window (`now_ms - DEBOUNCE_MS`).
    /// It also stamps the audit fields and `last_action_epoch_ms` atomically.
    /// Returns `Conflict` if the guard fails (lost race or too-fast).
    fn set_rate(
        &self,
        event_id: &str,
        expected: Option<u32>,
        rate: u32,
        actor: &str,
        now_ms: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the operator broadcast message, guarded on the debounce window and
    /// stamping the audit fields atomically.
    fn set_message(
        &self,
        event_id: &str,
        message: &str,
        actor: &str,
        now_ms: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Moves the admission control from `from` to `to`, guarded on the stored
    /// value still being `from` (so a transition that already happened is a
    /// no-op `Conflict`, making the action idempotent) and on the debounce
    /// window; stamps `action` and the audit fields atomically. `target_rate` is
    /// untouched, so resuming restores the configured rate.
    fn set_admission_control(
        &self,
        event_id: &str,
        from: AdmissionControl,
        to: AdmissionControl,
        action: AdminAction,
        actor: &str,
        now_ms: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Forces the maintenance phase, guarded on the expected current phase and
    /// stamping audit. NOT debounced — the emergency full-stop must always apply.
    fn force_maintenance(
        &self,
        event_id: &str,
        from: Phase,
        actor: &str,
        now_ms: u64,
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
    /// The operator's admission override. While it is anything but `Open` the
    /// controller admits nobody and the queue and positions stay intact;
    /// `target_rate` is preserved across a hold.
    pub admission_control: AdmissionControl,
    /// The last mutating action, who performed it (OIDC email), and when
    /// (RFC3339). Surfaced as "last changed by X at T".
    pub last_action: Option<String>,
    pub last_action_by: Option<String>,
    pub last_action_at: Option<String>,
    /// Epoch-millis of the last mutation, for the debounce guard.
    pub last_action_epoch_ms: Option<u64>,
}

/// The upper sanity bound on the admission target rate (ADR-0017 §4): a
/// per-second admission ceiling that stops a fat-fingered runaway value from
/// being written. Chosen well above any realistic origin capacity.
pub const MAX_ADMISSION_RATE: u32 = 100_000;

/// Debounce window for control-plane mutations (ADR-0017 defensive controls):
/// two mutations to the same event closer together than this are rejected as
/// [`ActionError::TooFast`], defeating double-clicks and fast toggling. The
/// emergency full-stop (force maintenance) is deliberately NOT debounced.
pub const DEBOUNCE_MS: u64 = 2000;

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
    now_ms: u64,
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
        .set_phase(
            event_id,
            state.phase,
            to,
            AdminAction::SetPhase,
            actor,
            now_ms,
        )
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
    now_ms: u64,
) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    store
        .force_maintenance(event_id, state.phase, actor, now_ms)
        .await?;
    Ok(())
}

/// Returns `Err(TooFast)` if the last mutation is within the debounce window.
fn debounce_check(state: &ControlState, now_ms: u64) -> Result<(), ActionError> {
    if let Some(last) = state.last_action_epoch_ms
        && now_ms.saturating_sub(last) < DEBOUNCE_MS
    {
        return Err(ActionError::TooFast);
    }
    Ok(())
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
    now_ms: u64,
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
    debounce_check(&state, now_ms)?;
    store
        .set_rate(event_id, state.target_rate, rate, actor, now_ms)
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
    now_ms: u64,
) -> Result<(), ApplyError> {
    apply_control(
        store,
        event_id,
        AdmissionControl::pause,
        AdminAction::Pause,
        actor,
        now_ms,
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
    now_ms: u64,
) -> Result<(), ApplyError> {
    apply_control(
        store,
        event_id,
        AdmissionControl::resume,
        AdminAction::Resume,
        actor,
        now_ms,
    )
    .await
}

/// Applies one admission-control transition. `transition` is the state
/// machine's own method, so a move it refuses is rejected here before any write
/// and the store's conditional update is left to catch only a lost race.
async fn apply_control<S, T>(
    store: &S,
    event_id: &str,
    transition: T,
    action: AdminAction,
    actor: &str,
    now_ms: u64,
) -> Result<(), ApplyError>
where
    S: Store,
    T: Fn(AdmissionControl) -> Result<AdmissionControl, IllegalControl>,
{
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now_ms)?;
    let from = state.admission_control;
    let to = transition(from).map_err(|_| ActionError::Conflict)?;
    store
        .set_admission_control(event_id, from, to, action, actor, now_ms)
        .await?;
    Ok(())
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
    now_ms: u64,
) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    debounce_check(&state, now_ms)?;
    store.set_message(event_id, message, actor, now_ms).await?;
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;

    struct FakeStore {
        phase: Mutex<Phase>,
        rate: Mutex<Option<u32>>,
        message: Mutex<Option<String>>,
        control: Mutex<AdmissionControl>,
        /// The audit label the last control change recorded.
        control_action: Mutex<Option<AdminAction>>,
        /// The four ADR-0017 §6 audit fields, mirroring `apply_audit_values` in
        /// `dynamo.rs` so the fake faithfully exposes the dashboard-facing
        /// attribution like the real store.
        last_action: Mutex<Option<String>>,
        last_action_by: Mutex<Option<String>>,
        last_action_at: Mutex<Option<String>>,
        last_epoch: Mutex<Option<u64>>,
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
                control: Mutex::new(AdmissionControl::Open),
                control_action: Mutex::new(None),
                last_action: Mutex::new(None),
                last_action_by: Mutex::new(None),
                last_action_at: Mutex::new(None),
                last_epoch: Mutex::new(None),
                missing: false,
                conflict: false,
            }
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
        fn stamp_audit(&self, action: AdminAction, actor: &str, now_ms: u64) {
            *self.last_action.lock().unwrap() = Some(action.as_str().to_owned());
            *self.last_action_by.lock().unwrap() = Some(actor.to_owned());
            *self.last_action_at.lock().unwrap() = Some(now_ms.to_string());
            *self.last_epoch.lock().unwrap() = Some(now_ms);
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
                    admission_control: *self.control.lock().unwrap(),
                    last_action: self.last_action.lock().unwrap().clone(),
                    last_action_by: self.last_action_by.lock().unwrap().clone(),
                    last_action_at: self.last_action_at.lock().unwrap().clone(),
                    last_action_epoch_ms: *self.last_epoch.lock().unwrap(),
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
            now_ms: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.phase.lock().unwrap() = to;
                self.stamp_audit(action, actor, now_ms);
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
            now_ms: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.rate.lock().unwrap() = Some(rate);
                self.stamp_audit(AdminAction::SetRate, actor, now_ms);
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_message(
            &self,
            _event_id: &str,
            message: &str,
            actor: &str,
            now_ms: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.message.lock().unwrap() = Some(message.to_owned());
            self.stamp_audit(AdminAction::SetMessage, actor, now_ms);
            std::future::ready(Ok(()))
        }

        fn set_admission_control(
            &self,
            _event_id: &str,
            from: AdmissionControl,
            to: AdmissionControl,
            action: AdminAction,
            actor: &str,
            now_ms: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            // Mirrors the conditional write: the move applies only if the stored
            // value is still the one the caller read.
            let mut guard = self.control.lock().unwrap();
            let result = if self.conflict || *guard != from {
                Err(StoreError::Conflict)
            } else {
                *guard = to;
                *self.control_action.lock().unwrap() = Some(action);
                self.stamp_audit(action, actor, now_ms);
                Ok(())
            };
            std::future::ready(result)
        }

        fn force_maintenance(
            &self,
            _event_id: &str,
            _from: Phase,
            actor: &str,
            now_ms: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.phase.lock().unwrap() = Phase::Maintenance;
                self.stamp_audit(AdminAction::ForceMaintenance, actor, now_ms);
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

    #[tokio::test]
    async fn apply_phase_advances_and_persists() {
        let store = FakeStore::with_phase(Phase::Idle);
        let to = apply_phase(&store, "evt", "pre_queue", "op@x", 1_000)
            .await
            .unwrap();
        assert_eq!(to, Phase::PreQueue);
        assert_eq!(*store.phase.lock().unwrap(), Phase::PreQueue);
        // Audit is stamped (ADR-0017 §6).
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_epoch_ms, Some(1_000));
    }

    #[tokio::test]
    async fn apply_phase_rejects_illegal_transition() {
        let store = FakeStore::with_phase(Phase::Idle);
        let err = apply_phase(&store, "evt", "active", "op@x", 1_000)
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
        let err = apply_phase(&store, "evt", "bogus", "op@x", 1_000)
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
        let err = apply_phase(&store, "evt", "pre_queue", "op@x", 1_000)
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
        let err = apply_phase(&store, "evt", "pre_queue", "op@x", 1_000)
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
            let err = apply_phase(&store, "evt", "maintenance", "op@x", 1_000)
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
        let err = apply_phase(&store, "evt", "maintenance", "op@x", 1_000)
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
        apply_reset(&store, "evt", "op@x", 1000).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
        // The emergency stop stamps audit (ADR-0017 §6).
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("force_maintenance"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_epoch_ms, Some(1000));
    }

    #[tokio::test]
    async fn recovery_from_maintenance_via_phase_stamps_audit() {
        // The full emergency-stop + recovery lifecycle, mirroring the
        // dashboard dropdown path: force_maintenance (audited) then
        // apply_phase(Maintenance -> Active) — the ADR-0017 Revision's
        // designated recovery route, which must also be audited.
        let store = FakeStore::with_phase(Phase::Active);

        // Audited emergency stop.
        apply_reset(&store, "evt", "op@x", 5_000).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("force_maintenance"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@x"));
        assert_eq!(state.last_action_epoch_ms, Some(5_000));

        // The dropdown's offered recovery path (next_phases(Maintenance)[0]).
        assert_eq!(
            next_phases(Phase::Maintenance),
            vec![Phase::Active, Phase::Idle]
        );
        apply_phase(&store, "evt", "active", "op@y", 6_000)
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Active);

        // FIXED: the recovery now stamps audit — the dashboard-facing
        // "last changed by X at T" line names the recovery (set_phase by
        // op@y at T6_000), not the entry into maintenance.
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@y"));
        assert_eq!(state.last_action_epoch_ms, Some(6_000));
    }

    #[tokio::test]
    async fn recovery_from_maintenance_to_idle_stamps_audit() {
        // The other dropdown recovery option: Maintenance -> Idle (reset).
        let store = FakeStore::with_phase(Phase::Maintenance);
        apply_phase(&store, "evt", "idle", "op@z", 7_000)
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Idle);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("op@z"));
        assert_eq!(state.last_action_epoch_ms, Some(7_000));
    }

    #[tokio::test]
    async fn apply_phase_stamps_audit_on_each_lifecycle_transition() {
        // Every lifecycle transition records the actor and time, not just the
        // recovery-from-maintenance path.
        let store = FakeStore::with_phase(Phase::Idle);
        apply_phase(&store, "evt", "pre_queue", "alice", 1_000)
            .await
            .unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::PreQueue);
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("alice"));
        assert_eq!(state.last_action_epoch_ms, Some(1_000));

        apply_phase(&store, "evt", "active", "bob", 2_000)
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("bob"));
        assert_eq!(state.last_action_epoch_ms, Some(2_000));

        apply_phase(&store, "evt", "post_event", "carol", 3_000)
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("carol"));
        assert_eq!(state.last_action_epoch_ms, Some(3_000));
    }

    #[tokio::test]
    async fn apply_phase_overwrites_prior_audit_from_a_different_action() {
        // A phase transition after a set_rate must update the audit line to
        // set_phase, proving set_phase does not leave the prior action's
        // stamp in place (the bug).
        let store = FakeStore::with_phase(Phase::Active);
        apply_rate(&store, "evt", "500", "rate-op", 1_000)
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_rate"));
        assert_eq!(state.last_action_by.as_deref(), Some("rate-op"));

        apply_phase(&store, "evt", "post_event", "phase-op", 2_000)
            .await
            .unwrap();
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.last_action.as_deref(), Some("set_phase"));
        assert_eq!(state.last_action_by.as_deref(), Some("phase-op"));
        assert_eq!(state.last_action_epoch_ms, Some(2_000));
    }

    #[tokio::test]
    async fn rate_rejects_zero_and_nonnumeric() {
        let store = FakeStore::with_phase(Phase::Active);
        assert!(matches!(
            apply_rate(&store, "evt", "0", "op@x", 1000)
                .await
                .unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert!(matches!(
            apply_rate(&store, "evt", "fast", "op@x", 1000)
                .await
                .unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert_eq!(
            apply_rate(&store, "evt", "500", "op@x", 1000)
                .await
                .unwrap(),
            500
        );
        assert_eq!(*store.rate.lock().unwrap(), Some(500));
    }

    #[tokio::test]
    async fn message_sets_and_allows_empty() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_message(&store, "evt", "Doors open at noon", "op@x", 1000)
            .await
            .unwrap();
        assert_eq!(
            store.message.lock().unwrap().as_deref(),
            Some("Doors open at noon")
        );
        // Second call is spaced beyond the debounce window.
        apply_message(&store, "evt", "", "op@x", 1000 + DEBOUNCE_MS)
            .await
            .unwrap();
        assert_eq!(store.message.lock().unwrap().as_deref(), Some(""));
    }

    #[tokio::test]
    async fn rate_over_ceiling_is_rejected() {
        let store = FakeStore::with_phase(Phase::Active);
        let over = (u64::from(MAX_ADMISSION_RATE) + 1).to_string();
        assert!(matches!(
            apply_rate(&store, "evt", &over, "op@x", 1000).await,
            Err(ApplyError::Action(ActionError::RateTooHigh { .. }))
        ));
        assert!(
            apply_rate(&store, "evt", &MAX_ADMISSION_RATE.to_string(), "op@x", 1000)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn pause_and_resume_move_the_control_between_open_and_paused() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_pause(&store, "evt", "op@x", 1000).await.unwrap();
        assert_eq!(*store.control.lock().unwrap(), AdmissionControl::Paused);
        assert_eq!(
            *store.control_action.lock().unwrap(),
            Some(AdminAction::Pause)
        );
        // Resume spaced beyond the debounce window.
        apply_resume(&store, "evt", "op@x", 1000 + DEBOUNCE_MS)
            .await
            .unwrap();
        assert_eq!(*store.control.lock().unwrap(), AdmissionControl::Open);
        assert_eq!(
            *store.control_action.lock().unwrap(),
            Some(AdminAction::Resume)
        );
    }

    #[tokio::test]
    async fn a_stored_fail_open_is_not_read_as_open() {
        // A bool would collapse fail_open into "not paused" and show an operator
        // a normal, admitting event while the waiting room is bypassed.
        let store = FakeStore {
            control: Mutex::new(AdmissionControl::FailOpen),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        let state = store.load("evt").await.unwrap().unwrap();
        assert_eq!(state.admission_control, AdmissionControl::FailOpen);
        assert_eq!(
            wr_common::serving_state(state.phase, state.admission_control),
            wr_common::ServingState::FailOpen
        );
    }

    #[tokio::test]
    async fn pause_and_resume_are_refused_from_fail_open() {
        // Fail-open is left by recovering, not by pausing or resuming; neither
        // action may quietly overwrite it.
        let store = FakeStore {
            control: Mutex::new(AdmissionControl::FailOpen),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        assert!(matches!(
            apply_pause(&store, "evt", "op@x", 1000).await,
            Err(ApplyError::Action(ActionError::Conflict))
        ));
        assert!(matches!(
            apply_resume(&store, "evt", "op@x", 1000).await,
            Err(ApplyError::Action(ActionError::Conflict))
        ));
        assert_eq!(*store.control.lock().unwrap(), AdmissionControl::FailOpen);
    }

    #[tokio::test]
    async fn pause_on_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..Default::default()
        };
        assert!(matches!(
            apply_pause(&store, "evt", "op@x", 1000).await,
            Err(ApplyError::Action(ActionError::NotFound))
        ));
    }

    #[tokio::test]
    async fn second_mutation_within_debounce_window_is_rejected() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_rate(&store, "evt", "500", "op@x", 1000)
            .await
            .unwrap();
        // A second mutation 100ms later (< DEBOUNCE_MS) is rejected.
        assert!(matches!(
            apply_rate(&store, "evt", "600", "op@x", 1100).await,
            Err(ApplyError::Action(ActionError::TooFast))
        ));
        // The rate did not change.
        assert_eq!(*store.rate.lock().unwrap(), Some(500));
        // After the window, it succeeds.
        assert!(
            apply_rate(&store, "evt", "600", "op@x", 1000 + DEBOUNCE_MS)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn pause_when_already_paused_is_a_conflict() {
        let store = FakeStore {
            control: Mutex::new(AdmissionControl::Paused),
            phase: Mutex::new(Phase::Active),
            ..Default::default()
        };
        // The transition is illegal from Paused, so it is refused before any
        // write is attempted.
        assert!(matches!(
            apply_pause(&store, "evt", "op@x", 1000).await,
            Err(ApplyError::Action(ActionError::Conflict))
        ));
        assert_eq!(*store.control.lock().unwrap(), AdmissionControl::Paused);
    }

    #[tokio::test]
    async fn force_maintenance_is_not_debounced() {
        let store = FakeStore::with_phase(Phase::Active);
        // A recent mutation sets the debounce clock.
        apply_rate(&store, "evt", "500", "op@x", 1000)
            .await
            .unwrap();
        // Force maintenance immediately after still applies (emergency stop).
        apply_reset(&store, "evt", "op@x", 1100).await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
    }
}
