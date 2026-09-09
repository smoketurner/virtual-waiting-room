//! Pure control-plane logic for the `admin` Lambda: phase-transition rules and
//! the operator actions, each expressed as a change to the `Counters` item.
//! Generic over a [`Store`] port so the logic runs without AWS; the SDK-backed
//! implementation lives in `dynamo`, the Axum wiring in `main`.

use std::future::Future;

use wr_domain::Phase;

pub mod dynamo;
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
    /// operators cannot race a transition. Returns `Conflict` if the stored
    /// phase no longer matches `from`.
    fn set_phase(
        &self,
        event_id: &str,
        from: Phase,
        to: Phase,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the admission target rate the outflow controller reads.
    fn set_rate(
        &self,
        event_id: &str,
        rate: u32,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Sets the operator broadcast message shown on phase pages / `/status`.
    fn set_message(
        &self,
        event_id: &str,
        message: &str,
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
    match s {
        "idle" => Ok(Phase::Idle),
        "pre_queue" => Ok(Phase::PreQueue),
        "active" => Ok(Phase::Active),
        "post_event" => Ok(Phase::PostEvent),
        "maintenance" => Ok(Phase::Maintenance),
        _ => Err(ActionError::UnknownPhase),
    }
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

/// Applies a phase-transition action: validates the target and the transition,
/// then performs the guarded store write.
///
/// # Errors
///
/// [`ActionError`] if the phase is unknown, the event is missing, or the
/// transition is illegal; the store's [`StoreError`] is mapped to
/// [`ActionError`] on a lost race.
pub async fn apply_phase<S: Store>(
    store: &S,
    event_id: &str,
    to: &str,
) -> Result<Phase, ApplyError> {
    let to = parse_phase(to)?;
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    if !transition_allowed(state.phase, to) {
        return Err(ActionError::IllegalTransition {
            from: state.phase,
            to,
        }
        .into());
    }
    store.set_phase(event_id, state.phase, to).await?;
    Ok(to)
}

/// Forces the maintenance phase — the safe operator stop, legal from any phase.
///
/// # Errors
///
/// [`ApplyError`] if the event is missing or the store write fails.
pub async fn apply_reset<S: Store>(store: &S, event_id: &str) -> Result<(), ApplyError> {
    let Some(state) = store.load(event_id).await? else {
        return Err(ActionError::NotFound.into());
    };
    store
        .set_phase(event_id, state.phase, Phase::Maintenance)
        .await?;
    Ok(())
}

/// Sets the admission target rate.
///
/// # Errors
///
/// [`ActionError::InvalidRate`] if `rate` is not a positive integer; store
/// errors otherwise.
pub async fn apply_rate<S: Store>(
    store: &S,
    event_id: &str,
    rate: &str,
) -> Result<u32, ApplyError> {
    let rate: u32 = rate.parse().map_err(|_| ActionError::InvalidRate)?;
    if rate == 0 {
        return Err(ActionError::InvalidRate.into());
    }
    store.set_rate(event_id, rate).await?;
    Ok(rate)
}

/// Sets the operator broadcast message.
///
/// # Errors
///
/// Store errors only; any string (including empty, which clears it) is allowed.
pub async fn apply_message<S: Store>(
    store: &S,
    event_id: &str,
    message: &str,
) -> Result<(), ApplyError> {
    store.set_message(event_id, message).await?;
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
        missing: bool,
        /// When set, the next `set_phase` reports a lost race.
        conflict: bool,
    }

    impl Default for FakeStore {
        fn default() -> Self {
            Self {
                phase: Mutex::new(Phase::Idle),
                rate: Mutex::new(None),
                message: Mutex::new(None),
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
                }))
            };
            std::future::ready(result)
        }

        fn set_phase(
            &self,
            _event_id: &str,
            _from: Phase,
            to: Phase,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            let result = if self.conflict {
                Err(StoreError::Conflict)
            } else {
                *self.phase.lock().unwrap() = to;
                Ok(())
            };
            std::future::ready(result)
        }

        fn set_rate(
            &self,
            _event_id: &str,
            rate: u32,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.rate.lock().unwrap() = Some(rate);
            std::future::ready(Ok(()))
        }

        fn set_message(
            &self,
            _event_id: &str,
            message: &str,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.message.lock().unwrap() = Some(message.to_owned());
            std::future::ready(Ok(()))
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

    #[tokio::test]
    async fn apply_phase_advances_and_persists() {
        let store = FakeStore::with_phase(Phase::Idle);
        let to = apply_phase(&store, "evt", "pre_queue").await.unwrap();
        assert_eq!(to, Phase::PreQueue);
        assert_eq!(*store.phase.lock().unwrap(), Phase::PreQueue);
    }

    #[tokio::test]
    async fn apply_phase_rejects_illegal_transition() {
        let store = FakeStore::with_phase(Phase::Idle);
        let err = apply_phase(&store, "evt", "active").await.unwrap_err();
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
        let err = apply_phase(&store, "evt", "bogus").await.unwrap_err();
        assert!(matches!(err, ApplyError::Action(ActionError::UnknownPhase)));
    }

    #[tokio::test]
    async fn apply_phase_missing_event_is_not_found() {
        let store = FakeStore {
            missing: true,
            ..FakeStore::default()
        };
        let err = apply_phase(&store, "evt", "pre_queue").await.unwrap_err();
        assert!(matches!(err, ApplyError::Action(ActionError::NotFound)));
    }

    #[tokio::test]
    async fn apply_phase_surfaces_a_lost_race() {
        let store = FakeStore {
            phase: Mutex::new(Phase::Idle),
            conflict: true,
            ..FakeStore::default()
        };
        let err = apply_phase(&store, "evt", "pre_queue").await.unwrap_err();
        assert!(matches!(err, ApplyError::Store(StoreError::Conflict)));
    }

    #[tokio::test]
    async fn reset_forces_maintenance_from_any_phase() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_reset(&store, "evt").await.unwrap();
        assert_eq!(*store.phase.lock().unwrap(), Phase::Maintenance);
    }

    #[tokio::test]
    async fn rate_rejects_zero_and_nonnumeric() {
        let store = FakeStore::with_phase(Phase::Active);
        assert!(matches!(
            apply_rate(&store, "evt", "0").await.unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert!(matches!(
            apply_rate(&store, "evt", "fast").await.unwrap_err(),
            ApplyError::Action(ActionError::InvalidRate)
        ));
        assert_eq!(apply_rate(&store, "evt", "500").await.unwrap(), 500);
        assert_eq!(*store.rate.lock().unwrap(), Some(500));
    }

    #[tokio::test]
    async fn message_sets_and_allows_empty() {
        let store = FakeStore::with_phase(Phase::Active);
        apply_message(&store, "evt", "Doors open at noon")
            .await
            .unwrap();
        assert_eq!(
            store.message.lock().unwrap().as_deref(),
            Some("Doors open at noon")
        );
        apply_message(&store, "evt", "").await.unwrap();
        assert_eq!(store.message.lock().unwrap().as_deref(), Some(""));
    }
}
