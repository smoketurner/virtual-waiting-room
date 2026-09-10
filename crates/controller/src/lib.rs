//! Closed-loop outflow controller (DESIGN §7, ADR-0006 position expiry).
//!
//! Each interval the controller runs one pass over an event's `Counters` item:
//! it measures the arrival rate against what it released last interval, derives
//! the no-show rate, smooths it, and advances `serving_counter` by a bounded
//! correction so the origin runs at the operator's target rate despite visitors
//! who never click through. It then expires positions whose `expires_at` has
//! passed and advances `max_expired_position` (ADR-0006).
//!
//! All arithmetic here is checked or saturating: the release profile has no
//! overflow checks, so a bare subtraction that underflows would wrap to a huge
//! value and release a damaging burst. `no_show_state` and `release_next` never
//! panic and never wrap for any input.

use std::future::Future;

use wr_domain::{Phase, SHARDS};

pub mod dynamo;

/// The controller interval in seconds. The `EventBridge` Scheduler `rate()`
/// minimum is one minute, so the schedule fires `rate(1 minute)` and the
/// handler runs [`PASSES_PER_INVOKE`] passes this many seconds apart, giving the
/// design's 10-second cadence (DESIGN §7) within the scheduler's floor.
pub const INTERVAL_SECS: u64 = 10;

/// Passes per Lambda invoke. `INTERVAL_SECS * PASSES_PER_INVOKE == 60`, so one
/// `rate(1 minute)` invoke covers a full minute at the 10-second cadence.
pub const PASSES_PER_INVOKE: u32 = 6;

/// EWMA smoothing factor for the no-show rate. The smoothed rate is
/// `alpha * observed + (1 - alpha) * previous`; a smaller alpha reacts more
/// slowly and damps oscillation harder. 0.3 tracks a real shift within a few
/// intervals while absorbing single-interval measurement noise (DESIGN §7).
pub const EWMA_ALPHA: f64 = 0.3;

/// Upper bound on the correction: `release_next` is capped at this multiple of
/// `target_rate`, so even a no-show rate measured near 1.0 (almost nobody
/// arrived) cannot release more than this many times the target in one interval
/// (DESIGN §7 "the correction is bounded").
pub const MAX_CORRECTION_MULTIPLE: f64 = 2.0;

/// The controller's smoothed view of the no-show rate, carried across intervals
/// on the `Counters` item so smoothing survives the stateless Lambda. `None`
/// before the first measurement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NoShowState {
    /// The EWMA-smoothed no-show rate in `[0.0, 1.0)`.
    pub smoothed_rate: f64,
}

/// Inputs read from the `Counters` item for one release computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseInputs {
    /// Per-shard arrival counts (`arrivals#0..9`), summed to the total arrivals
    /// observed since the event began.
    pub arrivals: [u64; SHARDS],
    /// The arrivals total observed at the end of the previous interval. The
    /// arrivals in this interval are `sum(arrivals) - last_arrivals_total`.
    pub last_arrivals_total: u64,
    /// `serving_counter` at the end of the previous interval. The positions
    /// released last interval are `serving_counter - last_serving_counter`.
    pub last_serving_counter: u64,
    /// The current `serving_counter`.
    pub serving_counter: u64,
    /// Operator target rate in visitors per second (the `/admin/rate` value).
    pub target_rate: u32,
}

/// The outcome of one release computation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReleaseDecision {
    /// Positions to release this interval (added to `serving_counter`).
    pub release: u64,
    /// The new `serving_counter` after the advance.
    pub next_serving_counter: u64,
    /// The arrivals total to persist as `last_arrivals_total` for next interval.
    pub arrivals_total: u64,
    /// The smoothed no-show state to persist for next interval.
    pub no_show: NoShowState,
}

/// Sums the per-shard arrival counts, saturating so a corrupt oversized shard
/// value cannot wrap the total.
#[must_use]
pub fn sum_arrivals(arrivals: [u64; SHARDS]) -> u64 {
    let mut total = 0u64;
    for count in arrivals {
        total = total.saturating_add(count);
    }
    total
}

/// The target release for one interval from a per-second rate: `target_rate`
/// visitors per second times [`INTERVAL_SECS`]. Saturating so a ceiling-valued
/// rate cannot wrap.
#[must_use]
pub fn target_release_per_interval(target_rate: u32) -> u64 {
    u64::from(target_rate).saturating_mul(INTERVAL_SECS)
}

/// Computes the release for this interval and the state to carry forward.
///
/// The no-show rate is `1 - observed_arrivals / released_last_interval`; with no
/// release last interval there is nothing to measure, so the correction falls
/// back to the raw target and the smoothed state is left unchanged. The smoothed
/// rate is fed through an EWMA and the resulting `release_next` is bounded at
/// [`MAX_CORRECTION_MULTIPLE`] times the target (DESIGN §7).
///
/// Every subtraction is saturating: `arrivals` and `serving_counter` are read
/// from separate updates and can momentarily read lower than the stored
/// baseline (an eventually-consistent read, or a manual counter edit), which a
/// bare subtraction would wrap under the overflow-check-free release profile.
#[must_use]
pub fn compute_release(inputs: ReleaseInputs, prev: Option<NoShowState>) -> ReleaseDecision {
    let arrivals_total = sum_arrivals(inputs.arrivals);
    let observed_arrivals = arrivals_total.saturating_sub(inputs.last_arrivals_total);
    let released_last = inputs
        .serving_counter
        .saturating_sub(inputs.last_serving_counter);

    let target = target_release_per_interval(inputs.target_rate);

    let (release, no_show) = if released_last == 0 {
        // No release last interval: nothing to measure, hold the target and keep
        // the prior smoothed state.
        let carried = prev.unwrap_or(NoShowState { smoothed_rate: 0.0 });
        (target, carried)
    } else {
        // observed / released clamps to [0, 1]; more arrivals than releases
        // (a straggler race) reads as a 0 no-show rate, never negative.
        #[expect(
            clippy::cast_precision_loss,
            reason = "counts far below f64's 2^53 exact-integer range at any realistic event size"
        )]
        let arrival_fraction = (observed_arrivals as f64 / released_last as f64).clamp(0.0, 1.0);
        let observed_no_show = 1.0 - arrival_fraction;

        let smoothed = match prev {
            Some(state) => EWMA_ALPHA * observed_no_show + (1.0 - EWMA_ALPHA) * state.smoothed_rate,
            None => observed_no_show,
        }
        .clamp(0.0, 1.0);

        (
            bounded_release(target, smoothed),
            NoShowState {
                smoothed_rate: smoothed,
            },
        )
    };

    let next_serving_counter = inputs.serving_counter.saturating_add(release);

    ReleaseDecision {
        release,
        next_serving_counter,
        arrivals_total,
        no_show,
    }
}

/// `target / (1 - smoothed_no_show_rate)`, bounded at [`MAX_CORRECTION_MULTIPLE`]
/// times the target and returned as a saturating `u64`. A smoothed rate at or
/// above the point where the correction would exceed the cap yields exactly the
/// capped value, so a near-1.0 rate cannot divide by (near) zero into a burst.
#[must_use]
fn bounded_release(target: u64, smoothed_no_show: f64) -> u64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "target is target_rate * 10, far below f64's exact-integer range"
    )]
    let target_f = target as f64;
    let cap = target_f * MAX_CORRECTION_MULTIPLE;

    let denom = 1.0 - smoothed_no_show;
    // smoothed_no_show is clamped to [0,1]; denom <= 0 only at exactly 1.0.
    let raw = if denom <= 0.0 {
        cap
    } else {
        (target_f / denom).min(cap)
    };

    // raw is finite and in [0, cap]; round to nearest and clamp into u64.
    let rounded = raw.round();
    #[expect(
        clippy::cast_precision_loss,
        reason = "u64::MAX as f64 is an upper-bound guard; exactness at the boundary is irrelevant"
    )]
    let u64_max_f = u64::MAX as f64;
    if rounded <= 0.0 {
        0
    } else if rounded >= u64_max_f {
        u64::MAX
    } else {
        #[expect(
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation,
            reason = "rounded is finite, > 0, and < u64::MAX by the guards above"
        )]
        let out = rounded as u64;
        out
    }
}

/// A position the expiry pass found live-but-expired: its `expires_at` has
/// passed and its status is still `issued`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredPosition {
    pub request_id: String,
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("controller store error: {0}")]
pub struct StoreError(pub String);

/// The current `Counters` state the controller needs before it acts.
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerState {
    pub phase: Phase,
    pub inputs: ReleaseInputs,
    pub prev_no_show: Option<NoShowState>,
    pub max_expired_position: u64,
}

/// The persistence port the controller drives. A trait seam so the pipeline runs
/// AWS-free in tests (the `dynamo` module supplies the live implementation).
pub trait Store {
    /// Reads the controller-relevant `Counters` state for the event.
    fn read_state(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send;

    /// Advances `serving_counter` to `decision.next_serving_counter` and
    /// persists the carried-forward smoothing state in one `UpdateItem`, guarded
    /// so a lost race against another controller invoke or an operator rate
    /// change does not double-advance.
    fn write_release(
        &self,
        event_id: &str,
        decision: &ReleaseDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Returns positions whose `expires_at < now` and `status = issued`, applying
    /// a `FilterExpression` on `expires_at` so a TTL-pending-but-still-visible
    /// item is never returned (ADR-0006).
    fn query_expired(
        &self,
        now: u64,
    ) -> impl Future<Output = Result<Vec<ExpiredPosition>, StoreError>> + Send;

    /// Marks a position `expired`, guarded on it still being `issued` so a
    /// completed/abandoned position is not overwritten.
    fn mark_expired(&self, request_id: &str)
    -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Advances `max_expired_position` on the `Counters` item to the given value
    /// (only ever forward; a lower value is a no-op at the store).
    fn advance_max_expired(
        &self,
        event_id: &str,
        max_expired_position: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The result of one controller pass.
#[derive(Debug, Clone, PartialEq)]
pub enum PassOutcome {
    /// The event is not `Active`; the controller did nothing.
    NotActive(Phase),
    /// The controller ran: it released `released` positions and expired
    /// `expired` positions.
    Ran { released: u64, expired: usize },
}

/// Runs one controller pass for the event: read state, gate on `Active`, compute
/// and write the release, then expire due positions and advance
/// `max_expired_position` (DESIGN §7, ADR-0006).
///
/// `now` is the current epoch-seconds, passed in so the logic is deterministic
/// under test.
///
/// # Errors
///
/// Returns [`StoreError`] if any read or write fails.
pub async fn run_pass<S: Store>(
    store: &S,
    event_id: &str,
    now: u64,
) -> Result<PassOutcome, StoreError> {
    let state = store.read_state(event_id).await?;
    if state.phase != Phase::Active {
        tracing::debug!(event_id, phase = ?state.phase, "controller skipped: not active");
        return Ok(PassOutcome::NotActive(state.phase));
    }

    let decision = compute_release(state.inputs, state.prev_no_show);
    store
        .write_release(event_id, &decision, state.inputs.serving_counter)
        .await?;

    let expired = expire_due(store, event_id, now, state.max_expired_position).await?;

    tracing::info!(
        event_id,
        released = decision.release,
        serving_counter = decision.next_serving_counter,
        no_show_rate = decision.no_show.smoothed_rate,
        expired,
        "controller pass"
    );
    Ok(PassOutcome::Ran {
        released: decision.release,
        expired,
    })
}

/// Expires every due position and advances `max_expired_position` past the
/// highest expired one. Returns the number expired.
async fn expire_due<S: Store>(
    store: &S,
    event_id: &str,
    now: u64,
    current_max_expired: u64,
) -> Result<usize, StoreError> {
    let due = store.query_expired(now).await?;
    if due.is_empty() {
        return Ok(0);
    }

    for position in &due {
        store.mark_expired(&position.request_id).await?;
    }

    // The controller advances the cursor past everything it just expired. The
    // count of newly expired positions is a monotonic advance; adding it is
    // saturating so the cursor cannot wrap.
    let advanced = current_max_expired.saturating_add(due.len() as u64);
    store.advance_max_expired(event_id, advanced).await?;

    Ok(due.len())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;

    fn inputs(
        arrivals_total: u64,
        last_arrivals: u64,
        serving: u64,
        last_serving: u64,
        rate: u32,
    ) -> ReleaseInputs {
        let mut arrivals = [0u64; SHARDS];
        arrivals[0] = arrivals_total;
        ReleaseInputs {
            arrivals,
            last_arrivals_total: last_arrivals,
            last_serving_counter: last_serving,
            serving_counter: serving,
            target_rate: rate,
        }
    }

    #[test]
    fn no_release_last_interval_falls_back_to_target() {
        // released_last = serving - last_serving = 0.
        let d = compute_release(inputs(0, 0, 100, 100, 50), None);
        assert_eq!(d.release, 500); // 50/s * 10s
        assert_eq!(d.next_serving_counter, 600);
        assert!(d.no_show.smoothed_rate.abs() < 1e-9);
    }

    #[test]
    fn full_arrival_yields_target_release() {
        // Released 500 last interval, all 500 arrived -> no-show 0 -> release target.
        let d = compute_release(inputs(500, 0, 1000, 500, 50), None);
        assert_eq!(d.release, 500);
    }

    #[test]
    fn half_no_show_doubles_but_is_capped() {
        // Released 500, only 250 arrived -> no-show 0.5 -> target/0.5 = 1000 = 2x cap.
        let d = compute_release(inputs(250, 0, 1000, 500, 50), None);
        assert!((d.no_show.smoothed_rate - 0.5).abs() < 1e-9);
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn near_total_no_show_is_bounded_at_cap() {
        // Released 500, 1 arrived -> no-show ~0.998 -> target/denom huge, capped at 2x.
        let d = compute_release(inputs(1, 0, 1000, 500, 50), None);
        assert_eq!(d.release, 1000); // 2 * 500 cap, never a burst
    }

    #[test]
    fn zero_arrivals_is_bounded_at_cap_not_infinite() {
        // no-show exactly 1.0 -> denom 0 -> cap, not a division by zero.
        let d = compute_release(inputs(0, 0, 1000, 500, 50), None);
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn ewma_smooths_across_intervals() {
        // Prior smoothed 0.0, observe 0.5: smoothed = 0.3*0.5 + 0.7*0.0 = 0.15.
        let prev = NoShowState { smoothed_rate: 0.0 };
        let d = compute_release(inputs(250, 0, 1000, 500, 50), Some(prev));
        assert!((d.no_show.smoothed_rate - 0.15).abs() < 1e-9);
        // release = 500 / (1 - 0.15) = 588.24 -> 588.
        assert_eq!(d.release, 588);
    }

    #[test]
    fn more_arrivals_than_released_reads_zero_no_show() {
        // Straggler race: 600 arrived against 500 released -> clamp to 0 no-show.
        let d = compute_release(inputs(600, 0, 1000, 500, 50), None);
        assert!(d.no_show.smoothed_rate.abs() < 1e-9);
        assert_eq!(d.release, 500);
    }

    #[test]
    fn arrivals_reading_below_baseline_does_not_underflow() {
        // last_arrivals_total > sum(arrivals): eventually-consistent low read.
        let d = compute_release(inputs(100, 500, 1000, 500, 50), None);
        // observed saturates to 0; released 500 -> no-show 1.0 -> capped.
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn serving_reading_below_baseline_does_not_underflow() {
        // serving_counter < last_serving_counter: released saturates to 0 -> target.
        let d = compute_release(inputs(0, 0, 400, 500, 50), None);
        assert_eq!(d.release, 500);
        assert_eq!(d.next_serving_counter, 900);
    }

    #[test]
    fn sum_arrivals_saturates() {
        let d = compute_release(inputs(u64::MAX, 0, u64::MAX, 0, 100_000), None);
        // next_serving_counter saturates rather than wrapping.
        assert_eq!(d.next_serving_counter, u64::MAX);
    }

    // --- Fake store for pass orchestration -----------------------------------

    struct FakeStore {
        state: ControllerState,
        due: Vec<ExpiredPosition>,
        marked: Mutex<Vec<String>>,
        released: Mutex<Option<ReleaseDecision>>,
        advanced: Mutex<Option<u64>>,
    }

    impl FakeStore {
        fn new(state: ControllerState, due: Vec<ExpiredPosition>) -> Self {
            Self {
                state,
                due,
                marked: Mutex::new(Vec::new()),
                released: Mutex::new(None),
                advanced: Mutex::new(None),
            }
        }
    }

    impl Store for FakeStore {
        fn read_state(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send {
            std::future::ready(Ok(self.state.clone()))
        }

        fn write_release(
            &self,
            _event_id: &str,
            decision: &ReleaseDecision,
            _expected: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.released.lock().unwrap() = Some(*decision);
            std::future::ready(Ok(()))
        }

        fn query_expired(
            &self,
            _now: u64,
        ) -> impl Future<Output = Result<Vec<ExpiredPosition>, StoreError>> + Send {
            std::future::ready(Ok(self.due.clone()))
        }

        fn mark_expired(
            &self,
            request_id: &str,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.marked.lock().unwrap().push(request_id.to_owned());
            std::future::ready(Ok(()))
        }

        fn advance_max_expired(
            &self,
            _event_id: &str,
            max_expired_position: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.advanced.lock().unwrap() = Some(max_expired_position);
            std::future::ready(Ok(()))
        }
    }

    fn active_state(due_max_expired: u64) -> ControllerState {
        ControllerState {
            phase: Phase::Active,
            inputs: inputs(250, 0, 1000, 500, 50),
            prev_no_show: None,
            max_expired_position: due_max_expired,
        }
    }

    #[tokio::test]
    async fn pass_releases_and_expires_when_active() {
        let due = vec![
            ExpiredPosition {
                request_id: "r1".to_owned(),
            },
            ExpiredPosition {
                request_id: "r2".to_owned(),
            },
        ];
        let store = FakeStore::new(active_state(10), due);
        let outcome = run_pass(&store, "evt", 1_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 1000,
                expired: 2,
            }
        );
        assert_eq!(store.marked.lock().unwrap().len(), 2);
        // max_expired advanced from 10 by the 2 expired.
        assert_eq!(*store.advanced.lock().unwrap(), Some(12));
        assert!(store.released.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn pass_is_noop_when_not_active() {
        let mut state = active_state(0);
        state.phase = Phase::PreQueue;
        let store = FakeStore::new(state, Vec::new());
        let outcome = run_pass(&store, "evt", 1_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::NotActive(Phase::PreQueue));
        assert!(store.released.lock().unwrap().is_none());
        assert!(store.advanced.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn no_due_positions_skips_the_cursor_write() {
        let store = FakeStore::new(active_state(5), Vec::new());
        let outcome = run_pass(&store, "evt", 1_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 1000,
                expired: 0,
            }
        );
        assert_eq!(*store.advanced.lock().unwrap(), None);
    }

    // --- Property tests: no input underflows/overflows or panics -------------

    use proptest::prelude::{Just, Strategy, any, prop_assert, prop_oneof, proptest};

    fn shard_arrivals() -> impl Strategy<Value = [u64; SHARDS]> {
        proptest::array::uniform10(prop_oneof![0u64..1_000_000, Just(u64::MAX), any::<u64>(),])
    }

    proptest! {
        #[test]
        fn compute_release_never_panics_or_wraps(
            arrivals in shard_arrivals(),
            last_arrivals in any::<u64>(),
            serving in any::<u64>(),
            last_serving in any::<u64>(),
            rate in 1u32..=100_000,
            prev_rate in prop_oneof![Just(None), (0.0f64..1.0).prop_map(|r| Some(NoShowState { smoothed_rate: r }))],
        ) {
            let d = compute_release(
                ReleaseInputs {
                    arrivals,
                    last_arrivals_total: last_arrivals,
                    last_serving_counter: last_serving,
                    serving_counter: serving,
                    target_rate: rate,
                },
                prev_rate,
            );
            // release is bounded by the cap (2x target per interval), a finite u64.
            let cap = target_release_per_interval(rate).saturating_mul(2);
            prop_assert!(d.release <= cap.saturating_add(1));
            // next_serving_counter never wraps below the current serving_counter.
            prop_assert!(d.next_serving_counter >= serving || d.next_serving_counter == u64::MAX);
            // smoothed rate stays a valid probability.
            prop_assert!(d.no_show.smoothed_rate >= 0.0 && d.no_show.smoothed_rate <= 1.0);
            prop_assert!(d.no_show.smoothed_rate.is_finite());
        }

        #[test]
        fn sum_arrivals_never_wraps(arrivals in shard_arrivals()) {
            let total = sum_arrivals(arrivals);
            // Each element <= total unless total saturated at u64::MAX.
            for a in arrivals {
                prop_assert!(a <= total || total == u64::MAX);
            }
        }
    }
}
