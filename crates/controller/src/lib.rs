//! Closed-loop outflow controller with position expiry.
//!
//! Each interval the controller runs one pass over an event's `Counters` item:
//! it measures the arrival rate against what it released last interval, derives
//! the no-show rate, smooths it, and advances `serving_counter` by a bounded
//! correction so the origin runs at the operator's target rate despite visitors
//! who never click through. It then expires positions whose `expires_at` has
//! passed and advances `max_expired_position`.
//!
//! A pass runs only when the event is `Active` **and** the operator's
//! [`AdmissionControl`] is `Open`. The phase says the event is running; the
//! admission control says the operator is letting visitors through. Either gate
//! closed means the pass returns without advancing `serving_counter`.
//!
//! All arithmetic here is checked or saturating: the release profile has no
//! overflow checks, so a bare subtraction that underflows would wrap to a huge
//! value and release a damaging burst. `no_show_state` and `release_next` never
//! panic and never wrap for any input.

use std::future::Future;

use serde::{Deserialize, Serialize};
use wr_common::{AdmissionControl, Phase, SHARDS, StoredControl, resolve};

pub mod dynamo;

/// The controller interval in seconds. The `EventBridge` Scheduler `rate()`
/// minimum is one minute, so the schedule fires `rate(1 minute)` and the
/// handler runs [`PASSES_PER_INVOKE`] passes this many seconds apart, giving the
/// design's 10-second cadence within the scheduler's floor.
pub const INTERVAL_SECS: u64 = 10;

/// Passes per durable execution. `INTERVAL_SECS * PASSES_PER_INVOKE == 60`, so
/// one execution started by the `rate(1 minute)` schedule covers a full minute
/// at the 10-second cadence. The execution spans several Lambda invocations:
/// each durable wait suspends it and Lambda invokes the function again to
/// resume.
pub const PASSES_PER_INVOKE: u32 = 6;

/// EWMA smoothing factor for the no-show rate. The smoothed rate is
/// `alpha * observed + (1 - alpha) * previous`; a smaller alpha reacts more
/// slowly and damps oscillation harder. 0.3 tracks a real shift within a few
/// intervals while absorbing single-interval measurement noise.
pub const EWMA_ALPHA: f64 = 0.3;

/// `DynamoDB`'s documented positive `Number` minimum magnitude.
///
/// The smoothed no-show rate is persisted to the event's `Counters` item as a
/// `DynamoDB` `Number`. A persist of a positive value below this floor is
/// rejected with a `ValidationException` (Number underflow), which is not the
/// lost-race path the [`dynamo::DynamoStore::write_release`] match swallows,
/// so it halts the pass and leaves `serving_counter` stuck. Under sustained
/// zero observed no-show the EWMA decays geometrically and crosses this floor
/// after ~836 intervals; [`compute_release`] then floors any such value to
/// `0.0`, which is operationally indistinguishable (the release correction is
/// identical at `1 - 0.7^836` and at `0`) but storable as a `DynamoDB` `Number`.
pub const DDB_NUMBER_MIN_POSITIVE: f64 = 1e-130;

/// How long a visitor has to claim a position after the cursor reaches it,
/// before the controller treats them as a no-show and expires it.
///
/// Expressed as time but applied positionally: at `target_rate` per second the
/// cursor covers `target_rate * ADMISSION_GRACE_SECS` positions in that window,
/// so a position this far behind the cursor was offered that long ago. Doing it
/// this way needs no per-position write when a position is reached, and it
/// stops automatically when admission is paused, because a paused cursor does
/// not move.
pub const ADMISSION_GRACE_SECS: u64 = 120;

/// Upper bound on the correction: `release_next` is capped at this multiple of
/// `target_rate`, so even a no-show rate measured near 1.0 (almost nobody
/// arrived) cannot release more than this many times the target in one interval
/// (the correction is bounded).
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
    /// `serving_counter` as it stood *before* the previous interval's release.
    /// The positions released then are `serving_counter - last_serving_counter`,
    /// which is zero for every interval if this records the value after the
    /// release instead — and a measured release of zero disables the whole
    /// no-show correction, because there is nothing to compare arrivals against.
    pub last_serving_counter: u64,
    /// The current `serving_counter`.
    pub serving_counter: u64,
    /// The highest position ever issued. The live-join sequence ends here, and
    /// after a seal it starts at the cohort size, so this is the end of the
    /// line however the positions were assigned.
    pub queue_counter: u64,
    /// Operator target rate in visitors per second (the `/admin/rate` value).
    pub target_rate: u32,
}

/// The outcome of one release computation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReleaseDecision {
    /// Positions actually released this interval — the distance the cursor
    /// moved, which is less than the computed target when the end of the line
    /// is reached.
    pub release: u64,
    /// The new `serving_counter` after the advance.
    pub next_serving_counter: u64,
    /// The cursor as it stood before this release. Persisted as
    /// `last_serving_counter` so the next interval can measure what this one
    /// released. Carried on the decision rather than recomputed at the store,
    /// so there is one place for it to be right.
    pub previous_serving_counter: u64,
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
/// [`MAX_CORRECTION_MULTIPLE`] times the target.
///
/// Every subtraction is saturating: `arrivals` and `serving_counter` are read
/// from separate updates and can momentarily read lower than the stored
/// baseline (an eventually-consistent read, or a manual counter edit), which a
/// bare subtraction would wrap under the overflow-check-free release profile.
#[must_use]
pub fn compute_release(inputs: ReleaseInputs, prev: Option<NoShowState>) -> ReleaseDecision {
    let arrivals_total = sum_arrivals(inputs.arrivals);
    let observed_arrivals = arrivals_total.saturating_sub(inputs.last_arrivals_total);
    // Positions are 1-indexed and the cursor is exclusive, so the people the
    // cursor passed last interval are `[max(last, 1), serving_counter)`.
    // Position 0 is never issued: a cursor moving from 0 to 1 releases nobody,
    // which is exactly what the end-of-line clamp does on an empty queue. Left
    // in, that phantom is measured against zero arrivals as a 100% no-show and
    // pins the smoothed rate at EWMA_ALPHA for the rest of the empty period —
    // an over-release waiting for the moment real traffic starts. Counting only
    // issued positions keeps this the same units as `observed_arrivals`: people.
    let released_last = inputs
        .serving_counter
        .saturating_sub(inputs.last_serving_counter.max(1));

    let target = target_release_per_interval(inputs.target_rate);

    let (release, smoothed) = if released_last == 0 {
        // No release last interval: nothing to measure, hold the target and keep
        // the prior smoothed state.
        (
            target,
            prev.unwrap_or(NoShowState { smoothed_rate: 0.0 })
                .smoothed_rate,
        )
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

        (bounded_release(target, smoothed), smoothed)
    };

    // DynamoDB rejects a persisted `Number` below its positive minimum
    // (`DDB_NUMBER_MIN_POSITIVE`) with a `ValidationException` (Number underflow)
    // that the `write_release` error match does not treat as a benign race, so
    // it halts the pass and leaves `serving_counter` stuck. The EWMA reaches
    // such a value only by decaying geometrically under sustained zero observed
    // no-show (~836 intervals); a rate this small is indistinguishable from
    // `0.0` for the release correction (`bounded_release` returns the target
    // either way), so floor it — the freshly-smoothed value or the carried
    // forward one — to `0.0` before it is persisted or carried into the next
    // pass.
    let smoothed = if smoothed > 0.0 && smoothed < DDB_NUMBER_MIN_POSITIVE {
        0.0
    } else {
        smoothed
    };

    let no_show = NoShowState {
        smoothed_rate: smoothed,
    };

    // The cursor is exclusive — position p is admitted once p < serving_counter —
    // and queue_counter is the highest position ever issued, so one past it
    // admits everyone in line and nobody who is not. Advancing further banks
    // admission credit against an empty queue, and the next burst to arrive
    // walks straight through every position already released, which is the one
    // thing the room exists to prevent.
    //
    // .max() keeps the cursor monotonic: a queue_counter that reads behind the
    // cursor must never drag admission backwards.
    let ceiling = inputs.queue_counter.saturating_add(1);
    let next_serving_counter = inputs
        .serving_counter
        .saturating_add(release)
        .min(ceiling)
        .max(inputs.serving_counter);

    // What the cursor actually moved, not what was asked for. Reporting the
    // requested figure would have the next interval measure arrivals against
    // people who were never released.
    let release = next_serving_counter.saturating_sub(inputs.serving_counter);

    ReleaseDecision {
        release,
        next_serving_counter,
        previous_serving_counter: inputs.serving_counter,
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

/// A position the cursor passed long enough ago to count as a no-show, whose
/// status is still `issued`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredPosition {
    pub request_id: String,
    pub position: u64,
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("controller store error: {0}")]
pub struct StoreError(pub String);

/// The outcome of a guarded [`Store::write_release`]: either the release
/// landed, or a concurrent invoke advanced the cursor first and this pass's
/// release was a no-op.
///
/// `run_pass` derives the expiry cutoff from `decision.next_serving_counter`,
/// which only reflects the persisted cursor when the release landed. On
/// [`ReleaseOutcome::LostRace`] that value was never written, so using it for
/// expiry would mark positions `expired` against a phantom cursor — positions
/// whose owners were offered a place fewer than [`ADMISSION_GRACE_SECS`] ago.
/// The next pass reads the persisted cursor with a consistent read and emits
/// the correct cutoff, so the lost pass skips expiry entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The `UpdateItem` landed: `serving_counter` advanced to
    /// `decision.next_serving_counter`, and the carried-forward smoothing
    /// state is persisted. The decision's `next_serving_counter` is the real
    /// cursor and is safe to derive an expiry cutoff from.
    Advanced,
    /// Another invoke advanced the cursor first; this pass's release is stale
    /// and did not persist. `decision.next_serving_counter` is **not** the
    /// persisted cursor — deriving an expiry cutoff from it would over-expire
    /// positions still inside the grace window. Expiry must be skipped on this
    /// pass.
    LostRace,
}

impl ReleaseOutcome {
    /// Returns `true` when the guarded write lost its race and did not land.
    ///
    /// Equivalent to `*self == ReleaseOutcome::LostRace`; named for readability
    /// at the call site, where the branch's meaning is "the release did not
    /// persist," not "compare two enums."
    #[must_use]
    pub fn is_lost(self) -> bool {
        matches!(self, Self::LostRace)
    }
}

/// The current `Counters` state the controller needs before it acts.
#[derive(Debug, Clone, PartialEq)]
pub struct ControllerState {
    pub phase: Phase,
    /// The operator's stored admission override (issue #71: never `FailOpen`,
    /// which is resolved from `fail_open_until` instead). The phase says
    /// where the event is on its timeline; this says whether the operator is
    /// letting visitors through right now. Both must permit admission before
    /// the controller advances `serving_counter`.
    pub stored_control: StoredControl,
    /// Epoch-seconds fail-open deadline; `0` means no window is in force.
    pub fail_open_until: u64,
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
    ///
    /// Returns [`ReleaseOutcome::Advanced`] when the write landed and
    /// [`ReleaseOutcome::LostRace`] when a concurrent invoke advanced first and
    /// this pass's `UpdateItem` failed its `serving_counter = :expected`
    /// condition. A lost race is **not** an error: the winner persisted a
    /// consistent cursor. But the caller must not derive an expiry cutoff from
    /// the (non-persisted) `decision.next_serving_counter` on a lost race, so
    /// the outcome distinguishes the two cases that [`Ok`] used to collapse.
    fn write_release(
        &self,
        event_id: &str,
        decision: &ReleaseDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send;

    /// Returns positions below `cutoff` whose status is still `issued` — those
    /// the cursor passed more than the grace window ago and nobody claimed.
    fn query_expired(
        &self,
        cutoff_position: u64,
    ) -> impl Future<Output = Result<Vec<ExpiredPosition>, StoreError>> + Send;

    /// Marks a position `expired`, guarded on it still being `issued` so a
    /// completed/abandoned position is not overwritten.
    fn mark_expired(&self, request_id: &str)
    -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Advances `max_expired_position` on the `Counters` item to the highest
    /// position just expired (only ever forward; a lower value is a no-op at
    /// the store).
    fn advance_max_expired(
        &self,
        event_id: &str,
        max_expired_position: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The result of one controller pass.
///
/// A pass runs as a durable step, so this is checkpointed and replayed from the
/// checkpoint instead of being recomputed: it has to round-trip through serde.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PassOutcome {
    /// The event is not `Active`; the controller did nothing.
    NotActive(Phase),
    /// The operator's admission override is not `Open`; the controller did
    /// nothing. Distinct from [`PassOutcome::NotActive`]: the event is running
    /// and only the operator's hold stops admission.
    Held(AdmissionControl),
    /// The controller ran: it released `released` positions and expired
    /// `expired` positions. When the release lost its race against a concurrent
    /// invoke, both are `0`: no release persisted and expiry was skipped rather
    /// than run against a non-persisted cursor.
    Ran { released: u64, expired: usize },
}

/// Runs one controller pass for the event: read state, gate on `Active` and on
/// the operator's admission override, compute and write the release, then expire
/// due positions and advance `max_expired_position`.
///
/// If the guarded `write_release` loses its race against a concurrent invoke,
/// the release did not persist and `decision.next_serving_counter` is a
/// phantom the guard refused to land. Expiry is skipped on that pass: the
/// cutoff is derived from the persisted cursor, and a non-persisted cursor
/// would over-expire positions still inside the grace window. The next pass
/// reads the persisted cursor with a consistent read and emits the correct
/// cutoff, so the pass reports `Ran { released: 0, expired: 0 }` only when it
/// did nothing observable.
///
/// `now` (epoch seconds) is a parameter rather than read from the system
/// clock inside this function, so the controller's arithmetic — including
/// resolving `stored_control` against `fail_open_until` — stays testable with
/// time injected rather than sampled. The durable execution loop in `main.rs`
/// supplies its own timestamp on every pass, which is also what makes the
/// per-pass lapse behaviour work: a fail-open window that expires mid-minute
/// is observed on the very next pass rather than only at the next invocation.
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

    let admission_control = resolve(state.stored_control, state.fail_open_until, now);

    // Any control other than Open returns the whole pass, so neither
    // `serving_counter` nor the expiry cursor advances: a visitor cannot lose a
    // position to expiry during a hold they had no way to act through. Under
    // fail-open the waiting room is bypassed, so a release would meter nothing;
    // leaving the counter put lets a recovery resume from it.
    match admission_control {
        AdmissionControl::Open => {}
        control @ (AdmissionControl::Paused | AdmissionControl::FailOpen) => {
            tracing::info!(
                event_id,
                control = control.as_wire_str(),
                "controller held: admission is not open"
            );
            return Ok(PassOutcome::Held(control));
        }
    }

    let decision = compute_release(state.inputs, state.prev_no_show);
    let race = store
        .write_release(event_id, &decision, state.inputs.serving_counter)
        .await?;

    // The expiry cutoff is derived from `decision.next_serving_counter`, which
    // is the persisted cursor only when the release landed. On a lost race the
    // guarded `UpdateItem` did not write, so that value is a phantom the
    // condition specifically refused to land — using it for expiry would mark
    // positions `expired` whose owners were offered a place fewer than
    // `ADMISSION_GRACE_SECS` ago, and the flip is one-way. Skip the whole
    // expiry phase: the next pass reads the persisted cursor with a consistent
    // read and computes the correct cutoff, so expiries that should have run
    // this pass arrive one pass late — within the positional approximation the
    // design already accepts — and no position is wrongly expired.
    if race.is_lost() {
        tracing::info!(
            event_id,
            stale_serving_counter = state.inputs.serving_counter,
            stale_next_serving_counter = decision.next_serving_counter,
            "controller pass skipped expiry: release lost the race"
        );
        return Ok(PassOutcome::Ran {
            released: 0,
            expired: 0,
        });
    }

    let expired = expire_due(
        store,
        event_id,
        expiry_cutoff(decision.next_serving_counter, state.inputs.target_rate),
    )
    .await?;

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

/// The position below which an unclaimed position counts as a no-show: the
/// cursor less the ground it covers during the grace window.
///
/// A rate of zero releases nobody, so nothing has been offered and nothing can
/// have been declined; returning zero expires nothing rather than treating the
/// entire queue as no-shows.
#[must_use]
pub fn expiry_cutoff(serving_counter: u64, target_rate: u32) -> u64 {
    if target_rate == 0 {
        return 0;
    }
    let grace = u64::from(target_rate).saturating_mul(ADMISSION_GRACE_SECS);
    serving_counter.saturating_sub(grace)
}

/// Expires every position the cursor left behind and advances
/// `max_expired_position` to the highest one. Returns the number expired.
async fn expire_due<S: Store>(store: &S, event_id: &str, cutoff: u64) -> Result<usize, StoreError> {
    if cutoff == 0 {
        return Ok(0);
    }
    let due = store.query_expired(cutoff).await?;
    if due.is_empty() {
        return Ok(0);
    }

    for position in &due {
        store.mark_expired(&position.request_id).await?;
    }

    // The highest position actually expired, not a count of them: the attribute
    // names a position, and adding a count to it produces a number that means
    // nothing and drifts further from the truth on every pass.
    let highest = due.iter().map(|p| p.position).max().unwrap_or(0);
    store.advance_max_expired(event_id, highest).await?;

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
            // Far beyond the cursor, so the cases below exercise the release
            // computation rather than the end-of-line clamp.
            queue_counter: u64::MAX,
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
    fn a_stuck_sub_floor_rate_recovers_to_zero_in_one_pass() {
        // An event already stuck with a `no_show_rate` just above the floor
        // (the last value DynamoDB accepted, ~1.36e-130) decays below it on
        // the next zero-no-show pass. The fix floors that to 0.0 so the
        // `write_release` is accepted and the event self-heals instead of
        // re-failing forever on the re-read value.
        let prev = NoShowState {
            smoothed_rate: 1.36e-130,
        };
        // Released 500, all 500 arrived -> observed_no_show 0 -> 0.7 * prev.
        let d = compute_release(inputs(500, 0, 1000, 500, 50), Some(prev));
        // 0.7 * 1.36e-130 ~= 9.5e-131, below the floor -> floored to 0.0.
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "sub-floor decay must be floored to exactly +0.0, got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(d.no_show.smoothed_rate.to_string(), "0");
        // The release correction is the target either way (1 / (1 - 0) == 1),
        // so flooring does not change the cursor advance.
        assert_eq!(d.release, 500);
    }

    #[test]
    fn a_rate_above_the_dynamodb_floor_is_not_floored() {
        // A smoothed rate that stays above the floor must be carried through
        // unchanged, so the fix does not perturb normal operation.
        let prev = NoShowState {
            smoothed_rate: 2e-130,
        };
        let d = compute_release(inputs(500, 0, 1000, 500, 50), Some(prev));
        // 0.7 * 2e-130 = 1.4e-130, still above 1e-130 -> preserved.
        assert!(
            d.no_show.smoothed_rate > DDB_NUMBER_MIN_POSITIVE,
            "an above-floor rate must not be collapsed, got {:e}",
            d.no_show.smoothed_rate
        );
        let expected = 1.4e-130;
        assert!(
            (d.no_show.smoothed_rate - expected).abs() < expected * 1e-6,
            "above-floor rate should be {expected:e}, got {:e}",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn the_carried_forward_rate_is_also_floored() {
        // When nothing was released last interval the smoothed state is held
        // unchanged — but a (synthetic or recover-from-stuck) sub-floor value
        // must still not be carried into a persist. Both branches floor, so
        // the DynamoDB-storable invariant holds for every input.
        let prev = NoShowState {
            smoothed_rate: 5e-131,
        };
        // released_last = 0 -> hold-the-state branch carries `prev` forward.
        let d = compute_release(inputs(0, 0, 1000, 1000, 50), Some(prev));
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "carried sub-floor rate must be floored to exactly +0.0, got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(d.no_show.smoothed_rate.to_string(), "0");
    }

    #[test]
    fn a_rate_exactly_at_the_dynamodb_floor_is_preserved() {
        // The floor is exclusive: exactly 1e-130 — the smallest positive
        // DynamoDB does accept — must be carried through unchanged, not
        // collapsed to 0.0. This pins the boundary so a regression that widens
        // the floor to `<=` (folding the storable boundary value away) fails.
        let prev = NoShowState {
            smoothed_rate: DDB_NUMBER_MIN_POSITIVE,
        };
        // released_last = 0 -> hold-the-state branch carries `prev` forward.
        let d = compute_release(inputs(0, 0, 1000, 1000, 50), Some(prev));
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            DDB_NUMBER_MIN_POSITIVE.to_bits(),
            "the exact floor (1e-130) is DynamoDB-storable and must be preserved, got {:e}",
            d.no_show.smoothed_rate
        );
        // The persisted string is the fixed-decimal form of 1e-130 ("0." +
        // 129 zeros + "1"); parse it back to confirm it round-trips to the
        // exact storable magnitude rather than pinning the zero count.
        let parsed: f64 = d.no_show.smoothed_rate.to_string().parse().unwrap();
        assert_eq!(
            parsed.to_bits(),
            DDB_NUMBER_MIN_POSITIVE.to_bits(),
            "persisted boundary string must round-trip to exactly 1e-130"
        );
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
    fn the_cursor_stops_at_the_end_of_the_line() {
        // 8 positions issued, cursor at 0, target release 500. The cursor may
        // reach one past the last position and no further: it is exclusive, so
        // 9 admits position 8 while admitting nothing that does not exist.
        let mut i = inputs(0, 0, 0, 0, 50);
        i.queue_counter = 8;
        let d = compute_release(i, None);
        assert_eq!(d.next_serving_counter, 9);
        assert_eq!(d.release, 9, "release must report the distance moved");
    }

    #[test]
    fn an_empty_queue_banks_no_admission_credit() {
        // Nobody has ever joined. Left unclamped the cursor climbs every
        // interval, and the next arrivals are admitted instantly because every
        // position below the cursor is already released.
        let mut i = inputs(0, 0, 0, 0, 50);
        i.queue_counter = 0;
        let d = compute_release(i, None);
        assert_eq!(d.next_serving_counter, 1);

        // Repeating the pass does not accumulate.
        let mut i = inputs(0, 0, 1, 1, 50);
        i.queue_counter = 0;
        let d = compute_release(i, None);
        assert_eq!(d.next_serving_counter, 1);
        assert_eq!(d.release, 0);
    }

    #[test]
    fn the_empty_queue_cursor_move_is_not_measured_as_a_no_show() {
        // The pass after `an_empty_queue_banks_no_admission_credit`: the clamp
        // left the cursor at 1, so `serving_counter - last_serving_counter` is
        // 1 — but position 0 is never issued, so nobody was released and there
        // is nothing to measure. Measuring it against zero arrivals reads as a
        // 100% no-show and pins the smoothed rate at EWMA_ALPHA.
        let mut i = inputs(0, 0, 1, 0, 50);
        i.queue_counter = 0;
        let d = compute_release(i, None);
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "a cursor move onto the empty-queue ceiling released nobody to measure, got {:e}",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn the_empty_queue_cursor_move_is_not_measured_once_the_queue_fills() {
        // The same phantom, measured on the pass where the first joiners have
        // landed: `queue_counter` is no longer 0, so anything keying off an
        // empty queue at measurement time misses it. What makes the release
        // phantom is which positions the cursor passed, not how long the queue
        // is by the time the next pass reads it.
        let mut i = inputs(0, 0, 1, 0, 50);
        i.queue_counter = 100_000;
        let d = compute_release(i, None);
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "a queue that filled after the phantom does not make it measurable, got {:e}",
            d.no_show.smoothed_rate
        );
        // With nothing measured the release is the operator's raw target, not
        // the 1 / (1 - 0.3) correction the phantom would have justified.
        assert_eq!(d.release, target_release_per_interval(50));
    }

    #[test]
    fn an_empty_period_does_not_over_release_when_traffic_begins() {
        // The whole arc from the bug report. An Active event sits empty, the
        // clamp parks the cursor at 1, and the operator's rate is 50/s (500 an
        // interval). When a cohort finally joins and every released visitor
        // arrives, the controller must meter at the target — not carry a
        // smoothed no-show rate banked from measuring the phantom.
        let mut empty = inputs(0, 0, 0, 0, 50);
        empty.queue_counter = 0;
        let parked = compute_release(empty, None);
        assert_eq!(parked.next_serving_counter, 1);

        // The measuring pass over the phantom: still empty, nothing learned.
        let mut measuring = inputs(0, 0, 1, 0, 50);
        measuring.queue_counter = 0;
        let measured = compute_release(measuring, Some(parked.no_show));

        // Traffic arrives and the cursor does real work: 500 released, all 500
        // arrived, so the observed no-show is genuinely 0.
        let first_real = compute_release(inputs(500, 0, 501, 1, 50), Some(measured.no_show));
        assert_eq!(
            first_real.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "the empty period banked no no-show rate to carry in, got {:e}",
            first_real.no_show.smoothed_rate
        );
        assert_eq!(
            first_real.release,
            target_release_per_interval(50),
            "no no-shows to compensate for: the release is the operator's target"
        );
    }

    #[test]
    fn the_first_real_release_counts_issued_positions_only() {
        // A cursor advancing 0 -> 501 passes positions 0..500, but position 0
        // was never issued, so 500 people were released. Counting 501 would
        // read 500 arrivals as a no-show that did not happen.
        let d = compute_release(inputs(500, 0, 501, 0, 50), None);
        assert_eq!(
            d.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "every released visitor arrived; position 0 is not one of them, got {:e}",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn a_queue_counter_behind_the_cursor_never_rewinds_admission() {
        // A stale or eventually-consistent read must not un-admit anyone.
        let mut i = inputs(0, 0, 5_000, 5_000, 50);
        i.queue_counter = 10;
        let d = compute_release(i, None);
        assert_eq!(d.next_serving_counter, 5_000);
        assert_eq!(d.release, 0);
    }

    #[test]
    fn a_release_is_measurable_by_the_interval_that_follows_it() {
        // The closed loop only works if what one interval releases is visible
        // to the next. Persisting the post-release cursor as
        // last_serving_counter makes every measurement zero, which silently
        // disables the no-show correction entirely.
        let first = compute_release(inputs(0, 0, 1_000, 1_000, 50), None);
        assert_eq!(first.release, 500);

        // Next interval reads the counters the store just wrote.
        let mut second = inputs(0, 0, first.next_serving_counter, 0, 50);
        second.last_serving_counter = first.previous_serving_counter;
        second.arrivals[0] = 250; // half of them showed up

        let d = compute_release(second, None);
        assert!(
            (d.no_show.smoothed_rate - 0.5).abs() < 1e-9,
            "measured no-show {} — the previous release was invisible",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn nothing_expires_until_the_grace_window_has_closed() {
        // A position is only a no-show once it was offered and declined. Early
        // in an event the cursor has not covered the grace window, so no
        // position is old enough to expire — expiring on a join-time deadline
        // instead throws people out for waiting the length of the queue.
        assert_eq!(expiry_cutoff(0, 50), 0);
        assert_eq!(expiry_cutoff(5_999, 50), 0);
        assert_eq!(expiry_cutoff(6_001, 50), 1);
    }

    #[test]
    fn a_zero_rate_expires_nobody() {
        // Releasing nobody means offering nobody, so nobody can have declined.
        // A cutoff at the cursor would expire the entire released queue.
        assert_eq!(expiry_cutoff(100_000, 0), 0);
    }

    #[test]
    fn the_grace_window_is_the_same_duration_at_any_rate() {
        // Positional grace has to track the rate, or a fast event expires
        // people seconds after offering them a place.
        let slow = 10_000 - expiry_cutoff(10_000, 5);
        let fast = 100_000 - expiry_cutoff(100_000, 50);
        assert_eq!(slow, 5 * ADMISSION_GRACE_SECS);
        assert_eq!(fast, 50 * ADMISSION_GRACE_SECS);
        assert_eq!(fast, slow * 10);
    }

    #[tokio::test]
    async fn a_paused_event_expires_nobody_while_it_is_held() {
        // The cursor does not move while paused, so the window behind it does
        // not either: a hold cannot cost anyone their place.
        let mut state = active_state(0);
        state.stored_control = StoredControl::Paused;
        let store = FakeStore::new(
            state,
            vec![ExpiredPosition {
                request_id: "r1".to_owned(),
                position: 3,
            }],
        );
        run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert!(store.marked.lock().unwrap().is_empty());
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
        /// When true, `write_release` reports a lost race without recording —
        /// mirroring `DynamoStore` returning `LostRace` on
        /// `ConditionalCheckFailedException`.
        lose_release: bool,
    }

    impl FakeStore {
        fn new(state: ControllerState, due: Vec<ExpiredPosition>) -> Self {
            Self {
                state,
                due,
                marked: Mutex::new(Vec::new()),
                released: Mutex::new(None),
                advanced: Mutex::new(None),
                lose_release: false,
            }
        }

        /// Returns a fake whose `write_release` loses the race on every call,
        /// mirroring a real store hitting `ConditionalCheckFailedException`
        /// after a concurrent invoke advanced `serving_counter` first.
        fn losing_race(state: ControllerState, due: Vec<ExpiredPosition>) -> Self {
            let mut s = Self::new(state, due);
            s.lose_release = true;
            s
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
        ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send {
            if self.lose_release {
                // Mirrors `DynamoStore` returning `LostRace` on
                // `ConditionalCheckFailedException`: the release did not
                // persist, so `decision.next_serving_counter` is a phantom the
                // guard refused and `run_pass` must skip `expire_due`.
                std::future::ready(Ok(ReleaseOutcome::LostRace))
            } else {
                *self.released.lock().unwrap() = Some(*decision);
                std::future::ready(Ok(ReleaseOutcome::Advanced))
            }
        }

        fn query_expired(
            &self,
            cutoff_position: u64,
        ) -> impl Future<Output = Result<Vec<ExpiredPosition>, StoreError>> + Send {
            // Mirrors the store's filter, so a test that changes the cutoff sees
            // the same rows the real scan would return.
            let due: Vec<ExpiredPosition> = self
                .due
                .iter()
                .filter(|p| p.position < cutoff_position)
                .cloned()
                .collect();
            std::future::ready(Ok(due))
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
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            // The cursor is far enough along that the grace window has closed
            // behind it: at 50/s over 120s it covers 6000 positions, so nothing
            // expires until it is past that. Early in an event nothing has been
            // offered long enough ago to count as declined.
            inputs: {
                let mut i = inputs(250, 0, 20_000, 19_500, 50);
                i.queue_counter = u64::MAX;
                i
            },
            prev_no_show: None,
            max_expired_position: due_max_expired,
        }
    }

    #[tokio::test]
    async fn pass_releases_and_expires_when_active() {
        let due = vec![
            ExpiredPosition {
                request_id: "r1".to_owned(),
                position: 3,
            },
            ExpiredPosition {
                request_id: "r2".to_owned(),
                position: 7,
            },
        ];
        let store = FakeStore::new(active_state(10), due);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 1000,
                expired: 2,
            }
        );
        assert_eq!(store.marked.lock().unwrap().len(), 2);
        // The highest position expired, not a count of them.
        assert_eq!(*store.advanced.lock().unwrap(), Some(7));
        assert!(store.released.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn pass_is_noop_when_not_active() {
        let mut state = active_state(0);
        state.phase = Phase::PreQueue;
        let store = FakeStore::new(state, Vec::new());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::NotActive(Phase::PreQueue));
        assert!(store.released.lock().unwrap().is_none());
        assert!(store.advanced.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn pause_stops_admission_entirely() {
        // A paused event is still Active, so the phase gate lets the pass
        // through and only the admission control stops it. Nothing may advance:
        // not serving_counter, not the expiry cursor.
        let mut state = active_state(10);
        state.stored_control = StoredControl::Paused;
        let due = vec![ExpiredPosition {
            request_id: "r1".to_owned(),
            position: 3,
        }];
        let store = FakeStore::new(state, due);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::Paused));
        assert!(
            store.released.lock().unwrap().is_none(),
            "paused event released positions: pause is not holding admission"
        );
        assert!(
            store.marked.lock().unwrap().is_empty(),
            "paused event expired a position the visitor could not act on"
        );
        assert_eq!(*store.advanced.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn fail_open_holds_the_controller_too() {
        // Under fail-open the waiting room is bypassed, so metering releases
        // nothing real; the counter stays put for recovery to resume from.
        let mut state = active_state(0);
        state.fail_open_until = 1000;
        let store = FakeStore::new(state, Vec::new());
        let outcome = run_pass(&store, "evt", 500).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::FailOpen));
        assert!(store.released.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_lapsed_fail_open_window_is_observed_on_the_very_next_pass() {
        // now supplied per pass (not read from a clock inside run_pass) is
        // what makes this work: the same stored state, evaluated a moment
        // after the epoch, resolves to Open with no write on either side.
        let mut state = active_state(0);
        state.fail_open_until = 1000;
        let store = FakeStore::new(state, Vec::new());
        let held = run_pass(&store, "evt", 500).await.unwrap();
        assert_eq!(held, PassOutcome::Held(AdmissionControl::FailOpen));
        let ran = run_pass(&store, "evt", 1000).await.unwrap();
        assert!(matches!(ran, PassOutcome::Ran { .. }));
    }

    #[tokio::test]
    async fn resuming_lets_the_controller_run_again() {
        // The same state with the control back to Open runs a full pass, so a
        // hold costs nothing but the intervals it covered.
        let store = FakeStore::new(active_state(0), Vec::new());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 1000,
                expired: 0,
            }
        );
    }

    #[tokio::test]
    async fn no_due_positions_skips_the_cursor_write() {
        let store = FakeStore::new(active_state(5), Vec::new());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 1000,
                expired: 0,
            }
        );
        assert_eq!(*store.advanced.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn a_stuck_sub_floor_event_recovers_in_one_pass() {
        // End-to-end: an event whose persisted `no_show_rate` sits just above
        // the DynamoDB floor (the stuck value) reads it back, recomputes a
        // sub-floor EWMA on a zero-no-show interval, and must persist the
        // floored 0.0 — the pass succeeds and `serving_counter` advances,
        // rather than the pass failing (or, on the live store, the
        // `UpdateItem` being rejected) every minute until the next no-show.
        let mut state = active_state(0);
        // Released 500 last interval, all 500 arrived -> observed_no_show 0,
        // so the only thing pulling the EWMA is the carried 1.36e-130.
        state.inputs = {
            let mut i = inputs(500, 0, 20_000, 19_500, 50);
            i.queue_counter = u64::MAX;
            i
        };
        state.prev_no_show = Some(NoShowState {
            smoothed_rate: 1.36e-130,
        });
        let store = FakeStore::new(state, Vec::new());

        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert!(
            matches!(outcome, PassOutcome::Ran { .. }),
            "a stuck event must recover, not fail the pass: {outcome:?}"
        );

        let recorded = *store.released.lock().unwrap();
        let written = recorded.unwrap();
        assert_eq!(
            written.no_show.smoothed_rate.to_bits(),
            0.0f64.to_bits(),
            "the persisted no_show_rate must be floored to exactly +0.0, got {:e}",
            written.no_show.smoothed_rate
        );
        assert_eq!(written.no_show.smoothed_rate.to_string(), "0");
    }

    // --- Lost release race: expiry must skip, not run against a phantom cursor

    /// State mirroring the bug report's realistic orientation: a staler arrivals
    /// read (200 vs the winner's 250) yields a larger bounded release, so a
    /// losing pass computes `next_serving_counter = 20_610` against the winner's
    /// persisted `20_588`. `prev_no_show = Some(0.0)` makes the EWMA produce the
    /// reported figures rather than the raw observed rate.
    fn stale_loser_state(due_max_expired: u64) -> ControllerState {
        let mut state = active_state(due_max_expired);
        // Arrivals 200 (stale) vs the winner's 250 (fresh); the loser reads
        // fewer arrivals -> higher no-show -> larger release (610 vs 588).
        state.inputs.arrivals[0] = 200;
        state.prev_no_show = Some(NoShowState { smoothed_rate: 0.0 });
        state
    }

    #[tokio::test]
    async fn lost_release_race_skips_expiry_so_grace_window_positions_survive() {
        // The harmful orientation: the staler/larger-release invoke loses the
        // guarded write. Its `decision.next_serving_counter = 20_610` was never
        // persisted (the winner persisted 20_588), so the cutoff derived from it
        // (14_610) is ahead of the real cursor's cutoff (14_588). Running
        // `expire_due` against 14_610 would mark positions 14_588..14_609
        // `expired` — still inside the 120s grace window — and the flip is
        // one-way, so those visitors are permanently denied.
        //
        // The fix: on a lost race `run_pass` skips `expire_due` entirely. The
        // next pass reads the persisted cursor with a consistent read and emits
        // the correct cutoff, so expiries that should run this pass arrive one
        // pass late — within the positional ±one-pass tolerance the design
        // accepts — and no position is wrongly expired.
        let due = vec![
            ExpiredPosition {
                request_id: "r_correct".to_owned(),
                position: 14_500,
            },
            ExpiredPosition {
                request_id: "r_in_window_a".to_owned(),
                position: 14_589,
            },
            ExpiredPosition {
                request_id: "r_in_window_b".to_owned(),
                position: 14_600,
            },
            ExpiredPosition {
                request_id: "r_in_window_c".to_owned(),
                position: 14_609,
            },
        ];
        let store = FakeStore::losing_race(stale_loser_state(10), due);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 0,
                expired: 0,
            },
            "a lost-race pass must report it released and expired nothing"
        );
        assert!(
            store.released.lock().unwrap().is_none(),
            "the release did not persist; FakeStore must not record it"
        );
        assert!(
            store.marked.lock().unwrap().is_empty(),
            "lost-race pass expired positions whose owners were offered a place \
             fewer than 120s ago — the grace-window over-expiry"
        );
        assert_eq!(
            *store.advanced.lock().unwrap(),
            None,
            "max_expired_position must not advance when no positions were expired"
        );
    }

    #[tokio::test]
    async fn lost_release_race_then_a_won_pass_expires_the_correct_positions() {
        // The skip is a one-pass delay, not a permanent loss of the expiry: the
        // next pass reads the persisted cursor and emits the correct cutoff.
        // Here the same fake loses the first pass, then a second fake bound to
        // the winner's persisted state (`serving_counter = 20_588`) wins its
        // release and expires the truly-due position, demonstrating the system
        // self-corrects within one pass — the tolerance the positional design
        // already accepts.
        let due = vec![
            ExpiredPosition {
                request_id: "r_correct".to_owned(),
                position: 14_500,
            },
            ExpiredPosition {
                request_id: "r_in_window".to_owned(),
                position: 14_589,
            },
        ];
        // First pass: loses the race; expiry skipped, nothing marked.
        let loser = FakeStore::losing_race(stale_loser_state(10), due.clone());
        let first = run_pass(&loser, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            first,
            PassOutcome::Ran {
                released: 0,
                expired: 0,
            }
        );
        assert!(loser.released.lock().unwrap().is_none());
        assert!(loser.marked.lock().unwrap().is_empty());

        // Second pass: reads the winner's persisted cursor (20_588). With
        // `last_serving_counter = 20_588` (released_last = 0), the no-show
        // correction falls back to the raw target (500) and carries the prior
        // smoothed state, so `release = 500`, `next_serving_counter = 21_088`,
        // and `expiry_cutoff(21_088, 50) = 15_088`. Both due positions (14_500,
        // 14_589) are below 15_088 and correctly expire — the lost pass skipped
        // them, and the won pass picked them up one pass later.
        let mut won_state = active_state(10);
        won_state.inputs.serving_counter = 20_588;
        won_state.inputs.last_serving_counter = 20_588;
        won_state.prev_no_show = Some(NoShowState {
            smoothed_rate: 0.15,
        });
        let winner = FakeStore::new(won_state, due);
        let second = run_pass(&winner, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            second,
            PassOutcome::Ran {
                released: 500,
                expired: 2,
            }
        );
        assert!(winner.released.lock().unwrap().is_some());
        assert_eq!(winner.marked.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_won_release_race_expires_positions_below_the_persisted_cutoff() {
        // Regression guard for the non-race path: when the release lands
        // (`Advanced`), `run_pass` must still derive the cutoff from the just-
        // persisted `next_serving_counter` and expire positions below it. The
        // lost-race skip must not swallow the normal expiry.
        //
        // Loser's inputs but the release *lands*: `next_serving_counter =
        // 20_610` is now the real cursor, so `expiry_cutoff(20_610, 50) =
        // 14_610` is the correct cutoff and all four due positions (below
        // 14_610) are correctly expired.
        let due = vec![
            ExpiredPosition {
                request_id: "r1".to_owned(),
                position: 14_500,
            },
            ExpiredPosition {
                request_id: "r2".to_owned(),
                position: 14_589,
            },
            ExpiredPosition {
                request_id: "r3".to_owned(),
                position: 14_600,
            },
            ExpiredPosition {
                request_id: "r4".to_owned(),
                position: 14_609,
            },
        ];
        let store = FakeStore::new(stale_loser_state(10), due);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(
            outcome,
            PassOutcome::Ran {
                released: 610,
                expired: 4,
            }
        );
        assert!(store.released.lock().unwrap().is_some());
        assert_eq!(store.marked.lock().unwrap().len(), 4);
        assert_eq!(*store.advanced.lock().unwrap(), Some(14_609));
    }

    // --- Property tests: no input underflows/overflows or panics -------------

    use proptest::prelude::{
        Just, Strategy, any, prop_assert, prop_assert_eq, prop_oneof, proptest,
    };

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
            queue in any::<u64>(),
            rate in 1u32..=100_000,
            prev_rate in prop_oneof![
                Just(None),
                (0.0f64..1.0).prop_map(|r| Some(NoShowState { smoothed_rate: r })),
                // Sub-floor and exact-floor carried state so the
                // DynamoDB-storable invariant is exercised on both sides of the
                // bound; the random [0,1) tail almost never lands at or below
                // 1e-130.
                Just(Some(NoShowState { smoothed_rate: 5e-131 })),
                Just(Some(NoShowState { smoothed_rate: 1e-130 })),
            ],
        ) {
            let d = compute_release(
                ReleaseInputs {
                    arrivals,
                    last_arrivals_total: last_arrivals,
                    last_serving_counter: last_serving,
                    serving_counter: serving,
                    queue_counter: queue,
                    target_rate: rate,
                },
                prev_rate,
            );
            // release is bounded by the cap (2x target per interval), a finite u64.
            let cap = target_release_per_interval(rate).saturating_mul(2);
            prop_assert!(d.release <= cap.saturating_add(1));
            // The cursor is monotonic for every input, including a queue_counter
            // that reads behind it.
            prop_assert!(d.next_serving_counter >= serving);
            // It never runs past the end of the line, so admission credit
            // cannot accumulate against a queue nobody is in.
            prop_assert!(
                d.next_serving_counter <= queue.saturating_add(1) || d.next_serving_counter == serving
            );
            // The reported release is exactly the distance moved.
            prop_assert_eq!(d.release, d.next_serving_counter - serving);
            prop_assert_eq!(d.previous_serving_counter, serving);
            // smoothed rate stays a valid probability.
            prop_assert!(d.no_show.smoothed_rate >= 0.0 && d.no_show.smoothed_rate <= 1.0);
            prop_assert!(d.no_show.smoothed_rate.is_finite());
            // The persisted smoothed rate is always DynamoDB-storable: exactly
            // 0.0 or at least the positive `Number` minimum, so the
            // `write_release` UpdateItem is never rejected for underflow —
            // both the measured-EWMA branch and the hold-the-state branch floor.
            prop_assert!(
                d.no_show.smoothed_rate == 0.0
                    || d.no_show.smoothed_rate >= DDB_NUMBER_MIN_POSITIVE,
                "smoothed rate {:e} is below the DynamoDB positive Number floor",
                d.no_show.smoothed_rate
            );
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
