//! Closed-loop outflow controller.
//!
//! Each interval the controller runs one pass over an event's `Counters` item:
//! it measures the arrival rate against what it released last interval, derives
//! the no-show rate, smooths it, and advances `serving_counter` by a bounded
//! correction so the origin runs at the operator's target rate despite visitors
//! who never click through.
//!
//! It expires nothing. A position lives until `DynamoDB` TTL reclaims its row,
//! and the no-show correction is what compensates for people who never arrive
//! ([`ADR-0031`](../../../docs/adr/0031-remove-controller-driven-expiry.md)).
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

/// A prior open pass older than this is treated as stale for the no-show
/// measurement, and the pass falls back to releasing the raw `target`.
///
/// The controller seeds the no-show EWMA from
/// `observed_arrivals / released_last` on every open pass that released
/// anyone last interval. That fraction is biased by *anything* that
/// suppresses arrivals without suppressing releases — most commonly an
/// operator's `Paused`/`FailOpen` hold, during which `decide` returns
/// `NotAdmitting` for everyone (already-released visitors included), so no
/// `record_arrival` runs and `last_open_pass_at` freezes while
/// `serving_counter` sits still. When the resume pass arrives, this
/// threshold fires instead of measuring the operator's hold as a 100%
/// no-show; the EWMA is held and the cadence resumes measuring on the next
/// interval, when the released visitors actually have time to click through.
///
/// Persisted as the `last_open_pass_at` attribute on the `Counters` item by
/// [`dynamo::DynamoStore::write_release`], the **only** writer, so the held
/// path stays a pure read. Defaults to `0` (absent) on a fresh event: the
/// first post-deploy pass reads `now - last_open_pass_at = now ≫ STALE`,
/// falls back, and releases exactly `target` rather than seeding the EWMA
/// off `0 / 0`.
///
/// The threshold is two intervals rather than `> INTERVAL_SECS` so the small
/// jitter in the fixed cadence (the [`INTERVAL_SECS`] durable wait plus a
/// Lambda invoke) does not misfire on a normal pass: a regular `now - last
/// ⟶ INTERVAL_SECS` is comfortably below, while a pause whose elapsed time is
/// at least one closed interval past the prior pass is comfortably at or
/// above. A lost race advances `last_open_pass_at` on the winning invoke at
/// the same `now`, so the next pass measures it as a normal cadence, not a
/// stale-measurement; the loser's `write_release` does not land, so it
/// leaves the attribute untouched.
pub const STALE_OPEN_PASS_SECS: u64 = 2 * INTERVAL_SECS;

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
    /// after an open it starts at the cohort size, so this is the end of the
    /// line however the positions were assigned.
    pub queue_counter: u64,
    /// Operator target rate in visitors per second (the `/admin/rate` value).
    pub target_rate: u32,
    /// Epoch-seconds timestamp of the last successful *open* pass (the last
    /// [`dynamo::DynamoStore::write_release`] that landed). Used by
    /// [`compute_release`] to detect a prior open pass that is more than
    /// [`STALE_OPEN_PASS_SECS`] in the past — the signature of a `Paused` or
    /// `FailOpen` hold that froze the measurement baselines while `decide`
    /// returned `NotAdmitting` for everyone, including already-released
    /// visitors. Defaults to `0` for a fresh event that has never run an open
    /// pass: `now - 0 ≫ STALE` falls back to the raw `target`, which is benign
    /// for first launch and means the cold-start EWMA is never seeded off
    /// `0/0`.
    pub last_open_pass_at: u64,
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
    /// Epoch-seconds at which this open pass ran, persisted as
    /// `last_open_pass_at` for the next interval's staleness check. Set to
    /// `now` regardless of whether the pass took a no-show measurement or
    /// fell back to the raw `target` for a stale prior pass: this pass *is*
    /// the latest open pass, and the next interval must measure against
    /// exactly one interval elapsed.
    pub last_open_pass_at: u64,
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
/// The same fall-back fires when the prior open pass is stale — when
/// `now - inputs.last_open_pass_at` is at least [`STALE_OPEN_PASS_SECS`] —
/// because in that case the depressed `observed_arrivals` reflects the hold
/// (or the scheduling gap) that froze the baselines, not genuine visitor
/// no-shows. `compute_release` itself is a pure function of `now` — the
/// cadence matters to the measurement, so it is a parameter rather than
/// read from a clock — and `decision.last_open_pass_at` is set to `now` so
/// the next interval sees exactly `INTERVAL_SECS` of elapsed open-pass time.
///
/// Every subtraction is saturating: `arrivals` and `serving_counter` are read
/// from separate updates and can momentarily read lower than the stored
/// baseline (an eventually-consistent read, or a manual counter edit), which a
/// bare subtraction would wrap under the overflow-check-free release profile.
#[must_use]
pub fn compute_release(
    inputs: ReleaseInputs,
    prev: Option<NoShowState>,
    now: u64,
) -> ReleaseDecision {
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
    //
    let released_last = inputs
        .serving_counter
        .saturating_sub(inputs.last_serving_counter.max(1));

    let target = target_release_per_interval(inputs.target_rate);

    // The prior open pass is *fresh* exactly when it was the immediately
    // preceding interval — the cadence's `now - last == INTERVAL_SECS` sits
    // comfortably below [`STALE_OPEN_PASS_SECS`], so the small jitter in the
    // fixed wait never misfires. Anything further in the past froze the
    // baselines: the operator's `Paused`/`FailOpen` held the cursor while
    // `decide` returned `NotAdmitting` for everyone (already-released
    // visitors included), so no `record_arrival` ran. Measuring that gap
    // against a nonzero `released_last` would seed an EWMA spike off the
    // operator's hold itself, not off visitor no-shows, and over-release on
    // the resume pass. Treat the interval as unmeasurable — release the raw
    // target and hold the EWMA — exactly as the `released_last == 0` branch
    // already does. `last_open_pass_at = 0` (a fresh event that has never
    // had an open pass) reads as `now ≫ STALE` and falls through here too,
    // which is exactly what cold start wants: release `target`, seed
    // nothing off `0/0`.
    let prior_open_pass_is_stale =
        now.saturating_sub(inputs.last_open_pass_at) >= STALE_OPEN_PASS_SECS;

    let (release, smoothed) = if released_last == 0 || prior_open_pass_is_stale {
        // No release last interval, or the prior open pass is stale: nothing
        // to measure, so hold the target and keep the prior smoothed state.
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

    // One position is one person, so the cursor moves by the release itself.
    let positions = release;

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
        .saturating_add(positions)
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
        last_open_pass_at: now,
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

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("controller store error: {0}")]
pub struct StoreError(pub String);

/// The outcome of a guarded [`Store::write_release`]: either the release
/// landed, or a concurrent invoke advanced the cursor first and this pass's
/// release was a no-op.
///
/// The distinction is what the pass reports. `decision.next_serving_counter`
/// reflects the persisted cursor only when the write landed; on
/// [`ReleaseOutcome::LostRace`] it is a value the guard specifically refused,
/// and the winner released those people. Collapsing the two into `Ok` would
/// have the losing pass claim a release it did not make, double-counting the
/// winner's in the logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The `UpdateItem` landed: `serving_counter` advanced to
    /// `decision.next_serving_counter`, and the carried-forward smoothing
    /// state is persisted. The decision's `next_serving_counter` is the real
    /// cursor, and this pass released `decision.release` people.
    Advanced,
    /// Another invoke advanced the cursor first; this pass's release is stale
    /// and did not persist. `decision.next_serving_counter` is **not** the
    /// persisted cursor, and this pass released nobody.
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
    /// consistent cursor. The outcome distinguishes the two cases that [`Ok`]
    /// used to collapse, so a losing pass reports releasing nobody rather than
    /// claiming the release the winner made.
    fn write_release(
        &self,
        event_id: &str,
        decision: &ReleaseDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send;
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
    /// The controller ran and released `released` positions. `0` when the
    /// release lost its race against a concurrent invoke: the winner persisted
    /// the cursor, so this pass released nobody.
    Ran { released: u64 },
}

/// Runs one controller pass for the event: read state, gate on `Active` and on
/// the operator's admission override, then compute and write the release.
///
/// If the guarded `write_release` loses its race against a concurrent invoke,
/// the release did not persist: the winner advanced the cursor, and this pass
/// released nobody. It reports `Ran { released: 0 }` rather than claiming the
/// release its own `UpdateItem` was refused.
///
/// `now` (epoch seconds) is a parameter rather than read from the system
/// clock inside this function, so the controller's arithmetic — including
/// resolving `stored_control` against `fail_open_until` and the
/// [`compute_release`] staleness check that suppresses the no-show
/// measurement after a hold froze the baselines — stays testable with time
/// injected rather than sampled. The durable execution loop in `main.rs`
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

    // Any control other than Open returns the whole pass, so `serving_counter`
    // does not advance: a hold means nobody is admitted, not that admissions
    // accrue silently. Under fail-open the waiting room is bypassed, so a
    // release would meter nothing; leaving the counter put lets a recovery
    // resume from it.
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

    let decision = compute_release(state.inputs, state.prev_no_show, now);
    let race = store
        .write_release(event_id, &decision, state.inputs.serving_counter)
        .await?;

    // A lost race is not an error: the winner persisted a consistent cursor.
    // But this pass released nobody, so it reports zero rather than claiming
    // the release the guarded `UpdateItem` specifically refused to land.
    if race.is_lost() {
        tracing::info!(
            event_id,
            stale_serving_counter = state.inputs.serving_counter,
            stale_next_serving_counter = decision.next_serving_counter,
            "controller pass released nothing: lost the race"
        );
        return Ok(PassOutcome::Ran { released: 0 });
    }

    tracing::info!(
        event_id,
        released = decision.release,
        serving_counter = decision.next_serving_counter,
        no_show_rate = decision.no_show.smoothed_rate,
        "controller pass"
    );
    Ok(PassOutcome::Ran {
        released: decision.release,
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure or asserts the unexpected via `panic!`"
    )]

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
            // `0` keeps the existing direct `compute_release(_, _, 0)` tests
            // out of the staleness path: `now - last_open_pass_at = 0` is
            // well below `STALE_OPEN_PASS_SECS`. The `run_pass` tests that
            // need to measure — `active_state` / the stuck-floor test — set
            // this to their pass's `now` so the cadence is fresh.
            last_open_pass_at: 0,
        }
    }

    #[test]
    fn no_release_last_interval_falls_back_to_target() {
        // released_last = serving - last_serving = 0.
        let d = compute_release(inputs(0, 0, 100, 100, 50), None, 0);
        assert_eq!(d.release, 500); // 50/s * 10s
        assert_eq!(d.next_serving_counter, 600);
        assert!(d.no_show.smoothed_rate.abs() < 1e-9);
    }

    #[test]
    fn full_arrival_yields_target_release() {
        // Released 500 last interval, all 500 arrived -> no-show 0 -> release target.
        let d = compute_release(inputs(500, 0, 1000, 500, 50), None, 0);
        assert_eq!(d.release, 500);
    }

    #[test]
    fn half_no_show_doubles_but_is_capped() {
        // Released 500, only 250 arrived -> no-show 0.5 -> target/0.5 = 1000 = 2x cap.
        let d = compute_release(inputs(250, 0, 1000, 500, 50), None, 0);
        assert!((d.no_show.smoothed_rate - 0.5).abs() < 1e-9);
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn near_total_no_show_is_bounded_at_cap() {
        // Released 500, 1 arrived -> no-show ~0.998 -> target/denom huge, capped at 2x.
        let d = compute_release(inputs(1, 0, 1000, 500, 50), None, 0);
        assert_eq!(d.release, 1000); // 2 * 500 cap, never a burst
    }

    #[test]
    fn zero_arrivals_is_bounded_at_cap_not_infinite() {
        // no-show exactly 1.0 -> denom 0 -> cap, not a division by zero.
        let d = compute_release(inputs(0, 0, 1000, 500, 50), None, 0);
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn ewma_smooths_across_intervals() {
        // Prior smoothed 0.0, observe 0.5: smoothed = 0.3*0.5 + 0.7*0.0 = 0.15.
        let prev = NoShowState { smoothed_rate: 0.0 };
        let d = compute_release(inputs(250, 0, 1000, 500, 50), Some(prev), 0);
        assert!((d.no_show.smoothed_rate - 0.15).abs() < 1e-9);
        // release = 500 / (1 - 0.15) = 588.24 -> 588.
        assert_eq!(d.release, 588);
    }

    #[test]
    fn more_arrivals_than_released_reads_zero_no_show() {
        // Straggler race: 600 arrived against 500 released -> clamp to 0 no-show.
        let d = compute_release(inputs(600, 0, 1000, 500, 50), None, 0);
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
        let d = compute_release(inputs(500, 0, 1000, 500, 50), Some(prev), 0);
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
        let d = compute_release(inputs(500, 0, 1000, 500, 50), Some(prev), 0);
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
        let d = compute_release(inputs(0, 0, 1000, 1000, 50), Some(prev), 0);
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
        let d = compute_release(inputs(0, 0, 1000, 1000, 50), Some(prev), 0);
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
        let d = compute_release(inputs(100, 500, 1000, 500, 50), None, 0);
        // observed saturates to 0; released 500 -> no-show 1.0 -> capped.
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn serving_reading_below_baseline_does_not_underflow() {
        // serving_counter < last_serving_counter: released saturates to 0 -> target.
        let d = compute_release(inputs(0, 0, 400, 500, 50), None, 0);
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
        let d = compute_release(i, None, 0);
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
        let d = compute_release(i, None, 0);
        assert_eq!(d.next_serving_counter, 1);

        // Repeating the pass does not accumulate.
        let mut i = inputs(0, 0, 1, 1, 50);
        i.queue_counter = 0;
        let d = compute_release(i, None, 0);
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
        let d = compute_release(i, None, 0);
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
        let d = compute_release(i, None, 0);
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
        let parked = compute_release(empty, None, 0);
        assert_eq!(parked.next_serving_counter, 1);

        // The measuring pass over the phantom: still empty, nothing learned.
        let mut measuring = inputs(0, 0, 1, 0, 50);
        measuring.queue_counter = 0;
        let measured = compute_release(measuring, Some(parked.no_show), 0);

        // Traffic arrives and the cursor does real work: 500 released, all 500
        // arrived, so the observed no-show is genuinely 0.
        let first_real = compute_release(inputs(500, 0, 501, 1, 50), Some(measured.no_show), 0);
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
        let d = compute_release(inputs(500, 0, 501, 0, 50), None, 0);
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
        let d = compute_release(i, None, 0);
        assert_eq!(d.next_serving_counter, 5_000);
        assert_eq!(d.release, 0);
    }

    #[test]
    fn a_release_is_measurable_by_the_interval_that_follows_it() {
        // The closed loop only works if what one interval releases is visible
        // to the next. Persisting the post-release cursor as
        // last_serving_counter makes every measurement zero, which silently
        // disables the no-show correction entirely.
        let first = compute_release(inputs(0, 0, 1_000, 1_000, 50), None, 0);
        assert_eq!(first.release, 500);

        // Next interval reads the counters the store just wrote.
        let mut second = inputs(0, 0, first.next_serving_counter, 0, 50);
        second.last_serving_counter = first.previous_serving_counter;
        second.arrivals[0] = 250; // half of them showed up

        let d = compute_release(second, None, 0);
        assert!(
            (d.no_show.smoothed_rate - 0.5).abs() < 1e-9,
            "measured no-show {} — the previous release was invisible",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn sum_arrivals_saturates() {
        let d = compute_release(inputs(u64::MAX, 0, u64::MAX, 0, 100_000), None, 0);
        // next_serving_counter saturates rather than wrapping.
        assert_eq!(d.next_serving_counter, u64::MAX);
    }

    // --- Fake store for pass orchestration -----------------------------------

    struct FakeStore {
        state: ControllerState,
        released: Mutex<Option<ReleaseDecision>>,
        /// When true, `write_release` reports a lost race without recording —
        /// mirroring `DynamoStore` returning `LostRace` on
        /// `ConditionalCheckFailedException`.
        lose_release: bool,
    }

    impl FakeStore {
        fn new(state: ControllerState) -> Self {
            Self {
                state,
                released: Mutex::new(None),
                lose_release: false,
            }
        }

        /// Returns a fake whose `write_release` loses the race on every call,
        /// mirroring a real store hitting `ConditionalCheckFailedException`
        /// after a concurrent invoke advanced `serving_counter` first.
        fn losing_race(state: ControllerState) -> Self {
            let mut s = Self::new(state);
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
                // guard refused, so this pass released nothing.
                std::future::ready(Ok(ReleaseOutcome::LostRace))
            } else {
                *self.released.lock().unwrap() = Some(*decision);
                std::future::ready(Ok(ReleaseOutcome::Advanced))
            }
        }
    }

    fn active_state() -> ControllerState {
        ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            // A cursor well into the event, with a release already behind it,
            // so the no-show measurement has something to measure. The pass's
            // `last_open_pass_at` is set to the same `now` the `run_pass`
            // tests pass (1_000_000), so the cadence reads as fresh
            // (`now - last_open_pass_at = 0 < STALE_OPEN_PASS_SECS`) and the
            // measurement proceeds — i.e. these tests exercise the
            // measurement path, not the staleness fall-back.
            inputs: {
                let mut i = inputs(250, 0, 20_000, 19_500, 50);
                i.queue_counter = u64::MAX;
                i.last_open_pass_at = 1_000_000;
                i
            },
            prev_no_show: None,
        }
    }

    #[tokio::test]
    async fn pass_releases_when_active() {
        let store = FakeStore::new(active_state());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert!(
            matches!(outcome, PassOutcome::Ran { released } if released > 0),
            "an active event with a rate must release, got {outcome:?}"
        );
        assert!(
            store.released.lock().unwrap().is_some(),
            "cursor not written"
        );
    }

    #[tokio::test]
    async fn a_lost_release_race_reports_releasing_nobody() {
        // The guarded UpdateItem refused to land, so another invoke advanced
        // the cursor and this pass released nobody. Reporting `decision.release`
        // here would double-count the winner's release in the logs — which is
        // the whole reason `write_release` returns an outcome rather than `()`.
        let store = FakeStore::losing_race(stale_loser_state());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 0 });
        assert!(
            store.released.lock().unwrap().is_none(),
            "a lost race must not record a release"
        );
    }

    #[tokio::test]
    async fn pass_is_noop_when_not_active() {
        let mut state = active_state();
        state.phase = Phase::PreQueue;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::NotActive(Phase::PreQueue));
        assert!(store.released.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn pause_stops_admission_entirely() {
        // A paused event is still Active, so the phase gate lets the pass
        // through and only the admission control stops it: serving_counter
        // must not advance.
        let mut state = active_state();
        state.stored_control = StoredControl::Paused;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::Paused));
        assert!(
            store.released.lock().unwrap().is_none(),
            "paused event released positions: pause is not holding admission"
        );
    }

    #[tokio::test]
    async fn fail_open_holds_the_controller_too() {
        // Under fail-open the waiting room is bypassed, so metering releases
        // nothing real; the counter stays put for recovery to resume from.
        let mut state = active_state();
        state.fail_open_until = 1000;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", 500).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::FailOpen));
        assert!(store.released.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_lapsed_fail_open_window_is_observed_on_the_very_next_pass() {
        // now supplied per pass (not read from a clock inside run_pass) is
        // what makes this work: the same stored state, evaluated a moment
        // after the epoch, resolves to Open with no write on either side.
        let mut state = active_state();
        state.fail_open_until = 1000;
        let store = FakeStore::new(state);
        let held = run_pass(&store, "evt", 500).await.unwrap();
        assert_eq!(held, PassOutcome::Held(AdmissionControl::FailOpen));
        let ran = run_pass(&store, "evt", 1000).await.unwrap();
        assert!(matches!(ran, PassOutcome::Ran { .. }));
    }

    #[tokio::test]
    async fn resuming_lets_the_controller_run_again() {
        // The same state with the control back to Open runs a full pass, so a
        // hold costs nothing but the intervals it covered.
        let store = FakeStore::new(active_state());
        let outcome = run_pass(&store, "evt", 1_000_000).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 1000 });
    }

    #[tokio::test]
    async fn a_stuck_sub_floor_event_recovers_in_one_pass() {
        // End-to-end: an event whose persisted `no_show_rate` sits just above
        // the DynamoDB floor (the stuck value) reads it back, recomputes a
        // sub-floor EWMA on a zero-no-show interval, and must persist the
        // floored 0.0 — the pass succeeds and `serving_counter` advances,
        // rather than the pass failing (or, on the live store, the
        // `UpdateItem` being rejected) every minute until the next no-show.
        let mut state = active_state();
        // Released 500 last interval, all 500 arrived -> observed_no_show 0,
        // so the only thing pulling the EWMA is the carried 1.36e-130.
        state.inputs = {
            let mut i = inputs(500, 0, 20_000, 19_500, 50);
            i.queue_counter = u64::MAX;
            // Keep the prior cadence fresh for the run pass at `now =
            // 1_000_000` so the test exercises the EWMA decay, not the
            // staleness fall-back (the fall-back would carry the sub-floor
            // prev through unchanged and never get to demonstrate the decay).
            i.last_open_pass_at = 1_000_000;
            i
        };
        state.prev_no_show = Some(NoShowState {
            smoothed_rate: 1.36e-130,
        });
        let store = FakeStore::new(state);

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

    // --- Lost release race: the pass reports what it actually released

    /// State in which a staler arrivals read (200 vs a winner's 250) yields a
    /// larger bounded release, so a losing pass computes a higher
    /// `next_serving_counter` than the winner persisted.
    fn stale_loser_state() -> ControllerState {
        let mut state = active_state();
        // Arrivals 200 (stale) vs the winner's 250 (fresh); the loser reads
        // fewer arrivals -> higher no-show -> larger release (610 vs 588).
        state.inputs.arrivals[0] = 200;
        state.prev_no_show = Some(NoShowState { smoothed_rate: 0.0 });
        state
    }

    // --- Resume after an operator hold: no over-release -----------------------
    //
    // A stateful store whose `write_release` mirrors the live `DynamoStore`'s
    // `SET` clause: persisting `serving_counter`, `last_serving_counter`,
    // `last_arrivals_total`, `no_show_rate`, and now `last_open_pass_at` into
    // the very state `read_state` returns next. The existing `FakeStore` is
    // deliberately stateless across reads — useful for hand-assembled inputs,
    // but the bug being fixed is precisely a cross-pass artifact: a held pass
    // that writes nothing, then a resume pass that reads the frozen baselines
    // back. That statefulness is what these tests drive end-to-end through
    // `run_pass`, not by hand-threading `ReleaseInputs`. The store never loses
    // a race; `loser_release(true)` models a concurrent invoke winning.

    struct StatefulStore {
        state: Mutex<ControllerState>,
    }

    impl StatefulStore {
        fn new(state: ControllerState) -> Self {
            Self {
                state: Mutex::new(state),
            }
        }
    }

    impl Store for StatefulStore {
        fn read_state(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send {
            let s = self.state.lock().unwrap().clone();
            std::future::ready(Ok(s))
        }

        fn write_release(
            &self,
            _event_id: &str,
            decision: &ReleaseDecision,
            _expected: u64,
        ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send {
            let mut s = self.state.lock().unwrap();
            // Mirror the live `DynamoStore::write_release` `SET` clause: the
            // cursor advances, the pre-release cursor and arrivals total
            // become next interval's baselines, the smoothed no-show state
            // carries forward, and the open-pass timestamp that
            // `compute_release` consults next time is `now`. Lost races are
            // already covered by the stateless `FakeStore::losing_race`;
            // this store's job is the cross-pass propagation.
            s.inputs.serving_counter = decision.next_serving_counter;
            s.inputs.last_serving_counter = decision.previous_serving_counter;
            s.inputs.last_arrivals_total = decision.arrivals_total;
            s.inputs.last_open_pass_at = decision.last_open_pass_at;
            s.prev_no_show = Some(decision.no_show);
            std::future::ready(Ok(ReleaseOutcome::Advanced))
        }
    }

    /// Drives the `Open → Paused → Open` resume arc through the stateful store.
    ///
    /// `f` is the fraction of the pre-pause release that clicked through
    /// *between* the pre-pause write and the pause engaging: the rest are
    /// stranded in-flight by `decide`'s `NotAdmitting`, exactly as in
    /// production. Returns `(resume-pass release, persisted smoothed no-show
    /// rate after the resume pass)`.
    async fn resume_after_pause(f: f64) -> (u64, f64) {
        // `inputs(500, 0, 500, 0, 50)`: 500 arrivals against a 500-release
        // prior interval -> full click-through, EWMA seeded at 0. Prior
        // smoothed is `0.0`, matching the report's worst-case baseline.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: inputs(500, 0, 500, 0, 50),
            prev_no_show: Some(NoShowState { smoothed_rate: 0.0 }),
        };
        let store = StatefulStore::new(state);
        // Pre-pause open pass at T = 1_000_000: releases `target` (500), all
        // 500 arrivals already recorded -> observed no-show 0, smoothed stays 0.
        assert!(
            matches!(
                run_pass(&store, "evt", 1_000_000).await.unwrap(),
                PassOutcome::Ran { released: 500 }
            ),
            "pre-pause pass must release exactly target (500)"
        );
        // Of the 500 the pre-pause pass released, `f · 500` clicked through
        // before the pause engaged; the rest were stranded in-flight by the
        // hold. These arrivals are observed against the pre-pause baseline.
        {
            let mut s = store.state.lock().unwrap();
            // `f ∈ [0, 1]`, so `500.0 * f` is in `[0, 500]` — well within
            // u64 precision and sign. `round` to the nearest click.
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "f ∈ [0, 1] forces (500.0 * f).round() into [0, 500], \
                          no truncation or sign loss is possible"
            )]
            let arrived = (500.0 * f).round() as u64;
            s.inputs.arrivals[0] += arrived;
        }
        // Engage the hold. The held pass returns `Held` and writes nothing —
        // the exact artifact the bug freezes the baselines through.
        store.state.lock().unwrap().stored_control = StoredControl::Paused;
        assert_eq!(
            run_pass(&store, "evt", 1_000_001).await.unwrap(),
            PassOutcome::Held(AdmissionControl::Paused)
        );
        // Resume. The release pass should release exactly `target` (500), not
        // the 714/588/500 f-sweep the bug seeds from the hold as a no-show.
        store.state.lock().unwrap().stored_control = StoredControl::Open;
        match run_pass(&store, "evt", 1_000_999).await.unwrap() {
            PassOutcome::Ran { released } => {
                let smooth = store
                    .state
                    .lock()
                    .unwrap()
                    .prev_no_show
                    .unwrap()
                    .smoothed_rate;
                (released, smooth)
            }
            other => panic!("resume pass should run, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resume_after_pause_releases_target_at_every_pause_timing() {
        // The bug's whole trigger table. Pre-fix the `f`-sweep asserted
        // `{714, 588, 500}` (the report's "Trigger conditions and bounds"
        // table); the staleness check now collapses every case onto the raw
        // `target = 500`, with the EWMA held at its pre-pause value (0.0).
        // The `f = 1.0` boundary — pause just before the next write, all the
        // pre-pause release clicked through — was never over-releasing, which
        // is why both rows agree there: the fix does not over-correct.
        let cases = [
            (0.0f64, 500u64, 0.0f64), // worst case: pause right after the write
            (0.5, 500, 0.0),          // midway (uniform-timing upper envelope)
            (1.0, 500, 0.0),          // boundary: nothing was stranded
        ];
        for (f, want_rel, want_smooth) in cases {
            let (released, smooth) = resume_after_pause(f).await;
            assert_eq!(
                released, want_rel,
                "f={f}: resume pass over-released; the hold was measured as a \
                 no-show (pre-fix would be 714/588/500)"
            );
            assert!(
                (smooth - want_smooth).abs() < 1e-9,
                "f={f}: smoothed rate {smooth} != {want_smooth} (the EWMA \
                 should be held across the hold, not seeded by it)"
            );
        }
    }

    #[tokio::test]
    async fn resume_after_pause_is_invariant_to_pause_duration() {
        // The over-release was controller-counter-invariant to pause
        // duration: once paused, every baseline is frozen, so a short hold
        // and a many-hour hold produce the same first resume pass. The fix
        // is invariant for the same reason — the staleness check sees the
        // same frozen `last_open_pass_at` either way — and a hold of 100
        // held passes (1000 s) lands the same 500 as a single one.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: inputs(500, 0, 500, 0, 50),
            prev_no_show: Some(NoShowState { smoothed_rate: 0.0 }),
        };
        let store = StatefulStore::new(state);
        assert!(matches!(
            run_pass(&store, "evt", 1_000_000).await.unwrap(),
            PassOutcome::Ran { released: 500 }
        ));
        store.state.lock().unwrap().stored_control = StoredControl::Paused;
        for i in 0..100 {
            assert_eq!(
                run_pass(&store, "evt", 1_000_001 + i).await.unwrap(),
                PassOutcome::Held(AdmissionControl::Paused)
            );
        }
        // No `write_release` landed on any of those 100 held passes, so the
        // baselines are frozen exactly as long as the report's short hold —
        // the staleness check fire is the only thing that has changed.
        store.state.lock().unwrap().stored_control = StoredControl::Open;
        match run_pass(&store, "evt", 1_000_999).await.unwrap() {
            PassOutcome::Ran { released } => {
                assert_eq!(
                    released, 500,
                    "long hold should release target after resume, not the \
                     pre-fix 714 — the over-release is duration-invariant either \
                     way"
                );
            }
            other => panic!("resume after long hold should run, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resume_pass_advances_last_open_pass_at_so_next_cadence_measures() {
        // The fall-back is one interval of relief, not a sticky new normal:
        // the resume pass persists `last_open_pass_at = now`, so the pass
        // after the resume runs at `now - last = INTERVAL_SECS`, which is
        // below `STALE_OPEN_PASS_SECS`, and the no-show measurement turns
        // back on. Without this the staleness check would oscillate forever.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: inputs(500, 0, 500, 0, 50),
            prev_no_show: Some(NoShowState { smoothed_rate: 0.0 }),
        };
        let store = StatefulStore::new(state);
        assert!(matches!(
            run_pass(&store, "evt", 1_000_000).await.unwrap(),
            PassOutcome::Ran { released: 500 }
        ));
        store.state.lock().unwrap().stored_control = StoredControl::Paused;
        assert_eq!(
            run_pass(&store, "evt", 1_000_001).await.unwrap(),
            PassOutcome::Held(AdmissionControl::Paused)
        );
        store.state.lock().unwrap().stored_control = StoredControl::Open;
        // Resume at the +1 s mark: `now - last_open_pass_at = 999 >= STALE`,
        // fall-back fires, `last_open_pass_at` advances to `1_000_999`.
        match run_pass(&store, "evt", 1_000_999).await.unwrap() {
            PassOutcome::Ran { released } => assert_eq!(released, 500),
            other => panic!("resume pass should run, got {other:?}"),
        }
        // Now `now - last = INTERVAL_SECS`, fresh, so the next pass measures
        // normally: the 500 released by the resume pass got a full interval
        // to click through, so observed arrivals == released, no-show 0, and
        // the EWMA stays at 0. The release is `target` again, *because the
        // measurement was taken this time*, not because it was suppressed.
        {
            let s = store.state.lock().unwrap();
            assert_eq!(
                s.inputs.last_open_pass_at, 1_000_999,
                "resume pass must persist last_open_pass_at = its own now"
            );
            assert!(
                s.inputs.last_serving_counter == s.inputs.serving_counter - 500,
                "the resume pass advanced serving_counter by the released 500"
            );
        }
        // Add the arrivals the resume pass released: all 500 clicked through.
        store.state.lock().unwrap().inputs.arrivals[0] += 500;
        match run_pass(&store, "evt", 1_001_009).await.unwrap() {
            PassOutcome::Ran { released } => assert_eq!(
                released, 500,
                "next cadence pass should measure a 0 no-show and release \
                 target — not fall back — confirming the staleness check does \
                 not stick"
            ),
            other => panic!("next cadence pass should run, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resume_pass_holds_a_nonzero_ewma_across_the_pause() {
        // The fall-back suppresses a *measurement*, not the carried state: a
        // real pre-pause no-show rate is held across the pause so the
        // post-resume cadence applies the (decaying) signal. Here the prior
        // smoothed rate is 0.3 from a genuine no-show before the pause, the
        // fall-back holds it at 0.3 and releases `target` (500), and the
        // *next* normal pass — measuring the resume pass's 500 release with
        // full click-through — decays the EWMA to 0.21 and releases `target
        // / 0.79 ≈ 633`, the controller doing its job again on real signal.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: inputs(500, 0, 500, 0, 50),
            prev_no_show: Some(NoShowState { smoothed_rate: 0.3 }),
        };
        let store = StatefulStore::new(state);
        assert!(
            matches!(
                run_pass(&store, "evt", 1_000_000).await.unwrap(),
                PassOutcome::Ran { released: 500 }
            ),
            "pre-pause pass releases target — the 0.3 EWMA is unchanged"
        );
        store.state.lock().unwrap().stored_control = StoredControl::Paused;
        assert_eq!(
            run_pass(&store, "evt", 1_000_001).await.unwrap(),
            PassOutcome::Held(AdmissionControl::Paused)
        );
        store.state.lock().unwrap().stored_control = StoredControl::Open;
        match run_pass(&store, "evt", 1_000_999).await.unwrap() {
            PassOutcome::Ran { released } => {
                assert_eq!(
                    released, 500,
                    "fall-back: resume pass releases `target`, NOT \
                     target / (1 - 0.3) = 714"
                );
                let smooth = store
                    .state
                    .lock()
                    .unwrap()
                    .prev_no_show
                    .unwrap()
                    .smoothed_rate;
                assert!(
                    (smooth - 0.3).abs() < 1e-9,
                    "the EWMA should be held at the pre-pause 0.3 across the \
                     pause, not seeded by the hold — got {smooth}"
                );
            }
            other => panic!("resume pass should run, got {other:?}"),
        }
        // Next cadence, every released visitor clicked through: no-show 0,
        // EWMA decays to 0.7 * 0.3 = 0.21, release 500 / 0.79 = 633.
        store.state.lock().unwrap().inputs.arrivals[0] += 500;
        match run_pass(&store, "evt", 1_001_009).await.unwrap() {
            PassOutcome::Ran { released } => {
                // 500 / 0.79 = 633.0 -> 633
                assert_eq!(released, 633, "next cadence should measure normally");
                let smooth = store
                    .state
                    .lock()
                    .unwrap()
                    .prev_no_show
                    .unwrap()
                    .smoothed_rate;
                assert!(
                    (smooth - 0.21).abs() < 1e-9,
                    "EWMA should decay from 0.3 to 0.21 on a 0 no-show, got \
                     {smooth}"
                );
            }
            other => panic!("next cadence pass should run, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_held_pass_does_not_advance_last_open_pass_at() {
        // The held pass stays a pure read: it neither advances the cursor
        // nor the open-pass timestamp, which is exactly what lets the resume
        // pass's staleness check fire. Asserting this directly guards the
        // regression-risks of Option A (write-on-hold) — the held branch's
        // write contract stays exactly as before.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: {
                let mut i = inputs(500, 0, 500, 0, 50);
                i.last_open_pass_at = 999_990;
                i
            },
            prev_no_show: Some(NoShowState { smoothed_rate: 0.0 }),
        };
        let store = StatefulStore::new(state);
        // Hold one pass before pausing too: the held pass must not touch
        // anything.
        store.state.lock().unwrap().stored_control = StoredControl::Paused;
        let before = {
            let s = store.state.lock().unwrap();
            (s.inputs.serving_counter, s.inputs.last_open_pass_at)
        };
        assert_eq!(
            run_pass(&store, "evt", 1_000_010).await.unwrap(),
            PassOutcome::Held(AdmissionControl::Paused)
        );
        let after = {
            let s = store.state.lock().unwrap();
            (s.inputs.serving_counter, s.inputs.last_open_pass_at)
        };
        assert_eq!(
            before, after,
            "a held pass must not advance the cursor or the open-pass \
             timestamp: write-on-hold is not the contract"
        );
    }

    #[tokio::test]
    async fn resume_after_fail_open_releases_target() {
        // `FailOpen` holds the controller for the same reason `Paused` does
        // (the held branch fires for both control values), and `decide`
        // returns `NotAdmitting` to `record_arrival` under fail-open too, so
        // the baselines freeze identically and the staleness check must fire
        // on resume from either hold. Drive it via `fail_open_until` so the
        // variant of `resolve` is `FailOpen`, which is what the live edge gate
        // actually sees.
        let state = ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            // No fail-open window yet: the pre-pause pass must run Open.
            fail_open_until: 0,
            inputs: inputs(500, 0, 500, 0, 50),
            prev_no_show: Some(NoShowState { smoothed_rate: 0.0 }),
        };
        let store = StatefulStore::new(state);
        assert!(
            matches!(
                run_pass(&store, "evt", 1_000_000).await.unwrap(),
                PassOutcome::Ran { released: 500 }
            ),
            "pre-pause pass must release target while the window is open"
        );
        // Engage fail-open: a window that covers the next held pass but lapses
        // before the resume pass. `now < fail_open_until` resolves to
        // `FailOpen`, the held branch fires, `write_release` never runs.
        store.state.lock().unwrap().fail_open_until = 1_000_500;
        assert_eq!(
            run_pass(&store, "evt", 1_000_001).await.unwrap(),
            PassOutcome::Held(AdmissionControl::FailOpen)
        );
        // Fail-open window has lapsed: stored Open is authoritative again.
        match run_pass(&store, "evt", 1_000_999).await.unwrap() {
            PassOutcome::Ran { released } => assert_eq!(
                released, 500,
                "resume from FailOpen must release target, just like Paused"
            ),
            other => panic!("resume from FailOpen should run, got {other:?}"),
        }
    }

    // --- Staleness check: direct `compute_release` cases ----------------------

    #[test]
    fn a_fresh_prior_open_pass_measures_no_show_normally() {
        // The control: the prior open pass was `INTERVAL_SECS` ago (a normal
        // 10 s cadence). `now - last == INTERVAL_SECS < STALE_OPEN_PASS_SECS`,
        // so the measurement branch runs exactly as it did before the fix.
        let mut i = inputs(250, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.0 }), 1_000_010);
        assert!(
            (d.no_show.smoothed_rate - 0.15).abs() < 1e-9,
            "a fresh prior pass should measure 0.5 no-show -> EWMA 0.15, got {:e}",
            d.no_show.smoothed_rate
        );
        // 500 / (1 - 0.15) = 588.24 -> 588.
        assert_eq!(d.release, 588);
    }

    #[test]
    fn a_prior_open_pass_exactly_one_interval_ago_measures_normally() {
        // The threshold is `>= 2 * INTERVAL_SECS`: the boundary case
        // `now - last == INTERVAL_SECS` is the normal cadence and must
        // measure, otherwise every single normal pass would fall back.
        let mut i = inputs(250, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, None, 1_000_010);
        assert!(
            (d.no_show.smoothed_rate - 0.5).abs() < 1e-9,
            "exactly one interval ago is fresh; observed 250/500 no-show 0.5, \
             got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(
            d.release, 1000,
            "smoothing 0.5 -> 500 / 0.5 = 1000 (capped)"
        );
    }

    #[test]
    fn a_prior_open_pass_at_the_staleness_threshold_falls_back() {
        // The boundary is `>= 2 * INTERVAL_SECS`: `now - last == 20` fires
        // the fall-back. The release is `target` and the EWMA is held; the
        // `f = 0` Pause-then-resume invariant rides on this firing, so the
        // boundary cannot drift to `>` without reopening the bug.
        let mut i = inputs(0, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.5 }), 1_000_020);
        // Depressed arrivals (0 against a 500 release) would normally read
        // as a 1.0 no-show, blow the EWMA up, and release the 2x cap; the
        // fall-back holds the prev (0.5) and releases `target` instead.
        assert!(
            (d.no_show.smoothed_rate - 0.5).abs() < 1e-9,
            "EWMA is held at the pre-pause 0.5, not seeded from a stale 1.0, \
             got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(
            d.release, 500,
            "release is the raw target, not 500 / 0.5 = 1000"
        );
        assert_eq!(d.last_open_pass_at, 1_000_020);
    }

    #[test]
    fn a_prior_open_pass_well_past_the_threshold_falls_back() {
        // A multi-interval pause: `now - last == 600s` (60 intervals) —
        // exactly the magnitude `resume_after_pause_is_invariant_to_pause_
        // duration` reaches. Same fall-back as the boundary, demonstrating
        // the threshold catches both single-interval-delayed invokes and
        // long holds with one rule.
        let mut i = inputs(0, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.0 }), 1_000_600);
        assert_eq!(
            d.release, 500,
            "a stale open pass releases the raw target regardless of how \
             stale"
        );
        assert!(
            d.no_show.smoothed_rate.abs() < 1e-9,
            "the held EWMA is 0, not measured — got {:e}",
            d.no_show.smoothed_rate
        );
    }

    #[test]
    fn a_cold_state_with_no_prior_open_pass_releases_target() {
        // A fresh event that has never run an open pass has `last_open_pass_at
        // = 0` (the attribute is absent), and the first post-deploy pass sees
        // `now - last ≫ STALE`: it falls back and releases exactly `target`,
        // instead of seeding the EWMA off `0/0`. Benign — and idempotent, the
        // first `write_release` lands and sets it for everything that follows.
        let i = inputs(0, 0, 100, 100, 50);
        let d = compute_release(i, None, 1_788_000_000);
        assert_eq!(d.release, 500, "first post-deploy pass releases raw target");
        assert!(
            d.no_show.smoothed_rate.abs() < 1e-9,
            "no measurement is taken on the first-ever pass, got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(d.last_open_pass_at, 1_788_000_000);
    }

    #[test]
    fn staleness_fires_even_when_released_last_is_nonzero() {
        // The whole point of the fix: the "no release last interval" guard
        // (`released_last == 0`) does not catch the resume case — `released_last`
        // is the *pre-pause* release, still nonzero because the cursor never
        // moved during the hold. Only the staleness check catches it. Slicing
        // this directly off `compute_release` shows the guard is not what
        // saves the resume pass.
        let mut i = inputs(0, 0, 1000, 500, 50); // released_last = 500 (!= 0)
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.0 }), 1_000_999);
        assert!(
            d.release == 500 && d.no_show.smoothed_rate.abs() < 1e-9,
            "released_last = 500 (nonzero) but stale -> fall-back, NOT \
             measured; pre-fix this returned 714 at EWMA 0.3"
        );
    }

    #[test]
    fn staleness_threshold_tolerates_a_sub_two_interval_jitter() {
        // The controller runs on a 10 s `ctx.wait(Duration::from_secs(INTERVAL
        // _SECS))` between passes plus a Lambda invoke per resume: a normal
        // cadence pass realistically sees `now - last_open_pass_at` in roughly
        // `(INTERVAL_SECS, 2 * INTERVAL_SECS)` from that jitter alone. The
        // threshold is `>= 2 * INTERVAL_SECS` so cadence jitter never
        // misfires; setting it to `> INTERVAL_SECS` would fall back on every
        // pass and turn the EWMA into a no-op. A pass at `1.5 * INTERVAL_SECS`
        // ago measures normally.
        let mut i = inputs(250, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        // 15 seconds later is below STALE_OPEN_PASS_SECS = 20.
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.0 }), 1_000_015);
        assert!(
            (d.no_show.smoothed_rate - 0.15).abs() < 1e-9,
            "1.5-interval jitter should not fire the staleness check, got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(d.release, 588);
    }

    #[test]
    fn a_clock_skew_now_before_last_open_pass_is_not_stale() {
        // `now.saturating_sub(last_open_pass_at)` clamps a backwards clock
        // skew to `0`, so a momentary skew reads as fresh rather than stale.
        // Otherwise a Lambda invoke with a wall clock behind DynamoDB
        // would fall back every pass for the duration of the skew.
        let mut i = inputs(250, 0, 1000, 500, 50);
        i.last_open_pass_at = 1_000_000;
        let d = compute_release(i, Some(NoShowState { smoothed_rate: 0.0 }), 999_990);
        assert!(
            (d.no_show.smoothed_rate - 0.15).abs() < 1e-9,
            "clock skew (now < last_open_pass_at) should saturate to 0, not \
             misfire as stale, got {:e}",
            d.no_show.smoothed_rate
        );
        assert_eq!(d.release, 588);
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
            last_open_pass_at in any::<u64>(),
            now in any::<u64>(),
        ) {
            let d = compute_release(
                ReleaseInputs {
                    arrivals,
                    last_arrivals_total: last_arrivals,
                    last_serving_counter: last_serving,
                    serving_counter: serving,
                    queue_counter: queue,
                    target_rate: rate,
                    last_open_pass_at,
                        },
                prev_rate,
                now,
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
            // The persisted open-pass timestamp is always exactly `now`,
            // whether the pass measured or fell back: the value is what the
            // next interval's staleness check reads back.
            prop_assert_eq!(d.last_open_pass_at, now);
            // smoothed rate stays a valid probability.
            prop_assert!(d.no_show.smoothed_rate >= 0.0 && d.no_show.smoothed_rate <= 1.0);
            prop_assert!(d.no_show.smoothed_rate.is_finite());
            // The persisted smoothed rate is always DynamoDB-storable: exactly
            // 0.0 or at least the positive `Number` minimum, so the
            // `write_release` UpdateItem is never rejected for underflow —
            // the staleness branch carries `prev` through the same floor as
            // the measured branch, so it holds on both sides.
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

        #[test]
        fn compute_release_never_panics_or_rewinds(
            arrivals in shard_arrivals(),
            last_arrivals in any::<u64>(),
            serving in any::<u64>(),
            last_serving in any::<u64>(),
            queue in any::<u64>(),
            rate in 1u32..=100_000,
            last_open_pass_at in any::<u64>(),
            now in any::<u64>(),
        ) {
            let d = compute_release(
                ReleaseInputs {
                    arrivals,
                    last_arrivals_total: last_arrivals,
                    last_serving_counter: last_serving,
                    serving_counter: serving,
                    queue_counter: queue,
                    target_rate: rate,
                    last_open_pass_at,
                },
                None,
                now,
            );
            prop_assert!(d.next_serving_counter >= serving);
            prop_assert!(
                d.next_serving_counter <= queue.saturating_add(1) || d.next_serving_counter == serving
            );
            prop_assert_eq!(d.release, d.next_serving_counter - serving);
            prop_assert_eq!(d.previous_serving_counter, serving);
            prop_assert_eq!(d.last_open_pass_at, now);
            prop_assert!(d.no_show.smoothed_rate.is_finite());
            // The release stays bounded by the cap.
            let cap = target_release_per_interval(rate).saturating_mul(2);
            prop_assert!(d.release <= cap);
        }
    }
}
