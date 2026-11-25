//! Outflow controller.
//!
//! Admission is metered where it happens, not where it is offered
//! ([`ADR-0039`](../../../docs/adr/0039-meter-admissions-not-the-cursor.md)).
//! `generate_token` takes one unit of an **admission budget** for every visitor
//! it admits, and refuses when there is none. Each pass, the controller:
//!
//! 1. grants budget for the time that has passed -- `target_rate` a second --
//!    on top of whatever went unused, up to [`BURST_INTERVALS`] intervals'
//!    worth, and
//! 2. advances `serving_counter` by the same amount plus the unused budget, so
//!    that people who never show up are replaced by the people behind them.
//!
//! The first bounds what the origin receives: over any window of `t` seconds no
//! more than `target_rate * t` plus the burst can be admitted, whoever turns up
//! and whenever. The second is the whole no-show correction. Budget left
//! unspent is, exactly, admissions the origin was promised and did not get, so
//! it is what the cursor has to make up; there is no rate to estimate and
//! nothing to smooth.
//!
//! The controller expires nothing. A position lives until `DynamoDB` TTL
//! reclaims its row
//! ([`ADR-0031`](../../../docs/adr/0031-remove-controller-driven-expiry.md)).
//!
//! A pass runs only when the event is `Active` **and** the operator's
//! [`AdmissionControl`] is `Open`.
//!
//! All arithmetic here is checked or saturating: the release profile has no
//! overflow checks, so a bare subtraction that underflows would wrap to a huge
//! value and release a damaging burst.

use std::future::Future;

use serde::{Deserialize, Serialize};
use wr_common::{AdmissionControl, Phase, SHARDS, StoredControl, resolve};

pub mod dynamo;

/// The controller interval in seconds. The `EventBridge` Scheduler `rate()`
/// minimum is one minute, so the schedule fires `rate(1 minute)` and the
/// handler runs [`PASSES_PER_INVOKE`] passes this many seconds apart.
pub const INTERVAL_SECS: u64 = 10;

/// Passes per durable execution. `INTERVAL_SECS * PASSES_PER_INVOKE == 60`, so
/// one execution started by the `rate(1 minute)` schedule covers a full minute.
pub const PASSES_PER_INVOKE: u32 = 6;

/// A prior open pass at least this old is not owed for.
///
/// On the regular cadence a pass grants budget for the seconds since the last
/// one. After a hold, a missed minute, or on a fresh event, that gap is not
/// admission anyone is owed: paying it out would hand a paused event its whole
/// pause as a burst on resume. Such a pass grants one interval and carries on.
pub const STALE_OPEN_PASS_SECS: u64 = 2 * INTERVAL_SECS;

/// The most budget that can ever be outstanding, in intervals of `target_rate`.
///
/// It is the burst the origin can be asked to absorb on top of the steady rate,
/// and it is also what bounds the no-show correction: unused budget is what the
/// cursor makes up, so with at most one interval carried over the cursor moves
/// at no more than twice the target, which covers a no-show rate up to one
/// half.
pub const BURST_INTERVALS: u64 = 2;

/// One arrivals shard as the controller reads it: how many admissions it has
/// recorded, and the cap `generate_token` may take it up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardBudget {
    /// Admissions recorded on this shard since the event began.
    pub taken: u64,
    /// The count this shard may be taken up to. A shard that has never been
    /// granted budget has none, and reads as zero.
    pub cap: u64,
}

impl ShardBudget {
    /// Budget granted on this shard and not yet taken.
    #[must_use]
    pub const fn unused(self) -> u64 {
        self.cap.saturating_sub(self.taken)
    }
}

/// Inputs read from the `Counters` table for one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassInputs {
    /// The ten arrivals shards.
    pub shards: [ShardBudget; SHARDS],
    /// The current `serving_counter`.
    pub serving_counter: u64,
    /// The highest position ever issued. The live-join sequence ends here, and
    /// after an open it starts at the cohort size, so this is the end of the
    /// line however the positions were assigned.
    pub queue_counter: u64,
    /// Operator target rate in visitors per second (the `/admin/rate` value).
    pub target_rate: u32,
    /// Epoch seconds of the last pass that ran while admission was open, `0`
    /// if there has never been one.
    pub last_open_pass_at: u64,
}

/// What one pass decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassDecision {
    /// Positions released: the distance the cursor moved, which is less than
    /// was computed when the end of the line is reached.
    pub release: u64,
    /// The new `serving_counter`.
    pub next_serving_counter: u64,
    /// The admission budget outstanding after this pass, across all shards.
    pub budget: u64,
    /// The cap to write on each shard: what it has taken, plus its share of
    /// `budget`.
    pub caps: [u64; SHARDS],
    /// Epoch seconds at which this pass ran, persisted for the next one.
    pub last_open_pass_at: u64,
}

/// Budget granted and not yet taken, across all shards.
#[must_use]
pub fn unused_budget(shards: [ShardBudget; SHARDS]) -> u64 {
    shards
        .iter()
        .fold(0u64, |total, shard| total.saturating_add(shard.unused()))
}

/// The admissions owed for the time since the last open pass.
///
/// `target_rate` times the elapsed seconds, so the total granted follows the
/// wall clock however many passes run (issue #236): an overlapping execution's
/// extra pass is owed only the seconds since the one before it, and nothing if
/// it ran in the same second. A stale or absent prior pass, or a clock that
/// reads behind it, is owed one interval -- see [`STALE_OPEN_PASS_SECS`].
#[must_use]
pub fn owed(target_rate: u32, last_open_pass_at: u64, now: u64) -> u64 {
    let rate = u64::from(target_rate);
    match now.checked_sub(last_open_pass_at) {
        Some(elapsed) if last_open_pass_at != 0 && elapsed < STALE_OPEN_PASS_SECS => {
            rate.saturating_mul(elapsed)
        }
        Some(_) | None => rate.saturating_mul(INTERVAL_SECS),
    }
}

/// Decides one pass: how far the cursor moves and what budget stands after it.
///
/// A pure function of its inputs and `now`, so the cadence is testable with
/// time injected.
#[must_use]
pub fn compute_pass(inputs: PassInputs, now: u64) -> PassDecision {
    let owed = owed(inputs.target_rate, inputs.last_open_pass_at, now);
    let unused = unused_budget(inputs.shards);
    let burst = u64::from(inputs.target_rate)
        .saturating_mul(INTERVAL_SECS)
        .saturating_mul(BURST_INTERVALS);

    // What may be admitted from here until the next pass. Capped, so budget
    // nobody took does not pile up into a burst: an event that sat idle for an
    // hour has the same budget as one that sat idle for twenty seconds.
    let budget = unused.saturating_add(owed).min(burst);

    // One position for each admission owed, plus one for each admission that
    // was granted and not taken. The second term is the no-show correction, and
    // it is bounded by the first: a pass that is owed nothing (it ran in the
    // same second as another) releases nothing, however much budget is unused.
    let positions = owed.saturating_add(unused.min(owed));

    // The cursor is exclusive -- position p is admitted once p < serving_counter
    // -- and queue_counter is the highest position ever issued, so one past it
    // admits everyone in line and nobody who is not. `.max()` keeps the cursor
    // monotonic: a queue_counter that reads behind it must never drag
    // admission backwards.
    let ceiling = inputs.queue_counter.saturating_add(1);
    let next_serving_counter = inputs
        .serving_counter
        .saturating_add(positions)
        .min(ceiling)
        .max(inputs.serving_counter);

    PassDecision {
        release: next_serving_counter.saturating_sub(inputs.serving_counter),
        next_serving_counter,
        budget,
        caps: caps_for(inputs.shards, budget),
        last_open_pass_at: now,
    }
}

/// Spreads `budget` across the shards as evenly as whole numbers allow, on top
/// of what each has already taken.
///
/// `generate_token` draws a shard at random for each admission, so an even
/// split is the one that matches where the takes will land. The shares sum to
/// `budget` exactly.
fn caps_for(shards: [ShardBudget; SHARDS], budget: u64) -> [u64; SHARDS] {
    let count = SHARDS as u64;
    let share = budget.checked_div(count).unwrap_or(0);
    let remainder = budget.checked_rem(count).unwrap_or(0);
    let mut caps = [0u64; SHARDS];
    for (index, (cap, shard)) in caps.iter_mut().zip(shards).enumerate() {
        let extra = u64::from((index as u64) < remainder);
        *cap = shard.taken.saturating_add(share).saturating_add(extra);
    }
    caps
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("controller store error: {0}")]
pub struct StoreError(pub String);

/// The outcome of a guarded [`Store::write_release`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The `UpdateItem` landed: `serving_counter` is now
    /// `decision.next_serving_counter`.
    Advanced,
    /// Another pass advanced the cursor first; this one's write did not land
    /// and it released nobody.
    LostRace,
}

/// The current state the controller needs before it acts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerState {
    pub phase: Phase,
    /// The operator's stored admission override (issue #71: never `FailOpen`,
    /// which is resolved from `fail_open_until` instead).
    pub stored_control: StoredControl,
    /// Epoch-seconds fail-open deadline; `0` means no window is in force.
    pub fail_open_until: u64,
    pub inputs: PassInputs,
}

/// The persistence port the controller drives. A trait seam so the pipeline runs
/// AWS-free in tests (the `dynamo` module supplies the live implementation).
pub trait Store {
    /// Reads the event's `Counters` item and its ten arrivals shards.
    fn read_state(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send;

    /// Advances `serving_counter` to `decision.next_serving_counter` and stamps
    /// `last_open_pass_at`, guarded on the cursor still being
    /// `expected_serving_counter` so two passes cannot both advance from the
    /// same read. A lost race is not an error.
    fn write_release(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send;

    /// Writes each shard's cap. The caps are absolute, so writing them twice
    /// grants nothing twice.
    fn write_caps(
        &self,
        event_id: &str,
        caps: &[u64; SHARDS],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// The result of one controller pass.
///
/// A pass runs as a durable step, so this is checkpointed and replayed from the
/// checkpoint instead of being recomputed: it has to round-trip through serde.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PassOutcome {
    /// The event is not `Active`; the controller did nothing.
    NotActive(Phase),
    /// The operator's admission override is not `Open`; the controller did
    /// nothing.
    Held(AdmissionControl),
    /// The controller ran and released `released` positions. `0` when the
    /// release lost its race against a concurrent pass.
    Ran { released: u64 },
}

/// Runs one controller pass for the event: read state, gate on `Active` and on
/// the operator's admission override, then move the cursor and grant budget.
///
/// The cursor is written first and the budget second. A pass that loses the
/// cursor race writes no budget: the winner computed the same grant from the
/// same state. A pass whose budget write fails has moved the cursor without
/// granting for it, which admits fewer people than intended until the next
/// pass makes it up -- the safe direction.
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

    // Any control other than Open returns the whole pass: a hold means nobody
    // is admitted, not that admissions accrue silently. Under fail-open the
    // waiting room is bypassed, so a release would meter nothing.
    match resolve(state.stored_control, state.fail_open_until, now) {
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

    let decision = compute_pass(state.inputs, now);
    match store
        .write_release(event_id, &decision, state.inputs.serving_counter)
        .await?
    {
        ReleaseOutcome::Advanced => {}
        ReleaseOutcome::LostRace => {
            tracing::info!(
                event_id,
                stale_serving_counter = state.inputs.serving_counter,
                "controller pass released nothing: lost the race"
            );
            return Ok(PassOutcome::Ran { released: 0 });
        }
    }
    store.write_caps(event_id, &decision.caps).await?;

    tracing::info!(
        event_id,
        released = decision.release,
        serving_counter = decision.next_serving_counter,
        unused_budget = unused_budget(state.inputs.shards),
        budget = decision.budget,
        "controller pass"
    );
    Ok(PassOutcome::Ran {
        released: decision.release,
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;

    const NOW: u64 = 1_000_000;

    /// A mid-event state: the last pass ran one interval ago, and every unit of
    /// the budget it granted was taken.
    fn inputs(serving: u64, rate: u32) -> PassInputs {
        PassInputs {
            shards: [ShardBudget::default(); SHARDS],
            serving_counter: serving,
            queue_counter: u64::MAX - 1,
            target_rate: rate,
            last_open_pass_at: NOW - INTERVAL_SECS,
        }
    }

    /// The same, with `unused` budget left over, all on shard 0.
    fn with_unused(mut i: PassInputs, unused: u64) -> PassInputs {
        i.shards[0] = ShardBudget {
            taken: 100,
            cap: 100 + unused,
        };
        i
    }

    fn total_cap_headroom(shards: [ShardBudget; SHARDS], caps: [u64; SHARDS]) -> u64 {
        caps.iter()
            .zip(shards)
            .map(|(cap, shard)| cap - shard.taken)
            .sum()
    }

    // --- the grant -----------------------------------------------------------

    #[test]
    fn a_regular_pass_grants_and_releases_one_interval() {
        let d = compute_pass(inputs(1000, 50), NOW);
        assert_eq!(d.budget, 500); // 50/s * 10 s
        assert_eq!(d.release, 500);
        assert_eq!(d.next_serving_counter, 1500);
        assert_eq!(d.last_open_pass_at, NOW);
    }

    #[test]
    fn the_caps_hand_out_exactly_the_budget_on_top_of_what_was_taken() {
        let mut i = inputs(1000, 5); // budget 50 + unused
        for (index, shard) in i.shards.iter_mut().enumerate() {
            *shard = ShardBudget {
                taken: 10 * index as u64,
                cap: 10 * index as u64,
            };
        }
        i.shards[3].cap += 7; // 7 unused
        let d = compute_pass(i, NOW);
        assert_eq!(d.budget, 57);
        assert_eq!(total_cap_headroom(i.shards, d.caps), 57);
        // As even as whole numbers allow: 5 each, and one more on the first 7.
        for (index, (cap, shard)) in d.caps.iter().zip(i.shards).enumerate() {
            let expected = if index < 7 { 6 } else { 5 };
            assert_eq!(cap - shard.taken, expected, "shard {index}");
        }
    }

    #[test]
    fn unused_budget_is_carried_but_never_past_the_burst() {
        // 300 unused + 500 owed fits under the 1000 burst.
        let d = compute_pass(with_unused(inputs(1000, 50), 300), NOW);
        assert_eq!(d.budget, 800);
        // 900 unused + 500 owed does not.
        let d = compute_pass(with_unused(inputs(1000, 50), 900), NOW);
        assert_eq!(d.budget, 1000);
    }

    #[test]
    fn an_idle_event_banks_no_more_than_the_burst() {
        // Nobody is in line for an hour of passes. The budget does not grow
        // past the burst, so the crowd that then arrives is metered from its
        // first second rather than walking through an hour of saved-up credit.
        let mut i = inputs(1, 50);
        i.queue_counter = 0;
        for step in 0..360u64 {
            let now = NOW + step * INTERVAL_SECS;
            let d = compute_pass(i, now);
            assert_eq!(d.release, 0);
            assert!(d.budget <= 1000);
            for (shard, cap) in i.shards.iter_mut().zip(d.caps) {
                shard.cap = cap;
            }
            i.last_open_pass_at = now;
        }
        assert_eq!(unused_budget(i.shards), 1000);
    }

    #[test]
    fn lowering_the_rate_shrinks_budget_already_granted() {
        // 1000 unused at 50/s; the operator drops to 5/s. The burst is now 100,
        // and that is all that stands.
        let d = compute_pass(with_unused(inputs(1000, 5), 1000), NOW);
        assert_eq!(d.budget, 100);
    }

    #[test]
    fn a_zero_rate_grants_and_releases_nothing() {
        let d = compute_pass(with_unused(inputs(1000, 0), 400), NOW);
        assert_eq!(d.budget, 0);
        assert_eq!(d.release, 0);
    }

    // --- the release ---------------------------------------------------------

    #[test]
    fn unused_budget_releases_extra_positions() {
        // 200 of last interval's admissions were not taken, so 200 more people
        // are invited to take them.
        let d = compute_pass(with_unused(inputs(1000, 50), 200), NOW);
        assert_eq!(d.release, 700);
    }

    #[test]
    fn the_correction_never_exceeds_what_is_owed() {
        // However much is unused, the cursor moves at most twice the target.
        let d = compute_pass(with_unused(inputs(1000, 50), 900), NOW);
        assert_eq!(d.release, 1000);
    }

    #[test]
    fn the_cursor_stops_at_the_end_of_the_line() {
        let mut i = inputs(1000, 50);
        i.queue_counter = 1200;
        let d = compute_pass(i, NOW);
        assert_eq!(d.next_serving_counter, 1201);
        assert_eq!(d.release, 201);
    }

    #[test]
    fn a_queue_counter_behind_the_cursor_never_rewinds_admission() {
        let mut i = inputs(1000, 50);
        i.queue_counter = 10;
        let d = compute_pass(i, NOW);
        assert_eq!(d.next_serving_counter, 1000);
        assert_eq!(d.release, 0);
    }

    // --- elapsed time (issue #236) -------------------------------------------

    #[test]
    fn a_fresh_event_is_owed_one_interval() {
        let mut i = inputs(0, 50);
        i.last_open_pass_at = 0;
        let d = compute_pass(i, 1_788_000_000);
        assert_eq!(d.budget, 500);
        assert_eq!(d.release, 500);
    }

    #[test]
    fn a_pass_after_a_hold_is_owed_one_interval_not_the_hold() {
        for gap in [STALE_OPEN_PASS_SECS, 600, 86_400] {
            let mut i = inputs(1000, 50);
            i.last_open_pass_at = NOW - gap;
            let d = compute_pass(i, NOW);
            assert_eq!(d.budget, 500, "gap {gap}");
            assert_eq!(d.release, 500, "gap {gap}");
        }
    }

    #[test]
    fn a_late_pass_is_owed_the_seconds_it_ran_late() {
        let mut i = inputs(1000, 50);
        i.last_open_pass_at = NOW - 15;
        assert_eq!(compute_pass(i, NOW).release, 750);
    }

    #[test]
    fn a_pass_in_the_same_second_as_the_last_grants_and_releases_nothing_new() {
        // An overlapping execution's pass lands on top of the regular one.
        let mut i = with_unused(inputs(1000, 50), 300);
        i.last_open_pass_at = NOW;
        let d = compute_pass(i, NOW);
        assert_eq!(d.release, 0);
        assert_eq!(d.budget, 300);
    }

    #[test]
    fn a_clock_behind_the_last_pass_is_owed_one_interval() {
        let mut i = inputs(1000, 50);
        i.last_open_pass_at = NOW + 10;
        assert_eq!(compute_pass(i, NOW).release, 500);
    }

    #[test]
    fn interleaved_executions_grant_what_one_cadence_would() {
        // Two executions five seconds out of phase run twelve passes in the
        // minute one would run six in. Everyone shows up and takes their
        // admission at once, so nothing is carried.
        let mut i = inputs(1, 50);
        i.last_open_pass_at = NOW;
        let (mut released, mut granted) = (0u64, 0u64);
        for step in 1..=12u64 {
            let now = NOW + step * 5;
            let d = compute_pass(i, now);
            released += d.release;
            granted += d.budget;
            for (shard, cap) in i.shards.iter_mut().zip(d.caps) {
                *shard = ShardBudget { taken: cap, cap };
            }
            i.serving_counter = d.next_serving_counter;
            i.last_open_pass_at = now;
        }
        assert_eq!(granted, 50 * 60);
        assert_eq!(released, 50 * 60);
    }

    // --- the loop ------------------------------------------------------------

    /// Runs `passes` regular passes in which a fixed share of the people
    /// released each pass show up, and take budget while there is any.
    /// Returns the admissions in each pass.
    fn simulate(show_per_thousand: u64, passes: u64) -> Vec<u64> {
        let rate = 50u32;
        let mut i = inputs(1, rate);
        let mut waiting = 0u64; // released, will show, not yet admitted
        let mut admitted = Vec::new();
        for step in 0..passes {
            let now = NOW + step * INTERVAL_SECS;
            let d = compute_pass(i, now);
            waiting += d.release * show_per_thousand / 1000;
            let take = waiting.min(d.budget);
            waiting -= take;
            admitted.push(take);
            // Spread the takes over the shards the way the caps were.
            let mut left = take;
            for (shard, cap) in i.shards.iter_mut().zip(d.caps) {
                let room = cap - shard.taken;
                let used = room.min(left);
                left -= used;
                *shard = ShardBudget {
                    taken: shard.taken + used,
                    cap,
                };
            }
            i.serving_counter = d.next_serving_counter;
            i.last_open_pass_at = now;
        }
        admitted
    }

    #[test]
    fn admissions_converge_on_the_target_despite_no_shows() {
        // F3.8: with 30% of released visitors never arriving, the origin still
        // receives the target rate once the correction has settled.
        let admitted = simulate(700, 60);
        for (pass, count) in admitted.iter().enumerate().skip(20) {
            assert!(
                (495..=505).contains(count),
                "pass {pass} admitted {count}, expected ~500"
            );
        }
    }

    #[test]
    fn no_window_admits_more_than_its_time_plus_the_burst() {
        for show in [1000, 700, 500, 300] {
            let admitted = simulate(show, 120);
            for (pass, count) in admitted.iter().enumerate() {
                assert!(*count <= 1000, "show {show} pass {pass}: {count}");
            }
            for start in 0..admitted.len() {
                let mut total = 0u64;
                for (width, count) in admitted.iter().skip(start).enumerate() {
                    total += count;
                    let allowed = 500 * (width as u64 + 1) + 500;
                    assert!(total <= allowed, "show {show} window {start}+{width}");
                }
            }
        }
    }

    // --- run_pass ------------------------------------------------------------

    #[derive(Default)]
    struct Writes {
        release: Option<PassDecision>,
        caps: Option<[u64; SHARDS]>,
    }

    struct FakeStore {
        state: ControllerState,
        lose_release: bool,
        writes: Mutex<Writes>,
    }

    impl FakeStore {
        fn new(state: ControllerState) -> Self {
            Self {
                state,
                lose_release: false,
                writes: Mutex::new(Writes::default()),
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
            decision: &PassDecision,
            _expected: u64,
        ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send {
            if self.lose_release {
                return std::future::ready(Ok(ReleaseOutcome::LostRace));
            }
            self.writes.lock().unwrap().release = Some(*decision);
            std::future::ready(Ok(ReleaseOutcome::Advanced))
        }

        fn write_caps(
            &self,
            _event_id: &str,
            caps: &[u64; SHARDS],
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.writes.lock().unwrap().caps = Some(*caps);
            std::future::ready(Ok(()))
        }
    }

    fn active_state() -> ControllerState {
        ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            inputs: inputs(20_000, 50),
        }
    }

    #[tokio::test]
    async fn an_open_pass_moves_the_cursor_and_grants_budget() {
        let store = FakeStore::new(active_state());
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 500 });
        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.release.unwrap().next_serving_counter, 20_500);
        assert_eq!(writes.caps.unwrap().iter().sum::<u64>(), 500);
    }

    #[tokio::test]
    async fn a_lost_release_race_releases_nobody_and_grants_nothing() {
        let mut store = FakeStore::new(active_state());
        store.lose_release = true;
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 0 });
        assert!(store.writes.lock().unwrap().caps.is_none());
    }

    #[tokio::test]
    async fn a_pass_is_a_noop_when_not_active() {
        let mut state = active_state();
        state.phase = Phase::PreQueue;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::NotActive(Phase::PreQueue));
        let writes = store.writes.lock().unwrap();
        assert!(writes.release.is_none() && writes.caps.is_none());
    }

    #[tokio::test]
    async fn a_pause_stops_the_cursor_and_the_budget() {
        let mut state = active_state();
        state.stored_control = StoredControl::Paused;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::Paused));
        let writes = store.writes.lock().unwrap();
        assert!(writes.release.is_none() && writes.caps.is_none());
    }

    #[tokio::test]
    async fn fail_open_holds_the_controller_until_the_window_lapses() {
        let mut state = active_state();
        state.fail_open_until = NOW + 5;
        let store = FakeStore::new(state);
        assert_eq!(
            run_pass(&store, "evt", NOW).await.unwrap(),
            PassOutcome::Held(AdmissionControl::FailOpen)
        );
        // The very next pass, past the deadline, runs.
        assert!(matches!(
            run_pass(&store, "evt", NOW + 10).await.unwrap(),
            PassOutcome::Ran { .. }
        ));
    }

    #[test]
    fn the_pass_outcome_round_trips_through_a_checkpoint() {
        for outcome in [
            PassOutcome::NotActive(Phase::Idle),
            PassOutcome::Held(AdmissionControl::Paused),
            PassOutcome::Ran { released: 42 },
        ] {
            let json = serde_json::to_string(&outcome).unwrap();
            assert_eq!(serde_json::from_str::<PassOutcome>(&json).unwrap(), outcome);
        }
    }

    // --- properties ----------------------------------------------------------

    use proptest::prelude::{Just, Strategy, any, prop_assert, prop_oneof, proptest};

    fn count() -> impl Strategy<Value = u64> {
        prop_oneof![0u64..1_000_000, Just(u64::MAX), any::<u64>()]
    }

    fn shards() -> impl Strategy<Value = [ShardBudget; SHARDS]> {
        proptest::array::uniform10(
            (count(), count()).prop_map(|(taken, cap)| ShardBudget { taken, cap }),
        )
    }

    proptest! {
        #[test]
        fn a_pass_never_panics_rewinds_or_exceeds_its_bounds(
            shards in shards(),
            serving in count(),
            queue in count(),
            rate in any::<u32>(),
            last in count(),
            now in count(),
        ) {
            let inputs = PassInputs {
                shards,
                serving_counter: serving,
                queue_counter: queue,
                target_rate: rate,
                last_open_pass_at: last,
            };
            let d = compute_pass(inputs, now);
            let interval = u64::from(rate).saturating_mul(INTERVAL_SECS);
            let burst = interval.saturating_mul(BURST_INTERVALS);

            // The cursor never rewinds, never passes the end of the line unless
            // it already had, and never moves more than twice what is owed.
            prop_assert!(d.next_serving_counter >= serving);
            prop_assert!(d.next_serving_counter <= serving.max(queue.saturating_add(1)));
            prop_assert!(d.release <= owed(rate, last, now).saturating_mul(2));

            // The budget never exceeds the burst, and no cap takes a shard
            // backwards.
            prop_assert!(d.budget <= burst);
            for (cap, shard) in d.caps.iter().zip(shards) {
                prop_assert!(*cap >= shard.taken);
            }
        }
    }
}
