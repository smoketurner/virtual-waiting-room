//! Outflow controller.
//!
//! The origin takes `origin_concurrency` visitors at once, so that is the most
//! sessions that may be live at any moment (issue #65). A session ends exactly
//! `session_secs` after its admission -- re-minting it keeps the same end
//! (issue #237) -- so the controller can count the live ones without anyone
//! reporting a departure: they are the admissions made in the last
//! `session_secs`. It keeps an [`AdmissionLog`] of the total admitted at each
//! pass for that.
//!
//! Admission is metered where it happens
//! ([`ADR-0039`](../../../docs/adr/0039-meter-admissions-not-the-cursor.md)).
//! `generate_token` takes one unit of an **admission budget** for every visitor
//! it admits, and refuses when there is none. Each pass, the controller:
//!
//! 1. sets the budget to the free slots, `origin_concurrency` minus the live
//!    sessions, so however many people turn up and whenever, no more than
//!    `origin_concurrency` sessions are ever live, and
//! 2. advances `serving_counter` by the slots that will have come free by the
//!    next pass, one pass early so the people released for them are already
//!    asking when the slots are granted, plus half of the slots that stayed
//!    free through the last pass -- the people released for those did not
//!    come, so the people behind them are released in their place. That is
//!    the whole no-show correction.
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

/// One pass's reading: at `at`, `admitted` visitors had been admitted in all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEntry {
    pub at: u64,
    pub admitted: u64,
}

/// What the controller carries from pass to pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AdmissionLog {
    /// The total admitted at each recent pass, oldest first: what live
    /// sessions are counted from.
    pub entries: Vec<LogEntry>,
    /// How many slots the cursor has been moved for, in all: the limit at the
    /// last pass plus every session due to have ended by the pass after it.
    pub provided: u64,
}

impl AdmissionLog {
    /// How many had been admitted by `horizon`: the latest entry at or before
    /// it, or nobody when there is none. Anyone admitted after that entry is
    /// counted as still live, so a gap in the log admits fewer, never more.
    #[must_use]
    pub fn admitted_by(&self, horizon: u64) -> u64 {
        let mut admitted = 0;
        for entry in &self.entries {
            if entry.at > horizon {
                break;
            }
            admitted = entry.admitted;
        }
        admitted
    }

    /// This log with `entry` recorded and `provided` carried, keeping only the
    /// entries a pass at or after `horizon` can need: those after it, and the
    /// latest one at or before it.
    #[must_use]
    pub fn recorded(&self, entry: LogEntry, horizon: u64, provided: u64) -> Self {
        let mut entries = self.entries.clone();
        entries.push(entry);
        entries.sort_by_key(|e| e.at);
        let keep_from = entries.iter().rposition(|e| e.at <= horizon).unwrap_or(0);
        entries.drain(..keep_from);
        Self { entries, provided }
    }
}

/// Inputs read for one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassInputs {
    /// The ten arrivals shards.
    pub shards: [ShardBudget; SHARDS],
    /// The current `serving_counter`.
    pub serving_counter: u64,
    /// The highest position ever issued. The live-join sequence ends here, and
    /// after an open it starts at the cohort size, so this is the end of the
    /// line however the positions were assigned.
    pub queue_counter: u64,
    /// How many sessions the origin takes at once.
    pub origin_concurrency: u32,
    /// How long a session lasts, the same value `generate_token` mints with.
    pub session_secs: u64,
    /// The total admitted at each recent pass.
    pub log: AdmissionLog,
    /// The whole epoch second, rounded down, at which the shards were read.
    /// Every admission they count began at or before it: a session starts at
    /// its request's whole second, and the request took its unit before the
    /// read. So this is the latest any of their sessions can end, less
    /// `session_secs`, and what the pass is logged at.
    pub shards_read_at: u64,
}

/// What one pass decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassDecision {
    /// Positions released: the distance the cursor moved, which is less than
    /// was computed when the end of the line is reached.
    pub release: u64,
    /// The new `serving_counter`.
    pub next_serving_counter: u64,
    /// Sessions live as this pass read the shards.
    pub live: u64,
    /// The admission budget after this pass, across all shards: the free
    /// slots.
    pub budget: u64,
    /// The cap to write on each shard: what it has taken, plus its share of
    /// `budget`.
    pub caps: [u64; SHARDS],
    /// The admission log with this pass recorded.
    pub log: AdmissionLog,
}

/// Budget granted and not yet taken, across all shards.
#[must_use]
pub fn unused_budget(shards: [ShardBudget; SHARDS]) -> u64 {
    shards
        .iter()
        .fold(0u64, |total, shard| total.saturating_add(shard.unused()))
}

/// Admissions recorded across all shards.
#[must_use]
pub fn admitted(shards: [ShardBudget; SHARDS]) -> u64 {
    shards
        .iter()
        .fold(0u64, |total, shard| total.saturating_add(shard.taken))
}

/// Decides one pass: how far the cursor moves and what budget stands after it.
///
/// A pure function of its inputs and `now`, so the cadence is testable with
/// time injected.
#[must_use]
pub fn compute_pass(inputs: &PassInputs, now: u64) -> PassDecision {
    let admitted = admitted(inputs.shards);
    let horizon = now.saturating_sub(inputs.session_secs);
    let live = admitted.saturating_sub(inputs.log.admitted_by(horizon));
    let limit = u64::from(inputs.origin_concurrency);
    let free = limit.saturating_sub(live);
    let unused = unused_budget(inputs.shards);

    // Every slot is released for once: the limit, and each session as it is
    // due to end, counted a pass ahead. A lowered limit releases nobody until
    // enough sessions have ended to make up the difference. A slot granted at
    // the last pass and still untaken had someone released for it who has not
    // come, so half as many again are released in their place: a slot nobody
    // takes is refilled within a few passes, while one whose person is merely
    // slow does not draw many more.
    let provided = limit.saturating_add(
        inputs
            .log
            .admitted_by(horizon.saturating_add(INTERVAL_SECS)),
    );
    let positions = provided
        .saturating_sub(inputs.log.provided)
        .saturating_add(unused.min(free).div_ceil(2));

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
        live,
        budget: free,
        caps: caps_for(inputs.shards, free),
        log: inputs.log.recorded(
            LogEntry {
                at: inputs.shards_read_at,
                admitted,
            },
            horizon,
            provided,
        ),
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
    /// Reads the event's `Counters` item, its ten arrivals shards and the
    /// admission log.
    fn read_state(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send;

    /// Advances `serving_counter` to `decision.next_serving_counter`, guarded
    /// on the cursor still being `expected_serving_counter` so two passes
    /// cannot both advance from the same read. A lost race is not an error.
    fn write_release(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<ReleaseOutcome, StoreError>> + Send;

    /// Replaces the admission log.
    fn write_log(
        &self,
        event_id: &str,
        log: &AdmissionLog,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

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
/// The cursor is written first, then the log, then the budget. A pass that
/// loses the cursor race writes neither: the winner computed the same grant
/// from the same state. Every later failure errs towards admitting fewer: a
/// log that misses a pass counts its admissions live for longer, and caps left
/// from the last pass grant only slots that were free then.
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

    let decision = compute_pass(&state.inputs, now);
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
    store.write_log(event_id, &decision.log).await?;
    store.write_caps(event_id, &decision.caps).await?;

    tracing::info!(
        event_id,
        released = decision.release,
        serving_counter = decision.next_serving_counter,
        live = decision.live,
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
    // Not a multiple of the interval, so sessions end between passes.
    const SESSION: u64 = 605;

    fn inputs(concurrency: u32) -> PassInputs {
        PassInputs {
            shards: [ShardBudget::default(); SHARDS],
            serving_counter: 0,
            queue_counter: u64::MAX - 1,
            origin_concurrency: concurrency,
            session_secs: SESSION,
            log: AdmissionLog::default(),
            shards_read_at: NOW,
        }
    }

    /// `taken` admissions so far, all on shard 0, with `unused` budget left on
    /// it from the last pass.
    fn with_taken(mut i: PassInputs, taken: u64, unused: u64) -> PassInputs {
        i.shards = [ShardBudget::default(); SHARDS];
        i.shards[0] = ShardBudget {
            taken,
            cap: taken + unused,
        };
        i
    }

    fn entry(at: u64, admitted: u64) -> LogEntry {
        LogEntry { at, admitted }
    }

    fn log_of(entries: Vec<LogEntry>) -> AdmissionLog {
        AdmissionLog {
            entries,
            provided: 0,
        }
    }

    // --- the admission log ---------------------------------------------------

    #[test]
    fn the_log_reads_the_latest_entry_at_or_before_the_horizon() {
        let log = log_of(vec![entry(100, 5), entry(110, 9), entry(120, 14)]);
        assert_eq!(log.admitted_by(99), 0);
        assert_eq!(log.admitted_by(100), 5);
        assert_eq!(log.admitted_by(119), 9);
        assert_eq!(log.admitted_by(10_000), 14);
    }

    #[test]
    fn recording_keeps_only_what_a_later_pass_can_need() {
        let log = log_of(vec![entry(100, 5), entry(110, 9), entry(120, 14)]);
        let next = log.recorded(entry(130, 20), 115, 0);
        assert_eq!(
            next.entries,
            vec![entry(110, 9), entry(120, 14), entry(130, 20)]
        );
        // Whatever horizon a later pass uses, it reads what it would have
        // read from the whole log.
        for horizon in 115..200 {
            let mut whole = log.entries.clone();
            whole.push(entry(130, 20));
            assert_eq!(
                next.admitted_by(horizon),
                log_of(whole).admitted_by(horizon)
            );
        }
    }

    #[test]
    fn an_entry_from_a_clock_behind_the_log_is_put_in_order() {
        let log = log_of(vec![entry(100, 5), entry(120, 14)]);
        let next = log.recorded(entry(110, 9), 0, 0);
        assert_eq!(
            next.entries,
            vec![entry(100, 5), entry(110, 9), entry(120, 14)]
        );
    }

    // --- one pass ------------------------------------------------------------

    #[test]
    fn the_first_pass_fills_every_slot() {
        let d = compute_pass(&inputs(100), NOW);
        assert_eq!(d.live, 0);
        assert_eq!(d.budget, 100);
        assert_eq!(d.release, 100);
        assert_eq!(d.caps.iter().sum::<u64>(), 100);
        assert_eq!(d.log.entries, vec![entry(NOW, 0)]);
        assert_eq!(d.log.provided, 100);
    }

    /// The log the pass before `NOW` left, having filled the origin.
    fn after_the_open() -> AdmissionLog {
        AdmissionLog {
            entries: vec![entry(NOW - 10, 0)],
            provided: 100,
        }
    }

    #[test]
    fn a_pass_is_logged_at_the_second_its_shards_were_read() {
        // A request that took its unit just before the read started its
        // session in that second, after the pass itself began.
        let mut i = with_taken(inputs(100), 7, 0);
        i.shards_read_at = NOW + 1;
        let d = compute_pass(&i, NOW);
        assert_eq!(d.log.entries, vec![entry(NOW + 1, 7)]);
    }

    #[test]
    fn a_full_origin_grants_nothing_and_releases_nobody() {
        let mut i = with_taken(inputs(100), 100, 0);
        i.log = after_the_open();
        let d = compute_pass(&i, NOW);
        assert_eq!(d.live, 100);
        assert_eq!(d.budget, 0);
        assert_eq!(d.release, 0);
    }

    #[test]
    fn a_session_frees_its_slot_once_its_length_has_passed() {
        // 100 admitted at the open, 30 more ten seconds later.
        let mut i = with_taken(inputs(100), 130, 0);
        let opened = NOW - SESSION;
        i.log = log_of(vec![entry(opened - 10, 0), entry(opened, 100)]);
        let d = compute_pass(&i, NOW);
        // The first 100 have ended; the 30 are live.
        assert_eq!(d.live, 30);
        assert_eq!(d.budget, 70);
        // A second earlier, they had not.
        let d = compute_pass(&i, NOW - 1);
        assert_eq!(d.live, 130);
        assert_eq!(d.budget, 0);
    }

    #[test]
    fn replacements_are_released_a_pass_before_sessions_end() {
        // 100 admitted at the open, which ends a session length later.
        let opened = NOW - SESSION;
        let mut i = with_taken(inputs(100), 100, 0);
        i.log = AdmissionLog {
            entries: vec![entry(opened - 10, 0), entry(opened, 100)],
            provided: 100,
        };
        // A pass before they end: nothing is granted yet, but the 100 behind
        // them are released.
        let before = compute_pass(&i, NOW - INTERVAL_SECS);
        assert_eq!((before.live, before.budget, before.release), (100, 0, 100));
        // When they end, the slots are granted and nobody more is released.
        i.log = before.log;
        let at = compute_pass(&i, NOW);
        assert_eq!((at.live, at.budget, at.release), (0, 100, 0));
    }

    #[test]
    fn a_slot_left_untaken_releases_a_replacement() {
        // 60 admitted and live, 40 slots granted last pass and still free.
        let mut i = with_taken(inputs(100), 60, 40);
        i.log = after_the_open();
        let d = compute_pass(&i, NOW);
        assert_eq!(d.budget, 40);
        assert_eq!(d.release, 20);
        // One slot nobody takes still draws someone.
        let mut i = with_taken(inputs(100), 99, 1);
        i.log = after_the_open();
        assert_eq!(compute_pass(&i, NOW).release, 1);
    }

    #[test]
    fn lowering_the_limit_below_the_live_sessions_admits_nobody() {
        let mut i = with_taken(inputs(100), 80, 20);
        i.log = after_the_open();
        i.origin_concurrency = 50;
        let d = compute_pass(&i, NOW);
        assert_eq!(d.budget, 0);
        assert_eq!(d.release, 0);
        for (cap, shard) in d.caps.iter().zip(i.shards) {
            assert_eq!(*cap, shard.taken);
        }
    }

    #[test]
    fn a_zero_limit_grants_and_releases_nothing() {
        let d = compute_pass(&inputs(0), NOW);
        assert_eq!((d.budget, d.release), (0, 0));
    }

    #[test]
    fn the_caps_hand_out_exactly_the_budget_on_top_of_what_was_taken() {
        let mut i = inputs(57);
        for (index, shard) in i.shards.iter_mut().enumerate() {
            *shard = ShardBudget {
                taken: 10 * index as u64,
                cap: 10 * index as u64,
            };
        }
        i.origin_concurrency = 57 + 450;
        let d = compute_pass(&i, NOW);
        assert_eq!(d.budget, 57);
        for (index, (cap, shard)) in d.caps.iter().zip(i.shards).enumerate() {
            let expected = 5 + u64::from(index < 7);
            assert_eq!(cap - shard.taken, expected, "shard {index}");
        }
    }

    #[test]
    fn the_cursor_stops_at_the_end_of_the_line() {
        let mut i = inputs(100);
        i.queue_counter = 40;
        let d = compute_pass(&i, NOW);
        assert_eq!(d.next_serving_counter, 41);
        assert_eq!(d.release, 41);
    }

    #[test]
    fn a_queue_counter_behind_the_cursor_never_rewinds_admission() {
        let mut i = inputs(100);
        i.serving_counter = 500;
        i.queue_counter = 10;
        let d = compute_pass(&i, NOW);
        assert_eq!(d.next_serving_counter, 500);
        assert_eq!(d.release, 0);
    }

    // --- the loop ------------------------------------------------------------

    /// Runs `passes` regular passes in which a fixed share of the people
    /// released show up, at the next pass, and take a slot while there is one.
    /// Returns the live sessions as each pass's admissions land.
    fn simulate(concurrency: u32, show_per_thousand: u64, passes: u64) -> (Vec<u64>, u64) {
        let mut i = inputs(concurrency);
        let mut waiting = 0u64;
        let mut admissions: Vec<(u64, u64)> = Vec::new();
        let mut live_after = Vec::new();
        for step in 0..passes {
            let now = NOW + step * INTERVAL_SECS;
            i.shards_read_at = now;
            let d = compute_pass(&i, now);
            let take = waiting.min(d.budget);
            waiting -= take;
            waiting += d.release * show_per_thousand / 1000;
            // Taken during the interval and counted by the next pass: at the
            // latest, the moment that pass reads them, which is the case that
            // would let a session outlive the controller's count.
            admissions.push((now + INTERVAL_SECS, take));
            let mut left = take;
            for (shard, cap) in i.shards.iter_mut().zip(d.caps) {
                let used = (cap - shard.taken).min(left);
                left -= used;
                *shard = ShardBudget {
                    taken: shard.taken + used,
                    cap,
                };
            }
            i.serving_counter = d.next_serving_counter;
            i.log = d.log;
            // Live the moment this pass's takes land, right after the grant:
            // every earlier session that has not yet run its length, each
            // taken as late as its log entry allows, plus the takes.
            let running: u64 = admissions
                .iter()
                .rev()
                .skip(1)
                .filter(|(at, _)| at + SESSION > now)
                .map(|(_, n)| n)
                .sum();
            live_after.push(running + take);
        }
        (live_after, i.serving_counter)
    }

    #[test]
    fn no_more_sessions_are_ever_live_than_the_origin_takes() {
        for show in [1000, 700, 300, 50] {
            let (live, _) = simulate(100, show, 600);
            for (pass, n) in live.iter().enumerate() {
                assert!(*n <= 100, "show {show} pass {pass}: {n} live");
            }
        }
    }

    #[test]
    fn the_origin_stays_nearly_full_despite_no_shows() {
        // A slot is seen free up to a pass after its session ends, and with
        // no-shows the people released for it are topped up over a few passes,
        // so the origin runs a little under its limit at each turnover.
        for (show, at_least) in [(1000, 95), (700, 90)] {
            let (live, _) = simulate(100, show, 600);
            let settled = &live[30..];
            let total: u64 = settled.iter().sum();
            assert!(
                total >= at_least * settled.len() as u64,
                "show {show}: {total} over {} passes",
                settled.len()
            );
        }
    }

    #[test]
    fn sessions_ending_are_replaced_at_their_rate() {
        // Everyone shows: the origin is refilled as each session ends, so the
        // cursor moves concurrency per session length, plus the open's fill.
        let passes = 600;
        let (_, served) = simulate(100, 1000, passes);
        let sessions = passes * INTERVAL_SECS / (SESSION + INTERVAL_SECS);
        assert!(served >= 100 * sessions, "{served}");
        assert!(served <= 100 * (sessions + 2), "{served}");
    }

    // --- run_pass ------------------------------------------------------------

    #[derive(Default)]
    struct Writes {
        release: Option<PassDecision>,
        log: Option<AdmissionLog>,
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
            self.writes.lock().unwrap().release = Some(decision.clone());
            std::future::ready(Ok(ReleaseOutcome::Advanced))
        }

        fn write_log(
            &self,
            _event_id: &str,
            log: &AdmissionLog,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.writes.lock().unwrap().log = Some(log.clone());
            std::future::ready(Ok(()))
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
            inputs: inputs(100),
        }
    }

    #[tokio::test]
    async fn an_open_pass_moves_the_cursor_records_the_pass_and_grants_the_free_slots() {
        let store = FakeStore::new(active_state());
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 100 });
        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.release.as_ref().unwrap().next_serving_counter, 100);
        assert_eq!(writes.log.as_ref().unwrap().entries, vec![entry(NOW, 0)]);
        assert_eq!(writes.caps.unwrap().iter().sum::<u64>(), 100);
    }

    #[tokio::test]
    async fn a_lost_release_race_releases_nobody_and_writes_nothing_else() {
        let mut store = FakeStore::new(active_state());
        store.lose_release = true;
        let outcome = run_pass(&store, "evt", NOW).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { released: 0 });
        let writes = store.writes.lock().unwrap();
        assert!(writes.log.is_none() && writes.caps.is_none());
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

    fn log() -> impl Strategy<Value = AdmissionLog> {
        proptest::collection::vec((count(), count()), 0..8).prop_map(|mut pairs| {
            pairs.sort_unstable();
            log_of(
                pairs
                    .into_iter()
                    .map(|(at, admitted)| LogEntry { at, admitted })
                    .collect(),
            )
        })
    }

    proptest! {
        #[test]
        fn a_pass_never_panics_rewinds_or_grants_past_the_limit(
            shards in shards(),
            serving in count(),
            queue in count(),
            concurrency in any::<u32>(),
            session in count(),
            log in log(),
            now in count(),
            read_at in count(),
        ) {
            let inputs = PassInputs {
                shards,
                serving_counter: serving,
                queue_counter: queue,
                origin_concurrency: concurrency,
                session_secs: session,
                log,
                shards_read_at: read_at,
            };
            let d = compute_pass(&inputs, now);

            prop_assert!(d.next_serving_counter >= serving);
            prop_assert!(d.next_serving_counter <= serving.max(queue.saturating_add(1)));

            // Live sessions plus the budget never exceed the limit, and no cap
            // takes a shard backwards.
            prop_assert!(d.budget <= u64::from(concurrency).saturating_sub(d.live));
            for (cap, shard) in d.caps.iter().zip(shards) {
                prop_assert!(*cap >= shard.taken);
            }
            prop_assert!(d.log.entries.windows(2).all(|w| w[0].at <= w[1].at));
        }
    }
}
