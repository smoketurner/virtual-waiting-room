//! Outflow controller.
//!
//! The origin takes `origin_concurrency` visitors at once: that many spots.
//! A session ends exactly `session_secs` after the visitor was let in --
//! re-minting it keeps the same end (issue #237) -- so the controller can count
//! the spots in use without anyone reporting a departure: they are the people
//! let in during the last `session_secs`. It keeps an [`AdmissionLog`] of the
//! total let in at each pass for that.
//!
//! Each pass, the controller counts the free spots -- `origin_concurrency`,
//! less the spots in use, less the spots still held for visitors whose turn
//! it already called -- and calls that many more places in line, moving now
//! serving past them. It holds one spot for each of them for
//! [`wr_common::TURN_HOLD`], on that call's own shelf of arrivals counters,
//! so nobody else can take it. A spot not taken in time stops being held and
//! is called for the next place in line: that is what replaces a visitor who
//! never came.
//!
//! The controller expires nothing. A place in line lives until `DynamoDB` TTL
//! reclaims its row.
//!
//! A pass runs only when the event is `Active` **and** the operator's
//! [`AdmissionControl`] is `Open`.
//!
//! All arithmetic here is checked or saturating: the release profile has no
//! overflow checks, so a bare subtraction that underflows would wrap to a huge
//! value and let in a damaging burst.

use jiff::{SignedDuration, Timestamp};
use std::future::Future;

use serde::{Deserialize, Serialize};
use wr_common::{
    AdmissionControl, Call, Phase, SHARDS, SHELVES, Shelf, StoredControl, hold_ends, resolve,
};

pub mod dynamo;

/// The controller interval in seconds. The `EventBridge` Scheduler `rate()`
/// minimum is one minute, so the schedule fires `rate(1 minute)` and the
/// handler runs [`PASSES_PER_INVOKE`] passes this many seconds apart.
pub const INTERVAL: SignedDuration = SignedDuration::from_secs(10);

/// Passes per durable execution. `INTERVAL_SECS * PASSES_PER_INVOKE == 60`, so
/// one execution started by the `rate(1 minute)` schedule covers a full minute.
pub const PASSES_PER_INVOKE: u32 = 6;

/// One arrivals counter as the controller reads it: how many visitors took a
/// spot from it, and how many it may be taken up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardBudget {
    /// Spots taken from this counter since the event began.
    pub taken: u64,
    /// The count this counter may be taken up to. A counter never given one
    /// reads as zero.
    pub cap: u64,
}

impl ShardBudget {
    /// Spots on this counter not yet taken.
    #[must_use]
    pub const fn untaken(self) -> u64 {
        self.cap.saturating_sub(self.taken)
    }
}

/// Every shelf's ten arrivals counters.
pub type Shelves = [[ShardBudget; SHARDS]; SHELVES];

/// One pass's reading: at `at`, `admitted` visitors had been let in in all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEntry {
    pub at: Timestamp,
    pub admitted: u64,
}

/// What the controller carries from pass to pass: the total let in at each
/// recent pass, oldest first, which the spots in use are counted from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AdmissionLog {
    pub entries: Vec<LogEntry>,
}

impl AdmissionLog {
    /// How many had been let in by `horizon`: the latest entry at or before
    /// it, or nobody when there is none. Anyone let in after that entry is
    /// counted as still on the site, so a gap in the log lets in fewer, never
    /// more.
    #[must_use]
    pub fn admitted_by(&self, horizon: Timestamp) -> u64 {
        let mut admitted = 0;
        for entry in &self.entries {
            if entry.at > horizon {
                break;
            }
            admitted = entry.admitted;
        }
        admitted
    }

    /// This log with `entry` recorded, keeping only the entries a pass at or
    /// after `horizon` can need: those after it, and the latest one at or
    /// before it.
    #[must_use]
    pub fn recorded(&self, entry: LogEntry, horizon: Timestamp) -> Self {
        let mut entries = self.entries.clone();
        entries.push(entry);
        entries.sort_by_key(|e| e.at);
        let keep_from = entries.iter().rposition(|e| e.at <= horizon).unwrap_or(0);
        entries.drain(..keep_from);
        Self { entries }
    }
}

/// Inputs read for one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassInputs {
    /// Every shelf's arrivals counters.
    pub shelves: Shelves,
    /// Now serving: every place in line below it has been called.
    pub serving_counter: u64,
    /// The highest place in line ever given out. The live-join sequence ends
    /// here, and after an open it starts at the cohort size, so this is the
    /// end of the line however the places were given out.
    pub queue_counter: u64,
    /// How many visitors the origin takes at once: its spots.
    pub origin_concurrency: u32,
    /// How long a session lasts, the same value `generate_token` mints with.
    pub session: SignedDuration,
    /// The total let in at each recent pass.
    pub log: AdmissionLog,
    /// When the counters were read, rounded down to the whole second. Every
    /// visitor they count was let in at or before it: a session starts at its
    /// request's time, and the request took its spot before the read. So this
    /// is the latest any of their sessions can end, less `session`, and what
    /// the pass is logged at.
    pub shards_read_at: Timestamp,
    /// The recent calls, oldest first.
    pub calls: Vec<Call>,
    /// When the draw closed, if the event has a scheduled start: a call made
    /// soon after it holds its spots until every waiting page has had time to
    /// learn its place in line.
    pub draw_cutoff: Option<Timestamp>,
}

/// A new call's spots: where they are held, and each counter's new cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCall {
    pub shelf: Shelf,
    pub caps: [u64; SHARDS],
}

/// What one pass decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassDecision {
    /// Places in line this pass called: the free spots, or fewer when the end
    /// of the line is reached.
    pub called: u64,
    /// The new now serving.
    pub next_serving_counter: u64,
    /// Spots in use as this pass read the counters.
    pub in_use: u64,
    /// Spots still held for visitors whose turn an earlier call called.
    pub held: u64,
    /// The spots this pass called places for.
    pub free: u64,
    /// The call this pass made, if it called anyone.
    pub call: Option<NewCall>,
    /// The calls to keep: those still counted, and this pass's.
    pub calls: Vec<Call>,
    /// The admission log with this pass recorded.
    pub log: AdmissionLog,
}

/// Everyone let in, across every counter on every shelf.
#[must_use]
pub fn admitted(shelves: &Shelves) -> u64 {
    shelves
        .iter()
        .flatten()
        .fold(0u64, |total, shard| total.saturating_add(shard.taken))
}

/// Spots on one shelf not yet taken.
#[must_use]
pub fn untaken(shelf: &[ShardBudget; SHARDS]) -> u64 {
    shelf
        .iter()
        .fold(0u64, |total, shard| total.saturating_add(shard.untaken()))
}

/// Decides one pass: how many places to call, and where their spots are held.
///
/// A pure function of its inputs and `now`, so the cadence is testable with
/// time injected.
#[must_use]
pub fn compute_pass(inputs: &PassInputs, now: Timestamp) -> PassDecision {
    let now = Timestamp::from_second(now.as_second()).unwrap_or(now);
    let admitted = admitted(&inputs.shelves);
    // Saturating at jiff's range, as the counts saturate at u64's.
    let horizon = now.checked_sub(inputs.session).unwrap_or(Timestamp::MIN);
    let in_use = admitted.saturating_sub(inputs.log.admitted_by(horizon));

    // A call's untaken spots stay held through its hold and one pass more.
    let mut calls: Vec<Call> = inputs
        .calls
        .iter()
        .filter(|c| c.is_counted(now))
        .copied()
        .collect();
    let held = calls.iter().fold(0u64, |total, call| {
        let shelf = inputs.shelves.get(call.shelf.index()).map_or(0, untaken);
        total.saturating_add(shelf)
    });

    let limit = u64::from(inputs.origin_concurrency);
    let free = limit.saturating_sub(in_use).saturating_sub(held);

    // Now serving is exclusive -- a place in line is called once it is below
    // it -- and queue_counter is the highest place ever given out, so one past
    // it calls everyone in line and nobody who is not.
    let line_end = inputs.queue_counter.saturating_add(1);
    let waiting = line_end.saturating_sub(inputs.serving_counter);

    // The next shelf in turn. One a counted call still uses is never reused:
    // that pass calls nobody rather than mix two calls' spots.
    let shelf = inputs
        .calls
        .last()
        .map_or_else(|| Shelf::all().next(), |c| Some(c.shelf.next()));
    let shelf_free = shelf.filter(|s| calls.iter().all(|c| c.shelf != *s));

    let (called, call) = match shelf_free {
        Some(shelf) if free.min(waiting) > 0 => {
            let called = free.min(waiting);
            let counters = inputs
                .shelves
                .get(shelf.index())
                .copied()
                .unwrap_or_default();
            calls.push(Call {
                first: inputs.serving_counter,
                until: hold_ends(now, inputs.draw_cutoff),
                shelf,
            });
            (
                called,
                Some(NewCall {
                    shelf,
                    caps: caps_for(counters, called),
                }),
            )
        }
        Some(_) | None => (0, None),
    };

    PassDecision {
        called,
        next_serving_counter: inputs.serving_counter.saturating_add(called),
        in_use,
        held,
        free,
        call,
        calls,
        log: inputs.log.recorded(
            LogEntry {
                at: inputs.shards_read_at,
                admitted,
            },
            horizon,
        ),
    }
}

/// Spreads `spots` across a shelf's counters as evenly as whole numbers allow,
/// on top of what each has already had taken.
///
/// `generate_token` draws a counter at random for each visitor, so an even
/// split is the one that matches where the takes will land. The shares sum to
/// `spots` exactly.
fn caps_for(shelf: [ShardBudget; SHARDS], spots: u64) -> [u64; SHARDS] {
    let count = SHARDS as u64;
    let share = spots.checked_div(count).unwrap_or(0);
    let remainder = spots.checked_rem(count).unwrap_or(0);
    let mut caps = [0u64; SHARDS];
    for (index, (cap, shard)) in caps.iter_mut().zip(shelf).enumerate() {
        let extra = u64::from((index as u64) < remainder);
        *cap = shard.taken.saturating_add(share).saturating_add(extra);
    }
    caps
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("controller store error: {0}")]
pub struct StoreError(pub String);

/// The outcome of a guarded [`Store::write_call`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOutcome {
    /// The `UpdateItem` landed: now serving is `decision.next_serving_counter`
    /// and the calls are `decision.calls`.
    Called,
    /// Another pass moved now serving first; this one's write did not land
    /// and it called nobody.
    LostRace,
}

/// The current state the controller needs before it acts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerState {
    pub phase: Phase,
    /// The operator's stored admission override (issue #71: never `FailOpen`,
    /// which is resolved from `fail_open_until` instead).
    pub stored_control: StoredControl,
    /// The fail-open deadline; `None` means no window is in force.
    pub fail_open_until: Option<Timestamp>,
    pub inputs: PassInputs,
}

/// The persistence port the controller drives. A trait seam so the pipeline runs
/// AWS-free in tests (the `dynamo` module supplies the live implementation).
pub trait Store {
    /// Reads the event's `Counters` item, every shelf's arrivals counters and
    /// the admission log.
    fn read_state(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<ControllerState, StoreError>> + Send;

    /// Moves now serving to `decision.next_serving_counter` and stores
    /// `decision.calls`, guarded on now serving still being
    /// `expected_serving_counter` so two passes cannot both call from the same
    /// read. A lost race is not an error.
    fn write_call(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> impl Future<Output = Result<CallOutcome, StoreError>> + Send;

    /// Replaces the admission log.
    fn write_log(
        &self,
        event_id: &str,
        log: &AdmissionLog,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Writes each counter's cap on one shelf. The caps are absolute, so
    /// writing them twice holds nothing twice.
    fn write_caps(
        &self,
        event_id: &str,
        shelf: Shelf,
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
    /// The controller ran and called `called` places in line. `0` when the
    /// call lost its race against a concurrent pass.
    Ran { called: u64 },
}

/// Runs one controller pass for the event: read state, gate on `Active` and on
/// the operator's admission override, then call the next places in line and
/// hold their spots.
///
/// The call is written first, then the log, then the held spots. A pass that
/// loses the race writes neither: the winner computed the same call from the
/// same state. Every later failure errs towards letting in fewer: a log that
/// misses a pass counts its visitors on the site for longer, and a call whose
/// caps were not written holds fewer spots than it called.
///
/// # Errors
///
/// Returns [`StoreError`] if any read or write fails.
pub async fn run_pass<S: Store>(
    store: &S,
    event_id: &str,
    now: Timestamp,
) -> Result<PassOutcome, StoreError> {
    let state = store.read_state(event_id).await?;
    if state.phase != Phase::Active {
        tracing::debug!(event_id, phase = ?state.phase, "controller skipped: not active");
        return Ok(PassOutcome::NotActive(state.phase));
    }

    // Any control other than Open returns the whole pass: a hold means nobody
    // is let in, not that turns accrue silently. Under fail-open the waiting
    // room is bypassed, so a call would meter nothing.
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
        .write_call(event_id, &decision, state.inputs.serving_counter)
        .await?
    {
        CallOutcome::Called => {}
        CallOutcome::LostRace => {
            tracing::info!(
                event_id,
                stale_serving_counter = state.inputs.serving_counter,
                "controller pass called nobody: lost the race"
            );
            return Ok(PassOutcome::Ran { called: 0 });
        }
    }
    store.write_log(event_id, &decision.log).await?;
    if let Some(call) = &decision.call {
        store.write_caps(event_id, call.shelf, &call.caps).await?;
    }

    tracing::info!(
        event_id,
        called = decision.called,
        serving_counter = decision.next_serving_counter,
        in_use = decision.in_use,
        held = decision.held,
        free = decision.free,
        "controller pass"
    );
    Ok(PassOutcome::Ran {
        called: decision.called,
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use wr_common::{TURN_HOLD, TURN_MARGIN};

    use super::*;

    const NOW: i64 = 1_000_000;
    // Not a multiple of the interval, so sessions end between passes.
    const SESSION: i64 = 605;
    const INTERVAL_SECS: i64 = INTERVAL.as_secs();

    fn at(secs: i64) -> Timestamp {
        Timestamp::constant(secs, 0)
    }

    fn shelf(i: usize) -> Shelf {
        Shelf::new(i).unwrap()
    }

    fn inputs(concurrency: u32) -> PassInputs {
        PassInputs {
            shelves: [[ShardBudget::default(); SHARDS]; SHELVES],
            serving_counter: 0,
            queue_counter: u64::MAX - 1,
            origin_concurrency: concurrency,
            session: SignedDuration::from_secs(SESSION),
            log: AdmissionLog::default(),
            shards_read_at: at(NOW),
            calls: Vec::new(),
            draw_cutoff: None,
        }
    }

    /// `taken` let in so far from shelf `on`, with `untaken` spots still held
    /// there.
    fn with_taken(mut i: PassInputs, on: usize, taken: u64, untaken: u64) -> PassInputs {
        i.shelves[on][0] = ShardBudget {
            taken,
            cap: taken + untaken,
        };
        i
    }

    fn entry(secs: i64, admitted: u64) -> LogEntry {
        LogEntry {
            at: at(secs),
            admitted,
        }
    }

    fn log_of(entries: Vec<LogEntry>) -> AdmissionLog {
        AdmissionLog { entries }
    }

    /// The log the pass before `NOW` left, having called the open's places.
    fn after_the_open() -> AdmissionLog {
        log_of(vec![entry(NOW - 10, 0)])
    }

    /// A call made at `secs`, long after the draw closed.
    fn call(first: u64, secs: i64, on: usize) -> Call {
        Call {
            first,
            until: at(secs) + TURN_HOLD,
            shelf: shelf(on),
        }
    }

    // --- the admission log ---------------------------------------------------

    #[test]
    fn the_log_reads_the_latest_entry_at_or_before_the_horizon() {
        let log = log_of(vec![entry(100, 5), entry(110, 9), entry(120, 14)]);
        assert_eq!(log.admitted_by(at(99)), 0);
        assert_eq!(log.admitted_by(at(100)), 5);
        assert_eq!(log.admitted_by(at(119)), 9);
        assert_eq!(log.admitted_by(at(10_000)), 14);
    }

    #[test]
    fn recording_keeps_only_what_a_later_pass_can_need() {
        let log = log_of(vec![entry(100, 5), entry(110, 9), entry(120, 14)]);
        let next = log.recorded(entry(130, 20), at(115));
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
                next.admitted_by(at(horizon)),
                log_of(whole).admitted_by(at(horizon))
            );
        }
    }

    #[test]
    fn an_entry_from_a_clock_behind_the_log_is_put_in_order() {
        let log = log_of(vec![entry(100, 5), entry(120, 14)]);
        let next = log.recorded(entry(110, 9), at(0));
        assert_eq!(
            next.entries,
            vec![entry(100, 5), entry(110, 9), entry(120, 14)]
        );
    }

    // --- one pass ------------------------------------------------------------

    #[test]
    fn the_first_pass_calls_one_place_for_every_spot() {
        let d = compute_pass(&inputs(100), at(NOW));
        assert_eq!((d.in_use, d.held, d.free, d.called), (0, 0, 100, 100));
        assert_eq!(d.next_serving_counter, 100);
        let new = d.call.unwrap();
        assert_eq!(new.shelf, shelf(0));
        assert_eq!(new.caps.iter().sum::<u64>(), 100);
        assert_eq!(d.calls, vec![call(0, NOW, 0)]);
        assert_eq!(d.log.entries, vec![entry(NOW, 0)]);
    }

    #[test]
    fn a_pass_is_logged_at_the_second_its_counters_were_read() {
        // A request that took its spot just before the read was let in in
        // that second, after the pass itself began.
        let mut i = with_taken(inputs(100), 0, 7, 0);
        i.shards_read_at = at(NOW + 1);
        let d = compute_pass(&i, at(NOW));
        assert_eq!(d.log.entries, vec![entry(NOW + 1, 7)]);
    }

    #[test]
    fn a_full_origin_calls_nobody() {
        let mut i = with_taken(inputs(100), 0, 100, 0);
        i.log = after_the_open();
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.in_use, d.free, d.called), (100, 0, 0));
        assert_eq!(d.call, None);
    }

    #[test]
    fn a_session_frees_its_spot_once_its_length_has_passed() {
        // 100 let in at the open, 30 more ten seconds later.
        let mut i = with_taken(inputs(100), 0, 130, 0);
        let opened = NOW - SESSION;
        i.log = log_of(vec![entry(opened - 10, 0), entry(opened, 100)]);
        let d = compute_pass(&i, at(NOW));
        // The first 100 have ended; the 30 are on the site.
        assert_eq!((d.in_use, d.free), (30, 70));
        // A second earlier, they had not.
        let d = compute_pass(&i, at(NOW - 1));
        assert_eq!((d.in_use, d.free), (130, 0));
    }

    #[test]
    fn spots_held_for_an_earlier_call_are_not_called_again() {
        // The last pass called 100; 60 took their turn, 40 have not yet.
        let mut i = with_taken(inputs(100), 0, 60, 40);
        i.log = after_the_open();
        i.serving_counter = 100;
        i.calls = vec![call(0, NOW - INTERVAL_SECS, 0)];
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.in_use, d.held, d.free, d.called), (60, 40, 0, 0));
        assert_eq!(d.calls, i.calls);
    }

    #[test]
    fn a_missed_turn_frees_its_spot_one_pass_after_the_hold() {
        let mut i = with_taken(inputs(100), 0, 60, 40);
        i.log = after_the_open();
        i.serving_counter = 100;
        let hold = TURN_HOLD.as_secs();
        let margin = TURN_MARGIN.as_secs();
        // Still counted through the hold and its margin.
        i.calls = vec![call(0, NOW - hold, 0)];
        assert_eq!(compute_pass(&i, at(NOW)).called, 0);
        // Then its 40 untaken spots are called for the next 40 places, on the
        // next shelf.
        i.calls = vec![call(0, NOW - hold - margin, 0)];
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.held, d.free, d.called), (0, 40, 40));
        assert_eq!(d.next_serving_counter, 140);
        assert_eq!(d.calls, vec![call(100, NOW, 1)]);
        assert_eq!(d.call.unwrap().caps.iter().sum::<u64>(), 40);
    }

    #[test]
    fn a_call_soon_after_the_draw_closes_holds_until_every_page_knows_its_place() {
        let mut i = inputs(100);
        i.draw_cutoff = Some(at(NOW - 30));
        let d = compute_pass(&i, at(NOW));
        assert_eq!(
            d.calls[0].until,
            at(NOW - 30) + wr_common::OPEN_SETTLE + TURN_HOLD
        );
        // Long after it, a call holds for the turn alone.
        i.draw_cutoff = Some(at(NOW - 3_600));
        let d = compute_pass(&i, at(NOW));
        assert_eq!(d.calls[0].until, at(NOW) + TURN_HOLD);
    }

    #[test]
    fn a_shelf_a_counted_call_still_uses_is_never_reused() {
        // The shelf after the last is the first, and a call on it is still
        // counted.
        let mut i = inputs(100);
        i.serving_counter = 10;
        i.calls = vec![call(0, NOW - 20, 0), call(5, NOW - 10, SHELVES - 1)];
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.free, d.called), (100, 0));
        assert_eq!(d.call, None);
    }

    #[test]
    fn lowering_the_limit_below_the_spots_in_use_calls_nobody() {
        let mut i = with_taken(inputs(100), 0, 80, 0);
        i.log = after_the_open();
        i.origin_concurrency = 50;
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.free, d.called), (0, 0));
    }

    #[test]
    fn a_zero_limit_calls_nobody() {
        let d = compute_pass(&inputs(0), at(NOW));
        assert_eq!((d.free, d.called), (0, 0));
        assert_eq!(d.call, None);
    }

    #[test]
    fn the_caps_hold_exactly_the_called_spots_on_top_of_what_was_taken() {
        let mut i = inputs(57);
        for (index, shard) in i.shelves[0].iter_mut().enumerate() {
            *shard = ShardBudget {
                taken: 10 * index as u64,
                cap: 10 * index as u64,
            };
        }
        i.origin_concurrency = 57 + 450;
        let d = compute_pass(&i, at(NOW));
        assert_eq!(d.called, 57);
        let new = d.call.unwrap();
        for (index, (cap, shard)) in new.caps.iter().zip(i.shelves[0]).enumerate() {
            let expected = 5 + u64::from(index < 7);
            assert_eq!(cap - shard.taken, expected, "shard {index}");
        }
    }

    #[test]
    fn now_serving_stops_at_the_end_of_the_line() {
        let mut i = inputs(100);
        i.queue_counter = 40;
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.next_serving_counter, d.called), (41, 41));
    }

    #[test]
    fn a_queue_counter_behind_now_serving_never_moves_it_back() {
        let mut i = inputs(100);
        i.serving_counter = 500;
        i.queue_counter = 10;
        let d = compute_pass(&i, at(NOW));
        assert_eq!((d.next_serving_counter, d.called), (500, 0));
    }

    #[test]
    fn a_ring_of_shelves_outlasts_every_call_it_could_hold() {
        // One call a pass: a shelf comes round again only once the call it
        // last held is no longer counted.
        let reuse_after = INTERVAL * i32::try_from(SHELVES).unwrap();
        assert!(reuse_after > wr_common::OPEN_SETTLE + TURN_HOLD + TURN_MARGIN);
    }

    // --- the loop ------------------------------------------------------------

    /// Runs `passes` passes in which a fixed share of each call's places show
    /// up and take their turn at the next pass. Returns the spots in use as
    /// each pass's takes land, and now serving at the end.
    fn simulate(concurrency: u32, show_per_thousand: u64, passes: i64) -> (Vec<u64>, u64) {
        let mut i = inputs(concurrency);
        // (shelf, takes due) from the last pass's call.
        let mut due: Option<(usize, u64)> = None;
        let mut admissions: Vec<(i64, u64)> = Vec::new();
        let mut in_use_after = Vec::new();
        for step in 0..passes {
            let now = NOW + step * INTERVAL_SECS;
            // Last pass's callers who showed take their spots, from their own
            // shelf, before this pass reads the counters.
            let mut take = 0;
            if let Some((on, mut left)) = due.take() {
                for shard in &mut i.shelves[on] {
                    let used = shard.untaken().min(left);
                    shard.taken += used;
                    left -= used;
                    take += used;
                }
            }
            admissions.push((now, take));
            i.shards_read_at = at(now);
            let d = compute_pass(&i, at(now));
            if let Some(new) = &d.call {
                for (shard, cap) in i.shelves[new.shelf.index()].iter_mut().zip(new.caps) {
                    shard.cap = cap;
                }
                due = Some((new.shelf.index(), d.called * show_per_thousand / 1000));
            }
            i.serving_counter = d.next_serving_counter;
            i.calls = d.calls;
            i.log = d.log;
            let running: u64 = admissions
                .iter()
                .filter(|(when, _)| when + SESSION > now)
                .map(|(_, n)| n)
                .sum();
            in_use_after.push(running);
        }
        (in_use_after, i.serving_counter)
    }

    #[test]
    fn no_more_visitors_are_ever_on_the_site_than_the_origin_takes() {
        for show in [1000, 700, 300, 50] {
            let (in_use, _) = simulate(100, show, 600);
            for (pass, n) in in_use.iter().enumerate() {
                assert!(*n <= 100, "show {show} pass {pass}: {n} on the site");
            }
        }
    }

    #[test]
    fn the_origin_stays_nearly_full_despite_no_shows() {
        // A spot is seen free up to a pass after its session ends, and a
        // no-show's spot is held for the turn and a pass more before it is
        // called again.
        for (show, at_least) in [(1000, 95), (700, 80)] {
            let (in_use, _) = simulate(100, show, 600);
            let settled = &in_use[100..];
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
        // Everyone shows: the origin is refilled as each session ends, so now
        // serving moves concurrency per session length, plus the open's fill.
        let passes = 600;
        let (_, served) = simulate(100, 1000, passes);
        let sessions =
            u64::try_from(passes * INTERVAL_SECS / (SESSION + 2 * INTERVAL_SECS)).unwrap();
        assert!(served >= 100 * sessions, "{served}");
        assert!(served <= 100 * (sessions + 2), "{served}");
    }

    // --- run_pass ------------------------------------------------------------

    #[derive(Default)]
    struct Writes {
        call: Option<PassDecision>,
        log: Option<AdmissionLog>,
        caps: Option<(Shelf, [u64; SHARDS])>,
    }

    struct FakeStore {
        state: ControllerState,
        lose_race: bool,
        writes: Mutex<Writes>,
    }

    impl FakeStore {
        fn new(state: ControllerState) -> Self {
            Self {
                state,
                lose_race: false,
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

        fn write_call(
            &self,
            _event_id: &str,
            decision: &PassDecision,
            _expected: u64,
        ) -> impl Future<Output = Result<CallOutcome, StoreError>> + Send {
            if self.lose_race {
                return std::future::ready(Ok(CallOutcome::LostRace));
            }
            self.writes.lock().unwrap().call = Some(decision.clone());
            std::future::ready(Ok(CallOutcome::Called))
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
            shelf: Shelf,
            caps: &[u64; SHARDS],
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.writes.lock().unwrap().caps = Some((shelf, *caps));
            std::future::ready(Ok(()))
        }
    }

    fn active_state() -> ControllerState {
        ControllerState {
            phase: Phase::Active,
            stored_control: StoredControl::Open,
            fail_open_until: None,
            inputs: inputs(100),
        }
    }

    #[tokio::test]
    async fn an_open_pass_calls_places_records_the_pass_and_holds_their_spots() {
        let store = FakeStore::new(active_state());
        let outcome = run_pass(&store, "evt", at(NOW)).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { called: 100 });
        let writes = store.writes.lock().unwrap();
        let decision = writes.call.as_ref().unwrap();
        assert_eq!(decision.next_serving_counter, 100);
        assert_eq!(decision.calls, vec![call(0, NOW, 0)]);
        assert_eq!(writes.log.as_ref().unwrap().entries, vec![entry(NOW, 0)]);
        let (on, caps) = writes.caps.unwrap();
        assert_eq!((on, caps.iter().sum::<u64>()), (shelf(0), 100));
    }

    #[tokio::test]
    async fn a_pass_that_calls_nobody_holds_no_spots() {
        let mut state = active_state();
        state.inputs = with_taken(state.inputs, 0, 100, 0);
        state.inputs.log = after_the_open();
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", at(NOW)).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { called: 0 });
        let writes = store.writes.lock().unwrap();
        assert!(writes.log.is_some());
        assert_eq!(writes.caps, None);
    }

    #[tokio::test]
    async fn a_lost_race_calls_nobody_and_writes_nothing_else() {
        let mut store = FakeStore::new(active_state());
        store.lose_race = true;
        let outcome = run_pass(&store, "evt", at(NOW)).await.unwrap();
        assert_eq!(outcome, PassOutcome::Ran { called: 0 });
        let writes = store.writes.lock().unwrap();
        assert!(writes.log.is_none() && writes.caps.is_none());
    }

    #[tokio::test]
    async fn a_pass_is_a_noop_when_not_active() {
        let mut state = active_state();
        state.phase = Phase::PreQueue;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", at(NOW)).await.unwrap();
        assert_eq!(outcome, PassOutcome::NotActive(Phase::PreQueue));
        let writes = store.writes.lock().unwrap();
        assert!(writes.call.is_none() && writes.caps.is_none());
    }

    #[tokio::test]
    async fn a_pause_calls_nobody() {
        let mut state = active_state();
        state.stored_control = StoredControl::Paused;
        let store = FakeStore::new(state);
        let outcome = run_pass(&store, "evt", at(NOW)).await.unwrap();
        assert_eq!(outcome, PassOutcome::Held(AdmissionControl::Paused));
        let writes = store.writes.lock().unwrap();
        assert!(writes.call.is_none() && writes.caps.is_none());
    }

    #[tokio::test]
    async fn fail_open_holds_the_controller_until_the_window_lapses() {
        let mut state = active_state();
        state.fail_open_until = Some(at(NOW + 5));
        let store = FakeStore::new(state);
        assert_eq!(
            run_pass(&store, "evt", at(NOW)).await.unwrap(),
            PassOutcome::Held(AdmissionControl::FailOpen)
        );
        // The very next pass, past the deadline, runs.
        assert!(matches!(
            run_pass(&store, "evt", at(NOW + 10)).await.unwrap(),
            PassOutcome::Ran { .. }
        ));
    }

    #[test]
    fn the_pass_outcome_round_trips_through_a_checkpoint() {
        for outcome in [
            PassOutcome::NotActive(Phase::Idle),
            PassOutcome::Held(AdmissionControl::Paused),
            PassOutcome::Ran { called: 42 },
        ] {
            let json = serde_json::to_string(&outcome).unwrap();
            assert_eq!(serde_json::from_str::<PassOutcome>(&json).unwrap(), outcome);
        }
    }

    // --- properties ----------------------------------------------------------

    use proptest::prelude::{
        Just, Strategy, any, prop_assert, prop_assert_eq, prop_oneof, proptest,
    };

    fn count() -> impl Strategy<Value = u64> {
        prop_oneof![0u64..1_000_000, Just(u64::MAX), any::<u64>()]
    }

    fn counters() -> impl Strategy<Value = [ShardBudget; SHARDS]> {
        proptest::array::uniform10(
            (count(), count()).prop_map(|(taken, cap)| ShardBudget { taken, cap }),
        )
    }

    fn shelves() -> impl Strategy<Value = Shelves> {
        proptest::collection::vec(counters(), SHELVES).prop_map(|v| <Shelves>::try_from(v).unwrap())
    }

    /// Any instant jiff can hold, edges included.
    fn instant() -> impl Strategy<Value = Timestamp> {
        let (min, max) = (Timestamp::MIN.as_second(), Timestamp::MAX.as_second());
        prop_oneof![Just(min), Just(max), min..=max].prop_map(at)
    }

    fn log() -> impl Strategy<Value = AdmissionLog> {
        proptest::collection::vec((instant(), count()), 0..8).prop_map(|mut pairs| {
            pairs.sort_unstable();
            log_of(
                pairs
                    .into_iter()
                    .map(|(at, admitted)| LogEntry { at, admitted })
                    .collect(),
            )
        })
    }

    fn calls() -> impl Strategy<Value = Vec<Call>> {
        proptest::collection::vec((count(), instant(), 0..SHELVES), 0..16).prop_map(|mut v| {
            v.sort_by_key(|(_, until, _)| *until);
            v.into_iter()
                .map(|(first, until, on)| Call {
                    first,
                    until,
                    shelf: shelf(on),
                })
                .collect()
        })
    }

    proptest! {
        #[test]
        fn a_pass_never_panics_moves_back_or_calls_past_the_limit(
            shelves in shelves(),
            serving in count(),
            queue in count(),
            concurrency in any::<u32>(),
            session in prop_oneof![0i64..1_000_000, Just(i64::MAX), 0..=i64::MAX],
            log in log(),
            now in instant(),
            read_at in instant(),
            calls in calls(),
        ) {
            let inputs = PassInputs {
                shelves,
                serving_counter: serving,
                queue_counter: queue,
                origin_concurrency: concurrency,
                session: SignedDuration::from_secs(session),
                log,
                shards_read_at: read_at,
                calls,
                draw_cutoff: None,
            };
            let d = compute_pass(&inputs, now);

            prop_assert!(d.next_serving_counter >= serving);
            prop_assert!(d.next_serving_counter <= serving.max(queue.saturating_add(1)));
            prop_assert_eq!(d.next_serving_counter - serving, d.called);

            // Spots in use, spots held and spots called never exceed the limit.
            prop_assert!(d.called <= d.free);
            prop_assert!(
                d.in_use.saturating_add(d.held).saturating_add(d.free) <= u64::from(concurrency)
                    || d.free == 0
            );
            if let Some(new) = &d.call {
                prop_assert!(d.called > 0);
                // The new call's shelf is shared with no other call kept.
                let sharing = d.calls.iter().filter(|c| c.shelf == new.shelf).count();
                prop_assert_eq!(sharing, 1);
                for (cap, shard) in new.caps.iter().zip(inputs.shelves[new.shelf.index()]) {
                    prop_assert!(*cap >= shard.taken);
                }
            } else {
                prop_assert_eq!(d.called, 0);
            }
            prop_assert!(d.log.entries.windows(2).all(|w| w[0].at <= w[1].at));
        }
    }
}
