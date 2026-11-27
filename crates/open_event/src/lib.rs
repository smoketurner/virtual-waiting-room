//! Open logic for the `open_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence AND the `pre_queue` phase — so a retry or a
//! double-fire opens exactly once, and an open from any other phase is
//! rejected rather than seeding a cohort of 0 and forfeiting the pre-queue
//! stage. The guard's two failure modes (already open, wrong phase) collapse
//! to one `ConditionalCheckFailedException`; [`open_event`] disambiguates
//! them with [`Store::is_already_open`] so a wrong-phase rejection surfaces as
//! an error rather than being misreported as already-open.
//!
//! The open is also where `queue_counter` starts behind the cohort, so live
//! joiners are numbered behind every pre-queue position instead of colliding
//! with one. That clause is in the same atomic update as the rest of the open
//! write: a separate write could be lost between the open and the first live
//! join.

use std::future::Future;

use jiff::{SignedDuration, Timestamp};
use wr_common::{CohortError, CohortOffsets, Counters, SHARDS};

pub mod dynamo;
pub mod sqs;

/// The values written when the event opens: the seed, cohort size, and prefix
/// offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenValues {
    pub seed: [u8; 32],
    pub participant_count: u64,
    pub offsets: [u64; SHARDS],
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("open store error: {0}")]
pub struct StoreError(pub String);

/// The result of attempting an open.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenResult {
    /// This call performed the open and wrote the values.
    Opened(Box<OpenValues>),
    /// The event was already open (the guard rejected the write); nothing
    /// changed. A double-fire or retry lands here.
    AlreadyOpen,
}

/// The persistence port the open drives.
pub trait Store {
    /// The event's `Counters` item, for when its draw closes.
    fn load_counters(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send;

    /// Reads the 10 shard counts of joins sent back to wait for the open.
    fn read_deferred_counts(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send;

    /// Reads the 10 pre-queue shard counts for the event.
    fn read_shard_counts(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send;

    /// Writes the open values, starts `queue_counter` at the cohort size, and
    /// flips the phase to active, guarded by `attribute_not_exists(shuffle_seed)
    /// AND phase = pre_queue`. Returns `false` if the guard rejected the write
    /// — either the seed already exists (already open) or the phase is not
    /// `pre_queue` (wrong phase). The two causes collapse to one `false`;
    /// [`Store::is_already_open`] disambiguates them, so a caller must not
    /// assume `false` means already-open.
    fn write_open(
        &self,
        event_id: &str,
        values: &OpenValues,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Whether the event's `shuffle_seed` is present — the single authoritative
    /// "already open" signal, since the open writes it and nothing else does.
    ///
    /// Used to disambiguate a [`Store::write_open`] `false`: `true` means the
    /// guard failed because the event was already open (a double-fire or
    /// retry); `false` means the guard failed because the phase was not
    /// `pre_queue` — a wrong-phase rejection, which wrote nothing and must
    /// surface as an error rather than be misreported as already-open. Reading
    /// the seed alone keeps the disambiguating read to one attribute.
    fn is_already_open(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// Folds the shard counts into the offsets and cohort size and pairs them with
/// a freshly generated seed.
///
/// # Errors
///
/// Returns [`CohortError::Overflow`] if the summed cohort size exceeds `u64`.
pub fn open_values(counts: [u64; SHARDS], seed: [u8; 32]) -> Result<OpenValues, CohortError> {
    let opened: CohortOffsets = CohortOffsets::from_counts(counts)?;
    let mut offsets = [0u64; SHARDS];
    for (shard, slot) in offsets.iter_mut().enumerate() {
        *slot = opened.offset(shard);
    }
    Ok(OpenValues {
        seed,
        participant_count: opened.participant_count(),
        offsets,
    })
}

/// Reads the shard counts, computes the open values with the supplied seed,
/// and writes the open under the once-only guard. The seed is passed in so the
/// logic is deterministic under test; production generates it from a CSPRNG.
///
/// # Errors
///
/// Returns [`StoreError`] if reading the shard counts, folding them, or
/// writing the open fails.
pub async fn open_event<S: Store>(
    store: &S,
    event_id: &str,
    seed: [u8; 32],
) -> Result<OpenResult, StoreError> {
    let counts = store.read_shard_counts(event_id).await?;
    let values =
        open_values(counts, seed).map_err(|e| StoreError(format!("fold shard counts: {e}")))?;

    if !store.write_open(event_id, &values).await? {
        // The guard rejects both a double-fire (the seed already exists) and
        // an open from the wrong phase (phase != pre_queue) with the same
        // `false`, so the seed's presence is the disambiguator. An already-open
        // event is a benign no-op a retry or a double-fire lands on; a
        // wrong-phase rejection wrote nothing and the event is still unopened,
        // which must surface as an error so EventBridge retries (and eventually
        // dead-letters) rather than recording a silent success — which is what
        // happened when this branch assumed every `false` meant already-open.
        if store.is_already_open(event_id).await? {
            tracing::info!(event_id, "event already open; no-op");
            return Ok(OpenResult::AlreadyOpen);
        }
        tracing::error!(
            event_id,
            event = "open_rejected_wrong_phase",
            "scheduled open rejected: event not in pre_queue phase"
        );
        return Err(StoreError(
            "open rejected: event not in pre_queue phase".to_owned(),
        ));
    }
    tracing::info!(
        event_id,
        participant_count = values.participant_count,
        "event opened"
    );
    Ok(OpenResult::Opened(Box::new(values)))
}

/// How many consecutive reads must find the queue caught up. `SQS` reports an
/// approximate count, so a single reading is not yet a drained queue.
pub const DRAIN_QUIET_POLLS: u32 = 2;

/// The join queue's backlog, and how fast it is being cleared, as the open
/// sees them.
pub trait Backlog {
    /// Messages waiting to be picked up -- accepted joins nothing has started
    /// processing.
    fn waiting(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Registrations counted into the cohort so far: the sum of the shard
    /// counts.
    fn counted(&self) -> impl Future<Output = Result<u64, StoreError>> + Send;
}

/// Whether the join queue was seen to drain before the open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drain {
    /// The queue read caught up [`DRAIN_QUIET_POLLS`] times running.
    Drained,
    /// It never did within the reads the open had time for; this many were
    /// waiting at the last one.
    TimedOut { waiting: u64 },
    /// The queue or the shard counts could not be read. The open goes ahead:
    /// it must not depend on a read that only makes it fairer.
    Unknown,
}

/// Waits for the join queue to catch up before the open reads the shard
/// counts (issue #239).
///
/// A join answered 200 before the start is a registration the visitor believes
/// they hold, but it is only in the cohort once `assign_position` has claimed
/// its index. Arrivals bunch in the last minute, which is when the consumers
/// are furthest behind, and the open reads only the counters: everything still
/// queued at that instant would be numbered behind the whole cohort. So the
/// open waits for the consumers to catch up, however long the backlog takes,
/// and how fast joins arrive decides only how late the open lands, never who
/// is in the cohort.
///
/// Caught up is a queue holding no more than the consumers counted since the
/// last read: whatever is waiting is cleared by the next one. An empty queue
/// is not the test, because joins keep arriving after the start and a queue
/// under steady traffic rarely reads empty twice. In-flight messages are not
/// waited for: a batch that has claimed its indices is counted by the open,
/// and one that has not is the straggler case the batch itself repairs.
///
/// `polls` is how many reads the open has time for before it must go ahead
/// anyway; `pause` separates them. Both are parameters so the wait is testable
/// without a clock.
pub async fn wait_for_drain<B, P, F>(backlog: &B, polls: u32, pause: P) -> Drain
where
    B: Backlog,
    P: Fn() -> F,
    F: Future<Output = ()>,
{
    let mut quiet = 0u32;
    let mut waiting = 0u64;
    let mut last: Option<u64> = None;
    for poll in 0..polls {
        if poll > 0 {
            pause().await;
        }
        let read = match backlog.waiting().await {
            Ok(w) => backlog.counted().await.map(|c| (w, c)),
            Err(err) => Err(err),
        };
        let counted;
        (waiting, counted) = match read {
            Ok(read) => read,
            Err(err) => {
                tracing::error!(error = %err, event = "open_drain_unreadable", "could not read the join queue; opening without waiting for it");
                return Drain::Unknown;
            }
        };
        let cleared = last.map_or(0, |prev| counted.saturating_sub(prev));
        last = Some(counted);
        quiet = if waiting <= cleared {
            quiet.saturating_add(1)
        } else {
            0
        };
        if quiet >= DRAIN_QUIET_POLLS {
            return Drain::Drained;
        }
    }
    tracing::warn!(
        waiting,
        event = "open_drain_timed_out",
        "the join queue did not drain before the open; joins still queued are numbered behind the cohort"
    );
    Drain::TimedOut { waiting }
}

/// The pause between reads of the join queue while it drains.
pub const DRAIN_PAUSE: SignedDuration = SignedDuration::from_secs(1);

/// What the open keeps of its deadline for the open itself: one `BatchGetItem`
/// and one conditional `UpdateItem`.
pub const OPEN_RESERVE: SignedDuration = SignedDuration::from_secs(10);

/// How many reads of the join queue fit before `deadline`, keeping
/// [`OPEN_RESERVE`] for the open itself.
#[must_use]
pub fn drain_polls(deadline: Timestamp, now: Timestamp) -> u32 {
    let left = deadline
        .duration_since(now)
        .saturating_sub(OPEN_RESERVE)
        .max(SignedDuration::ZERO);
    u32::try_from(left.as_secs() / DRAIN_PAUSE.as_secs()).unwrap_or(u32::MAX)
}

/// Opens the event once its draw has closed and the join queue has caught up
/// on the joins accepted before then, as long as `deadline` allows.
///
/// A join accepted before the draw's cutoff is on time, so nothing is written
/// before it. Neither wait is a reason not to open: a cutoff past what the
/// deadline allows, or a read that fails, opens without it.
///
/// # Errors
///
/// The open itself failed; see [`open_event`].
pub async fn open_when_ready<S, B, C, Z, F>(
    store: &S,
    backlog: &B,
    event_id: &str,
    seed: [u8; 32],
    deadline: Timestamp,
    now: C,
    sleep: Z,
) -> Result<OpenResult, StoreError>
where
    S: Store,
    B: Backlog,
    C: Fn() -> Timestamp,
    Z: Fn(SignedDuration) -> F,
    F: Future<Output = ()>,
{
    let latest = deadline.checked_sub(OPEN_RESERVE).unwrap_or(Timestamp::MIN);
    match store.load_counters(event_id).await {
        Ok(counters) => {
            if let Some(cutoff) = counters.as_ref().and_then(Counters::draw_cutoff) {
                if cutoff > latest {
                    tracing::warn!(
                        %cutoff,
                        event = "open_before_draw_cutoff",
                        "the draw closes after the open must go ahead; joins accepted between are numbered behind the cohort"
                    );
                }
                let wait = cutoff.min(latest).duration_since(now());
                if wait.is_positive() {
                    sleep(wait).await;
                }
            }
        }
        Err(err) => {
            tracing::error!(error = %err, event = "open_cutoff_unreadable", "could not read when the draw closes; opening without waiting for it");
        }
    }
    let polls = drain_polls(deadline, now());
    let drain = wait_for_drain(backlog, polls, || sleep(DRAIN_PAUSE)).await;
    tracing::info!(drain = ?drain, "join queue checked before the open");
    open_event(store, event_id, seed).await
}

/// The phase the open leaves the event in.
#[must_use]
pub fn opened_phase() -> wr_common::Phase {
    wr_common::Phase::Active
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure"
    )]

    use std::sync::Mutex;

    use wr_common::Phase;

    use super::*;

    struct FakeStore {
        counts: [u64; SHARDS],
        already_open: bool,
        /// The phase the open guard conditions on. Defaults to `PreQueue` so a
        /// `FakeStore::new` models the only phase a real open succeeds from;
        /// tests set this to `Idle` (or any other phase) to exercise the
        /// wrong-phase rejection the real `DynamoDB` guard produces.
        phase: Phase,
        written: Mutex<Option<OpenValues>>,
        starts_at: Option<Timestamp>,
        counters_fail: bool,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_open: bool) -> Self {
            Self {
                counts,
                already_open,
                phase: Phase::PreQueue,
                written: Mutex::new(None),
                starts_at: None,
                counters_fail: false,
            }
        }

        fn starting(starts_at: Timestamp) -> Self {
            Self {
                starts_at: Some(starts_at),
                ..Self::new([0; SHARDS], false)
            }
        }
    }

    impl Store for FakeStore {
        fn load_counters(
            &self,
            event_id: &str,
        ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send {
            let counters = Counters {
                event_id: event_id.to_owned(),
                phase: self.phase,
                queue_counter: 0,
                serving_counter: 0,
                shuffle_seed: None,
                participant_count: None,
                prequeue_offsets: None,
                message: None,
                origin_concurrency: None,
                stored_control: wr_common::StoredControl::Open,
                fail_open_until: None,
                starts_at: self.starts_at,
                calls: Vec::new(),
            };
            std::future::ready(if self.counters_fail {
                Err(StoreError("get_item counters: boom".to_owned()))
            } else {
                Ok(Some(counters))
            })
        }

        fn read_shard_counts(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
            std::future::ready(Ok(self.counts))
        }

        fn read_deferred_counts(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
            std::future::ready(Ok([0; SHARDS]))
        }

        fn write_open(
            &self,
            _event_id: &str,
            values: &OpenValues,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            // Mirrors the real open_guard: the guard rejects (Ok(false)) when
            // the seed already exists OR the phase is not pre_queue, the two
            // causes a ConditionalCheckFailedException collapses into one.
            let wrote = if self.already_open || self.phase != Phase::PreQueue {
                false
            } else {
                *self.written.lock().unwrap() = Some(values.clone());
                true
            };
            std::future::ready(Ok(wrote))
        }

        fn is_already_open(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            std::future::ready(Ok(self.already_open))
        }
    }

    #[test]
    fn open_values_are_prefix_sums_and_total() {
        let values = open_values([3, 0, 5, 1, 0, 0, 2, 0, 0, 4], [7u8; 32]).unwrap();
        assert_eq!(values.participant_count, 15);
        assert_eq!(values.offsets, [0, 3, 3, 8, 9, 9, 9, 11, 11, 11]);
        assert_eq!(values.seed, [7u8; 32]);
    }

    #[tokio::test]
    async fn first_open_writes_values() {
        let store = FakeStore::new([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], false);
        let result = open_event(&store, "evt-1", [9u8; 32]).await.unwrap();
        match result {
            OpenResult::Opened(values) => assert_eq!(values.participant_count, 10),
            OpenResult::AlreadyOpen => panic!("expected a first open"),
        }
        let written = store.written.lock().unwrap().clone().unwrap();
        assert_eq!(written.participant_count, 10);
        assert_eq!(written.seed, [9u8; 32]);
    }

    #[tokio::test]
    async fn second_open_is_noop() {
        // The guard rejected the write, and is_already_open confirms the seed
        // already exists, so the disambiguation returns AlreadyOpen rather than
        // an error: a double-fire or a retry must not hand the cohort a second
        // permutation, and must not retry forever on an event that is already
        // open.
        let store = FakeStore::new([1; SHARDS], true);
        let result = open_event(&store, "evt-1", [9u8; 32]).await.unwrap();
        assert_eq!(result, OpenResult::AlreadyOpen);
        assert!(store.written.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_wrong_phase_rejection_is_surfaced_as_an_error_not_already_open() {
        // The narrowed open_guard rejects an open from any phase other than
        // pre_queue with the same `false` it rejects a double-fire with. That
        // false must not collapse to AlreadyOpen: nothing was written and the
        // event is still unopened, so the rejection must surface as an error
        // the scheduler retries (and eventually dead-letters), not a silent
        // no-op EventBridge would record as success.
        let mut store = FakeStore::new([0; SHARDS], false);
        store.phase = Phase::Idle;
        let result = open_event(&store, "evt-1", [9u8; 32]).await;
        assert!(
            result.is_err(),
            "wrong-phase rejection must surface as Err, not AlreadyOpen: {result:?}"
        );
        assert!(
            store.written.lock().unwrap().is_none(),
            "a rejected open wrote nothing"
        );
    }

    #[tokio::test]
    async fn a_wrong_phase_rejection_from_every_non_pre_queue_phase_is_an_error() {
        // The guard's phase clause admits only pre_queue, so every other phase
        // the lifecycle has must reject — not just idle. This pins the
        // disambiguation against a regression that re-admits one of them by
        // special-casing a single phase instead of `!= PreQueue`.
        for phase in [
            Phase::Idle,
            Phase::Active,
            Phase::PostEvent,
            Phase::Maintenance,
        ] {
            let mut store = FakeStore::new([0; SHARDS], false);
            store.phase = phase;
            let result = open_event(&store, "evt-1", [9u8; 32]).await;
            assert!(
                result.is_err(),
                "open from {phase:?} must reject as Err, not AlreadyOpen: {result:?}"
            );
            assert!(
                store.written.lock().unwrap().is_none(),
                "open from {phase:?} wrote nothing"
            );
        }
    }

    #[tokio::test]
    async fn an_is_already_open_failure_propagates_as_an_error_not_already_open() {
        // The disambiguating read can itself fail (a transient DynamoDB error)
        // and must not be swallowed into AlreadyOpen, or a wrong-phase
        // rejection whose follow-up read failed would be misreported the same
        // way the original bug misreported it.
        struct ReadFails;
        impl Store for ReadFails {
            fn load_counters(
                &self,
                _event_id: &str,
            ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send {
                std::future::ready(Ok(None))
            }
            fn read_shard_counts(
                &self,
                _event_id: &str,
            ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
                std::future::ready(Ok([0; SHARDS]))
            }
            fn read_deferred_counts(
                &self,
                _event_id: &str,
            ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
                std::future::ready(Ok([0; SHARDS]))
            }
            fn write_open(
                &self,
                _event_id: &str,
                _values: &OpenValues,
            ) -> impl Future<Output = Result<bool, StoreError>> + Send {
                std::future::ready(Ok(false))
            }
            fn is_already_open(
                &self,
                _event_id: &str,
            ) -> impl Future<Output = Result<bool, StoreError>> + Send {
                std::future::ready(Err(StoreError("get_item shuffle_seed: boom".to_owned())))
            }
        }
        let result = open_event(&ReadFails, "evt-1", [9u8; 32]).await;
        assert!(
            matches!(result, Err(ref e) if e.to_string().contains("shuffle_seed")),
            "an is_already_open failure must propagate, not become AlreadyOpen: {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_cohort_opens_to_zero() {
        let store = FakeStore::new([0; SHARDS], false);
        let result = open_event(&store, "evt-1", [1u8; 32]).await.unwrap();
        match result {
            OpenResult::Opened(values) => {
                assert_eq!(values.participant_count, 0);
                assert_eq!(values.offsets, [0; SHARDS]);
            }
            OpenResult::AlreadyOpen => panic!("expected a first open"),
        }
    }

    #[test]
    fn opening_always_activates_the_event() {
        // The phase is written in the same conditional update as the values, so
        // there is no window in which an event is opened but not active.
        assert_eq!(opened_phase(), Phase::Active);
    }

    // --- waiting for the join queue (issue #239) ------------------------------

    const POLLS: u32 = 15;

    /// A backlog that answers from a script, then repeats its last answer.
    /// The shard counts stand still unless a test scripts them.
    struct ScriptedBacklog {
        script: Vec<Result<u64, ()>>,
        counted: Vec<u64>,
        reads: Mutex<usize>,
    }

    impl ScriptedBacklog {
        fn new(script: Vec<Result<u64, ()>>) -> Self {
            Self::counting(script, vec![0])
        }

        fn counting(script: Vec<Result<u64, ()>>, counted: Vec<u64>) -> Self {
            Self {
                script,
                counted,
                reads: Mutex::new(0),
            }
        }

        fn reads(&self) -> usize {
            *self.reads.lock().unwrap()
        }
    }

    impl Backlog for ScriptedBacklog {
        fn waiting(&self) -> impl Future<Output = Result<u64, StoreError>> + Send {
            let mut reads = self.reads.lock().unwrap();
            let index = (*reads).min(self.script.len() - 1);
            *reads += 1;
            std::future::ready(
                self.script[index].map_err(|()| StoreError("queue unreadable".to_owned())),
            )
        }

        fn counted(&self) -> impl Future<Output = Result<u64, StoreError>> + Send {
            // Read after `waiting`, so the read it pairs with is one behind.
            let reads = *self.reads.lock().unwrap();
            let index = reads.saturating_sub(1).min(self.counted.len() - 1);
            std::future::ready(Ok(self.counted[index]))
        }
    }

    /// Runs `open_when_ready` at `now` against an empty queue, returning what
    /// it opened and every pause it took.
    async fn open_at(
        store: &FakeStore,
        now: Timestamp,
        deadline: Timestamp,
    ) -> (Result<OpenResult, StoreError>, Vec<SignedDuration>) {
        let pauses = Mutex::new(Vec::new());
        let backlog = ScriptedBacklog::new(vec![Ok(0)]);
        let opened = open_when_ready(
            store,
            &backlog,
            "evt-1",
            [3u8; 32],
            deadline,
            || now,
            |d| {
                pauses.lock().unwrap().push(d);
                std::future::ready(())
            },
        )
        .await;
        (opened, pauses.into_inner().unwrap())
    }

    const T0: Timestamp = Timestamp::constant(1_000_000, 0);

    #[tokio::test]
    async fn the_open_waits_for_the_draw_to_close_before_writing() {
        let store = FakeStore::starting(T0);
        let (opened, pauses) = open_at(&store, T0, T0 + SignedDuration::from_mins(15)).await;
        assert!(matches!(opened, Ok(OpenResult::Opened(_))));
        // The grace window, then the drain's one pause between its two reads.
        assert_eq!(pauses, vec![wr_common::DRAW_GRACE, DRAIN_PAUSE]);
    }

    #[tokio::test]
    async fn an_open_after_the_cutoff_or_unscheduled_does_not_wait_for_it() {
        let late = FakeStore::starting(T0);
        let (_, pauses) = open_at(
            &late,
            T0 + SignedDuration::from_mins(1),
            T0 + SignedDuration::from_mins(16),
        )
        .await;
        assert_eq!(pauses, vec![DRAIN_PAUSE]);
        let unscheduled = FakeStore::new([0; SHARDS], false);
        let (_, pauses) = open_at(&unscheduled, T0, T0 + SignedDuration::from_mins(15)).await;
        assert_eq!(pauses, vec![DRAIN_PAUSE]);
    }

    #[tokio::test]
    async fn a_cutoff_past_the_deadline_waits_only_as_long_as_the_open_can() {
        let store = FakeStore::starting(T0 + SignedDuration::from_hours(1));
        let deadline = T0 + SignedDuration::from_mins(15);
        let (opened, pauses) = open_at(&store, T0, deadline).await;
        assert!(matches!(opened, Ok(OpenResult::Opened(_))));
        assert_eq!(
            pauses.first(),
            Some(&(SignedDuration::from_mins(15) - OPEN_RESERVE))
        );
    }

    #[tokio::test]
    async fn an_unreadable_cutoff_opens_without_waiting_for_it() {
        let store = FakeStore {
            counters_fail: true,
            ..FakeStore::starting(T0)
        };
        let (opened, pauses) = open_at(&store, T0, T0 + SignedDuration::from_mins(15)).await;
        assert!(matches!(opened, Ok(OpenResult::Opened(_))));
        assert_eq!(pauses, vec![DRAIN_PAUSE]);
    }

    #[test]
    fn the_drain_runs_until_the_open_must_go_ahead() {
        let now = Timestamp::constant(1_000, 0);
        assert_eq!(drain_polls(now + SignedDuration::from_mins(15), now), 890);
        assert_eq!(drain_polls(now + SignedDuration::from_secs(10), now), 0);
        assert_eq!(drain_polls(now, now + SignedDuration::from_secs(5)), 0);
    }

    #[tokio::test]
    async fn an_empty_queue_is_confirmed_with_a_second_read() {
        let backlog = ScriptedBacklog::new(vec![Ok(0)]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Drained);
        assert_eq!(backlog.reads(), DRAIN_QUIET_POLLS as usize);
    }

    #[tokio::test]
    async fn the_open_waits_while_joins_are_still_queued() {
        let backlog = ScriptedBacklog::new(vec![Ok(4_000), Ok(900), Ok(0), Ok(0)]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Drained);
        assert_eq!(backlog.reads(), 4);
    }

    #[tokio::test]
    async fn a_backlog_longer_than_a_fixed_wait_is_waited_out() {
        // Ten minutes of backlog at one read a second: the open waits for it
        // rather than numbering those joins behind the cohort.
        let mut script = vec![Ok(250_000); 600];
        script.extend([Ok(0), Ok(0)]);
        let counted = (0..602).map(|read| read * 4_000).collect();
        let backlog = ScriptedBacklog::counting(script, counted);
        let drain = wait_for_drain(&backlog, 890, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Drained);
        assert_eq!(backlog.reads(), 602);
    }

    #[tokio::test]
    async fn joins_arriving_after_the_start_do_not_hold_the_open() {
        // Steady traffic: a few joins always waiting, each read finding the
        // consumers have counted more than that since the last one.
        let backlog = ScriptedBacklog::counting(vec![Ok(30)], vec![0, 200, 400, 600]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Drained);
        assert_eq!(backlog.reads(), 3);
    }

    #[tokio::test]
    async fn a_backlog_the_consumers_are_not_clearing_holds_the_open() {
        // More waiting than the consumers count in a second: still behind.
        let backlog =
            ScriptedBacklog::counting(vec![Ok(5_000)], (0..15).map(|r| r * 4_000).collect());
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::TimedOut { waiting: 5_000 });
    }

    #[tokio::test]
    async fn one_empty_read_between_busy_ones_is_not_a_drained_queue() {
        // The count is approximate: a zero followed by more messages was not
        // an empty queue.
        let backlog = ScriptedBacklog::new(vec![Ok(0), Ok(50), Ok(0), Ok(0)]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Drained);
        assert_eq!(backlog.reads(), 4);
    }

    #[tokio::test]
    async fn a_queue_that_never_drains_does_not_hold_the_open_forever() {
        let backlog = ScriptedBacklog::new(vec![Ok(10_000)]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::TimedOut { waiting: 10_000 });
        assert_eq!(backlog.reads(), POLLS as usize);
    }

    #[tokio::test]
    async fn an_unreadable_queue_does_not_block_the_open() {
        let backlog = ScriptedBacklog::new(vec![Ok(10), Err(())]);
        let drain = wait_for_drain(&backlog, POLLS, || std::future::ready(())).await;
        assert_eq!(drain, Drain::Unknown);
        assert_eq!(backlog.reads(), 2);
    }
}
