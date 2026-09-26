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

use wr_common::{CohortError, CohortOffsets, SHARDS};

pub mod dynamo;

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
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_open: bool) -> Self {
            Self {
                counts,
                already_open,
                phase: Phase::PreQueue,
                written: Mutex::new(None),
            }
        }
    }

    impl Store for FakeStore {
        fn read_shard_counts(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
            std::future::ready(Ok(self.counts))
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
            fn read_shard_counts(
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
}
