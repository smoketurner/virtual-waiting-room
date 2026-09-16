//! Open logic for the `open_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence — so a retry or a double-fire opens exactly
//! once.
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
    /// flips the phase to active, guarded by `attribute_not_exists(shuffle_seed)`.
    /// Returns `false` if the guard rejected the write (already open).
    fn write_open(
        &self,
        event_id: &str,
        values: &OpenValues,
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
        tracing::info!(event_id, "event already open; no-op");
        return Ok(OpenResult::AlreadyOpen);
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
        written: Mutex<Option<OpenValues>>,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_open: bool) -> Self {
            Self {
                counts,
                already_open,
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
            let wrote = if self.already_open {
                false
            } else {
                *self.written.lock().unwrap() = Some(values.clone());
                true
            };
            std::future::ready(Ok(wrote))
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
        // The guard rejected the write, so nothing was reseeded: a double-fire
        // or a retry must not hand the cohort a second permutation.
        let store = FakeStore::new([1; SHARDS], true);
        let result = open_event(&store, "evt-1", [9u8; 32]).await.unwrap();
        assert_eq!(result, OpenResult::AlreadyOpen);
        assert!(store.written.lock().unwrap().is_none());
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
