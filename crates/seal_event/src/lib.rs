//! Seal logic for the `seal_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence — so a retry or a double-fire seals exactly
//! once.
//!
//! The seal is also where `queue_counter` starts at the cohort size, so live
//! joiners are numbered behind the whole pre-queue cohort instead of colliding
//! with `[0, N)`. That clause lives in [`wr_common::expr::seal_update`] with the
//! rest of the seal write, because it must be in the same atomic update: a
//! separate write could be lost between the seal and the first live join.

use std::future::Future;

use wr_common::{Phase, SHARDS, SealError, SealedOffsets};

pub mod dynamo;

/// The values written by a seal: the seed, cohort size, and prefix offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealValues {
    pub seed: [u8; 32],
    pub participant_count: u64,
    pub offsets: [u64; SHARDS],
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("seal store error: {0}")]
pub struct StoreError(pub String);

/// The result of attempting a seal.
#[derive(Debug, PartialEq, Eq)]
pub enum SealResult {
    /// This call performed the seal and wrote the values.
    Sealed(Box<SealValues>),
    /// The event was already sealed (the guard rejected the write); nothing
    /// changed. A double-fire or retry lands here.
    AlreadySealed,
}

/// The persistence port the seal drives.
pub trait Store {
    /// Reads the 10 pre-queue shard counts for the event.
    fn read_shard_counts(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send;

    /// Writes the seal values, starts `queue_counter` at the cohort size, and
    /// flips the phase to active, guarded by
    /// `attribute_not_exists(shuffle_seed)`. Returns `false` if the guard
    /// rejected the write (already sealed).
    fn write_seal(
        &self,
        event_id: &str,
        values: &SealValues,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// Folds the shard counts into the offsets and cohort size and pairs them with
/// a freshly generated seed.
///
/// # Errors
///
/// Returns [`SealError::Overflow`] if the summed cohort size exceeds `u64`.
pub fn seal_values(counts: [u64; SHARDS], seed: [u8; 32]) -> Result<SealValues, SealError> {
    let sealed: SealedOffsets = SealedOffsets::seal(counts)?;
    let mut offsets = [0u64; SHARDS];
    for (shard, slot) in offsets.iter_mut().enumerate() {
        *slot = sealed.offset(shard);
    }
    Ok(SealValues {
        seed,
        participant_count: sealed.participant_count(),
        offsets,
    })
}

/// Reads the shard counts, computes the seal values with the supplied seed, and
/// writes them under the once-only guard. The seed is passed in so the logic is
/// deterministic under test; production generates it from a CSPRNG.
///
/// # Errors
///
/// Returns [`StoreError`] if reading the shard counts, folding them, or writing
/// the seal fails.
pub async fn seal_event<S: Store>(
    store: &S,
    event_id: &str,
    seed: [u8; 32],
) -> Result<SealResult, StoreError> {
    let counts = store.read_shard_counts(event_id).await?;
    let values =
        seal_values(counts, seed).map_err(|e| StoreError(format!("fold shard counts: {e}")))?;
    if store.write_seal(event_id, &values).await? {
        tracing::info!(
            event_id,
            participant_count = values.participant_count,
            "event sealed"
        );
        Ok(SealResult::Sealed(Box::new(values)))
    } else {
        tracing::info!(event_id, "event already sealed; no-op");
        Ok(SealResult::AlreadySealed)
    }
}

/// The phase a sealed event is in.
#[must_use]
pub fn sealed_phase() -> Phase {
    Phase::Active
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure"
    )]

    use std::sync::Mutex;

    use super::*;

    struct FakeStore {
        counts: [u64; SHARDS],
        already_sealed: bool,
        written: Mutex<Option<SealValues>>,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_sealed: bool) -> Self {
            Self {
                counts,
                already_sealed,
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

        fn write_seal(
            &self,
            _event_id: &str,
            values: &SealValues,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            let wrote = if self.already_sealed {
                false
            } else {
                *self.written.lock().unwrap() = Some(values.clone());
                true
            };
            std::future::ready(Ok(wrote))
        }
    }

    #[test]
    fn seal_values_are_prefix_sums_and_total() {
        let values = seal_values([3, 0, 5, 1, 0, 0, 2, 0, 0, 4], [7u8; 32]).unwrap();
        assert_eq!(values.participant_count, 15);
        assert_eq!(values.offsets, [0, 3, 3, 8, 9, 9, 9, 11, 11, 11]);
        assert_eq!(values.seed, [7u8; 32]);
    }

    #[tokio::test]
    async fn first_seal_writes_values() {
        let store = FakeStore::new([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], false);
        let result = seal_event(&store, "evt-1", [9u8; 32]).await.unwrap();
        match result {
            SealResult::Sealed(values) => assert_eq!(values.participant_count, 10),
            SealResult::AlreadySealed => panic!("expected a first seal"),
        }
        assert!(store.written.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn second_seal_is_noop() {
        let store = FakeStore::new([1; SHARDS], true);
        let result = seal_event(&store, "evt-1", [9u8; 32]).await.unwrap();
        assert_eq!(result, SealResult::AlreadySealed);
        assert!(store.written.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_cohort_seals_to_zero() {
        let store = FakeStore::new([0; SHARDS], false);
        let result = seal_event(&store, "evt-1", [1u8; 32]).await.unwrap();
        match result {
            SealResult::Sealed(values) => {
                assert_eq!(values.participant_count, 0);
                assert_eq!(values.offsets, [0; SHARDS]);
            }
            SealResult::AlreadySealed => panic!("expected a first seal"),
        }
    }
}
