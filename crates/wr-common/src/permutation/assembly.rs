//! Pre-queue index-space assembly.
//!
//! Registration stripes the pre-queue counter across `K = 10` shards: each
//! join draws a shard `s` uniformly at random (issue #59 — a client-supplied
//! `request_id` let an attacker steer `hash(request_id) % 10` in about ten
//! tries, biasing the seal's prefix offsets), claims a local index `l` by
//! atomically incrementing that shard's counter, and stores `(request_id, s,
//! l)` — never a global index. Sealing the cohort reads the 10 shard counts,
//! computes prefix offsets `offset[s] = Σ counts[0..s)` and cohort size `N = Σ
//! counts`. A visitor's global registration index is reconstructed on read as
//! `i = offset[s] + l`.
//!
//! Because the shards partition the cohort and the offsets are a prefix sum,
//! the set of global indices is exactly the contiguous range `[0, N)` — the
//! domain [`crate::prp`] permutes. `N` counts indices *issued*, not rows
//! written: an index whose row write failed after the counter incremented is a
//! burned slot that maps to a position no one claims, which is permitted (a
//! served position that admits nobody).

/// Number of pre-queue counter shards; fixed at 10 for every deployment.
pub const SHARDS: usize = 10;

/// The sealed pre-queue index space: per-shard prefix offsets and the cohort
/// size `N`, computed once from the shard counts when the cohort is sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedOffsets {
    offsets: [u64; SHARDS],
    participant_count: u64,
}

/// The outcome of reconstructing a visitor's global registration index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignment {
    /// The local index is within the shard's own issued count; [`crate::prp`]
    /// derives the pre-queue position from the reconstructed global index.
    PreQueue { index: u64 },
    /// The local index is at or past the shard's own issued count — claimed
    /// after the seal read that shard's count. The caller assigns a live-join
    /// position instead. This is a per-shard bound, not a global `i >= N`
    /// test: a global test would let an over-count on one shard reconstruct
    /// into the index range a *later* shard legitimately owns, handing two
    /// visitors the same position.
    LiveJoin,
}

/// Error from sealing the pre-queue index space.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SealError {
    /// A shard count plus the running total would exceed `u64`. Unreachable in
    /// practice (cohort sizes are ~`10^6`), but summed explicitly rather than
    /// wrapped.
    #[error("pre-queue cohort size overflows u64")]
    Overflow,
}

impl SealedOffsets {
    /// Seals the index space from the 10 shard counts: computes the prefix
    /// offsets and `N = Σ counts`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError::Overflow`] if the summed cohort size would exceed
    /// `u64`.
    pub fn seal(counts: [u64; SHARDS]) -> Result<Self, SealError> {
        let mut offsets = [0u64; SHARDS];
        let mut running = 0u64;
        let mut shard = 0;
        while shard < SHARDS {
            offsets[shard] = running;
            running = running
                .checked_add(counts[shard])
                .ok_or(SealError::Overflow)?;
            shard += 1;
        }
        Ok(Self {
            offsets,
            participant_count: running,
        })
    }

    /// Reconstructs a sealed index space from the prefix offsets and cohort
    /// size already published for an event, without re-reading the shard
    /// counts. The read path uses this to resolve positions from the values a
    /// reader already fetched.
    #[must_use]
    pub fn from_parts(offsets: [u64; SHARDS], participant_count: u64) -> Self {
        Self {
            offsets,
            participant_count,
        }
    }

    /// The cohort size `N` — the number of registration indices issued across
    /// all shards, and the domain size for [`crate::prp`].
    #[must_use]
    pub fn participant_count(&self) -> u64 {
        self.participant_count
    }

    /// The prefix offset for a shard, i.e. `Σ counts[0..s)`.
    #[must_use]
    pub fn offset(&self, shard: usize) -> u64 {
        self.offsets[shard]
    }

    /// The number of local indices issued to a shard: the width between its
    /// offset and the next shard's offset (or the cohort size, for the last
    /// shard). `None` if `shard` is out of range or the offsets are
    /// inconsistent (a later offset behind an earlier one).
    fn shard_count(&self, shard: usize) -> Option<u64> {
        let start = *self.offsets.get(shard)?;
        let end = self
            .offsets
            .get(shard + 1)
            .copied()
            .unwrap_or(self.participant_count);
        end.checked_sub(start)
    }

    /// Reconstructs a visitor's assignment from their stored `(shard, local
    /// index)`.
    ///
    /// Returns [`Assignment::PreQueue`] with the global index `i = offset[s] +
    /// l` when `l` is within shard `s`'s own issued count (derived from the
    /// gap to the next shard's offset), and [`Assignment::LiveJoin`] when `l`
    /// is at or past it — a local index claimed after the seal read that
    /// shard's count. This is deliberately a **per-shard** bound rather than
    /// a global `i < N` test: a global test admits an over-count on shard `s`
    /// whose reconstructed index still lands inside `[0, N)`, because that
    /// range legitimately belongs to shards after `s` — the same global index
    /// would then be handed to two visitors.
    ///
    /// A `shard` outside `[0, SHARDS)` has no sealed offset, so it also
    /// degrades to a live join rather than panicking.
    #[must_use]
    pub fn assign(&self, shard: usize, local_index: u64) -> Assignment {
        let Some(&offset) = self.offsets.get(shard) else {
            return Assignment::LiveJoin;
        };
        let Some(count) = self.shard_count(shard) else {
            return Assignment::LiveJoin;
        };
        if local_index >= count {
            return Assignment::LiveJoin;
        }
        match offset.checked_add(local_index) {
            Some(index) => Assignment::PreQueue { index },
            None => Assignment::LiveJoin,
        }
    }
}

/// A registration shard index in `0..SHARDS`.
///
/// Drawn uniformly at random per Lambda invocation (issue #59) rather than
/// hashed from `request_id`: a client-supplied id gave an attacker roughly ten
/// tries to steer which shard — and, through it, the seal's prefix offsets —
/// their registration landed on. Determinism was never load-bearing: every
/// caller draws the value once at write time and the row stores it, so a
/// retried claim never needs to reproduce the same shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard(usize);

/// Failure drawing random bytes for a [`Shard`]. Never silently defaulted —
/// `unwrap_used` and `panic` are denied workspace-wide, so a caller must
/// decide how to degrade (e.g. failing the batch, or skipping a non-essential
/// arrival record).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("failed to draw random bytes for a shard")]
pub struct RandError;

impl Shard {
    /// Draws a shard uniformly from `0..SHARDS` using `aws-lc-rs`'s system
    /// RNG.
    ///
    /// Reduces a random `u64` mod `SHARDS` rather than rejection-sampling: the
    /// bias this introduces is about `SHARDS / 2^64`, roughly `10^-19` per
    /// shard, and shard balance affects only write spreading across partition
    /// keys — never the permutation's bijectivity or uniformity, which are
    /// proven independently in `crate::prp`.
    ///
    /// # Errors
    ///
    /// [`RandError`] if the underlying RNG call fails.
    pub fn random() -> Result<Self, RandError> {
        let mut bytes = [0u8; 8];
        aws_lc_rs::rand::fill(&mut bytes).map_err(|_err| RandError)?;
        let index = usize::try_from(u64::from_be_bytes(bytes) % SHARDS as u64).unwrap_or(0);
        Ok(Self(index))
    }

    /// Builds a shard from a stored index, or `None` if it is out of range.
    #[must_use]
    pub fn new(index: usize) -> Option<Self> {
        (index < SHARDS).then_some(Self(index))
    }

    /// The raw shard index, always `< SHARDS`.
    #[must_use]
    pub fn index(self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure"
    )]

    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;
    use crate::{Seed, prp};

    /// Seal for tests, panicking on the unreachable overflow case.
    fn seal_ok(counts: [u64; SHARDS]) -> SealedOffsets {
        SealedOffsets::seal(counts).unwrap()
    }

    #[test]
    fn offsets_are_prefix_sums() {
        let sealed = seal_ok([3, 0, 5, 1, 0, 0, 2, 0, 0, 4]);
        assert_eq!(sealed.offset(0), 0);
        assert_eq!(sealed.offset(1), 3);
        assert_eq!(sealed.offset(2), 3);
        assert_eq!(sealed.offset(3), 8);
        assert_eq!(sealed.offset(6), 9);
        assert_eq!(sealed.offset(9), 11);
        assert_eq!(sealed.participant_count(), 15);
    }

    #[test]
    fn empty_cohort_is_zero() {
        let sealed = seal_ok([0; SHARDS]);
        assert_eq!(sealed.participant_count(), 0);
        // Any local index in any shard is a live join over an empty cohort.
        assert_eq!(sealed.assign(0, 0), Assignment::LiveJoin);
    }

    #[test]
    fn seal_overflow_is_reported_not_wrapped() {
        let counts = [u64::MAX, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(SealedOffsets::seal(counts), Err(SealError::Overflow));
    }

    #[test]
    fn straggler_past_total_is_live_join() {
        // A straggler is defined by overshooting the SHARD's own issued
        // count, not by the reconstructed global index landing inside [0, N):
        // an interior over-count still reconstructs to an index inside
        // [0, N), but that index belongs to a shard after it, so treating it
        // as pre-queue would hand two visitors the same position.
        // Counts [3,0,5,1,0,0,2,0,0,4] -> offsets [0,3,3,8,9,9,9,11,11,11], N=15.
        let sealed = seal_ok([3, 0, 5, 1, 0, 0, 2, 0, 0, 4]);
        // Shard 2's own count is 5 (offset[3] - offset[2] = 8 - 3): local 4 is
        // its last valid index, still pre-queue.
        assert_eq!(sealed.assign(2, 4), Assignment::PreQueue { index: 7 });
        // Local 11 reconstructs to i = 3 + 11 = 14 < 15 — inside [0, N) and,
        // under the old global-index rule, indistinguishable from shard 9's
        // legitimate local 3 (which also reconstructs to i = 14). The
        // per-shard rule catches it: local 11 is far past shard 2's own
        // count of 5, so it is a straggler, not a second claimant of index 14.
        assert_eq!(sealed.assign(2, 11), Assignment::LiveJoin);
        // Last shard (offset 11, count 4): local 3 -> i = 14, still pre-queue;
        // local 4 -> past the shard's own count, a straggler.
        assert_eq!(sealed.assign(9, 3), Assignment::PreQueue { index: 14 });
        assert_eq!(sealed.assign(9, 4), Assignment::LiveJoin);
    }

    #[test]
    fn shard_out_of_range_is_live_join() {
        let sealed = seal_ok([1; SHARDS]);
        assert_eq!(sealed.assign(SHARDS, 0), Assignment::LiveJoin);
    }

    #[test]
    fn random_shard_is_always_in_range() {
        for _ in 0..1000 {
            let shard = Shard::random().unwrap();
            assert!(shard.index() < SHARDS);
        }
    }

    #[test]
    fn new_rejects_an_out_of_range_index() {
        assert_eq!(Shard::new(SHARDS - 1).map(Shard::index), Some(SHARDS - 1));
        assert_eq!(Shard::new(SHARDS), None);
    }

    /// For any per-shard counts, reconstructing every issued (shard, local
    /// index) yields exactly the contiguous range [0, N) — no gap, no
    /// duplicate — which is the domain the permutation requires.
    #[test]
    fn assembled_index_space_is_contiguous() {
        let counts = [3u64, 0, 5, 1, 0, 7, 2, 0, 0, 4];
        let sealed = seal_ok(counts);
        let n = sealed.participant_count();
        let mut seen = BTreeSet::new();
        for (shard, &count) in counts.iter().enumerate() {
            for local in 0..count {
                if let Assignment::PreQueue { index } = sealed.assign(shard, local) {
                    assert!(seen.insert(index), "duplicate global index {index}");
                } else {
                    panic!("issued index (shard {shard}, local {local}) is not pre-queue");
                }
            }
        }
        let expected: BTreeSet<u64> = (0..n).collect();
        assert_eq!(seen, expected, "index space is not contiguous [0, {n})");
    }

    proptest! {
        /// For arbitrary shard counts, the assembled index space is exactly the
        /// contiguous [0, N) — the invariant the permutation domain rests on.
        #[test]
        fn prop_contiguous_index_space(counts in prop::array::uniform10(0u64..300)) {
            let sealed = seal_ok(counts);
            let n = sealed.participant_count();
            let mut seen = BTreeSet::new();
            for (shard, &count) in counts.iter().enumerate() {
                for local in 0..count {
                    match sealed.assign(shard, local) {
                        Assignment::PreQueue { index } => {
                            prop_assert!(seen.insert(index), "duplicate index {}", index);
                        }
                        Assignment::LiveJoin => {
                            prop_assert!(false, "issued index degraded to live join");
                        }
                    }
                }
            }
            let expected: BTreeSet<u64> = (0..n).collect();
            prop_assert_eq!(seen, expected);
        }

        /// Burned slot: with an injected registration-write failure (counter
        /// incremented, row absent), N = Σ shard counts is unchanged, PRP stays
        /// bijective over [0, N), and the burned index still resolves to a valid
        /// position that no row claims — no duplicate, no panic, no gap.
        #[test]
        fn prop_burned_slot_leaves_permutation_intact(
            counts in prop::array::uniform10(0u64..80),
            seed_byte: u8,
            burn_pick: u64,
        ) {
            let sealed = seal_ok(counts);
            let n = sealed.participant_count();
            prop_assume!(n > 0);
            let seed = Seed([seed_byte; 32]);

            // N counts indices issued, unaffected by which rows survived.
            prop_assert_eq!(n, counts.iter().sum::<u64>());

            // Pick one issued index to "burn" (its row write failed).
            let burned = burn_pick % n;

            // PRP is a bijection over [0, N): every index, burned or not, maps to
            // a distinct position in [0, N).
            let mut positions = BTreeSet::new();
            for i in 0..n {
                let p = prp(&seed, i, n);
                prop_assert!(p < n);
                prop_assert!(positions.insert(p), "PRP collision at index {}", i);
            }
            prop_assert_eq!(u64::try_from(positions.len()).unwrap(), n);

            // The burned index still resolves to a valid position; a reader
            // advancing past it simply admits nobody (no row to serve).
            let burned_position = prp(&seed, burned, n);
            prop_assert!(burned_position < n);
        }

        /// A local index at or past a shard's own issued count is a live join
        /// regardless of WHICH shard — not only the last one, where an
        /// overshoot happens to also cross the global total. This is the
        /// case the old `i >= N` rule missed: an interior shard's overshoot.
        #[test]
        fn prop_straggler_is_per_shard_not_only_last(
            counts in prop::array::uniform10(1u64..80),
            shard in 0usize..SHARDS,
            overshoot in 0u64..50,
        ) {
            let sealed = seal_ok(counts);
            let straggler_local = counts[shard] + overshoot;
            prop_assert_eq!(sealed.assign(shard, straggler_local), Assignment::LiveJoin);
            // And the shard's own last valid index is still pre-queue, in domain.
            let last_valid = counts[shard] - 1;
            match sealed.assign(shard, last_valid) {
                Assignment::PreQueue { index } => {
                    prop_assert!(index < sealed.participant_count());
                }
                Assignment::LiveJoin => prop_assert!(false, "issued index degraded to live join"),
            }
        }

        /// The direct fix for the duplicate-position bug: injecting a
        /// straggler (one local index past a shard's own count) at every
        /// shard never produces a `PreQueue` assignment that collides with
        /// another shard's legitimately-issued index. Under the old
        /// global-index rule, an interior shard's straggler could reconstruct
        /// into a later shard's legitimate range and both would resolve to
        /// the same `PreQueue { index }`.
        #[test]
        fn prop_no_duplicate_indices_with_injected_stragglers(
            counts in prop::array::uniform10(0u64..40),
        ) {
            let sealed = seal_ok(counts);
            let mut seen = BTreeSet::new();
            for (shard, &count) in counts.iter().enumerate() {
                // One local index past this shard's own count: a straggler.
                for local in 0..=count {
                    match sealed.assign(shard, local) {
                        Assignment::PreQueue { index } => {
                            prop_assert!(
                                local < count,
                                "straggler at shard {shard} local {local} resolved in-domain"
                            );
                            prop_assert!(seen.insert(index), "duplicate global index {index}");
                        }
                        Assignment::LiveJoin => {
                            prop_assert!(
                                local >= count,
                                "issued index (shard {shard}, local {local}) degraded to live join"
                            );
                        }
                    }
                }
            }
        }
    }
}
