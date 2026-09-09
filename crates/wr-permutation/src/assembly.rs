//! Pre-queue index-space assembly (ADR-0015, design §4.1–4.2).
//!
//! Registration stripes `prequeue_counter` across `K = 10` shards: each
//! `POST /join` picks shard `s = hash(request_id) % 10`, claims a **local
//! index** `l` via `ADD prequeue_counter#s :1` / `ALL_NEW`, and stores
//! `PreQueue {r, s, l, t}` — never a global index.
//!
//! At T−0 the seal reads the 10 shard counts, computes prefix offsets
//! `offset[s] = Σ counts[0..s)` and cohort size `N = Σ counts`, and stores
//! them alongside the seed in one conditional write. A visitor's global
//! registration index is reconstructed on read as `i = offset[s] + l`.
//!
//! Because the shards partition the cohort and the offsets are a prefix sum,
//! the set of global indices is exactly the contiguous range `[0, N)` — the
//! domain the permutation ([`crate::prp`]) runs over. `N` counts indices
//! *issued*, not participants confirmed: an index whose `PreQueue` write failed
//! after the counter incremented is a burned slot that maps to a position no
//! one claims (the gaps-permitted property, F2.3).

/// Number of pre-queue counter shards. Fixed at 10 for every deployment
/// (ADR-0015), matching the `arrivals` shard count.
pub const SHARDS: usize = 10;

/// The sealed pre-queue index space: per-shard prefix offsets and the cohort
/// size `N`, computed once at T−0 from the shard counts and thereafter
/// published in `/status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedOffsets {
    offsets: [u64; SHARDS],
    participant_count: u64,
}

/// The outcome of reconstructing a visitor's global registration index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignment {
    /// The visitor registered before the seal counted their shard: their global
    /// index `i` is in `[0, N)` and `PRP` derives the pre-queue position.
    PreQueue { index: u64 },
    /// The visitor's local index was claimed after the seal counted their shard
    /// (a straggler racing the seal), so the reconstructed `i >= N` falls
    /// outside the permutation domain. `/queue_num` serves a live-join position
    /// behind the whole pre-queue cohort instead of evaluating `PRP` out of
    /// range (design §4.2, ADR-0015 failure mode).
    LiveJoin,
}

/// Error from sealing the pre-queue index space.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SealError {
    /// A shard count plus the running total would exceed `u64`. Unreachable in
    /// practice (the cohort target is ~`10^6`), but summed explicitly rather
    /// than wrapped.
    #[error("pre-queue cohort size overflows u64")]
    Overflow,
}

impl SealedOffsets {
    /// Seals the index space from the 10 shard counts read at T−0: computes the
    /// prefix offsets and `N = Σ counts`.
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

    /// Reconstructs a visitor's assignment from their stored `(shard, local
    /// index)`.
    ///
    /// Returns [`Assignment::PreQueue`] with the global index `i = offset[s] +
    /// l` when `i < N`, and [`Assignment::LiveJoin`] when `i >= N` (a straggler
    /// that raced the seal — never evaluate `PRP` out of domain).
    ///
    /// A `shard` outside `[0, SHARDS)` is a straggler by definition (no offset
    /// was sealed for it), so it degrades to a live join rather than panicking.
    #[must_use]
    pub fn assign(&self, shard: usize, local_index: u64) -> Assignment {
        let Some(&offset) = self.offsets.get(shard) else {
            return Assignment::LiveJoin;
        };
        match offset.checked_add(local_index) {
            Some(index) if index < self.participant_count => Assignment::PreQueue { index },
            Some(_) | None => Assignment::LiveJoin,
        }
    }
}

/// Selects the registration shard for a request id: `hash(request_id) % 10`.
///
/// Hashing (not round-robin) means a retried join lands on the same shard, so
/// the `attribute_not_exists(r)` conditional rejects the duplicate and consumes
/// no index — idempotency (F2.5) is preserved. The hash need not be
/// cryptographic; it need only spread `UUIDv7` ids uniformly mod 10.
#[must_use]
pub fn shard_for(request_id: &[u8]) -> usize {
    // FNV-1a over the id bytes, reduced mod SHARDS. Deterministic and
    // dependency-free; UUIDv7 ids are uniform mod 10 under it.
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in request_id {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    usize::try_from(hash % SHARDS as u64).unwrap_or(0)
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
        // A straggler is defined by the reconstructed GLOBAL index i >= N, not
        // by overshooting a shard's own count: an interior over-count still
        // lands inside a later shard's range and is a valid pre-queue index.
        // Counts [3,0,5,1,0,0,2,0,0,4] -> offsets [0,3,3,8,9,9,9,11,11,11], N=15.
        let sealed = seal_ok([3, 0, 5, 1, 0, 0, 2, 0, 0, 4]);
        // Shard 2 (offset 3): local 11 -> i = 14 < 15, still pre-queue;
        // local 12 -> i = 15 >= 15, a straggler.
        assert_eq!(sealed.assign(2, 11), Assignment::PreQueue { index: 14 });
        assert_eq!(sealed.assign(2, 12), Assignment::LiveJoin);
        // Last shard (offset 11): local 3 -> i = 14 < 15; local 4 -> i = 15, a
        // straggler.
        assert_eq!(sealed.assign(9, 3), Assignment::PreQueue { index: 14 });
        assert_eq!(sealed.assign(9, 4), Assignment::LiveJoin);
    }

    #[test]
    fn shard_out_of_range_is_live_join() {
        let sealed = seal_ok([1; SHARDS]);
        assert_eq!(sealed.assign(SHARDS, 0), Assignment::LiveJoin);
    }

    #[test]
    fn retried_request_id_hashes_to_same_shard() {
        let id = b"018f3a2b-7c9d-7e1f-abcd-0123456789ab";
        assert_eq!(shard_for(id), shard_for(id));
    }

    /// The whole point of the assembly: for any per-shard counts, reconstructing
    /// every issued (shard, local index) yields exactly the contiguous range
    /// [0, N) — no gap, no duplicate — which is the domain the permutation
    /// requires (ADR-0015).
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

        /// Burned slot (ADR-0015, F2.3): with an injected registration-write
        /// failure (counter incremented, PreQueue row absent), the assembled
        /// index space is still contiguous [0, N) where N = Σ shard counts, PRP
        /// remains bijective over [0, N), and a burned index resolves to a valid
        /// position that maps to no PreQueue row — no duplicate, no panic, no
        /// gap in the permutation.
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

            // Pick one issued index to "burn" (its PreQueue write failed).
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

            // The burned index still resolves to a valid position; the serving
            // counter advancing past it simply admits nobody (no row to serve).
            let burned_position = prp(&seed, burned, n);
            prop_assert!(burned_position < n);
        }

        /// Straggler racing the seal (ADR-0015): a (shard, local) whose
        /// reconstructed global index i = offset[s] + l >= N returns a live-join
        /// assignment, and any i < N returns pre-queue — PRP is never called out
        /// of domain. Uses the last shard, where l >= counts[9] is exactly the
        /// i >= N boundary.
        #[test]
        fn prop_straggler_past_total_is_live_join(
            counts in prop::array::uniform10(1u64..80),
            overshoot in 0u64..50,
        ) {
            let sealed = seal_ok(counts);
            let last = SHARDS - 1;
            // On the last shard, offset[last] + counts[last] == N, so local index
            // counts[last] + k reconstructs to i = N + k >= N.
            let straggler_local = counts[last] + overshoot;
            prop_assert_eq!(sealed.assign(last, straggler_local), Assignment::LiveJoin);
            // And the last valid index (l = counts[last] - 1) is pre-queue.
            let last_valid = counts[last] - 1;
            prop_assert_eq!(
                sealed.assign(last, last_valid),
                Assignment::PreQueue { index: sealed.participant_count() - 1 }
            );
        }
    }
}
