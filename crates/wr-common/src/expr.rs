//! `DynamoDB` update / condition expression fragments for the counter and
//! position writes, kept in one place so the attribute names live beside the
//! item shapes rather than scattered across handlers.

use crate::permutation::SHARDS;

/// The `Counters` attribute name for a pre-queue shard counter,
/// `prequeue_counter#<shard>`.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`; callers pick the shard with
/// `crate::permutation::shard_for`, which is always in range.
#[must_use]
pub fn prequeue_shard_attr(shard: usize) -> String {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    format!("prequeue_counter#{shard}")
}

/// `ADD <shard> :one` — claims one local index in a pre-queue shard. Paired
/// with `ReturnValue::AllNew`, the returned counter value is the count after
/// the add, so the claimed local index is `new - 1`.
#[must_use]
pub fn claim_local_index_update(shard: usize) -> String {
    format!("ADD {} :one", prequeue_shard_attr(shard))
}

/// The `Counters` attribute name for an arrival shard counter,
/// `arrivals#<shard>`, incremented by the authorizer and summed by the
/// controller.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`; callers pick the shard with
/// `crate::permutation::shard_for`, which is always in range.
#[must_use]
pub fn arrivals_shard_attr(shard: usize) -> String {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    format!("arrivals#{shard}")
}

/// `ADD arrivals#<shard> :one` — the authorizer's one write per admitted
/// visitor, recording an arrival for the controller's no-show measurement.
#[must_use]
pub fn record_arrival_update(shard: usize) -> String {
    format!("ADD {} :one", arrivals_shard_attr(shard))
}

/// `ADD queue_counter :n` — claims a contiguous block of `n` live-join
/// positions. With `ReturnValue::AllNew` the returned value is the block end;
/// the block is `[end - n + 1, end]`. `n` must be the count of *valid* records.
#[must_use]
pub fn claim_live_block_update() -> &'static str {
    "ADD queue_counter :n"
}

/// `attribute_not_exists(<key>)` — the idempotency guard on every position and
/// pre-queue write, so a retried request id is rejected rather than duplicated.
#[must_use]
pub fn not_exists_condition(key_attr: &str) -> String {
    format!("attribute_not_exists({key_attr})")
}

/// The seal's single `SET` clause, writing every value the seal produces in one
/// update.
///
/// `queue_counter` is set to the same `:n` as `participant_count`: the live-join
/// sequence starts at the cohort size, so the first post-seal live joiner is
/// numbered behind the whole pre-queue cohort rather than colliding with it.
/// Without that clause `ADD queue_counter :n` would hand a live joiner position
/// 1, already owned by a pre-queue member of `[0, N)`.
#[must_use]
pub fn seal_update() -> &'static str {
    "SET shuffle_seed = :seed, participant_count = :n, queue_counter = :n, \
     prequeue_offsets = :offsets, phase = :active"
}

/// `attribute_not_exists(shuffle_seed)` — the seal's once-only guard. The seed
/// is written by the seal and nothing else, so its absence means "not yet
/// sealed" and a double-fire or retry is rejected rather than reseeding.
#[must_use]
pub fn seal_guard() -> &'static str {
    "attribute_not_exists(shuffle_seed)"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_attr_names_are_hash_suffixed() {
        assert_eq!(prequeue_shard_attr(0), "prequeue_counter#0");
        assert_eq!(prequeue_shard_attr(9), "prequeue_counter#9");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn shard_attr_out_of_range_panics() {
        let _ = prequeue_shard_attr(SHARDS);
    }

    #[test]
    fn live_block_and_condition_fragments() {
        assert_eq!(claim_live_block_update(), "ADD queue_counter :n");
        assert_eq!(
            not_exists_condition("request_id"),
            "attribute_not_exists(request_id)"
        );
    }

    #[test]
    fn shard_add_update_names_the_shard() {
        assert_eq!(claim_local_index_update(3), "ADD prequeue_counter#3 :one");
    }

    #[test]
    fn arrivals_attr_names_are_hash_suffixed() {
        assert_eq!(arrivals_shard_attr(0), "arrivals#0");
        assert_eq!(arrivals_shard_attr(9), "arrivals#9");
        assert_eq!(record_arrival_update(4), "ADD arrivals#4 :one");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn arrivals_attr_out_of_range_panics() {
        let _ = arrivals_shard_attr(SHARDS);
    }

    #[test]
    fn seal_starts_the_live_join_sequence_at_the_cohort_size() {
        // The one clause that keeps a post-seal live joiner off the pre-queue
        // cohort's [0, N): queue_counter takes the same :n as participant_count,
        // so `ADD queue_counter :1` next returns N + 1, not 1.
        let update = seal_update();
        assert!(
            update.contains("queue_counter = :n"),
            "seal must seed queue_counter; without it live joins collide with [0, N)"
        );
        assert!(update.contains("participant_count = :n"));
        assert!(update.starts_with("SET "));
    }

    #[test]
    fn seal_writes_every_value_in_one_guarded_update() {
        // All four seal outputs plus the phase flip in a single SET, guarded on
        // the seed's absence, so the seal is atomic and happens exactly once.
        let update = seal_update();
        for attr in [
            "shuffle_seed = :seed",
            "participant_count = :n",
            "queue_counter = :n",
            "prequeue_offsets = :offsets",
            "phase = :active",
        ] {
            assert!(update.contains(attr), "seal update missing {attr}");
        }
        assert_eq!(seal_guard(), "attribute_not_exists(shuffle_seed)");
    }
}
