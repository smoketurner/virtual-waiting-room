//! `DynamoDB` update / condition expression fragments for the counter and
//! position writes, kept in one place so the attribute names live beside the
//! item shapes rather than scattered across handlers.

use crate::permutation::SHARDS;

/// The attribute a shard item holds its count in.
///
/// One letter on purpose. `UpdateItem` is billed on the size of the whole item
/// including its attribute names, so a verbose name is paid for on every single
/// increment, forever, for no benefit — nothing queries by attribute name.
pub const SHARD_COUNT_ATTR: &str = "n";

/// Partition key of the event's own item, holding the sequences, phase, seal
/// outputs, and operator state.
///
/// Every key in this table is built here rather than at each call site. The
/// uppercase `EVT#` tag marks the structural part of the key so it cannot be
/// confused with the event id itself, and it keeps the key space
/// self-describing if events ever share a table.
#[must_use]
pub fn event_key(event_id: &str) -> String {
    format!("EVT#{event_id}")
}

/// Partition key of one pre-queue registration shard.
///
/// A shard is its own ITEM, not an attribute on a shared one. `DynamoDB` caps
/// throughput at 1,000 writes per second per partition key, so ten attributes
/// on one item share one budget and distribute nothing; ten items are ten
/// partition keys and ten budgets. The item is tiny, so every increment costs
/// exactly one write unit rather than the size of a growing shared item.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`; callers pick the shard with
/// [`crate::permutation::shard_for`], which is always in range.
#[must_use]
pub fn prequeue_shard_key(event_id: &str, shard: usize) -> String {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    format!("EVT#{event_id}#PQ#{shard}")
}

/// Partition key of one arrivals shard, incremented when a visitor claims
/// their admission and summed by the controller to measure the no-show rate.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`.
#[must_use]
pub fn arrivals_shard_key(event_id: &str, shard: usize) -> String {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    format!("EVT#{event_id}#AR#{shard}")
}

/// `ADD n :one` — adds one to a shard's count. With `ReturnValue::AllNew` the
/// returned value is the count after the add, so a pre-queue registration's
/// local index is `new - 1`.
#[must_use]
pub fn increment_shard_update() -> &'static str {
    "ADD n :one"
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
    fn shard_keys_are_distinct_partition_keys() {
        // The whole point: distinct KEYS, not distinct attributes on one item.
        // Ten attributes on a shared item share that item's 1,000 writes per
        // second; ten keys do not.
        let keys: std::collections::BTreeSet<String> =
            (0..SHARDS).map(|s| prequeue_shard_key("evt", s)).collect();
        assert_eq!(keys.len(), SHARDS);
        assert!(
            !keys.contains("evt"),
            "a shard must not collide with the Counters item"
        );

        let arrivals: std::collections::BTreeSet<String> =
            (0..SHARDS).map(|s| arrivals_shard_key("evt", s)).collect();
        assert_eq!(arrivals.len(), SHARDS);
        // The two counter families must not collide with each other either.
        assert!(keys.is_disjoint(&arrivals));
    }

    #[test]
    fn shard_keys_are_scoped_to_their_event() {
        // Two events in one table must not share a counter.
        assert_ne!(
            prequeue_shard_key("evt-a", 3),
            prequeue_shard_key("evt-b", 3)
        );
        assert_eq!(event_key("evt"), "EVT#evt");
        assert_eq!(prequeue_shard_key("evt", 3), "EVT#evt#PQ#3");
        assert_eq!(arrivals_shard_key("evt", 3), "EVT#evt#AR#3");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn prequeue_shard_key_out_of_range_panics() {
        let _ = prequeue_shard_key("evt", SHARDS);
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn arrivals_shard_key_out_of_range_panics() {
        let _ = arrivals_shard_key("evt", SHARDS);
    }

    #[test]
    fn a_hash_in_an_event_id_would_collide_two_keys() {
        // EVT#a#PQ#1 is both event "a"'s first pre-queue shard and event
        // "a#PQ#1"'s own item. One event per deployment makes this
        // unreachable today, and the Terraform variable rejects a '#' so it
        // stays that way — this records why that validation exists.
        assert_eq!(prequeue_shard_key("a", 1), event_key("a#PQ#1"));
    }

    #[test]
    fn a_shard_increment_names_only_the_short_attribute() {
        // Attribute names are billed on every write, so the increment must not
        // carry a long one.
        assert_eq!(increment_shard_update(), "ADD n :one");
        assert_eq!(SHARD_COUNT_ATTR, "n");
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
