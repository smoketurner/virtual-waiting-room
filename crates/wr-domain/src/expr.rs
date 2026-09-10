//! `DynamoDB` update / condition expression fragments for the counter and
//! position writes, kept in one place so the attribute names live beside the
//! item shapes rather than scattered across handlers.

use wr_permutation::SHARDS;

/// The `Counters` attribute name for a pre-queue shard counter,
/// `prequeue_counter#<shard>`.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`; callers pick the shard with
/// `wr_permutation::shard_for`, which is always in range.
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
/// controller (DESIGN §7).
///
/// # Panics
///
/// Panics if `shard >= SHARDS`; callers pick the shard with
/// `wr_permutation::shard_for`, which is always in range.
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
}
