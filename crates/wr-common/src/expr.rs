//! `DynamoDB` keys, update expressions, and condition fragments, kept in one
//! place so the key shape and the attribute names live beside the item shapes
//! rather than scattered across handlers.
//!
//! The key builders return the complete primary key rather than a string or a
//! bare value, so the key attribute's own name is written once too. Call sites
//! pass the result straight to `set_key`, or to `BatchGetItem`, which takes
//! exactly this type.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use crate::permutation::SHARDS;

/// The partition key attribute of the `Counters` table.
const KEY_ATTR: &str = "event_id";

/// The partition key attribute of the `Tokens` table.
///
/// Named `request_id` for the admission tokens it was built for, which is a
/// misnomer for the other two things it now holds — an OIDC session id is not a
/// request id, and neither is a PKCE state. Renaming it changes the table's hash
/// key, which replaces the table.
const TOKENS_KEY_ATTR: &str = "request_id";

/// The attribute a shard item records its own index in, so a reader that
/// fetched a batch of shards knows which is which without taking the key apart
/// again.
pub const SHARD_INDEX_ATTR: &str = "s";

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
pub fn event_key(event_id: &str) -> HashMap<String, AttributeValue> {
    key(format!("EVT#{event_id}"))
}

/// Wraps a key value as the complete primary key. Returning the whole key,
/// rather than the string or a bare `AttributeValue`, means the attribute name
/// is written once as well as the key shape — call sites pass this straight to
/// `set_key` or to `BatchGetItem`.
fn key(value: String) -> HashMap<String, AttributeValue> {
    HashMap::from([(KEY_ATTR.to_owned(), AttributeValue::S(value))])
}

/// Partition key of a single-use admission token reservation.
///
/// The `Tokens` table is the one table here holding more than one kind of item
/// — token reservations, OIDC sessions, and PKCE transactions all share its key
/// space — so the tag is what keeps a session id from colliding with a token.
/// `Positions` and `PreQueue` hold one kind each and take bare ids: a tag there
/// would disambiguate nothing while costing bytes in the partition key of every
/// row, of which there is one per visitor.
#[must_use]
pub fn admission_token_key(request_id: &str) -> HashMap<String, AttributeValue> {
    tokens_key(format!("TKN#{request_id}"))
}

/// Partition key of an operator's OIDC session.
#[must_use]
pub fn oidc_session_key(session_id: &str) -> HashMap<String, AttributeValue> {
    tokens_key(format!("SESS#{session_id}"))
}

/// Partition key of a pending OIDC login, keyed by its CSRF state.
#[must_use]
pub fn pkce_transaction_key(state: &str) -> HashMap<String, AttributeValue> {
    tokens_key(format!("PKCE#{state}"))
}

fn tokens_key(value: String) -> HashMap<String, AttributeValue> {
    HashMap::from([(TOKENS_KEY_ATTR.to_owned(), AttributeValue::S(value))])
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
pub fn prequeue_shard_key(event_id: &str, shard: usize) -> HashMap<String, AttributeValue> {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    key(format!("EVT#{event_id}#PQ#{shard}"))
}

/// Partition key of one arrivals shard, incremented when a visitor claims
/// their admission and summed by the controller to measure the no-show rate.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`.
#[must_use]
pub fn arrivals_shard_key(event_id: &str, shard: usize) -> HashMap<String, AttributeValue> {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    key(format!("EVT#{event_id}#AR#{shard}"))
}

/// `SET s = :shard ADD n :one` — adds one to a shard's count and stamps which
/// shard it is. With `ReturnValue::AllNew` the returned count is the value
/// after the add, so a pre-queue registration's local index is `new - 1`.
#[must_use]
pub fn increment_shard_update() -> &'static str {
    "SET s = :shard ADD n :one"
}

/// The values [`increment_shard_update`] refers to.
///
/// Returned with the expression's placeholders already filled rather than left
/// to the call site, because an expression naming a placeholder nothing binds
/// compiles perfectly and fails only when it reaches `DynamoDB`.
///
/// # Panics
///
/// Panics if `shard >= SHARDS`.
#[must_use]
pub fn increment_shard_values(shard: usize) -> HashMap<String, AttributeValue> {
    assert!(shard < SHARDS, "shard {shard} out of range 0..{SHARDS}");
    HashMap::from([
        (":one".to_owned(), AttributeValue::N("1".to_owned())),
        (":shard".to_owned(), AttributeValue::N(shard.to_string())),
    ])
}

/// Reads a shard item's own index, or `None` if it is missing or out of range.
#[must_use]
pub fn shard_index_of<S: std::hash::BuildHasher>(
    item: &HashMap<String, AttributeValue, S>,
) -> Option<usize> {
    let shard = item
        .get(SHARD_INDEX_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<usize>().ok())?;
    (shard < SHARDS).then_some(shard)
}

/// Reads a shard item's count, defaulting to zero for a shard nothing has
/// written yet.
#[must_use]
pub fn shard_count_of<S: std::hash::BuildHasher>(item: &HashMap<String, AttributeValue, S>) -> u64 {
    item.get(SHARD_COUNT_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
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

    /// The single key value, for assertions.
    fn key_string(k: &HashMap<String, AttributeValue>) -> String {
        key_string_of(k, KEY_ATTR)
    }

    fn key_string_of(k: &HashMap<String, AttributeValue>, attr: &str) -> String {
        k.get(attr)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default()
    }

    #[test]
    fn a_shard_item_reports_its_own_index() {
        // Readers fetch shards in a batch and get them back in arbitrary order,
        // so each item says which shard it is rather than having its key taken
        // apart to find out.
        let item = HashMap::from([
            (
                SHARD_INDEX_ATTR.to_owned(),
                AttributeValue::N("7".to_owned()),
            ),
            (
                SHARD_COUNT_ATTR.to_owned(),
                AttributeValue::N("42".to_owned()),
            ),
        ]);
        assert_eq!(shard_index_of(&item), Some(7));
        assert_eq!(shard_count_of(&item), 42);

        // Out of range or absent is None, never a wrong shard.
        let bad = HashMap::from([(
            SHARD_INDEX_ATTR.to_owned(),
            AttributeValue::N(SHARDS.to_string()),
        )]);
        assert_eq!(shard_index_of(&bad), None);
        assert_eq!(shard_index_of(&HashMap::new()), None);
        // A shard nothing has written counts zero rather than failing.
        assert_eq!(shard_count_of(&HashMap::new()), 0);
    }

    #[test]
    fn shard_keys_are_distinct_partition_keys() {
        // The whole point: distinct KEYS, not distinct attributes on one item.
        // Ten attributes on a shared item share that item's 1,000 writes per
        // second; ten keys do not.
        let keys: std::collections::BTreeSet<String> = (0..SHARDS)
            .map(|s| key_string(&prequeue_shard_key("evt", s)))
            .collect();
        assert_eq!(keys.len(), SHARDS);
        assert!(
            !keys.contains("evt"),
            "a shard must not collide with the Counters item"
        );

        let arrivals: std::collections::BTreeSet<String> = (0..SHARDS)
            .map(|s| key_string(&arrivals_shard_key("evt", s)))
            .collect();
        assert_eq!(arrivals.len(), SHARDS);
        // The two counter families must not collide with each other either.
        assert!(keys.is_disjoint(&arrivals));
    }

    #[test]
    fn the_tokens_table_key_space_does_not_collide_across_kinds() {
        // Three kinds of item share this table's key. Without the tags an
        // operator session id and an admission token reservation for the same
        // string would be the same row.
        let id = "abc";
        let keys = [
            key_string_of(&admission_token_key(id), TOKENS_KEY_ATTR),
            key_string_of(&oidc_session_key(id), TOKENS_KEY_ATTR),
            key_string_of(&pkce_transaction_key(id), TOKENS_KEY_ATTR),
        ];
        let distinct: std::collections::BTreeSet<&String> = keys.iter().collect();
        assert_eq!(distinct.len(), 3, "two kinds share a key: {keys:?}");
        assert_eq!(keys[0], "TKN#abc");
        assert_eq!(keys[1], "SESS#abc");
        assert_eq!(keys[2], "PKCE#abc");
    }

    #[test]
    fn shard_keys_are_scoped_to_their_event() {
        // Two events in one table must not share a counter.
        assert_ne!(
            prequeue_shard_key("evt-a", 3),
            prequeue_shard_key("evt-b", 3)
        );
        assert_eq!(key_string(&event_key("evt")), "EVT#evt");
        assert_eq!(key_string(&prequeue_shard_key("evt", 3)), "EVT#evt#PQ#3");
        assert_eq!(key_string(&arrivals_shard_key("evt", 3)), "EVT#evt#AR#3");
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
    fn a_shard_increment_names_only_short_attributes() {
        // Attribute names are billed on every write, so the increment must not
        // carry long ones.
        assert_eq!(increment_shard_update(), "SET s = :shard ADD n :one");
        assert_eq!(SHARD_COUNT_ATTR, "n");
        assert_eq!(SHARD_INDEX_ATTR, "s");
    }

    #[test]
    fn no_expression_inlines_an_attribute_name_containing_a_hash() {
        // '#' opens an expression-attribute-name placeholder, so an attribute
        // whose name contains one cannot be written into an expression
        // literally: `ADD arrivals#4 :one` parses as the attribute `arrivals`
        // plus an undefined placeholder `#4`, and DynamoDB rejects it. That is
        // exactly how the arrivals counter failed on every admitted visitor
        // while the handler logged a warning and admitted them anyway.
        //
        // Keys are values, not expression text, so EVT#... and TKN#... are
        // unaffected — this is only about the expression strings.
        for expression in [
            increment_shard_update(),
            claim_live_block_update(),
            seal_update(),
            seal_guard(),
        ] {
            assert!(
                !expression.contains('#'),
                "{expression:?} inlines an attribute name containing '#'; \
                 it must use ExpressionAttributeNames or a name without one"
            );
        }
        assert!(!not_exists_condition("request_id").contains('#'));
    }

    #[test]
    fn every_placeholder_in_the_increment_is_bound() {
        // An expression referring to a placeholder nothing supplies is accepted
        // by the compiler and rejected by DynamoDB at runtime, so the pairing
        // is asserted here rather than discovered in a deploy.
        let expression = increment_shard_update();
        let values = increment_shard_values(3);
        for placeholder in expression.split_whitespace().filter(|t| t.starts_with(':')) {
            assert!(
                values.contains_key(placeholder),
                "{placeholder} is used but never bound"
            );
        }
        assert_eq!(values[":shard"], AttributeValue::N("3".to_owned()));
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
