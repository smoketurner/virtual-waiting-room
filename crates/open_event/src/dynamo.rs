//! The `aws-sdk-dynamodb`-backed [`Store`] for the open.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::get_item::builders::GetItemFluentBuilder;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{Condition, Expression, Key, Update};
use wr_common::{Phase, SHARDS, Shard, shard_count_of, shard_index_of};

use crate::{OpenValues, Store, StoreError};

/// A live `DynamoDB` store bound to the counters and pre-queue tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String) -> Self {
        Self {
            client,
            counters_table,
        }
    }

    /// Builds the `is_already_open` `GetItem` — a single-attribute projection
    /// on `shuffle_seed` — without sending it, so a regression test can pin
    /// the projection shape. The open wrote `shuffle_seed` and nothing else
    /// does, so its presence is the single authoritative "already open"
    /// signal; the projection keeps the disambiguating read to one attribute
    /// regardless of how many other attributes the event item accumulates.
    /// Names `shuffle_seed` through a placeholder so a reserved word cannot
    /// silently break the projection, matching the [`wr_common::expr`]
    /// builders' convention of name-binding every attribute.
    fn build_is_already_open_req(&self, event_id: &str) -> GetItemFluentBuilder {
        self.client
            .get_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .projection_expression("#seed")
            .expression_attribute_names("#seed", "shuffle_seed")
    }

    /// A `DynamoStore` over a zero-config client for regression tests that
    /// inspect the `projection_expression`/`key` the builder emits. The
    /// builder is never sent, so no network or credentials are needed; this
    /// only exercises the SDK's fluent-builder construction.
    #[cfg(test)]
    fn for_test() -> Self {
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .build();
        Self {
            client: aws_sdk_dynamodb::Client::from_conf(conf),
            counters_table: "Counters".to_owned(),
        }
    }
}

impl Store for DynamoStore {
    async fn read_shard_counts(&self, event_id: &str) -> Result<[u64; SHARDS], StoreError> {
        // One BatchGetItem across the ten shard items rather than one GetItem
        // on a shared one. The shards are separate partition keys precisely so
        // registration writes do not contend, which means the open has to
        // gather them.
        let keys: Vec<_> = (0..SHARDS)
            .filter_map(Shard::new)
            .map(|shard| Key::PrequeueShard { event_id, shard }.build())
            .collect();

        let request = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            // Consistent: the open folds these into the cohort size, and a
            // registration missed here is a visitor with no position at all.
            .consistent_read(true)
            .build()
            .map_err(|e| StoreError(format!("build batch keys: {e}")))?;

        let out = self
            .client
            .batch_get_item()
            .request_items(&self.counters_table, request)
            .send()
            .await
            .map_err(|e| StoreError(format!("batch_get_item shards: {e}")))?;

        if out
            .unprocessed_keys()
            .is_some_and(|u| u.contains_key(&self.counters_table))
        {
            // Opening on a partial read would under-count the cohort and strand
            // every registration in the shards that were dropped.
            return Err(StoreError("shard read incomplete; retry".to_owned()));
        }

        let items = out
            .responses()
            .and_then(|r| r.get(&self.counters_table))
            .map(Vec::as_slice)
            .unwrap_or_default();
        counts_from_shard_items(items)
    }

    async fn write_open(&self, event_id: &str, values: &OpenValues) -> Result<bool, StoreError> {
        let offsets_list: Vec<AttributeValue> = values
            .offsets
            .iter()
            .map(|o| AttributeValue::N(o.to_string()))
            .collect();

        let count = AttributeValue::N(values.participant_count.to_string());
        // queue_counter starts behind the cohort: the live-join sequence must
        // not hand a post-open joiner a position already owned inside `[0, N)`.
        let live_join_start = values.participant_count;
        let open = Update::new()
            .set(
                "shuffle_seed",
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(values.seed)),
            )
            .set("participant_count", count)
            .set(
                "queue_counter",
                AttributeValue::N(live_join_start.to_string()),
            )
            .set("prequeue_offsets", AttributeValue::L(offsets_list))
            .set(
                "phase",
                AttributeValue::S(Phase::Active.as_wire_str().to_owned()),
            );
        let open = open.build();

        // The seed is written by the open and nothing else, so its absence
        // means "not yet open" and a double-fire is rejected rather than
        // reseeding. The phase clause narrows the open to the
        // `pre_queue → active` step the lifecycle is built around: an open
        // from `idle` (or any other phase) closes a pre-queue that does not
        // exist, seeds a cohort of 0, and forfeits the pre-queue stage for
        // the life of the event — so the database rejects it rather than the
        // admin path being trusted to. Both triggers — Open now and the
        // scheduled fire — funnel through this one conditional update.
        let guard = open_guard();

        let mut names = open.names;
        names.extend(guard.names);
        // The seed-absence guard binds no values, but the phase clause does,
        // so the guard's values must travel with the update's values on the
        // same request or DynamoDB rejects it for an unbound placeholder.
        let mut values = open.values;
        values.extend(guard.values);

        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(open.expression)
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                Ok(false)
            }
            Err(e) => Err(StoreError(format!("update_item: {e}"))),
        }
    }

    async fn is_already_open(&self, event_id: &str) -> Result<bool, StoreError> {
        // A GetItem projecting only shuffle_seed: the open wrote it and
        // nothing else does, so its presence is the single authoritative
        // "already open" signal. The projection keeps the read to one
        // attribute however large the event item grows. This disambiguates a
        // write_open false into "already open" (seed present) vs "wrong phase"
        // (seed absent) — the two causes the narrowed open_guard collapses
        // into one ConditionalCheckFailedException. A missing event item
        // (the event does not exist) reads as "not already open" rather than
        // an error: the caller treats that as a wrong-phase rejection and
        // surfaces it, which is correct — an open against a non-existent
        // event cannot succeed either.
        let out = self
            .build_is_already_open_req(event_id)
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item shuffle_seed: {e}")))?;
        Ok(out
            .item()
            .is_some_and(|item| item.contains_key("shuffle_seed")))
    }
}

/// The conditional-update guard on the open: the seed must be absent (the
/// event has not been opened yet) AND the event must be in the `pre_queue`
/// phase (the open is the `pre_queue → active` step). Both clauses land on
/// one `ConditionExpression` so a double-fire, an Open now from the wrong
/// phase, or a scheduled fire on an `idle` event all reject in the same
/// `UpdateItem` — rather than the open seeding a cohort of 0 and forfeiting
/// the pre-queue stage the lifecycle is built around.
///
/// Extracted so the unit test can pin the clause shape and the bindings; the
/// SDK call itself is the untested boundary (see the repo's testing notes).
fn open_guard() -> Expression {
    Condition::attribute_not_exists("shuffle_seed")
        .and_equals(
            "phase",
            AttributeValue::S(Phase::PreQueue.as_wire_str().to_owned()),
        )
        .build()
}

/// Folds a batch-get response's shard items into per-shard counts.
///
/// An item that reached this fold exists, so it is corruption rather than an
/// empty shard whenever either of its attributes cannot be read: a shard with
/// no registrations has no item at all (`BatchGetItem` never returns one), so
/// it never reaches this loop and correctly counts zero. An item with an
/// unreadable shard index `s` would zero the count for the wrong shard, and
/// one with an unreadable count `n` would silently zero its own; under the
/// per-shard straggler rule either failure unadmits every registrant in the
/// affected shard, so both are hard errors rather than silent zeros.
fn counts_from_shard_items(
    items: &[HashMap<String, AttributeValue>],
) -> Result<[u64; SHARDS], StoreError> {
    let mut counts = [0u64; SHARDS];
    for item in items {
        // The item says which shard it is, so a batch returned in arbitrary
        // order needs no key parsing.
        let shard = shard_index_of(item)
            .ok_or_else(|| StoreError("shard item has an unreadable shard index".to_owned()))?;
        counts[shard] = shard_count_of(item)
            .ok_or_else(|| StoreError("shard item has an unreadable count".to_owned()))?;
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        reason = "test code panics on setup failure"
    )]

    use wr_common::expr::{SHARD_COUNT_ATTR, SHARD_INDEX_ATTR};

    use super::*;

    fn shard_item(shard: usize, count: u64) -> HashMap<String, AttributeValue> {
        HashMap::from([
            (
                SHARD_INDEX_ATTR.to_owned(),
                AttributeValue::N(shard.to_string()),
            ),
            (
                SHARD_COUNT_ATTR.to_owned(),
                AttributeValue::N(count.to_string()),
            ),
        ])
    }

    #[test]
    fn a_shard_with_no_registrations_has_no_item_and_counts_zero() {
        // BatchGetItem never returns an item for a shard nothing has written;
        // the fold must still report zero for it, not fail.
        let items = vec![shard_item(0, 3), shard_item(2, 5)];
        let counts = counts_from_shard_items(&items).unwrap();
        assert_eq!(counts, [3, 0, 5, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn a_shard_item_with_an_unreadable_index_is_an_error_not_a_zero_count() {
        let corrupt = HashMap::from([(
            SHARD_COUNT_ATTR.to_owned(),
            AttributeValue::N("9".to_owned()),
        )]);
        let items = vec![shard_item(0, 3), corrupt];
        assert!(counts_from_shard_items(&items).is_err());
    }

    #[test]
    fn a_shard_item_with_an_unreadable_count_is_an_error_not_a_zero_count() {
        // Symmetric with the test above: a present item with a readable index
        // `s` but a missing count `n` is corruption — the item exists, so at
        // least one registration wrote both — yet the fold would have silently
        // zeroed its count and, under the per-shard straggler rule, demoted
        // every registrant in that shard to the back of the live-join queue.
        let corrupt = HashMap::from([(
            SHARD_INDEX_ATTR.to_owned(),
            AttributeValue::N("3".to_owned()),
        )]);
        let items = vec![shard_item(0, 3), corrupt];
        assert!(counts_from_shard_items(&items).is_err());
    }

    #[test]
    fn the_open_guard_requires_an_absent_seed_and_a_pre_queue_phase() {
        // The open is the `pre_queue → active` step. The guard must keep the
        // seed-absence clause (a double-fire must not reseed) AND add a phase
        // clause, so an open from `idle`/`maintenance`/`active` cannot reach
        // the write at the single chokepoint both triggers funnel through.
        let guard = open_guard();
        assert!(
            guard.expression.contains("attribute_not_exists("),
            "the seed-absence guard must remain: {guard:?}"
        );
        assert!(
            guard.expression.contains(" AND "),
            "the phase precondition must be AND-ed onto the seed guard: {guard:?}"
        );
        assert!(
            !guard.expression.contains(" OR "),
            "AND must not degrade to OR: {}",
            guard.expression
        );
        assert_eq!(
            guard.names.len(),
            2,
            "one name placeholder per referenced attribute (shuffle_seed, phase): {guard:?}"
        );
        assert!(
            guard.names.values().any(|v| v == "shuffle_seed"),
            "shuffle_seed must stay name-bound: {guard:?}"
        );
        assert!(
            guard.names.values().any(|v| v == "phase"),
            "phase must be name-bound: {guard:?}"
        );
        // The seed-absence guard binds no values, so this one value is the
        // phase clause's — and the one the caller must merge into
        // ExpressionAttributeValues (which write_open now does).
        assert_eq!(guard.values.len(), 1, "exactly the phase value: {guard:?}");
        let phase_value = guard.values.values().next().unwrap();
        assert!(
            matches!(phase_value, AttributeValue::S(s) if s == Phase::PreQueue.as_wire_str()),
            "the phase value must be the pre_queue wire string: {phase_value:?}"
        );
    }

    #[test]
    fn is_already_open_projects_only_shuffle_seed_on_the_event_item() {
        // is_already_open disambiguates a write_open false. It must project only
        // the one attribute that names "already open" — `shuffle_seed`, which
        // the open wrote and nothing else does — so the read stays one
        // attribute however large the event item grows, and a missing event
        // item (the event does not exist) reads as "not already open" rather
        // than an error. The FakeStore double cannot catch a regression that
        // drops the projection or names the wrong attribute, so this pins the
        // shape on the live builder the trait method actually sends.
        let store = DynamoStore::for_test();
        let req = store.build_is_already_open_req("evt");
        assert_eq!(
            req.get_projection_expression().as_deref(),
            Some("#seed"),
            "is_already_open must project shuffle_seed through a name placeholder"
        );
        let names = req
            .get_expression_attribute_names()
            .as_ref()
            .expect("is_already_open binds #seed to shuffle_seed");
        assert_eq!(
            names.get("#seed").map(String::as_str),
            Some("shuffle_seed"),
            "the #seed name placeholder must bind to shuffle_seed"
        );
        // The key addresses the event item (the open's target), not a shard.
        let key = req.get_key().as_ref().expect("is_already_open sets a key");
        assert_eq!(
            key.get("event_id")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str),
            Some("EVT#evt"),
            "is_already_open must address the event item"
        );
    }

    /// Live disambiguation test against DynamoDB-Local. Validates the part the
    /// `cfg(test)` builder test and `FakeStore` cannot reach: that a real
    /// `write_open` against a `phase = idle` (no `shuffle_seed`) event returns
    /// `Ok(false)` (the narrowed `open_guard` rejects), that
    /// `is_already_open` then reads `Ok(false)` (no seed — the wrong-phase
    /// disambiguator), that nothing was written, and that after moving the
    /// phase to `pre_queue` the next `write_open` returns `Ok(true)` with the
    /// seed/phase/count/offsets all written. Skipped unless `DDB_LOCAL_ENDPOINT`
    /// is set, so the normal `cargo test` run is unaffected; run with
    /// `cargo test -p open_event -- --ignored live_open_disambiguates_wrong_phase_from_already_open`.
    #[tokio::test]
    #[ignore = "requires DDB_LOCAL_ENDPOINT (DynamoDB-Local)"]
    #[expect(
        clippy::too_many_lines,
        reason = "live DDB-Local: table setup + a two-phase (wrong-phase then pre_queue) scenario under test"
    )]
    async fn live_open_disambiguates_wrong_phase_from_already_open() {
        use crate::open_values;

        let endpoint = std::env::var("DDB_LOCAL_ENDPOINT")
            .expect("DDB_LOCAL_ENDPOINT must point at a running DynamoDB-Local");
        let table = format!("CountersTest_{}", std::process::id());
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .endpoint_url(endpoint)
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "key", "secret", None, None, "test",
            ))
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(conf);
        let store = DynamoStore::new(client.clone(), table.clone());

        client
            .create_table()
            .table_name(&table)
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("event_id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("event_id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .expect("create_table");

        // Seed: an event seeded `idle` with no shuffle_seed — exactly the path
        // the scheduled open rejects. The store addresses the row by the
        // `EVT#<event_id>` prefixed key (`Key::Event`), so the seed uses that
        // key, not the bare id.
        let key = Key::Event { event_id: "evt" }.build();
        client
            .put_item()
            .table_name(&table)
            .set_item(Some(key.clone()))
            .item("phase", AttributeValue::S("idle".to_owned()))
            .send()
            .await
            .expect("seed put");

        // A wrong-phase open: the narrowed open_guard (seed absent AND
        // phase = pre_queue) rejects because phase = idle, so write_open
        // returns Ok(false) — the same false a double-fire would, but with no
        // seed present.
        let values = open_values([0; SHARDS], [9u8; 32]).unwrap();
        let wrote = store
            .write_open("evt", &values)
            .await
            .expect("write_open returns Ok");
        assert!(
            !wrote,
            "a wrong-phase open must reject (Ok(false)), not write"
        );

        // The disambiguating read: no shuffle_seed means NOT already open, so
        // a caller must treat this as a wrong-phase rejection (Err), not an
        // already-open no-op. This is the read the open_event disambiguation
        // branches on.
        let already = store
            .is_already_open("evt")
            .await
            .expect("is_already_open returns Ok");
        assert!(
            !already,
            "an idle event with no seed is not already open; the guard failed on phase, not on the seed"
        );

        // Net effect: nothing was written. The event stays idle with no seed —
        // the protective rejection the phase clause exists for.
        let item = client
            .get_item()
            .table_name(&table)
            .set_key(Some(key.clone()))
            .send()
            .await
            .expect("get_item")
            .item
            .expect("item exists");
        assert_eq!(
            item.get("phase")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str),
            Some("idle"),
            "a rejected open must not flip the phase"
        );
        assert!(
            !item.contains_key("shuffle_seed"),
            "a rejected open must not write the seed"
        );
        assert!(
            !item.contains_key("participant_count"),
            "a rejected open must not write the cohort size"
        );

        // Move the phase to pre_queue (the operator's lifecycle step), then
        // re-open: the guard now passes, and the open writes the seed, cohort
        // size, offsets, queue_counter, and active phase together.
        client
            .update_item()
            .table_name(&table)
            .set_key(Some(key.clone()))
            .update_expression("SET phase = :p")
            .expression_attribute_values(":p", AttributeValue::S("pre_queue".to_owned()))
            .send()
            .await
            .expect("move to pre_queue");

        let wrote = store
            .write_open("evt", &values)
            .await
            .expect("write_open returns Ok");
        assert!(wrote, "an open from pre_queue must succeed (Ok(true))");

        let item = client
            .get_item()
            .table_name(&table)
            .set_key(Some(key.clone()))
            .send()
            .await
            .expect("get_item")
            .item
            .expect("item exists");
        assert_eq!(
            item.get("phase")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str),
            Some("active"),
            "a successful open flips the phase to active"
        );
        assert!(
            item.contains_key("shuffle_seed"),
            "a successful open writes the seed"
        );
        assert_eq!(
            item.get("participant_count")
                .and_then(|v| v.as_n().ok())
                .map(String::as_str),
            Some("0"),
            "an empty cohort opens to participant_count = 0"
        );
        assert!(
            item.contains_key("prequeue_offsets"),
            "a successful open writes the prefix offsets"
        );

        // And the disambiguator now reads "already open" — a retried fire or a
        // double-fire must land in AlreadyOpen, not retry forever.
        let already = store
            .is_already_open("evt")
            .await
            .expect("is_already_open returns Ok");
        assert!(already, "after a successful open the seed is present");

        let _ = client.delete_table().table_name(&table).send().await;
    }
}
