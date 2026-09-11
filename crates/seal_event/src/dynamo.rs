//! The `aws-sdk-dynamodb`-backed [`Store`] for the seal.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{
    event_key, prequeue_shard_key, seal_guard, seal_update, shard_count_of, shard_index_of,
};
use wr_common::{Phase, SHARDS};

use crate::{SealValues, Store, StoreError};

/// A live `DynamoDB` store bound to the counters table.
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
}

impl Store for DynamoStore {
    async fn read_shard_counts(&self, event_id: &str) -> Result<[u64; SHARDS], StoreError> {
        // One BatchGetItem across the ten shard items rather than one GetItem
        // on a shared one. The shards are separate partition keys precisely so
        // registration writes do not contend, which means the seal has to
        // gather them.
        let keys: Vec<_> = (0..SHARDS)
            .map(|shard| prequeue_shard_key(event_id, shard))
            .collect();

        let request = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            // Consistent: the seal folds these into the cohort size, and a
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
            // Sealing on a partial read would under-count the cohort and strand
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

    async fn write_seal(&self, event_id: &str, values: &SealValues) -> Result<bool, StoreError> {
        let offsets_list: Vec<AttributeValue> = values
            .offsets
            .iter()
            .map(|o| AttributeValue::N(o.to_string()))
            .collect();

        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)))
            .update_expression(seal_update())
            .condition_expression(seal_guard())
            .expression_attribute_values(
                ":seed",
                AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(values.seed)),
            )
            .expression_attribute_values(
                ":n",
                AttributeValue::N(values.participant_count.to_string()),
            )
            .expression_attribute_values(":offsets", AttributeValue::L(offsets_list))
            .expression_attribute_values(
                ":active",
                AttributeValue::S(Phase::Active.as_wire_str().to_owned()),
            )
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
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

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
}
