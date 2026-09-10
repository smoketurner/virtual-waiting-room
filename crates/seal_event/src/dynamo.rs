//! The `aws-sdk-dynamodb`-backed [`Store`] for the seal.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
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
            .map(|shard| {
                std::collections::HashMap::from([(
                    "event_id".to_owned(),
                    AttributeValue::S(wr_common::expr::prequeue_shard_key(event_id, shard)),
                )])
            })
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

        let mut counts = [0u64; SHARDS];
        for item in out
            .responses()
            .and_then(|r| r.get(&self.counters_table))
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let Some(key) = item.get("event_id").and_then(|v| v.as_s().ok()) else {
                continue;
            };
            // A shard that never took a registration has no item at all.
            let Some(shard) =
                (0..SHARDS).find(|&s| wr_common::expr::prequeue_shard_key(event_id, s) == *key)
            else {
                continue;
            };
            counts[shard] = item
                .get(wr_common::expr::SHARD_COUNT_ATTR)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
        }
        Ok(counts)
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
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(wr_common::expr::seal_update())
            .condition_expression(wr_common::expr::seal_guard())
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
