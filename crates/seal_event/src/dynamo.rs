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
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item: {e}")))?;

        let item = out
            .item()
            .ok_or_else(|| StoreError(format!("no Counters item for event {event_id}")))?;

        let mut counts = [0u64; SHARDS];
        for (shard, slot) in counts.iter_mut().enumerate() {
            let attr = format!("prequeue_counter#{shard}");
            // A shard that never took a registration has no attribute; treat as 0.
            *slot = item
                .get(&attr)
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
