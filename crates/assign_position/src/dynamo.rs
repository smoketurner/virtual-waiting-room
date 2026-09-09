//! The `aws-sdk-dynamodb`-backed [`Store`] implementation.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use wr_domain::{PositionItem, PositionStatus};

use crate::{PositionWrite, Store, StoreError, WriteOutcome};

/// Seconds a `Positions` row lives past its expiry before TTL reclaims it.
const POSITION_TTL_GRACE_SECS: u64 = 86_400;
/// Seconds after admission a position is considered expired by the controller.
const POSITION_EXPIRY_SECS: u64 = 300;

/// A live `DynamoDB` store bound to the counters and positions tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    positions_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String, positions_table: String) -> Self {
        Self {
            client,
            counters_table,
            positions_table,
        }
    }
}

impl Store for DynamoStore {
    async fn claim_block(&self, event_id: &str, n: u64) -> Result<u64, StoreError> {
        let out = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(wr_domain::expr::claim_live_block_update())
            .expression_attribute_values(":n", AttributeValue::N(n.to_string()))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .map_err(|e| StoreError(format!("update_item: {e}")))?;

        let end = out
            .attributes()
            .and_then(|a| a.get("queue_counter"))
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| StoreError("queue_counter missing in ALL_NEW".to_owned()))?;
        Ok(end)
    }

    async fn put_position(&self, write: &PositionWrite) -> Result<WriteOutcome, StoreError> {
        let now = now_epoch_secs();
        let item = PositionItem {
            request_id: write.request_id.clone(),
            entry_time: write.position.to_string(),
            status: PositionStatus::Issued,
            expires_at: now + POSITION_EXPIRY_SECS,
            ttl: now + POSITION_EXPIRY_SECS + POSITION_TTL_GRACE_SECS,
        };
        let attrs: std::collections::HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).map_err(|e| StoreError(format!("serialize: {e}")))?;

        let result = self
            .client
            .put_item()
            .table_name(&self.positions_table)
            .set_item(Some(attrs))
            .condition_expression(wr_domain::expr::not_exists_condition("request_id"))
            .send()
            .await;

        match result {
            Ok(_) => Ok(WriteOutcome::Written),
            Err(SdkError::ServiceError(se))
                if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_)) =>
            {
                Ok(WriteOutcome::Duplicate)
            }
            Err(e) => Err(StoreError(format!("put_item: {e}"))),
        }
    }
}

/// Current Unix time in whole seconds.
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
