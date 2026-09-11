//! The `aws-sdk-dynamodb`-backed [`Store`] implementation.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use wr_common::expr::{
    SHARD_COUNT_ATTR, claim_live_block_update, claim_prequeue_block_update,
    claim_prequeue_block_values, event_key, not_exists_condition, prequeue_shard_key,
};
use wr_common::{Counters, PositionItem, PositionStatus, PreQueueItem};

use crate::{PositionWrite, PreQueueWrite, Store, StoreError, WriteOutcome};

/// How long a `Positions` row is kept before `DynamoDB` TTL reclaims it. Storage
/// hygiene only: whether a position is still claimable is decided by the
/// controller against the admission cursor, not by this.
const POSITION_TTL_SECS: u64 = 86_400;

/// A live `DynamoDB` store bound to the counters, pre-queue, and positions
/// tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    prequeue_table: String,
    positions_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(
        client: Client,
        counters_table: String,
        prequeue_table: String,
        positions_table: String,
    ) -> Self {
        Self {
            client,
            counters_table,
            prequeue_table,
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
            .set_key(Some(event_key(event_id)))
            .update_expression(claim_live_block_update())
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
            queue_position: write.position,
            entry_time: now,
            status: PositionStatus::Issued,
            ttl: now.saturating_add(POSITION_TTL_SECS),
        };
        let attrs: HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).map_err(|e| StoreError(format!("serialize: {e}")))?;

        let result = self
            .client
            .put_item()
            .table_name(&self.positions_table)
            .set_item(Some(attrs))
            .condition_expression(not_exists_condition("request_id"))
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

    async fn load_counters(&self, event_id: &str) -> Result<Option<Counters>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            // Consistent: the batch's routing decision (live vs. pre-queue)
            // and the fix-up's straggler check both need the freshest write.
            .consistent_read(true)
            .set_key(Some(event_key(event_id)))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item counters: {e}")))?;

        Ok(out.item().map(|item| Counters::from_item(event_id, item)))
    }

    async fn claim_prequeue_block(
        &self,
        event_id: &str,
        shard: usize,
        count: u64,
    ) -> Result<u64, StoreError> {
        let out = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(prequeue_shard_key(event_id, shard)))
            .update_expression(claim_prequeue_block_update())
            .set_expression_attribute_values(Some(claim_prequeue_block_values(shard, count)))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .map_err(|e| StoreError(format!("update_item prequeue shard: {e}")))?;

        let end = out
            .attributes()
            .and_then(|a| a.get(SHARD_COUNT_ATTR))
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| StoreError("shard count missing in ALL_NEW".to_owned()))?;

        first_local_index(shard, end, count)
    }

    async fn put_prequeue(&self, write: &PreQueueWrite) -> Result<WriteOutcome, StoreError> {
        let shard = u8::try_from(write.shard)
            .map_err(|_| StoreError(format!("shard {} out of range", write.shard)))?;
        let item = PreQueueItem {
            r: write.request_id.clone(),
            s: shard,
            l: write.local_index,
            t: now_epoch_secs(),
        };
        let attrs: HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).map_err(|e| StoreError(format!("serialize: {e}")))?;

        let result = self
            .client
            .put_item()
            .table_name(&self.prequeue_table)
            .set_item(Some(attrs))
            .condition_expression(not_exists_condition("r"))
            .send()
            .await;

        match result {
            Ok(_) => Ok(WriteOutcome::Written),
            Err(SdkError::ServiceError(se))
                if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_)) =>
            {
                Ok(WriteOutcome::Duplicate)
            }
            Err(e) => Err(StoreError(format!("put_item prequeue: {e}"))),
        }
    }
}

/// Current Unix time in whole seconds.
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The first local index of a just-claimed block: the shard's count after the
/// `ADD`, minus the size just added.
///
/// Errors rather than saturates: a saturated local index here would be a
/// duplicate global index, not a harmless gap (unlike the live-join block
/// claim, which saturates because a saturated live position is only a gap).
/// Pulled out of [`DynamoStore::claim_prequeue_block`] so this arithmetic is
/// unit-tested directly — the `Store` trait's fake in `lib.rs`'s tests
/// computes the block start a different way, so the real subtraction here has
/// no seam to exercise it otherwise.
fn first_local_index(
    shard: usize,
    shard_count_after_add: u64,
    count: u64,
) -> Result<u64, StoreError> {
    shard_count_after_add.checked_sub(count).ok_or_else(|| {
        StoreError(format!(
            "shard {shard} count {shard_count_after_add} underflowed claiming {count}"
        ))
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn first_local_index_is_the_shard_count_before_the_add() {
        assert_eq!(first_local_index(0, 103, 3).unwrap(), 100);
    }

    #[test]
    fn first_local_index_errors_rather_than_saturates_on_underflow() {
        // A shard count lower than the block just added to it: the ALL_NEW
        // value cannot be trusted to reconstruct a first index, so this must
        // be a StoreError, never a saturated (and silently wrong) 0.
        assert!(first_local_index(3, 2, 5).is_err());
    }
}
