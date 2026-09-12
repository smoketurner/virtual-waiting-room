//! The `aws-sdk-dynamodb`-backed [`Store`] implementation.

use std::collections::{HashMap, HashSet};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes, ReturnValue};
use wr_common::expr::{
    Condition, Key, POSITIONS_KEY_ATTR, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR, STATUS_ATTR,
    STATUS_EXPIRED, Update,
};
use wr_common::{Counters, PositionItem, PositionStatus, PreQueueItem, Shard, Telemetry};

use crate::{PositionWrite, PreQueueWrite, Store, StoreError, WriteOutcome};

/// How long a `Positions` row is kept before `DynamoDB` TTL reclaims it. Storage
/// hygiene only: whether a position is still claimable is decided by the
/// controller against the admission cursor, not by this.
const POSITION_TTL_SECS: u64 = 86_400;

/// The maximum number of keys `BatchGetItem` accepts in one call.
const BATCH_GET_LIMIT: usize = 100;

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

/// `Some(telemetry)` unless every field is absent, in which case the `v`
/// attribute is omitted entirely rather than writing an empty map.
fn telemetry_or_none(telemetry: Telemetry) -> Option<Telemetry> {
    (telemetry != Telemetry::default()).then_some(telemetry)
}

impl Store for DynamoStore {
    async fn claim_block(&self, event_id: &str, n: u64) -> Result<u64, StoreError> {
        // `ALL_NEW` returns the value after the add, so the claimed block is
        // `[end - n + 1, end]`.
        let claim = Update::new()
            .add("queue_counter", AttributeValue::N(n.to_string()))
            .build();
        let out = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(claim.expression)
            .set_expression_attribute_names(Some(claim.names))
            .set_expression_attribute_values(Some(claim.values))
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
            v: telemetry_or_none(write.telemetry.clone()),
        };
        let attrs: HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).map_err(|e| StoreError(format!("serialize: {e}")))?;

        let mut request = self
            .client
            .put_item()
            .table_name(&self.positions_table)
            .set_item(Some(attrs));
        // Widened for a derived request_id so a re-join can reclaim a row the
        // controller expired; `completed` and `abandoned` stay terminal.
        let guard = Condition::attribute_not_exists(POSITIONS_KEY_ATTR);
        let guard = if write.allow_expired_overwrite {
            guard.or_equals(STATUS_ATTR, AttributeValue::S(STATUS_EXPIRED.to_owned()))
        } else {
            guard
        }
        .build();
        request = request
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(guard.names))
            .set_expression_attribute_values(Some(guard.values));

        match request.send().await {
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
            .set_key(Some(Key::Event { event_id }.build()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item counters: {e}")))?;

        Ok(out.item().map(|item| Counters::from_item(event_id, item)))
    }

    async fn claim_prequeue_block(
        &self,
        event_id: &str,
        shard: Shard,
        count: u64,
    ) -> Result<u64, StoreError> {
        // Stamps which shard this is alongside the add, so a reader that
        // fetched a batch of shards does not have to take the key apart.
        // `ALL_NEW` returns the count after the add; the block is
        // `[n - count, n - 1]`.
        let claim = Update::new()
            .set(
                SHARD_INDEX_ATTR,
                AttributeValue::N(shard.index().to_string()),
            )
            .add(SHARD_COUNT_ATTR, AttributeValue::N(count.to_string()))
            .build();
        let out = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::PrequeueShard { event_id, shard }.build()))
            .update_expression(claim.expression)
            .set_expression_attribute_names(Some(claim.names))
            .set_expression_attribute_values(Some(claim.values))
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

        first_local_index(shard.index(), end, count)
    }

    async fn put_prequeue(&self, write: &PreQueueWrite) -> Result<WriteOutcome, StoreError> {
        let shard = u8::try_from(write.shard.index())
            .map_err(|_err| StoreError(format!("shard {} out of range", write.shard.index())))?;
        let item = PreQueueItem {
            r: write.request_id.clone(),
            s: shard,
            l: write.local_index,
            t: now_epoch_secs(),
            v: telemetry_or_none(write.telemetry.clone()),
        };
        let attrs: HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).map_err(|e| StoreError(format!("serialize: {e}")))?;

        let guard = Condition::attribute_not_exists(
            Key::Prequeue {
                request_id: &write.request_id,
            }
            .attr(),
        )
        .build();
        let result = self
            .client
            .put_item()
            .table_name(&self.prequeue_table)
            .set_item(Some(attrs))
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(guard.names))
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

    async fn registered_ids(&self, request_ids: &[String]) -> Result<HashSet<String>, StoreError> {
        let mut found = HashSet::new();
        for chunk in request_ids.chunks(BATCH_GET_LIMIT) {
            if chunk.is_empty() {
                continue;
            }
            let keys_and_attrs = KeysAndAttributes::builder()
                .set_keys(Some(
                    chunk
                        .iter()
                        .map(|id| Key::Prequeue { request_id: id }.build())
                        .collect(),
                ))
                .consistent_read(true)
                .projection_expression("r")
                .build()
                .map_err(|e| StoreError(format!("keys_and_attributes: {e}")))?;

            let out = self
                .client
                .batch_get_item()
                .request_items(&self.prequeue_table, keys_and_attrs)
                .send()
                .await
                .map_err(|e| StoreError(format!("batch_get_item: {e}")))?;

            // Unprocessed keys degrade to "unknown" by simply not appearing
            // in `found`: the caller treats an id's absence as "claim
            // anyway", the same degrade a store error gets.
            if let Some(items) = out
                .responses()
                .and_then(|responses| responses.get(&self.prequeue_table))
            {
                for item in items {
                    if let Some(id) = item.get("r").and_then(|v| v.as_s().ok()) {
                        found.insert(id.clone());
                    }
                }
            }
        }
        Ok(found)
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

    #[test]
    fn telemetry_or_none_omits_an_entirely_empty_value() {
        assert_eq!(telemetry_or_none(Telemetry::default()), None);
        let present = Telemetry {
            c: Some("US".to_owned()),
            ..Telemetry::default()
        };
        assert_eq!(telemetry_or_none(present.clone()), Some(present));
    }
}
