//! The `aws-sdk-dynamodb`-backed [`Store`] for the controller.
//!
//! The controller reads and writes one table. The event's own state is a
//! single-item `GetItem`/`UpdateItem`. The arrivals shards are separate items
//! under their own partition keys, read with one `BatchGetItem` and each given
//! its cap with its own `UpdateItem`.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{Key, SHARD_CAP_ATTR, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR, Update};
use wr_common::{Phase, SHARDS, Shard, StoredControl};
use wr_common::{shard_cap_of, shard_count_of, shard_index_of};

use crate::{
    ControllerState, PassDecision, PassInputs, ReleaseOutcome, ShardBudget, Store, StoreError,
};

/// A live `DynamoDB` store bound to the counters table.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    event_id: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String, event_id: String) -> Self {
        Self {
            client,
            counters_table,
            event_id,
        }
    }

    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
}

fn num(item: &HashMap<String, AttributeValue>, attr: &str) -> u64 {
    item.get(attr)
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

impl Store for DynamoStore {
    async fn read_state(&self, event_id: &str) -> Result<ControllerState, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item counters: {e}")))?;

        let item = out
            .item()
            .ok_or_else(|| StoreError(format!("no Counters item for event {event_id}")))?;

        let phase = item
            .get("phase")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<Phase>().ok())
            .unwrap_or(Phase::Idle);

        // An unreadable control decays to `Paused` in `stored_control_of`; say
        // so, because a controller that silently holds looks like one that is
        // working.
        let stored_control = wr_common::stored_control_of(item);
        if stored_control == StoredControl::Paused
            && item.get("admission_control").and_then(|v| v.as_s().ok())
                != Some(&StoredControl::Paused.as_wire_str().to_owned())
        {
            tracing::error!(
                event_id,
                event = "admission_control_unreadable",
                "stored admission control could not be parsed; holding admission"
            );
        }

        Ok(ControllerState {
            phase,
            stored_control,
            fail_open_until: num(item, "fail_open_until"),
            inputs: PassInputs {
                shards: self.read_shards(event_id).await?,
                serving_counter: num(item, "serving_counter"),
                queue_counter: num(item, "queue_counter"),
                target_rate: u32::try_from(num(item, "target_rate")).unwrap_or(0),
                last_open_pass_at: num(item, "last_open_pass_at"),
            },
        })
    }

    async fn write_release(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> Result<ReleaseOutcome, StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression("SET serving_counter = :next, last_open_pass_at = :at")
            .condition_expression(
                "attribute_not_exists(serving_counter) OR serving_counter = :expected",
            )
            .expression_attribute_values(
                ":next",
                AttributeValue::N(decision.next_serving_counter.to_string()),
            )
            .expression_attribute_values(
                ":at",
                AttributeValue::N(decision.last_open_pass_at.to_string()),
            )
            .expression_attribute_values(
                ":expected",
                AttributeValue::N(expected_serving_counter.to_string()),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(ReleaseOutcome::Advanced),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                Ok(ReleaseOutcome::LostRace)
            }
            Err(e) => Err(StoreError(format!("update_item release: {e}"))),
        }
    }

    async fn write_caps(&self, event_id: &str, caps: &[u64; SHARDS]) -> Result<(), StoreError> {
        for (index, cap) in caps.iter().enumerate() {
            let shard = Shard::new(index)
                .ok_or_else(|| StoreError(format!("shard index {index} out of range")))?;
            // The count is created at zero beside the cap: `generate_token`
            // takes budget with `n < c`, and a comparison against an absent
            // attribute is false, so a shard with a cap and no count would
            // admit nobody.
            let grant = Update::new()
                .set(SHARD_CAP_ATTR, AttributeValue::N(cap.to_string()))
                .set(SHARD_INDEX_ATTR, AttributeValue::N(index.to_string()))
                .set_if_not_exists(SHARD_COUNT_ATTR, AttributeValue::N("0".to_owned()))
                .build();
            self.client
                .update_item()
                .table_name(&self.counters_table)
                .set_key(Some(Key::ArrivalsShard { event_id, shard }.build()))
                .update_expression(grant.expression)
                .set_expression_attribute_names(Some(grant.names))
                .set_expression_attribute_values(Some(grant.values))
                .send()
                .await
                .map_err(|e| StoreError(format!("update_item shard {index} cap: {e}")))?;
        }
        Ok(())
    }
}

impl DynamoStore {
    /// One `BatchGetItem` over the ten arrivals shards, read consistently: the
    /// caps written from this read are `taken` plus a share, so a stale count
    /// would re-grant budget that has already been spent.
    async fn read_shards(&self, event_id: &str) -> Result<[ShardBudget; SHARDS], StoreError> {
        let keys: Vec<_> = (0..SHARDS)
            .filter_map(Shard::new)
            .map(|shard| Key::ArrivalsShard { event_id, shard }.build())
            .collect();

        let request = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .consistent_read(true)
            .build()
            .map_err(|e| StoreError(format!("build arrivals keys: {e}")))?;

        let out = self
            .client
            .batch_get_item()
            .request_items(&self.counters_table, request)
            .send()
            .await
            .map_err(|e| StoreError(format!("batch_get_item arrivals: {e}")))?;

        // A partial batch must not be folded: a shard that was not returned
        // would read as holding no budget and having admitted nobody.
        if out
            .unprocessed_keys()
            .is_some_and(|u| u.contains_key(&self.counters_table))
        {
            return Err(StoreError("arrivals read incomplete; retry".to_owned()));
        }
        let items = out
            .responses()
            .and_then(|r| r.get(&self.counters_table))
            .map_or_default(Vec::as_slice);
        shards_from_items(items)
    }
}

/// Folds `BatchGetItem` responses into the per-shard budgets. A shard with no
/// item has admitted nobody and holds no budget.
///
/// An item with a readable index and an unreadable count is corruption, and an
/// error: defaulting it to zero would grant that shard a cap far below what it
/// has already taken, closing it. An absent cap is not -- `generate_token`
/// creates a shard's count only through a cap, but a shard never granted one
/// is a legitimate state for the first pass to find.
fn shards_from_items(
    items: &[HashMap<String, AttributeValue>],
) -> Result<[ShardBudget; SHARDS], StoreError> {
    let mut shards = [ShardBudget::default(); SHARDS];
    for item in items {
        let Some(index) = shard_index_of(item) else {
            continue;
        };
        let taken = shard_count_of(item)
            .ok_or_else(|| StoreError("arrivals item has an unreadable count".to_owned()))?;
        if let Some(slot) = shards.get_mut(index) {
            *slot = ShardBudget {
                taken,
                cap: shard_cap_of(item).unwrap_or(0),
            };
        }
    }
    Ok(shards)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    fn shard_item(shard: usize, taken: u64, cap: Option<u64>) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::from([
            (
                SHARD_INDEX_ATTR.to_owned(),
                AttributeValue::N(shard.to_string()),
            ),
            (
                SHARD_COUNT_ATTR.to_owned(),
                AttributeValue::N(taken.to_string()),
            ),
        ]);
        if let Some(cap) = cap {
            item.insert(
                SHARD_CAP_ATTR.to_owned(),
                AttributeValue::N(cap.to_string()),
            );
        }
        item
    }

    #[test]
    fn a_shard_with_no_item_has_taken_nothing_and_holds_no_budget() {
        let items = vec![shard_item(0, 3, Some(5)), shard_item(2, 5, Some(5))];
        let shards = shards_from_items(&items).unwrap();
        assert_eq!(shards[0], ShardBudget { taken: 3, cap: 5 });
        assert_eq!(shards[1], ShardBudget::default());
        assert_eq!(shards[2].unused(), 0);
    }

    #[test]
    fn a_shard_without_a_cap_holds_no_budget() {
        let shards = shards_from_items(&[shard_item(4, 9, None)]).unwrap();
        assert_eq!(shards[4], ShardBudget { taken: 9, cap: 0 });
        assert_eq!(shards[4].unused(), 0);
    }

    #[test]
    fn an_item_with_an_unreadable_count_is_an_error_not_a_zero_count() {
        let corrupt = HashMap::from([(
            SHARD_INDEX_ATTR.to_owned(),
            AttributeValue::N("3".to_owned()),
        )]);
        assert!(shards_from_items(&[shard_item(0, 3, Some(4)), corrupt]).is_err());
    }

    #[test]
    fn an_item_with_an_unreadable_index_is_skipped() {
        let corrupt = HashMap::from([(
            SHARD_COUNT_ATTR.to_owned(),
            AttributeValue::N("9".to_owned()),
        )]);
        let shards = shards_from_items(&[shard_item(0, 3, Some(4)), corrupt]).unwrap();
        assert_eq!(shards[0], ShardBudget { taken: 3, cap: 4 });
        assert_eq!(unused_total(shards), 1);
    }

    fn unused_total(shards: [ShardBudget; SHARDS]) -> u64 {
        crate::unused_budget(shards)
    }
}
