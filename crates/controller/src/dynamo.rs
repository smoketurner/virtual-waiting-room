//! The `aws-sdk-dynamodb`-backed [`Store`] for the controller.
//!
//! The controller reads and writes one table. The event's own state is a single-item `GetItem`/`UpdateItem`. The arrivals
//! shards are separate items under their own partition keys, so summing them
//! is a `BatchGetItem` rather than attributes already in hand.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::Key;
use wr_common::{Phase, SHARDS, StoredControl};
use wr_common::{Shard, shard_count_of, shard_index_of};

use crate::{
    ControllerState, NoShowState, ReleaseDecision, ReleaseInputs, ReleaseOutcome, Store, StoreError,
};

/// A live `DynamoDB` store bound to the counters and positions tables.
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
}

/// Reads a numeric `Counters` attribute as `u64`, defaulting to 0 when absent or
/// unparsable (an attribute a fresh event has never written).
fn num(item: &std::collections::HashMap<String, AttributeValue>, attr: &str) -> u64 {
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

        // A stored phase that fails to parse resolves to Idle, gating the
        // controller off rather than acting on a garbage phase.
        let phase = item
            .get("phase")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<Phase>().ok())
            .unwrap_or(Phase::Idle);

        // One rule, shared with every other reader: absent is normal
        // admission, present-and-unreadable holds. This is the component that
        // would act on a misread by releasing people into an origin an
        // operator was trying to protect, so it says so under a stable event
        // name -- a controller that silently stops releasing looks exactly
        // like one with nothing to release.
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

        let fail_open_until = num(item, "fail_open_until");

        // target_rate absent (never set by the operator) means no admission
        // target yet; treat as 0 so the controller releases nothing.
        let target_rate = u32::try_from(num(item, "target_rate")).unwrap_or(0);

        let prev_no_show = item
            .get("no_show_rate")
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|r| r.is_finite() && (0.0..=1.0).contains(r))
            .map(|smoothed_rate| NoShowState { smoothed_rate });

        let arrivals = self.read_arrivals(event_id).await?;

        Ok(ControllerState {
            phase,
            stored_control,
            fail_open_until,
            inputs: ReleaseInputs {
                arrivals,
                last_arrivals_total: num(item, "last_arrivals_total"),
                last_serving_counter: num(item, "last_serving_counter"),
                serving_counter: num(item, "serving_counter"),
                queue_counter: num(item, "queue_counter"),
                target_rate,
            },
            prev_no_show,
        })
    }

    async fn write_release(
        &self,
        event_id: &str,
        decision: &ReleaseDecision,
        expected_serving_counter: u64,
    ) -> Result<ReleaseOutcome, StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(
                "SET serving_counter = :next, last_serving_counter = :last_serving, \
                 last_arrivals_total = :arrivals_total, no_show_rate = :no_show",
            )
            // Guard against a lost race: only advance if serving_counter is still
            // what we read, so two overlapping invokes cannot double-advance.
            .condition_expression(
                "attribute_not_exists(serving_counter) OR serving_counter = :expected",
            )
            .expression_attribute_values(
                ":next",
                AttributeValue::N(decision.next_serving_counter.to_string()),
            )
            // The cursor as it was BEFORE this release. Writing the value after
            // it makes the next interval measure a release of zero, which takes
            // the "nothing to measure" branch every time and leaves the
            // controller running open-loop at the raw target forever.
            .expression_attribute_values(
                ":last_serving",
                AttributeValue::N(decision.previous_serving_counter.to_string()),
            )
            .expression_attribute_values(
                ":arrivals_total",
                AttributeValue::N(decision.arrivals_total.to_string()),
            )
            .expression_attribute_values(
                ":no_show",
                AttributeValue::N(decision.no_show.smoothed_rate.to_string()),
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
                // Another invoke advanced first; this pass's release is stale
                // and did not persist. Returning `LostRace` rather than `Ok`
                // lets `run_pass` report that it released nobody instead of
                // claiming the release the winner actually made.
                tracing::info!(event_id, "release write lost the race; skipping");
                Ok(ReleaseOutcome::LostRace)
            }
            Err(e) => Err(StoreError(format!("update_item release: {e}"))),
        }
    }
}

impl DynamoStore {
    /// Sums the arrivals shards. They are separate items under their own
    /// partition keys so that recording an arrival never contends with the
    /// sequences on the `Counters` item, which means reading them is a
    /// `BatchGetItem` rather than attributes already in hand.
    async fn read_arrivals(&self, event_id: &str) -> Result<[u64; SHARDS], StoreError> {
        let keys: Vec<_> = (0..SHARDS)
            .filter_map(Shard::new)
            .map(|shard| Key::ArrivalsShard { event_id, shard }.build())
            .collect();

        let request = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(keys))
            .build()
            .map_err(|e| StoreError(format!("build arrivals keys: {e}")))?;

        let out = self
            .client
            .batch_get_item()
            .request_items(&self.counters_table, request)
            .send()
            .await
            .map_err(|e| StoreError(format!("batch_get_item arrivals: {e}")))?;

        let items = out
            .responses()
            .and_then(|r| r.get(&self.counters_table))
            .map(Vec::as_slice)
            .unwrap_or_default();
        let arrivals = arrivals_from_shard_items(items)?;
        // An incomplete batch under-counts arrivals, which reads as a higher
        // no-show rate and releases more. Better to skip the pass than to
        // over-release on a partial read.
        if out
            .unprocessed_keys()
            .is_some_and(|u| u.contains_key(&self.counters_table))
        {
            return Err(StoreError("arrivals read incomplete; retry".to_owned()));
        }
        Ok(arrivals)
    }
}

/// Folds a batch-get response's arrivals shard items into per-shard counts.
///
/// The arrivals shards mirror the pre-queue shards the open reads: each is its
/// own item under its own partition key, so a `BatchGetItem` response only
/// contains one when something has written it. A present item whose count
/// attribute (`n`) cannot be read is therefore corruption, not an empty shard
/// — reading it as zero would under-count that shard's arrivals, which reads
/// as a higher no-show rate and over-releases, exactly the outcome the
/// `unprocessed_keys` check guards against for the partial-read case. An
/// unreadable count is a hard error rather than a silent zero.
///
/// An item whose shard index (`s`) cannot be read still has no home in
/// `[0, SHARDS)`; it is skipped as before, since there is no count to
/// misattribute it to.
fn arrivals_from_shard_items(
    items: &[HashMap<String, AttributeValue>],
) -> Result<[u64; SHARDS], StoreError> {
    let mut arrivals = [0u64; SHARDS];
    for item in items {
        let Some(shard) = shard_index_of(item) else {
            continue;
        };
        arrivals[shard] = shard_count_of(item)
            .ok_or_else(|| StoreError("arrivals item has an unreadable count".to_owned()))?;
    }
    Ok(arrivals)
}

/// The `event_id` this store is bound to, for the handler's logging.
impl DynamoStore {
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
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
    fn a_shard_with_no_arrivals_item_counts_zero() {
        // `BatchGetItem` returns no item for an arrivals shard nothing has
        // written; the fold must still report zero for it, not fail.
        let items = vec![shard_item(0, 3), shard_item(2, 5)];
        let arrivals = arrivals_from_shard_items(&items).unwrap();
        assert_eq!(arrivals, [3, 0, 5, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn an_arrivals_item_with_an_unreadable_count_is_an_error_not_a_zero_count() {
        // A present item with a readable index `s` but a missing count `n` is
        // corruption — the item exists, so something wrote both — yet the
        // silent default would zero `arrivals[shard]`, under-count arrivals for
        // that shard, read it as a higher no-show rate, and over-release.
        let corrupt = HashMap::from([(
            SHARD_INDEX_ATTR.to_owned(),
            AttributeValue::N("3".to_owned()),
        )]);
        let items = vec![shard_item(0, 3), corrupt];
        assert!(arrivals_from_shard_items(&items).is_err());
    }

    #[test]
    fn an_arrivals_item_with_an_unreadable_index_is_skipped_not_an_error() {
        // An item with an unreadable shard index has no home in
        // `[0, SHARDS)`; it is skipped rather than failing the whole batch,
        // matching the prior `continue`.
        let corrupt = HashMap::from([(
            SHARD_COUNT_ATTR.to_owned(),
            AttributeValue::N("9".to_owned()),
        )]);
        let items = vec![shard_item(0, 3), corrupt];
        let arrivals = arrivals_from_shard_items(&items).unwrap();
        assert_eq!(arrivals, [3, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    }
}
