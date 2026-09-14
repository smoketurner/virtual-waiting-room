//! The `aws-sdk-dynamodb`-backed [`Store`] for the seal.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use tokio::task::JoinSet;
use wr_common::expr::{Condition, DEMOTED_COUNT_ATTR, DEMOTION_APPLIED_ATTR, Key, Update};
use wr_common::{
    DemotionReport, Phase, PreQueueItem, SHARDS, Shard, shard_count_of, shard_index_of,
};

use crate::{ScannedRow, SealValues, Store, StoreError};

/// Parallel scan segments. The scan is bounded by page round trips, not CPU:
/// at a million rows and 1 MB pages, eight segments finish in seconds.
const SCAN_SEGMENTS: i32 = 8;

/// Tail-index writes in flight at once. The pre-queue table just absorbed
/// registration at thousands of writes per second, so this is far below what
/// it serves; it bounds this Lambda's own open connections.
const TAIL_WRITE_CONCURRENCY: usize = 32;

/// A live `DynamoDB` store bound to the counters and pre-queue tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    prequeue_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String, prequeue_table: String) -> Self {
        Self {
            client,
            counters_table,
            prequeue_table,
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
            .filter_map(Shard::new)
            .map(|shard| Key::PrequeueShard { event_id, shard }.build())
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

    async fn scan_prequeue(
        &self,
        visit: Arc<dyn Fn(ScannedRow) + Send + Sync>,
    ) -> Result<u64, StoreError> {
        let mut segments = JoinSet::new();
        for segment in 0..SCAN_SEGMENTS {
            let client = self.client.clone();
            let table = self.prequeue_table.clone();
            let visit = Arc::clone(&visit);
            segments.spawn(async move { scan_segment(client, table, segment, visit).await });
        }
        let mut scanned = 0u64;
        while let Some(joined) = segments.join_next().await {
            let count = joined.map_err(|e| StoreError(format!("scan segment panicked: {e}")))??;
            scanned = scanned.saturating_add(count);
        }
        Ok(scanned)
    }

    async fn write_seal(&self, event_id: &str, values: &SealValues) -> Result<bool, StoreError> {
        let offsets_list: Vec<AttributeValue> = values
            .offsets
            .iter()
            .map(|o| AttributeValue::N(o.to_string()))
            .collect();

        let count = AttributeValue::N(values.participant_count.to_string());
        // queue_counter starts behind the cohort *and its tail*: the live-join
        // sequence must not hand a post-seal joiner a position already owned
        // inside `[0, N)` or inside the tail `[N, N + D)`.
        let live_join_start = values
            .queue_counter_start()
            .map_err(|e| StoreError(format!("live-join start: {e}")))?;
        let mut seal = Update::new()
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
                AttributeValue::S(values.phase.as_wire_str().to_owned()),
            );
        if values.demoted_count > 0 {
            seal = seal.set(
                DEMOTED_COUNT_ATTR,
                AttributeValue::N(values.demoted_count.to_string()),
            );
        }
        let seal = seal.build();

        // The seed is written by the seal and nothing else, so its absence
        // means "not yet sealed" and a double-fire is rejected rather than
        // reseeding.
        let guard = Condition::attribute_not_exists("shuffle_seed").build();

        let mut names = seal.names;
        names.extend(guard.names);

        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(seal.expression)
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(seal.values))
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

    async fn write_report(
        &self,
        event_id: &str,
        report: &DemotionReport,
    ) -> Result<(), StoreError> {
        let mut item: HashMap<String, AttributeValue> = serde_dynamo::to_item(report)
            .map_err(|e| StoreError(format!("serialize report: {e}")))?;
        let key = Key::DemotionReport { event_id };
        item.insert(key.attr().to_owned(), AttributeValue::S(key.value()));
        self.client
            .put_item()
            .table_name(&self.counters_table)
            .set_item(Some(item))
            .send()
            .await
            .map_err(|e| StoreError(format!("put_item report: {e}")))?;
        Ok(())
    }

    async fn write_tail_indices(&self, request_ids: &[String]) -> Result<u64, StoreError> {
        let mut writes = JoinSet::new();
        let mut applied = 0u64;
        let mut failures = 0u64;
        for (tail_index, request_id) in request_ids.iter().enumerate() {
            if writes.len() >= TAIL_WRITE_CONCURRENCY
                && let Some(joined) = writes.join_next().await
            {
                tally(joined, &mut applied, &mut failures);
            }
            let client = self.client.clone();
            let table = self.prequeue_table.clone();
            let request_id = request_id.clone();
            let tail_index = u64::try_from(tail_index).unwrap_or(u64::MAX);
            writes.spawn(
                async move { write_tail_index(client, table, request_id, tail_index).await },
            );
        }
        while let Some(joined) = writes.join_next().await {
            tally(joined, &mut applied, &mut failures);
        }
        if failures > 0 {
            return Err(StoreError(format!(
                "{failures} tail index writes failed ({applied} applied)"
            )));
        }
        Ok(applied)
    }

    async fn finish_demotion(&self, event_id: &str, applied: u64) -> Result<(), StoreError> {
        let update = Update::new()
            .set(
                "phase",
                AttributeValue::S(Phase::Active.as_wire_str().to_owned()),
            )
            .set(
                DEMOTION_APPLIED_ATTR,
                AttributeValue::N(applied.to_string()),
            )
            .build();
        let mut values = update.values;
        values.insert(
            ":held".to_owned(),
            AttributeValue::S(Phase::PreQueue.as_wire_str().to_owned()),
        );
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(update.expression)
            // Only the held phase flips: an operator who already moved the
            // event on (or into maintenance) is not overridden by the seal.
            .condition_expression("phase = :held")
            .set_expression_attribute_names(Some(update.names))
            .set_expression_attribute_values(Some(values))
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                tracing::warn!(
                    event_id,
                    "phase was no longer held at pre_queue; left as is"
                );
                Ok(())
            }
            Err(e) => Err(StoreError(format!("update_item finish: {e}"))),
        }
    }
}

/// Folds one tail write's outcome into the running totals.
fn tally(
    joined: Result<Result<bool, StoreError>, tokio::task::JoinError>,
    applied: &mut u64,
    failures: &mut u64,
) {
    match joined {
        Ok(Ok(true)) => *applied = applied.saturating_add(1),
        // The row already carried a tail index: not this run's write, so not
        // counted as applied here, and never overwritten.
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::error!(error = %e, "tail index write failed");
            *failures = failures.saturating_add(1);
        }
        Err(e) => {
            tracing::error!(error = %e, "tail index write panicked");
            *failures = failures.saturating_add(1);
        }
    }
}

/// `SET d = :k` on one row, guarded by `attribute_not_exists(d)`. `Ok(false)`
/// when the row already had one.
async fn write_tail_index(
    client: Client,
    table: String,
    request_id: String,
    tail_index: u64,
) -> Result<bool, StoreError> {
    let update = Update::new()
        .set("d", AttributeValue::N(tail_index.to_string()))
        .build();
    let guard = Condition::attribute_not_exists("d").build();
    let mut names = update.names;
    names.extend(guard.names);
    let result = client
        .update_item()
        .table_name(&table)
        .set_key(Some(
            Key::Prequeue {
                request_id: &request_id,
            }
            .build(),
        ))
        .update_expression(update.expression)
        .condition_expression(guard.expression)
        .set_expression_attribute_names(Some(names))
        .set_expression_attribute_values(Some(update.values))
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
        Err(e) => Err(StoreError(format!("update_item tail {request_id}: {e}"))),
    }
}

/// One segment of the parallel scan, feeding every row to `visit`.
async fn scan_segment(
    client: Client,
    table: String,
    segment: i32,
    visit: Arc<dyn Fn(ScannedRow) + Send + Sync>,
) -> Result<u64, StoreError> {
    let mut scanned = 0u64;
    let mut pages = client
        .scan()
        .table_name(&table)
        .segment(segment)
        .total_segments(SCAN_SEGMENTS)
        // Consistent, like the shard-count read: the cohort is every row
        // written before the seal, and an eventually consistent scan could
        // miss the last seconds of registrations — exactly the ones a farm
        // times for T−0.
        .consistent_read(true)
        .into_paginator()
        .items()
        .send();
    while let Some(item) = pages.next().await {
        let item = item.map_err(|e| StoreError(format!("scan segment {segment}: {e}")))?;
        let row: PreQueueItem = match serde_dynamo::from_item(item) {
            Ok(row) => row,
            Err(e) => {
                // A row this crate cannot read is a row it cannot classify;
                // it keeps its primary slot, and the seal goes on.
                tracing::warn!(error = %e, "skipping an unreadable pre-queue row");
                continue;
            }
        };
        visit(ScannedRow {
            request_id: row.r,
            shard: row.s,
            local_index: row.l,
            telemetry: row.v,
        });
        scanned = scanned.saturating_add(1);
    }
    Ok(scanned)
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

    #[test]
    fn tally_counts_applied_and_failures_separately() {
        let mut applied = 0;
        let mut failures = 0;
        tally(Ok(Ok(true)), &mut applied, &mut failures);
        tally(Ok(Ok(false)), &mut applied, &mut failures);
        tally(
            Ok(Err(StoreError("boom".to_owned()))),
            &mut applied,
            &mut failures,
        );
        assert_eq!((applied, failures), (1, 1));
    }
}
