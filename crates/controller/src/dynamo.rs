//! The `aws-sdk-dynamodb`-backed [`Store`] for the controller.
//!
//! The controller reads and writes one table. The event's own state is a
//! single-item `GetItem`/`UpdateItem`. The arrivals counters are separate items
//! under their own partition keys, ten to a shelf, read with `BatchGetItem` and
//! each given its cap with its own `UpdateItem`. The admission log is one more
//! item, replaced whole each pass.

use jiff::{SignedDuration, Timestamp};
use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{
    ADMISSION_LOG_ATTR, Key, SHARD_CAP_ATTR, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR, SHELF_INDEX_ATTR,
    Update,
};
use wr_common::{CALLS_ATTR, Counters, Phase, SHARDS, Shard, Shelf, StoredControl, calls_to_attr};
use wr_common::{shard_cap_of, shard_count_of, shard_index_of};

use crate::{
    AdmissionLog, CallOutcome, ControllerState, LogEntry, PassDecision, PassInputs, ShardBudget,
    Shelves, Store, StoreError,
};

/// The most keys one `BatchGetItem` takes.
const BATCH_GET_LIMIT: usize = 100;

/// A live `DynamoDB` store bound to the counters table.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    event_id: String,
    session: SignedDuration,
}

impl DynamoStore {
    #[must_use]
    pub fn new(
        client: Client,
        counters_table: String,
        event_id: String,
        session: SignedDuration,
    ) -> Self {
        Self {
            client,
            counters_table,
            event_id,
            session,
        }
    }

    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
}

/// Now, rounded down to the whole second the log records.
fn whole_second_now() -> Timestamp {
    let now = Timestamp::now();
    Timestamp::from_second(now.as_second()).unwrap_or(now)
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

        let counters = Counters::from_item(event_id, item);
        Ok(ControllerState {
            phase,
            stored_control,
            fail_open_until: counters.fail_open_until,
            inputs: PassInputs {
                shelves: self.read_shelves(event_id).await?,
                shards_read_at: whole_second_now(),
                serving_counter: num(item, "serving_counter"),
                queue_counter: num(item, "queue_counter"),
                origin_concurrency: u32::try_from(num(item, "origin_concurrency")).unwrap_or(0),
                session: self.session,
                log: self.read_log(event_id).await?,
                draw_cutoff: counters.draw_cutoff(),
                calls: counters.calls,
            },
        })
    }

    async fn write_call(
        &self,
        event_id: &str,
        decision: &PassDecision,
        expected_serving_counter: u64,
    ) -> Result<CallOutcome, StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression("SET serving_counter = :next, #calls = :calls")
            .condition_expression(
                "attribute_not_exists(serving_counter) OR serving_counter = :expected",
            )
            .expression_attribute_names("#calls", CALLS_ATTR)
            .expression_attribute_values(
                ":next",
                AttributeValue::N(decision.next_serving_counter.to_string()),
            )
            .expression_attribute_values(":calls", calls_to_attr(&decision.calls))
            .expression_attribute_values(
                ":expected",
                AttributeValue::N(expected_serving_counter.to_string()),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(CallOutcome::Called),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                Ok(CallOutcome::LostRace)
            }
            Err(e) => Err(StoreError(format!("update_item call: {e}"))),
        }
    }

    async fn write_log(&self, event_id: &str, log: &AdmissionLog) -> Result<(), StoreError> {
        let write = Update::new()
            .set(ADMISSION_LOG_ATTR, entries_to_attr(log))
            .build();
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::AdmissionLog { event_id }.build()))
            .update_expression(write.expression)
            .set_expression_attribute_names(Some(write.names))
            .set_expression_attribute_values(Some(write.values))
            .send()
            .await
            .map_err(|e| StoreError(format!("update_item admission log: {e}")))?;
        Ok(())
    }

    async fn write_caps(
        &self,
        event_id: &str,
        shelf: Shelf,
        caps: &[u64; SHARDS],
    ) -> Result<(), StoreError> {
        for (index, cap) in caps.iter().enumerate() {
            let shard = Shard::new(index)
                .ok_or_else(|| StoreError(format!("shard index {index} out of range")))?;
            // The count is created at zero beside the cap: `generate_token`
            // takes a spot with `n < c`, and a comparison against an absent
            // attribute is false, so a counter with a cap and no count would
            // let nobody in.
            let grant = Update::new()
                .set(SHARD_CAP_ATTR, AttributeValue::N(cap.to_string()))
                .set(SHARD_INDEX_ATTR, AttributeValue::N(index.to_string()))
                .set(
                    SHELF_INDEX_ATTR,
                    AttributeValue::N(shelf.index().to_string()),
                )
                .set_if_not_exists(SHARD_COUNT_ATTR, AttributeValue::N("0".to_owned()))
                .build();
            self.client
                .update_item()
                .table_name(&self.counters_table)
                .set_key(Some(
                    Key::ArrivalsShard {
                        event_id,
                        shelf,
                        shard,
                    }
                    .build(),
                ))
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
    /// The admission log, read consistently: a log missing the latest pass
    /// would only count more sessions live, but there is no reason to.
    ///
    /// An absent log is the first pass's. An unreadable one is read as empty,
    /// which counts every admission so far as live: nobody more is admitted
    /// until the passes logged from here on reach back a session length.
    async fn read_log(&self, event_id: &str) -> Result<AdmissionLog, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::AdmissionLog { event_id }.build()))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item admission log: {e}")))?;
        let Some(item) = out.item() else {
            return Ok(AdmissionLog::default());
        };
        Ok(log_from_item(item).unwrap_or_else(|| {
            tracing::error!(
                event_id,
                event = "admission_log_unreadable",
                "admission log could not be read; counting every admission as live"
            );
            AdmissionLog::default()
        }))
    }

    /// Every shelf's arrivals counters, read consistently: the caps written
    /// from this read are `taken` plus a share, so a stale count would hold
    /// spots that have already been taken. `BatchGetItem` takes 100 keys, so
    /// the 140 are read in two.
    async fn read_shelves(&self, event_id: &str) -> Result<Shelves, StoreError> {
        let keys: Vec<_> = Shelf::all()
            .flat_map(|shelf| {
                (0..SHARDS).filter_map(Shard::new).map(move |shard| {
                    Key::ArrivalsShard {
                        event_id,
                        shelf,
                        shard,
                    }
                    .build()
                })
            })
            .collect();

        let mut items = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(BATCH_GET_LIMIT) {
            let request = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                .set_keys(Some(chunk.to_vec()))
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

            // A partial batch must not be folded: a counter that was not
            // returned would read as holding nothing and having let nobody in.
            if out
                .unprocessed_keys()
                .is_some_and(|u| u.contains_key(&self.counters_table))
            {
                return Err(StoreError("arrivals read incomplete; retry".to_owned()));
            }
            if let Some(found) = out.responses().and_then(|r| r.get(&self.counters_table)) {
                items.extend(found.iter().cloned());
            }
        }
        shelves_from_items(&items)
    }
}

/// The log's entries as a flat list of numbers, each its time then its count.
fn entries_to_attr(log: &AdmissionLog) -> AttributeValue {
    let mut numbers = Vec::with_capacity(log.entries.len() * 2);
    for entry in &log.entries {
        numbers.push(AttributeValue::N(entry.at.as_second().to_string()));
        numbers.push(AttributeValue::N(entry.admitted.to_string()));
    }
    AttributeValue::L(numbers)
}

fn log_from_item(item: &HashMap<String, AttributeValue>) -> Option<AdmissionLog> {
    let (pairs, rest) = item.get(ADMISSION_LOG_ATTR)?.as_l().ok()?.as_chunks::<2>();
    if !rest.is_empty() {
        return None;
    }
    let mut entries = Vec::with_capacity(pairs.len());
    for [at, admitted] in pairs {
        let read = |v: &AttributeValue| v.as_n().ok().and_then(|n| n.parse::<u64>().ok());
        entries.push(LogEntry {
            at: at
                .as_n()
                .ok()
                .and_then(|n| n.parse::<i64>().ok())
                .and_then(|secs| Timestamp::from_second(secs).ok())?,
            admitted: read(admitted)?,
        });
    }
    Some(AdmissionLog { entries })
}

/// Folds `BatchGetItem` responses into every shelf's counters. A counter with
/// no item has let nobody in and holds nothing.
///
/// An item with a readable place and an unreadable count is corruption, and an
/// error: defaulting it to zero would give that counter a cap far below what
/// has already been taken from it, closing it. An absent cap is not --
/// `generate_token` creates a counter's count only through a cap, but a
/// counter never given one is a legitimate state for the first pass to find.
fn shelves_from_items(items: &[HashMap<String, AttributeValue>]) -> Result<Shelves, StoreError> {
    let mut shelves: Shelves = [[ShardBudget::default(); SHARDS]; wr_common::SHELVES];
    for item in items {
        let Some(index) = shard_index_of(item) else {
            continue;
        };
        let Some(shelf) = item
            .get(SHELF_INDEX_ATTR)
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<usize>().ok())
            .and_then(Shelf::new)
        else {
            continue;
        };
        let taken = shard_count_of(item)
            .ok_or_else(|| StoreError("arrivals item has an unreadable count".to_owned()))?;
        if let Some(counter) = shelves
            .get_mut(shelf.index())
            .and_then(|counters| counters.get_mut(index))
        {
            *counter = ShardBudget {
                taken,
                cap: shard_cap_of(item).unwrap_or(0),
            };
        }
    }
    Ok(shelves)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    fn counter_item(
        shelf: usize,
        shard: usize,
        taken: u64,
        cap: Option<u64>,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::from([
            (
                SHELF_INDEX_ATTR.to_owned(),
                AttributeValue::N(shelf.to_string()),
            ),
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
    fn each_counter_lands_on_its_own_shelf_and_a_missing_one_holds_nothing() {
        let items = vec![
            counter_item(0, 0, 3, Some(5)),
            counter_item(13, 2, 5, Some(9)),
        ];
        let shelves = shelves_from_items(&items).unwrap();
        assert_eq!(shelves[0][0], ShardBudget { taken: 3, cap: 5 });
        assert_eq!(shelves[13][2], ShardBudget { taken: 5, cap: 9 });
        assert_eq!(shelves[0][1], ShardBudget::default());
        assert_eq!(crate::admitted(&shelves), 8);
    }

    #[test]
    fn a_counter_without_a_cap_holds_nothing() {
        let shelves = shelves_from_items(&[counter_item(1, 4, 9, None)]).unwrap();
        assert_eq!(shelves[1][4], ShardBudget { taken: 9, cap: 0 });
        assert_eq!(shelves[1][4].untaken(), 0);
    }

    #[test]
    fn an_item_with_an_unreadable_count_is_an_error_not_a_zero_count() {
        let mut corrupt = counter_item(0, 3, 0, None);
        corrupt.remove(SHARD_COUNT_ATTR);
        assert!(shelves_from_items(&[counter_item(0, 0, 3, Some(4)), corrupt]).is_err());
    }

    #[test]
    fn an_item_with_an_unreadable_place_is_skipped() {
        let mut no_shard = counter_item(0, 0, 9, None);
        no_shard.remove(SHARD_INDEX_ATTR);
        let mut no_shelf = counter_item(0, 1, 9, None);
        no_shelf.remove(SHELF_INDEX_ATTR);
        let off_the_end = counter_item(wr_common::SHELVES, 1, 9, None);
        let shelves = shelves_from_items(&[
            counter_item(0, 0, 3, Some(4)),
            no_shard,
            no_shelf,
            off_the_end,
        ])
        .unwrap();
        assert_eq!(crate::admitted(&shelves), 3);
    }

    fn log_item(log: &AdmissionLog) -> HashMap<String, AttributeValue> {
        HashMap::from([(ADMISSION_LOG_ATTR.to_owned(), entries_to_attr(log))])
    }

    #[test]
    fn the_log_round_trips_through_its_item() {
        let log = AdmissionLog {
            entries: vec![
                LogEntry {
                    at: jiff::Timestamp::from_second(100).unwrap(),
                    admitted: 0,
                },
                LogEntry {
                    at: jiff::Timestamp::from_second(110).unwrap(),
                    admitted: u64::MAX,
                },
            ],
        };
        assert_eq!(log_from_item(&log_item(&log)), Some(log));
        let empty = AdmissionLog::default();
        assert_eq!(log_from_item(&log_item(&empty)), Some(empty));
    }

    #[test]
    fn a_malformed_log_is_unreadable() {
        let n = |v: &str| AttributeValue::N(v.to_owned());
        let with_entries =
            |entries: AttributeValue| HashMap::from([(ADMISSION_LOG_ATTR.to_owned(), entries)]);
        assert!(log_from_item(&with_entries(AttributeValue::L(vec![n("1")]))).is_none());
        assert!(log_from_item(&with_entries(AttributeValue::L(vec![n("1"), n("x")]))).is_none());
        assert!(log_from_item(&with_entries(AttributeValue::S("1,2".to_owned()))).is_none());
        assert!(log_from_item(&HashMap::new()).is_none());
    }
}
