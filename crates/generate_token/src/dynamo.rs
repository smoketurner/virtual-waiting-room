//! The `aws-sdk-dynamodb`-backed [`Store`] for `generate_token`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use jiff::{SignedDuration, Timestamp};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{
    ADMITTED_AT_ATTR, Condition, ENTRY_TIME_ATTR, Key, POSITION_TTL, POSITIONS_KEY_ATTR,
    POSITIONS_TTL_ATTR, POSSESSION_DIGEST_ATTR, QUEUE_POSITION_ATTR, SHARD_CAP_ATTR,
    SHARD_COUNT_ATTR, STATUS_ATTR, Update,
};
use wr_common::{
    Counters, PositionStatus, PreQueueItem, SHARDS, SHELVES, SecretDigest, Shard, Shelf,
};

use crate::{PositionRow, Store, StoreError};

/// How long one execution environment answers from what it last read or
/// learned. See [`SpentShards`] and `load_counters`.
const MEMO: SignedDuration = SignedDuration::from_secs(1);

/// Which arrivals counters this execution environment last saw with no spot
/// left, and until when it will take that as still true.
///
/// A refused take is a conditional write that failed, and a failed conditional
/// write is billed and counted against the counter's partition like one that
/// landed. A visitor tries every counter on their shelf, so without this a
/// shelf nearly taken costs up to ten such writes a request. A counter's cap
/// only rises when the controller holds a new call's spots on it, so one seen
/// empty is not asked again for [`MEMO`]; the cost is that new spots are
/// noticed up to that much late.
#[derive(Debug)]
pub struct SpentShards {
    /// Epoch seconds, per counter of every shelf: atomics hold no `Timestamp`.
    until: [AtomicI64; SHARDS * SHELVES],
}

impl Default for SpentShards {
    fn default() -> Self {
        Self {
            until: std::array::from_fn(|_| AtomicI64::new(i64::MIN)),
        }
    }
}

impl SpentShards {
    fn slot(&self, shelf: Shelf, shard: Shard) -> Option<&AtomicI64> {
        self.until.get(shelf.index() * SHARDS + shard.index())
    }

    /// Whether counter `shard` on `shelf` was seen empty recently enough to
    /// skip asking it.
    #[must_use]
    pub fn is_spent(&self, shelf: Shelf, shard: Shard, now: Timestamp) -> bool {
        self.slot(shelf, shard)
            .is_some_and(|until| now.as_second() <= until.load(Ordering::Relaxed))
    }

    /// Records that counter `shard` on `shelf` refused a take at `now`.
    pub fn mark(&self, shelf: Shelf, shard: Shard, now: Timestamp) {
        if let Some(until) = self.slot(shelf, shard) {
            until.store(
                now.as_second().saturating_add(MEMO.as_secs()),
                Ordering::Relaxed,
            );
        }
    }
}

/// A live store bound to the three tables the admission check reads.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    prequeue_table: String,
    positions_table: String,
    spent: SpentShards,
    /// The event's `Counters` item as last read, and when.
    counters: Mutex<Option<(Instant, Option<Counters>)>>,
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
            spent: SpentShards::default(),
            counters: Mutex::new(None),
        }
    }
}

impl Store for DynamoStore {
    async fn load_counters(&self, event_id: &str) -> Result<Option<Counters>, StoreError> {
        // Every admission attempt, refused or not, reads this one item, and a
        // single partition key serves 3,000 consistent reads a second. Each
        // execution environment therefore holds it for a second, as `read`
        // does for the same item: the cursor moves every ten seconds and a
        // pause takes effect at most a second late.
        if let Ok(guard) = self.counters.lock()
            && let Some((at, cached)) = guard.as_ref()
            && at.elapsed() < MEMO.unsigned_abs()
        {
            return Ok(cached.clone());
        }

        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            // Consistent: a visitor polling for admission must not be told to
            // keep waiting because a replica lagged behind the controller.
            .consistent_read(true)
            .set_key(Some(Key::Event { event_id }.build()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item counters: {e}")))?;

        let counters = out.item().map(|item| Counters::from_item(event_id, item));
        if let Ok(mut guard) = self.counters.lock() {
            *guard = Some((Instant::now(), counters.clone()));
        }
        Ok(counters)
    }

    async fn load_prequeue(&self, request_id: &str) -> Result<Option<PreQueueItem>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.prequeue_table)
            .key("r", AttributeValue::S(request_id.to_owned()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item prequeue: {e}")))?;

        match out.item() {
            Some(item) => serde_dynamo::from_item(item.clone())
                .map(Some)
                .map_err(|e| StoreError(format!("decode prequeue: {e}"))),
            None => Ok(None),
        }
    }

    async fn load_position(&self, request_id: &str) -> Result<Option<PositionRow>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.positions_table)
            .consistent_read(true)
            .set_key(Some(Key::Position { request_id }.build()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item positions: {e}")))?;

        Ok(out.item().and_then(position_from_item))
    }

    async fn mark_admitted(
        &self,
        request_id: &str,
        position: u64,
        digest: &SecretDigest,
        now: Timestamp,
    ) -> Result<(), StoreError> {
        // One `UpdateItem` serves both populations. A live joiner already has
        // the row, written at registration with `status = issued`; a pre-queue
        // member has none, and this creates it. The guard is the disjunction of
        // those two states, so exactly the first call through either path
        // succeeds and every later one fails the condition.
        //
        // `entry_time` is set only on creation: a live joiner's is the moment
        // they claimed their position, and overwriting it with the admission
        // time would lose the one timestamp the row is documented to carry. The
        // ttl is refreshed either way -- it is storage reclamation, so counting
        // it from the last write is the behaviour that keeps a row around for
        // as long as it is being used.
        let claim = Update::new()
            .set(
                STATUS_ATTR,
                AttributeValue::S(status_wire(PositionStatus::Admitted)),
            )
            .set(QUEUE_POSITION_ATTR, AttributeValue::N(position.to_string()))
            // The guard lets this write land once per row, so this is the
            // first admission and every later session expires from it (#237).
            .set(
                ADMITTED_AT_ATTR,
                AttributeValue::N(now.as_second().to_string()),
            )
            .set_if_not_exists(
                ENTRY_TIME_ATTR,
                AttributeValue::N(now.as_second().to_string()),
            )
            // A live joiner's row has the digest already; a pre-queue member's
            // is created here, and its later calls read this row first.
            .set_if_not_exists(
                POSSESSION_DIGEST_ATTR,
                AttributeValue::S(digest.as_str().to_owned()),
            )
            .set(
                POSITIONS_TTL_ATTR,
                AttributeValue::N(
                    now.checked_add(POSITION_TTL)
                        .map_err(|e| StoreError(format!("position ttl: {e}")))?
                        .as_second()
                        .to_string(),
                ),
            )
            .build();
        let guard = Condition::attribute_not_exists(POSITIONS_KEY_ATTR)
            .or_equals(
                STATUS_ATTR,
                AttributeValue::S(status_wire(PositionStatus::Issued)),
            )
            .build();

        // The two builders allocate disjoint placeholder prefixes (`#u`/`:u`
        // and `#c`/`:c`), so merging them cannot collide.
        let mut names = claim.names;
        names.extend(guard.names);
        let mut values = claim.values;
        values.extend(guard.values);

        let result = self
            .client
            .update_item()
            .table_name(&self.positions_table)
            .set_key(Some(Key::Position { request_id }.build()))
            .update_expression(claim.expression)
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(names))
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
                // Already marked by a concurrent call for the same visitor.
                Ok(())
            }
            Err(e) => Err(StoreError(format!("update_item mark admitted: {e}"))),
        }
    }

    async fn take_spot(
        &self,
        event_id: &str,
        shelf: Shelf,
        shard: Shard,
    ) -> Result<bool, StoreError> {
        let now = Timestamp::now();
        if self.spent.is_spent(shelf, shard, now) {
            return Ok(false);
        }
        let bump = Update::new()
            .add(SHARD_COUNT_ATTR, AttributeValue::N("1".to_owned()))
            .build();
        // Allowed only below the cap the controller wrote. A counter with no
        // cap yet fails the comparison and lets nobody in, which is the right
        // answer before any spot has been held there.
        let guard = Condition::attr_less_than(SHARD_COUNT_ATTR, SHARD_CAP_ATTR).build();

        let mut names = bump.names;
        names.extend(guard.names);

        // The counter is its own item, so taking a spot does not contend with
        // the queue_counter claims on the event's item.
        let result = self
            .client
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
            .update_expression(bump.expression)
            .condition_expression(guard.expression)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(bump.values))
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
                self.spent.mark(shelf, shard, now);
                Ok(false)
            }
            Err(e) => Err(StoreError(format!("update_item take spot: {e}"))),
        }
    }
}

/// The stored spelling of a status.
///
/// Paired with [`status_of`] so the writer here and the reader below cannot
/// drift: a status written in a spelling the reader does not recognise makes
/// the row unreadable, which reads as "no row" and denies a registered visitor.
fn status_wire(status: PositionStatus) -> String {
    match status {
        PositionStatus::Issued => "issued",
        PositionStatus::Admitted => "admitted",
    }
    .to_owned()
}

/// The status a stored spelling means, or `None` for one nothing writes.
fn status_of(wire: &str) -> Option<PositionStatus> {
    match wire {
        "issued" => Some(PositionStatus::Issued),
        "admitted" => Some(PositionStatus::Admitted),
        _ => None,
    }
}

/// Reads the position and status off a `Positions` item. Both must be present
/// and well-formed; a row missing either is treated as no row rather than
/// admitting on a default.
fn position_from_item(item: &HashMap<String, AttributeValue>) -> Option<PositionRow> {
    let position = item
        .get(QUEUE_POSITION_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())?;
    let status = item
        .get(STATUS_ATTR)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| status_of(s))?;
    // A missing or malformed digest is not "no row": the row still answers
    // the position, and the admission check refuses it as NotHolder.
    let digest = item
        .get(POSSESSION_DIGEST_ATTR)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| SecretDigest::parse(s).ok());
    let admitted_at = item
        .get(ADMITTED_AT_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(|secs| Timestamp::from_second(secs).ok());
    Some(PositionRow {
        position,
        status,
        digest,
        admitted_at,
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    fn position_item(position: &str, status: &str) -> HashMap<String, AttributeValue> {
        HashMap::from([
            (
                POSITIONS_KEY_ATTR.to_owned(),
                AttributeValue::S("r1".to_owned()),
            ),
            (
                QUEUE_POSITION_ATTR.to_owned(),
                AttributeValue::N(position.to_owned()),
            ),
            (STATUS_ATTR.to_owned(), AttributeValue::S(status.to_owned())),
        ])
    }

    #[test]
    fn every_position_status_round_trips_through_its_wire_spelling() {
        // The writer and the reader are the two halves of one codec. A status
        // this crate can write but not read back makes the row unreadable,
        // which is indistinguishable from an unregistered visitor.
        for status in [PositionStatus::Issued, PositionStatus::Admitted] {
            let wire = status_wire(status);
            assert_eq!(status_of(&wire), Some(status), "{wire} did not round-trip");
            assert_eq!(
                position_from_item(&position_item("7", &wire)),
                Some(PositionRow {
                    position: 7,
                    status,
                    digest: None,
                    admitted_at: None,
                })
            );
        }
    }

    #[test]
    fn a_row_missing_either_half_is_no_row() {
        // Defaulting a missing status to issued, or a missing position to 0,
        // would admit a visitor who holds neither.
        let mut no_position = position_item("7", "issued");
        no_position.remove("queue_position");
        assert_eq!(position_from_item(&no_position), None);

        let mut no_status = position_item("7", "issued");
        no_status.remove("status");
        assert_eq!(position_from_item(&no_status), None);

        assert_eq!(position_from_item(&position_item("7", "nonsense")), None);
    }

    #[test]
    fn the_digest_is_read_back_and_a_malformed_one_is_none() {
        let mut item = position_item("7", "issued");
        item.insert(
            POSSESSION_DIGEST_ATTR.to_owned(),
            AttributeValue::S("DwBzhbb51LfusnSGBa_hqYSgo7-j8BTQnip4TOnlzRo".to_owned()),
        );
        assert!(position_from_item(&item).unwrap().digest.is_some());
        item.insert(
            POSSESSION_DIGEST_ATTR.to_owned(),
            AttributeValue::S("short".to_owned()),
        );
        assert_eq!(position_from_item(&item).unwrap().digest, None);
    }

    #[test]
    fn a_shard_seen_spent_is_not_asked_again_until_the_memo_lapses() {
        let spent = SpentShards::default();
        let shelf = Shelf::new(13).unwrap();
        let shard = Shard::new(3).unwrap();
        let other = Shard::new(4).unwrap();
        let t = |secs| Timestamp::constant(secs, 0);
        assert!(!spent.is_spent(shelf, shard, t(100)));
        spent.mark(shelf, shard, t(100));
        assert!(spent.is_spent(shelf, shard, t(100)));
        assert!(spent.is_spent(shelf, shard, t(100 + MEMO.as_secs())));
        assert!(!spent.is_spent(shelf, shard, t(101 + MEMO.as_secs())));
        // One counter running out says nothing about the others, on this
        // shelf or any other.
        assert!(!spent.is_spent(shelf, other, t(100)));
        assert!(!spent.is_spent(Shelf::new(0).unwrap(), shard, t(100)));
    }
}
