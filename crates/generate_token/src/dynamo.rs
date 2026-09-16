//! The `aws-sdk-dynamodb`-backed [`Store`] for `generate_token`.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{
    Condition, ENTRY_TIME_ATTR, Key, POSITION_TTL_SECS, POSITIONS_KEY_ATTR, POSITIONS_TTL_ATTR,
    QUEUE_POSITION_ATTR, SHARD_COUNT_ATTR, SHARD_INDEX_ATTR, STATUS_ATTR, Update,
};
use wr_common::{Counters, PositionStatus, PreQueueItem, Shard};

use crate::{AdmissionClaim, Store, StoreError};

/// A live store bound to the three tables the admission check reads.
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
    async fn load_counters(&self, event_id: &str) -> Result<Option<Counters>, StoreError> {
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

        Ok(out.item().map(|item| Counters::from_item(event_id, item)))
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

    async fn load_position(
        &self,
        request_id: &str,
    ) -> Result<Option<(u64, PositionStatus)>, StoreError> {
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

    async fn claim_admission(
        &self,
        request_id: &str,
        position: u64,
        now: u64,
    ) -> Result<AdmissionClaim, StoreError> {
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
            .set_if_not_exists(ENTRY_TIME_ATTR, AttributeValue::N(now.to_string()))
            .set(
                POSITIONS_TTL_ATTR,
                AttributeValue::N(now.saturating_add(POSITION_TTL_SECS).to_string()),
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
            Ok(_) => Ok(AdmissionClaim::First),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                Ok(AdmissionClaim::Repeat)
            }
            Err(e) => Err(StoreError(format!("update_item claim admission: {e}"))),
        }
    }

    async fn record_arrival(&self, event_id: &str, shard: Shard) -> Result<(), StoreError> {
        // Stamps the shard's own index alongside the +1 so a reader that
        // fetched a batch of shards knows which is which.
        let bump = Update::new()
            .set(
                SHARD_INDEX_ATTR,
                AttributeValue::N(shard.index().to_string()),
            )
            .add(SHARD_COUNT_ATTR, AttributeValue::N("1".to_owned()))
            .build();
        // The shard is its own item, so arrivals at the admission rate do not
        // contend with the queue_counter claims on the Counters item.
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::ArrivalsShard { event_id, shard }.build()))
            .update_expression(bump.expression)
            .set_expression_attribute_names(Some(bump.names))
            .set_expression_attribute_values(Some(bump.values))
            .send()
            .await
            .map_err(|e| StoreError(format!("update_item arrivals: {e}")))?;
        Ok(())
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
fn position_from_item(item: &HashMap<String, AttributeValue>) -> Option<(u64, PositionStatus)> {
    let position = item
        .get(QUEUE_POSITION_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())?;
    let status = item
        .get(STATUS_ATTR)
        .and_then(|v| v.as_s().ok())
        .and_then(|s| status_of(s))?;
    Some((position, status))
}

#[cfg(test)]
mod tests {
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
                Some((7, status))
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
}
