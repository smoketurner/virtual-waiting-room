//! The `aws-sdk-dynamodb`-backed [`Store`].

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::Key;
use wr_common::{Counters, PreQueueItem};

use crate::{Store, StoreError};

/// The three tables the read path reads, one item each.
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
            .set_key(Some(Key::Event { event_id }.build()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get counters: {e}")))?;
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
            .map_err(|e| StoreError(format!("get prequeue: {e}")))?;
        match out.item() {
            Some(item) => serde_dynamo::from_item(item.clone())
                .map(Some)
                .map_err(|e| StoreError(format!("decode prequeue: {e}"))),
            None => Ok(None),
        }
    }

    /// Fetches a live joiner's position from the `queue_position` attribute of
    /// their `Positions` row, written by `assign_position`. Returns `None` when
    /// the request id has no row.
    async fn load_position(&self, request_id: &str) -> Result<Option<u64>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.positions_table)
            .key("request_id", AttributeValue::S(request_id.to_owned()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get position: {e}")))?;
        Ok(out.item().and_then(position_from_item))
    }
}

/// Reads `queue_position` off a `Positions` item. `None` when the attribute is
/// absent or is not a number, so a row written in some other shape reads as "no
/// position" rather than as position zero.
fn position_from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<u64> {
    item.get("queue_position")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use wr_common::{PositionItem, PositionStatus};

    use super::*;
    use jiff::Timestamp;

    #[test]
    fn reads_the_position_assign_position_wrote() {
        // Round-trips a real PositionItem, so the reader and the writer cannot
        // drift onto different attribute names or types.
        let written = PositionItem {
            request_id: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            queue_position: 4_242,
            entry_time: Timestamp::constant(1_788_000_000, 0),
            status: PositionStatus::Issued,
            ttl: Timestamp::constant(1_788_086_700, 0),
            h: None,
        };
        let item: std::collections::HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&written).unwrap();
        assert_eq!(position_from_item(&item), Some(4_242));
    }

    #[test]
    fn a_row_without_a_position_reads_as_none_not_zero() {
        // Position zero is a real position at the head of the queue; a missing
        // attribute must never be reported as one.
        let mut item = std::collections::HashMap::new();
        item.insert(
            "request_id".to_owned(),
            AttributeValue::S("req-1".to_owned()),
        );
        assert_eq!(position_from_item(&item), None);
        // A timestamp-shaped string in the old attribute is not a position.
        item.insert(
            "entry_time".to_owned(),
            AttributeValue::S("2026-08-03T19:12:52.000Z".to_owned()),
        );
        assert_eq!(position_from_item(&item), None);
    }
}
