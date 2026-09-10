//! The `aws-sdk-dynamodb`-backed [`Store`] for `generate_token`.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{
    arrivals_shard_key, event_key, increment_shard_update, increment_shard_values,
};
use wr_common::{AdmissionControl, Counters, Phase, PositionStatus, PreQueueItem, SHARDS};

use crate::{Store, StoreError};

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
            .set_key(Some(event_key(event_id)))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item counters: {e}")))?;

        Ok(out.item().map(|item| counters_from_item(event_id, item)))
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
            .key("request_id", AttributeValue::S(request_id.to_owned()))
            .send()
            .await
            .map_err(|e| StoreError(format!("get_item positions: {e}")))?;

        Ok(out.item().and_then(position_from_item))
    }

    async fn record_arrival(&self, event_id: &str, shard: usize) -> Result<(), StoreError> {
        // The shard is its own item, so arrivals at the admission rate do not
        // contend with the queue_counter claims on the Counters item.
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(arrivals_shard_key(event_id, shard)))
            .update_expression(increment_shard_update())
            .set_expression_attribute_values(Some(increment_shard_values(shard)))
            .send()
            .await
            .map_err(|e| StoreError(format!("update_item arrivals: {e}")))?;
        Ok(())
    }
}

/// Reads the position and status off a `Positions` item. Both must be present
/// and well-formed; a row missing either is treated as no row rather than
/// admitting on a default.
fn position_from_item(item: &HashMap<String, AttributeValue>) -> Option<(u64, PositionStatus)> {
    let position = item
        .get("queue_position")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())?;
    let status = item
        .get("status")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| match s.as_str() {
            "issued" => Some(PositionStatus::Issued),
            "completed" => Some(PositionStatus::Completed),
            "abandoned" => Some(PositionStatus::Abandoned),
            "expired" => Some(PositionStatus::Expired),
            _ => None,
        })?;
    Some((position, status))
}

/// Assembles the flat `Counters` item, including the `#`-suffixed shard
/// attributes `serde` cannot express as fields.
fn counters_from_item(event_id: &str, item: &HashMap<String, AttributeValue>) -> Counters {
    let num = |key: &str| -> Option<u64> {
        item.get(key)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
    };

    Counters {
        event_id: event_id.to_owned(),
        phase: item
            .get("phase")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(Phase::Idle),
        queue_counter: num("queue_counter").unwrap_or(0),
        serving_counter: num("serving_counter").unwrap_or(0),
        shuffle_seed: item
            .get("shuffle_seed")
            .and_then(|v| v.as_b().ok())
            .and_then(|b| <[u8; 32]>::try_from(b.as_ref()).ok()),
        participant_count: num("participant_count"),
        prequeue_offsets: item
            .get("prequeue_offsets")
            .and_then(|v| v.as_l().ok())
            .and_then(|list| {
                let parsed: Vec<u64> = list
                    .iter()
                    .filter_map(|e| e.as_n().ok().and_then(|s| s.parse().ok()))
                    .collect();
                <[u64; SHARDS]>::try_from(parsed).ok()
            }),
        message: item
            .get("message")
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned(),
        admission_control: item
            .get("admission_control")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(AdmissionControl::Open),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position_item(position: &str, status: &str) -> HashMap<String, AttributeValue> {
        HashMap::from([
            ("request_id".to_owned(), AttributeValue::S("r1".to_owned())),
            (
                "queue_position".to_owned(),
                AttributeValue::N(position.to_owned()),
            ),
            ("status".to_owned(), AttributeValue::S(status.to_owned())),
        ])
    }

    #[test]
    fn every_position_status_decodes() {
        for (wire, expected) in [
            ("issued", PositionStatus::Issued),
            ("completed", PositionStatus::Completed),
            ("abandoned", PositionStatus::Abandoned),
            ("expired", PositionStatus::Expired),
        ] {
            assert_eq!(
                position_from_item(&position_item("7", wire)),
                Some((7, expected))
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
    fn counters_default_to_a_closed_event() {
        // An item with nothing set must not read as an active, admitting event.
        let counters = counters_from_item("evt", &HashMap::new());
        assert_eq!(counters.phase, Phase::Idle);
        assert_eq!(counters.serving_counter, 0);
        assert_eq!(counters.admission_control, AdmissionControl::Open);
        assert!(counters.shuffle_seed.is_none());
    }
}
