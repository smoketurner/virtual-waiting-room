//! The `aws-sdk-dynamodb`-backed [`Store`] for the controller.
//!
//! `Positions` is keyed only by `request_id` with no secondary index, so the
//! expiry read is a `Scan` with a `FilterExpression` on `expires_at` and
//! `status` (the filter also drops a TTL-pending-but-still-visible
//! item). The `Counters` reads and writes are single-item `GetItem`/`UpdateItem`
//! on the event key.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::{AdmissionControl, Phase, PositionStatus, SHARDS};

use crate::{
    ControllerState, ExpiredPosition, NoShowState, ReleaseDecision, ReleaseInputs, Store,
    StoreError,
};

/// A live `DynamoDB` store bound to the counters and positions tables.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
    positions_table: String,
    event_id: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(
        client: Client,
        counters_table: String,
        positions_table: String,
        event_id: String,
    ) -> Self {
        Self {
            client,
            counters_table,
            positions_table,
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
            .key(
                "event_id",
                AttributeValue::S(wr_common::expr::event_key(event_id)),
            )
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

        // A stored control that is absent or fails to parse resolves to Open,
        // matching every other reader (`read`, `admin`): "open" is the default
        // an event is created in, and a garbage value must not silently hold
        // admission for an event nobody paused.
        let admission_control = item
            .get("admission_control")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse::<AdmissionControl>().ok())
            .unwrap_or(AdmissionControl::Open);

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
            admission_control,
            inputs: ReleaseInputs {
                arrivals,
                last_arrivals_total: num(item, "last_arrivals_total"),
                last_serving_counter: num(item, "last_serving_counter"),
                serving_counter: num(item, "serving_counter"),
                queue_counter: num(item, "queue_counter"),
                target_rate,
            },
            prev_no_show,
            max_expired_position: num(item, "max_expired_position"),
        })
    }

    async fn write_release(
        &self,
        event_id: &str,
        decision: &ReleaseDecision,
        expected_serving_counter: u64,
    ) -> Result<(), StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key(
                "event_id",
                AttributeValue::S(wr_common::expr::event_key(event_id)),
            )
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
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(se))
                if matches!(
                    se.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                // Another invoke advanced first; this pass's release is stale.
                tracing::info!(event_id, "release write lost the race; skipping");
                Ok(())
            }
            Err(e) => Err(StoreError(format!("update_item release: {e}"))),
        }
    }

    async fn query_expired(
        &self,
        cutoff_position: u64,
    ) -> Result<Vec<ExpiredPosition>, StoreError> {
        let mut expired = Vec::new();
        let mut pages = self
            .client
            .scan()
            .table_name(&self.positions_table)
            .filter_expression("queue_position < :cutoff AND #s = :issued")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(":cutoff", AttributeValue::N(cutoff_position.to_string()))
            .expression_attribute_values(
                ":issued",
                AttributeValue::S(status_wire(PositionStatus::Issued).to_owned()),
            )
            .projection_expression("request_id, queue_position")
            .into_paginator()
            .items()
            .send();

        while let Some(item) = pages.next().await {
            let item = item.map_err(|e| StoreError(format!("scan positions: {e}")))?;
            let request_id = item.get("request_id").and_then(|v| v.as_s().ok());
            let position = item
                .get("queue_position")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse::<u64>().ok());
            if let (Some(request_id), Some(position)) = (request_id, position) {
                expired.push(ExpiredPosition {
                    request_id: request_id.clone(),
                    position,
                });
            }
        }
        Ok(expired)
    }

    async fn mark_expired(&self, request_id: &str) -> Result<(), StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.positions_table)
            .key("request_id", AttributeValue::S(request_id.to_owned()))
            .update_expression("SET #s = :expired")
            .condition_expression("#s = :issued")
            .expression_attribute_names("#s", "status")
            .expression_attribute_values(
                ":expired",
                AttributeValue::S(status_wire(PositionStatus::Expired).to_owned()),
            )
            .expression_attribute_values(
                ":issued",
                AttributeValue::S(status_wire(PositionStatus::Issued).to_owned()),
            )
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
                // The position was completed/abandoned/already-expired between the
                // scan and this write; leave it as it is.
                Ok(())
            }
            Err(e) => Err(StoreError(format!("update_item mark_expired: {e}"))),
        }
    }

    async fn advance_max_expired(
        &self,
        event_id: &str,
        max_expired_position: u64,
    ) -> Result<(), StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key(
                "event_id",
                AttributeValue::S(wr_common::expr::event_key(event_id)),
            )
            .update_expression("SET max_expired_position = :m")
            // Only ever move the cursor forward.
            .condition_expression(
                "attribute_not_exists(max_expired_position) OR max_expired_position < :m",
            )
            .expression_attribute_values(":m", AttributeValue::N(max_expired_position.to_string()))
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
                Ok(())
            }
            Err(e) => Err(StoreError(format!("update_item max_expired: {e}"))),
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
            .map(|shard| {
                std::collections::HashMap::from([(
                    "event_id".to_owned(),
                    AttributeValue::S(wr_common::expr::arrivals_shard_key(event_id, shard)),
                )])
            })
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

        let mut arrivals = [0u64; SHARDS];
        for item in out
            .responses()
            .and_then(|r| r.get(&self.counters_table))
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let Some(key) = item.get("event_id").and_then(|v| v.as_s().ok()) else {
                continue;
            };
            // A shard nobody has arrived on has no item; it stays zero.
            let Some(shard) =
                (0..SHARDS).find(|&s| wr_common::expr::arrivals_shard_key(event_id, s) == *key)
            else {
                continue;
            };
            arrivals[shard] = item
                .get(wr_common::expr::SHARD_COUNT_ATTR)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
        }
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

/// The stored wire string for a [`PositionStatus`], matching its `serde`
/// `snake_case` representation. Single source so no filter or write hand-writes
/// "issued"/"expired".
fn status_wire(status: PositionStatus) -> &'static str {
    match status {
        PositionStatus::Issued => "issued",
        PositionStatus::Completed => "completed",
        PositionStatus::Abandoned => "abandoned",
        PositionStatus::Expired => "expired",
    }
}

/// The `event_id` this store is bound to, for the handler's logging.
impl DynamoStore {
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }
}
