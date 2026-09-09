//! The `aws-sdk-dynamodb`-backed [`Store`] implementation over the single
//! `Counters` item.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_domain::Phase;

use crate::{ControlState, Store, StoreError};

/// A live `Counters`-table store for one deployment.
pub struct DynamoStore {
    client: Client,
    counters_table: String,
}

impl DynamoStore {
    #[must_use]
    pub fn new(client: Client, counters_table: String) -> Self {
        Self {
            client,
            counters_table,
        }
    }
}

/// The wire string for a phase, matching `Phase`'s `snake_case` serialization.
fn phase_str(phase: Phase) -> &'static str {
    match phase {
        Phase::Idle => "idle",
        Phase::PreQueue => "pre_queue",
        Phase::Active => "active",
        Phase::PostEvent => "post_event",
        Phase::Maintenance => "maintenance",
    }
}

fn phase_from_str(s: Option<&str>) -> Phase {
    match s {
        Some("pre_queue") => Phase::PreQueue,
        Some("active") => Phase::Active,
        Some("post_event") => Phase::PostEvent,
        Some("maintenance") => Phase::Maintenance,
        _ => Phase::Idle,
    }
}

impl Store for DynamoStore {
    async fn load(&self, event_id: &str) -> Result<Option<ControlState>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("get_item: {e}")))?;

        let Some(item) = out.item() else {
            return Ok(None);
        };

        let num = |key: &str| -> Option<u64> {
            item.get(key)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse().ok())
        };
        let phase = phase_from_str(
            item.get("phase")
                .and_then(|v| v.as_s().ok())
                .map(String::as_str),
        );

        Ok(Some(ControlState {
            event_id: event_id.to_owned(),
            phase,
            serving_counter: num("serving_counter").unwrap_or(0),
            queue_counter: num("queue_counter").unwrap_or(0),
            participant_count: num("participant_count"),
            target_rate: num("target_rate").and_then(|n| u32::try_from(n).ok()),
            message: item.get("message").and_then(|v| v.as_s().ok()).cloned(),
        }))
    }

    async fn set_phase(&self, event_id: &str, from: Phase, to: Phase) -> Result<(), StoreError> {
        let result = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression("SET phase = :to")
            .condition_expression("phase = :from")
            .expression_attribute_values(":to", AttributeValue::S(phase_str(to).to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(phase_str(from).to_owned()))
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
                Err(StoreError::Conflict)
            }
            Err(e) => Err(StoreError::Backend(format!("update_item(phase): {e}"))),
        }
    }

    async fn set_rate(&self, event_id: &str, rate: u32) -> Result<(), StoreError> {
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression("SET target_rate = :r")
            .expression_attribute_values(":r", AttributeValue::N(rate.to_string()))
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("update_item(rate): {e}")))?;
        Ok(())
    }

    async fn set_message(&self, event_id: &str, message: &str) -> Result<(), StoreError> {
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression("SET message = :m")
            .expression_attribute_values(":m", AttributeValue::S(message.to_owned()))
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("update_item(message): {e}")))?;
        Ok(())
    }
}
