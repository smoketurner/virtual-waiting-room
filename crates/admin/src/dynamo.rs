//! The `aws-sdk-dynamodb`-backed [`Store`] implementation over the single
//! `Counters` item.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_domain::{AdmissionControl, Phase};

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
        let phase = item
            .get("phase")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(Phase::Idle);

        let str_attr =
            |key: &str| -> Option<String> { item.get(key).and_then(|v| v.as_s().ok()).cloned() };

        Ok(Some(ControlState {
            event_id: event_id.to_owned(),
            phase,
            serving_counter: num("serving_counter").unwrap_or(0),
            queue_counter: num("queue_counter").unwrap_or(0),
            participant_count: num("participant_count"),
            target_rate: num("target_rate").and_then(|n| u32::try_from(n).ok()),
            message: item.get("message").and_then(|v| v.as_s().ok()).cloned(),
            admission_paused: item
                .get("admission_control")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.parse::<AdmissionControl>().ok())
                == Some(AdmissionControl::Paused),
            last_action: str_attr("last_action"),
            last_action_by: str_attr("last_action_by"),
            last_action_at: str_attr("last_action_at"),
            last_action_epoch_ms: item
                .get("last_action_epoch_ms")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok()),
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
            .expression_attribute_values(":to", AttributeValue::S(to.as_wire_str().to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()))
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

    async fn set_rate(
        &self,
        event_id: &str,
        expected: Option<u32>,
        rate: u32,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(
                "SET target_rate = :r, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":r", AttributeValue::N(rate.to_string()));
        req = apply_audit_values(req, crate::AdminAction::SetRate, actor, now_ms);
        req = guard_expected_rate(req, expected);
        req = guard_debounce(req, now_ms);
        send_guarded(req, "rate").await
    }

    async fn set_message(
        &self,
        event_id: &str,
        message: &str,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(
                "SET message = :m, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":m", AttributeValue::S(message.to_owned()));
        req = apply_audit_values(req, crate::AdminAction::SetMessage, actor, now_ms);
        req = guard_debounce(req, now_ms);
        send_guarded(req, "message").await
    }

    async fn set_paused(
        &self,
        event_id: &str,
        from: bool,
        to: bool,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // Idempotency guard on the stored admission_control (ADR-0019): the
        // toggle is legal only from the expected prior value. `open` is the
        // default, so attribute_not_exists covers a never-written row.
        let from_ctl = if from {
            AdmissionControl::Paused
        } else {
            AdmissionControl::Open
        };
        let to_ctl = if to {
            AdmissionControl::Paused
        } else {
            AdmissionControl::Open
        };
        let control_guard = if from {
            "admission_control = :from"
        } else {
            "(attribute_not_exists(admission_control) OR admission_control = :from)"
        };
        let cutoff = now_ms.saturating_sub(crate::DEBOUNCE_MS).to_string();
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(
                "SET admission_control = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":to", AttributeValue::S(to_ctl.as_wire_str().to_owned()))
            .expression_attribute_values(
                ":from",
                AttributeValue::S(from_ctl.as_wire_str().to_owned()),
            )
            .condition_expression(format!(
                "{control_guard} AND (attribute_not_exists(last_action_epoch_ms) \
                 OR last_action_epoch_ms < :cutoff)"
            ))
            .expression_attribute_values(":cutoff", AttributeValue::N(cutoff));
        req = apply_audit_values(
            req,
            if to {
                crate::AdminAction::Pause
            } else {
                crate::AdminAction::Resume
            },
            actor,
            now_ms,
        );
        send_guarded(req, "paused").await
    }

    async fn force_maintenance(
        &self,
        event_id: &str,
        from: Phase,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // Guarded on the expected phase (lost-race safety) but NOT debounced —
        // the emergency stop must always apply.
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .key("event_id", AttributeValue::S(event_id.to_owned()))
            .update_expression(
                "SET phase = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(
                ":to",
                AttributeValue::S(Phase::Maintenance.as_wire_str().to_owned()),
            )
            .condition_expression("phase = :from")
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()));
        req = apply_audit_values(req, crate::AdminAction::ForceMaintenance, actor, now_ms);
        send_guarded(req, "force_maintenance").await
    }
}

type UpdateReq = aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;

/// Stamps the shared audit + epoch values onto a mutation.
fn apply_audit_values(
    req: UpdateReq,
    action: crate::AdminAction,
    actor: &str,
    now_ms: u64,
) -> UpdateReq {
    let at = aws_smithy_types::DateTime::from_millis(i64::try_from(now_ms).unwrap_or(0))
        .fmt(aws_smithy_types::date_time::Format::DateTime)
        .unwrap_or_default();
    req.expression_attribute_values(":a", AttributeValue::S(action.as_str().to_owned()))
        .expression_attribute_values(":by", AttributeValue::S(actor.to_owned()))
        .expression_attribute_values(":at", AttributeValue::S(at))
        .expression_attribute_values(":ms", AttributeValue::N(now_ms.to_string()))
}

/// Adds the expected-prior-rate guard (lost-race safety).
fn guard_expected_rate(req: UpdateReq, expected: Option<u32>) -> UpdateReq {
    match expected {
        Some(r) => req
            .condition_expression("target_rate = :exp")
            .expression_attribute_values(":exp", AttributeValue::N(r.to_string())),
        None => req.condition_expression("attribute_not_exists(target_rate)"),
    }
}

/// Adds the debounce guard (last mutation older than the window), composing with
/// any existing condition via AND.
fn guard_debounce(req: UpdateReq, now_ms: u64) -> UpdateReq {
    let cutoff = now_ms.saturating_sub(crate::DEBOUNCE_MS).to_string();
    let debounce = "(attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms < :cutoff)";
    let combined = match req.get_condition_expression().clone() {
        Some(c) => format!("{c} AND {debounce}"),
        None => debounce.to_owned(),
    };
    req.condition_expression(combined)
        .expression_attribute_values(":cutoff", AttributeValue::N(cutoff))
}

/// Sends a guarded update, mapping a failed condition to `Conflict`.
async fn send_guarded(req: UpdateReq, what: &str) -> Result<(), StoreError> {
    match req.send().await {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(se))
            if matches!(
                se.err(),
                UpdateItemError::ConditionalCheckFailedException(_)
            ) =>
        {
            Err(StoreError::Conflict)
        }
        Err(e) => Err(StoreError::Backend(format!("update_item({what}): {e}"))),
    }
}
