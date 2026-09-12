//! The `aws-sdk-dynamodb`-backed [`Store`] implementation over the single
//! `Counters` item.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::event_key;
use wr_common::{Phase, StoredControl};

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
            .set_key(Some(event_key(event_id)))
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
            stored_control: stored_control_from(item),
            fail_open_until: num("fail_open_until").unwrap_or(0),
            last_action: str_attr("last_action"),
            last_action_by: str_attr("last_action_by"),
            last_action_at: str_attr("last_action_at"),
            last_action_epoch_ms: item
                .get("last_action_epoch_ms")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok()),
            starts_at: num(wr_common::STARTS_AT_ATTR),
            starts_at_timezone: str_attr(wr_common::STARTS_AT_TZ_ATTR),
        }))
    }

    async fn set_phase(
        &self,
        event_id: &str,
        from: Phase,
        to: Phase,
        action: crate::AdminAction,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // Guarded on the expected phase (lost-race safety) but NOT debounced —
        // like force_maintenance, the operator's lifecycle/recovery move must
        // always apply (ADR-0017 §6 audit stamped atomically).
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET phase = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .condition_expression("phase = :from")
            .expression_attribute_values(":to", AttributeValue::S(to.as_wire_str().to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()));
        req = apply_audit_values(req, action, actor, now_ms)?;
        send_guarded(req, "phase").await
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
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET target_rate = :r, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":r", AttributeValue::N(rate.to_string()));
        req = apply_audit_values(req, crate::AdminAction::SetRate, actor, now_ms)?;
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
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET message = :m, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":m", AttributeValue::S(message.to_owned()));
        req = apply_audit_values(req, crate::AdminAction::SetMessage, actor, now_ms)?;
        req = guard_debounce(req, now_ms);
        send_guarded(req, "message").await
    }

    async fn set_stored_control(
        &self,
        event_id: &str,
        from: StoredControl,
        to: StoredControl,
        action: crate::AdminAction,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // The write applies only from the expected prior value, so a transition
        // another operator already made is a Conflict rather than a second
        // apply. An event that has never been written carries no attribute at
        // all, which reads as `open`. Never touches fail_open_until — the two
        // are orthogonal (issue #71).
        let control_guard = if from == StoredControl::Open {
            "(attribute_not_exists(admission_control) OR admission_control = :from)"
        } else {
            "admission_control = :from"
        };
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET admission_control = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":to", AttributeValue::S(to.as_wire_str().to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()))
            .condition_expression(control_guard);
        req = apply_audit_values(req, action, actor, now_ms)?;
        // The debounce boundary lives in the single shared `guard_debounce`
        // helper so the admission-control transition uses the same inclusive
        // cutoff as `set_rate` and `set_message`.
        req = guard_debounce(req, now_ms);
        send_guarded(req, "admission_control").await
    }

    async fn set_fail_open_until(
        &self,
        event_id: &str,
        until: u64,
        action: crate::AdminAction,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // Unconditional (break-glass), unlike set_stored_control: the operator
        // must always be able to engage or clear it, mirroring
        // force_maintenance rather than the guarded pause/resume pair.
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET fail_open_until = :u, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":u", AttributeValue::N(until.to_string()));
        req = apply_audit_values(req, action, actor, now_ms)?;
        send_guarded(req, "fail_open_until").await
    }

    async fn set_starts_at(
        &self,
        event_id: &str,
        starts_at: Option<(u64, &str)>,
        action: crate::AdminAction,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        const AUDIT: &str = "last_action = :a, last_action_by = :by, \
                             last_action_at = :at, last_action_epoch_ms = :ms";
        let (at_attr, tz_attr) = (wr_common::STARTS_AT_ATTR, wr_common::STARTS_AT_TZ_ATTR);

        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)));

        // Clearing REMOVEs both attributes instead of writing a zero, so the
        // read path can still tell an unscheduled event from one whose start
        // has already passed. Note both branches keep a SET clause: the audit
        // stamp is written either way, and a bare REMOVE would drop it.
        req = match starts_at {
            Some((secs, tz)) => req
                .update_expression(format!("SET {at_attr} = :s, {tz_attr} = :tz, {AUDIT}"))
                .expression_attribute_values(":s", AttributeValue::N(secs.to_string()))
                .expression_attribute_values(":tz", AttributeValue::S(tz.to_owned())),
            None => req.update_expression(format!("SET {AUDIT} REMOVE {at_attr}, {tz_attr}")),
        };

        req = apply_audit_values(req, action, actor, now_ms)?;
        // Debounced like set_rate and set_message: scheduling is a routine
        // control, not the break-glass that set_fail_open_until is.
        req = guard_debounce(req, now_ms);
        send_guarded(req, wr_common::STARTS_AT_ATTR).await
    }

    async fn set_rules_audit(
        &self,
        event_id: &str,
        rules_digest: &str,
        rules_count: usize,
        actor: &str,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        // Unconditional, like set_fail_open_until: the ruleset itself already
        // landed in the KeyValueStore by the time this runs, so there is
        // nothing here to guard a race against — only the record of it.
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(event_key(event_id)))
            .update_expression(
                "SET rules_digest = :d, rules_count = :c, last_action = :a, \
                 last_action_by = :by, last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":d", AttributeValue::S(rules_digest.to_owned()))
            .expression_attribute_values(":c", AttributeValue::N(rules_count.to_string()));
        req = apply_audit_values(req, crate::AdminAction::SetRules, actor, now_ms)?;
        send_guarded(req, "rules_audit").await
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
            .set_key(Some(event_key(event_id)))
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
        req = apply_audit_values(req, crate::AdminAction::ForceMaintenance, actor, now_ms)?;
        send_guarded(req, "force_maintenance").await
    }
}

/// Reads the stored admission control off a `Counters` item. Absent or
/// unparsable — including a legacy "`fail_open`" string left by a table written
/// before issue #71 — resolves to `Open`: that is the state an event is
/// created in and the value no operator action has written, and the epoch
/// (`fail_open_until`) is the sole authority for fail-open now, so a stale
/// string carries no window to reopen.
fn stored_control_from(item: &std::collections::HashMap<String, AttributeValue>) -> StoredControl {
    item.get("admission_control")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(StoredControl::Open)
}

type UpdateReq = aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;

/// Stamps the shared audit + epoch values onto a mutation.
///
/// The row records the same instant twice — `last_action_at` for an operator
/// to read and `last_action_epoch_ms` for the debounce guard to compare — so
/// both are rendered from `now_ms` or neither is written. A timestamp that
/// cannot be rendered fails the mutation instead of falling back to a
/// different instant, which would leave the two attributes making
/// contradictory claims about when the action happened.
///
/// # Errors
///
/// [`StoreError::Backend`] if `now_ms` is not a representable instant.
fn apply_audit_values(
    req: UpdateReq,
    action: crate::AdminAction,
    actor: &str,
    now_ms: u64,
) -> Result<UpdateReq, StoreError> {
    let at = audit_timestamp(now_ms)?;
    Ok(req
        .expression_attribute_values(":a", AttributeValue::S(action.as_str().to_owned()))
        .expression_attribute_values(":by", AttributeValue::S(actor.to_owned()))
        .expression_attribute_values(":at", AttributeValue::S(at))
        .expression_attribute_values(":ms", AttributeValue::N(now_ms.to_string())))
}

/// Renders epoch-millis as the `YYYY-MM-DDTHH:MM:SSZ` string an operator reads
/// off the dashboard.
///
/// # Errors
///
/// [`StoreError::Backend`] if `now_ms` is past the range of representable
/// instants. Failing is the point: the alternative is a stand-in date that
/// disagrees with the epoch written beside it in the same row.
fn audit_timestamp(now_ms: u64) -> Result<String, StoreError> {
    let at = i64::try_from(now_ms)
        .ok()
        .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        .ok_or_else(|| StoreError::Backend(format!("{now_ms} is not a representable instant")))?;
    Ok(at.strftime("%Y-%m-%dT%H:%M:%SZ").to_string())
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

/// The debounce predicate appended to every guarded mutation. Inclusive at
/// the cutoff (`<= :cutoff`): with `cutoff = now_ms - DEBOUNCE_MS`,
/// `last_action_epoch_ms <= cutoff` is the same inequality as
/// `now_ms - last_action_epoch_ms >= DEBOUNCE_MS`, which is exactly what the
/// in-process `debounce_check` accepts. A mutation at the boundary (`delta ==
/// DEBOUNCE_MS`) must succeed on both halves of the guard — a strict `<` here
/// would reject it and surface a misleading HTTP 409 instead of the expected
/// success.
///
/// The third clause matches [`crate::debounce_check`]'s treatment of a stamp
/// later than now: it describes no prior action, so it does not hold the
/// window open. Without it a stamp in the future — the far side of a clock
/// jump — would satisfy neither of the first two clauses and reject every
/// mutation from then on, with nothing to bring it back into the past.
const DEBOUNCE_PREDICATE: &str = "(attribute_not_exists(last_action_epoch_ms) \
     OR last_action_epoch_ms <= :cutoff OR last_action_epoch_ms > :now)";

/// Composes [`DEBOUNCE_PREDICATE`] with any existing condition via `AND`. Pure
/// so the inclusive boundary is unit-testable without a `DynamoDB` client; the
/// live `DynamoStore` runs the same string via [`guard_debounce`].
fn combine_debounce(existing: Option<&str>) -> String {
    match existing {
        Some(c) => format!("{c} AND {DEBOUNCE_PREDICATE}"),
        None => DEBOUNCE_PREDICATE.to_owned(),
    }
}

/// Adds the debounce guard (a prior mutation at least `DEBOUNCE_MS` ago),
/// composing with any existing condition via AND.
fn guard_debounce(req: UpdateReq, now_ms: u64) -> UpdateReq {
    let cutoff = now_ms.saturating_sub(crate::DEBOUNCE_MS).to_string();
    let combined = match req.get_condition_expression().clone() {
        Some(c) => combine_debounce(Some(c.as_str())),
        None => combine_debounce(None),
    };
    req.condition_expression(combined)
        .expression_attribute_values(":cutoff", AttributeValue::N(cutoff))
        .expression_attribute_values(":now", AttributeValue::N(now_ms.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn item(control: Option<&str>) -> std::collections::HashMap<String, AttributeValue> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "event_id".to_owned(),
            AttributeValue::S("launch".to_owned()),
        );
        if let Some(c) = control {
            map.insert(
                "admission_control".to_owned(),
                AttributeValue::S(c.to_owned()),
            );
        }
        map
    }

    #[test]
    fn every_stored_control_reads_back_as_itself() {
        for control in [StoredControl::Open, StoredControl::Paused] {
            assert_eq!(
                stored_control_from(&item(Some(control.as_wire_str()))),
                control
            );
        }
    }

    #[test]
    fn a_legacy_fail_open_string_now_reads_back_as_open() {
        // INVERTED from the pre-#71 invariant: fail-open is no longer a
        // storable string at all (StoredControl has two values), so a
        // "fail_open" string left by a table written before this change must
        // decay to Open — the epoch (fail_open_until) is the sole authority
        // now, and a stale string carries no window to reopen.
        let control = stored_control_from(&item(Some("fail_open")));
        assert_eq!(control, StoredControl::Open);
    }

    #[test]
    fn absent_or_unknown_control_defaults_to_open() {
        assert_eq!(stored_control_from(&item(None)), StoredControl::Open);
        assert_eq!(
            stored_control_from(&item(Some("nonsense"))),
            StoredControl::Open
        );
        // A non-string attribute is as unusable as a missing one.
        let mut wrong_type = item(None);
        wrong_type.insert(
            "admission_control".to_owned(),
            AttributeValue::N("1".to_owned()),
        );
        assert_eq!(stored_control_from(&wrong_type), StoredControl::Open);
    }

    #[test]
    fn debounce_predicate_is_inclusive_at_the_cutoff() {
        // With `cutoff = now_ms - DEBOUNCE_MS`, the DynamoDB condition must
        // accept the exact boundary `delta == DEBOUNCE_MS`, matching the
        // in-process `debounce_check` (which rejects only `delta < DEBOUNCE_MS`).
        // Reverting to the strict `< :cutoff` rejects the boundary and surfaces a
        // misleading HTTP 409 instead of the expected success.
        assert!(
            DEBOUNCE_PREDICATE.contains("<= :cutoff"),
            "debounce predicate must be inclusive at the cutoff, got: {DEBOUNCE_PREDICATE}"
        );
        assert!(
            !DEBOUNCE_PREDICATE.contains("< :cutoff"),
            "debounce predicate must not use the strict `<` operator at the cutoff"
        );
    }

    #[test]
    fn combine_debounce_ands_the_predicate_onto_an_existing_guard() {
        // A prior guard (expected-rate or admission-control) is preserved and
        // the inclusive debounce predicate is AND-ed after it.
        assert_eq!(
            combine_debounce(Some("target_rate = :exp")).as_str(),
            "target_rate = :exp AND (attribute_not_exists(last_action_epoch_ms) \
             OR last_action_epoch_ms <= :cutoff OR last_action_epoch_ms > :now)"
        );
        // `set_message` has no prior guard: the predicate stands alone.
        assert_eq!(combine_debounce(None).as_str(), DEBOUNCE_PREDICATE);
    }

    #[test]
    fn debounce_predicate_ignores_a_stamp_later_than_now() {
        // Without this clause a stamp in the future satisfies neither
        // `attribute_not_exists` nor `<= :cutoff`, so every debounced action —
        // rate, message, pause, resume, rules, start time — is rejected from
        // then on, and no later action can move the stamp back into the past.
        assert!(
            DEBOUNCE_PREDICATE.contains("last_action_epoch_ms > :now"),
            "debounce predicate must tolerate a stamp later than now, got: {DEBOUNCE_PREDICATE}"
        );
    }

    #[test]
    fn the_audit_timestamp_matches_the_epoch_written_beside_it() {
        // Both attributes describe one instant, so the rendered string has to
        // be that instant and not a stand-in for it.
        assert_eq!(
            audit_timestamp(1_788_000_000_000).ok(),
            Some("2026-08-29T10:40:00Z".to_owned())
        );
        assert_eq!(
            audit_timestamp(0).ok(),
            Some("1970-01-01T00:00:00Z".to_owned())
        );
    }

    #[test]
    fn an_unrepresentable_timestamp_is_refused_not_substituted() {
        // The saturated case: rendering it as the epoch would write
        // 1970-01-01 next to an epoch-millis of u64::MAX in the same row, two
        // claims about one action that cannot both be true.
        let refused = audit_timestamp(u64::MAX);
        assert!(
            matches!(refused, Err(StoreError::Backend(_))),
            "an unrepresentable instant must fail the mutation, got: {refused:?}"
        );
    }
}
