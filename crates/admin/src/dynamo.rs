//! The `aws-sdk-dynamodb`-backed [`Store`] implementation over the single
//! `Counters` item.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use jiff::Timestamp;

use crate::arrival::ArrivalTime;
use wr_common::expr::Key;
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

    /// Builds the guarded rules-audit `UpdateItem` (the `KeyValueStore` ruleset
    /// itself has already landed) without sending it, so a regression test can
    /// assert the anti-regression guard is wired into the live builder. Guarded
    /// by the same [`STAMP_PREDICATE`] that [`Self::stamp_action`] uses: the
    /// audit row carries the shared `last_action_epoch_ms` anchor the debounce
    /// guard reads back, so a stamp issued after the yielding `KeyValueStore`
    /// read-modify-write must not overwrite a newer concurrent writer's anchor.
    fn build_rules_audit_req(
        &self,
        event_id: &str,
        rules_digest: &str,
        rules_count: usize,
        actor: &str,
        now: ArrivalTime,
    ) -> UpdateReq {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .condition_expression(STAMP_PREDICATE)
            .update_expression(
                "SET rules_digest = :d, rules_count = :c, last_action = :a, \
                 last_action_by = :by, last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":d", AttributeValue::S(rules_digest.to_owned()))
            .expression_attribute_values(":c", AttributeValue::N(rules_count.to_string()));
        req = apply_audit_values(req, crate::AdminAction::SetRules, actor, now);
        req
    }

    /// Builds the guarded (happy-path) `set_fail_open_until` `UpdateItem`:
    /// `fail_open_until`, the audit fields, and the `last_action_epoch_ms` anchor
    /// land together in one atomic write guarded by [`STAMP_PREDICATE`]. On the
    /// happy path the guard passes and the write is the same single `UpdateItem`
    /// the code has always had, with the anchor additionally guarded against
    /// regression. Returns the builder without sending so a regression test can
    /// assert the guard is wired in.
    fn build_fail_open_until_req(
        &self,
        event_id: &str,
        until: u64,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
    ) -> UpdateReq {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .condition_expression(STAMP_PREDICATE)
            .update_expression(
                "SET fail_open_until = :u, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":u", AttributeValue::N(until.to_string()));
        req = apply_audit_values(req, action, actor, now);
        req
    }

    /// Builds the Conflict-only fallback `UpdateItem` that writes `fail_open_until`
    /// alone — no audit fields, no `last_action_epoch_ms` — so the operator's
    /// break-glass effect still reaches the controller's data plane while the
    /// winning writer's newer anchor and audit row are preserved. Unconditional,
    /// mirroring the contract that an operator must always be able to engage or
    /// clear fail-open; the guard that rejected the first attempt only governs
    /// the *stamp*, not the functional write. Returns the builder without sending
    /// so a regression test can assert the fallback touches only `fail_open_until`.
    fn build_fail_open_until_fallback_req(&self, event_id: &str, until: u64) -> UpdateReq {
        self.client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression("SET fail_open_until = :u")
            .expression_attribute_values(":u", AttributeValue::N(until.to_string()))
    }

    /// Builds the guarded `stamp_action` `UpdateItem` (audit fields + the
    /// shared `last_action_epoch_ms` anchor) without sending it, so a
    /// regression test can assert the anti-regression guard is wired into the
    /// live builder. Extracted for symmetry with [`Self::build_rules_audit_req`]
    /// and [`Self::build_fail_open_until_req`]: every post-yield stamper that
    /// writes the shared anchor is built by a named helper the guard-presence
    /// test can reach.
    fn build_stamp_action_req(
        &self,
        event_id: &str,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
    ) -> UpdateReq {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .condition_expression(STAMP_PREDICATE)
            .update_expression(
                "SET last_action = :a, last_action_by = :by, last_action_at = :at, \
                 last_action_epoch_ms = :ms",
            );
        req = apply_audit_values(req, action, actor, now);
        req
    }

    /// A `DynamoStore` over a zero-config client for regression tests that
    /// inspect the `condition_expression` / `update_expression` each builder
    /// emits. The builder is never sent, so no network or credentials are
    /// needed; this only exercises the SDK's fluent-builder construction.
    #[cfg(test)]
    fn for_test() -> Self {
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .build();
        Self {
            client: aws_sdk_dynamodb::Client::from_conf(conf),
            counters_table: "Counters".to_owned(),
        }
    }
}

impl Store for DynamoStore {
    async fn load(&self, event_id: &str) -> Result<Option<ControlState>, StoreError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
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
            stored_control: wr_common::stored_control_of(item),
            fail_open_until: num("fail_open_until").unwrap_or(0),
            last_action: str_attr("last_action"),
            last_action_by: str_attr("last_action_by"),
            last_action_at: str_attr("last_action_at"),
            // A stored epoch outside the range of an instant is dropped here
            // rather than carried inland: the debounce guard is the only
            // reader, and a value it cannot subtract from would reject every
            // later action. Absent and unusable mean the same thing.
            last_action_time: item
                .get("last_action_epoch_ms")
                .and_then(|v| v.as_n().ok())
                .and_then(|n| n.parse().ok())
                .and_then(|ms| Timestamp::from_millisecond(ms).ok()),
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
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        // Guarded on the expected phase (lost-race safety) but NOT debounced —
        // like force_maintenance, the operator's lifecycle/recovery move must
        // always apply (ADR-0017 §6 audit stamped atomically).
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(
                "SET phase = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .condition_expression("phase = :from")
            .expression_attribute_values(":to", AttributeValue::S(to.as_wire_str().to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()));
        req = apply_audit_values(req, action, actor, now);
        send_guarded(req, "phase").await
    }

    async fn set_rate(
        &self,
        event_id: &str,
        expected: Option<u32>,
        rate: u32,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(
                "SET target_rate = :r, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":r", AttributeValue::N(rate.to_string()));
        req = apply_audit_values(req, crate::AdminAction::SetRate, actor, now);
        req = guard_expected_rate(req, expected);
        req = guard_debounce(req, now);
        send_guarded(req, "rate").await
    }

    async fn set_message(
        &self,
        event_id: &str,
        message: &str,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(
                "SET message = :m, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":m", AttributeValue::S(message.to_owned()));
        req = apply_audit_values(req, crate::AdminAction::SetMessage, actor, now);
        req = guard_debounce(req, now);
        send_guarded(req, "message").await
    }

    async fn set_stored_control(
        &self,
        event_id: &str,
        from: StoredControl,
        to: StoredControl,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
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
            .set_key(Some(Key::Event { event_id }.build()))
            .update_expression(
                "SET admission_control = :to, last_action = :a, last_action_by = :by, \
                 last_action_at = :at, last_action_epoch_ms = :ms",
            )
            .expression_attribute_values(":to", AttributeValue::S(to.as_wire_str().to_owned()))
            .expression_attribute_values(":from", AttributeValue::S(from.as_wire_str().to_owned()))
            .condition_expression(control_guard);
        req = apply_audit_values(req, action, actor, now);
        // The debounce boundary lives in the single shared `guard_debounce`
        // helper so the admission-control transition uses the same inclusive
        // cutoff as `set_rate` and `set_message`.
        req = guard_debounce(req, now);
        send_guarded(req, "admission_control").await
    }

    async fn set_fail_open_until(
        &self,
        event_id: &str,
        until: u64,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        // Break-glass, like force_maintenance: an operator must always be able
        // to engage or clear fail_open. But `fail_open_until`'s `UpdateItem`
        // also writes the shared `last_action_epoch_ms` anchor the debounce
        // guard reads back, and this runs *after* the yielding KVStore
        // read-modify-write in `apply_fail_open`/`apply_recover` — so, like
        // `stamp_action`, the stamp is guarded against regression by
        // `STAMP_PREDICATE`. The guard is per-item, so on the happy path the
        // functional write, audit fields, and stamp land atomically together
        // — the same single-`UpdateItem` shape the code has always had, with
        // the anchor now protected.
        //
        // On the race path a newer concurrent writer has already advanced the
        // anchor, the guard rejects the whole (atomic) write as `Conflict`,
        // and the fallback commits `fail_open_until` alone — *without*
        // touching the stamp or audit fields — so the operator's effect still
        // reaches the controller's data plane while the winning writer's newer
        // anchor and audit row prevail. The audit trace of *this* fail-open is
        // lost on that path (logged + metric'd, matching `rules_audit_failed`),
        // a stale-but-present-vs-absent trade, not a strict improvement.
        let req = self.build_fail_open_until_req(event_id, until, action, actor, now);
        match send_guarded(req, "fail_open_until").await {
            Ok(()) => Ok(()),
            Err(StoreError::Conflict) => {
                tracing::warn!(
                    event = "fail_open_audit_lost",
                    "fail_open anchor already advanced by a newer writer; committing \
                     fail_open_until without the audit stamp to preserve the newer anchor"
                );
                let fallback = self.build_fail_open_until_fallback_req(event_id, until);
                fallback.send().await.map_err(|e| {
                    StoreError::Backend(format!("update_item(fail_open_until_fallback): {e}"))
                })?;
                Ok(())
            }
            // Backend errors: the guarded attempt didn't commit, fail_open_until
            // is unchanged, and the caller surfaces it exactly as the previous
            // unconditional write would have.
            Err(e) => Err(e),
        }
    }

    async fn set_starts_at(
        &self,
        event_id: &str,
        starts_at: Option<(u64, &str)>,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        const AUDIT: &str = "last_action = :a, last_action_by = :by, \
                             last_action_at = :at, last_action_epoch_ms = :ms";
        let (at_attr, tz_attr) = (wr_common::STARTS_AT_ATTR, wr_common::STARTS_AT_TZ_ATTR);

        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()));

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

        req = apply_audit_values(req, action, actor, now);
        // Debounced like set_rate and set_message: scheduling is a routine
        // control, not the break-glass that set_fail_open_until is.
        req = guard_debounce(req, now);
        send_guarded(req, wr_common::STARTS_AT_ATTR).await
    }

    async fn set_rules_audit(
        &self,
        event_id: &str,
        rules_digest: &str,
        rules_count: usize,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        // The ruleset itself already landed in the KeyValueStore before this
        // runs, so the effect fields (`rules_digest`/`rules_count`) are an
        // unconditional record of a change that happened — there is nothing to
        // guard a race against. But the same `UpdateItem` also writes the shared
        // `last_action_epoch_ms` anchor the debounce guard reads back, and this
        // runs *after* the yielding KVStore read-modify-write in
        // `apply_set_rules`, so the stamp is guarded against regression by the
        // same `STAMP_PREDICATE` `stamp_action` uses. A lost race surfaces as
        // `Conflict`, which `apply_set_rules` already logs and swallows (the KV
        // ruleset already landed), keeping the dashboard's "latest action" line
        // on the actual latest action rather than a stale, earlier stamp.
        let req = self.build_rules_audit_req(event_id, rules_digest, rules_count, actor, now);
        send_guarded(req, "rules_audit").await
    }

    async fn stamp_action(
        &self,
        event_id: &str,
        action: crate::AdminAction,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        // Audit fields only -- the action's real effect landed elsewhere (the
        // open's own conditional write) -- but this runs *after* a yielding
        // network call (the open invoke), so a concurrent debounced mutation
        // (`set_rate`, `set_message`, ...) can advance `last_action*` and
        // `last_action_epoch_ms` between that effect and this `UpdateItem`.
        // `last_action_epoch_ms` is also the anchor `DEBOUNCE_PREDICATE` reads,
        // so the stamp is guarded against regression: it applies only when no
        // newer value already exists, matching the predicate's own
        // anti-regression contract. A lost race surfaces as
        // `ConditionalCheckFailedException` -> `Conflict`, which the caller
        // logs and swallows (the open already happened), so the dashboard's
        // "latest action" line keeps the actual latest action rather than a
        // stale, earlier open's arrival time -- at the cost of leaving no
        // audit trace of the open itself when it loses the race.
        let req = self.build_stamp_action_req(event_id, action, actor, now);
        send_guarded(req, "stamp_action").await
    }

    async fn force_maintenance(
        &self,
        event_id: &str,
        from: Phase,
        actor: &str,
        now: ArrivalTime,
    ) -> Result<(), StoreError> {
        // Guarded on the expected phase (lost-race safety) but NOT debounced —
        // the emergency stop must always apply.
        let mut req = self
            .client
            .update_item()
            .table_name(&self.counters_table)
            .set_key(Some(Key::Event { event_id }.build()))
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
        req = apply_audit_values(req, crate::AdminAction::ForceMaintenance, actor, now);
        send_guarded(req, "force_maintenance").await
    }
}

type UpdateReq = aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder;

/// Stamps the shared audit + epoch values onto a mutation.
///
/// The row records the same instant twice — `last_action_at` for an operator
/// to read and `last_action_epoch_ms` for the debounce guard to compare — and
/// both are derived here from one [`Timestamp`], so the two attributes cannot
/// disagree about when the action happened. Neither conversion can fail: a
/// `Timestamp` spans at most ±9999 years, which is well inside the range of
/// both the rendered string and the epoch-millis integer.
fn apply_audit_values(
    req: UpdateReq,
    action: crate::AdminAction,
    actor: &str,
    now: ArrivalTime,
) -> UpdateReq {
    req.expression_attribute_values(":a", AttributeValue::S(action.as_str().to_owned()))
        .expression_attribute_values(":by", AttributeValue::S(actor.to_owned()))
        .expression_attribute_values(":at", AttributeValue::S(audit_timestamp(now.timestamp())))
        .expression_attribute_values(
            ":ms",
            AttributeValue::N(now.timestamp().as_millisecond().to_string()),
        )
}

/// Renders an instant as the `YYYY-MM-DDTHH:MM:SSZ` string an operator reads
/// off the dashboard.
fn audit_timestamp(now: Timestamp) -> String {
    now.strftime("%Y-%m-%dT%H:%M:%SZ").to_string()
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
/// the cutoff (`<= :cutoff`): with `cutoff = now - DEBOUNCE`,
/// `last_action_epoch_ms <= cutoff` is the same inequality as
/// `now - last_action_epoch_ms >= DEBOUNCE`, which is exactly what the
/// in-process `debounce_check` accepts. A mutation at the boundary (`delta ==
/// DEBOUNCE`) must succeed on both halves of the guard — a strict `<` here
/// would reject it and surface a misleading HTTP 409 instead of the expected
/// success.
const DEBOUNCE_PREDICATE: &str =
    "(attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms <= :cutoff)";

/// The anti-regression contract a post-effect audit stamp holds: the stamp
/// applies only when no newer `last_action_epoch_ms` already exists. `stamp_action`
/// runs *after* a yielding network call (the open invoke), so a concurrent
/// debounced mutation can have advanced the anchor between the open's effect
/// and the stamp; without this guard that newer anchor and its audit fields
/// would be overwritten by the older open's arrival time. Mirrors
/// [`DEBOUNCE_PREDICATE`]'s own inclusive `<= :ms`, so a stamp landing at the
/// exact instant of the stored anchor (neither newer nor older) succeeds, like
/// a debounce at the boundary, rather than rejecting a stamp that should
/// apply. Pure so the inclusive boundary is unit-testable without a `DynamoDB`
/// client; the live `DynamoStore` runs the same string via `stamp_action`.
const STAMP_PREDICATE: &str =
    "attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms <= :ms";

/// Composes [`DEBOUNCE_PREDICATE`] with any existing condition via `AND`. Pure
/// so the inclusive boundary is unit-testable without a `DynamoDB` client; the
/// live `DynamoStore` runs the same string via [`guard_debounce`].
fn combine_debounce(existing: Option<&str>) -> String {
    match existing {
        Some(c) => format!("{c} AND {DEBOUNCE_PREDICATE}"),
        None => DEBOUNCE_PREDICATE.to_owned(),
    }
}

/// Adds the debounce guard (a prior mutation at least [`crate::DEBOUNCE`]
/// ago), composing with any existing condition via AND.
fn guard_debounce(req: UpdateReq, now: ArrivalTime) -> UpdateReq {
    let cutoff = now
        .timestamp()
        .as_millisecond()
        .saturating_sub(crate::DEBOUNCE_MILLIS)
        .to_string();
    let combined = match req.get_condition_expression().clone() {
        Some(c) => combine_debounce(Some(c.as_str())),
        None => combine_debounce(None),
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

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        reason = "test code panics on setup failure"
    )]

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
                wr_common::stored_control_of(&item(Some(control.as_wire_str()))),
                control
            );
        }
    }

    #[test]
    fn a_present_unreadable_admission_control_holds_rather_than_resumes() {
        // #175 made the rule shared rather than duplicated per reader: the
        // controller is the component that releases people, and the admin
        // dashboard is the one that displays the state, so two copies of the
        // rule are two chances for them to disagree on the same stored
        // value. The only writer of this attribute is the operator's pause,
        // so a value that will not parse is a pause that did not land cleanly;
        // reading it as Open would resume admission during the incident
        // someone was trying to stop. Holding is both the safe direction and
        // the visible one — a queue that stops moving gets noticed; an
        // un-pause does not. This drives the same set as
        // wr_common::items::an_unreadable_admission_control_holds_rather_than_resumes
        // through the same shared rule the controller's read_state uses, so
        // the admin suite fails if its reader is ever re-forked off it back
        // to the permissive Open default this crate once carried.
        for stored in ["fail_open", "PAUSED", "paused ", "\u{1}", "0"] {
            assert_eq!(
                wr_common::stored_control_of(&item(Some(stored))),
                StoredControl::Paused,
                "{stored:?} resumed admission"
            );
        }
    }

    #[test]
    fn an_absent_admission_control_is_still_normal_admission() {
        // Absent is not corrupt: it is an event nobody has paused, and a
        // non-string or empty attribute is as unusable as a missing one.
        // Holding here would stall every event that never touched the
        // control.
        assert_eq!(
            wr_common::stored_control_of(&item(None)),
            StoredControl::Open
        );
        assert_eq!(
            wr_common::stored_control_of(&item(Some(""))),
            StoredControl::Open
        );
        // A non-string attribute is as unusable as a missing one.
        let mut wrong_type = item(None);
        wrong_type.insert(
            "admission_control".to_owned(),
            AttributeValue::N("1".to_owned()),
        );
        assert_eq!(
            wr_common::stored_control_of(&wrong_type),
            StoredControl::Open
        );
    }

    #[test]
    fn debounce_predicate_is_inclusive_at_the_cutoff() {
        // With `cutoff = now - DEBOUNCE`, the DynamoDB condition must
        // accept the exact boundary `delta == DEBOUNCE`, matching the
        // in-process `debounce_check` (which rejects only `delta < DEBOUNCE`).
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
    fn stamp_predicate_refuses_to_regress_a_newer_anchor() {
        // `stamp_action` runs after the open's yielding invoke, so a concurrent
        // mutation can have advanced `last_action_epoch_ms` past the open's
        // arrival time by the time the stamp lands. The predicate must refuse
        // to overwrite that newer anchor (and its audit fields), and must still
        // allow the very first stamp on an event that has no anchor, mirroring
        // `DEBOUNCE_PREDICATE`'s own contract. Inclusive at the boundary (`<=`),
        // so a stamp landing at the exact instant of the stored anchor succeeds
        // rather than rejecting a stamp that should apply.
        assert!(
            STAMP_PREDICATE.contains("attribute_not_exists(last_action_epoch_ms)"),
            "stamp predicate must allow the first-ever stamp on an anchor-less event, got: \
             {STAMP_PREDICATE}"
        );
        assert!(
            STAMP_PREDICATE.contains("<= :ms"),
            "stamp predicate must be inclusive at the cutoff, got: {STAMP_PREDICATE}"
        );
        assert!(
            !STAMP_PREDICATE.contains("< :ms"),
            "stamp predicate must not use the strict `<` operator, which would reject a stamp \
             landing at the exact instant of the stored anchor"
        );
    }

    /// An arrival `ms` milliseconds after the epoch; the only thing the
    /// cross-method builders below care about is that the value threads through
    /// to `:ms`, so small round numbers keep that readable. Mirrors the `ts`
    /// helper in the `lib.rs` test module.
    fn ts(ms: i64) -> ArrivalTime {
        ArrivalTime::for_test_millis(ms)
    }

    #[test]
    fn set_rules_audit_builder_carries_the_anti_regression_guard() {
        // Bug (anchor regression, sibling of #186): `set_rules_audit` runs
        // after the yielding KVStore read-modify-write and wrote
        // `last_action_epoch_ms` unconditionally, so a concurrent debounced
        // mutation that landed during that read-modify-write had its newer
        // anchor overwritten by the older arrival stamp — a bounded debounce
        // bypass. The live builder must carry `STAMP_PREDICATE`, mirroring
        // `stamp_action`, so the stamp applies only when no newer anchor
        // exists. The `FakeStore` double cannot catch a regression that drops
        // this guard (it does not model the live `DynamoStore`'s builder), so
        // this asserts the guard on the live builder the trait method actually
        // sends.
        let store = DynamoStore::for_test();
        let req = store.build_rules_audit_req("evt", "deadbeefdeadbeef", 2, "op@x", ts(1_000));
        assert_eq!(
            req.get_condition_expression().as_deref(),
            Some(STAMP_PREDICATE),
            "rules_audit must guard the shared anchor against regression"
        );
        let upd = req
            .get_update_expression()
            .as_deref()
            .expect("rules_audit sets an update expression");
        assert!(
            upd.contains("last_action_epoch_ms = :ms"),
            "rules_audit must stamp the shared anchor: {upd}"
        );
        assert!(
            upd.contains("rules_digest = :d") && upd.contains("rules_count = :c"),
            "rules_audit must still record the ruleset: {upd}"
        );
    }

    #[test]
    fn set_fail_open_until_happy_path_builder_carries_the_anti_regression_guard() {
        // The functional `fail_open_until` write the controller reads must
        // remain unconditional, but the `last_action_epoch_ms` anchor that
        // shares its `UpdateItem` is guarded: the primary (happy-path) builder
        // carries `STAMP_PREDICATE` so the stamp cannot regress a newer anchor
        // a concurrent writer advanced during the yielding KVStore
        // read-modify-write. On the happy path the guard passes, so
        // `fail_open_until`, the audit fields, and the anchor land atomically
        // together — the same single-`UpdateItem` shape the code has always
        // had, with the anchor now protected.
        let store = DynamoStore::for_test();
        let req = store.build_fail_open_until_req(
            "evt",
            5_000,
            crate::AdminAction::FailOpen,
            "op@x",
            ts(1_000),
        );
        assert_eq!(
            req.get_condition_expression().as_deref(),
            Some(STAMP_PREDICATE),
            "fail_open_until happy-path write must guard the shared anchor against regression"
        );
        let upd = req
            .get_update_expression()
            .as_deref()
            .expect("fail_open_until sets an update expression");
        assert!(
            upd.contains("fail_open_until = :u") && upd.contains("last_action_epoch_ms = :ms"),
            "fail_open_until happy-path write lands the functional field and the anchor together: \
             {upd}"
        );
    }

    #[test]
    fn set_fail_open_until_fallback_builder_writes_only_the_functional_field() {
        // On the race path the guard rejects the whole primary write as
        // `Conflict`; the fallback must commit `fail_open_until` alone so the
        // operator's break-glass effect reaches the controller's data plane
        // while the winning writer's newer anchor and audit row are preserved.
        // Touching `last_action_epoch_ms` or any audit field here would
        // re-introduce the regression the guard just prevented; carrying a
        // condition here would risk rejecting the break-glass write itself.
        let store = DynamoStore::for_test();
        let req = store.build_fail_open_until_fallback_req("evt", 5_000);
        assert_eq!(
            req.get_condition_expression().as_deref(),
            None,
            "fallback must be unconditional so the break-glass write always lands"
        );
        let upd = req
            .get_update_expression()
            .as_deref()
            .expect("fallback sets an update expression");
        assert_eq!(
            upd, "SET fail_open_until = :u",
            "fallback must touch only fail_open_until, not the anchor or audit fields: {upd}"
        );
        for forbidden in [
            "last_action",
            "last_action_by",
            "last_action_at",
            "last_action_epoch_ms",
        ] {
            assert!(
                !upd.contains(forbidden),
                "fallback must not write the audit field `{forbidden}`: {upd}"
            );
        }
    }

    #[test]
    fn stamp_action_builder_carries_the_anti_regression_guard() {
        // Hardening the already-fixed path (#186): the `STAMP_PREDICATE` guard
        // on `stamp_action` was previously asserted only via the predicate
        // string constant and the `FakeStore` double, never on the live
        // builder. Asserting it on the live builder closes the same detection
        // gap the two sibling stampers just had — a regression that drops the
        // guard from the builder would not be caught by the double.
        let store = DynamoStore::for_test();
        let req =
            store.build_stamp_action_req("evt", crate::AdminAction::OpenNow, "op@x", ts(1_000));
        assert_eq!(
            req.get_condition_expression().as_deref(),
            Some(STAMP_PREDICATE),
            "stamp_action must guard the shared anchor against regression"
        );
        let upd = req
            .get_update_expression()
            .as_deref()
            .expect("stamp_action sets an update expression");
        assert!(
            upd.contains("last_action_epoch_ms = :ms"),
            "stamp_action must stamp the shared anchor: {upd}"
        );
    }

    /// Live two-attempt sequence test against DynamoDB-Local. Validates the
    /// part the `cfg(test)` builder test and `FakeStore` cannot reach: that the
    /// live `DynamoStore::set_fail_open_until` issues a guarded `UpdateItem`,
    /// and on a `ConditionalCheckFailedException` falls back to a second
    /// `UpdateItem` writing only `fail_open_until`. Skipped unless
    /// `DDB_LOCAL_ENDPOINT` is set, so the normal `cargo test` run is unaffected;
    /// run with `cargo test -p admin -- --ignored live_set_fail_open_until`.
    #[tokio::test]
    #[ignore = "requires DDB_LOCAL_ENDPOINT (DynamoDB-Local)"]
    async fn live_set_fail_open_until_falls_back_to_a_functional_only_write() {
        let endpoint = std::env::var("DDB_LOCAL_ENDPOINT")
            .expect("DDB_LOCAL_ENDPOINT must point at a running DynamoDB-Local");
        let table = format!("CountersTest_{}", std::process::id());
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .endpoint_url(endpoint)
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "key", "secret", None, None, "test",
            ))
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(conf);
        let store = DynamoStore::new(client.clone(), table.clone());

        // Create the table the store reads/writes.
        client
            .create_table()
            .table_name(&table)
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("event_id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("event_id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .expect("create_table");

        // Seed: a concurrent writer already advanced the anchor to T=1100, with
        // an audit row attributing the latest action to opB's set_rate. The store
        // addresses the row by the `EVT#<event_id>` prefixed key (`Key::Event`),
        // so the seed and the assertions use that exact key, not the bare id.
        let key = Key::Event { event_id: "evt" }.build();
        client
            .put_item()
            .table_name(&table)
            .set_item(Some(key.clone()))
            .item("last_action_epoch_ms", AttributeValue::N("1100".to_owned()))
            .item("last_action", AttributeValue::S("set_rate".to_owned()))
            .item("last_action_by", AttributeValue::S("opB@x".to_owned()))
            .send()
            .await
            .expect("seed put");

        // Drive the live store with opA's fail_open arrival at T=1000 (older
        // than the seeded anchor at T=1100).
        let outcome = store
            .set_fail_open_until(
                "evt",
                5_000,
                crate::AdminAction::FailOpen,
                "opA@x",
                ts(1_000),
            )
            .await;
        outcome.expect("set_fail_open_until returns Ok");

        // Read back and assert the two-attempt net effect:
        let item = client
            .get_item()
            .table_name(&table)
            .set_key(Some(key.clone()))
            .send()
            .await
            .expect("get_item")
            .item
            .expect("item exists");

        // (1) The fallback functional write landed.
        assert_eq!(
            item.get("fail_open_until")
                .and_then(|v| v.as_n().ok())
                .map(std::string::String::as_str),
            Some("5000"),
            "fallback must commit fail_open_until"
        );
        // (2) The anchor was NOT regressed to the older arrival stamp 1000.
        assert_eq!(
            item.get("last_action_epoch_ms")
                .and_then(|v| v.as_n().ok())
                .map(std::string::String::as_str),
            Some("1100"),
            "anchor must not regress to the older fail_open arrival"
        );
        // (3) The audit row was NOT overwritten: still opB's set_rate.
        assert_eq!(
            item.get("last_action")
                .and_then(|v| v.as_s().ok())
                .map(std::string::String::as_str),
            Some("set_rate"),
            "audit label must stay on the concurrent writer"
        );
        assert_eq!(
            item.get("last_action_by")
                .and_then(|v| v.as_s().ok())
                .map(std::string::String::as_str),
            Some("opB@x"),
            "audit actor must stay on the concurrent writer"
        );

        // Clean up the table created for this run.
        let _ = client.delete_table().table_name(&table).send().await;
    }

    /// Builds a `DynamoStore` pointing at DynamoDB-Local (set up & torn down by
    /// the caller) and a freshly-created `Counters` table whose name includes
    /// the PID so parallel test runs don't clash. Returns `(store, client,
    /// table)`; the caller deletes the table at the end.
    async fn local_store_and_table(
        client: aws_sdk_dynamodb::Client,
    ) -> (DynamoStore, aws_sdk_dynamodb::Client, String) {
        let table = format!("CountersTest_{}", std::process::id());
        client
            .create_table()
            .table_name(&table)
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name("event_id")
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name("event_id")
                    .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
            .send()
            .await
            .expect("create_table");
        (
            DynamoStore::new(client.clone(), table.clone()),
            client,
            table,
        )
    }

    #[tokio::test]
    #[ignore = "requires DDB_LOCAL_ENDPOINT (DynamoDB-Local)"]
    async fn live_happy_path_fail_open_lands_atomically_and_advances_absent_anchor() {
        // Happy path: no prior anchor, so `STAMP_PREDICATE` passes via
        // `attribute_not_exists(last_action_epoch_ms)`. `fail_open_until`, the
        // audit fields, and the anchor must land atomically together — the same
        // single-`UpdateItem` shape the code has always had (no fallback).
        let endpoint = std::env::var("DDB_LOCAL_ENDPOINT")
            .expect("DDB_LOCAL_ENDPOINT must point at a running DynamoDB-Local");
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .endpoint_url(endpoint)
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "key", "secret", None, None, "test",
            ))
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(conf);
        let (store, client, table) = local_store_and_table(client).await;

        store
            .set_fail_open_until(
                "evt",
                5_000,
                crate::AdminAction::FailOpen,
                "opA@x",
                ts(1_000),
            )
            .await
            .expect("set_fail_open_until returns Ok on the happy path");

        let key = Key::Event { event_id: "evt" }.build();
        let item = client
            .get_item()
            .table_name(&table)
            .set_key(Some(key))
            .send()
            .await
            .expect("get_item")
            .item
            .expect("item exists");

        // One atomic write: functional, audit and anchor all present.
        assert_eq!(
            item.get("fail_open_until")
                .and_then(|v| v.as_n().ok())
                .map(std::string::String::as_str),
            Some("5000"),
            "happy-path functional write landed"
        );
        assert_eq!(
            item.get("last_action_epoch_ms")
                .and_then(|v| v.as_n().ok())
                .map(std::string::String::as_str),
            Some("1000"),
            "happy-path anchor stamped from the arrival time"
        );
        assert_eq!(
            item.get("last_action")
                .and_then(|v| v.as_s().ok())
                .map(std::string::String::as_str),
            Some("fail_open"),
            "happy-path audit label"
        );
        assert_eq!(
            item.get("last_action_by")
                .and_then(|v| v.as_s().ok())
                .map(std::string::String::as_str),
            Some("opA@x"),
            "happy-path audit actor"
        );

        let _ = client.delete_table().table_name(&table).send().await;
    }

    #[tokio::test]
    #[ignore = "requires DDB_LOCAL_ENDPOINT (DynamoDB-Local)"]
    async fn live_backend_error_does_not_take_the_fallback_arm() {
        // G12 / P12 step 5: a `Backend` error on the primary attempt must
        // bubble (arm 3), not fall through to the fallback. A request against
        // a non-existent table surfaces as a `Backend` error here, not a
        // `ConditionalCheckFailedException`, so `set_fail_open_until` must
        // return `Err(Backend(_))` without taking the fallback arm.
        let endpoint = std::env::var("DDB_LOCAL_ENDPOINT")
            .expect("DDB_LOCAL_ENDPOINT must point at a running DynamoDB-Local");
        let conf = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .endpoint_url(endpoint)
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "key", "secret", None, None, "test",
            ))
            .build();
        let client = aws_sdk_dynamodb::Client::from_conf(conf);
        // Point the store at a table that does not exist — the primary
        // `UpdateItem` fails with a `ResourceNotFoundException`, which
        // `send_guarded` maps to `StoreError::Backend`, NOT `Conflict`.
        let store = DynamoStore::new(client.clone(), "NoSuchTable_anchors_test".to_owned());

        let outcome = store
            .set_fail_open_until(
                "evt",
                5_000,
                crate::AdminAction::FailOpen,
                "opA@x",
                ts(1_000),
            )
            .await;

        assert!(
            matches!(outcome, Err(crate::StoreError::Backend(_))),
            "a Backend error must bubble, not be swallowed or fall to the fallback: {outcome:?}"
        );
    }

    #[test]
    fn combine_debounce_ands_the_predicate_onto_an_existing_guard() {
        // A prior guard (expected-rate or admission-control) is preserved and
        // the inclusive debounce predicate is AND-ed after it.
        assert_eq!(
            combine_debounce(Some("target_rate = :exp")).as_str(),
            "target_rate = :exp AND (attribute_not_exists(last_action_epoch_ms) \
             OR last_action_epoch_ms <= :cutoff)"
        );
        // `set_message` has no prior guard: the predicate stands alone.
        assert_eq!(combine_debounce(None).as_str(), DEBOUNCE_PREDICATE);
    }

    #[test]
    fn the_audit_timestamp_matches_the_epoch_written_beside_it() {
        // `last_action_at` and `last_action_epoch_ms` are one instant recorded
        // twice, so the rendered string has to be the same instant the epoch
        // is — both are derived from one `Timestamp` for exactly this reason.
        let now = Timestamp::from_millisecond(1_788_000_000_000).unwrap();
        assert_eq!(audit_timestamp(now), "2026-08-29T10:40:00Z");
        assert_eq!(now.as_millisecond(), 1_788_000_000_000);

        assert_eq!(
            audit_timestamp(Timestamp::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
    }

    #[test]
    fn an_epoch_beyond_an_instant_cannot_be_stamped() {
        // The stamp that used to lock the control plane out: an epoch-millis
        // no instant can hold has no `Timestamp` to travel as, so it cannot
        // reach the audit row or the debounce guard in the first place.
        assert!(Timestamp::from_millisecond(i64::MAX).is_err());
    }
}
