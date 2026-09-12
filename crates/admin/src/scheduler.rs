//! The `aws-sdk-scheduler`-backed [`SealSchedule`] implementation (issue
//! #128): arms or disables the one-time schedule that fires `seal_event` at
//! the operator's start time.
//!
//! `UpdateSchedule` **replaces** a schedule rather than patching it — every
//! field left unset reverts to its service default — so a write here is
//! get-then-resend, in the same shape as `edge`'s describe-then-put. Getting
//! that wrong does not fail loudly: it silently drops the target's `input`,
//! which carries the event id the seal needs, and its retry policy.
//!
//! Terraform owns the schedule's existence, target and role; only the
//! expression and the state are ever changed here.

use aws_sdk_scheduler::Client;
use aws_sdk_scheduler::operation::get_schedule::GetScheduleOutput;
use aws_sdk_scheduler::operation::update_schedule::builders::UpdateScheduleInputBuilder;
use aws_sdk_scheduler::types::ScheduleState;

use crate::{ScheduleError, SealSchedule};

/// A live `EventBridge` Scheduler-backed [`SealSchedule`] bound to one
/// schedule.
pub struct SchedulerStore {
    client: Client,
    schedule_name: String,
}

impl SchedulerStore {
    #[must_use]
    pub fn new(client: Client, schedule_name: String) -> Self {
        Self {
            client,
            schedule_name,
        }
    }
}

/// Rebuilds the whole schedule from what `GetSchedule` returned, changing only
/// the expression and the state.
///
/// Every other field is carried across verbatim. The target moves as one whole
/// [`aws_sdk_scheduler::types::Target`] rather than being reassembled field by
/// field, so there is no way to add a field to the target and forget to copy it
/// here.
///
/// Disabling reuses the schedule's current expression, because the field is
/// required even when the state is `DISABLED`. The one exception is an
/// expression that has already fired: a one-time `at()` in the past is not
/// necessarily still accepted on update, so disabling falls back to the
/// far-future placeholder Terraform created the schedule with.
fn rewrite(
    name: &str,
    current: &GetScheduleOutput,
    at: Option<(&str, &str)>,
) -> Result<UpdateScheduleInputBuilder, ScheduleError> {
    let target = current
        .target()
        .ok_or_else(|| ScheduleError("schedule has no target".to_owned()))?
        .clone();
    let window = current
        .flexible_time_window()
        .ok_or_else(|| ScheduleError("schedule has no flexible time window".to_owned()))?
        .clone();

    // The timezone travels with the expression rather than being folded into
    // it, so a schedule set months out still fires at the local hour the
    // operator chose once a daylight-saving change has moved the offset.
    let (expression, timezone, state) = match at {
        Some((at, tz)) => (format!("at({at})"), tz.to_owned(), ScheduleState::Enabled),
        None => (
            current
                .schedule_expression()
                .unwrap_or(DISABLED_PLACEHOLDER)
                .to_owned(),
            current
                .schedule_expression_timezone()
                .unwrap_or("UTC")
                .to_owned(),
            ScheduleState::Disabled,
        ),
    };

    Ok(UpdateScheduleInputBuilder::default()
        .name(name)
        .set_group_name(current.group_name().map(str::to_owned))
        .flexible_time_window(window)
        .schedule_expression(expression)
        .schedule_expression_timezone(timezone)
        .state(state)
        .target(target)
        .set_description(current.description().map(str::to_owned))
        .set_kms_key_arn(current.kms_key_arn().map(str::to_owned))
        .set_start_date(current.start_date().copied())
        .set_end_date(current.end_date().copied())
        .set_action_after_completion(current.action_after_completion().cloned()))
}

/// The expression Terraform creates the schedule with. Far enough out that it
/// cannot fire even if something enabled it, and used again as the fallback
/// when disabling a schedule whose own expression is no longer usable.
const DISABLED_PLACEHOLDER: &str = "at(2099-12-31T23:59:59)";

impl SealSchedule for SchedulerStore {
    async fn set_start_time(&self, at: Option<(&str, &str)>) -> Result<(), ScheduleError> {
        let current = self
            .client
            .get_schedule()
            .name(&self.schedule_name)
            .send()
            .await
            .map_err(|e| ScheduleError(format!("get_schedule: {e}")))?;

        let input = rewrite(&self.schedule_name, &current, at)?;

        input
            .send_with(&self.client)
            .await
            .map(|_| ())
            .map_err(|e| ScheduleError(format!("update_schedule: {e}")))
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use aws_sdk_scheduler::types::{
        DeadLetterConfig, FlexibleTimeWindow, FlexibleTimeWindowMode, RetryPolicy, Target,
    };

    use super::*;

    const NAME: &str = "wr-dev-seal";
    const INPUT: &str = r#"{"event_id":"evt-1"}"#;

    /// A schedule shaped like the one Terraform creates: a target carrying the
    /// event id and a deliberately non-default retry policy, plus the optional
    /// fields `UpdateSchedule` would revert to service defaults if dropped.
    fn deployed_schedule() -> GetScheduleOutput {
        GetScheduleOutput::builder()
            .name(NAME)
            .group_name("default")
            .schedule_expression("at(2099-12-31T23:59:59)")
            .schedule_expression_timezone("UTC")
            .state(ScheduleState::Disabled)
            .description("seal the event at T-0")
            .flexible_time_window(
                FlexibleTimeWindow::builder()
                    .mode(FlexibleTimeWindowMode::Off)
                    .build()
                    .unwrap(),
            )
            .target(
                Target::builder()
                    .arn("arn:aws:lambda:us-east-1:111122223333:function:wr-dev-seal-event")
                    .role_arn("arn:aws:iam::111122223333:role/wr-dev-seal-scheduler-role")
                    .input(INPUT)
                    .retry_policy(
                        RetryPolicy::builder()
                            .maximum_event_age_in_seconds(600)
                            .maximum_retry_attempts(10)
                            .build(),
                    )
                    .dead_letter_config(
                        DeadLetterConfig::builder()
                            .arn("arn:aws:sqs:us-east-1:111122223333:wr-dev-dlq")
                            .build(),
                    )
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[test]
    fn arming_preserves_the_target_input_and_retry_policy() {
        // The acceptance criterion for issue #128. UpdateSchedule replaces the
        // schedule, so a writer that sent only the expression would blank the
        // event id the seal reads and revert the retry policy to the service
        // default — and because that default equals the provider's, the
        // regression would be invisible without asserting it here.
        let current = deployed_schedule();
        let updated = rewrite(
            NAME,
            &current,
            Some(("2026-09-10T18:00:00", "America/New_York")),
        )
        .unwrap()
        .build()
        .unwrap();

        let target = updated.target().unwrap();
        assert_eq!(target.input(), Some(INPUT));
        let retry = target.retry_policy().unwrap();
        assert_eq!(retry.maximum_event_age_in_seconds(), Some(600));
        assert_eq!(retry.maximum_retry_attempts(), Some(10));
        assert_eq!(
            target.dead_letter_config().unwrap().arn(),
            Some("arn:aws:sqs:us-east-1:111122223333:wr-dev-dlq")
        );
        assert_eq!(target.arn(), current.target().unwrap().arn());
        assert_eq!(target.role_arn(), current.target().unwrap().role_arn());
    }

    #[test]
    fn arming_changes_only_the_expression_and_state() {
        let current = deployed_schedule();
        let updated = rewrite(
            NAME,
            &current,
            Some(("2026-09-10T18:00:00", "America/New_York")),
        )
        .unwrap()
        .build()
        .unwrap();

        assert_eq!(
            updated.schedule_expression(),
            Some("at(2026-09-10T18:00:00)")
        );
        assert_eq!(updated.state(), Some(&ScheduleState::Enabled));
        // Everything else is what it was.
        assert_eq!(updated.group_name(), current.group_name());
        assert_eq!(updated.description(), current.description());
        // The zone is the operator's, so it moves with the expression rather
        // than being carried over from the placeholder Terraform created.
        assert_eq!(
            updated.schedule_expression_timezone(),
            Some("America/New_York")
        );
        assert_eq!(
            updated.flexible_time_window().unwrap().mode(),
            current.flexible_time_window().unwrap().mode()
        );
    }

    #[test]
    fn clearing_disables_and_keeps_the_target_intact() {
        // "Clearing disables rather than deletes" — there is no delete path to
        // take, and the target survives so a later re-arm needs no repair.
        let current = deployed_schedule();
        let updated = rewrite(NAME, &current, None).unwrap().build().unwrap();

        assert_eq!(updated.state(), Some(&ScheduleState::Disabled));
        assert_eq!(updated.target().unwrap().input(), Some(INPUT));
        assert!(updated.target().unwrap().retry_policy().is_some());
    }

    #[test]
    fn disabling_reuses_the_current_expression() {
        let mut current = deployed_schedule();
        current.schedule_expression = Some("at(2026-09-10T18:00:00)".to_owned());
        let updated = rewrite(NAME, &current, None).unwrap().build().unwrap();
        assert_eq!(
            updated.schedule_expression(),
            Some("at(2026-09-10T18:00:00)")
        );
    }

    #[test]
    fn disabling_a_schedule_without_an_expression_falls_back_to_the_placeholder() {
        // schedule_expression is required on update, so there must always be
        // something to send even if the read came back without one.
        let mut current = deployed_schedule();
        current.schedule_expression = None;
        let updated = rewrite(NAME, &current, None).unwrap().build().unwrap();
        assert_eq!(updated.schedule_expression(), Some(DISABLED_PLACEHOLDER));
    }

    #[test]
    fn a_schedule_missing_its_target_is_an_error_not_a_blanking_update() {
        // Sending an update built from an incomplete read would erase the
        // target rather than fail, so this must not be recoverable.
        let mut current = deployed_schedule();
        current.target = None;
        assert!(
            rewrite(
                NAME,
                &current,
                Some(("2026-09-10T18:00:00", "America/New_York"))
            )
            .is_err()
        );

        let mut current = deployed_schedule();
        current.flexible_time_window = None;
        assert!(
            rewrite(
                NAME,
                &current,
                Some(("2026-09-10T18:00:00", "America/New_York"))
            )
            .is_err()
        );
    }
}
