# Asserts the signals exist and key on what the Rust actually emits.
#
# A metric filter whose pattern does not match is indistinguishable from a
# healthy system: the metric publishes its default of zero forever and the alarm
# sits green. That is the same class of silent failure this whole file exists to
# catch, so the pattern is asserted against the literal event names the crates
# log, not against itself.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}
mock_provider "archive" {}
mock_provider "random" {}

variables {
  name_prefix                   = "test"
  env                           = "test"
  event_id                      = "an-event-id"
  assign_position_artifact_path = "tests/fixtures/bootstrap.zip"
  open_event_artifact_path      = "tests/fixtures/bootstrap.zip"
  read_artifact_path            = "tests/fixtures/bootstrap.zip"
  admin_artifact_path           = "tests/fixtures/bootstrap.zip"
  controller_artifact_path      = "tests/fixtures/bootstrap.zip"
  generate_token_artifact_path  = "tests/fixtures/bootstrap.zip"

  oidc_client_id      = "test-client"
  oidc_redirect_uri   = "https://example.invalid/admin/callback"
  oidc_allowed_emails = "operator@example.invalid"
}

run "every_lambda_has_a_log_group_that_expires" {
  command = plan

  # Lambda auto-creates a group that never expires for any function Terraform
  # does not own, so a function missing here grows without bound and silently
  # bills for it.
  assert {
    condition     = length(aws_cloudwatch_log_group.lambda) == 6
    error_message = "every Lambda needs a Terraform-owned log group; six functions ship"
  }

  assert {
    condition = alltrue([
      for g in aws_cloudwatch_log_group.lambda : g.retention_in_days > 0
    ])
    error_message = "a log group with no retention keeps every byte forever, which is a standing cost between events (N1)"
  }
}

run "filters_key_on_the_event_names_the_crates_emit" {
  command = plan

  # These strings are `event = "..."` literals in the Rust. If a crate renames
  # one without renaming it here, the filter matches nothing and the alarm stays
  # green through exactly the failure it exists to catch.
  assert {
    condition = alltrue([
      for name in [
        "join_dropped",
        "arrival_record_failed",
        "arrival_shard_draw_failed",
        "rules_audit_failed",
        "admission_control_unreadable",
        "admission_claim_failed",
        "fail_open_audit_lost",
        ] : strcontains(
        aws_cloudwatch_log_metric_filter.event[name].pattern,
        "$.event = \"${name}\""
      )
    ])
    error_message = "each filter must key on the stable event name its crate logs"
  }
}

run "the_open_event_error_filter_keys_on_the_top_level_level_field" {
  command = plan

  # The open_event Lambda's tracing-subscriber JSON formatter (no
  # flatten_event) nests user fields like `event` under a `fields` object but
  # serializes `level` at the root as uppercase "ERROR" (tracing-serde). So a
  # filter keying on `$.event` would match nothing; this one deliberately keys
  # on `$.level = "ERROR"` to actually match the open_event log group's output
  # and alarm on the wrong-phase rejection (and any future open_event error).
  # The pattern is pinned here so a regression that re-keyed it on the nested
  # `event` field would fail loudly rather than silently disarm the alarm.
  assert {
    condition = strcontains(
      aws_cloudwatch_log_metric_filter.event["open_event_error"].pattern,
      "$.level = \"ERROR\""
    )
    error_message = "the open_event error filter must key on the top-level `level` field (uppercase ERROR); keying on the nested `$.event` would never match the formatter's output"
  }

  # The filter must watch the open_event Lambda's own log group, not one of the
  # others; the for_each over local.log_metrics plan-fails on an unknown log_group
  # key, but this also pins the resolved name against a wrong-key regression.
  assert {
    condition = (
      aws_cloudwatch_log_metric_filter.event["open_event_error"].log_group_name
      == "/aws/lambda/${aws_lambda_function.open_event.function_name}"
    )
    error_message = "the open_event error filter must watch the open_event Lambda's log group"
  }
}

run "a_quiet_system_publishes_zero_rather_than_nothing" {
  command = plan

  # Without a default, the metric has no datapoints while everything is
  # working, the alarm sits in INSUFFICIENT_DATA, and an alarm never seen green
  # is not believed when it goes red.
  assert {
    condition = alltrue([
      for f in aws_cloudwatch_log_metric_filter.event :
      f.metric_transformation[0].default_value == "0"
    ])
    error_message = "every filter needs default_value = 0 so a healthy system publishes zeroes"
  }
}

run "every_alarm_fires_on_a_single_occurrence" {
  command = plan

  # None of these are rates to tune. Each is an event that should never happen,
  # so one is the signal.
  assert {
    condition = alltrue([
      for a in aws_cloudwatch_metric_alarm.event :
      a.threshold == 0 && a.comparison_operator == "GreaterThanThreshold"
    ])
    error_message = "these alarms fire on one occurrence; a threshold above zero would be tuning away the signal"
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.join_dlq_not_empty.threshold == 0
    error_message = "one message on the dead-letter queue is one join accepted from a visitor and then lost"
  }

  assert {
    condition     = aws_cloudwatch_metric_alarm.open_dlq_not_empty.threshold == 0
    error_message = "one message on the open dead-letter queue is one scheduled open retried to exhaustion and lost"
  }
}
