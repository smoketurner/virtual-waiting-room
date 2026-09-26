# Asserts the admin Lambda is told which function opens the event.
#
# `OPEN_EVENT_FUNCTION_NAME` is read at Init and its absence is a hard error, so
# a missing or misspelt value is a control plane that 502s on every request --
# on an apply that reported success. The name is asserted against the function
# resource rather than against a string built from name_prefix, which would pass
# while pointing at a function that does not exist.
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

# The DLQ ARN and the open_event Lambda ARN are both provider-computed, so the
# mock provider leaves them (and every value that references them — the
# schedule's dead_letter_config and the scheduler role policy's embedded
# Resource) unknown at plan. The role policy embeds both ARNs (the Lambda's
# for lambda:InvokeFunction, the DLQ's for sqs:SendMessage), so both must be
# known for the policy string to decode. Override them to known, valid-shaped
# ARNs for the plan so the wiring assertions below resolve without an apply
# (apply under the mock provider fails earlier on the Lambda role ARN, which
# the mock also leaves non-ARN-shaped).
override_resource {
  target = aws_sqs_queue.open_dlq
  values = {
    arn = "arn:aws:sqs:us-east-1:123456789012:test-open-dlq"
  }
  override_during = plan
}

override_resource {
  target = aws_lambda_function.open_event
  values = {
    arn = "arn:aws:lambda:us-east-1:123456789012:function:test-open-event"
  }
  override_during = plan
}

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

run "the_admin_is_pointed_at_the_function_the_schedule_invokes" {
  command = plan

  assert {
    condition = (
      aws_lambda_function.admin.environment[0].variables["OPEN_EVENT_FUNCTION_NAME"]
      == aws_lambda_function.open_event.function_name
    )
    error_message = "the admin's Open now must invoke the same function the open schedule targets, or it opens nothing"
  }

  # Both fires must name the same event, or they are two opens rather than one
  # racing a single conditional write. The admin sends its own EVENT_ID; the
  # schedule sends the one baked into its target input.
  assert {
    condition = (
      jsondecode(aws_scheduler_schedule.open.target[0].input).event_id
      == aws_lambda_function.admin.environment[0].variables["EVENT_ID"]
    )
    error_message = "the schedule and the admin must open the same event"
  }
}

# A wrong-phase rejection now returns Err from the open handler, so EventBridge
# retries up to maximum_retry_attempts and then delivers the failed invocation
# to the schedule's dead-letter queue. Without the DLQ the retry exhaust path is
# invisible: the schedule records a final failed fire and nothing retains the
# payload, so the event stays unopened with no inspectable artefact (the exact
# silent failure the open_event disambiguation exists to surface). The DLQ ARN
# is overridden above so these wiring assertions resolve at plan.
run "the_open_schedule_dead_letters_failed_invocations_to_the_open_dlq" {
  command = plan

  assert {
    condition = (
      aws_scheduler_schedule.open.target[0].dead_letter_config[0].arn
      == aws_sqs_queue.open_dlq.arn
    )
    error_message = "the open schedule must dead-letter a failed invocation to the open DLQ, or a wrong-phase rejection's retries silently drop instead of surfacing"
  }

  # EventBridge delivers to the DLQ assuming the schedule's execution role, so
  # that role needs sqs:SendMessage on the DLQ ARN. A regression that dropped
  # the grant would let the schedule fail to deliver to the DLQ with an
  # AccessDenied that nothing alarms on (the DLQ stays empty, so its alarm
  # stays green through the silent delivery failure).
  assert {
    condition = anytrue([
      for s in jsondecode(aws_iam_role_policy.open_scheduler.policy).Statement :
      s.Effect == "Allow"
      && try(contains(s.Action, "sqs:SendMessage"), s.Action == "sqs:SendMessage")
      && s.Resource == aws_sqs_queue.open_dlq.arn
    ])
    error_message = "the open scheduler role must be allowed sqs:SendMessage on the open DLQ ARN, or EventBridge cannot deliver a failed invocation to it"
  }
}
