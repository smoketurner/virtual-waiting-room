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
