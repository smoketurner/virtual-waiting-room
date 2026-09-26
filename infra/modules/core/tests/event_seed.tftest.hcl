# The seeded event item must agree with the seeded open schedule: the open only
# runs from pre_queue, so an event whose schedule Terraform arms has to start
# there, or the scheduled open is refused as wrong-phase (#213).
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

run "an_unscheduled_event_is_seeded_idle" {
  command = plan

  assert {
    condition     = jsondecode(aws_dynamodb_table_item.event.item).phase.S == "idle"
    error_message = "with no start time nothing is scheduled, so the event starts idle"
  }

  assert {
    condition     = aws_scheduler_schedule.open.state == "DISABLED"
    error_message = "with no start time the open schedule is disabled"
  }
}

run "a_scheduled_event_is_seeded_where_the_open_can_run" {
  command = plan

  variables {
    starts_at = "2099-03-14T10:00:00"
  }

  assert {
    condition     = aws_scheduler_schedule.open.state == "ENABLED"
    error_message = "a start time arms the open schedule"
  }

  assert {
    condition     = jsondecode(aws_dynamodb_table_item.event.item).phase.S == "pre_queue"
    error_message = "an armed schedule on an idle event is refused as wrong-phase at T-0; the event must start in pre_queue"
  }
}
