# Asserts every string the join schema accepts is bounded. The join path runs
# no compute, so nothing downstream throttles on the size of what it accepts:
# an unbounded field is billed transfer the operator cannot refuse, and below
# the SQS 256 KB limit it also buys a queue message and a dead-letter record.
#
# mock_provider and the fixture artifacts are there for the same reason as in
# join_failure_mapping.tftest.hcl: this runs in CI without credentials.
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
  seal_event_artifact_path      = "tests/fixtures/bootstrap.zip"
  read_artifact_path            = "tests/fixtures/bootstrap.zip"
  admin_artifact_path           = "tests/fixtures/bootstrap.zip"
  controller_artifact_path      = "tests/fixtures/bootstrap.zip"
  generate_token_artifact_path  = "tests/fixtures/bootstrap.zip"
}

run "no_join_field_is_unbounded" {
  command = plan

  assert {
    condition = alltrue([
      for name, spec in jsondecode(aws_api_gateway_model.join.schema).properties :
      can(spec.maxLength)
    ])
    error_message = "every property of the join schema needs a maxLength; an unbounded one is transfer an anonymous client can spend on the operator's behalf, on a path that runs no compute and so throttles on nothing"
  }
}

run "event_id_is_bounded_by_the_deployment_it_must_match" {
  command = plan

  # A legitimate join carries exactly this deployment's event id: the client
  # reads it from /status and assign_position discards anything else. So the
  # tightest bound that cannot reject a real join is its own length.
  assert {
    condition     = jsondecode(aws_api_gateway_model.join.schema).properties.event_id.maxLength == length(var.event_id)
    error_message = "event_id must be capped at the length of this deployment's own event id, which is the only value a legitimate join can carry"
  }
}
