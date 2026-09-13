# Asserts that a failed SQS SendMessage cannot reach the client as a successful
# join (issue #144). The join integration is compute-free — API Gateway calls
# SendMessage itself — so the integration response mapping is the only place
# that distinguishes "enqueued" from "dropped". When it maps both to 200, a
# visitor who holds no position is told they hold one, and the first symptom is
# the event starting with an empty queue.
#
# For an AWS-service integration the selection pattern is matched against the
# backend's HTTP status code, so these assertions exercise the configured
# pattern against the status codes SQS actually returns rather than comparing it
# to a literal.
#
# mock_provider avoids needing AWS credentials in CI: nothing here reads live
# state, it only inspects the plan this module would produce. The artifact paths
# point at a fixture because the module hashes each Lambda zip at plan time; its
# contents are never read.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  # A mocked data source returns a generated string for every attribute, and
  # the IAM roles validate that this one parses as a policy.
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
  assign_position_artifact_path = "tests/fixtures/bootstrap.zip"
  seal_event_artifact_path      = "tests/fixtures/bootstrap.zip"
  read_artifact_path            = "tests/fixtures/bootstrap.zip"
  admin_artifact_path           = "tests/fixtures/bootstrap.zip"
  controller_artifact_path      = "tests/fixtures/bootstrap.zip"
  generate_token_artifact_path  = "tests/fixtures/bootstrap.zip"
}

run "success_is_claimed_only_by_a_status_code_sqs_returns_on_success" {
  command = plan

  # API Gateway matches the pattern against the whole status code, which is
  # why each candidate is anchored here.
  assert {
    condition = can(
      regex("^${aws_api_gateway_integration_response.join_200.selection_pattern}$", "200")
    )
    error_message = "the join 200 integration response must still claim a successful SendMessage"
  }

  assert {
    condition = alltrue([
      for code in ["400", "403", "500", "503"] : !can(
        regex("^${aws_api_gateway_integration_response.join_200.selection_pattern}$", code)
      )
    ])
    error_message = "the join 200 integration response must not claim an SQS error; with an empty or permissive selection pattern a SendMessage failure is reported to the visitor as a successful join"
  }
}

run "every_other_outcome_falls_through_to_a_5xx" {
  command = plan

  # An empty selection pattern is what makes an integration response the
  # default. Exactly one response may be the default, and it has to be this
  # one: enumerating the error codes instead would leave any status code nobody
  # anticipated with no response at all.
  assert {
    condition = contains(
      [null, ""], aws_api_gateway_integration_response.join_502.selection_pattern
    )
    error_message = "the join error integration response must be the default (no selection pattern), so no SQS outcome is left unmapped"
  }

  assert {
    condition     = can(regex("^5\\d{2}$", aws_api_gateway_integration_response.join_502.status_code))
    error_message = "a SendMessage failure must surface as a 5xx: the visitor did nothing wrong and the client treats only a non-2xx as a failed attempt"
  }

  # Without a method response for the status code the integration response maps
  # to, API Gateway discards the mapping and returns its own 500.
  assert {
    condition     = aws_api_gateway_method_response.join_502.status_code == aws_api_gateway_integration_response.join_502.status_code
    error_message = "the join error status code needs a method response, or the mapping is dead configuration"
  }
}

run "the_error_body_does_not_pass_the_sqs_response_through" {
  command = plan

  # SQS's error carries the queue name, the AWS error code and a request id.
  assert {
    condition     = !strcontains(aws_api_gateway_integration_response.join_502.response_templates["application/json"], "$input")
    error_message = "the join error response must emit a fixed body rather than pass the SQS error through to an anonymous visitor"
  }
}
