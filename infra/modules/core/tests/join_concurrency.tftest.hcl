# The join queue's poller must not outscale assign_position's reserved
# concurrency: throttled batches return to the queue with their receive count
# raised, and at maxReceiveCount they dead-letter joins that were accepted.
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
  nojs_artifact_path            = "tests/fixtures/bootstrap.zip"

  oidc_client_id      = "test-client"
  oidc_redirect_uri   = "https://example.invalid/admin/callback"
  oidc_allowed_emails = "operator@example.invalid"
}

run "the_join_poller_is_capped_at_the_reserved_concurrency" {
  command = plan

  assert {
    condition     = aws_lambda_event_source_mapping.join.scaling_config[0].maximum_concurrency == aws_lambda_function.assign_position.reserved_concurrent_executions
    error_message = "the join poller must scale no further than assign_position's reserved concurrency"
  }
}

run "an_unreserved_function_leaves_the_poller_uncapped" {
  command = plan

  variables {
    assign_position_reserved_concurrency = -1
  }

  assert {
    condition     = length(aws_lambda_event_source_mapping.join.scaling_config) == 0
    error_message = "with no reservation there is nothing to be throttled against, and -1 is not a valid cap"
  }
}
