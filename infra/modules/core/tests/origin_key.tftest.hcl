# Asserts every REST API method requires the origin key (ADR-0038).
#
# The key is what makes the distribution the API's only client. One method
# left keyless is a door around the edge: its web ACL, its challenge and rate
# limits, and the CloudFront plan's cost ceiling all stop applying to anything
# that posts to execute-api directly. Nothing fails when that happens — the
# method answers as it always did — so the plan is checked here instead.
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

run "every_method_requires_the_origin_key" {
  command = plan

  assert {
    condition     = aws_api_gateway_method.join_post.api_key_required
    error_message = "POST /v1/join must require the origin key, or bots post straight to SQS past the edge"
  }

  assert {
    condition     = alltrue([for m in aws_api_gateway_method.endpoint : m.api_key_required])
    error_message = "every public and admin endpoint must require the origin key"
  }

  assert {
    condition     = aws_api_gateway_method.admin_proxy.api_key_required && aws_api_gateway_method.static_get.api_key_required
    error_message = "the admin proxy and its static assets must require the origin key"
  }
}

run "the_key_is_bound_to_the_stage" {
  command = plan

  # A key authorises nothing on its own: without a usage plan naming the stage,
  # API Gateway refuses CloudFront's requests too and the whole room goes dark.
  assert {
    condition     = one(aws_api_gateway_usage_plan.origin.api_stages).stage == var.env
    error_message = "the origin key's usage plan must cover the deployed stage"
  }

  assert {
    condition     = aws_api_gateway_usage_plan_key.origin.key_type == "API_KEY"
    error_message = "the origin key must be attached to its usage plan"
  }
}
