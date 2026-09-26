# The no-JavaScript queue (issue #67) is the one join that runs compute, so its
# concurrency is capped, and it must join through the same queue /v1/join
# writes to, or assign_position never sees those visitors.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}
# The queue URL and the function's invoke ARN are provider-computed, so the
# mock leaves them unknown at plan; pin them so the wiring can be compared.
override_resource {
  target = aws_sqs_queue.join
  values = {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-join"
  }
  override_during = plan
}

override_resource {
  target = aws_lambda_function.nojs
  values = {
    invoke_arn = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:test-nojs/invocations"
  }
  override_during = plan
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

run "the_no_javascript_path_is_capped_and_joins_the_real_queue" {
  command = plan

  assert {
    condition     = aws_lambda_function.nojs.reserved_concurrent_executions == 5
    error_message = "the nojs function must carry its own small reserved concurrency"
  }

  assert {
    condition     = aws_lambda_function.nojs.environment[0].variables.JOIN_QUEUE_URL == aws_sqs_queue.join.url
    error_message = "a no-JavaScript join must go on the queue assign_position reads"
  }

  assert {
    condition     = aws_api_gateway_integration.endpoint["enter"].uri == aws_lambda_function.nojs.invoke_arn && aws_api_gateway_integration.endpoint["wait"].uri == aws_lambda_function.nojs.invoke_arn
    error_message = "/v1/enter and /v1/wait must be served by the nojs function"
  }

  assert {
    condition     = aws_api_gateway_method.endpoint["enter"].http_method == "POST" && aws_api_gateway_method.endpoint["wait"].http_method == "GET"
    error_message = "enter is the form post, wait is the page"
  }
}
