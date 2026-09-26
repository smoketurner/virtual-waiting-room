# Asserts the admin role holds the read-only grants its readiness panel
# (issue #70) calls, and that the admin Lambda is told what to read.
#
# A missing grant does not fail the apply or the dashboard: the panel renders
# the row as "could not evaluate: AccessDenied", which reads like a transient
# AWS error rather than a deployment bug. So the grants are pinned here, by
# action and resource, and every one of them must be a read.
#
# The ARNs the policy embeds are provider-computed, so they are overridden to
# known values for the plan. What is asserted is the policy document's
# statement blocks -- the input the JSON is rendered from.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
  # The rendered JSON is mocked (the mock provider would otherwise fill it with
  # a random string the roles reject); the statement blocks it is rendered
  # from are configuration, which the mock leaves intact.
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }
}
mock_provider "archive" {}
mock_provider "random" {}

override_resource {
  target = aws_dynamodb_table.counters
  values = {
    arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-Counters"
  }
  override_during = plan
}

override_resource {
  target = aws_dynamodb_table.prequeue
  values = {
    arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-PreQueue"
  }
  override_during = plan
}

override_resource {
  target = aws_dynamodb_table.positions
  values = {
    arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-Positions"
  }
  override_during = plan
}

override_resource {
  target = aws_dynamodb_table.tokens
  values = {
    arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-Tokens"
  }
  override_during = plan
}

override_resource {
  target = aws_lambda_function.assign_position
  values = {
    arn = "arn:aws:lambda:us-east-1:123456789012:function:test-assign-position"
  }
  override_during = plan
}

override_resource {
  target = aws_scheduler_schedule.controller
  values = {
    arn = "arn:aws:scheduler:us-east-1:123456789012:schedule/default/test-controller"
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

  warm_throughput_write_units = 10000
  warm_throughput_read_units  = 12000

  oidc_client_id      = "test-client"
  oidc_redirect_uri   = "https://example.invalid/admin/callback"
  oidc_allowed_emails = "operator@example.invalid"
}

run "the_admin_role_holds_every_readiness_grant" {
  command = plan

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["dynamodb:DescribeTable"])
      && toset(s.resources) == toset([
        aws_dynamodb_table.counters.arn,
        aws_dynamodb_table.prequeue.arn,
        aws_dynamodb_table.positions.arn,
        aws_dynamodb_table.tokens.arn,
      ])
    ])
    error_message = "the admin must be able to DescribeTable on all four tables, and only those, to read their applied warm throughput (O1)"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["dynamodb:DescribeLimits"])
      && toset(s.resources) == toset(["*"])
    ])
    error_message = "the admin must be able to DescribeLimits (O2); the action has no resource type, so its resource is *"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["apigateway:GET"])
      && toset(s.resources) == toset(["arn:aws:apigateway:us-east-1::/account"])
    ])
    error_message = "the admin must be able to GET the API Gateway account (throttle settings, O2), scoped to /account"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["lambda:GetFunctionConcurrency"])
      && toset(s.resources) == toset([aws_lambda_function.assign_position.arn])
    ])
    error_message = "the admin must be able to read assign_position's reserved concurrency (N9), on that function only"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && contains(s.actions, "scheduler:GetSchedule")
      && contains(s.resources, aws_scheduler_schedule.controller.arn)
    ])
    error_message = "the admin must be able to read the controller schedule"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["ssm:GetParameter"])
      && toset(s.resources) == toset(["arn:aws:ssm:us-east-1:123456789012:parameter/test/edge/readiness"])
    ])
    error_message = "the admin must be able to read the edge readiness parameter, and only that one, for the CloudFront rows"
  }

  assert {
    condition = anytrue([
      for s in data.aws_iam_policy_document.admin.statement :
      s.effect == "Allow"
      && toset(s.actions) == toset(["cloudfront:GetDistributionConfig", "cloudfront:GetCachePolicy"])
      && toset(s.resources) == toset([
        "arn:aws:cloudfront::123456789012:distribution/*",
        "arn:aws:cloudfront::123456789012:cache-policy/*",
      ])
    ])
    error_message = "the admin must be able to read distribution and cache policy config (C4, N7), scoped to this account"
  }

  # Diagnostics only: nothing the panel added may write. Every readiness
  # statement is a Describe, a Get, or API Gateway's GET.
  assert {
    condition = alltrue(flatten([
      for s in data.aws_iam_policy_document.admin.statement : [
        for a in s.actions :
        can(regex("^([a-z-]+:(Describe|Get)[A-Za-z]*|apigateway:GET)$", a))
      ] if startswith(coalesce(s.sid, ""), "Readiness")
    ]))
    error_message = "every Readiness* grant must be read-only"
  }
}

run "the_admin_is_told_what_to_check" {
  command = plan

  assert {
    condition = (
      aws_lambda_function.admin.environment[0].variables["ASSIGN_POSITION_FUNCTION_NAME"]
      == aws_lambda_function.assign_position.function_name
    )
    error_message = "the concurrency row must read the function that consumes the join queue"
  }

  assert {
    condition = (
      aws_lambda_function.admin.environment[0].variables["CONTROLLER_SCHEDULE_NAME"]
      == aws_scheduler_schedule.controller.name
    )
    error_message = "the controller row must read the schedule that fires the controller"
  }

  assert {
    condition = (
      aws_lambda_function.admin.environment[0].variables["WARM_THROUGHPUT_WRITE_UNITS"] == "10000"
      && aws_lambda_function.admin.environment[0].variables["WARM_THROUGHPUT_READ_UNITS"] == "12000"
    )
    error_message = "the warm-throughput row must compare against the units the tables were configured with"
  }

  assert {
    condition = (
      aws_lambda_function.admin.environment[0].variables["EDGE_READINESS_PARAM"]
      == output.edge_readiness_parameter_name
    )
    error_message = "the admin must read the parameter edge is told to write"
  }
}
