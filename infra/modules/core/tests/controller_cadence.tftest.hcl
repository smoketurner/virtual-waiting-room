# Asserts the controller cannot pile up (issue #236) and that the pieces an
# event depends on are wired: a late or failed fire is dropped rather than
# queued behind the next one, the functions that must run during a burst hold
# a reservation, and a new signing key or event id reaches what depends on it.
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

# The schedule's input embeds the function ARN, which is computed.
override_resource {
  target = aws_lambda_function.controller
  values = {
    arn = "arn:aws:lambda:us-east-1:123456789012:function:test-controller"
  }
  override_during = plan
}

# The redrive policy embeds the dead-letter queue's ARN.
override_resource {
  target = aws_sqs_queue.join_dlq
  values = {
    arn = "arn:aws:sqs:us-east-1:123456789012:test-join-dlq"
  }
  override_during = plan
}

run "a_late_controller_fire_is_dropped_not_queued" {
  command = plan

  assert {
    condition = (
      aws_scheduler_schedule.controller.target[0].retry_policy[0].maximum_retry_attempts == 0
      && aws_scheduler_schedule.controller.target[0].retry_policy[0].maximum_event_age_in_seconds == 60
    )
    error_message = "the controller schedule must not retry: a fire delivered late starts a second execution beside the next minute's"
  }

  # The schedule invokes asynchronously, so Lambda's own queue would otherwise
  # hold and retry what the schedule had already given up on.
  assert {
    condition = (
      jsondecode(aws_scheduler_schedule.controller.target[0].input).InvocationType == "Event"
      && aws_lambda_function_event_invoke_config.controller.maximum_retry_attempts == 0
      && aws_lambda_function_event_invoke_config.controller.maximum_event_age_in_seconds == 60
    )
    error_message = "the controller's async invoke config must drop a late or failed invoke instead of retrying it for up to six hours"
  }

  assert {
    condition     = endswith(jsondecode(aws_scheduler_schedule.controller.target[0].input).FunctionName, ":${aws_lambda_function_event_invoke_config.controller.qualifier}")
    error_message = "the invoke config must be on the qualifier the schedule invokes, or it configures nothing"
  }

  assert {
    condition     = aws_lambda_function.controller.durable_config[0].execution_timeout <= 60
    error_message = "an execution must not outlive the schedule's one-minute period"
  }
}

run "the_functions_an_event_cannot_run_without_hold_a_reservation" {
  command = plan

  assert {
    condition     = aws_lambda_function.controller.reserved_concurrent_executions == 2
    error_message = "the controller needs reserved concurrency, or a burst on the visitor path can throttle admission"
  }

  assert {
    condition     = aws_lambda_function.open_event.reserved_concurrent_executions == 1
    error_message = "open_event needs reserved concurrency, or the invoke that opens the event can be throttled at T-0"
  }
}

run "a_new_signing_key_republishes_every_function_that_reads_it" {
  command = plan

  assert {
    condition = alltrue([
      for f in [
        aws_lambda_function.assign_position,
        aws_lambda_function.open_event,
        aws_lambda_function.read,
        aws_lambda_function.admin,
        aws_lambda_function.controller,
        aws_lambda_function.generate_token,
        aws_lambda_function.nojs,
      ] :
      contains(keys(f.environment[0].variables), "SIGNING_KEY_PARAMETER") == contains(keys(f.environment[0].variables), "SIGNING_KEY_FINGERPRINT")
    ])
    error_message = "a function that reads the signing key must carry its fingerprint, or a regenerated key leaves warm environments minting sessions the gate refuses (issue #247)"
  }

  assert {
    condition     = contains(keys(aws_lambda_function.generate_token.environment[0].variables), "SIGNING_KEY_FINGERPRINT") && contains(keys(aws_lambda_function.nojs.environment[0].variables), "SIGNING_KEY_FINGERPRINT")
    error_message = "generate_token and nojs both mint sessions and both read the key"
  }
}

run "a_new_event_id_is_a_new_event" {
  command = plan

  # replace_triggered_by is not visible to a plan-time assertion, so this pins
  # the value the trigger hangs off; the wiring itself is in main.tf, on the
  # event item and the PreQueue and Positions tables.
  assert {
    condition     = terraform_data.event_id.input == var.event_id
    error_message = "the replacement trigger must track event_id (issue #248)"
  }
}

run "tables_are_unprotected_unless_asked" {
  command = plan

  assert {
    condition = alltrue([
      for t in [aws_dynamodb_table.counters, aws_dynamodb_table.prequeue, aws_dynamodb_table.positions, aws_dynamodb_table.tokens] :
      t.deletion_protection_enabled == false
    ])
    error_message = "deletion protection is opt-in: a dev stack must be destroyable"
  }
}

run "deletion_protection_covers_all_four_tables" {
  command = plan

  variables {
    table_deletion_protection = true
  }

  assert {
    condition = alltrue([
      for t in [aws_dynamodb_table.counters, aws_dynamodb_table.prequeue, aws_dynamodb_table.positions, aws_dynamodb_table.tokens] :
      t.deletion_protection_enabled == true
    ])
    error_message = "table_deletion_protection must reach every table"
  }
}

run "a_join_is_retried_ten_times_before_it_is_given_up_on" {
  command = plan

  assert {
    condition     = jsondecode(aws_sqs_queue.join.redrive_policy).maxReceiveCount == 10
    error_message = "the join queue must redrive after ten receives (issue #251)"
  }
}

run "one_path_cannot_spend_the_account_throttle" {
  command = plan

  assert {
    condition     = aws_api_gateway_method_settings.default.method_path == "*/*" && aws_api_gateway_method_settings.default.settings[0].throttling_rate_limit == 10000 && aws_api_gateway_method_settings.default.settings[0].throttling_burst_limit == 5000
    error_message = "every method needs a throttle of its own below the account bucket (issue #245)"
  }

  assert {
    condition     = aws_api_gateway_method_settings.join.settings[0].throttling_rate_limit == 10000 && aws_api_gateway_method_settings.join.settings[0].throttling_burst_limit == 5000
    error_message = "the join path keeps the rate the ingest is designed for"
  }
}
