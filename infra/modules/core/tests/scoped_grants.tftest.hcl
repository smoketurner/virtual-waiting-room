# Asserts each function can write only the Counters items it actually writes
# (issue #250). The table holds the event's own item beside its shard counters,
# so a table-wide UpdateItem lets the function an anonymous visitor reaches
# advance serving_counter. The key strings are pinned here as literals because
# they must equal what wr_common::expr::Key::value builds: a pattern that
# matches nothing denies every call, and the plan would still be green.
#
# What is asserted is the policy document's statement blocks -- the input the
# JSON is rendered from -- since the mock provider does not render it.
#
# Run with: terraform -chdir=infra/modules/core test

mock_provider "aws" {
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
  oidc_redirect_uri   = "https://example.invalid/_wr/admin/callback"
  oidc_allowed_emails = "operator@example.invalid"
}

# The ARNs the statements embed are provider-computed.
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
  target = aws_cloudwatch_log_group.lambda
  values = {
    arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/test"
  }
  override_during = plan
}

override_resource {
  target = aws_sqs_queue.join
  values = {
    arn = "arn:aws:sqs:us-east-1:123456789012:test-join"
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-join"
  }
  override_during = plan
}

run "every_counters_write_is_scoped_by_partition_key" {
  command = plan

  # No statement in any function's policy may update Counters without naming
  # the keys it may touch.
  assert {
    condition = alltrue(flatten([
      for doc in [
        data.aws_iam_policy_document.assign_position,
        data.aws_iam_policy_document.open_event,
        data.aws_iam_policy_document.admin,
        data.aws_iam_policy_document.controller,
        data.aws_iam_policy_document.admission,
        ] : [
        for s in doc.statement :
        anytrue([for c in coalesce(s.condition, []) : c.variable == "dynamodb:LeadingKeys" && c.test == "ForAllValues:StringLike"])
        if contains(s.actions, "dynamodb:UpdateItem") && contains(s.resources, aws_dynamodb_table.counters.arn)
      ]
    ]))
    error_message = "every UpdateItem grant on Counters must carry a dynamodb:LeadingKeys condition (issue #250)"
  }
}

run "the_admission_path_writes_only_arrivals_shards" {
  command = plan

  assert {
    condition = toset(one(one([
      for s in data.aws_iam_policy_document.admission.statement : s if s.sid == "RecordArrival"
    ]).condition).values) == toset(["EVT#an-event-id#AR#*"])
    error_message = "generate_token and nojs may update the arrivals shards and nothing else on Counters, least of all the event item holding serving_counter"
  }
}

run "each_writer_reaches_only_its_own_items" {
  command = plan

  assert {
    condition = toset(one(one([
      for s in data.aws_iam_policy_document.assign_position.statement : s if s.sid == "ReadAndClaimCounters"
    ]).condition).values) == toset(["EVT#an-event-id", "EVT#an-event-id#PQ#*"])
    error_message = "assign_position touches the event item and the pre-queue shards"
  }

  assert {
    condition = toset(one(one([
      for s in data.aws_iam_policy_document.open_event.statement : s if s.sid == "ReadAndOpenCounters"
    ]).condition).values) == toset(["EVT#an-event-id", "EVT#an-event-id#PQ#*"])
    error_message = "open_event reads the pre-queue shards and writes the event item"
  }

  assert {
    condition = toset(one(one([
      for s in data.aws_iam_policy_document.controller.statement : s if s.sid == "AdvanceCounters"
    ]).condition).values) == toset(["EVT#an-event-id", "EVT#an-event-id#AR#*", "EVT#an-event-id#LOG"])
    error_message = "the controller touches the event item, the arrivals shards and its admission log"
  }

  assert {
    condition = toset(one(one([
      for s in data.aws_iam_policy_document.admin.statement : s if s.sid == "ReadAndWriteCounters"
    ]).condition).values) == toset(["EVT#an-event-id"])
    error_message = "the admin touches the event item only"
  }
}

run "the_admission_functions_log_only_to_their_own_groups" {
  command = plan

  assert {
    condition = alltrue([
      for s in concat(data.aws_iam_policy_document.generate_token.statement, data.aws_iam_policy_document.nojs.statement) :
      alltrue([for r in s.resources : strcontains(r, ":log-group:/aws/lambda/") && !endswith(r, ":${data.aws_caller_identity.current.account_id}:*")])
      if anytrue([for a in s.actions : startswith(a, "logs:")])
    ])
    error_message = "a logs grant on the whole account lets the function write any log group in it (issue #250)"
  }
}

# The open waits for the join queue to drain (issue #239), so it has to be told
# which queue and allowed to read its depth -- and nothing more.
run "the_open_can_read_the_join_queue_depth_and_only_that" {
  command = plan

  assert {
    condition     = aws_lambda_function.open_event.environment[0].variables["JOIN_QUEUE_URL"] == aws_sqs_queue.join.url
    error_message = "open_event must be pointed at the join queue it waits on"
  }

  assert {
    condition = [
      for s in data.aws_iam_policy_document.open_event.statement : s.actions
      if contains(s.resources, aws_sqs_queue.join.arn)
    ] == [toset(["sqs:GetQueueAttributes"])]
    error_message = "open_event needs sqs:GetQueueAttributes on the join queue, and no other SQS action: it reads the depth, it never consumes a join"
  }

  assert {
    condition     = aws_lambda_function.open_event.timeout == 30
    error_message = "the open's timeout must leave room for the drain wait"
  }
}
