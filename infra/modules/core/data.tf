# Ambient context looked up from the provider. Used to build ARNs and to scope
# IAM policies to this account/region without hardcoding either.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

# Partition-key values on the Counters table, for scoping each function to the
# items it touches (issue #250). The table holds the event's own item and its
# striped shard counters, and a grant on the whole table lets the function that
# counts arrivals also rewrite serving_counter. These must stay equal to what
# wr_common::expr::Key::value builds: a value that matches nothing denies every
# call the function makes. event_id cannot contain '#', so a shard pattern
# reaches no other event's items.
locals {
  counters_event_key           = "EVT#${var.event_id}"
  counters_prequeue_shard_keys = "EVT#${var.event_id}#PQ#*"
  counters_arrivals_shard_keys = "EVT#${var.event_id}#AR#*"
}

# Trust policy for the assign_position Lambda execution role.
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# Execution-role permissions: consume from the join queue, write positions and
# increment counters, and emit logs. Scoped to this deployment's resources.
data "aws_iam_policy_document" "assign_position" {
  statement {
    sid    = "ConsumeJoinQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.join.arn]
  }

  statement {
    sid    = "WritePositions"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
    ]
    resources = [aws_dynamodb_table.positions.arn]
  }

  # Reads the event item, claims a block on it, and bumps a pre-queue shard.
  # Never an arrivals shard.
  statement {
    sid    = "ReadAndClaimCounters"
    effect = "Allow"
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = [local.counters_event_key, local.counters_prequeue_shard_keys]
    }
  }

  statement {
    sid    = "ReadAndWritePrequeue"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      # Reads which request ids already hold a row, so a reload does not burn a
      # fresh pre-queue index (issue #59).
      "dynamodb:BatchGetItem",
    ]
    resources = [aws_dynamodb_table.prequeue.arn]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.assign_position_name}*"]
  }
}

# Trust policy for the API Gateway role that writes directly to SQS (no Lambda
# in the ingest path).
data "aws_iam_policy_document" "apigw_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["apigateway.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "apigw_sqs" {
  statement {
    sid       = "SendToJoinQueue"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.join.arn]
  }
}

# Execution-role permissions for open_event: read the shard counts and write the
# open (seed + offsets + count + phase) on the Counters item, plus logs.
data "aws_iam_policy_document" "open_event" {
  statement {
    sid    = "ReadAndOpenCounters"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      # The pre-queue shards are separate items, so the open gathers them in one
      # BatchGetItem. GetItem does not authorise it — it is its own action.
      "dynamodb:BatchGetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = [local.counters_event_key, local.counters_prequeue_shard_keys]
    }
  }

  # The open waits for the join queue to drain before it counts the cohort
  # (issue #239). Depth only: it never receives or deletes a join.
  statement {
    sid       = "ReadJoinQueueDepth"
    effect    = "Allow"
    actions   = ["sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.join.arn]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.open_event_name}*"]
  }
}

# Execution-role permissions for read: GetItem on Counters and PreQueue, plus
# logs. The public read path never writes.
data "aws_iam_policy_document" "read" {
  statement {
    sid    = "ReadCountersAndPreQueue"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
    ]
    resources = [
      aws_dynamodb_table.counters.arn,
      aws_dynamodb_table.prequeue.arn,
      aws_dynamodb_table.positions.arn,
    ]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.read_name}*"]
  }
}

# Execution-role permissions for the admin control plane: read and conditionally
# update the Counters item (phase / rate / message), plus logs. It never touches
# PreQueue or Positions.
data "aws_iam_policy_document" "admin" {
  # The event item only: every operator action is a write to it, and none
  # touches a shard.
  statement {
    sid    = "ReadAndWriteCounters"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = [local.counters_event_key]
    }
  }

  statement {
    sid    = "AdminSessions"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]
    resources = [aws_dynamodb_table.tokens.arn]
  }

  statement {
    sid       = "ReadOidcClientSecret"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.oidc_client_secret.arn]
  }

  # Issue #71: mirrors fail_open_until to the edge gate's KeyValueStore.
  # Data-plane actions on the store's own ARN, distinct from the
  # cloudfront:* control-plane permissions (which this role does not hold —
  # it never creates or deletes the store, only reads and writes its keys).
  #
  # Reaches key 'k' (the signing secret) as well as 'c'. It cannot be narrowed:
  # the store is the only resource type the service defines and it publishes no
  # condition keys.
  statement {
    sid    = "WriteEdgeGateConfig"
    effect = "Allow"
    actions = [
      "cloudfront-keyvaluestore:DescribeKeyValueStore",
      "cloudfront-keyvaluestore:GetKey",
      "cloudfront-keyvaluestore:PutKey",
    ]
    resources = [aws_cloudfront_key_value_store.gate.arn]
  }

  # Issue #128: the operator sets the event's start time from the dashboard,
  # which rewrites the one-time open schedule. Read as well as write, because
  # UpdateSchedule replaces the whole schedule rather than patching it, so the
  # writer has to fetch the current definition to resend it intact.
  #
  # No CreateSchedule or DeleteSchedule: Terraform owns whether the schedule
  # exists, the admin owns only when it fires. Clearing a start time therefore
  # cannot delete it even if the code asked to.
  statement {
    sid    = "ReadAndWriteOpenSchedule"
    effect = "Allow"
    actions = [
      "scheduler:GetSchedule",
      "scheduler:UpdateSchedule",
    ]
    resources = [aws_scheduler_schedule.open.arn]
  }

  # UpdateSchedule resends the target's RoleArn, so the caller must be allowed
  # to pass it. Scoped to that one role and to Scheduler as the only service it
  # may be passed to, rather than the role/* the AWS example uses. What the
  # grant is worth to an attacker is bounded by the role itself: its whole
  # policy is a single lambda:InvokeFunction on open_event.
  statement {
    sid       = "PassOpenSchedulerRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.open_scheduler.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["scheduler.amazonaws.com"]
    }
  }

  # --- Readiness panel (issue #70) -------------------------------------------
  # Read-only. Every statement below is a Describe/Get the dashboard's
  # readiness panel makes; none of them can change what it reads.

  statement {
    sid       = "ReadinessDescribeTables"
    effect    = "Allow"
    actions   = ["dynamodb:DescribeTable"]
    resources = local.readiness_table_arns
  }

  # DescribeLimits is an account-level action with no resource type, so it
  # cannot be scoped below "*".
  statement {
    sid       = "ReadinessDynamoDbLimits"
    effect    = "Allow"
    actions   = ["dynamodb:DescribeLimits"]
    resources = ["*"]
  }

  # API Gateway's GetAccount is authorised as a GET on the /account resource.
  statement {
    sid       = "ReadinessApiGatewayAccount"
    effect    = "Allow"
    actions   = ["apigateway:GET"]
    resources = ["arn:${local.aws_partition}:apigateway:${local.aws_region}::/account"]
  }

  statement {
    sid       = "ReadinessReservedConcurrency"
    effect    = "Allow"
    actions   = ["lambda:GetFunctionConcurrency"]
    resources = [aws_lambda_function.assign_position.arn]
  }

  # The open schedule's GetSchedule is already granted above.
  statement {
    sid       = "ReadinessControllerSchedule"
    effect    = "Allow"
    actions   = ["scheduler:GetSchedule"]
    resources = [aws_scheduler_schedule.controller.arn]
  }

  statement {
    sid       = "ReadinessEdgeParameter"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:${local.aws_partition}:ssm:${local.aws_region}:${local.aws_account_id}:parameter${local.edge_readiness_parameter_name}"]
  }

  # The distribution and its cache policies are created by the edge module,
  # which depends on this one, so their ARNs cannot be named here. The edge
  # module attaches that grant to this role itself, scoped to its own
  # distribution (issue #250, edge/main.tf).

  # "Open now" starts the event without waiting for the schedule, by invoking
  # the function the schedule invokes. The same grant the scheduler's own role
  # holds, on the same one function.
  statement {
    sid       = "OpenTheEventNow"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.open_event.arn]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.admin_name}*"]
  }
}

# Execution-role permissions for the controller: read and advance the event's
# Counters item (serving_counter, smoothing state), sum and settle the arrivals
# shards, plus logs. It holds nothing on Positions or the pre-queue shards.
data "aws_iam_policy_document" "controller" {
  statement {
    sid    = "AdvanceCounters"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      # Summing the arrivals shards is a BatchGetItem over their own items.
      "dynamodb:BatchGetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = [local.counters_event_key, local.counters_arrivals_shard_keys]
    }
  }

  statement {
    sid    = "DurableExecution"
    effect = "Allow"
    actions = [
      "lambda:CheckpointDurableExecution",
      "lambda:GetDurableExecutionState",
    ]
    # A durable execution is a sub-resource of a function VERSION, so its ARN
    # always carries a qualifier. An unqualified function ARN never matches one
    # and the checkpoint is denied.
    resources = ["arn:${local.aws_partition}:lambda:${local.aws_region}:${local.aws_account_id}:function:${local.controller_name}:*"]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.controller_name}*"]
  }
}

# Trust policy for the EventBridge Scheduler role that invokes the open Lambda.
data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

# What admitting a visitor needs: read the counters and the visitor's position,
# claim the admission, count the arrival, and read the signing key. Shared by
# generate_token and nojs (issue #67), which admits through the same code. Each
# adds its own log grant, so neither can write the other's log.
data "aws_iam_policy_document" "admission" {
  statement {
    sid     = "ReadQueueState"
    effect  = "Allow"
    actions = ["dynamodb:GetItem"]
    resources = [
      aws_dynamodb_table.counters.arn,
      aws_dynamodb_table.prequeue.arn,
      aws_dynamodb_table.positions.arn,
    ]
  }

  # An arrivals shard and nothing else on Counters. This is the function an
  # anonymous visitor reaches, and with the table-wide grant it could have
  # advanced serving_counter (issue #250).
  statement {
    sid       = "RecordArrival"
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.counters.arn]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = [local.counters_arrivals_shard_keys]
    }
  }

  # Issue #62: one conditional write per visitor marks their admission claimed,
  # so a reloaded page does not count a second arrival against one release. It
  # creates the row for a pre-queue member, who has none until then, which is
  # why this is UpdateItem on Positions rather than a narrower grant.
  statement {
    sid       = "ClaimAdmission"
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.positions.arn]
  }

  statement {
    sid       = "ReadSigningKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.signing_key.arn]
  }
}

# No CreateLogGroup on either: Terraform owns the groups (logging.tf) and each
# function depends on its own existing.
data "aws_iam_policy_document" "generate_token" {
  source_policy_documents = [data.aws_iam_policy_document.admission.json]

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda[local.generate_token_name].arn}:*"]
  }
}

# nojs adds SendMessage on the join queue its form post joins through.
data "aws_iam_policy_document" "nojs" {
  source_policy_documents = [data.aws_iam_policy_document.admission.json]

  statement {
    sid       = "Join"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.join.arn]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda[local.nojs_name].arn}:*"]
  }
}
