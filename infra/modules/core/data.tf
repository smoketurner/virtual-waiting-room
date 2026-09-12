# Ambient context looked up from the provider. Used to build ARNs and to scope
# IAM policies to this account/region without hardcoding either.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

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

  statement {
    sid    = "ReadAndClaimCounters"
    effect = "Allow"
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:GetItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]
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

# Execution-role permissions for seal_event: read the shard counts and write the
# seal (seed + offsets + count + phase) on the Counters item, plus logs.
data "aws_iam_policy_document" "seal_event" {
  statement {
    sid    = "ReadAndSealCounters"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      # The pre-queue shards are separate items, so the seal gathers them in one
      # BatchGetItem. GetItem does not authorise it — it is its own action.
      "dynamodb:BatchGetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]
  }

  statement {
    sid    = "Logs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:log-group:/aws/lambda/${local.seal_event_name}*"]
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
  statement {
    sid    = "ReadAndWriteCounters"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.counters.arn]
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
  # which rewrites the one-time seal schedule. Read as well as write, because
  # UpdateSchedule replaces the whole schedule rather than patching it, so the
  # writer has to fetch the current definition to resend it intact.
  #
  # No CreateSchedule or DeleteSchedule: Terraform owns whether the schedule
  # exists, the admin owns only when it fires. Clearing a start time therefore
  # cannot delete it even if the code asked to.
  statement {
    sid    = "ReadAndWriteSealSchedule"
    effect = "Allow"
    actions = [
      "scheduler:GetSchedule",
      "scheduler:UpdateSchedule",
    ]
    resources = [aws_scheduler_schedule.seal.arn]
  }

  # UpdateSchedule resends the target's RoleArn, so the caller must be allowed
  # to pass it. Scoped to that one role and to Scheduler as the only service it
  # may be passed to, rather than the role/* the AWS example uses. What the
  # grant is worth to an attacker is bounded by the role itself: its whole
  # policy is a single lambda:InvokeFunction on seal_event.
  statement {
    sid       = "PassSealSchedulerRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.seal_scheduler.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["scheduler.amazonaws.com"]
    }
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

# Execution-role permissions for the controller: read and advance the Counters
# item (serving_counter, max_expired_position, smoothing state), scan Positions
# for expiry and mark them expired, plus logs.
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
  }

  statement {
    sid    = "ExpirePositions"
    effect = "Allow"
    actions = [
      "dynamodb:Scan",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.positions.arn]
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

# Trust policy for the EventBridge Scheduler role that invokes the seal Lambda.
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

# generate_token: read the counters and the visitor's position, count the
# arrival, and read the signing key. It writes only the arrivals counter, so an
# UpdateItem on Counters is the whole write surface.
data "aws_iam_policy_document" "generate_token" {
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

  statement {
    sid       = "RecordArrival"
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.counters.arn]
  }

  statement {
    sid       = "ReadSigningKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.signing_key.arn]
  }

  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:${local.aws_partition}:logs:${local.aws_region}:${local.aws_account_id}:*"]
  }
}
