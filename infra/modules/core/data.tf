# Ambient context looked up from the provider. Used to build ARNs and to scope
# IAM policies to this account/region without hardcoding either.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

# Placeholder Lambda artifact. Always built: the token-minting and admin API
# endpoints in api.tf are still fronted by it until those crates land, and each
# real function falls back to it when its artifact path is empty. The vendored
# source is a raw bootstrap binary, so Terraform zips it here; the real function
# artifacts are already zips produced by `cargo lambda build --output-format
# zip`, referenced directly (see locals), so they need no archive_file.
data "archive_file" "placeholder" {
  count       = 1
  type        = "zip"
  source_file = "${path.module}/placeholder-lambda/bootstrap"
  output_path = "${path.module}/placeholder-lambda/placeholder.zip"
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
    sid    = "WritePositionsAndCounters"
    effect = "Allow"
    actions = [
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
    ]
    resources = [
      aws_dynamodb_table.positions.arn,
      aws_dynamodb_table.counters.arn,
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
    resources = ["arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.assign_position_name}*"]
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
    resources = ["arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.seal_event_name}*"]
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
    resources = ["arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.read_name}*"]
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
