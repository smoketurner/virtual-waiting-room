# Ambient context looked up from the provider. Used to build ARNs and to scope
# IAM policies to this account/region without hardcoding either.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

# Placeholder Lambda artifact. When var.lambda_artifact_path is empty, Terraform
# zips the vendored placeholder bootstrap so the compute plane can be created
# before the Rust crate is built. Replaced by pointing the variable at the real
# build artifact.
# Placeholder Lambda artifact. Always built: the token-minting and admin API
# endpoints in api.tf are still fronted by it until those crates land. Each real
# function points at its own archive below instead.
data "archive_file" "placeholder" {
  count       = 1
  type        = "zip"
  source_file = "${path.module}/placeholder-lambda/bootstrap"
  output_path = "${path.module}/placeholder-lambda/placeholder.zip"
}

# Real function artifacts, zipped from the built Rust bootstrap. Each exists only
# when its path variable is set; otherwise the function falls back to the
# placeholder (see locals).
data "archive_file" "assign_position" {
  count       = var.assign_position_artifact_path == "" ? 0 : 1
  type        = "zip"
  source_file = var.assign_position_artifact_path
  output_path = "${path.module}/.artifacts/assign_position.zip"
}

data "archive_file" "seal_event" {
  count       = var.seal_event_artifact_path == "" ? 0 : 1
  type        = "zip"
  source_file = var.seal_event_artifact_path
  output_path = "${path.module}/.artifacts/seal_event.zip"
}

data "archive_file" "read" {
  count       = var.read_artifact_path == "" ? 0 : 1
  type        = "zip"
  source_file = var.read_artifact_path
  output_path = "${path.module}/.artifacts/read.zip"
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
