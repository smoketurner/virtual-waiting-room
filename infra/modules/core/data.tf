# Ambient context looked up from the provider. Used to build ARNs and to scope
# IAM policies to this account/region without hardcoding either.

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}

# Placeholder Lambda artifact. When var.lambda_artifact_path is empty, Terraform
# zips the vendored placeholder bootstrap so the compute plane can be created
# before the Rust crate is built. Replaced by pointing the variable at the real
# build artifact.
data "archive_file" "placeholder" {
  count       = var.lambda_artifact_path == "" ? 1 : 0
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
