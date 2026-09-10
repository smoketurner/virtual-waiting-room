# Trust policy for the authorizer Lambda execution role. Independent of the
# Lambda artifact, so it plans clean before the Rust crate is built.

data "aws_iam_policy_document" "authorizer_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# Least-privilege runtime policy: read the signing key, record arrivals on the
# Counters item, and reserve single-use tokens on the Tokens table.
data "aws_iam_policy_document" "authorizer" {
  statement {
    sid       = "ReadSigningKey"
    effect    = "Allow"
    actions   = ["ssm:GetParameter"]
    resources = [var.signing_key_parameter_arn]
  }

  statement {
    sid       = "RecordArrivals"
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [var.counters_table_arn]
  }

  statement {
    sid       = "ReserveTokens"
    effect    = "Allow"
    actions   = ["dynamodb:PutItem"]
    resources = [var.tokens_table_arn]
  }
}
