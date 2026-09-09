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
