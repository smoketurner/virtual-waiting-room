# modules/authorizer - origin authorizer Lambda plus the optional CloudFront
# VPC origin (DESIGN §2.3, §11, §12).
#
# The authorizer validates the request at the protected origin: session cookie
# -> forward; admission token -> set session cookie, strip token, count the
# arrival (ADD arrivals#(hash%10)), forward; unprotected path -> forward;
# waiting room unreachable -> forward with a time-limited bypass cookie
# (fail open, ADR-0009); otherwise 302 to the waiting room.
#
# The Lambda resources are added once the Rust crate is built and
# var.lambda_artifact_path points at a bootstrap zip (PLAN Phase 1f/2). The
# execution role's trust policy (data.aws_iam_policy_document.authorizer_assume_role)
# is defined now because it has no artifact dependency.
#
# Planned resources:
#   aws_iam_role                 (1) - execution role (trust policy ready in data.tf)
#   aws_iam_role_policy          (1) - read signing key (ssm:GetParameter), ADD arrivals on Counters
#   aws_lambda_function          (1) - provided.al2023, arm64
#   aws_cloudfront_vpc_origin    (1) - only when enable_vpc = true
#
# Fail fast on a misconfigured VPC seam.
resource "terraform_data" "vpc_config_guard" {
  count = local.vpc_config_valid ? 0 : 1

  lifecycle {
    precondition {
      condition     = local.vpc_config_valid
      error_message = "enable_vpc = true requires at least one entry in vpc_origin_subnet_ids."
    }
  }
}
