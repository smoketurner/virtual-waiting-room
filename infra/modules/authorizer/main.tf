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

# --- execution role -----------------------------------------------------------

resource "aws_iam_role" "authorizer" {
  name               = "${var.name_prefix}-authorizer-role"
  assume_role_policy = data.aws_iam_policy_document.authorizer_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "authorizer_logs" {
  role       = aws_iam_role.authorizer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "authorizer" {
  name   = "${var.name_prefix}-authorizer-policy"
  role   = aws_iam_role.authorizer.id
  policy = data.aws_iam_policy_document.authorizer.json
}

# --- authorizer Lambda --------------------------------------------------------
#
# provided.al2023 on arm64. Reads the signing key from SSM at cold start and
# decides every origin request locally: ssm:GetParameter on the key,
# ADD arrivals#<shard> on Counters when a token becomes a session, and a
# single-use reservation on Tokens. Created only once an artifact is supplied.

resource "aws_lambda_function" "authorizer" {
  count = var.lambda_artifact_path == "" ? 0 : 1

  function_name = "${var.name_prefix}-authorizer"
  role          = aws_iam_role.authorizer.arn
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  handler       = "bootstrap"
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_size

  filename         = var.lambda_artifact_path
  source_code_hash = filebase64sha256(var.lambda_artifact_path)

  environment {
    variables = {
      COUNTERS_TABLE          = var.counters_table_name
      TOKENS_TABLE            = var.tokens_table_name
      EVENT_ID                = var.event_id
      SIGNING_KEY_PARAMETER   = var.signing_key_parameter_name
      WAITING_ROOM_URL        = var.waiting_room_url
      PROTECTED_PATH_PREFIXES = join(",", var.protected_path_prefixes)
    }
  }

  tags = var.tags
}

# --- CloudFront VPC origin (opt-in) -------------------------------------------
#
# Only when enable_vpc = true: places the client origin in a private subnet with
# CloudFront as the sole ingress. VPC origins forbid Lambda@Edge origin triggers
# and are unavailable in GovCloud (DESIGN §12).

resource "aws_cloudfront_vpc_origin" "this" {
  count = var.enable_vpc ? 1 : 0

  vpc_origin_endpoint_config {
    name                   = "${var.name_prefix}-vpc-origin"
    arn                    = var.origin_arn
    http_port              = 80
    https_port             = 443
    origin_protocol_policy = "https-only"

    origin_ssl_protocols {
      items    = ["TLSv1.2"]
      quantity = 1
    }
  }

  tags = var.tags
}
