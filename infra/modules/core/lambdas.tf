# modules/core - the seal and read Lambdas.
#
#   seal_event  fired once at the event start by an EventBridge schedule; reads
#               the shard counts and writes the seal (seed + offsets + count +
#               phase) in one conditional UpdateItem.
#   read        serves GET /v1/status and /v1/queue_num over API Gateway; the
#               route wiring lives in api.tf.
#
# Every function deploys its own build; there is no stub fallback.

# --- seal_event ---------------------------------------------------------------

resource "aws_iam_role" "seal_event" {
  name               = "${local.seal_event_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "seal_event" {
  name   = "${local.seal_event_name}-policy"
  role   = aws_iam_role.seal_event.id
  policy = data.aws_iam_policy_document.seal_event.json
}

resource "aws_lambda_function" "seal_event" {
  function_name = local.seal_event_name
  role          = aws_iam_role.seal_event.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 30
  memory_size   = 256

  filename         = local.lambda_zip["seal_event"]
  source_code_hash = local.lambda_hash["seal_event"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
    })
  }

  tags = var.tags
}

# --- read ---------------------------------------------------------------------

resource "aws_iam_role" "read" {
  name               = "${local.read_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "read" {
  name   = "${local.read_name}-policy"
  role   = aws_iam_role.read.id
  policy = data.aws_iam_policy_document.read.json
}

resource "aws_lambda_function" "read" {
  function_name = local.read_name
  role          = aws_iam_role.read.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["read"]
  source_code_hash = local.lambda_hash["read"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE  = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE  = aws_dynamodb_table.prequeue.name
      POSITIONS_TABLE = aws_dynamodb_table.positions.name
      EVENT_ID        = var.event_id
    })
  }

  tags = var.tags
}

# API Gateway invokes the read Lambda for the status / queue_num routes.
resource "aws_lambda_permission" "read_apigw" {
  statement_id  = "AllowAPIGatewayInvokeRead"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.read.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# --- admin (SigV4 control plane) ----------------------------------------------

resource "aws_iam_role" "admin" {
  name               = "${local.admin_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "admin" {
  name   = "${local.admin_name}-policy"
  role   = aws_iam_role.admin.id
  policy = data.aws_iam_policy_document.admin.json
}

resource "aws_lambda_function" "admin" {
  function_name = local.admin_name
  role          = aws_iam_role.admin.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["admin"]
  source_code_hash = local.lambda_hash["admin"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
      TOKENS_TABLE   = aws_dynamodb_table.tokens.name
      EVENT_ID       = var.event_id
      # API Gateway prefixes the path with the stage (e.g. /dev/admin); this
      # makes the Rust runtime strip it so the Axum routes match unprefixed.
      AWS_LAMBDA_HTTP_IGNORE_STAGE_IN_PATH = "true"
      # OIDC admin login (ADR-0016). The client secret is read from the SSM
      # SecureString named here; the rest are non-secret config.
      OIDC_ISSUER              = var.oidc_issuer
      OIDC_CLIENT_ID           = var.oidc_client_id
      OIDC_REDIRECT_URI        = var.oidc_redirect_uri
      OIDC_CLIENT_SECRET_PARAM = aws_ssm_parameter.oidc_client_secret.name
      # Comma-separated allowlist of operator emails permitted to log in. Empty
      # = deny all (the admin Lambda fails closed).
      OIDC_ALLOWED_EMAILS = var.oidc_allowed_emails
    })
  }

  tags = var.tags
}

# API Gateway invokes the admin Lambda for the SigV4 /admin, /metrics, and
# /update_session routes.
resource "aws_lambda_permission" "admin_apigw" {
  statement_id  = "AllowAPIGatewayInvokeAdmin"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.admin.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# --- seal schedule (EventBridge Scheduler) ------------------------------------
# One-time trigger at the event start. Disabled by default (no start time set);
# the operator sets seal_start_time and flips it on ahead of the event. The
# target payload names the event to seal.

resource "aws_scheduler_schedule" "seal" {
  count = var.seal_start_time == "" ? 0 : 1

  name = "${var.name_prefix}-seal"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "at(${var.seal_start_time})"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.seal_event.arn
    role_arn = aws_iam_role.seal_scheduler[0].arn
    input    = jsonencode({ event_id = var.event_id })
  }
}

resource "aws_iam_role" "seal_scheduler" {
  count              = var.seal_start_time == "" ? 0 : 1
  name               = "${var.name_prefix}-seal-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "seal_scheduler" {
  count = var.seal_start_time == "" ? 0 : 1
  name  = "${var.name_prefix}-seal-scheduler-policy"
  role  = aws_iam_role.seal_scheduler[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.seal_event.arn
    }]
  })
}

# --- controller ---------------------------------------------------------------
#
# The closed-loop outflow controller. It advances
# serving_counter to meter admission against the operator's target rate while
# compensating for no-shows, and expires positions past expires_at. Falls back
# from its own build.

resource "aws_iam_role" "controller" {
  name               = "${local.controller_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "controller" {
  name   = "${local.controller_name}-policy"
  role   = aws_iam_role.controller.id
  policy = data.aws_iam_policy_document.controller.json
}

resource "aws_lambda_function" "controller" {
  function_name = local.controller_name
  role          = aws_iam_role.controller.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  # One invoke runs six 10s passes, so the timeout must exceed a full minute of
  # cadence plus the per-pass DynamoDB work.
  timeout     = 90
  memory_size = 256

  filename         = local.lambda_zip["controller"]
  source_code_hash = local.lambda_hash["controller"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE  = aws_dynamodb_table.counters.name
      POSITIONS_TABLE = aws_dynamodb_table.positions.name
      EVENT_ID        = var.event_id
    })
  }

  tags = var.tags
}

# --- controller schedule (EventBridge Scheduler) ------------------------------
# The design cadence is 10s, but the Scheduler rate() minimum is 1 minute, so
# the schedule fires every minute and each invoke runs six 10s passes. Always
# created: a deployed controller nothing fires means the queue forms and never
# drains, and an idle pass is one GetItem that returns early unless the event is
# active and admitting.

resource "aws_scheduler_schedule" "controller" {
  name = "${var.name_prefix}-controller"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "rate(1 minute)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.controller.arn
    role_arn = aws_iam_role.controller_scheduler.arn
    input    = jsonencode({ event_id = var.event_id })
  }
}

resource "aws_iam_role" "controller_scheduler" {
  name               = "${var.name_prefix}-controller-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "controller_scheduler" {
  name = "${var.name_prefix}-controller-scheduler-policy"
  role = aws_iam_role.controller_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.controller.arn
    }]
  })
}
