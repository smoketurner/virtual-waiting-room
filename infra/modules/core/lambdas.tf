# modules/core - the seal and read Lambdas.
#
#   seal_event  fired once at the event start by an EventBridge schedule; reads
#               the shard counts and writes the seal (seed + offsets + count +
#               phase) in one conditional UpdateItem.
#   read        serves GET /v1/status and /v1/queue_num over API Gateway; the
#               route wiring lives in api.tf.
#
# Both fall back to the shared placeholder until their artifact path is set, so
# the plane can be created before the crates are built.

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

  filename         = local.seal_event_zip
  source_code_hash = local.seal_event_hash

  environment {
    variables = {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
    }
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

  filename         = local.read_zip
  source_code_hash = local.read_hash

  environment {
    variables = {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE = aws_dynamodb_table.prequeue.name
      EVENT_ID       = var.event_id
    }
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
