# modules/core - generate_token, the Lambda that mints admission session
# cookies (issue #71). The gate itself is a CloudFront Function (modules/edge);
# generate_token checks the queue state, records the arrival, and signs an
# HMAC session cookie with the per-deployment signing key. That key and its
# CloudFront-Function-readable mirror live in main.tf / edge_gate.tf.

resource "aws_iam_role" "generate_token" {
  name               = "${local.generate_token_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "generate_token" {
  name   = "${local.generate_token_name}-policy"
  role   = aws_iam_role.generate_token.id
  policy = data.aws_iam_policy_document.generate_token.json
}

resource "aws_lambda_function" "generate_token" {
  function_name = local.generate_token_name
  role          = aws_iam_role.generate_token.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["generate_token"]
  source_code_hash = local.lambda_hash["generate_token"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE        = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE        = aws_dynamodb_table.prequeue.name
      POSITIONS_TABLE       = aws_dynamodb_table.positions.name
      EVENT_ID              = var.event_id
      SIGNING_KEY_PARAMETER = aws_ssm_parameter.signing_key.name
      SESSION_COOKIE_NAME   = var.session_cookie_name
      SESSION_TTL_SECS      = tostring(var.session_ttl_seconds)
    })
  }

  tags = var.tags
}

resource "aws_lambda_permission" "generate_token_apigw" {
  statement_id  = "AllowAPIGatewayInvokeGenerateToken"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.generate_token.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}
