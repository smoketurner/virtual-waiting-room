# modules/core - nojs, the queue for a visitor without JavaScript (issue #67).
#
# The waiting page is a script. For a browser that runs none, its <noscript>
# form posts to /v1/enter, which joins through the same SQS queue /v1/join
# writes to, and /v1/wait renders the visitor's place and admits them through
# generate_token's own admission path (the crate is a library dependency, so
# there is one way a session is minted). This is the only join that runs
# compute, so the function's concurrency is reserved and small: a flood of
# form posts throttles here, not in generate_token or the burst path.

resource "aws_iam_role" "nojs" {
  name               = "${local.nojs_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "nojs" {
  name   = "${local.nojs_name}-policy"
  role   = aws_iam_role.nojs.id
  policy = data.aws_iam_policy_document.nojs.json
}

resource "aws_lambda_function" "nojs" {
  function_name = local.nojs_name
  role          = aws_iam_role.nojs.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  reserved_concurrent_executions = var.nojs_reserved_concurrency

  filename         = local.lambda_zip["nojs"]
  source_code_hash = local.lambda_hash["nojs"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE        = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE        = aws_dynamodb_table.prequeue.name
      POSITIONS_TABLE       = aws_dynamodb_table.positions.name
      EVENT_ID              = var.event_id
      SIGNING_KEY_PARAMETER = aws_ssm_parameter.signing_key.name
      SESSION_COOKIE_NAME   = var.session_cookie_name
      SESSION_TTL_SECS      = tostring(var.session_ttl_seconds)
      JOIN_QUEUE_URL        = aws_sqs_queue.join.url
    })
  }

  tags = var.tags

  depends_on = [aws_cloudwatch_log_group.lambda]
}

resource "aws_lambda_permission" "nojs_apigw" {
  statement_id  = "AllowAPIGatewayInvokeNojs"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.nojs.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}
