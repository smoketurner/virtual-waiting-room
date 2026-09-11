output "table_names" {
  description = "Map of logical table key to its DynamoDB table name."
  value = {
    counters  = aws_dynamodb_table.counters.name
    prequeue  = aws_dynamodb_table.prequeue.name
    positions = aws_dynamodb_table.positions.name
    tokens    = aws_dynamodb_table.tokens.name
  }
}

output "table_arns" {
  description = "Map of logical table key to its DynamoDB table ARN."
  value = {
    counters  = aws_dynamodb_table.counters.arn
    prequeue  = aws_dynamodb_table.prequeue.arn
    positions = aws_dynamodb_table.positions.arn
    tokens    = aws_dynamodb_table.tokens.arn
  }
}

output "signing_key_parameter_name" {
  description = "Name of the SSM SecureString parameter holding the signing key. The authorizer, generate_token, and admin functions read it by name (ssm:GetParameter)."
  value       = aws_ssm_parameter.signing_key.name
}

output "signing_key_parameter_arn" {
  description = "ARN of the SSM signing-key parameter, for scoping ssm:GetParameter IAM statements."
  value       = aws_ssm_parameter.signing_key.arn
}

output "join_queue_url" {
  description = "URL of the live-join SQS queue."
  value       = aws_sqs_queue.join.url
}

output "join_queue_arn" {
  description = "ARN of the live-join SQS queue."
  value       = aws_sqs_queue.join.arn
}

output "assign_position_function_name" {
  description = "Name of the assign_position Lambda, the SQS live-join consumer."
  value       = aws_lambda_function.assign_position.function_name
}

output "seal_event_function_name" {
  description = "Name of the seal_event Lambda. Invoke it manually or via the seal schedule to open the event."
  value       = aws_lambda_function.seal_event.function_name
}

output "read_function_name" {
  description = "Name of the read Lambda backing /v1/status and /v1/queue_num."
  value       = aws_lambda_function.read.function_name
}

output "admin_function_name" {
  description = "Name of the admin Lambda backing the SigV4 /admin control plane."
  value       = aws_lambda_function.admin.function_name
}

output "controller_function_name" {
  description = "Name of the controller Lambda, fired every minute by its schedule."
  value       = aws_lambda_function.controller.function_name
}

output "event_id" {
  description = "The single event id this deployment serves. The read Lambda scopes /status and /queue_num to it."
  value       = var.event_id
}

output "rest_api_id" {
  description = "ID of the regional REST API."
  value       = aws_api_gateway_rest_api.this.id
}

output "api_invoke_url" {
  description = "Base invoke URL of the deployed stage, e.g. https://<id>.execute-api.<region>.amazonaws.com/<stage>."
  value       = aws_api_gateway_stage.this.invoke_url
}

output "api_gateway_domain_name" {
  description = "Host of the regional API (no scheme, no stage path). This is the origin the edge module's polled + write behaviours point at; the stage is set as the CloudFront origin_path."
  value       = "${aws_api_gateway_rest_api.this.id}.execute-api.${local.aws_region}.amazonaws.com"
}


output "gate_kvs_arn" {
  description = "ARN of the edge gate's CloudFront KeyValueStore (issue #71). The edge module associates its CloudFront Function with it."
  value       = aws_cloudfront_key_value_store.gate.arn
}

output "generate_token_function_name" {
  description = "Name of the Lambda that mints admission cookies and records arrivals."
  value       = aws_lambda_function.generate_token.function_name
}
