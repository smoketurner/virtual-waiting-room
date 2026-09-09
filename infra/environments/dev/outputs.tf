output "table_names" {
  description = "DynamoDB table names for this deployment."
  value       = module.core.table_names
}

output "table_arns" {
  description = "DynamoDB table ARNs for this deployment."
  value       = module.core.table_arns
}

output "signing_key_parameter_name" {
  description = "Name of the SSM SecureString parameter holding the signing key."
  value       = module.core.signing_key_parameter_name
}

output "join_queue_url" {
  description = "URL of the live-join SQS queue."
  value       = module.core.join_queue_url
}

output "rest_api_id" {
  description = "ID of the regional REST API."
  value       = module.core.rest_api_id
}

output "api_invoke_url" {
  description = "Base invoke URL of the deployed API stage."
  value       = module.core.api_invoke_url
}

output "seal_event_function_name" {
  description = "Name of the seal_event Lambda. Invoke it manually or via the seal schedule to open the event."
  value       = module.core.seal_event_function_name
}

output "event_id" {
  description = "The single event id this deployment serves."
  value       = module.core.event_id
}

output "cloudfront_domain_name" {
  description = "CloudFront distribution domain name, when a client origin is supplied (edge created)."
  value       = length(module.edge) > 0 ? module.edge[0].distribution_domain_name : null
}
