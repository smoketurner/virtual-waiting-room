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
  description = "CloudFront distribution domain name — the waiting room's public host."
  value       = module.edge.distribution_domain_name
}

output "controller_function_name" {
  description = "Name of the controller Lambda."
  value       = module.core.controller_function_name
}


output "authorizer_function_arn" {
  description = "ARN of the origin authorizer Lambda. Attach it at a protected origin you control: it is invoked with the ALB / API Gateway request shape and answers 200 to serve or 302 to send the visitor to wait."
  value       = module.authorizer.authorizer_function_arn
}

output "authorizer_role_arn" {
  description = "ARN of the authorizer execution role."
  value       = module.authorizer.authorizer_role_arn
}

output "waiting_room_url" {
  description = "The URL the authorizer redirects un-admitted visitors to."
  value       = local.waiting_room_url
}

output "waiting_room_page_url" {
  description = "The page an un-admitted visitor is shown. CloudFront serves it in place of the 403 it returns when admission cookies are missing."
  value       = "https://${module.edge.distribution_domain_name}/_wr/waiting.html"
}

output "admission_key_pair_id" {
  description = "ID of the CloudFront public key that verifies admission cookies."
  value       = module.core.admission_key_pair_id
}

output "demo_origin_bucket" {
  description = "Bucket holding the demo protected origin's pages. Only serving traffic while client_origin_domain_name is empty."
  value       = module.demo_origin.bucket_name
}
