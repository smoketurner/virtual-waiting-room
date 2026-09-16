output "table_names" {
  description = "DynamoDB table names for this deployment."
  value       = module.core.table_names
}

output "table_arns" {
  description = "DynamoDB table ARNs for this deployment."
  value       = module.core.table_arns
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

output "open_event_function_name" {
  description = "Name of the open_event Lambda. Invoke it manually or via the open schedule to open the event."
  value       = module.core.open_event_function_name
}

output "event_id" {
  description = "The single event id this deployment serves."
  value       = module.core.event_id
}

output "cloudfront_domain_name" {
  description = "The waiting room's public host: the custom domain when one is configured, otherwise the distribution's own *.cloudfront.net name. This is the host the OIDC redirect URI is built from."
  value       = module.edge.viewer_domain_name
}

output "controller_function_name" {
  description = "Name of the controller Lambda."
  value       = module.core.controller_function_name
}


output "waiting_room_url" {
  description = "The waiting room's own URL: the CloudFront distribution created here."
  value       = local.waiting_room_url
}

output "waiting_room_page_url" {
  description = "The page an un-admitted visitor is shown. CloudFront serves it in place of the 403 it returns when admission cookies are missing."
  value       = "https://${module.edge.viewer_domain_name}/_wr/waiting.html"
}

output "gate_kvs_arn" {
  description = "ARN of the edge gate's CloudFront KeyValueStore (issue #71)."
  value       = module.core.gate_kvs_arn
}

output "demo_origin_bucket" {
  description = "Bucket holding the demo protected origin's pages. Only serving traffic while client_origin_domain_name is empty."
  value       = module.demo_origin.bucket_name
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID. Needed to invalidate the waiting-room pages after changing them, since they are cached at the edge for five minutes."
  value       = module.edge.distribution_id
}

output "open_schedule_name" {
  description = "Name of the one-time open schedule (issue #128), so scripts can disable it alongside a reset."
  value       = module.core.open_schedule_name
}
