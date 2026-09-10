output "authorizer_role_arn" {
  description = "ARN of the authorizer Lambda execution role."
  value       = aws_iam_role.authorizer.arn
}

output "authorizer_function_name" {
  description = "Name of the authorizer Lambda."
  value       = aws_lambda_function.authorizer.function_name
}

output "authorizer_function_arn" {
  description = "ARN of the authorizer Lambda. Attach it at the protected origin; nothing in this account invokes it."
  value       = aws_lambda_function.authorizer.arn
}

output "vpc_origin_id" {
  description = "ID of the CloudFront VPC origin, or null when enable_vpc = false."
  value       = one(aws_cloudfront_vpc_origin.this[*].id)
}
