# Outputs (authorizer Lambda ARN, execution role ARN, VPC origin ID) are added
# in Phase 2 alongside the Lambda and VPC-origin resources.

output "authorizer_role_arn" {
  description = "ARN of the authorizer Lambda execution role."
  value       = aws_iam_role.authorizer.arn
}

output "authorizer_function_name" {
  description = "Name of the authorizer Lambda, or null until an artifact is supplied."
  value       = one(aws_lambda_function.authorizer[*].function_name)
}

output "authorizer_function_arn" {
  description = "ARN of the authorizer Lambda, or null until an artifact is supplied."
  value       = one(aws_lambda_function.authorizer[*].arn)
}

output "vpc_origin_id" {
  description = "ID of the CloudFront VPC origin, or null when enable_vpc = false."
  value       = one(aws_cloudfront_vpc_origin.this[*].id)
}
