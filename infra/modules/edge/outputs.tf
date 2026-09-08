output "distribution_id" {
  description = "CloudFront distribution ID."
  value       = aws_cloudfront_distribution.this.id
}

output "distribution_arn" {
  description = "CloudFront distribution ARN. Needed to associate a WAFv2 web ACL later."
  value       = aws_cloudfront_distribution.this.arn
}

output "distribution_domain_name" {
  description = "The *.cloudfront.net domain name viewers hit (unless custom aliases are set)."
  value       = aws_cloudfront_distribution.this.domain_name
}

output "distribution_hosted_zone_id" {
  description = "CloudFront's hosted zone ID, for Route53 alias records pointing at the distribution."
  value       = aws_cloudfront_distribution.this.hosted_zone_id
}
