output "distribution_id" {
  description = "CloudFront distribution ID."
  value       = aws_cloudfront_distribution.this.id
}

output "distribution_arn" {
  description = "CloudFront distribution ARN. What a CloudFront flat-rate plan subscription names (ADR-0038)."
  value       = aws_cloudfront_distribution.this.arn
}

output "distribution_domain_name" {
  description = "The distribution's own *.cloudfront.net domain name. Viewers reach it under this name only when no alias is configured; see viewer_domain_name."
  value       = aws_cloudfront_distribution.this.domain_name
}

output "viewer_domain_name" {
  description = "The host viewers actually use: the first alias when a custom domain is configured, otherwise the *.cloudfront.net name. This is the host the OIDC redirect URI and any external link must be built from."
  value = (
    local.use_custom_domain
    ? var.aliases[0]
    : aws_cloudfront_distribution.this.domain_name
  )
}

output "distribution_hosted_zone_id" {
  description = "CloudFront's hosted zone ID, for Route53 alias records pointing at the distribution."
  value       = aws_cloudfront_distribution.this.hosted_zone_id
}

output "web_acl_arn" {
  description = "ARN of the edge web ACL (ADR-0038), or null when waf_enabled is off. A flat-rate plan subscription names it alongside the distribution."
  value       = local.waf_enabled ? aws_wafv2_web_acl.this[0].arn : null
}
