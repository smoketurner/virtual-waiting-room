terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# NOTE: CloudFront distributions and cache/origin-request policies are global
# resources managed through the default provider. The CLOUDFRONT-scoped WAF web
# ACL (waf.tf) must live in us-east-1 and pins that with its own `region`
# argument, so no aliased provider is needed for it.
