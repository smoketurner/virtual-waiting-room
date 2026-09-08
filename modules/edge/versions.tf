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
# resources managed through the default provider. A us-east-1 aliased provider
# (configuration_aliases) is reintroduced when the WAFv2 web ACL (scope
# CLOUDFRONT) and the AWS/CloudFront Requests standby alarm are added - both are
# us-east-1 only.
