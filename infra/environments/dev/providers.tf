provider "aws" {
  region  = var.region
  profile = var.aws_profile != "" ? var.aws_profile : null

  default_tags {
    tags = local.common_tags
  }
}

# Global / us-east-1 provider for the edge module: WAFv2 (scope CLOUDFRONT) and
# the AWS/CloudFront Requests standby alarm are us-east-1 only.
provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1"
  profile = var.aws_profile != "" ? var.aws_profile : null

  default_tags {
    tags = local.common_tags
  }
}
