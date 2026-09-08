terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Remote state backend. Left partial on purpose: pass -backend-config at init
  # (or add a dev.s3.tfbackend file) so the bucket/table/region are not baked
  # into the repo. Until then `terraform init` uses local state.
  #
  # backend "s3" {
  #   key          = "virtual-waiting-room/dev/terraform.tfstate"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.common_tags
  }
}

# Global / us-east-1 provider for the edge module: WAFv2 (scope CLOUDFRONT) and
# the AWS/CloudFront Requests standby alarm are us-east-1 only.
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"

  default_tags {
    tags = local.common_tags
  }
}
