terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # S3 remote state. Partial config: the non-sensitive settings live here, but
  # the bucket name (which embeds the AWS account id) is supplied at init so no
  # account identifier is committed:
  #   terraform init -backend-config="bucket=terraform-state-<account>-<region>-<suffix>"
  # `make init` / `make plan` / `make apply` pass it via the STATE_BUCKET var.
  backend "s3" {
    key          = "virtual-waiting-room/dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
