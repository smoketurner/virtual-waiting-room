terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # S3 remote state.
  backend "s3" {
    profile      = "dev-admin"
    bucket       = "terraform-state-952961969614-us-east-1-an"
    key          = "virtual-waiting-room/dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
