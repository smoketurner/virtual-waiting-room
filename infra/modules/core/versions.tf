# Provider requirements for the core module.
# The exact provider version is locked by the calling environment's lock file
# (environments/<env>/.terraform.lock.hcl); modules declare only a compatible range.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
    # random_bytes, which generates the signing key, needs 3.5 or later.
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}
