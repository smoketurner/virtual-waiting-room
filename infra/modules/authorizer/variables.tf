variable "name_prefix" {
  description = "Prefix applied to every resource name in this deployment."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource in the module."
  type        = map(string)
  default     = {}
}

variable "signing_key_parameter_arn" {
  description = "ARN of the SSM SecureString parameter holding the signing key (from modules/core). The authorizer reads it (ssm:GetParameter) to validate admission tokens and session cookies."
  type        = string
}

variable "lambda_artifact_path" {
  description = "Path to the built authorizer Lambda bootstrap zip (provided.al2023, arm64). Empty until the Rust crate is built (Phase 1/2)."
  type        = string
  default     = ""
}

variable "lambda_memory_size" {
  description = "Authorizer Lambda memory (MB). Drives CPU allocation, which affects the aws-lc-rs jitter-entropy cold-start tax (tech.md)."
  type        = number
  default     = 256
}

variable "lambda_timeout_seconds" {
  description = "Authorizer Lambda timeout (seconds). Kept low: the authorizer is on the request hot path and fails open on error."
  type        = number
  default     = 5
}

# --- VPC origin seam (DESIGN §11, §12) ----------------------------------------
# enable_vpc is a variable, NOT a topology fork: the authorizer's code is
# identical either way. When true, the client origin sits in a private subnet
# with CloudFront as sole ingress via a CloudFront VPC origin. VPC origins forbid
# Lambda@Edge origin triggers, require an unused internet gateway, and are
# unavailable in GovCloud (which is a separate topology entirely - Phase 5).

variable "enable_vpc" {
  description = "Place the client origin behind a CloudFront VPC origin in a private subnet, for ATO-constrained clients. Opt-in seam; Lambda code is unchanged."
  type        = bool
  default     = false
}

variable "vpc_origin_subnet_ids" {
  description = "Private subnet IDs for the CloudFront VPC origin. Required when enable_vpc = true."
  type        = list(string)
  default     = []
}
