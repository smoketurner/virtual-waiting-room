variable "region" {
  description = "AWS region for the regional core resources (DynamoDB, SQS, Lambda, REST API)."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Named AWS profile from the shared config to authenticate with. Empty uses the default credential chain (environment, SSO session, instance role)."
  type        = string
  default     = ""
}

variable "name_prefix" {
  description = "Prefix for every resource name in this deployment. Isolates a second deployment in the same account."
  type        = string
  default     = "vwr-dev"
}

variable "env" {
  description = "Deployment environment. Drives the REST API stage name and the Environment tag."
  type        = string
  default     = "dev"
}

variable "warm_throughput_write_units" {
  description = "Warm write units/s to pre-provision per table ahead of an event. 0 = no pre-warm; >= 4000 if set."
  type        = number
  default     = 0
}

variable "warm_throughput_read_units" {
  description = "Warm read units/s to pre-provision per table ahead of an event. 0 = no pre-warm; >= 12000 if set."
  type        = number
  default     = 0
}

variable "client_origin_domain_name" {
  description = "Bare domain name of the client's protected origin (e.g. www.example.com). Empty protects the demo origin instead, which is what makes the gate exercisable without a real origin to protect."
  type        = string
  default     = ""

  validation {
    condition     = var.client_origin_domain_name == "" || !can(regex("://|/", var.client_origin_domain_name))
    error_message = "client_origin_domain_name must be a bare domain (host only, no scheme and no path) - e.g. www.example.com, not https://www.example.com."
  }
}

variable "lambda_architecture" {
  description = "Lambda CPU architecture for every function: arm64 or x86_64. Must match the built artifacts."
  type        = string
  default     = "arm64"
}

# --- Origin authorizer --------------------------------------------------------
# The authorizer runs at the customer's protected origin, not at the edge: it is
# invoked with the ALB / API Gateway request shape and answers 200 to serve the
# request or 302 to send the visitor to wait. This root creates the function, its
# role, and its policy, and exports the ARN; attaching it to the origin happens
# where the origin lives, which this configuration does not own.
#
# The whole origin is protected. Narrowing that is a per-deployment decision made
# at the origin, and defaulting to "gate everything" fails safe.

variable "event_id" {
  description = "The single event id this MVP deployment serves."
  type        = string
  default     = "default"
  validation {
    condition     = !can(regex("#", var.event_id))
    error_message = "event_id must not contain '#': it is the separator in the DynamoDB key, so 'a#PQ#1' would collide with the first pre-queue shard of event 'a'."
  }
}

variable "seal_start_time" {
  description = "One-time UTC seal time as an EventBridge at() value, e.g. \"2026-09-10T18:00:00\". Empty = seal invoked manually."
  type        = string
  default     = ""
}

# --- Admin OIDC login (ADR-0016) ----------------------------------------------

variable "oidc_issuer" {
  description = "OIDC issuer / discovery base URL for admin login (Vouch by default)."
  type        = string
  default     = "https://us.vouch.sh"
}

variable "oidc_client_id" {
  description = "OAuth2 client id for the admin OIDC application. Empty until registered with the provider."
  type        = string
  default     = ""
}

variable "oidc_redirect_uri" {
  description = "OIDC callback URL registered with the provider — the CloudFront URL of /admin/callback."
  type        = string
  default     = ""
}

variable "oidc_allowed_emails" {
  description = "Comma-separated allowlist of operator emails permitted admin access. Empty = deny all (fail closed)."
  type        = string
  default     = ""
}
