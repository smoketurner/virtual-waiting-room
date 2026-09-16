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

variable "event_id" {
  description = "The single event id this MVP deployment serves."
  type        = string
  default     = "default"
  validation {
    condition     = !can(regex("#", var.event_id))
    error_message = "event_id must not contain '#': it is the separator in the DynamoDB key, so 'a#PQ#1' would collide with the first pre-queue shard of event 'a'."
  }
}

variable "session_cookie_name" {
  description = "Name of the session cookie generate_token sets and the edge gate's CloudFront Function verifies (issue #71). Shared between modules.core and modules.edge so they cannot drift apart."
  type        = string
  default     = "vwr_session"
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

# --- Adaptive poll policy (#69) -------------------------------------
# Published on /status; see modules/core/variables.tf for the full rationale.

variable "poll_floor_ms" {
  description = "Minimum client poll interval in milliseconds."
  type        = number
  default     = 5000
}

variable "poll_ceiling_ms" {
  description = "Maximum client poll interval in milliseconds."
  type        = number
  default     = 30000
}

variable "poll_divisor" {
  description = "Divides a visitor's estimated wait (seconds) into their poll interval."
  type        = number
  default     = 10
}

# --- Event seeding ------------------------------------------------------------
# Seeded onto the stack at apply so a freshly applied deployment serves. The
# control plane owns all three from there: Terraform writes them once and then
# ignores changes.

variable "admission_rate" {
  description = "Target admission rate in visitors per second. Seeded onto the event item so the controller drains from the first apply; changed live from the dashboard."
  type        = number
  default     = 5
}

variable "gate_rules" {
  description = "Which requests the edge gate covers, one rule per line: `p <path prefix>`, `c <cookie name>`, `u <user agent substring>`, `h <header name> <header value>`. Empty means dormant -- every request passes through."
  type        = string
  default     = ""
}

variable "starts_at" {
  description = "When the event opens, local date-time with no zone (`2027-03-14T10:00:00`), evaluated in starts_at_timezone. Empty leaves the schedule disabled."
  type        = string
  default     = ""
}

variable "starts_at_timezone" {
  description = "IANA zone the start time is evaluated in, e.g. America/New_York."
  type        = string
  default     = "UTC"
}
