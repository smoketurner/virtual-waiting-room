variable "name_prefix" {
  description = "Prefix applied to every resource name in this deployment (e.g. \"vwr-prod\"). Keeps a second deployment in the same account isolated."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name_prefix))
    error_message = "name_prefix must be 2-31 chars, lowercase alphanumeric or hyphen, starting with a letter."
  }
}

variable "tags" {
  description = "Tags applied to every resource in the module."
  type        = map(string)
  default     = {}
}

# --- DynamoDB pre-warm (DESIGN §5.5, O1/O2) -----------------------------------
# The one data-plane knob that matters: warm throughput is an operator action
# taken ahead of a large event. 0 = the on-demand cold baseline (idle default,
# no cost). PITR and TTL are always on - they are near-free and required, so
# they are not toggles.

variable "warm_throughput_write_units" {
  description = "Warm write units/s to pre-provision per table ahead of an event, sized to the target admission rate. 0 = no pre-warm. If set, AWS enforces a minimum of 4000."
  type        = number
  default     = 0

  validation {
    condition     = var.warm_throughput_write_units == 0 || var.warm_throughput_write_units >= 4000
    error_message = "warm_throughput_write_units must be 0 (no pre-warm) or >= 4000 (AWS DynamoDB warm-throughput minimum)."
  }
}

variable "warm_throughput_read_units" {
  description = "Warm read units/s to pre-provision per table ahead of an event. 0 = no pre-warm. If set, AWS enforces a minimum of 12000."
  type        = number
  default     = 0

  validation {
    condition     = var.warm_throughput_read_units == 0 || var.warm_throughput_read_units >= 12000
    error_message = "warm_throughput_read_units must be 0 (no pre-warm) or >= 12000 (AWS DynamoDB warm-throughput minimum)."
  }
}

# --- Lambda artifacts (DESIGN §2.2, §5.3, §6) ---------------------------------
# Each function is a Rust bootstrap zip. An empty path falls back to the vendored
# placeholder so the plane can be created before the crates are built. The join
# event-source mapping stays DISABLED whenever assign_position is a placeholder.

variable "assign_position_artifact_path" {
  description = "Path to the assign_position Lambda bootstrap zip. Empty = vendored placeholder (join ESM stays disabled)."
  type        = string
  default     = ""
}

variable "seal_event_artifact_path" {
  description = "Path to the seal_event Lambda bootstrap zip. Empty = vendored placeholder."
  type        = string
  default     = ""
}

variable "read_artifact_path" {
  description = "Path to the read Lambda bootstrap zip (serves /v1/status and /v1/queue_num). Empty = vendored placeholder."
  type        = string
  default     = ""
}

variable "admin_artifact_path" {
  description = "Path to the admin Lambda bootstrap zip (SigV4 /admin control plane). Empty = vendored placeholder."
  type        = string
  default     = ""
}

variable "controller_artifact_path" {
  description = "Path to the controller Lambda bootstrap zip (10s outflow controller, DESIGN section 7). Empty = vendored placeholder."
  type        = string
  default     = ""
}

variable "enable_controller" {
  description = "Create the recurring controller schedule (rate(1 minute), six 10s passes per invoke). Off by default; enable ahead of an event so admission is metered and positions expire."
  type        = bool
  default     = false
}

variable "lambda_architecture" {
  description = "Lambda CPU architecture for every function: arm64 (design default) or x86_64. Must match the built artifacts."
  type        = string
  default     = "arm64"

  validation {
    condition     = contains(["arm64", "x86_64"], var.lambda_architecture)
    error_message = "lambda_architecture must be arm64 or x86_64."
  }
}

variable "event_id" {
  description = "The single event id this MVP deployment serves. The read Lambda scopes /status and /queue_num to it."
  type        = string
  default     = "default"
}

variable "seal_start_time" {
  description = "One-time UTC start time for the seal, as an EventBridge at() value without the 'at(' wrapper, e.g. \"2026-09-10T18:00:00\". Empty = no schedule created (seal invoked manually)."
  type        = string
  default     = ""
}

# --- Admin OIDC login (ADR-0016) ----------------------------------------------
# The client secret is NOT a variable — it is an SSM SecureString written out of
# band. These are the non-secret OIDC config the admin Lambda needs.

variable "oidc_issuer" {
  description = "OIDC issuer / discovery base URL for admin login (ADR-0016), e.g. https://us.vouch.sh."
  type        = string
  default     = "https://us.vouch.sh"
}

variable "oidc_client_id" {
  description = "OAuth2 client id for the admin OIDC application. Empty in dev until an application is registered with the provider."
  type        = string
  default     = ""
}

variable "oidc_redirect_uri" {
  description = "OIDC callback URL registered with the provider — the CloudFront (or API) URL of /admin/callback, e.g. https://d123.cloudfront.net/admin/callback."
  type        = string
  default     = ""
}

variable "oidc_allowed_emails" {
  description = "Comma-separated allowlist of operator emails permitted to hold an admin session. Empty = deny all (the admin Lambda fails closed)."
  type        = string
  default     = ""
}

variable "assign_position_reserved_concurrency" {
  description = "Reserved concurrency on the assign_position function. Mandatory for event isolation (ADR-0008): without it a runaway event starves the others. -1 leaves it unreserved (single-event dev only)."
  type        = number
  default     = 10
}

variable "env" {
  description = "Deployment environment (e.g. dev, staging, prod). Drives the REST API stage name, so the invoke URL is https://<id>.execute-api.<region>.amazonaws.com/<env>."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,63}$", var.env))
    error_message = "env must be lowercase alphanumeric or hyphen, starting with a letter (valid API Gateway stage name)."
  }
}
