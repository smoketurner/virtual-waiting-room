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

# --- Live-join Lambda (DESIGN §2.2, §5.3, §6) ---------------------------------

variable "lambda_artifact_path" {
  description = "Path to the assign_position Lambda bootstrap zip (provided.al2023, arm64). Defaults to the vendored placeholder that returns success and drops the batch; replaced by the Rust build artifact."
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
