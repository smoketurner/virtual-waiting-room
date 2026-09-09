variable "region" {
  description = "AWS region for the regional core resources (DynamoDB, SQS, Lambda, REST API)."
  type        = string
  default     = "us-east-1"
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
  description = "Domain name of the client's protected origin (the CloudFront default behaviour). Empty means the edge/CloudFront module is not created - it cannot exist without a real origin."
  type        = string
  default     = ""
}

# --- Rust Lambda artifacts ----------------------------------------------------
# Point these at the built bootstrap binaries to deploy the real functions.
# Empty leaves each as the vendored placeholder (and the join ESM disabled).

variable "assign_position_artifact_path" {
  description = "Path to the built assign_position bootstrap binary."
  type        = string
  default     = ""
}

variable "seal_event_artifact_path" {
  description = "Path to the built seal_event bootstrap binary."
  type        = string
  default     = ""
}

variable "read_artifact_path" {
  description = "Path to the built read bootstrap binary."
  type        = string
  default     = ""
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
}

variable "seal_start_time" {
  description = "One-time UTC seal time as an EventBridge at() value, e.g. \"2026-09-10T18:00:00\". Empty = seal invoked manually."
  type        = string
  default     = ""
}
