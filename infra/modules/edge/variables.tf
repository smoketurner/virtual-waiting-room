variable "name_prefix" {
  description = "Prefix applied to every resource name in this deployment."
  type        = string
}

variable "tags" {
  description = "Tags applied to every resource in the module."
  type        = map(string)
  default     = {}
}

# --- Origins ------------------------------------------------------------------

variable "api_gateway_domain_name" {
  description = "Regional domain name of the core REST API (the polled + write behaviours' origin). Host header only, no scheme or path."
  type        = string
}

variable "env" {
  description = "Deployment environment. It is the REST API stage name, so it is the API origin's origin_path: a viewer request for /v1/status is forwarded to /<env>/v1/status."
  type        = string
}

variable "client_origin_domain_name" {
  description = "Domain name of the client's protected origin (the default behaviour). Empty points the protected behaviour at the demo origin instead, so the gate can be exercised without a real origin to protect."
  type        = string
  default     = ""
}

variable "demo_origin_domain_name" {
  description = "Regional domain name of the demo origin bucket, used as the protected origin when client_origin_domain_name is empty."
  type        = string
  default     = ""
}

variable "demo_origin_access_control_id" {
  description = "Origin access control CloudFront signs its demo-origin reads with."
  type        = string
  default     = ""
}

# --- Caching (ADR-0013, DESIGN §8) --------------------------------------------
# Polled behaviours use Min TTL > 0 and forward zero cookies, so CloudFront
# collapses simultaneous misses into one origin fetch. Cache keys differ per
# endpoint: /status is keyed on path alone; /queue_num adds event_id and
# request_id because its answer is per visitor.

variable "polled_min_ttl_seconds" {
  description = "Min TTL for the polled cache policies. Must be > 0 or CloudFront disables request collapsing and every poll hits origin (ADR-0013)."
  type        = number
  default     = 1

  validation {
    condition     = var.polled_min_ttl_seconds > 0
    error_message = "polled_min_ttl_seconds must be > 0 to preserve request collapsing (ADR-0013)."
  }
}

variable "session_cookie_name" {
  description = "Name of the session cookie the authorizer sets. Forwarded to the protected origin on the default behaviour; never forwarded on polled or write behaviours (ADR-0013)."
  type        = string
  default     = "vwr_session"

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]+$", var.session_cookie_name))
    error_message = "session_cookie_name must be alphanumeric, '_', or '-' only: it is templated into a single-quoted JavaScript string literal in the gate's CloudFront Function source (issue #71), and a quote, backslash, or newline here would inject script rather than fail cleanly."
  }
}

# --- Distribution -------------------------------------------------------------

variable "price_class" {
  description = "CloudFront price class. PriceClass_100 (NA + EU) is the cheapest; widen per client reach."
  type        = string
  default     = "PriceClass_100"

  validation {
    condition     = contains(["PriceClass_All", "PriceClass_200", "PriceClass_100"], var.price_class)
    error_message = "price_class must be one of PriceClass_All, PriceClass_200, PriceClass_100."
  }
}

# --- Edge gate (issue #71) ------------------------------------------------
# The gate is a CloudFront Function at viewer-request on the protected
# behaviour only (never distribution-wide — Functions bill per invocation,
# and a distribution-wide association would bill every /status poll from
# every waiter). It reads its whole configuration and the signing secret from
# gate_kvs_arn; event_id and session_cookie_name are templated into the
# function's own source instead, so Terraform stays their single source of
# truth and a change to either republishes the function in the same apply
# that changes generate_token (docs/adr/0021-edge-function-gate.md §3).

variable "gate_kvs_arn" {
  description = "ARN of the edge gate's CloudFront KeyValueStore (modules/core's gate_kvs_arn output). Required: the gate cannot verify anything without it."
  type        = string
}

variable "event_id" {
  description = "The event id a session credential must carry. Templated into the CloudFront Function's source (not carried in the KeyValueStore value)."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9_-]+$", var.event_id))
    error_message = "event_id must be alphanumeric, '_', or '-' only: it is templated into a single-quoted JavaScript string literal in the gate's CloudFront Function source (issue #71), and a quote, backslash, or newline here would inject script rather than fail cleanly."
  }
}
