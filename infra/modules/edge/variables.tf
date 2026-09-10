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

variable "trusted_key_group_ids" {
  description = "CloudFront key group IDs allowed to sign admission cookies for the protected behaviour. Empty leaves the origin ungated, which is only correct before the signing key exists."
  type        = list(string)
  default     = []
}
