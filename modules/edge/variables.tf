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
  description = "Domain name of the client's protected origin (the default behaviour). Behind a CloudFront VPC origin when the authorizer module enables it (not yet wired)."
  type        = string
}

# --- Caching (ADR-0013, DESIGN §8) --------------------------------------------
# Three behaviours: polled (Min TTL > 0, zero cookies forwarded so CloudFront
# collapses simultaneous misses into one origin fetch), write (uncached), and
# the protected default (uncached, session cookie forwarded to origin). Per-
# endpoint cache keys differ (DESIGN §8): /status = path only; /queue_num &
# /queue_pos_expiry = path + event_id + request_id; /public_key = path + event_id.

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
