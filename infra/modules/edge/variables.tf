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

variable "api_origin_key" {
  description = "The API key the core REST API requires on every method (modules/core's api_origin_key output, ADR-0038). Sent as the x-api-key origin custom header on the API origin, so a request straight to execute-api is refused."
  type        = string
  sensitive   = true
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
  description = "Name of the session cookie generate_token mints. Forwarded to the protected origin on the default behaviour; never forwarded on polled or write behaviours (ADR-0013)."
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

# --- Readiness panel (issue #70) --------------------------------------------

variable "readiness_parameter_name" {
  description = "Name of the SSM parameter this module writes the distribution id, the gate function's ARN and the polled /status path to (modules/core's edge_readiness_parameter_name output). The admin Lambda's readiness panel reads it; core names it because edge depends on core, not the reverse."
  type        = string

  validation {
    condition     = startswith(var.readiness_parameter_name, "/")
    error_message = "readiness_parameter_name must be a hierarchical SSM name starting with '/'."
  }
}

# --- Custom domain ------------------------------------------------------------

variable "aliases" {
  description = "Alternate domain names (CNAMEs) for the distribution, e.g. [\"waiting.example.com\"]. Empty deploys on the default *.cloudfront.net domain."
  type        = list(string)
  default     = []
}

variable "acm_certificate_arn" {
  description = "ARN of an ACM certificate (in us-east-1) covering every name in aliases. Required together with aliases; CloudFront rejects a custom domain with no certificate."
  type        = string
  default     = ""

  validation {
    condition     = (length(var.aliases) > 0) == (var.acm_certificate_arn != "")
    error_message = "aliases and acm_certificate_arn must be set together or both left empty: CloudFront requires a certificate for every custom domain, and a certificate with no alias to serve is meaningless."
  }
}

# --- WAF web ACL (ADR-0038) ---------------------------------------------------
# Opt-in: priced into a CloudFront flat-rate plan (which also requires a web
# ACL), and billed per ACL and per rule every month on pay-as-you-go, which N1
# does not allow for an idle deployment.

variable "waf_enabled" {
  description = "Attach the edge web ACL (ADR-0038): a WAF token on every JavaScript API path, rate limits per token and per IP, and AWS managed rules. Turn on for a deployment subscribed to a CloudFront flat-rate plan (Business or above), which bundles its cost and requires a web ACL. Leave off on pay-as-you-go, where it bills between events."
  type        = bool
  default     = false
}

variable "waf_managed_rules_mode" {
  description = "\"count\" observes the AWS managed rules (Anti-DDoS, IP reputation, Bot Control common) and the anonymous-IP rules without acting; \"enforce\" lets them act. New rules run one event in count before enforce (O5, ADR-0012). The token and rate-limit rules act in either mode: they are the mechanism, not a classifier with an unknown false-positive rate."
  type        = string
  default     = "count"

  validation {
    condition     = contains(["count", "enforce"], var.waf_managed_rules_mode)
    error_message = "waf_managed_rules_mode must be \"count\" or \"enforce\"."
  }
}

variable "waf_token_immunity_seconds" {
  description = "How long a solved challenge or CAPTCHA is honoured. Long enough to cover a visitor's whole wait: when it lapses the next API call is challenged and the waiting page reloads to renew it, keeping the visitor's place."
  type        = number
  default     = 14400

  validation {
    condition     = var.waf_token_immunity_seconds >= 60 && var.waf_token_immunity_seconds <= 259200
    error_message = "waf_token_immunity_seconds must be between 60 and 259200 (WAF's bounds)."
  }
}

variable "waf_join_token_limit" {
  description = "Joins one WAF token may make in 10 minutes before it is refused with a 429. An honest page joins once and retries a few times; 10 is WAF's minimum."
  type        = number
  default     = 10

  validation {
    condition     = var.waf_join_token_limit >= 10
    error_message = "waf_join_token_limit must be at least 10, WAF's minimum rate limit."
  }
}

variable "waf_api_token_limit" {
  description = "Requests one WAF token may make across the JavaScript API paths in 5 minutes. The waiting page polls at most once per poll_floor_ms (5 s by default, about 60 requests), so the default leaves wide headroom while stopping a script that forces /v1/queue_num origin misses."
  type        = number
  default     = 300

  validation {
    condition     = var.waf_api_token_limit >= 10
    error_message = "waf_api_token_limit must be at least 10, WAF's minimum rate limit."
  }
}

variable "waf_join_ip_limit" {
  description = "Token requests (navigations to /_wr/verify.html) plus joins from one IP in 5 minutes before each further visitor on it must solve a CAPTCHA. A CAPTCHA rather than a block because a mobile carrier's NAT puts many real buyers behind one address."
  type        = number
  default     = 100

  validation {
    condition     = var.waf_join_ip_limit >= 10
    error_message = "waf_join_ip_limit must be at least 10, WAF's minimum rate limit."
  }
}

variable "waf_nojs_ip_limit" {
  description = "No-JavaScript joins (POST /v1/enter, ADR-0037) from one IP in 5 minutes before further ones are refused. Tight, because that path cannot run a challenge or CAPTCHA to fall back on."
  type        = number
  default     = 10

  validation {
    condition     = var.waf_nojs_ip_limit >= 10
    error_message = "waf_nojs_ip_limit must be at least 10, WAF's minimum rate limit."
  }
}

variable "nojs_enabled" {
  description = "Whether the no-JavaScript queue (/v1/enter, /v1/wait, ADR-0037) is open. Only takes effect with waf_enabled: false blocks both paths at the edge for an event where it is being abused, at the cost of visitors without JavaScript being unable to queue."
  type        = bool
  default     = true
}
