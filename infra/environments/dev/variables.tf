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
  description = "OIDC callback URL registered with the provider — the CloudFront URL of /_wr/admin/callback."
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

variable "origin_concurrency" {
  description = "How many visitors the origin takes at once. Seeded onto the event item so the controller admits from the first apply; changed live from the dashboard."
  type        = number
  default     = 100
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

# --- Custom domain ------------------------------------------------------------
# A real event fronts the distribution with the customer's own hostname. The
# *.cloudfront.net default works and is the fallback, but the session cookie is
# set for the host the visitor is on, so the waiting room and the origin it
# protects belong on the same domain.

variable "aliases" {
  description = "Alternate domain names for the distribution, e.g. [\"waiting.example.com\"]. Empty deploys on the default *.cloudfront.net name."
  type        = list(string)
  default     = []
}

variable "acm_certificate_arn" {
  description = "ARN of an ACM certificate in us-east-1 covering every name in aliases. Required together with aliases; the edge module validates the pair."
  type        = string
  default     = ""
}

# --- Sessions -----------------------------------------------------------------

variable "session_ttl_seconds" {
  description = "How long an admitted visitor has on the origin, such as the time to complete a purchase. Each session holds a slot of origin_concurrency for exactly this long, so the room admits about origin_concurrency visitors per session length. When it lapses the visitor is returned to the queue; nothing renews it at the edge."
  type        = number
  default     = 900
}

# --- Flat-rate plan and edge protection (ADR-0038) ---------------------------
# Pay-as-you-go unless a tier is set. A plan bundles the WAF web ACL and
# requires one; on pay-as-you-go the ACL bills every month whether or not an
# event is running, so it exists only with a plan.

variable "flat_rate_plan_tier" {
  description = "CloudFront flat-rate plan for the distribution (BUSINESS or PREMIUM), which also attaches the edge web ACL: a WAF token on the JavaScript API, per-token and per-IP rate limits, and AWS managed rules. null stays on pay-as-you-go with no web ACL. Billed monthly; the distribution cannot be deleted until a cancelled plan's billing period ends."
  type        = string
  default     = null
}

variable "waf_managed_rules_mode" {
  description = "\"count\" (the default) observes the managed and anonymous-IP rules; \"enforce\" lets them act, after an event's worth of Count data (O5)."
  type        = string
  default     = "count"
}

variable "waf_join_ip_limit" {
  description = "Token requests (verify-page loads) plus joins per IP per 5 minutes before further visitors on that address must solve a CAPTCHA."
  type        = number
  default     = 100
}

variable "waf_nojs_wait_ip_limit" {
  description = "Loads of the no-JavaScript waiting page (GET /_wr/v1/wait) per IP per 5 minutes before further ones are refused."
  type        = number
  default     = 300
}

variable "waf_require_token_cookie" {
  description = "Whether a JavaScript API request holding a WAF token must carry it as the cookie. False counts such requests instead of blocking them."
  type        = bool
  default     = true
}

variable "nojs_enabled" {
  description = "Whether the no-JavaScript queue stays open on a flat-rate plan. false closes it at the edge for an event where it is being abused."
  type        = bool
  default     = true
}

# --- Operations ---------------------------------------------------------------
# Validated in modules/core/variables.tf, which also carries the rationale.

variable "alarm_sns_topic_arn" {
  description = "SNS topic every CloudWatch alarm notifies, on alarm and on recovery. Empty attaches no action: the alarms evaluate and nobody is told."
  type        = string
  default     = ""
}

variable "table_deletion_protection" {
  description = "DynamoDB deletion protection on all four tables. Off for dev stacks, which are destroyed. Must be off for the apply that follows a change of event_id, which recreates PreQueue and Positions."
  type        = bool
  default     = false
}

variable "controller_reserved_concurrency" {
  description = "Reserved concurrency on the controller function, so a burst cannot starve admission. -1 leaves it unreserved."
  type        = number
  default     = 2
}

# --- REST API stage throttles (issue #245) ------------------------------------

variable "signing_key_generation" {
  description = "Generation of the session signing key. Raise by one to rotate; the previous generation's sessions stay valid until they expire."
  type        = number
  default     = 1
}

variable "origin_key_generation" {
  description = "Generation of the API key CloudFront sends the REST API. Raise by one to rotate; the previous generation stays valid until the next rotation."
  type        = number
  default     = 1
}


variable "oidc_require_email_verified" {
  description = "Whether an admin login needs the provider's email_verified claim to be present and true. Turn off only for a provider that omits the claim and verifies every address."
  type        = bool
  default     = true
}

variable "api_join_throttle_rate_limit" {
  description = "Requests per second POST /_wr/v1/join accepts before answering 429."
  type        = number
  default     = 7000
}

variable "api_join_throttle_burst_limit" {
  description = "Token-bucket size for POST /_wr/v1/join."
  type        = number
  default     = 3500
}

variable "api_throttle_rate_limit" {
  description = "Requests per second each other API method accepts before answering 429."
  type        = number
  default     = 10000
}

variable "api_throttle_burst_limit" {
  description = "Token-bucket size for each API method other than POST /_wr/v1/join."
  type        = number
  default     = 5000
}
