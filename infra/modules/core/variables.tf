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

# --- Lambda artifacts ---------------------------------------------------------
# Each function is a Rust bootstrap zip produced by `make build`. All are
# required: there is no stub fallback, so a stack cannot come up looking
# deployed while serving nobody.

variable "assign_position_artifact_path" {
  description = "Path to the assign_position Lambda bootstrap zip (the SQS live-join consumer)."
  type        = string
}

variable "open_event_artifact_path" {
  description = "Path to the open_event Lambda bootstrap zip."
  type        = string
}

variable "read_artifact_path" {
  description = "Path to the read Lambda bootstrap zip (serves /v1/status and /v1/queue_num)."
  type        = string
}

variable "admin_artifact_path" {
  description = "Path to the admin Lambda bootstrap zip (the OIDC-gated operator control plane)."
  type        = string
}

variable "controller_artifact_path" {
  description = "Path to the controller Lambda bootstrap zip (meters admission, expires positions)."
  type        = string
}

variable "generate_token_artifact_path" {
  description = "Path to the generate_token Lambda bootstrap zip (mints admission cookies, records arrivals)."
  type        = string
}

variable "session_ttl_seconds" {
  description = "How long an admission session credential stays valid. Long enough to complete a purchase, short enough that a leaked cookie is not a standing bypass."
  type        = number
  default     = 3600

  validation {
    condition     = var.session_ttl_seconds > 0 && var.session_ttl_seconds <= 86400
    error_message = "session_ttl_seconds must be between 1 second and 24 hours."
  }
}

variable "session_cookie_name" {
  description = "Name of the session cookie generate_token sets and the edge gate's CloudFront Function (modules/edge) verifies. Shared between the two modules at the dev root so they cannot drift apart."
  type        = string
  default     = "vwr_session"
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
  validation {
    condition     = !can(regex("#", var.event_id))
    error_message = "event_id must not contain '#': it is the separator in the DynamoDB key, so 'a#PQ#1' would collide with the first pre-queue shard of event 'a'."
  }
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
  description = <<-EOT
    Reserved concurrency on the assign_position function. Mandatory for event
    isolation (ADR-0008): without it a runaway event starves the others. -1
    leaves it unreserved (single-event dev only).

    Sized so the ingest queue drains faster than registrations arrive at the
    ~10,000/s target, with headroom: a batch is 100 records and up to ~112
    sequential DynamoDB calls, so the per-call latency sets the drain rate, and
    it is not measured. Headroom is free — this is a ceiling, not a
    reservation that bills when idle — and running short is not merely slow: a
    registrant still queued when the event opens is demoted to the back of the
    live-join queue.

    Raising this alone does not raise the ceiling on `PreQueue` writes. Set
    `warm_throughput_write_units` on that table before a large-cohort
    rehearsal, or its on-demand ramp becomes the new bottleneck.
  EOT
  type        = number
  default     = 150
}

variable "env" {
  description = "Deployment environment (e.g. dev, staging, prod). Drives the REST API stage name, so the invoke URL is https://<id>.execute-api.<region>.amazonaws.com/<env>."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,63}$", var.env))
    error_message = "env must be lowercase alphanumeric or hyphen, starting with a letter (valid API Gateway stage name)."
  }
}

# --- Adaptive poll policy (#69) -------------------------------------
# Published on /status so waiting.js can space polls out with distance from
# the front instead of every waiting visitor polling at the same fixed
# interval regardless of their own wait. A Terraform variable rather than an
# admin lever: a value set here reaches every client on their next 1-second
# /status miss, exactly as fast as an admin form would, for three env reads
# instead of a Store method, a route, a form, and their own debounce/audit
# surface — and it costs zero Terraform resources against the N6 ceiling.

variable "poll_floor_ms" {
  description = "Minimum client poll interval in milliseconds: the front of the queue never waits longer than this to learn it has been admitted, and it is also the interval used before a position or admission rate is known. The default (5000) matches the fixed interval every client used before #69. It is also the per-visitor request-rate multiplier the cost model in DESIGN.md §12 is built on — lowering it raises every waiting visitor's request rate against the operator's own CloudFront allowance."
  type        = number
  default     = 5000

  validation {
    condition     = var.poll_floor_ms >= 1000 && var.poll_floor_ms <= 300000
    error_message = "poll_floor_ms must be between 1,000 and 300,000 milliseconds."
  }

  validation {
    condition     = floor(var.poll_floor_ms) == var.poll_floor_ms
    error_message = "poll_floor_ms must be a whole number of milliseconds — a fractional value fails read's u32 parse and silently disables the whole policy."
  }
}

variable "poll_ceiling_ms" {
  description = "Maximum client poll interval in milliseconds, reached by a visitor far from the front. Below 30,000 the client's own rate-measurement window (two samples spanning 30s) takes one extra poll to converge — not a break, just worth knowing before setting it low."
  type        = number
  default     = 30000

  validation {
    condition     = var.poll_ceiling_ms >= 1000 && var.poll_ceiling_ms <= 300000
    error_message = "poll_ceiling_ms must be between 1,000 and 300,000 milliseconds."
  }

  validation {
    condition     = var.poll_ceiling_ms >= var.poll_floor_ms
    error_message = "poll_ceiling_ms must be >= poll_floor_ms."
  }

  validation {
    condition     = floor(var.poll_ceiling_ms) == var.poll_ceiling_ms
    error_message = "poll_ceiling_ms must be a whole number of milliseconds — a fractional value fails read's u32 parse and silently disables the whole policy."
  }
}

variable "poll_divisor" {
  description = "Divides a visitor's estimated wait, in seconds, into their poll interval in milliseconds per second of wait: a visitor who can see N seconds left polls roughly every N/divisor seconds, clamped to [poll_floor_ms, poll_ceiling_ms]."
  type        = number
  default     = 10

  validation {
    condition     = var.poll_divisor >= 1 && var.poll_divisor <= 1000
    error_message = "poll_divisor must be between 1 and 1,000."
  }

  validation {
    condition     = floor(var.poll_divisor) == var.poll_divisor
    error_message = "poll_divisor must be a whole number — a fractional value fails read's u32 parse and silently disables the whole policy."
  }
}

variable "admission_rate" {
  description = "Target admission rate in visitors per second, seeded onto the event item so the controller drains from the first apply. An absent or zero target_rate releases nobody and expires nobody, forever, while the controller logs a successful pass every ten seconds -- which is why this has a real default rather than zero. The operator changes it live from the dashboard; Terraform only seeds it."
  type        = number
  default     = 5

  validation {
    condition     = var.admission_rate >= 1 && var.admission_rate <= 100000 && floor(var.admission_rate) == var.admission_rate
    error_message = "admission_rate must be a whole number of visitors per second between 1 and 100000 (MAX_ADMISSION_RATE)."
  }
}

variable "gate_rules" {
  description = "Which requests the edge gate covers, one rule per line, in the same grammar the dashboard's Set rules form takes: `p <path prefix>`, `c <cookie name>`, `u <user agent substring>`, or `h <header name> <header value>`. Blank lines and `#` comments are ignored. Empty means dormant -- every request passes through untouched (issue #60) -- which is a deliberate configuration, not a broken one. Seeded at apply so protection can be declared in terraform.tfvars; the dashboard owns it live thereafter."
  type        = string
  default     = ""

  validation {
    condition = alltrue([
      for line in compact([for l in split("\n", var.gate_rules) : trimspace(l)]) :
      startswith(line, "#") || can(regex("^(?:[pcu]\\s+\\S.*|h\\s+\\S+\\s+\\S.*)$", line))
    ])
    error_message = "each gate_rules line must be `p <path prefix>`, `c <cookie name>`, `u <user agent substring>`, `h <header name> <header value>`, a # comment, or blank."
  }
}

variable "starts_at" {
  description = "When the event opens, as a local date-time without a zone (`2027-03-14T10:00:00`), evaluated in starts_at_timezone. Seeds the one-time schedule so a scheduled event opens without an operator logging in first. Empty leaves the schedule disabled at a placeholder instant that is never the real value, so an accidental enable cannot fire an open. The dashboard owns the time from here; Terraform only seeds it."
  type        = string
  default     = ""

  validation {
    condition     = var.starts_at == "" || can(regex("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$", var.starts_at))
    error_message = "starts_at must be empty or a local date-time with no zone or offset, e.g. 2027-03-14T10:00:00 -- the zone is starts_at_timezone."
  }
}

variable "starts_at_timezone" {
  description = "IANA zone the start time is evaluated in (`America/New_York`). The operator picks the zone their event opens in, and EventBridge Scheduler evaluates the expression in it, so a daylight-saving change between now and then does not move the opening."
  type        = string
  default     = "UTC"

  validation {
    condition     = can(regex("^(UTC|[A-Za-z]+/[A-Za-z_+-]+)$", var.starts_at_timezone))
    error_message = "starts_at_timezone must be UTC or an IANA zone name such as America/New_York."
  }
}
