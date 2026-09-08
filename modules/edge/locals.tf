locals {
  # Origin identifiers used to bind cache behaviours to origins.
  api_origin_id    = "${var.name_prefix}-api"
  client_origin_id = "${var.name_prefix}-origin"

  # AWS-managed cache policy "CachingDisabled" - the blessed way to make a
  # behaviour uncached. Used for the write behaviours and the protected default.
  # https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/using-managed-cache-policies.html
  caching_disabled_policy_id = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"

  # Polled behaviour path patterns (viewer-facing, path-versioned under /v1) and
  # the cache policy each uses. Grouped by cache key (DESIGN §8):
  #   status  - key is path only
  #   keyed   - key adds event_id + request_id (per-visitor answers)
  #   pubkey  - key adds event_id only
  polled_status_path = "/v1/status"
  polled_keyed_paths = ["/v1/queue_num", "/v1/queue_pos_expiry"]
  polled_pubkey_path = "/v1/public_key"

  # Uncached write behaviour path patterns: ingest and token minting.
  write_paths = ["/v1/join", "/v1/generate_token"]

  # Polled default TTL tracks the min TTL: /status carries phase and serving
  # position, so it must stay fresh. Not a separate knob - min TTL is the one
  # load-bearing value (ADR-0013).
  polled_default_ttl = var.polled_min_ttl_seconds
}
