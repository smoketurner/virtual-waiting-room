# modules/edge - CloudFront distribution with three cache behaviours (ADR-0013).
#
#   Polled   (/v1/status, /v1/queue_num)
#            Min TTL > 0, zero cookies forwarded -> CloudFront collapses
#            concurrent misses into one origin fetch, so origin load is
#            independent of waiter count (C4). Cache keys differ per endpoint,
#            so the two split across cache policies keyed by path and by
#            path + event_id + request_id.
#   Write    (/v1/join, /v1/generate_token) - uncached (managed CachingDisabled).
#   Default  (/*) - the protected origin: uncached, session cookie forwarded.
#
# Public paths are path-versioned under /v1; the API origin's origin_path is the
# stage (= env), so /v1/status is forwarded to /<env>/v1/status. WAF and standby
# activation are added to this module in later steps.

# --- Cache policies (polled behaviours) --------------------------------------

resource "aws_cloudfront_cache_policy" "polled_status" {
  name        = "${var.name_prefix}-polled-status"
  comment     = "Polled /status: cache key is path only, no cookies (ADR-0013)."
  min_ttl     = var.polled_min_ttl_seconds
  default_ttl = local.polled_default_ttl
  max_ttl     = local.polled_default_ttl

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true
  }
}

resource "aws_cloudfront_cache_policy" "polled_keyed" {
  name        = "${var.name_prefix}-polled-keyed"
  comment     = "Polled /queue_num, /queue_pos_expiry: cache key adds event_id + request_id, no cookies."
  min_ttl     = var.polled_min_ttl_seconds
  default_ttl = local.polled_default_ttl
  max_ttl     = local.polled_default_ttl

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "whitelist"
      query_strings {
        items = ["event_id", "request_id"]
      }
    }
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true
  }
}

# --- Origin request policy (protected default behaviour) ----------------------
# Forwards the session cookie to the client origin so the authorizer can read
# it. This is the ONLY behaviour that forwards a cookie - doing so on a polled
# behaviour would disable request collapsing (ADR-0013).

resource "aws_cloudfront_origin_request_policy" "protected" {
  name    = "${var.name_prefix}-protected"
  comment = "Protected origin: forward the session cookie and all viewer headers/query strings."

  cookies_config {
    cookie_behavior = "whitelist"
    cookies {
      items = [var.session_cookie_name]
    }
  }
  headers_config {
    header_behavior = "allViewer"
  }
  query_strings_config {
    query_string_behavior = "all"
  }
}

# --- Edge gate (issue #71) -----------------------------------------------
# A CloudFront Function at viewer-request, decides locally from the
# KeyValueStore (gate_kvs_arn) — no compute in the request path beyond the
# function's own sub-millisecond budget, and it works against an origin we
# cannot run code in. Replaces the CloudFront trusted-key-group gate
# (ADR-0020, retired) with a decision point that can express #58's mechanism,
# #60's dormancy, #66's rules, and #72/#73's refusal shaping.
resource "aws_cloudfront_function" "gate" {
  name    = "${var.name_prefix}-gate"
  comment = "Virtual Waiting Room admission gate (issue #71)."
  runtime = "cloudfront-js-2.0"
  publish = true
  code    = local.gate_js_source

  key_value_store_associations = [var.gate_kvs_arn]

  tags = var.tags

  # A tags-only change to this resource fails apply with a 412
  # PreconditionFailed on PublishFunction, because the Update handler reuses a
  # stale ETag when tags are the only diff
  # (hashicorp/terraform-provider-aws#49572). Omitting `tags` does not avoid
  # it: the provider sets default_tags, so this function is tagged either way
  # and a change to those tags still produces the tags-only diff. Ignoring both
  # attributes is what actually prevents it — the tags applied at create stay
  # put, and a later tag change is a no-op here rather than a failed apply.
  lifecycle {
    ignore_changes = [tags, tags_all]

    # The ceiling is not adjustable and CloudFront rejects the publish, so
    # fail here rather than at apply.
    precondition {
      condition     = length(local.gate_js_source) <= 10240
      error_message = "gate.js is ${length(local.gate_js_source)} bytes, over CloudFront's 10,240-byte function limit."
    }
  }
}

# The function source is entirely ASCII, so length() is also its byte count
# against CloudFront's 10,240-byte ceiling. A check block warns rather than
# blocks, which is what the headroom signal wants: the hard stop is the
# resource's own precondition below. Both exist because a single assertion at
# the ceiling gives no notice — the budget went from 16% headroom to over the
# limit in one round of fixes, with nothing said until it was already broken.
check "gate_function_headroom" {
  assert {
    condition     = length(local.gate_js_source) < 9000
    error_message = "gate.js is ${length(local.gate_js_source)} bytes of the 10,240 CloudFront allows. Past 9,000 there is not enough headroom left for a routine change; trim it before adding more."
  }
}

# --- Distribution -------------------------------------------------------------

resource "aws_cloudfront_distribution" "this" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = local.demo_root_object
  comment             = "Virtual Waiting Room - ${var.name_prefix}"
  price_class         = var.price_class

  # Origin 1: the core REST API (polled + write behaviours). origin_path is the
  # stage (= env), so a viewer request for /v1/status is forwarded to
  # /<env>/v1/status where the deployed, path-versioned methods live.
  origin {
    origin_id   = local.api_origin_id
    domain_name = var.api_gateway_domain_name
    origin_path = "/${var.env}"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  # Origin 2: the client's protected origin (default behaviour). Absent when no
  # customer origin is configured — the demo origin below stands in.
  dynamic "origin" {
    for_each = local.use_demo_origin ? [] : [1]
    content {
      origin_id   = local.client_origin_id
      domain_name = var.client_origin_domain_name

      custom_origin_config {
        http_port              = 80
        https_port             = 443
        origin_protocol_policy = "https-only"
        origin_ssl_protocols   = ["TLSv1.2"]
      }
    }
  }

  # Origin 2a: the demo fixture standing in for a customer origin.
  dynamic "origin" {
    for_each = local.use_demo_origin ? [1] : []
    content {
      origin_id                = local.demo_origin_id
      domain_name              = var.demo_origin_domain_name
      origin_access_control_id = var.demo_origin_access_control_id
    }
  }

  # Origin 3: the waiting room's own pages. Private bucket, read only by this
  # distribution through the origin access control.
  origin {
    origin_id                = local.waiting_origin_id
    domain_name              = aws_s3_bucket.waiting.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.waiting.id
  }

  # Default behaviour: the protected origin. Uncached, session cookie forwarded.
  default_cache_behavior {
    target_origin_id         = local.protected_origin_id
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = local.caching_disabled_policy_id
    origin_request_policy_id = local.protected_origin_request_policy
    compress                 = true

    # The gate (issue #71). Associated with this behaviour only, never
    # distribution-wide: Functions bill per invocation, and a
    # distribution-wide association would bill every /status poll from every
    # waiter, which dominates the cost model at a million waiters. Every
    # other behaviour below has its own ordered_cache_behavior, so this is
    # correct by construction — asserted by tests/gate_association.tftest.hcl.
    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.gate.arn
    }
  }

  # The waiting room's pages. Deliberately outside the gate: this is what a
  # refused visitor is shown.
  ordered_cache_behavior {
    path_pattern           = local.waiting_path_pattern
    target_origin_id       = local.waiting_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.waiting.id
    compress               = true
  }

  # Polled: /status (path-only cache key).
  ordered_cache_behavior {
    path_pattern           = local.polled_status_path
    target_origin_id       = local.api_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.polled_status.id
    compress               = true
  }

  # Polled: /queue_num, /queue_pos_expiry (cache key adds event_id + request_id).
  dynamic "ordered_cache_behavior" {
    for_each = toset(local.polled_keyed_paths)
    content {
      path_pattern           = ordered_cache_behavior.value
      target_origin_id       = local.api_origin_id
      viewer_protocol_policy = "redirect-to-https"
      allowed_methods        = ["GET", "HEAD", "OPTIONS"]
      cached_methods         = ["GET", "HEAD"]
      cache_policy_id        = aws_cloudfront_cache_policy.polled_keyed.id
      compress               = true
    }
  }

  # Write: /join, /generate_token (uncached).
  #
  # The origin request policy is load-bearing, not tidiness: a behaviour that
  # forwards no cookies has its Set-Cookie response headers STRIPPED by
  # CloudFront before they reach the viewer. /generate_token's whole job is to
  # return the admission cookies, so without this the visitor is admitted,
  # receives nothing, and waits forever. AllViewerExceptHostHeader forwards
  # cookies and drops Host, which API Gateway rejects if forwarded.
  dynamic "ordered_cache_behavior" {
    for_each = toset(local.write_paths)
    content {
      path_pattern             = ordered_cache_behavior.value
      target_origin_id         = local.api_origin_id
      viewer_protocol_policy   = "redirect-to-https"
      allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
      cached_methods           = ["GET", "HEAD"]
      cache_policy_id          = local.caching_disabled_policy_id
      origin_request_policy_id = local.all_viewer_except_host_policy_id
      compress                 = true
    }
  }

  # Admin control plane: /admin, /admin/*, /static/* (ADR-0016). Uncached, all
  # methods, forward everything except Host so the OIDC session cookie + callback
  # query reach the admin Lambda. Access is gated by the admin Lambda's OIDC
  # session, not at the edge.
  dynamic "ordered_cache_behavior" {
    for_each = toset(local.admin_paths)
    content {
      path_pattern             = ordered_cache_behavior.value
      target_origin_id         = local.api_origin_id
      viewer_protocol_policy   = "redirect-to-https"
      allowed_methods          = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
      cached_methods           = ["GET", "HEAD"]
      cache_policy_id          = local.caching_disabled_policy_id
      origin_request_policy_id = local.all_viewer_except_host_policy_id
      compress                 = true
    }
  }

  # No custom_error_response (issue #71, closes #64): the gate itself decides
  # a refusal's shape — a 302 to the waiting page for navigation, 403 JSON for
  # XHR (#72) — so origin 403s reach the visitor unrewritten instead of being
  # mapped to the waiting page regardless of which origin produced them.

  restrictions {
    geo_restriction {
      restriction_type = "none"
      locations        = []
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = var.tags
}
