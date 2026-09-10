# modules/edge - CloudFront distribution with three cache behaviours (ADR-0013).
#
#   Polled   (/v1/status, /v1/queue_num, /v1/queue_pos_expiry, /v1/public_key)
#            Min TTL > 0, zero cookies forwarded -> CloudFront collapses
#            concurrent misses into one origin fetch, so origin load is
#            independent of waiter count (C4). Cache keys differ per endpoint
#            (DESIGN §8), so the polled endpoints split across three cache
#            policies keyed by path / +event_id+request_id / +event_id.
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

resource "aws_cloudfront_cache_policy" "polled_pubkey" {
  name        = "${var.name_prefix}-polled-pubkey"
  comment     = "Polled /public_key: cache key adds event_id, no cookies."
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
        items = ["event_id"]
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

# --- Distribution -------------------------------------------------------------

resource "aws_cloudfront_distribution" "this" {
  enabled         = true
  is_ipv6_enabled = true
  comment         = "Virtual Waiting Room - ${var.name_prefix}"
  price_class     = var.price_class

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

  # Origin 2: the client's protected origin (default behaviour). A plain custom
  # origin today; swapped for a vpc_origin_config when the authorizer module
  # enables the CloudFront VPC origin (var.enable_vpc).
  origin {
    origin_id   = local.client_origin_id
    domain_name = var.client_origin_domain_name

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
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
    target_origin_id         = local.client_origin_id
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods           = ["GET", "HEAD"]
    cache_policy_id          = local.caching_disabled_policy_id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.protected.id
    compress                 = true

    # The gate. CloudFront verifies the admission cookies at the edge and
    # refuses anyone without them, so no compute sits in the request path and
    # the origin never sees an un-admitted visitor. Refusals are 403s, mapped to
    # the waiting page by custom_error_response below.
    trusted_key_groups = var.trusted_key_group_ids
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

  # Polled: /public_key (cache key adds event_id).
  ordered_cache_behavior {
    path_pattern           = local.polled_pubkey_path
    target_origin_id       = local.api_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.polled_pubkey.id
    compress               = true
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

  # A refused visitor is shown the waiting page rather than CloudFront's error.
  # The 200 is deliberate: the page is the correct answer to "you are not
  # admitted yet", and a 403 body would keep browsers from rendering it as a
  # normal page.
  #
  # error_caching_min_ttl must stay 0. CloudFront caches its own error responses,
  # and a cached refusal would keep showing the waiting page to a visitor who has
  # since been admitted.
  custom_error_response {
    error_code            = 403
    response_code         = 200
    response_page_path    = local.waiting_page_path
    error_caching_min_ttl = 0
  }

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
