locals {
  # Origin identifiers used to bind cache behaviours to origins.
  api_origin_id     = "${var.name_prefix}-api"
  client_origin_id  = "${var.name_prefix}-origin"
  waiting_origin_id = "${var.name_prefix}-waiting"

  # The waiting room's own pages, on their own unprotected behaviour. A visitor
  # refused by the gate is shown waiting_page_path, so it must be reachable
  # without a credential or the refusal would loop.
  waiting_path_pattern = "/_wr/*"
  waiting_page_path    = "/_wr/waiting.html"

  demo_origin_id = "${var.name_prefix}-demo-origin"

  # With no customer origin configured, the protected behaviour points at the
  # demo fixture instead, so the gate has something real to let a visitor
  # through to.
  use_demo_origin     = var.client_origin_domain_name == ""
  protected_origin_id = local.use_demo_origin ? local.demo_origin_id : local.client_origin_id

  # S3 with an origin access control signs SigV4 over the origin's host, so
  # forwarding the viewer's Host breaks the signature and every request 403s.
  # A real customer origin wants the opposite: it serves the viewer's hostname
  # and needs that Host to route. Hence one policy or none, by origin type.
  protected_origin_request_policy = local.use_demo_origin ? null : aws_cloudfront_origin_request_policy.protected.id

  # Only meaningful for the demo fixture, whose page lives at index.html. A
  # customer origin serves its own root and must not have /index.html forced
  # onto it.
  demo_root_object = local.use_demo_origin ? "index.html" : null

  # AWS-managed cache policy "CachingDisabled" - the blessed way to make a
  # behaviour uncached. Used for the write behaviours and the protected default.
  # https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/using-managed-cache-policies.html
  caching_disabled_policy_id = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"

  # Polled behaviour path patterns (viewer-facing, path-versioned under /v1) and
  # the cache policy each uses. Grouped by cache key (DESIGN §8):
  #   status  - key is path only
  #   keyed   - key adds event_id + request_id (per-visitor answers)
  polled_status_path = "/v1/status"
  polled_keyed_paths = ["/v1/queue_num"]

  # Uncached write behaviour path patterns: ingest and token minting.
  write_paths = ["/v1/join", "/v1/generate_token"]

  # Admin control plane (ADR-0016): the operator dashboard + OIDC login, plus its
  # static assets. Served by the admin Lambda; uncached, all methods, forward
  # everything to the origin. Auth is the admin Lambda's OIDC session, not the edge.
  admin_paths = ["/admin", "/admin/*", "/static/*"]

  # AWS-managed origin request policy "AllViewerExceptHostHeader" — forwards
  # query string, cookies, and viewer headers except Host (API Gateway rejects a
  # forwarded CloudFront Host). Required so the session cookie + OIDC callback
  # query reach the admin Lambda.
  all_viewer_except_host_policy_id = "b689b0a8-53d0-40ab-baf2-68738e2966ac"

  # Polled default TTL tracks the min TTL: /status carries phase and serving
  # position, so it must stay fresh. Not a separate knob - min TTL is the one
  # load-bearing value (ADR-0013).
  polled_default_ttl = var.polled_min_ttl_seconds

  # The edge gate (issue #71): event_id, session_cookie_name and the waiting
  # path are templated into the function's own source rather than carried in
  # the KeyValueStore value, so Terraform stays their single source of truth.
  gate_js_source = templatefile("${path.module}/functions/gate.js.tftpl", {
    event_id            = var.event_id
    session_cookie_name = var.session_cookie_name
    waiting_path        = local.waiting_page_path
  })
}
