# Asserts the edge web ACL (ADR-0038) is wired the way the design depends on.
#
# The ACL is opt-in because it bills monthly on pay-as-you-go (N1), so the
# default must create nothing. When it is on, three things fail silently if
# they drift and are checked here instead:
#
#   - the distribution must actually carry it; a web ACL that exists but is
#     not associated protects nothing and still bills;
#   - every JavaScript API path the waiting page calls must require a WAF
#     token, or a script joins through the one left out;
#   - the no-JavaScript paths must never be challenged, because a visitor
#     without JavaScript cannot solve one and would be locked out of the queue
#     they exist for (ADR-0037).
#
# Run with: terraform -chdir=infra/modules/edge test

mock_provider "aws" {}

variables {
  name_prefix             = "test"
  api_gateway_domain_name = "api.example.com"
  api_origin_key          = "test-origin-key"
  env                     = "test"
  demo_origin_domain_name = "demo.s3.example.com"
  gate_kvs_arn            = "arn:aws:cloudfront::123456789012:key-value-store/test"
  event_id                = "smoke"

  readiness_parameter_name = "/test/edge/readiness"
}

run "off_by_default_creates_nothing" {
  command = plan

  assert {
    condition     = length(aws_wafv2_web_acl.this) == 0
    error_message = "the web ACL bills monthly on pay-as-you-go; it must be opt-in (N1)"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.web_acl_id == null
    error_message = "with waf_enabled off the distribution must carry no web ACL"
  }
}

run "the_api_origin_carries_the_origin_key" {
  command = plan

  assert {
    condition = anytrue([
      for o in aws_cloudfront_distribution.this.origin :
      o.domain_name == var.api_gateway_domain_name && anytrue([
        for h in o.custom_header : h.name == "x-api-key" && h.value == var.api_origin_key
      ])
    ])
    error_message = "CloudFront must send the origin key to the API, or every API method answers 403 (ADR-0038)"
  }
}

run "enabled_attaches_a_cloudfront_scoped_acl" {
  command = plan

  variables {
    waf_enabled = true
  }

  # The ARN is computed, so a plan knows it only when the mock supplies it.
  override_resource {
    target          = aws_wafv2_web_acl.this
    override_during = plan
    values = {
      arn = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/test-edge/00000000-0000-0000-0000-000000000000"
    }
  }

  assert {
    condition     = aws_wafv2_web_acl.this[0].scope == "CLOUDFRONT" && aws_wafv2_web_acl.this[0].region == "us-east-1"
    error_message = "a web ACL for CloudFront must be CLOUDFRONT-scoped and live in us-east-1"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.web_acl_id == aws_wafv2_web_acl.this[0].arn
    error_message = "the distribution must carry the web ACL it enables, or it protects nothing"
  }
}

run "every_path_the_waiting_page_calls_requires_a_token" {
  command = plan

  variables {
    waf_enabled = true
  }

  # Read from the page itself, so a new endpoint waiting.js starts calling is
  # caught here rather than left open.
  assert {
    condition = alltrue([
      for p in distinct(flatten(regexall("\"(/v1/[a-z_]+)", local.waiting_js_source))) :
      can(regex(local.waf_js_api_regex, p))
    ])
    error_message = "every /v1 path waiting.js calls must match waf_js_api_regex, or it can be called without a WAF token"
  }

  assert {
    condition     = length(distinct(flatten(regexall("\"(/v1/[a-z_]+)", local.waiting_js_source)))) >= 4
    error_message = "the page scan found fewer API paths than waiting.js is known to call; the scan itself is broken"
  }
}

run "the_no_javascript_queue_is_never_challenged" {
  command = plan

  variables {
    waf_enabled = true
  }

  assert {
    condition     = !can(regex(local.waf_js_api_regex, "/v1/enter")) && !can(regex(local.waf_js_api_regex, "/v1/wait"))
    error_message = "the no-JS paths must not require a WAF token: their visitors cannot run the challenge (ADR-0037)"
  }

  assert {
    condition = alltrue([
      for r in aws_wafv2_web_acl.this[0].rule :
      length(r.action) == 0 || length(r.action[0].challenge) == 0 || r.name == "api-requires-token" || r.name == "verify-page-challenge"
    ])
    error_message = "only the verify page and the JS API paths may carry a Challenge action"
  }

  # waiting.html carries the no-JS queue's form. A browser without JavaScript
  # cannot pass an interstitial, so no rule may challenge or CAPTCHA it.
  assert {
    condition     = !can(regex(local.waf_join_regex, local.waiting_page_path)) && !can(regex(local.waf_js_api_regex, local.waiting_page_path)) && local.waf_verify_path != local.waiting_page_path
    error_message = "the waiting page must never be challenged: it is how a visitor without JavaScript reaches the no-JS queue"
  }
}

run "closing_the_no_javascript_queue_blocks_both_paths" {
  command = plan

  variables {
    waf_enabled  = true
    nojs_enabled = false
  }

  assert {
    condition     = contains([for r in aws_wafv2_web_acl.this[0].rule : r.name], "nojs-closed")
    error_message = "nojs_enabled = false must add the rule that closes /v1/enter and /v1/wait"
  }

  assert {
    condition     = can(regex(local.waf_nojs_regex, "/v1/enter")) && can(regex(local.waf_nojs_regex, "/v1/wait"))
    error_message = "the closing rule must cover both no-JS paths"
  }
}

run "managed_rules_observe_until_promoted" {
  command = plan

  variables {
    waf_enabled = true
  }

  assert {
    condition = alltrue([
      for r in aws_wafv2_web_acl.this[0].rule :
      length(r.override_action) == 0 || length(r.override_action[0].count) == 1
    ])
    error_message = "every managed rule group must run in Count by default (O5)"
  }
}
