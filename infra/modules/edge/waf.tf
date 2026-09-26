# --- WAF web ACL (ADR-0038) ---------------------------------------------------
#
# Makes a place in the queue cost the client something. Without it a script
# mints request ids for free, and the pre-queue shuffle turns that volume
# straight into a share of the front (ADR-0001).
#
#   1. A WAF token is required on every JavaScript API path. A request without
#      a valid token gets a 202 carrying x-amzn-waf-action: challenge, and
#      waiting.js navigates to /_wr/verify.html, which is answered with the
#      Challenge action: a silent interstitial, once per immunity period, that
#      sets the token and sends the visitor back. The challenge is on that page
#      and not on waiting.html, because waiting.html also serves the
#      no-JavaScript queue's form, and a browser without JavaScript cannot pass
#      an interstitial.
#   2. Rate limits keyed on that token cap what one challenge solve buys: a
#      handful of joins, and a poll rate no honest page reaches.
#   3. A per-IP limit on joins and page loads escalates to CAPTCHA instead of
#      blocking, because a mobile carrier's NAT puts a crowd of real buyers
#      behind one address.
#   4. The no-JavaScript queue (ADR-0037) cannot run a challenge, so it gets a
#      strict per-IP limit, anonymous-network blocking, and a switch to close
#      it for an event.
#   5. AWS managed rules (Anti-DDoS, IP reputation, Bot Control common,
#      anonymous IPs) run in Count until an event's worth of data says
#      otherwise (O5, ADR-0012).
#
# Opt-in. The web ACL is priced into a CloudFront flat-rate plan, which is how
# it is meant to be run; on pay-as-you-go it bills a monthly fee per ACL and
# per rule between events, which N1 does not allow. The plan also requires a
# web ACL, so turning this on is the first step of subscribing.
#
# Only rules the flat-rate plans accept are used: no rule groups of our own,
# no Targeted Bot Control, no ATP/ACFP. The Business plan's 50-rule allowance
# covers everything here.

locals {
  waf_enabled = var.waf_enabled

  # The token WAF issues after a challenge or CAPTCHA, as a first-party cookie
  # on the viewer's host. Rate limits keyed on it bound what one solve buys.
  waf_token_cookie = "aws-waf-token"

  # Paths waiting.js calls. Listed rather than matched by prefix because the
  # no-JavaScript paths share /v1/ and must not be challenged; a new JS endpoint
  # has to be added here to be covered, which the terraform test checks.
  waf_js_api_regex = "^/v1/(join|status|queue_num|generate_token)$"

  # Where waiting.js sends a browser to earn a token. Only a JavaScript client
  # ever navigates here, so challenging it cannot lock out a no-JS visitor.
  waf_verify_path = "/_wr/verify.html"

  # The two requests a visitor makes to take a place: earning a token and
  # posting the join. Counted together per IP, so a CAPTCHA triggered by the
  # join is shown on the verify page waiting.js navigates to in response.
  waf_join_regex = "^(/v1/join|${replace(local.waf_verify_path, ".", "\\.")})$"

  waf_nojs_regex = "^/v1/(enter|wait)$"
  waf_nojs_join  = "/v1/enter"

  # Everything the waiting room itself serves. Managed rules are scoped to it
  # so they never change what the customer's origin sees.
  waf_room_regex = "^/(v1|_wr)/"

  waf_anonymous_ip_namespace = "awswaf:managed:aws:anonymous-ip-list:"

  waf_enforce_managed = var.waf_managed_rules_mode == "enforce"
}

resource "aws_wafv2_web_acl" "this" {
  count = local.waf_enabled ? 1 : 0

  # A CLOUDFRONT-scoped web ACL lives in us-east-1 whatever region the rest of
  # the stack is in.
  region      = "us-east-1"
  name        = "${var.name_prefix}-edge"
  description = "Virtual Waiting Room edge protection (ADR-0038)."
  scope       = "CLOUDFRONT"

  default_action {
    allow {}
  }

  # How long a solved challenge or CAPTCHA is honoured. Long enough that a
  # visitor waiting through a whole event is not re-challenged mid-queue; a
  # lapse costs one silent page reload, not their place.
  challenge_config {
    immunity_time_property {
      immunity_time = var.waf_token_immunity_seconds
    }
  }

  captcha_config {
    immunity_time_property {
      immunity_time = var.waf_token_immunity_seconds
    }
  }

  # A rate-limited join is a failed attempt to waiting.js, which backs off; a
  # 429 says so without looking like the ingest broke.
  custom_response_body {
    key          = "rate_limited_json"
    content_type = "APPLICATION_JSON"
    content      = jsonencode({ message = "too many requests; retry later" })
  }

  # The no-JavaScript queue is a form post in a browser tab, so its refusal is
  # a page a person reads.
  custom_response_body {
    key          = "nojs_refused_html"
    content_type = "TEXT_HTML"
    content      = "<!doctype html><title>Please try again</title><p>We could not add you to the queue from this connection. Please try again in a few minutes, or open this page with JavaScript enabled.</p>"
  }

  # --- AWS managed rules (Count until promoted, O5) --------------------------

  # Anti-DDoS first, as AWS recommends, so it sees traffic before anything
  # else terminates it. Its own challenge is withheld from the API paths: a
  # fetch() cannot run an interstitial, and those paths carry tokens anyway.
  rule {
    name     = "anti-ddos"
    priority = 0

    override_action {
      dynamic "none" {
        for_each = local.waf_enforce_managed ? [1] : []
        content {}
      }
      dynamic "count" {
        for_each = local.waf_enforce_managed ? [] : [1]
        content {}
      }
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesAntiDDoSRuleSet"

        managed_rule_group_configs {
          aws_managed_rules_anti_ddos_rule_set {
            sensitivity_to_block = "LOW"

            client_side_action_config {
              challenge {
                usage_of_action = "ENABLED"

                exempt_uri_regular_expression {
                  regex_string = "^/(v1|admin|static)/"
                }
              }
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-anti-ddos"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "ip-reputation"
    priority = 1

    override_action {
      dynamic "none" {
        for_each = local.waf_enforce_managed ? [1] : []
        content {}
      }
      dynamic "count" {
        for_each = local.waf_enforce_managed ? [] : [1]
        content {}
      }
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesAmazonIpReputationList"

        scope_down_statement {
          regex_match_statement {
            regex_string = local.waf_room_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-ip-reputation"
      sampled_requests_enabled   = true
    }
  }

  # Always Count: this group only labels anonymous and hosting-provider
  # addresses. The rules further down decide what a label costs on each path,
  # because a VPN user on the JS path can solve a CAPTCHA and a no-JS visitor
  # cannot.
  rule {
    name     = "anonymous-ip-labels"
    priority = 2

    override_action {
      count {}
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesAnonymousIpList"

        scope_down_statement {
          regex_match_statement {
            regex_string = local.waf_room_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-anonymous-ip-labels"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "bot-control-common"
    priority = 3

    override_action {
      dynamic "none" {
        for_each = local.waf_enforce_managed ? [1] : []
        content {}
      }
      dynamic "count" {
        for_each = local.waf_enforce_managed ? [] : [1]
        content {}
      }
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesBotControlRuleSet"

        managed_rule_group_configs {
          aws_managed_rules_bot_control_rule_set {
            inspection_level = "COMMON"
          }
        }

        scope_down_statement {
          regex_match_statement {
            regex_string = local.waf_room_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-bot-control-common"
      sampled_requests_enabled   = true
    }
  }

  # --- The token: a challenge solved per browser -----------------------------

  # A navigation is where a browser can run the challenge interstitial, and
  # the verify page is the one navigation only JavaScript clients make, so this
  # is where a visitor gets the token every API path below requires.
  rule {
    name     = "verify-page-challenge"
    priority = 10

    action {
      challenge {}
    }

    statement {
      byte_match_statement {
        search_string         = local.waf_verify_path
        positional_constraint = "EXACTLY"
        field_to_match {
          uri_path {}
        }
        text_transformation {
          priority = 0
          type     = "NONE"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-verify-page-challenge"
      sampled_requests_enabled   = true
    }
  }

  # Every call waiting.js makes carries the token or is refused with a 202
  # challenge response. This is what closes the join to a script that never
  # loads the page.
  rule {
    name     = "api-requires-token"
    priority = 11

    action {
      challenge {}
    }

    statement {
      regex_match_statement {
        regex_string = local.waf_js_api_regex
        field_to_match {
          uri_path {}
        }
        text_transformation {
          priority = 0
          type     = "NONE"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-api-requires-token"
      sampled_requests_enabled   = true
    }
  }

  # --- What one token buys ---------------------------------------------------

  # An honest page joins once and retries a failed join a few times. WAF's
  # floor for a rate limit is 10, so over its longest window this is roughly
  # one place a minute per solve; a script that wants more places pays for
  # more challenges.
  rule {
    name     = "join-per-token"
    priority = 20

    action {
      block {
        custom_response {
          response_code            = 429
          custom_response_body_key = "rate_limited_json"
        }
      }
    }

    statement {
      rate_based_statement {
        limit                 = var.waf_join_token_limit
        evaluation_window_sec = 600
        aggregate_key_type    = "CUSTOM_KEYS"

        custom_key {
          cookie {
            name = local.waf_token_cookie
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }

        scope_down_statement {
          byte_match_statement {
            search_string         = "/v1/join"
            positional_constraint = "EXACTLY"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-join-per-token"
      sampled_requests_enabled   = true
    }
  }

  # A ceiling on the whole JS API per token. The page's own floor is one poll
  # every few seconds; this is well above it and far below a script hammering
  # /v1/queue_num with invented ids to force origin misses.
  rule {
    name     = "api-per-token"
    priority = 21

    action {
      block {
        custom_response {
          response_code            = 429
          custom_response_body_key = "rate_limited_json"
        }
      }
    }

    statement {
      rate_based_statement {
        limit                 = var.waf_api_token_limit
        evaluation_window_sec = 300
        aggregate_key_type    = "CUSTOM_KEYS"

        custom_key {
          cookie {
            name = local.waf_token_cookie
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }

        scope_down_statement {
          regex_match_statement {
            regex_string = local.waf_js_api_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-api-per-token"
      sampled_requests_enabled   = true
    }
  }

  # --- Per address: escalate, don't lock out ---------------------------------

  # Token requests and joins from one address. Past the limit each visitor on
  # it solves a CAPTCHA once, then passes: a carrier NAT full of real buyers gets
  # friction, a farm on one address pays per place.
  rule {
    name     = "join-per-ip"
    priority = 30

    action {
      captcha {}
    }

    statement {
      rate_based_statement {
        limit                 = var.waf_join_ip_limit
        evaluation_window_sec = 300
        aggregate_key_type    = "IP"

        scope_down_statement {
          regex_match_statement {
            regex_string = local.waf_join_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-join-per-ip"
      sampled_requests_enabled   = true
    }
  }

  # Anonymising networks and hosting providers are where scripted joins come
  # from. On the JS path they solve a CAPTCHA once; Count until promoted, like
  # the managed rules whose labels it reads.
  rule {
    name     = "anonymous-ip-join-captcha"
    priority = 31

    action {
      dynamic "captcha" {
        for_each = local.waf_enforce_managed ? [1] : []
        content {}
      }
      dynamic "count" {
        for_each = local.waf_enforce_managed ? [] : [1]
        content {}
      }
    }

    statement {
      and_statement {
        statement {
          label_match_statement {
            scope = "NAMESPACE"
            key   = local.waf_anonymous_ip_namespace
          }
        }
        statement {
          regex_match_statement {
            regex_string = local.waf_join_regex
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-anonymous-ip-join-captcha"
      sampled_requests_enabled   = true
    }
  }

  # --- The no-JavaScript queue (ADR-0037) -------------------------------------

  # Closes the no-JS queue for an event where it is being abused. Its visitors
  # cannot queue while this is on; that is the trade the switch makes.
  dynamic "rule" {
    for_each = var.nojs_enabled ? [] : [1]
    content {
      name     = "nojs-closed"
      priority = 40

      action {
        block {
          custom_response {
            response_code            = 403
            custom_response_body_key = "nojs_refused_html"
          }
        }
      }

      statement {
        regex_match_statement {
          regex_string = local.waf_nojs_regex
          field_to_match {
            uri_path {}
          }
          text_transformation {
            priority = 0
            type     = "NONE"
          }
        }
      }

      visibility_config {
        cloudwatch_metrics_enabled = true
        metric_name                = "${var.name_prefix}-nojs-closed"
        sampled_requests_enabled   = true
      }
    }
  }

  # A no-JS visitor joins once. There is no challenge to fall back on here,
  # so the limit is tight and over it the answer is a refusal, not a CAPTCHA
  # the visitor could not run.
  rule {
    name     = "nojs-per-ip"
    priority = 41

    action {
      block {
        custom_response {
          response_code            = 429
          custom_response_body_key = "nojs_refused_html"
        }
      }
    }

    statement {
      rate_based_statement {
        limit                 = var.waf_nojs_ip_limit
        evaluation_window_sec = 300
        aggregate_key_type    = "IP"

        scope_down_statement {
          byte_match_statement {
            search_string         = local.waf_nojs_join
            positional_constraint = "EXACTLY"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-nojs-per-ip"
      sampled_requests_enabled   = true
    }
  }

  # Anonymous and hosting-provider addresses cannot join without JavaScript:
  # the JS path offers them a CAPTCHA, and this one has nothing to offer.
  rule {
    name     = "nojs-anonymous-ip"
    priority = 42

    action {
      dynamic "block" {
        for_each = local.waf_enforce_managed ? [1] : []
        content {
          custom_response {
            response_code            = 403
            custom_response_body_key = "nojs_refused_html"
          }
        }
      }
      dynamic "count" {
        for_each = local.waf_enforce_managed ? [] : [1]
        content {}
      }
    }

    statement {
      and_statement {
        statement {
          label_match_statement {
            scope = "NAMESPACE"
            key   = local.waf_anonymous_ip_namespace
          }
        }
        statement {
          byte_match_statement {
            search_string         = local.waf_nojs_join
            positional_constraint = "EXACTLY"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "NONE"
            }
          }
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.name_prefix}-nojs-anonymous-ip"
      sampled_requests_enabled   = true
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${var.name_prefix}-edge"
    sampled_requests_enabled   = true
  }

  tags = var.tags
}
