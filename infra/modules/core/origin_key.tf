# --- Origin key: the REST API answers CloudFront only (ADR-0038) --------------
#
# The regional execute-api hostname is public, and without this a client can
# post to it directly and skip everything the edge does: the WAF web ACL, its
# challenge and rate limits, and the CloudFront plan's cost ceiling. Every
# method requires an API key, and the only holder of the key is the
# distribution, which attaches it to every request for the API origin as an
# origin custom header. The key is never sent to a browser, so it is not the
# public key DESIGN §8 rules out: a viewer never sees it, and one a viewer
# sends is overwritten by CloudFront before the request leaves the edge.
#
# API Gateway refuses a request with a missing or wrong key with a 403 before
# the integration runs, and does not bill it. No Lambda runs in the refusal, so
# the join path keeps having no compute in it.
#
# The key is not a secret against the account's own operators: it sits in
# Terraform state and in the distribution's configuration. It is a secret
# against the internet, which is the only thing it has to keep out.

resource "random_password" "origin_key" {
  length  = 48
  special = false
}

resource "aws_api_gateway_api_key" "origin" {
  name        = "${var.name_prefix}-cloudfront-origin"
  description = "Held only by this deployment's CloudFront distribution, which sends it as x-api-key on every API origin request (ADR-0038)."
  value       = random_password.origin_key.result

  tags = var.tags
}

# A key only authorises a stage through a usage plan. No throttle or quota is
# set here: the plan carries the key, and rate limiting lives in the edge's web
# ACL, where it can be keyed per viewer. A plan-level throttle would be one
# bucket shared by every visitor behind the distribution.
resource "aws_api_gateway_usage_plan" "origin" {
  name        = "${var.name_prefix}-cloudfront-origin"
  description = "Authorises the CloudFront origin key on this deployment's stage (ADR-0038)."

  api_stages {
    api_id = aws_api_gateway_rest_api.this.id
    stage  = aws_api_gateway_stage.this.stage_name
  }

  tags = var.tags
}

resource "aws_api_gateway_usage_plan_key" "origin" {
  key_id        = aws_api_gateway_api_key.origin.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.origin.id
}
