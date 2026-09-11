# Asserts the edge gate (issue #71) is associated with the protected
# behaviour only. This is what stands between the design and a
# distribution-wide Function invocation bill — CloudFront Functions bill per
# invocation, and a distribution-wide association would bill every /status
# poll from every waiter, dominating the cost model at a million waiters
# (ADR-0021 §4.3).
#
# mock_provider avoids needing real AWS credentials in CI: nothing here reads
# live state, it only inspects the plan this module would produce.
#
# Run with: terraform -chdir=infra/modules/edge test

mock_provider "aws" {}

variables {
  name_prefix             = "test"
  api_gateway_domain_name = "api.example.com"
  env                     = "test"
  demo_origin_domain_name = "demo.s3.example.com"
  gate_kvs_arn            = "arn:aws:cloudfront::123456789012:key-value-store/test"
  event_id                = "smoke"
}

run "default_behaviour_carries_the_gate" {
  command = plan

  assert {
    condition = anytrue([
      for fa in aws_cloudfront_distribution.this.default_cache_behavior[0].function_association :
      fa.event_type == "viewer-request"
    ])
    error_message = "the protected default behaviour must associate the gate at viewer-request"
  }
}

run "no_other_behaviour_carries_a_function_association" {
  command = plan

  assert {
    condition = alltrue([
      for b in aws_cloudfront_distribution.this.ordered_cache_behavior : length(b.function_association) == 0
    ])
    error_message = "only the protected default behaviour may carry the gate — a function_association on any ordered_cache_behavior would bill every request on that path per invocation"
  }
}
