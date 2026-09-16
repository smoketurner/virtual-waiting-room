# Asserts a configured custom domain actually reaches the distribution.
#
# `aliases` and `acm_certificate_arn` were declared, cross-validated and even
# reduced to a `use_custom_domain` local, while the distribution hardcoded
# `cloudfront_default_certificate = true` and no alias block. Setting a custom
# domain applied cleanly, reported success and served nothing under that name.
# Nothing failed, so nothing said so.
#
# The certificate half matters as much as the alias: CloudFront refuses to serve
# a custom domain under its own default certificate, so an alias without the ACM
# switch is a distribution that fails to update rather than one that quietly
# does nothing -- and the reverse, a certificate left set with no alias, is the
# silent case again.
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

run "no_alias_serves_the_default_certificate" {
  command = plan

  assert {
    condition     = length(aws_cloudfront_distribution.this.aliases) == 0
    error_message = "with no aliases configured the distribution must claim no alternate name"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.viewer_certificate[0].cloudfront_default_certificate == true
    error_message = "with no custom domain the distribution must serve CloudFront's own certificate"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.viewer_certificate[0].acm_certificate_arn == null
    error_message = "an ACM certificate with no alias to serve is the silent half of the same mistake"
  }

  # The no-alias half of viewer_domain_name is the distribution's own
  # domain_name, which CloudFront assigns and a plan cannot know. It is the
  # branch that was always correct; the one worth pinning is the alias, below.
}

run "an_alias_is_served_under_its_own_certificate" {
  command = plan

  variables {
    aliases             = ["waiting.example.com"]
    acm_certificate_arn = "arn:aws:acm:us-east-1:123456789012:certificate/abc"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.aliases == toset(["waiting.example.com"])
    error_message = "a configured alias must reach the distribution, or the custom domain serves nothing"
  }

  assert {
    condition = (
      aws_cloudfront_distribution.this.viewer_certificate[0].acm_certificate_arn
      == "arn:aws:acm:us-east-1:123456789012:certificate/abc"
    )
    error_message = "the alias must be served under the configured ACM certificate"
  }

  # Both set at once is rejected by CloudFront, so the branch has to clear the
  # default rather than add to it.
  assert {
    condition     = aws_cloudfront_distribution.this.viewer_certificate[0].cloudfront_default_certificate == null
    error_message = "the default certificate must be off when an ACM certificate is in use"
  }

  assert {
    condition     = aws_cloudfront_distribution.this.viewer_certificate[0].ssl_support_method == "sni-only"
    error_message = "a custom domain needs an SSL support method; dedicated IP is a per-month charge nothing here asks for"
  }

  # The OIDC redirect URI and every external link are built from this. Reporting
  # the *.cloudfront.net name for a distribution serving a custom domain sends
  # an operator to configure their identity provider with the wrong host.
  assert {
    condition     = output.viewer_domain_name == "waiting.example.com"
    error_message = "the viewer-facing host must be the alias once one is configured"
  }
}
