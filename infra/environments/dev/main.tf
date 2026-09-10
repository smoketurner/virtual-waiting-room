# Virtual Waiting Room - dev environment.
#
# The only deployable root: `terraform apply` runs here, never inside a module.
# `terraform validate` / `plan` are the default verification; apply/destroy touch
# real AWS resources and are run only on request.

module "core" {
  source = "../../modules/core"

  name_prefix = var.name_prefix
  tags        = local.common_tags

  env                         = var.env
  warm_throughput_write_units = var.warm_throughput_write_units
  warm_throughput_read_units  = var.warm_throughput_read_units

  # Rust Lambda artifacts. Empty = vendored placeholder; set these to the built
  # bootstrap zips to deploy the real functions and enable the join ESM.
  assign_position_artifact_path = var.assign_position_artifact_path
  seal_event_artifact_path      = var.seal_event_artifact_path
  read_artifact_path            = var.read_artifact_path
  admin_artifact_path           = var.admin_artifact_path
  lambda_architecture           = var.lambda_architecture
  event_id                      = var.event_id
  seal_start_time               = var.seal_start_time

  # Admin OIDC login (ADR-0016). Secret is an SSM SecureString written out of band.
  oidc_issuer         = var.oidc_issuer
  oidc_client_id      = var.oidc_client_id
  oidc_redirect_uri   = var.oidc_redirect_uri
  oidc_allowed_emails = var.oidc_allowed_emails
}

# edge (CloudFront). Always created: client_origin_domain_name is a required
# variable (the customer's own site is the default-behaviour origin), so the
# edge cannot be configured without one. The API origin points at core's REST
# API host, with the stage as origin_path so /status is forwarded to
# /<env>/status. WAF is added to modules/edge behind a single enable_waf toggle
# (off by default: the WAF web ACL is the one component with a fixed monthly
# cost, breaking N1 idle).
#
# count is kept (pinned to 1) so the module stays addressed as module.edge[0] in
# state - dropping count would rename every edge resource and force a
# destroy/recreate of the live distribution.
module "edge" {
  source = "../../modules/edge"
  count  = 1

  name_prefix = var.name_prefix
  tags        = local.common_tags

  api_gateway_domain_name   = module.core.api_gateway_domain_name
  env                       = var.env
  client_origin_domain_name = var.client_origin_domain_name
}

# authorizer is wired once its Rust artifact is built (PLAN Phase 1f/2).
#
# module "authorizer" {
#   source = "../../modules/authorizer"
#
#   name_prefix                = var.name_prefix
#   tags                       = local.common_tags
#   signing_key_parameter_arn  = module.core.signing_key_parameter_arn
#   lambda_artifact_path       = "../../../target/lambda/authorizer/bootstrap.zip"
# }
