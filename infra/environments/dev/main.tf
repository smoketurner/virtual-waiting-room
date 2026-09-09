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
  lambda_architecture           = var.lambda_architecture
  event_id                      = var.event_id
  seal_start_time               = var.seal_start_time
}

# edge (CloudFront). Created once a client origin is supplied - the origin is the
# customer's own site, so there is no sensible default and edge cannot exist
# without it. The API origin points at core's REST API host, with the stage as
# origin_path so /status is forwarded to /<env>/status. WAF is added to
# modules/edge behind a single enable_waf toggle (off by default: the WAF web
# ACL is the one component with a fixed monthly cost, breaking N1 idle).
module "edge" {
  source = "../../modules/edge"
  count  = var.client_origin_domain_name != "" ? 1 : 0

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
