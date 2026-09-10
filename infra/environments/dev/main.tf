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
  controller_artifact_path      = var.controller_artifact_path
  # Schedule the controller whenever a real one is deployed, the same way the
  # join event-source mapping follows assign_position. A deployed controller that
  # nothing fires means the queue forms and never drains, and the schedule costs
  # a GetItem every ten seconds while the event is idle.
  enable_controller   = var.controller_artifact_path != ""
  lambda_architecture = var.lambda_architecture
  event_id            = var.event_id
  seal_start_time     = var.seal_start_time

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

  # Turns the gate on: CloudFront verifies admission cookies signed by this key
  # group before it will reach the protected origin.
  trusted_key_group_ids = [module.core.admission_key_group_id]
}

# authorizer. The function is the gate at the customer's protected origin: it
# answers 200 to serve a request or 302 to send the visitor to wait, and it is
# the only writer of the arrivals counters the controller measures no-shows
# against. An idle Lambda costs nothing, so it is created whenever its artifact
# is built; attaching it at the origin happens where the origin lives.
#
# Un-admitted visitors are sent to the CloudFront distribution created above,
# which is the waiting room.
module "authorizer" {
  source = "../../modules/authorizer"

  name_prefix = var.name_prefix
  tags        = local.common_tags

  signing_key_parameter_arn  = module.core.signing_key_parameter_arn
  signing_key_parameter_name = module.core.signing_key_parameter_name
  counters_table_name        = module.core.table_names.counters
  counters_table_arn         = module.core.table_arns.counters
  tokens_table_name          = module.core.table_names.tokens
  tokens_table_arn           = module.core.table_arns.tokens
  event_id                   = var.event_id
  waiting_room_url           = local.waiting_room_url
  lambda_artifact_path       = var.authorizer_artifact_path
}
