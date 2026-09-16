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

  # Rust Lambda artifacts, built by `make build` to a fixed path per crate.
  assign_position_artifact_path = local.artifact["assign_position"]
  open_event_artifact_path      = local.artifact["open_event"]
  read_artifact_path            = local.artifact["read"]
  admin_artifact_path           = local.artifact["admin"]
  controller_artifact_path      = local.artifact["controller"]
  generate_token_artifact_path  = local.artifact["generate_token"]
  lambda_architecture           = var.lambda_architecture
  event_id                      = var.event_id

  # Admin OIDC login (ADR-0016). Secret is an SSM SecureString written out of band.
  oidc_issuer         = var.oidc_issuer
  oidc_client_id      = var.oidc_client_id
  oidc_redirect_uri   = var.oidc_redirect_uri
  oidc_allowed_emails = var.oidc_allowed_emails

  # Adaptive poll policy (#69), published on /status.
  poll_floor_ms   = var.poll_floor_ms
  poll_ceiling_ms = var.poll_ceiling_ms
  poll_divisor    = var.poll_divisor


  # Shared with module.edge below so generate_token and the gate's CloudFront
  # Function cannot drift onto different cookie names (issue #71).
  session_cookie_name = var.session_cookie_name

  # Seeded at apply so the stack serves without an operator driving a script or
  # the dashboard first.
  admission_rate     = var.admission_rate
  gate_rules         = var.gate_rules
  starts_at          = var.starts_at
  starts_at_timezone = var.starts_at_timezone
}

# demo-origin: a stand-in for the customer's protected origin. This is the dev
# root, which is where the gate gets exercised, so the fixture is always built —
# an S3 bucket holding one page costs nothing. A production root does not
# include this module and points client_origin_domain_name at the real origin.
module "demo_origin" {
  source = "../../modules/demo-origin"

  name_prefix = var.name_prefix
  tags        = local.common_tags
}

# edge (CloudFront). client_origin_domain_name is a required variable (the
# customer's own site is the default-behaviour origin), so the edge cannot be
# configured without one and is never optional. The API origin points at core's
# REST API host, with the stage as origin_path so /status is forwarded to
# /<env>/status.
module "edge" {
  source = "../../modules/edge"

  name_prefix = var.name_prefix
  tags        = local.common_tags

  api_gateway_domain_name   = module.core.api_gateway_domain_name
  env                       = var.env
  client_origin_domain_name = var.client_origin_domain_name

  # Used as the protected origin when no customer origin is configured.
  demo_origin_domain_name       = module.demo_origin.bucket_regional_domain_name
  demo_origin_access_control_id = module.demo_origin.origin_access_control_id

  # The edge gate (issue #71): the CloudFront Function reads its config and
  # signing secret from this store, and event_id is templated into its source.
  gate_kvs_arn        = module.core.gate_kvs_arn
  event_id            = var.event_id
  session_cookie_name = var.session_cookie_name
}
