locals {
  # AWS SDK tuning applied to every Lambda in the module. Defined once and
  # merged into each function's environment so the set cannot drift between
  # functions. regional STS endpoints; the 2026 retry defaults (faster backoff
  # + a retry quota that fails fast under sustained outage); and the in-region
  # defaults mode (Lambda calls DynamoDB/SSM in the same region).
  common_lambda_env = {
    AWS_STS_REGIONAL_ENDPOINTS = "regional"
    AWS_NEW_RETRIES_2026       = "true"
    AWS_DEFAULTS_MODE          = "in-region"
  }

  # Env for the DynamoDB-using Lambdas: the common set plus the account id, which
  # lets the SDK use account-based DynamoDB endpoints. Sourced from the caller
  # identity, never hardcoded, so it is correct in any account. The
  dynamo_lambda_env = merge(local.common_lambda_env, {
    AWS_ACCOUNT_ID = data.aws_caller_identity.current.account_id
  })

  # Canonical table names. Every table is keyed by a single partition key and
  # carries no sort key (DESIGN §5.5); non-key attributes are schemaless and are
  # NOT declared here - DynamoDB only needs key attributes at create time.
  table_names = {
    counters  = "${var.name_prefix}-Counters"
    prequeue  = "${var.name_prefix}-PreQueue"
    positions = "${var.name_prefix}-Positions"
    tokens    = "${var.name_prefix}-Tokens"
  }

  assign_position_name = "${var.name_prefix}-assign-position"
  seal_event_name      = "${var.name_prefix}-seal-event"
  read_name            = "${var.name_prefix}-read"
  admin_name           = "${var.name_prefix}-admin"
  controller_name      = "${var.name_prefix}-controller"
  generate_token_name  = "${var.name_prefix}-generate-token"

  # warm_throughput is omitted from the table entirely when both units are 0, so
  # an un-warmed table stays at the on-demand cold baseline (idle default, no
  # cost) rather than pinning a floor.
  warm_throughput_enabled = var.warm_throughput_write_units > 0 || var.warm_throughput_read_units > 0

  # Every function deploys a real build. There is no placeholder fallback: a
  # stack that stands up with stub binaries looks deployed and serves nobody,
  # and every crate is built by `make build` before plan or apply runs.
  lambda_zip = {
    assign_position = var.assign_position_artifact_path
    seal_event      = var.seal_event_artifact_path
    read            = var.read_artifact_path
    admin           = var.admin_artifact_path
    controller      = var.controller_artifact_path
    generate_token  = var.generate_token_artifact_path
  }

  lambda_hash = { for name, path in local.lambda_zip : name => filebase64sha256(path) }

  lambda_runtime_arch = var.lambda_architecture
}
