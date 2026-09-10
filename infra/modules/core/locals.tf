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
  # api_placeholder makes no DynamoDB calls and uses common_lambda_env instead.
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

  # Per-function artifact resolution. An empty path falls back to the vendored
  # placeholder zip; a real path is a cargo-lambda zip referenced directly. The
  # join event-source mapping is enabled only when assign_position is real.
  assign_position_is_placeholder = var.assign_position_artifact_path == ""
  seal_event_is_placeholder      = var.seal_event_artifact_path == ""
  read_is_placeholder            = var.read_artifact_path == ""
  admin_is_placeholder           = var.admin_artifact_path == ""
  controller_is_placeholder      = var.controller_artifact_path == ""
  generate_token_is_placeholder  = var.generate_token_artifact_path == ""

  assign_position_zip = local.assign_position_is_placeholder ? data.archive_file.placeholder[0].output_path : var.assign_position_artifact_path
  seal_event_zip      = local.seal_event_is_placeholder ? data.archive_file.placeholder[0].output_path : var.seal_event_artifact_path
  read_zip            = local.read_is_placeholder ? data.archive_file.placeholder[0].output_path : var.read_artifact_path
  admin_zip           = local.admin_is_placeholder ? data.archive_file.placeholder[0].output_path : var.admin_artifact_path
  controller_zip      = local.controller_is_placeholder ? data.archive_file.placeholder[0].output_path : var.controller_artifact_path
  generate_token_zip  = local.generate_token_is_placeholder ? data.archive_file.placeholder[0].output_path : var.generate_token_artifact_path

  assign_position_hash = local.assign_position_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.assign_position_artifact_path)
  seal_event_hash      = local.seal_event_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.seal_event_artifact_path)
  read_hash            = local.read_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.read_artifact_path)
  admin_hash           = local.admin_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.admin_artifact_path)
  controller_hash      = local.controller_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.controller_artifact_path)
  generate_token_hash  = local.generate_token_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.generate_token_artifact_path)

  lambda_runtime_arch = var.lambda_architecture
}
