locals {
  # The deployment's own coordinates, resolved once from the provider rather
  # than hardcoded, so every ARN this module builds is correct in any account,
  # region, and partition.
  aws_partition  = data.aws_partition.current.partition
  aws_region     = data.aws_region.current.region
  aws_account_id = data.aws_caller_identity.current.account_id

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
    AWS_ACCOUNT_ID = local.aws_account_id
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
  open_event_name      = "${var.name_prefix}-open-event"
  read_name            = "${var.name_prefix}-read"
  admin_name           = "${var.name_prefix}-admin"
  controller_name      = "${var.name_prefix}-controller"
  generate_token_name  = "${var.name_prefix}-generate-token"
  nojs_name            = "${var.name_prefix}-nojs"

  # warm_throughput is omitted from the table entirely when both units are 0, so
  # an un-warmed table stays at the on-demand cold baseline (idle default, no
  # cost) rather than pinning a floor.
  warm_throughput_enabled = var.warm_throughput_write_units > 0 || var.warm_throughput_read_units > 0

  # Every function deploys a real build. There is no placeholder fallback: a
  # stack that stands up with stub binaries looks deployed and serves nobody,
  # and every crate is built by `make build` before plan or apply runs.
  lambda_zip = {
    assign_position = var.assign_position_artifact_path
    open_event      = var.open_event_artifact_path
    read            = var.read_artifact_path
    admin           = var.admin_artifact_path
    controller      = var.controller_artifact_path
    generate_token  = var.generate_token_artifact_path
    nojs            = var.nojs_artifact_path
  }

  lambda_hash = { for name, path in local.lambda_zip : name => filebase64sha256(path) }

  lambda_runtime_arch = var.lambda_architecture

  # Where the edge module publishes the facts the admin's readiness panel
  # (issue #70) needs about the distribution: its id, the gate function's ARN
  # and the polled /status path. Named here and handed to edge through an
  # output, because edge already depends on core (it consumes gate_kvs_arn):
  # core reading edge's distribution id directly would be a module cycle.
  # core knows only the name, edge writes the value, and the admin reads it at
  # request time, so it is absent only between core's and edge's halves of the
  # first apply -- which the panel reports as "not configured".
  edge_readiness_parameter_name = "/${var.name_prefix}/edge/readiness"

  # The four tables the readiness panel describes for applied warm throughput.
  readiness_table_arns = [
    aws_dynamodb_table.counters.arn,
    aws_dynamodb_table.prequeue.arn,
    aws_dynamodb_table.positions.arn,
    aws_dynamodb_table.tokens.arn,
  ]
}

locals {
  # var.gate_rules is the dashboard's own one-rule-per-line grammar, so an
  # operator writes the same thing whether they type it into terraform.tfvars
  # or the Set rules form. Encoded here into the compact wire tuple the
  # CloudFront Function reads and wr_common::rules round-trips:
  # ["p","/checkout"], ["c","name"], ["u","substring"], ["h","name","value"].
  # The variable's own validation has already rejected any other shape.
  gate_rule_lines = [
    for line in compact([for l in split("\n", var.gate_rules) : trimspace(l)]) :
    line if !startswith(line, "#")
  ]

  gate_rule_wire = [
    for line in local.gate_rule_lines :
    startswith(line, "h")
    ? concat(["h"], regex("^h\\s+(\\S+)\\s+(.+)$", line))
    : concat([substr(line, 0, 1)], regex("^[pcu]\\s+(.+)$", line))
  ]
}
