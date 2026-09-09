locals {
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

  # warm_throughput is omitted from the table entirely when both units are 0, so
  # an un-warmed table stays at the on-demand cold baseline (idle default, no
  # cost) rather than pinning a floor.
  warm_throughput_enabled = var.warm_throughput_write_units > 0 || var.warm_throughput_read_units > 0

  # Use the vendored placeholder zip until a real build artifact is supplied.
  using_placeholder = var.lambda_artifact_path == ""
  lambda_zip        = local.using_placeholder ? data.archive_file.placeholder[0].output_path : var.lambda_artifact_path
}
