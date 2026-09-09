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
  seal_event_name      = "${var.name_prefix}-seal-event"
  read_name            = "${var.name_prefix}-read"

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

  assign_position_zip = local.assign_position_is_placeholder ? data.archive_file.placeholder[0].output_path : var.assign_position_artifact_path
  seal_event_zip      = local.seal_event_is_placeholder ? data.archive_file.placeholder[0].output_path : var.seal_event_artifact_path
  read_zip            = local.read_is_placeholder ? data.archive_file.placeholder[0].output_path : var.read_artifact_path

  assign_position_hash = local.assign_position_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.assign_position_artifact_path)
  seal_event_hash      = local.seal_event_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.seal_event_artifact_path)
  read_hash            = local.read_is_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.read_artifact_path)

  lambda_runtime_arch = var.lambda_architecture
}
