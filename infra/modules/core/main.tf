# modules/core - the data plane, the live-join compute path, and the ingest API.
#
#   DynamoDB   four tables (DESIGN §5.5, §7). PITR and TTL are always on: both
#              are near-free and required, so neither is a toggle.
#   SSM        the per-deployment signing key as an encrypted SecureString
#              (DESIGN §8, ADR-0011).
#   SQS        one join queue + DLQ per deployment (event isolation, ADR-0008).
#   Lambda     assign_position - SQS consumer (the Rust crate
#              is built; ESM created disabled so no live batch is lost).
#   API GW     regional REST API integrating DIRECTLY with SQS SendMessage - no
#              Lambda in the ingest/burst path (DESIGN §6, ADR-0005).
#
# Only key attributes are declared per table: declaring a non-key attribute
# forces an infinite plan loop (provider note). All four tables are
# single-partition-key, no sort key, PAY_PER_REQUEST (on-demand) - DESIGN §5.5.

# --- DynamoDB -----------------------------------------------------------------

resource "aws_dynamodb_table" "counters" {
  name         = local.table_names.counters
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  dynamic "warm_throughput" {
    for_each = local.warm_throughput_enabled ? [1] : []
    content {
      read_units_per_second  = var.warm_throughput_read_units > 0 ? var.warm_throughput_read_units : null
      write_units_per_second = var.warm_throughput_write_units > 0 ? var.warm_throughput_write_units : null
    }
  }

  tags = var.tags
}

resource "aws_dynamodb_table" "prequeue" {
  name         = local.table_names.prequeue
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "r" # request_id (UUIDv7); short name because scanned at audit

  attribute {
    name = "r"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  dynamic "warm_throughput" {
    for_each = local.warm_throughput_enabled ? [1] : []
    content {
      read_units_per_second  = var.warm_throughput_read_units > 0 ? var.warm_throughput_read_units : null
      write_units_per_second = var.warm_throughput_write_units > 0 ? var.warm_throughput_write_units : null
    }
  }

  tags = var.tags
}

resource "aws_dynamodb_table" "positions" {
  name         = local.table_names.positions
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "request_id"

  attribute {
    name = "request_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  # TTL is post-event storage reclamation ONLY, never the expiry mechanism
  # (ADR-0006). The controller expires positions on a schedule; reads apply a
  # FilterExpression on expires_at so a TTL-pending item is never served.
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  dynamic "warm_throughput" {
    for_each = local.warm_throughput_enabled ? [1] : []
    content {
      read_units_per_second  = var.warm_throughput_read_units > 0 ? var.warm_throughput_read_units : null
      write_units_per_second = var.warm_throughput_write_units > 0 ? var.warm_throughput_write_units : null
    }
  }

  tags = var.tags
}

resource "aws_dynamodb_table" "tokens" {
  name         = local.table_names.tokens
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "request_id"

  attribute {
    name = "request_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  dynamic "warm_throughput" {
    for_each = local.warm_throughput_enabled ? [1] : []
    content {
      read_units_per_second  = var.warm_throughput_read_units > 0 ? var.warm_throughput_read_units : null
      write_units_per_second = var.warm_throughput_write_units > 0 ? var.warm_throughput_write_units : null
    }
  }

  tags = var.tags
}

# --- SSM Parameter Store (signing key) ----------------------------------------
# Single per-deployment signing key. Signs both admission tokens and session
# cookies over different inputs (ADR-0011). Held as an encrypted SecureString
# (AWS-managed alias/aws/ssm key - a standard SecureString parameter is free,
# unlike a Secrets Manager secret's $0.40/mo, which matters for N1 idle cost).
#
# The real key is generated and rotated OUT OF BAND (never in the repo, never in
# state). Terraform creates the parameter with a placeholder and then ignores
# its value, so the operator/bootstrap can overwrite it without drift and the
# secret material never lands in Terraform state.
resource "aws_ssm_parameter" "signing_key" {
  name        = "/${var.name_prefix}/signing-key"
  description = "Virtual Waiting Room per-deployment signing key (admission tokens + session cookies)."
  type        = "SecureString"
  value       = "PLACEHOLDER-overwrite-out-of-band" # nosemgrep: not a real secret

  lifecycle {
    ignore_changes = [value]
  }

  tags = var.tags
}

# OIDC client secret for the admin login flow (ADR-0016). Same pattern as the
# signing key: an encrypted SecureString created with a placeholder, whose value
# is written OUT OF BAND (never in the repo or state). The admin Lambda reads it
# by name (ssm:GetParameter with decryption) at Init.
resource "aws_ssm_parameter" "oidc_client_secret" {
  name        = "/${var.name_prefix}/oidc-client-secret"
  description = "Virtual Waiting Room admin OIDC client secret (read by the admin Lambda)."
  type        = "SecureString"
  value       = "PLACEHOLDER-overwrite-out-of-band" # nosemgrep: not a real secret

  lifecycle {
    ignore_changes = [value]
  }

  tags = var.tags
}

# --- SQS (ingest buffer, DESIGN §6, §11) --------------------------------------
# One join queue + DLQ per deployment. maxReceiveCount 5 -> DLQ. Visibility
# timeout follows the design formula: 6 x function_timeout + batching window.

resource "aws_sqs_queue" "join_dlq" {
  name                      = "${var.name_prefix}-join-dlq"
  message_retention_seconds = 1209600 # 14 days, to inspect poison batches

  tags = var.tags
}

resource "aws_sqs_queue" "join" {
  name                       = "${var.name_prefix}-join"
  visibility_timeout_seconds = 6 * 30 + 1 # 6 x function timeout (30s) + batching window (1s)

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.join_dlq.arn
    maxReceiveCount     = 5
  })

  tags = var.tags
}

# --- assign_position Lambda (DESIGN §2.2) -------------------------------------

resource "aws_iam_role" "assign_position" {
  name               = "${local.assign_position_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "assign_position" {
  name   = "${local.assign_position_name}-policy"
  role   = aws_iam_role.assign_position.id
  policy = data.aws_iam_policy_document.assign_position.json
}

resource "aws_lambda_function" "assign_position" {
  function_name = local.assign_position_name
  role          = aws_iam_role.assign_position.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 30
  memory_size   = 256

  filename         = local.lambda_zip["assign_position"]
  source_code_hash = local.lambda_hash["assign_position"]

  reserved_concurrent_executions = var.assign_position_reserved_concurrency

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE  = aws_dynamodb_table.counters.name
      POSITIONS_TABLE = aws_dynamodb_table.positions.name
    })
  }

  tags = var.tags
}

# Event-source mapping. Enabled only when a real assign_position artifact is
# deployed, so the live join batch is
# consumed by a non-functional handler. BatchSize 100 / batching window 1s,
# ReportBatchItemFailures so only failed record IDs return to the queue.
# batch_size is 100 because each invocation makes exactly ONE `ADD queue_counter`
# no matter how many joins are in the batch, and that counter is a single
# DynamoDB item with a write ceiling near 1,000/s. At the 10k joins/s target,
# batching by 100 costs 100 counter writes a second; batching by 10 would cost
# 1,000 and sit on the ceiling. Do not lower it to chase latency.
#
# The cost of that choice: a batch size above 10 requires a batching window of
# at least 1 second, and AWS documents that any window at all lets Lambda wait
# up to 20 seconds before invoking on a low-traffic queue. So a lone join during
# testing, or the last straggler of an event, can take ~20s to get a position.
# Under load — which is the case this system exists for — messages are always
# available and the window never binds.
resource "aws_lambda_event_source_mapping" "join" {
  event_source_arn                   = aws_sqs_queue.join.arn
  function_name                      = aws_lambda_function.assign_position.arn
  enabled                            = true
  batch_size                         = 100
  maximum_batching_window_in_seconds = 1
  function_response_types            = ["ReportBatchItemFailures"]
}

# --- API Gateway REST -> SQS (ingest, no Lambda in the burst path) ------------

resource "aws_iam_role" "apigw_sqs" {
  name               = "${var.name_prefix}-apigw-sqs-role"
  assume_role_policy = data.aws_iam_policy_document.apigw_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "apigw_sqs" {
  name   = "${var.name_prefix}-apigw-sqs-policy"
  role   = aws_iam_role.apigw_sqs.id
  policy = data.aws_iam_policy_document.apigw_sqs.json
}

resource "aws_api_gateway_rest_api" "this" {
  name        = "${var.name_prefix}-api"
  description = "Virtual Waiting Room public + admin API. /join integrates directly with SQS (no Lambda in the ingest path)."

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = var.tags
}

# JSON Schema model rejecting a malformed/missing request_id with a synchronous
# 400 at the edge, before anything is enqueued (DESIGN §6, F2.4).
resource "aws_api_gateway_model" "join" {
  rest_api_id  = aws_api_gateway_rest_api.this.id
  name         = "JoinRequest"
  content_type = "application/json"

  schema = jsonencode({
    "$schema"            = "http://json-schema.org/draft-04/schema#"
    title                = "JoinRequest"
    type                 = "object"
    required             = ["request_id", "event_id"]
    additionalProperties = false
    properties = {
      request_id = { type = "string", minLength = 1 }
      event_id   = { type = "string", minLength = 1 }
    }
  })
}

resource "aws_api_gateway_request_validator" "body" {
  name                        = "validate-body"
  rest_api_id                 = aws_api_gateway_rest_api.this.id
  validate_request_body       = true
  validate_request_parameters = false
}

# Presence check for required query-string parameters on the GET read endpoints.
# REST API validators only assert presence (no regex); format (e.g. UUIDv7) is
# still enforced in the handler. A missing param is rejected at the edge with a
# 400, so no Lambda is invoked for it.
resource "aws_api_gateway_request_validator" "params" {
  name                        = "validate-params"
  rest_api_id                 = aws_api_gateway_rest_api.this.id
  validate_request_body       = false
  validate_request_parameters = true
}

resource "aws_api_gateway_resource" "join" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.v1.id # path-versioned: /v1/join
  path_part   = "join"
}

resource "aws_api_gateway_method" "join_post" {
  rest_api_id          = aws_api_gateway_rest_api.this.id
  resource_id          = aws_api_gateway_resource.join.id
  http_method          = "POST"
  authorization        = "NONE"
  request_validator_id = aws_api_gateway_request_validator.body.id

  request_models = {
    "application/json" = aws_api_gateway_model.join.name
  }
}

# Direct AWS-service integration: API Gateway calls SQS SendMessage itself.
resource "aws_api_gateway_integration" "join_sqs" {
  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = aws_api_gateway_resource.join.id
  http_method             = aws_api_gateway_method.join_post.http_method
  type                    = "AWS"
  integration_http_method = "POST"
  credentials             = aws_iam_role.apigw_sqs.arn
  uri                     = "arn:${data.aws_partition.current.partition}:apigateway:${data.aws_region.current.region}:sqs:path/${data.aws_caller_identity.current.account_id}/${aws_sqs_queue.join.name}"

  request_parameters = {
    "integration.request.header.Content-Type" = "'application/x-www-form-urlencoded'"
  }

  request_templates = {
    "application/json" = "Action=SendMessage&MessageBody=$util.urlEncode($input.body)"
  }
}

resource "aws_api_gateway_integration_response" "join_200" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.join.id
  http_method = aws_api_gateway_method.join_post.http_method
  status_code = aws_api_gateway_method_response.join_200.status_code

  depends_on = [aws_api_gateway_integration.join_sqs]
}

resource "aws_api_gateway_method_response" "join_200" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = aws_api_gateway_resource.join.id
  http_method = aws_api_gateway_method.join_post.http_method
  status_code = "200"
}
