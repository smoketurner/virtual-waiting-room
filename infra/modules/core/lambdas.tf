# modules/core - the open and read Lambdas.
#
#   open_event  fired once at the event start by an EventBridge schedule; reads
#               the shard counts and writes the open (seed + offsets + count +
#               phase) in one conditional UpdateItem.
#   read        serves GET /v1/status and /v1/queue_num over API Gateway; the
#               route wiring lives in api.tf.
#
# Every function deploys its own build; there is no stub fallback.

# --- open_event ---------------------------------------------------------------

resource "aws_iam_role" "open_event" {
  name               = "${local.open_event_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "open_event" {
  name   = "${local.open_event_name}-policy"
  role   = aws_iam_role.open_event.id
  policy = data.aws_iam_policy_document.open_event.json
}

resource "aws_lambda_function" "open_event" {
  function_name = local.open_event_name
  role          = aws_iam_role.open_event.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  # One BatchGetItem and one conditional UpdateItem; it finishes in well under
  # a second whatever the cohort size, because it reads counters, not rows.
  timeout     = 10
  memory_size = 256

  filename         = local.lambda_zip["open_event"]
  source_code_hash = local.lambda_hash["open_event"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
    })
  }

  tags = var.tags
}

# --- read ---------------------------------------------------------------------

resource "aws_iam_role" "read" {
  name               = "${local.read_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "read" {
  name   = "${local.read_name}-policy"
  role   = aws_iam_role.read.id
  policy = data.aws_iam_policy_document.read.json
}

resource "aws_lambda_function" "read" {
  function_name = local.read_name
  role          = aws_iam_role.read.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["read"]
  source_code_hash = local.lambda_hash["read"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE  = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE  = aws_dynamodb_table.prequeue.name
      POSITIONS_TABLE = aws_dynamodb_table.positions.name
      EVENT_ID        = var.event_id
      # Adaptive poll policy (#69), published verbatim on /status.
      POLL_FLOOR_MS   = tostring(var.poll_floor_ms)
      POLL_CEILING_MS = tostring(var.poll_ceiling_ms)
      POLL_DIVISOR    = tostring(var.poll_divisor)
    })
  }

  tags = var.tags
}

# API Gateway invokes the read Lambda for the status / queue_num routes.
resource "aws_lambda_permission" "read_apigw" {
  statement_id  = "AllowAPIGatewayInvokeRead"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.read.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# --- admin (SigV4 control plane) ----------------------------------------------

resource "aws_iam_role" "admin" {
  name               = "${local.admin_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "admin" {
  name   = "${local.admin_name}-policy"
  role   = aws_iam_role.admin.id
  policy = data.aws_iam_policy_document.admin.json
}

resource "aws_lambda_function" "admin" {
  function_name = local.admin_name
  role          = aws_iam_role.admin.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["admin"]
  source_code_hash = local.lambda_hash["admin"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
      TOKENS_TABLE   = aws_dynamodb_table.tokens.name
      EVENT_ID       = var.event_id
      # Issue #71: mirrors fail_open_until to the edge gate's KeyValueStore.
      EDGE_KVS_ARN = aws_cloudfront_key_value_store.gate.arn
      # Issue #128: the operator's start time is written here, which arms the
      # one-time open schedule Terraform created disabled.
      OPEN_SCHEDULE_NAME = aws_scheduler_schedule.open.name
      # API Gateway prefixes the path with the stage (e.g. /dev/admin); this
      # makes the Rust runtime strip it so the Axum routes match unprefixed.
      AWS_LAMBDA_HTTP_IGNORE_STAGE_IN_PATH = "true"
      # OIDC admin login (ADR-0016). The client secret is read from the SSM
      # SecureString named here; the rest are non-secret config.
      OIDC_ISSUER              = var.oidc_issuer
      OIDC_CLIENT_ID           = var.oidc_client_id
      OIDC_REDIRECT_URI        = var.oidc_redirect_uri
      OIDC_CLIENT_SECRET_PARAM = aws_ssm_parameter.oidc_client_secret.name
      # Comma-separated allowlist of operator emails permitted to log in. Empty
      # = deny all (the admin Lambda fails closed).
      OIDC_ALLOWED_EMAILS = var.oidc_allowed_emails
    })
  }

  tags = var.tags

  # The admin Lambda reads OIDC_CLIENT_ID and OIDC_REDIRECT_URI at Init and
  # treats either being absent as a hard error, so deploying without them
  # produces a function that fails on every invoke -- a dead control plane whose
  # only symptom is a 502 from API Gateway, on an apply that reported success.
  # An empty allowlist is the same shape of failure one step later: the login
  # completes and every operator is denied. Fail the apply instead.
  #
  # The redirect URI cannot be derived here: it is a path on the CloudFront
  # distribution, and edge consumes core's KeyValueStore ARN, so core cannot
  # depend on edge without a cycle. Apply once to create the distribution, read
  # cloudfront_domain_name, then set these three -- which is the one sequencing
  # step the deployment genuinely has, and it now fails loudly instead of
  # silently.
  lifecycle {
    precondition {
      condition     = var.oidc_client_id != "" && var.oidc_redirect_uri != ""
      error_message = "oidc_client_id and oidc_redirect_uri must both be set, or the admin Lambda deploys and then fails at Init on every invoke. The redirect URI is https://<cloudfront_domain_name>/admin/callback -- apply once to create the distribution, then set them and apply again."
    }

    precondition {
      condition     = trimspace(var.oidc_allowed_emails) != ""
      error_message = "oidc_allowed_emails must list at least one operator, or every login is denied and the event has no control plane."
    }
  }
}

# API Gateway invokes the admin Lambda for the SigV4 /admin, /metrics, and
# /update_session routes.
resource "aws_lambda_permission" "admin_apigw" {
  statement_id  = "AllowAPIGatewayInvokeAdmin"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.admin.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# --- open schedule (EventBridge Scheduler) ------------------------------------
# One-time trigger at the event start (issue #128). The schedule always exists
# so that destroying the stack destroys it too; the operator sets the time from
# the admin dashboard, which is the only thing that ever writes the expression.
#
# Ownership is split. Terraform owns the schedule's existence, its target, its
# retry policy and its role; the admin owns the expression and the state. The
# expression below is therefore a placeholder that is never the real value, in
# the same shape as aws_ssm_parameter.oidc_client_secret: required by the API,
# overwritten out of band, and excluded from drift by ignore_changes. It is set
# far in the future rather than near, so that even an accidental enable of the
# placeholder cannot fire an open.

resource "aws_scheduler_schedule" "open" {
  name = "${var.name_prefix}-open"

  flexible_time_window {
    mode = "OFF"
  }

  # Seeded from var.starts_at when one is configured, so an event scheduled in
  # terraform.tfvars opens without an operator logging in first. Unset, the
  # schedule is created disabled at a placeholder instant that is never the real
  # value -- an accidental enable cannot fire an open.
  schedule_expression          = var.starts_at == "" ? "at(2099-12-31T23:59:59)" : "at(${var.starts_at})"
  schedule_expression_timezone = var.starts_at_timezone
  state                        = var.starts_at == "" ? "DISABLED" : "ENABLED"

  target {
    arn      = aws_lambda_function.open_event.arn
    role_arn = aws_iam_role.open_scheduler.arn
    input    = jsonencode({ event_id = var.event_id })

    # Deliberately not the AWS defaults (86400 seconds / 185 attempts). Two
    # reasons. An open that could not be delivered for 24 hours would open the
    # event a day late, which is worse than not opening it at all, so the
    # attempt is bounded to minutes. And because the provider's defaults are
    # also the service's, an UpdateSchedule that dropped this block would be
    # indistinguishable from one that preserved it -- declaring a non-default
    # value is what makes the admin writer's read-modify-write observable.
    retry_policy {
      maximum_event_age_in_seconds = 600
      maximum_retry_attempts       = 10
    }
  }

  # The operator's start time lives here, written by the admin Lambda. Without
  # this, every apply after an operator set a time would revert it. The
  # timezone is theirs too: the operator picks the zone their event opens in,
  # and Scheduler evaluates the expression in it.
  lifecycle {
    ignore_changes = [schedule_expression, schedule_expression_timezone, state]
  }
}

resource "aws_iam_role" "open_scheduler" {
  name               = "${var.name_prefix}-open-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "open_scheduler" {
  name = "${var.name_prefix}-open-scheduler-policy"
  role = aws_iam_role.open_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.open_event.arn
    }]
  })
}

# --- controller ---------------------------------------------------------------
#
# The closed-loop outflow controller. It advances
# serving_counter to meter admission against the operator's target rate while
# compensating for no-shows, and expires positions past expires_at. Falls back
# from its own build.

resource "aws_iam_role" "controller" {
  name               = "${local.controller_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "controller" {
  name   = "${local.controller_name}-policy"
  role   = aws_iam_role.controller.id
  policy = data.aws_iam_policy_document.controller.json
}

resource "aws_lambda_function" "controller" {
  function_name = local.controller_name
  role          = aws_iam_role.controller.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  # Each durable wait suspends the execution, so an invocation covers replay
  # plus a single pass rather than a whole minute of cadence.
  timeout     = 30
  memory_size = 256

  # The 10s cadence is built from durable waits: the execution suspends between
  # passes without incurring duration charges, instead of holding the invocation
  # open across six passes. ExecutionTimeout bounds one minute of cadence with
  # headroom for step retries; a stuck execution is abandoned rather than
  # overlapping the next scheduled one.
  durable_config {
    execution_timeout = 120
    retention_period  = 7
  }

  # Destroy stops in-flight durable executions first, which the provider
  # documents as taking up to an hour.
  timeouts {
    delete = "60m"
  }

  filename         = local.lambda_zip["controller"]
  source_code_hash = local.lambda_hash["controller"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE = aws_dynamodb_table.counters.name
      EVENT_ID       = var.event_id
    })
  }

  tags = var.tags
}

# --- controller schedule (EventBridge Scheduler) ------------------------------
# The design cadence is 10s, but the Scheduler rate() minimum is 1 minute, so
# the schedule fires every minute and each invoke runs six 10s passes. Always
# created: a deployed controller nothing fires means the queue forms and never
# drains, and an idle pass is one GetItem that returns early unless the event is
# active and admitting.

resource "aws_scheduler_schedule" "controller" {
  name = "${var.name_prefix}-controller"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "rate(1 minute)"
  schedule_expression_timezone = "UTC"

  # The universal target, not the templated Lambda one. Two things a durable
  # function needs cannot be expressed by the templated target:
  #
  #   InvocationType = Event. The templated target calls Invoke with no
  #   invocation type, so Lambda defaults to RequestResponse, and a synchronous
  #   durable invocation is held open for the whole execution - including the
  #   waits it is supposed to suspend through. That bills the full minute of
  #   cadence and defeats the point of ADR-0022.
  #
  #   A qualified FunctionName. Durable functions cannot be invoked through an
  #   unqualified identifier; an execution is pinned to the version that started
  #   it so that replay runs the same code.
  #
  # $LATEST is the qualifier because the function publishes no versions. An
  # execution that is mid-flight when a deploy lands may fail to replay against
  # changed code; the window is one minute of cadence. Publishing versions and
  # pointing this at an alias removes even that.
  target {
    arn      = "arn:aws:scheduler:::aws-sdk:lambda:invoke"
    role_arn = aws_iam_role.controller_scheduler.arn
    input = jsonencode({
      FunctionName   = "${aws_lambda_function.controller.arn}:$LATEST"
      InvocationType = "Event"
      Payload        = jsonencode({ event_id = var.event_id })
    })
  }
}

resource "aws_iam_role" "controller_scheduler" {
  name               = "${var.name_prefix}-controller-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "controller_scheduler" {
  name = "${var.name_prefix}-controller-scheduler-policy"
  role = aws_iam_role.controller_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "lambda:InvokeFunction"
      # Both forms: IAM evaluates a qualified invoke against the qualified ARN,
      # so granting only the unqualified one fails once the schedule names a
      # version. The unqualified entry stays for a direct manual invoke.
      Resource = [
        aws_lambda_function.controller.arn,
        "${aws_lambda_function.controller.arn}:*",
      ]
    }]
  })
}
