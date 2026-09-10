# modules/core - the rest of the REST API surface: the read endpoints, the
# token-minting and session endpoints, and the admin control plane, plus the
# stage/deployment.
#
# /join lives in main.tf: it integrates DIRECTLY with SQS (no Lambda). Every
# OTHER endpoint is fronted by a Lambda. Those Rust handlers are not built yet,
# so all Lambda-fronted endpoints share ONE placeholder function via AWS_PROXY
# until the real crates land (read handlers, generate_token, admin). Swapping in
# the real functions is a later step; the API shape, stage, and auth model are
# correct now.

# --- Shared placeholder Lambda for the API-fronted endpoints ------------------

resource "aws_iam_role" "api_placeholder" {
  name               = "${var.name_prefix}-api-placeholder-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "api_placeholder_logs" {
  role       = aws_iam_role.api_placeholder.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "api_placeholder" {
  function_name = "${var.name_prefix}-api-placeholder"
  role          = aws_iam_role.api_placeholder.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = data.archive_file.placeholder[0].output_path
  source_code_hash = data.archive_file.placeholder[0].output_base64sha256

  # The placeholder makes no AWS SDK calls, but carry the common env so every
  # Lambda in the module is uniform (no "why is this one different" later).
  environment {
    variables = local.common_lambda_env
  }

  tags = var.tags
}

resource "aws_lambda_permission" "api_placeholder" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_placeholder.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}

# --- Endpoint definitions -----------------------------------------------------
# Public endpoints are PATH-VERSIONED under /v1 (the stage is the environment,
# not the version - viewers hit /v1/status, forwarded to /<env>/v1/status).
# The admin control plane is unversioned: it is the SigV4 operator surface, not
# a public API contract.
#
# parent: "v1" nests under the /v1 version prefix (public);
#         "root" is a top-level path; "admin" nests under /admin.
# auth:   "NONE" for public, "AWS_IAM" for the SigV4 admin surface (F3.10, F5.5).

resource "aws_api_gateway_resource" "v1" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "v1"
}

locals {
  api_endpoints = {
    # Public read under /v1 (DESIGN §8, F3.1) - /status is the one polled payload.
    status           = { parent = "v1", path_part = "status", method = "GET", auth = "NONE" }
    queue_num        = { parent = "v1", path_part = "queue_num", method = "GET", auth = "NONE", required_query = ["request_id"] }
    queue_pos_expiry = { parent = "v1", path_part = "queue_pos_expiry", method = "GET", auth = "NONE", required_query = ["request_id"] }
    public_key       = { parent = "v1", path_part = "public_key", method = "GET", auth = "NONE" }

    # Public write under /v1 (F3.3) - single-use admission token.
    generate_token = { parent = "v1", path_part = "generate_token", method = "POST", auth = "NONE" }

    # Admin control plane, unversioned. Auth is NONE at API Gateway because the
    # admin Lambda enforces access via an OIDC login session (ADR-0016).
    #
    # /admin (the dashboard GET) is explicit because a {proxy+} resource does not
    # match the bare parent path. Every /admin/* action (login, callback, logout,
    # phase, rate, message, reset, rules, metrics) is served by the
    # /admin/{proxy+} greedy resource below → the same admin Lambda, whose axum
    # router does the real routing. /metrics and /update_session are top-level
    # paths (outside /admin/*) that also front the admin Lambda.
    admin          = { parent = "root", path_part = "admin", method = "GET", auth = "NONE" }
    metrics        = { parent = "root", path_part = "metrics", method = "GET", auth = "NONE" }
    update_session = { parent = "root", path_part = "update_session", method = "POST", auth = "NONE" }
  }

  # Split by parent so each container resource is created before its children.
  v1_endpoints   = { for k, v in local.api_endpoints : k => v if v.parent == "v1" }
  root_endpoints = { for k, v in local.api_endpoints : k => v if v.parent == "root" }

  # Endpoints that front the admin Lambda (all top-level here; the /admin/*
  # children are handled by the greedy proxy, not this map).
  is_admin_endpoint = {
    for k, v in local.api_endpoints : k => contains(["admin", "metrics", "update_session"], k)
  }
}

# --- Resources (paths) --------------------------------------------------------

resource "aws_api_gateway_resource" "v1_child" {
  for_each = local.v1_endpoints

  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.v1.id
  path_part   = each.value.path_part
}

resource "aws_api_gateway_resource" "root" {
  for_each = local.root_endpoints

  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = each.value.path_part
}

# Every /admin/* action (login, callback, logout, phase, rate, message, reset,
# rules, metrics) is a greedy proxy to the admin Lambda; its axum router does the
# routing. ANY covers the GET login flow and the POST actions in one resource.
# The bare /admin path is the explicit "admin" endpoint above ({proxy+} does not
# match the parent). Auth is NONE — the Lambda enforces the OIDC session.
resource "aws_api_gateway_resource" "admin_proxy" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.root["admin"].id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "admin_proxy" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = aws_api_gateway_resource.admin_proxy.id
  http_method   = "ANY"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "admin_proxy" {
  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = aws_api_gateway_resource.admin_proxy.id
  http_method             = aws_api_gateway_method.admin_proxy.http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = local.admin_is_placeholder ? aws_lambda_function.api_placeholder.invoke_arn : aws_lambda_function.admin.invoke_arn
}

# Static assets (CSS) for the admin UI, served by the admin Lambda from its
# embedded files. Public (no SigV4): a browser <link> cannot sign the request
# and the stylesheets carry no secrets. Greedy {proxy+} under /static.
resource "aws_api_gateway_resource" "static" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "static"
}

resource "aws_api_gateway_resource" "static_proxy" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.static.id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "static_get" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = aws_api_gateway_resource.static_proxy.id
  http_method   = "GET"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "static" {
  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = aws_api_gateway_resource.static_proxy.id
  http_method             = aws_api_gateway_method.static_get.http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = local.admin_is_placeholder ? aws_lambda_function.api_placeholder.invoke_arn : aws_lambda_function.admin.invoke_arn
}

locals {
  # Resolve each endpoint to its resource id, regardless of parent.
  endpoint_resource_id = merge(
    { for k, v in local.v1_endpoints : k => aws_api_gateway_resource.v1_child[k].id },
    { for k, v in local.root_endpoints : k => aws_api_gateway_resource.root[k].id },
  )
}

# --- Methods + Lambda proxy integrations --------------------------------------

resource "aws_api_gateway_method" "endpoint" {
  for_each = local.api_endpoints

  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = local.endpoint_resource_id[each.key]
  http_method   = each.value.method
  authorization = each.value.auth

  # Endpoints that declare required_query get edge presence validation: each
  # named query-string parameter is marked required and the params validator is
  # attached, so a missing parameter is rejected with a 400 before any Lambda.
  request_validator_id = length(lookup(each.value, "required_query", [])) > 0 ? aws_api_gateway_request_validator.params.id : null
  request_parameters = {
    for p in lookup(each.value, "required_query", []) :
    "method.request.querystring.${p}" => true
  }
}

resource "aws_api_gateway_integration" "endpoint" {
  for_each = local.api_endpoints

  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = local.endpoint_resource_id[each.key]
  http_method             = aws_api_gateway_method.endpoint[each.key].http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  # Route each endpoint to its backing Lambda: the read Lambda for the two
  # public read endpoints, the admin Lambda for the OIDC-gated control plane
  # (when its artifact is deployed), and the shared placeholder for everything
  # else until its crate lands. Admin routing keys on the endpoint being an admin
  # one, not on its auth type (auth is NONE — the Lambda enforces the session).
  uri = contains(["status", "queue_num"], each.key) ? aws_lambda_function.read.invoke_arn : (
    each.key == "generate_token" ? aws_lambda_function.generate_token.invoke_arn : (
      local.is_admin_endpoint[each.key] && !local.admin_is_placeholder ? aws_lambda_function.admin.invoke_arn : aws_lambda_function.api_placeholder.invoke_arn
    )
  )
}

# --- Stage + deployment -------------------------------------------------------
# The deployment redeploys whenever any resource/method/integration changes
# (triggers hash). create_before_destroy orders redeployments correctly.

resource "aws_api_gateway_deployment" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.v1.id,
      aws_api_gateway_resource.join.id,
      aws_api_gateway_method.join_post.id,
      aws_api_gateway_integration.join_sqs.id,
      [for k in sort(keys(local.api_endpoints)) : local.endpoint_resource_id[k]],
      [for k in sort(keys(local.api_endpoints)) : aws_api_gateway_method.endpoint[k].id],
      [for k in sort(keys(local.api_endpoints)) : aws_api_gateway_integration.endpoint[k].id],
      # Method ids are stable across in-place updates, so also hash the mutable
      # method config (validator + required params); otherwise a validation
      # change never triggers a new deployment and the stage serves stale config.
      [for k in sort(keys(local.api_endpoints)) : aws_api_gateway_method.endpoint[k].request_validator_id],
      [for k in sort(keys(local.api_endpoints)) : jsonencode(aws_api_gateway_method.endpoint[k].request_parameters)],
      # Integration URIs change in place when an endpoint is retargeted from the
      # placeholder to a real Lambda; hash them so that forces a redeployment.
      [for k in sort(keys(local.api_endpoints)) : aws_api_gateway_integration.endpoint[k].uri],
      # Static-asset route (admin CSS).
      aws_api_gateway_resource.static_proxy.id,
      aws_api_gateway_method.static_get.id,
      aws_api_gateway_integration.static.uri,
      # Admin greedy proxy (/admin/*).
      aws_api_gateway_resource.admin_proxy.id,
      aws_api_gateway_method.admin_proxy.id,
      aws_api_gateway_integration.admin_proxy.uri,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_integration.join_sqs,
    aws_api_gateway_integration.endpoint,
    aws_api_gateway_integration.static,
    aws_api_gateway_integration.admin_proxy,
  ]
}

resource "aws_api_gateway_stage" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  deployment_id = aws_api_gateway_deployment.this.id
  stage_name    = var.env

  tags = var.tags
}
