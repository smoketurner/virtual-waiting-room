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
  architectures = ["arm64"]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip
  source_code_hash = local.using_placeholder ? data.archive_file.placeholder[0].output_base64sha256 : filebase64sha256(var.lambda_artifact_path)

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
    queue_num        = { parent = "v1", path_part = "queue_num", method = "GET", auth = "NONE" }
    queue_pos_expiry = { parent = "v1", path_part = "queue_pos_expiry", method = "GET", auth = "NONE" }
    public_key       = { parent = "v1", path_part = "public_key", method = "GET", auth = "NONE" }

    # Public write under /v1 (F3.3) - single-use admission token.
    generate_token = { parent = "v1", path_part = "generate_token", method = "POST", auth = "NONE" }

    # Admin control plane (SigV4), unversioned. UI GET + per-action POSTs.
    admin          = { parent = "root", path_part = "admin", method = "GET", auth = "AWS_IAM" }
    metrics        = { parent = "root", path_part = "metrics", method = "GET", auth = "AWS_IAM" }
    update_session = { parent = "root", path_part = "update_session", method = "POST", auth = "AWS_IAM" }
    admin_phase    = { parent = "admin", path_part = "phase", method = "POST", auth = "AWS_IAM" }
    admin_rate     = { parent = "admin", path_part = "rate", method = "POST", auth = "AWS_IAM" }
    admin_message  = { parent = "admin", path_part = "message", method = "POST", auth = "AWS_IAM" }
    admin_reset    = { parent = "admin", path_part = "reset", method = "POST", auth = "AWS_IAM" }
    admin_rules    = { parent = "admin", path_part = "rules", method = "POST", auth = "AWS_IAM" }
    admin_metrics  = { parent = "admin", path_part = "metrics", method = "GET", auth = "AWS_IAM" }
  }

  # Split by parent so each container resource is created before its children.
  v1_endpoints    = { for k, v in local.api_endpoints : k => v if v.parent == "v1" }
  root_endpoints  = { for k, v in local.api_endpoints : k => v if v.parent == "root" }
  admin_endpoints = { for k, v in local.api_endpoints : k => v if v.parent == "admin" }
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

resource "aws_api_gateway_resource" "admin_child" {
  for_each = local.admin_endpoints

  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.root["admin"].id
  path_part   = each.value.path_part
}

locals {
  # Resolve each endpoint to its resource id, regardless of parent.
  endpoint_resource_id = merge(
    { for k, v in local.v1_endpoints : k => aws_api_gateway_resource.v1_child[k].id },
    { for k, v in local.root_endpoints : k => aws_api_gateway_resource.root[k].id },
    { for k, v in local.admin_endpoints : k => aws_api_gateway_resource.admin_child[k].id },
  )
}

# --- Methods + Lambda proxy integrations --------------------------------------

resource "aws_api_gateway_method" "endpoint" {
  for_each = local.api_endpoints

  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = local.endpoint_resource_id[each.key]
  http_method   = each.value.method
  authorization = each.value.auth
}

resource "aws_api_gateway_integration" "endpoint" {
  for_each = local.api_endpoints

  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = local.endpoint_resource_id[each.key]
  http_method             = aws_api_gateway_method.endpoint[each.key].http_method
  type                    = "AWS_PROXY"
  integration_http_method = "POST"
  uri                     = aws_lambda_function.api_placeholder.invoke_arn
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
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_integration.join_sqs,
    aws_api_gateway_integration.endpoint,
  ]
}

resource "aws_api_gateway_stage" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  deployment_id = aws_api_gateway_deployment.this.id
  stage_name    = var.env

  tags = var.tags
}
