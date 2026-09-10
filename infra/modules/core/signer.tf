# modules/core - the CloudFront signed-cookie key pair and the Lambda that mints
# admission cookies with it.
#
# The gate itself is CloudFront: the protected behaviour names a trusted key
# group, so CloudFront verifies each request's cookies at the edge and refuses
# an un-admitted visitor with a 403 before the origin is touched. Nothing runs
# per request, and it works against any origin — including one the customer does
# not let us run code in.
#
# The public key and key group live here rather than in modules/edge so the
# key-pair id is available to the Lambda that signs with it. Both are global
# CloudFront resources with no reference to a distribution, so owning them here
# costs nothing and avoids core and edge depending on each other.

# The key pair is generated at apply time, which puts the private key in
# Terraform state. That is the trade for a stack that stands up in one command;
# a deployment whose threat model excludes state supplies the pair out of band
# and points the parameter at it instead.
resource "tls_private_key" "cf_signer" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "aws_ssm_parameter" "cf_signer_key" {
  name        = "/${var.name_prefix}/cloudfront-signer-key"
  description = "PKCS#8 private key that signs CloudFront admission cookies. Read once at generate_token cold start."
  type        = "SecureString"
  value       = tls_private_key.cf_signer.private_key_pem_pkcs8

  tags = var.tags
}

resource "aws_cloudfront_public_key" "signer" {
  name_prefix = "${var.name_prefix}-signer-"
  comment     = "Verifies Virtual Waiting Room admission cookies at the edge."
  encoded_key = tls_private_key.cf_signer.public_key_pem

  # A public key cannot be updated in place, and the key group referencing it
  # cannot be left pointing at a deleted one, so the replacement is created
  # first. The name prefix keeps the two from colliding during that overlap.
  lifecycle {
    create_before_destroy = true
  }
}

# The group is stable across key rotations: its items list updates in place to
# name the new public key, so the distribution keeps referencing the same group
# and the gate never blinks.
resource "aws_cloudfront_key_group" "signer" {
  name    = "${var.name_prefix}-signers"
  comment = "Trusted signers for the Virtual Waiting Room protected behaviour."
  items   = [aws_cloudfront_public_key.signer.id]
}

# --- generate_token -----------------------------------------------------------
#
# Exchanges a reached queue position for a set of signed cookies. It reads the
# counters and the visitor's position, records the arrival the controller
# measures no-shows against, and signs a CloudFront policy. This is the only
# place arrivals are counted, so the controller's correction loop depends on it
# rather than on the origin-side authorizer.

resource "aws_iam_role" "generate_token" {
  name               = "${local.generate_token_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "generate_token" {
  name   = "${local.generate_token_name}-policy"
  role   = aws_iam_role.generate_token.id
  policy = data.aws_iam_policy_document.generate_token.json
}

resource "aws_lambda_function" "generate_token" {
  function_name = local.generate_token_name
  role          = aws_iam_role.generate_token.arn
  runtime       = "provided.al2023"
  architectures = [local.lambda_runtime_arch]
  handler       = "bootstrap"
  timeout       = 10
  memory_size   = 256

  filename         = local.lambda_zip["generate_token"]
  source_code_hash = local.lambda_hash["generate_token"]

  environment {
    variables = merge(local.dynamo_lambda_env, {
      COUNTERS_TABLE       = aws_dynamodb_table.counters.name
      PREQUEUE_TABLE       = aws_dynamodb_table.prequeue.name
      POSITIONS_TABLE      = aws_dynamodb_table.positions.name
      EVENT_ID             = var.event_id
      SIGNER_KEY_PARAMETER = aws_ssm_parameter.cf_signer_key.name
      SIGNER_KEY_PAIR_ID   = aws_cloudfront_public_key.signer.id
      COOKIE_TTL_SECS      = tostring(var.admission_cookie_ttl_seconds)
    })
  }

  tags = var.tags
}

resource "aws_lambda_permission" "generate_token_apigw" {
  statement_id  = "AllowAPIGatewayInvokeGenerateToken"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.generate_token.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/*"
}
