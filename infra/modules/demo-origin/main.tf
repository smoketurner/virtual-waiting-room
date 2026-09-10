# modules/demo-origin - a stand-in for the customer's protected origin.
#
# This is a test fixture, not part of the product. It exists so the gate can be
# exercised end to end in a deployment that does not have a real origin to
# protect: a visitor who gets through CloudFront lands on a page that says so,
# which is the only way to tell "the gate let me through" apart from "the gate
# is broken" without owning the origin.
#
# A public site cannot stand in for it. CloudFront forwards the viewer's Host to
# the protected origin, so a third-party host answers 404 for a name it does not
# serve, and any redirect it issues takes the visitor off the distribution
# entirely — past the gate, which then proves nothing.
#
# A production root does not instantiate this module at all; it points
# client_origin_domain_name at the real origin.

resource "aws_s3_bucket" "this" {
  bucket_prefix = "${var.name_prefix}-demo-origin-"
  force_destroy = true
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudfront_origin_access_control" "this" {
  name                              = "${var.name_prefix}-demo-origin-oac"
  description                       = "Signs CloudFront's reads of the demo protected origin."
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# Scoped to the account rather than to a single distribution ARN. The
# distribution cannot be named here: the edge module needs this bucket's domain
# and access control to build its origin, so depending on the distribution in
# return is a module cycle. Account scope means any CloudFront distribution in
# this account could read the bucket, which for a fixture serving one static
# page is an acceptable trade — the product's own waiting-page bucket lives
# inside the edge module precisely so it can name the distribution exactly.
data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "this" {
  statement {
    sid       = "AllowCloudFrontRead"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this.arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.this.json
}

# The page a visitor sees once the gate lets them through. Served at the
# distribution root via default_root_object.
resource "aws_s3_object" "index" {
  bucket       = aws_s3_bucket.this.id
  key          = "index.html"
  content      = file("${path.module}/pages/index.html")
  content_type = "text/html; charset=utf-8"
  etag         = filemd5("${path.module}/pages/index.html")
  tags         = var.tags
}
