# modules/edge - the waiting room's own pages and the gate that sends visitors
# to them.
#
# The pages are static objects in a private S3 bucket reached through an origin
# access control, on their own unprotected behaviour. They have to be reachable
# without a credential, because they are what a visitor without one is shown,
# and they have to survive the origin being overwhelmed, which is the entire
# reason the queue exists.

# The pages are static, so they cache properly rather than borrowing the polled
# policy's 1-second TTL, which exists to keep /status fresh and would have every
# edge location re-fetching an unchanging page every second. Five minutes is
# short enough that a corrected page reaches waiting visitors quickly and long
# enough that the bucket never sees load.
resource "aws_cloudfront_cache_policy" "waiting" {
  name        = "${var.name_prefix}-waiting-pages"
  comment     = "Static waiting-room pages: cache on path alone."
  min_ttl     = 0
  default_ttl = 300
  max_ttl     = 3600

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true
  }
}

resource "aws_s3_bucket" "waiting" {
  bucket_prefix = "${var.name_prefix}-waiting-"
  force_destroy = true
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "waiting" {
  bucket                  = aws_s3_bucket.waiting.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The realistic failure for these objects is a bad deploy overwriting the page
# every waiting visitor is looking at, so keep the previous versions to roll back
# to. Three small text objects, so the retained versions cost nothing.
resource "aws_s3_bucket_versioning" "waiting" {
  bucket = aws_s3_bucket.waiting.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "waiting" {
  bucket = aws_s3_bucket.waiting.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudfront_origin_access_control" "waiting" {
  name                              = "${var.name_prefix}-waiting-oac"
  description                       = "Signs CloudFront's reads of the waiting-room pages."
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# Only this distribution may read the bucket, so the pages cannot be fetched
# around the edge.
data "aws_iam_policy_document" "waiting_bucket" {
  statement {
    sid       = "AllowCloudFrontRead"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.waiting.arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.this.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "waiting" {
  bucket = aws_s3_bucket.waiting.id
  policy = data.aws_iam_policy_document.waiting_bucket.json
}

# The pages themselves. Content-typed explicitly because S3 does not infer it,
# and a page served as application/octet-stream downloads instead of rendering.
resource "aws_s3_object" "waiting_page" {
  bucket       = aws_s3_bucket.waiting.id
  key          = "waiting.html"
  content      = file("${path.module}/pages/waiting.html")
  content_type = "text/html; charset=utf-8"
  etag         = filemd5("${path.module}/pages/waiting.html")
  tags         = var.tags
}

resource "aws_s3_object" "waiting_style" {
  bucket       = aws_s3_bucket.waiting.id
  key          = "waiting.css"
  content      = file("${path.module}/pages/waiting.css")
  content_type = "text/css; charset=utf-8"
  etag         = filemd5("${path.module}/pages/waiting.css")
  tags         = var.tags
}

resource "aws_s3_object" "waiting_script" {
  bucket       = aws_s3_bucket.waiting.id
  key          = "waiting.js"
  content      = file("${path.module}/pages/waiting.js")
  content_type = "text/javascript; charset=utf-8"
  etag         = filemd5("${path.module}/pages/waiting.js")
  tags         = var.tags
}
