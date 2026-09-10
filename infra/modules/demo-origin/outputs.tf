output "bucket_regional_domain_name" {
  description = "Regional domain name of the demo origin bucket, for the edge module's origin block."
  value       = aws_s3_bucket.this.bucket_regional_domain_name
}

output "origin_access_control_id" {
  description = "ID of the origin access control CloudFront signs its reads with."
  value       = aws_cloudfront_origin_access_control.this.id
}

output "bucket_name" {
  description = "Name of the demo origin bucket, for uploading other fixture pages by hand."
  value       = aws_s3_bucket.this.id
}
