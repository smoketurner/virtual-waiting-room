# Terraform owns the log group because Lambda's auto-created one never expires.

resource "aws_cloudwatch_log_group" "assign_position" {
  name              = "/aws/lambda/${local.assign_position_name}"
  retention_in_days = 30

  tags = var.tags
}
