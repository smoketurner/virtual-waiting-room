# A well-formed ENTRY_TICKET_PUBLIC_KEY from the wrong pair passes init
# validation and then silently drops every registration, so the drop rate is
# the only signal that it is wrong (issue #59). Terraform owns the log group
# because Lambda's auto-created one has no retention and nothing to attach a
# metric filter to.

resource "aws_cloudwatch_log_group" "assign_position" {
  name              = "/aws/lambda/${local.assign_position_name}"
  retention_in_days = 30

  tags = var.tags
}

resource "aws_cloudwatch_log_metric_filter" "join_dropped" {
  name           = "${var.name_prefix}-join-dropped"
  log_group_name = aws_cloudwatch_log_group.assign_position.name
  pattern        = "{ $.event = \"join_dropped\" }"

  metric_transformation {
    name          = "${var.name_prefix}-JoinDropped"
    namespace     = "VirtualWaitingRoom"
    value         = "$.total"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "join_dropped" {
  alarm_name        = "${var.name_prefix}-join-dropped"
  alarm_description = "assign_position has dropped at least 10 join records a minute for 3 minutes. Most likely ENTRY_TICKET_PUBLIC_KEY does not match what the issuer signs with, silently discarding every registration."
  namespace         = "VirtualWaitingRoom"
  metric_name       = aws_cloudwatch_log_metric_filter.join_dropped.metric_transformation[0].name
  statistic         = "Sum"
  period            = 60
  # Three consecutive minutes, so stray garbage against a public endpoint does
  # not page while a sustained total-drop event still trips within a control
  # interval.
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  threshold           = 10
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  tags = var.tags
}
