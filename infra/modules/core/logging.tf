# Logs, metric filters and alarms.
#
# Before this file had more than one log group, the whole observability surface
# of the deployment was a single group for assign_position. Every other function
# auto-created one that never expired, there were no metric filters, no alarms
# and no dashboard — while the Rust carried comments telling an operator to
# "attach a metric filter" to event names that had none.
#
# The failure mode this exists for is the one the system actually has: it does
# not crash, it looks healthy while doing nothing. Every alarm below fires on a
# condition that is otherwise completely silent.
#
# Idle cost (N1): log groups and metric filters bill nothing to exist and
# nothing to evaluate — only ingested log data is charged, and between events no
# function is invoked. The alarms are the only standing charge, at $0.10 each
# per month.

# Terraform owns every log group because Lambda's auto-created ones never
# expire. One block, one retention, no function left to grow without bound.
resource "aws_cloudwatch_log_group" "lambda" {
  for_each = toset([
    local.assign_position_name,
    local.open_event_name,
    local.read_name,
    local.admin_name,
    local.controller_name,
    local.generate_token_name,
  ])

  name              = "/aws/lambda/${each.value}"
  retention_in_days = 30

  tags = var.tags
}

# --- Metric filters ------------------------------------------------------------
# Each keys on a stable `event` name the Rust already emits. The code comments
# that used to say "attach a metric filter to this" are gone; this is the filter
# they meant.

locals {
  # `log_group` is the function that emits the event; `pattern` keys on the
  # stable name; `value` is what is published per match.
  log_metrics = {
    # A join that reached SQS and was then discarded: a malformed body, or an
    # event_id that does not match this deployment's. The visitor got a 200,
    # SQS deleted the message, and DynamoDB stayed empty. An event_id mismatch
    # drops *every* join this way, and the first symptom is an empty queue at
    # T-0.
    join_dropped = {
      log_group = local.assign_position_name
      pattern   = "{ $.event = \"join_dropped\" }"
      value     = "$.total"
    }

    # The arrival went unrecorded, so the controller measures a no-show that
    # did show. Cumulative and silent: every uncounted arrival inflates the
    # no-show rate, and the controller answers by releasing more people than
    # the origin agreed to serve.
    arrival_record_failed = {
      log_group = local.generate_token_name
      pattern   = "{ $.event = \"arrival_record_failed\" }"
      value     = "1"
    }

    # Same consequence, one step earlier: no shard drawn, so nothing to record
    # the arrival against.
    arrival_shard_draw_failed = {
      log_group = local.generate_token_name
      pattern   = "{ $.event = \"arrival_shard_draw_failed\" }"
      value     = "1"
    }

    # The stored admission control could not be parsed, so the controller is
    # holding admission rather than resuming it. Holding is the safe direction
    # (the only writer of that attribute is the operator's pause, so an
    # unreadable value is a pause that did not land), but it is silent from the
    # outside: a controller that stops releasing looks exactly like one with
    # nothing to release, and the queue simply stops moving.
    admission_control_unreadable = {
      log_group = local.controller_name
      pattern   = "{ $.event = \"admission_control_unreadable\" }"
      value     = "1"
    }

    # The ruleset reached the edge but the audit stamp did not, so the
    # dashboard shows a stale "last changed by" for a gate that has already
    # changed. The KeyValueStore data plane is not covered by CloudTrail
    # management events, so this log line is the only trail it leaves.
    rules_audit_failed = {
      log_group = local.admin_name
      pattern   = "{ $.event = \"rules_audit_failed\" }"
      value     = "1"
    }
  }

  metric_namespace = "VirtualWaitingRoom/${var.name_prefix}"
}

resource "aws_cloudwatch_log_metric_filter" "event" {
  for_each = local.log_metrics

  name           = "${var.name_prefix}-${each.key}"
  log_group_name = aws_cloudwatch_log_group.lambda["${each.value.log_group}"].name
  pattern        = each.value.pattern

  metric_transformation {
    name      = each.key
    namespace = local.metric_namespace
    value     = each.value.value
    # Absent means "nothing went wrong", not "no data": without this the alarm
    # sits in INSUFFICIENT_DATA whenever the system is behaving, which is most
    # of the time, and an alarm nobody ever sees green is an alarm nobody
    # trusts when it goes red.
    default_value = 0
  }
}

# --- Alarms --------------------------------------------------------------------
# Every one of these is a condition the system would otherwise survive in
# silence. The threshold is zero in each case: these are not rates to tune, they
# are events that should never happen.

resource "aws_cloudwatch_metric_alarm" "event" {
  for_each = local.log_metrics

  alarm_name        = "${var.name_prefix}-${each.key}"
  alarm_description = "${each.key}: see the metric filter in modules/core/logging.tf for what this means and why it is silent without an alarm."

  namespace   = local.metric_namespace
  metric_name = each.key
  statistic   = "Sum"
  period      = 300
  # One occurrence is the signal. These do not happen in normal operation, so
  # there is no threshold worth tuning above zero.
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # default_value = 0 on the filter means a quiet system publishes zeroes, so
  # missing data is a broken pipeline rather than a healthy one.
  treat_missing_data = "notBreaching"

  tags = var.tags
}

# The dead-letter queue is the one signal that needs no filter: SQS publishes
# its depth itself. A message here has been received maxReceiveCount times and
# given up on -- a join that was accepted, retried, and lost.
resource "aws_cloudwatch_metric_alarm" "join_dlq_not_empty" {
  alarm_name        = "${var.name_prefix}-join-dlq-not-empty"
  alarm_description = "A join reached the dead-letter queue: accepted from the visitor, retried to exhaustion, and lost. The visitor was told 200."

  namespace   = "AWS/SQS"
  metric_name = "ApproximateNumberOfMessagesVisible"
  dimensions = {
    QueueName = aws_sqs_queue.join_dlq.name
  }

  statistic           = "Maximum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # SQS publishes this metric only while the queue has been active, so absent
  # data is an idle queue, which is the healthy state.
  treat_missing_data = "notBreaching"

  tags = var.tags
}
