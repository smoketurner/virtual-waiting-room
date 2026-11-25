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
# per month, and there are 31 of them: $3.10 of the $5 idle budget. That is the
# reason for what is absent. One alarm per signal and no composites; DynamoDB
# throttles on writes only, and only on the three visitor-path tables; no
# per-function duration or concurrency alarms. Add one only for a failure
# nothing here already reports.

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
    local.nojs_name,
  ])

  name              = "/aws/lambda/${each.value}"
  retention_in_days = 30

  tags = var.tags
}

# --- Metric filters ------------------------------------------------------------
# Each keys on a stable `event` name the Rust already emits. The code comments
# that used to say "attach a metric filter to this" are gone; this is the filter
# they meant.
#
# Every Lambda logs through `tracing_subscriber::fmt().json()` without
# `flatten_event`, which nests the event's own fields under a `fields` object and
# puts only `level` at the root:
#
#   {"level":"WARN","fields":{"message":"...","event":"join_dropped","total":3}}
#
# So a pattern or value names `$.fields.<name>`. A bare `$.event` matches nothing,
# and a filter that matches nothing publishes zero forever with its alarm green.

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
      pattern   = "{ $.fields.event = \"join_dropped\" }"
      value     = "$.fields.total"
    }

    # A unit of the admission budget could not be taken (ADR-0039), so the
    # visitor was not admitted and will ask again. Harmless once; if it is
    # failing for everyone -- a missing grant, a throttled table -- nobody is
    # admitted at all, while the queue page keeps saying it is their turn.
    arrival_record_failed = {
      log_group = local.generate_token_name
      pattern   = "{ $.fields.event = \"arrival_record_failed\" }"
      value     = "1"
    }

    # Same consequence, one step earlier: no shard drawn, so no budget to take
    # the unit from.
    arrival_shard_draw_failed = {
      log_group = local.generate_token_name
      pattern   = "{ $.fields.event = \"arrival_shard_draw_failed\" }"
      value     = "1"
    }

    # The write that marks a visitor admitted did not land. They were admitted
    # anyway, having taken their unit of budget. Harmless for one visitor; if
    # it is failing for everyone -- a missing grant, a throttled table -- every
    # reload takes another unit, so fewer people are admitted than the rate
    # allows. Visitors keep being admitted throughout, so nothing else reports
    # it.
    admission_claim_failed = {
      log_group = local.generate_token_name
      pattern   = "{ $.fields.event = \"admission_claim_failed\" }"
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
      pattern   = "{ $.fields.event = \"admission_control_unreadable\" }"
      value     = "1"
    }

    # The ruleset reached the edge but the audit stamp did not, so the
    # dashboard shows a stale "last changed by" for a gate that has already
    # changed. The KeyValueStore data plane is not covered by CloudTrail
    # management events, so this log line is the only trail it leaves.
    rules_audit_failed = {
      log_group = local.admin_name
      pattern   = "{ $.fields.event = \"rules_audit_failed\" }"
      value     = "1"
    }

    # A `set_fail_open_until` audit stamp lost the anti-regression race to a
    # newer concurrent writer, so the break-glass `fail_open_until` was
    # committed without the stamp to preserve the newer anchor. The functional
    # write landed (the operator's fail-open is in force), but the dashboard's
    # "latest action" line did not record this fail-open -- the audit-loss trade
    # of the guard. Rare by construction (needs a concurrent writer to have
    # advanced the anchor inside the same event's yielding KVStore
    # read-modify-write), but a regression that spuriously took the fallback on
    # every `set_fail_open_until` call would produce no other signal, so this
    # alarms at threshold zero like `rules_audit_failed`.
    fail_open_audit_lost = {
      log_group = local.admin_name
      pattern   = "{ $.fields.event = \"fail_open_audit_lost\" }"
      value     = "1"
    }

    # A visitor without JavaScript pressed "Join the line" and the join could
    # not be enqueued (issue #67). They were told so and hold no place; if
    # this is continuous, nobody without JavaScript can get in line at all,
    # and nothing else reports it.
    nojs_join_failed = {
      log_group = local.nojs_name
      pattern   = "{ $.fields.event = \"nojs_join_failed\" }"
      value     = "1"
    }

    # An error from the open_event Lambda -- today only the scheduled open
    # rejected because the event was not in the pre_queue phase when the
    # schedule fired (event = "open_rejected_wrong_phase" in the structured
    # log). Before the open_event disambiguation, this was misreported as
    # AlreadyOpen and recorded by EventBridge as a silent success, leaving the
    # event unopened with no signal; the open now returns Err (so EventBridge
    # retries and eventually dead-letters via open_dlq), and this filter
    # alarms on the first occurrence so a misfire is surfaced even when the
    # retries that follow it eventually succeed.
    #
    # Keys on the root `level` ("ERROR", uppercase) rather than one event name,
    # so any future open_event error alarms too, not just the wrong-phase one.
    open_event_error = {
      log_group = local.open_event_name
      pattern   = "{ $.level = \"ERROR\" }"
      value     = "1"
    }
  }

  metric_namespace = "VirtualWaitingRoom/${var.name_prefix}"

  # Where every alarm below reports, in both directions. Empty leaves them
  # visible in the console and silent, which is all a dev stack wants.
  alarm_actions = var.alarm_sns_topic_arn == "" ? [] : [var.alarm_sns_topic_arn]
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

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

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

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

# The open schedule's dead-letter queue: a message here is a scheduled open
# EventBridge retried to exhaustion and gave up on -- the handler kept
# returning Err (e.g. the event never reached pre_queue before the retry
# window elapsed), so the event stayed unopened with no further automatic
# attempt, and the one-time `at(...)` schedule is consumed. Mirrors
# join_dlq_not_empty: SQS publishes its own depth, so this alarms at one
# message rather than via a log metric filter.
resource "aws_cloudwatch_metric_alarm" "open_dlq_not_empty" {
  alarm_name        = "${var.name_prefix}-open-dlq-not-empty"
  alarm_description = "A scheduled open reached the dead-letter queue: retried to exhaustion and lost. The event stayed unopened; the one-time schedule is consumed and needs the operator to re-arm."

  namespace   = "AWS/SQS"
  metric_name = "ApproximateNumberOfMessagesVisible"
  dimensions = {
    QueueName = aws_sqs_queue.open_dlq.name
  }

  statistic           = "Maximum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # SQS publishes this metric only while the queue has been active, so absent
  # data is an idle queue, which is the healthy state.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

# --- Service metrics ----------------------------------------------------------
# The alarms above catch what the code knows went wrong. These catch what it
# never got to log: a function that crashed or was throttled before its first
# line, a queue nobody is draining, a schedule that stopped firing (issue #246).
# The services publish these metrics themselves, so they need no filter.

# A join waiting this long is a registrant without a position. assign_position
# normally empties the queue within its one-second batching window; a minute
# means it is failing, throttled, or its event-source mapping is off -- and at
# T-0 whoever is still queued is not in the cohort. Two periods, so the up to
# 20 seconds Lambda may hold a lone message on an idle queue cannot trip it.
resource "aws_cloudwatch_metric_alarm" "join_queue_age" {
  alarm_name        = "${var.name_prefix}-join-queue-age"
  alarm_description = "The oldest join has waited over a minute for a position: assign_position is not keeping up with the queue, or not consuming it at all."

  namespace   = "AWS/SQS"
  metric_name = "ApproximateAgeOfOldestMessage"
  dimensions = {
    QueueName = aws_sqs_queue.join.name
  }

  statistic           = "Maximum"
  period              = 60
  threshold           = 60
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  # An empty queue reports no age.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

locals {
  alarmed_functions = {
    assign_position = local.assign_position_name
    open_event      = local.open_event_name
    read            = local.read_name
    admin           = local.admin_name
    controller      = local.controller_name
    generate_token  = local.generate_token_name
    nojs            = local.nojs_name
  }

  # Errors is an invocation that failed: a panic, a timeout, a handler Err.
  # Throttles is one that never ran. Neither leaves a log line the filters
  # above could key on -- a throttled controller is indistinguishable, from the
  # logs, from an idle one.
  function_alarms = merge([
    for key, name in local.alarmed_functions : {
      "${key}-errors"    = { function = name, metric = "Errors" }
      "${key}-throttles" = { function = name, metric = "Throttles" }
    }
  ]...)

  # The tables a visitor's request writes. Tokens holds operator sessions at
  # operator rates and is left out. Reads are left out too: a throttled read
  # surfaces as a 5XX on the API, which is alarmed below.
  alarmed_tables = {
    counters  = aws_dynamodb_table.counters.name
    prequeue  = aws_dynamodb_table.prequeue.name
    positions = aws_dynamodb_table.positions.name
  }
}

resource "aws_cloudwatch_metric_alarm" "function" {
  for_each = local.function_alarms

  alarm_name        = "${var.name_prefix}-${each.key}"
  alarm_description = "Lambda ${each.value.metric} on ${each.value.function}: invocations that failed or never ran, which the function's own log cannot report."

  namespace   = "AWS/Lambda"
  metric_name = each.value.metric
  dimensions = {
    FunctionName = each.value.function
  }

  statistic           = "Sum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # Lambda publishes nothing for a function that was not invoked.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

# The SDK retries a throttled write, so the first throttles cost latency rather
# than failures and nothing else reports them. They are the early sign of a
# table that was not pre-warmed for the cohort it is getting.
resource "aws_cloudwatch_metric_alarm" "table_write_throttles" {
  for_each = local.alarmed_tables

  alarm_name        = "${var.name_prefix}-${each.key}-write-throttles"
  alarm_description = "DynamoDB throttled writes to ${each.value}: the table is ramping behind the load. Raise warm_throughput_write_units before the next event."

  namespace   = "AWS/DynamoDB"
  metric_name = "WriteThrottleEvents"
  dimensions = {
    TableName = each.value
  }

  statistic           = "Sum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # Published only when a write is throttled.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

# Everything behind the API answering with a server error: a Lambda that
# failed or timed out, a join SQS refused (the 502 in main.tf), an integration
# API Gateway could not reach. A visitor sees a retry; nobody else sees
# anything.
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name        = "${var.name_prefix}-api-5xx"
  alarm_description = "The REST API answered 5XX: a function behind it failed or timed out, or SQS refused a join."

  namespace   = "AWS/ApiGateway"
  metric_name = "5XXError"
  dimensions = {
    ApiName = aws_api_gateway_rest_api.this.name
    Stage   = aws_api_gateway_stage.this.stage_name
  }

  statistic           = "Sum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # No requests, no datapoints.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}

# The controller's schedule could not deliver a fire (issue #228): the role
# lost its grant, the function was deleted or renamed. Its retry policy drops
# an undeliverable fire rather than queueing it, so this is the only trace. No
# controller means no admissions, and the queue page gives no sign of it.
#
# Scheduler publishes this per schedule group, not per schedule, and both
# schedules live in the default group -- the admin Lambda addresses them
# there. So this also covers the open schedule, which is wanted, and any other
# schedule in the account's default group in this region, which is noise worth
# having over silence.
resource "aws_cloudwatch_metric_alarm" "schedule_target_errors" {
  alarm_name        = "${var.name_prefix}-schedule-target-errors"
  alarm_description = "EventBridge Scheduler could not invoke a target in the default group: the controller (nobody is being admitted) or the open."

  namespace   = "AWS/Scheduler"
  metric_name = "TargetErrorCount"
  dimensions = {
    ScheduleGroup = aws_scheduler_schedule.controller.group_name
  }

  statistic           = "Sum"
  period              = 300
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # Published only when a delivery fails.
  treat_missing_data = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions

  tags = var.tags
}
