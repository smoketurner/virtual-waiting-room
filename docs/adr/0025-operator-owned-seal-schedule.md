# ADR-0025: The operator owns the seal schedule's time; Terraform owns the schedule

**Status:** Accepted

## 1. Context

The event's start time was `seal_start_time`, a Terraform variable that decided whether an
EventBridge Scheduler schedule existed at all. Changing when an event opened meant editing
`terraform.tfvars` and running `make apply`.

That made scheduling — the product's headline mode — the one operator control not on the control
plane. Phase, rate, message, pause, fail-open and rules are all forms on the dashboard. It also
left the visitor half unbuildable: nothing stored the start time where a reader could reach it, so
`/status` had no instant to publish and the waiting page could only say "the event isn't open yet"
where the requirements promise a countdown.

## 2. Decision

**Terraform owns the schedule; the operator owns its time.**

Terraform creates `aws_scheduler_schedule.seal` unconditionally, with its target, target input,
retry policy and IAM role, in state `DISABLED` and carrying a far-future placeholder expression.
`schedule_expression`, `schedule_expression_timezone` and `state` are under `ignore_changes`. The
admin Lambda writes exactly those three fields and nothing else.

The operator also picks the **timezone**, from a dropdown beside the time. The zone is stored and
sent to Scheduler rather than folded into a UTC instant, because an offset chosen when the event is
scheduled is wrong on the far side of a daylight-saving change — a 10am onsale scheduled in
February must still open at 10am local in June.

Setting a time writes two places: `Counters.starts_at` (a UTC epoch, plus `starts_at_tz`), which
`/status` publishes and the waiting page counts down to, and the schedule, which actually fires the
seal.

## 3. Why not the alternatives

**Why not let the admin create the schedule?** It would be simpler HCL — no placeholder expression,
no `ignore_changes` — and the IAM cost is not the objection, since `scheduler:CreateSchedule` can be
scoped to one ARN. Teardown is. These are ephemeral per-event deployments, and a schedule the admin
created is invisible to Terraform state, so `make destroy` would leave an armed schedule behind in
the customer's account on every event. The retry policy and target input would also move out of HCL
into Rust, which is the wrong home for infrastructure configuration.

**Why not have the controller fire the seal?** It already runs six passes a minute and the seal is
idempotent, so it could check the clock and invoke. That avoids the SDK dependency and the
`PassRole` grant entirely. Rejected because it folds event lifecycle into the outflow-metering loop,
which is a scope blur, and because the controller's schedule only exists when its artifact is built.

**Why not have `read` call `GetSchedule` instead of mirroring `starts_at` into `Counters`?** That
would be a control-plane API call on the polled path. `/status` is edge-cached with request
collapsing precisely so origin load is independent of how many people are waiting; a `GetSchedule`
behind it would be throttled long before the event opened.

## 4. `UpdateSchedule` replaces, so the writer reads first

`UpdateSchedule` is not a patch. Every field left unset reverts to its service default, which would
silently drop the target's `input` — the event id the seal reads — and its retry policy. The writer
therefore calls `GetSchedule`, changes only the expression, timezone and state, and resends the
whole definition. The target moves as one value rather than field by field, so a field added to it
later cannot be forgotten.

This failure mode is invisible by default, because the provider's retry-policy defaults are also the
service's: a dropped block and a preserved one look identical. The Terraform therefore declares a
**deliberately non-default** retry policy (600 seconds, 10 attempts), which is what makes the
regression assertable in a test. A bounded age is correct on its own terms too — a seal delivered 24
hours late would open the event a day late, which is worse than not opening it.

## 5. Write ordering

Two stores describe one fact, so the order is fixed to make the residue of a half-completed change
the harmless one:

> **The schedule may be armed without an announcement; an announcement must never outlive its
> schedule.**

Setting arms the schedule first, then writes `starts_at`. Clearing removes `starts_at` first, then
disables the schedule. Every crash window therefore leaves "armed, unannounced" — which is exactly
how the system behaved before start times existed, and self-corrects at the seal when the phase
flips and the page leaves `closed`. The opposite order would leave a page counting down to a moment
at which nothing fires.

The debounce is evaluated in the action before either write, not left to the store's own conditional
update. A double-submit rejected by DynamoDB *after* the schedule had moved would leave the two
describing different times for the rest of the window.

`scripts/reset-env.py` disables the schedule as part of a reset for the same reason: it rewrites
`Counters` without a start time, and an armed schedule would seal a cohort whose rows it just
deleted.

## 6. The `PassRole` grant

`UpdateSchedule` resends the target's `RoleArn`, so the admin needs `iam:PassRole`. This is a real
step up from the read-only grants it otherwise holds, and it is scoped two ways: to that one role
ARN, and by an `iam:PassedToService` condition naming Scheduler — not the `role/*` the AWS example
uses.

What the grant is worth to an attacker is bounded by the role itself. Its entire policy is a single
`lambda:InvokeFunction` on `seal_event`. With no `CreateSchedule` or `DeleteSchedule` granted, the
worst available is re-pointing one existing schedule at a target that role cannot invoke.

## 7. Consequences

- An operator schedules and reschedules an event without a deploy, and `terraform plan` afterwards
  is clean.
- Clearing a start time disables the schedule rather than deleting it — enforced structurally: the
  `SealSchedule` port has no delete operation and the IAM policy grants none.
- The placeholder expression in the HCL is permanently not the real value. This is the accepted cost
  of `schedule_expression` being a required argument on a resource that must always exist.
- `admin` gains `aws-sdk-scheduler` and `jiff` (tz database compiled in, since `provided.al2023`
  does not guarantee one on disk). Both are control-plane only and neither touches the admission
  path's `aws-lc-rs` requirement.
- Dropping `count` moves the three seal resources' addresses, so the first apply after this change
  replaces them. Harmless here — the schedule is disabled and holds no state — but it is a
  destroy-and-create in the plan, not an update.
