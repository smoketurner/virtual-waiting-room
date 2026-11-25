# Runbook

What an operator does before, during and after an event, and what to do when something goes
wrong. [`DEPLOY.md`](./DEPLOY.md) covers standing the stack up.

Every control is a form on the dashboard at `https://<host>/_wr/admin` and works without JavaScript.
One deployment serves one event and is torn down afterwards.

---

## The controls

| Control | What it does | What a waiting visitor sees |
|---|---|---|
| **Set concurrency** | Changes how many visitors the origin takes at once | The queue moves faster or slower |
| **Pause** | Holds admission; the queue keeps forming | Still queued, place kept, not moving |
| **Resume** | Releases the hold at the configured concurrency | The queue moves again |
| **Fail open** | For a fixed number of minutes, every request reaches the origin, admitted or not | Nothing; they are through |
| **Recover** | Ends the fail-open window early | Back to the queue, unless admitted meanwhile |
| **Force maintenance** | Full stop; the event is down | An outage page, not a queue |
| **Phase** | Moves the lifecycle phase (out of maintenance, to `post_event`) | The page for that phase |
| **Open now** | Opens the event immediately instead of at the scheduled start | Their place, and the queue starts moving |
| **Start time** | Sets the scheduled open and its timezone | A countdown during the pre-queue |
| **Set message** | Publishes a line of text to the waiting page | The message, within about 5 s |
| **Protection rules** | Replaces the gate's ruleset | Protected paths start or stop sending them to the queue |
| **IP binding** | An admitted visitor's pass works only from the network it was issued to (IPv4 address or IPv6 /64) | Nothing, unless their address changes: then one trip through the waiting page |

Two pairs are easy to confuse:

- **Pause is not Force maintenance.** Pause holds the line; maintenance takes the event down. Pause
  when the origin is struggling, maintenance when it is gone.
- **Recover is not Resume.** If admission was paused before fail-open, it is still paused after
  Recover. Press Resume as well.

Every action is stamped "last changed by X at T". Two changes within 2 seconds are rejected as too
fast, to defeat double-clicks; Force maintenance, Phase, Fail open and Recover are exempt.

---

## Before the event

### Readiness checklist

The dashboard's **Readiness** panel checks these on every page load and links each failing row to
its fix. It changes nothing, and "could not evaluate" is not a pass. Start the quota and pre-warm
items (O1, O2) days ahead; the panel shows limits in force, not pending requests.

<!-- Generated from crates/admin/src/readiness.rs; a test fails if it drifts. Regenerate with
`cargo test -p admin -- --ignored regenerate_runbook_checklist`. -->

<!-- readiness-checks:begin (generated from crates/admin/src/readiness.rs; do not edit) -->
- [ ] **Tables pre-warmed** (O1). All four tables report warm throughput at or above the configured `warm_throughput_write_units` / `warm_throughput_read_units`, and `PreQueue` at or above the 10,000/s registration rate. The 4,000-unit AWS minimum is a floor, not a sized value; 0 configured means no pre-warm. Fix: Set `warm_throughput_write_units` (and `_read_units`) in `terraform.tfvars` at or above the event's target write rate and apply, the day before rather than the hour before: warming is asynchronous. ([fix](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/warm-throughput.html))
- [ ] **DynamoDB throughput quotas** (O2). `DescribeLimits` reports a per-table write limit above the 40,000-unit default and at or above the configured warm throughput. Fix: File a Service Quotas increase for DynamoDB table-level write throughput weeks ahead; an increase still pending at T-0 means the load test measured throttling. ([fix](https://console.aws.amazon.com/servicequotas/home/services/dynamodb/quotas))
- [ ] **API Gateway account throttle** (O2). The account's API Gateway steady-state rate is above the 10,000 requests/s default, which is what C3's 40,000 joins/s needs. Fix: File a Service Quotas increase for API Gateway throttle rate in this region, weeks ahead. ([fix](https://console.aws.amazon.com/servicequotas/home/services/apigateway/quotas))
- [ ] **assign_position reserved concurrency** (N9). The join consumer has reserved concurrency above zero, so one event cannot starve another and the queue drains at a known rate. Fix: Set `assign_position_reserved_concurrency` in the core module (the default is sized for 10,000/s) and apply. ([fix](https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html))
- [ ] **Open schedule armed** (F0.3). The one-time open schedule is enabled at a time still in the future, so the event opens on its own at T-0. Fix: Set the start time on the dashboard (Start time), in the timezone you mean. The schedule itself is in EventBridge Scheduler. ([fix](https://console.aws.amazon.com/scheduler/home#schedules))
- [ ] **Controller schedule running** (F3.2). The controller's `rate(1 minute)` schedule exists and is enabled; without it the queue forms and nobody is admitted. Fix: Re-apply Terraform, which creates the schedule enabled; check nobody disabled it in EventBridge Scheduler. ([fix](https://console.aws.amazon.com/scheduler/home#schedules))
- [ ] **Gate ruleset** (F0.6). The gate's KeyValueStore holds at least one protection rule and no fail-open window is active. An empty ruleset passes every request through, which looks exactly like a working deployment. Fix: Set rules under Protection rules on the dashboard, then load a protected URL in a private window and confirm you are sent to the waiting page. (the dashboard)
- [ ] **/status cache behaviour** (C4). The polled `/_wr/v1/status` behaviour has a Min TTL above zero and keeps cookies out of its cache key, so CloudFront collapses polls and origin load is independent of waiter count. Fix: Re-apply the edge module (`polled_min_ttl_seconds` must be at least 1); do not edit the cache policy in the console. ([fix](https://console.aws.amazon.com/cloudfront/v4/home#/policies/cache))
- [ ] **Gate on the protected behaviour only** (N7). The gate CloudFront Function is associated at viewer-request with the default (protected) behaviour and with no other: elsewhere it bills every poll and refuses joins. Fix: Re-apply the edge module; remove any function association added to another behaviour in the console. ([fix](https://console.aws.amazon.com/cloudfront/v4/home#/distributions))
<!-- readiness-checks:end -->

**Manual**: the panel cannot check these:

- [ ] **Load test at the event's origin concurrency, report reviewed with the client** (O3). Test the
      live-join path and `/status` under polling load; the pre-queue open is one write at any size.
- [ ] **Cost modelled for this event** (O6), including the CloudFront plan tier.
- [ ] **Edge protection matches the plan** (N7, [ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)).
      On a flat-rate plan, `terraform -chdir=infra/environments/dev output flat_rate_plan_status`
      is `ACTIVE`, and a private-window load of the waiting page passes the browser check on
      `/_wr/verify.html` and joins. Decide whether the managed rules stay in Count.

On the day (**manual**):

- [ ] **The stack answers.** `uv run scripts/smoke_test.py` registers, opens and checks that no two
      visitors share a position.
- [ ] **You can log in to the dashboard.** Check early: OIDC misconfiguration only shows up when
      you need it.
- [ ] **The gate covers the right paths.** The panel counts rules; it cannot judge them. Load a
      protected URL in a private window and confirm you land on the waiting page. **An empty
      ruleset passes every request through**, which looks exactly like a working deployment.
- [ ] **The origin concurrency is what the origin can serve at once**, not the seeded default.
- [ ] **Alarms reach someone.** The alarms below notify only when `alarm_sns_topic_arn` is set to
      a topic with a subscriber. Without it they change state in CloudWatch and tell nobody.
- [ ] **Someone who can act is watching.**

### Deciding the origin concurrency and session length

Enter how many visitors the origin can serve at once, as a whole number. No more admitted visitors
than that hold a live session at any moment, and there is no burst beyond it, but the open admits
that many at once, so the origin must survive the whole limit arriving together. Each session holds
its slot for `session_ttl_seconds` (set at deploy, [`DEPLOY.md`](./DEPLOY.md#choosing-the-session-lifetime)),
whether or not the visitor is still there, so throughput is the concurrency divided by the session
length: at the 900 s default, a limit of 100 admits about 400 an hour. Lowering the limit below the
sessions already live admits nobody until enough of them have ended. 0 is rejected; use Pause to
stop admission.

---

## During the event

### What to watch

This system fails by **looking healthy while admitting nobody**. Watch for movement, not errors.

- **Serving counter flat while the queue counter climbs:** admission has stopped. See below.
- **Queue counter flat during a burst:** joins are being accepted and discarded. Check the
  `join_dropped` alarm.

### If the queue is not moving

In order:

1. **Is the event open?** An unopened pre-queue event has no seed, and `/queue_num` answers "not
   yet open" to everyone. Press **Open now**.
2. **Is admission paused?** The badge at the top shows paused, open or fail open. Press **Resume**.
3. **Is the origin concurrency unset?** Set it. With every slot holding a live session, the queue
   moves only as sessions end.
4. **Is the phase `maintenance`?** Move it back to `active` with the Phase control.
5. **Is `admission_control_unreadable` firing?** The controller is holding admission because the
   stored control could not be parsed. Pause, then Resume, to rewrite it.

### If the origin is in trouble

1. **Lower the origin concurrency.** It takes effect within one controller pass (about 10 s):
   nobody more is admitted until the live sessions fall below it. Sessions already live run to
   their end, and nobody loses their place.
2. **Pause** if that is not enough.
3. **Force maintenance** only if the origin is gone. Waiting visitors see an outage page.

### If the waiting room itself is in trouble

**Fail open** for a bounded number of minutes. Every request reaches the origin, admitted or not.
The window expires on its own. Nothing trips fail-open automatically
([ADR-0009](adr/0009-fail-open.md), [#58](https://github.com/smoketurner/virtual-waiting-room/issues/58)).

### If the AWS region is impaired

The queue, API and dashboard are in the deployment region; the gate is not
([ADR-0034](adr/0034-single-region-failure-domain.md)). Admitted visitors keep going, nobody new
is admitted, and the dashboard's **Fail open** is unreachable.

Engage fail-open by writing the gate's config directly, through CloudFront's global control plane.
The AWS CLI needs `awscrt` for SigV4A (`pip install awscrt`; the CLI v2 installer bundles it).

```bash
KVS_ARN=$(terraform -chdir=infra/environments/dev output -raw gate_kvs_arn)
ETAG=$(aws cloudfront-keyvaluestore describe-key-value-store --kvs-arn "$KVS_ARN" --query ETag --output text)
aws cloudfront-keyvaluestore get-key --kvs-arn "$KVS_ARN" --key c --query Value --output text
# → {"v":1,"s":0,"f":0,"r":[...]}   (may also carry "b" for IP binding)
# Keep every field exactly as printed; change only f.
UNTIL=$(( $(date +%s) + 30 * 60 ))  # 30 minutes; keep it bounded
aws cloudfront-keyvaluestore put-key --kvs-arn "$KVS_ARN" --if-match "$ETAG" --key c \
  --value '{"v":1,"s":0,"f":'"$UNTIL"',"r":[...the r array as printed...]}'
```

To end it early, write the document back with `"f":0`; if the region is still impaired at expiry,
write a new deadline. The dashboard does not show a fail-open set this way. Queue state survives
the impairment.

### If scripts are flooding the queue

With the edge web ACL on, each place already costs a browser challenge, and past
`waf_join_ip_limit` visitors on an address solve a CAPTCHA
([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)). Further levers are `terraform.tfvars`
variables, each one `make apply`, reaching the edge in about a minute:

- **Lower `waf_join_ip_limit`** so CAPTCHAs arrive sooner. Buyers behind a carrier NAT see more
  CAPTCHAs but are not locked out.
- **Set `waf_managed_rules_mode = "enforce"`** to let Bot Control, IP reputation and anonymous-IP
  rules act. Mid-event this skips the Count review (O5); do it only when the flood is worse than the
  false positives would be.
- **Set `nojs_enabled = false`** if the no-JavaScript queue is the path being abused. Visitors
  without JavaScript cannot queue while it is off.

None of this binds a place to a person.

### Opening early

**Open now** performs the same guarded open as the schedule, so the schedule firing later does
nothing. It cannot be undone: everyone waiting is assigned their place at that moment.

---

## The alarms

Each is named `<name_prefix>-<alarm>` and fires on a single occurrence; none happens in normal
operation.

| Alarm | What happened | What to do |
|---|---|---|
| `join_dropped` | A join reached SQS and was discarded (malformed body, or an `event_id` that does not match). The visitor got a 200 | Check `event_id` in `terraform.tfvars` against the event item. A mismatch drops **every** join |
| `join-dlq-not-empty` | A join was received 10 times without being written and was dead-lettered | Check `assign_position`'s logs. The messages are in the dead-letter queue and can be redriven |
| `arrival_record_failed` | A unit of the admission budget could not be taken, so the visitor was **not** admitted and will ask again | Harmless once. If continuous, admission has stopped: check `generate_token`'s logs and its grant on the `Counters` table |
| `arrival_shard_draw_failed` | Same, one step earlier | As above |
| `admission_claim_failed` | The write that marks a visitor admitted did not land; they were admitted anyway | Harmless once. If continuous, every reload takes a second unit of budget, so fewer people are admitted than the concurrency allows, never more |
| `admission_control_unreadable` | The stored admission control could not be parsed; the controller is holding admission | The queue has stopped. Pause, then Resume |
| `rules_audit_failed` | The ruleset changed but its audit stamp did not | Gate is correct; "last changed by" is stale. No visitor impact |
| `fail_open_audit_lost` | Fail-open changed but its audit stamp lost a race | Change took effect; "last changed by" is stale. No visitor impact |
| `nojs_join_failed` | A no-JavaScript join was not enqueued | The visitor was told and can retry. If continuous, check the `nojs` function's logs and its SQS grant |
| `open_event_error` | The open failed, usually because the event was not in the pre-queue phase | Check the phase. The scheduler retries for up to 10 minutes; or fix the phase and **Open now** |
| `open-dlq-not-empty` | A scheduled open failed every retry | The event is **not open** and the one-time schedule is spent. Fix the cause, then **Open now** |
| `join-queue-age` | The oldest join has waited over 60 s: `assign_position` is not keeping up, or is failing | Check its `Errors`/`Throttles` alarms and its reserved concurrency. Joins are safe in the queue; visitors see "Getting your place" until it drains |
| `<function>-errors`, `<function>-throttles` | A function failed or was throttled (one pair per function) | Read that function's log. A throttle on `read` or `generate_token` is the account concurrency limit; on `assign_position` or `nojs`, its reservation |
| `counters-write-throttles`, `prequeue-write-throttles`, `positions-write-throttles` | DynamoDB refused writes on that table | The table was not pre-warmed for this rate. Raise `warm_throughput_write_units`; throttled joins retry from the queue |
| `api-5xx` | The REST API answered a 5XX | Check the function alarms; a `/_wr/v1/join` 5XX means SQS refused the message |
| `schedule-target-errors` | EventBridge Scheduler could not invoke a target. The metric is for the default schedule group, so any schedule in it counts | Usually the controller: nobody is being admitted. Check the schedule's role and the function's reserved concurrency (#228) |

These are logged (`$.fields.event`) without an alarm of their own:

| Event | What happened | What to do |
|---|---|---|
| `controller_pass_failed` | One controller pass failed and was skipped; the next computes the same free slots from the same state | Harmless once. If continuous the queue has stopped: check the `controller` function's logs |
| `open_drain_timed_out` | The join queue did not empty within 15 s, so the open went ahead; joins still queued are numbered behind the cohort | Check `assign_position`'s concurrency and the tables' pre-warming before the next event |
| `open_drain_unreadable` | The open could not read the join queue and did not wait for it | Check `open_event`'s `sqs:GetQueueAttributes` grant |
| `nojs_join_resent` | A no-JavaScript join had not registered after 60 s and was sent again | Expected occasionally. Many at once means the join consumer is behind |
| `nojs_join_lost` | A no-JavaScript join never registered after three resends; the visitor was shown the form again | Check the join queue, its dead-letter queue and `assign_position` |
| `admin_login_refused` | A login was refused: not on the allowlist, or the provider did not say the email is verified | If every login is refused for `email_verified`, the provider omits the claim: set `oidc_require_email_verified = false` |
| `admin_session_refused` | A session's email is no longer on the allowlist | Expected after removing an operator |
| `admin_csrf_refused` | A state-changing request lacked the session's `csrf` field | A stale dashboard tab (reload it), or a request another page made |

**Visitors reporting "We let you in, but the site turned you back".** The gate refused a session
`generate_token` had just minted, twice within a minute, and the page stopped rather than loop.
The usual cause is a signing key two generations old (rotated twice within one session
lifetime, so the gate no longer holds it as `kp`); with IP binding on, it is a visitor whose
network keeps changing.

**`/_wr/admin/rules` rejecting a path prefix.** A prefix containing `%`, `//`, a `.` or `..` segment,
`;` or a backslash can never match a normalised request path, so it is refused when the rule is
written rather than accepted and silently ignored.

---

## After the event

1. **Move the phase to `post_event`** so visitors see the event is over.
2. **Export what you need from CloudWatch.** Logs are kept 30 days but are deleted with the stack.
3. **Review the web ACL's Count metrics**, if it was on. The per-rule metrics are under
   `<name_prefix>-*` and decide whether the managed rules are promoted to `enforce` next time (O5).
4. **`make destroy`.** On a flat-rate plan the first run cancels the plan and fails on the
   distribution; run it again after the billing cycle ends ([`DEPLOY.md`](./DEPLOY.md#tear-down)).

---

## What this system will not do for you

- **Trip fail-open, fail over to another region, or change phase on its own** (except the
  scheduled open).
- **Renew sessions.** A visitor still on the origin when `session_ttl_seconds` lapses goes back to
  the queue. Set it longer than the worst realistic time on the origin.
- **Revoke a session.** A session cookie is a bearer credential unless IP binding is on. Binding
  stops a pass moving between networks, not sharing behind one NAT, and sends a visitor whose
  address changes back through the waiting page once ([ADR-0036](adr/0036-optional-session-ip-binding.md)).
  Leave it off unless passes are being resold.
- **Ration places per person.** The web ACL makes places cost a challenge; a farm of real browsers
  across many addresses still takes them. A request id alone admits no one (it also needs the
  secret the browser joined with, [ADR-0035](adr/0035-request-id-proof-of-possession.md)), but a
  visitor who hands over both hands over their place.
- **Survive two signing-key rotations.** Raising `signing_key_generation` once keeps issued
  sessions valid until they expire (the gate still accepts the previous generation); raising it
  again inside one session lifetime ends the oldest. See
  [`DEPLOY.md`](./DEPLOY.md#rotating-keys).
