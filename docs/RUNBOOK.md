# Runbook

What an operator does before, during and after an event, and what to do when something goes
wrong. [`DEPLOY.md`](./DEPLOY.md) covers standing the stack up; this covers running it.

Every control here is a form on the dashboard at `https://<host>/admin`. There is no capability
that needs the AWS console, and every core action works with JavaScript disabled — a confirmation
dialog is the only thing JavaScript adds.

**One deployment serves one event and is torn down afterwards.** Nothing here is about running a
second event on the same stack; for that, deploy again.

---

## The controls, and what each one actually does

The dashboard's controls differ mostly in **what the waiting visitor sees**, which is the only
way to choose between them under pressure.

| Control | What it does | What a waiting visitor sees |
|---|---|---|
| **Set rate** | Changes the target admission rate the controller works toward | Nothing directly; the queue moves faster or slower |
| **Pause** | Holds admission. The queue keeps forming and everyone keeps their place | "You keep your place" — still queued, not moving |
| **Resume** | Releases the hold at the configured rate | The queue starts moving again |
| **Fail open** | Opens the gate for a fixed number of minutes: every request reaches the origin, admitted or not | Nothing — they are through |
| **Recover** | Ends the fail-open window early | Back to the queue, unless they were admitted meanwhile |
| **Force maintenance** | The full stop. The event is down | An outage page, not a queue |
| **Open now** | Opens the event immediately instead of waiting for the scheduled start | Their place in line, and the queue starts moving |
| **Set message** | Publishes a line of text to the waiting page | The message, on the next poll (within ~5 s) |
| **IP binding** | While on, an admitted visitor's pass only works from the network it was issued to (IPv4 address, or IPv6 /64), so it cannot be handed around | Nothing, unless their address changes: then one trip through the waiting page, which lets them straight back in |

Two pairs are easy to confuse:

- **Pause is not Force maintenance.** Pause holds the line; maintenance takes the event down.
  Reach for pause when the origin is struggling and for maintenance when it is gone.
- **Recover is not Resume.** Recover clears the fail-open window. If admission was paused before
  fail-open was engaged, it is still paused after Recover — you will need Resume as well.

Every action is stamped with who did it and when, shown on the dashboard as "last changed by X
at T". A second press of the same control within a few seconds is rejected as too fast; this
defeats a double-click, not you.

---

## Before the event

### Readiness checklist

The dashboard's **Readiness** panel, at the top of the page above Current state, checks the
items below against the running deployment every time the page loads: each row names the
requirement it protects, shows the measured value, and on a failing or warning row links to
where the fix happens. It changes nothing. A row it could not read says "could not evaluate"
and why, which is not a pass.

The quota and pre-warm rows (O1, O2) need lead time measured in days, not minutes: a quota
increase still pending at T−0 means the load test measured throttling rather than the design,
and pre-warming takes effect asynchronously. The panel shows the limits in force, not a request
that is still pending.

Checked by the panel (this list is generated from `crates/admin/src/readiness.rs`; a test fails
if the two differ — regenerate with `cargo test -p admin -- --ignored regenerate_runbook_checklist`):

<!-- readiness-checks:begin (generated from crates/admin/src/readiness.rs; do not edit) -->
- [ ] **Tables pre-warmed** (O1). All four tables report warm throughput at or above the configured `warm_throughput_write_units` / `warm_throughput_read_units`, and `PreQueue` at or above the 10,000/s registration rate. The 4,000-unit AWS minimum is a floor, not a sized value; 0 configured means no pre-warm. Fix: Set `warm_throughput_write_units` (and `_read_units`) in `terraform.tfvars` at or above the event's target write rate and apply, the day before rather than the hour before: warming is asynchronous. ([fix](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/warm-throughput.html))
- [ ] **DynamoDB throughput quotas** (O2). `DescribeLimits` reports a per-table write limit above the 40,000-unit default and at or above the configured warm throughput. Fix: File a Service Quotas increase for DynamoDB table-level write throughput weeks ahead; an increase still pending at T-0 means the load test measured throttling. ([fix](https://console.aws.amazon.com/servicequotas/home/services/dynamodb/quotas))
- [ ] **API Gateway account throttle** (O2). The account's API Gateway steady-state rate is above the 10,000 requests/s default, which is what C3's 40,000 joins/s needs. Fix: File a Service Quotas increase for API Gateway throttle rate in this region, weeks ahead. ([fix](https://console.aws.amazon.com/servicequotas/home/services/apigateway/quotas))
- [ ] **assign_position reserved concurrency** (N9). The join consumer has reserved concurrency above zero, so one event cannot starve another and the queue drains at a known rate. Fix: Set `assign_position_reserved_concurrency` in the core module (the default is sized for 10,000/s) and apply. ([fix](https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html))
- [ ] **Open schedule armed** (F0.3). The one-time open schedule is enabled at a time still in the future, so the event opens on its own at T-0. Fix: Set the start time on the dashboard (Start time), in the timezone you mean. The schedule itself is in EventBridge Scheduler. ([fix](https://console.aws.amazon.com/scheduler/home#schedules))
- [ ] **Controller schedule running** (F3.2). The controller's `rate(1 minute)` schedule exists and is enabled; without it the queue forms and nobody is admitted. Fix: Re-apply Terraform, which creates the schedule enabled; check nobody disabled it in EventBridge Scheduler. ([fix](https://console.aws.amazon.com/scheduler/home#schedules))
- [ ] **Gate ruleset** (F0.6). The gate's KeyValueStore holds at least one protection rule and no fail-open window is active. An empty ruleset passes every request through, which looks exactly like a working deployment. Fix: Set rules under Protection rules on the dashboard, then load a protected URL in a private window and confirm you are sent to the waiting page. (the dashboard)
- [ ] **/status cache behaviour** (C4). The polled `/v1/status` behaviour has a Min TTL above zero and keeps cookies out of its cache key, so CloudFront collapses polls and origin load is independent of waiter count. Fix: Re-apply the edge module (`polled_min_ttl_seconds` must be at least 1); do not edit the cache policy in the console. ([fix](https://console.aws.amazon.com/cloudfront/v4/home#/policies/cache))
- [ ] **Gate on the protected behaviour only** (N7). The gate CloudFront Function is associated at viewer-request with the default (protected) behaviour and with no other: elsewhere it bills every poll and refuses joins. Fix: Re-apply the edge module; remove any function association added to another behaviour in the console. ([fix](https://console.aws.amazon.com/cloudfront/v4/home#/distributions))
<!-- readiness-checks:end -->

**Manual** — the panel cannot check these:

- [ ] **Load test executed at the event's target rate, report reviewed with the client** (O3).
      The pre-queue path is one write however large the cohort, so what needs testing is the
      live-join path and `/status` under polling load.
- [ ] **Cost modelled for this event** (O6), including the CloudFront plan tier.

Then, on the day (**manual**):

- [ ] **The stack answers.** `uv run scripts/smoke_test.py` registers, opens and verifies that no
      two visitors get the same position. It reads everything it needs from `terraform output`.
- [ ] **The dashboard loads and you can log in.** Check this early: the OIDC path is the one part
      of the system that can be misconfigured in a way that only shows up when you need it.
- [ ] **The gate covers the right paths.** The panel counts the rules; it cannot tell whether they
      are the right ones. Load a protected URL in a private window and confirm you are sent to the
      waiting page. **An empty ruleset passes every request through** — which looks exactly like a
      working deployment.
- [ ] **The admission rate is set to something the origin can serve**, not the seeded default.
- [ ] **Alarms have a destination.** The alarms below exist; check something is subscribed to
      them, or they fire into nothing.
- [ ] **Someone is watching who can act.** Every automatic behaviour in this system is
      conservative by design; the decisions that are not conservative are all yours.

### Deciding the admission rate

Set it to the requests per second the origin sustains, as a whole number. The controller releases
that many per second and corrects upward for no-shows, to at most twice the target — so the
origin must survive **twice** the number you enter, in the worst case. Rate 0 is rejected; use
Pause to stop admission.

---

## During the event

### The one thing to watch

The system's characteristic failure is not a crash. **It looks completely healthy while doing
nothing.** A stack with a clean `terraform plan`, no errors in the logs and not one visitor
admitted is the failure mode to expect, so the checks below are all "is anything moving",
not "is anything broken".

- The dashboard's serving counter should be advancing. If it is flat while the queue counter
  climbs, admission has stopped: check the phase, check Pause, check the rate is not zero.
- The queue counter should be climbing if visitors are joining. If it is flat during a burst,
  joins are being accepted and discarded — see below.

### If the queue is not moving

In order:

1. **Is the event open?** The dashboard says. A pre-queue event that was never opened has no
   permutation seed, and `/queue_num` answers "not yet open" to everyone. Use **Open now**.
2. **Is admission paused?** The badge at the top of the dashboard shows paused, open or fail
   open. Use **Resume**.
3. **Is the rate zero or unset?** Set it.
4. **Is the phase `active`?** If it is `maintenance`, recover from the Phase control.
5. **Is the `admission_control_unreadable` alarm firing?** The stored admission control could not
   be parsed, so the controller is holding admission deliberately. Set Pause and then Resume to
   rewrite the attribute.

### If the origin is in trouble

- **Lower the rate first.** It takes effect within one controller pass (about 10 seconds) and
  nobody loses their place.
- **Pause** if lowering is not enough. The queue keeps forming; everyone keeps their place.
- **Force maintenance** only if the origin is gone. Waiting visitors see an outage page rather
  than a queue, so this is the control that tells people to come back later.

### If the waiting room itself is in trouble

**Fail open**, for a bounded number of minutes. Every request reaches the origin, admitted or
not — which is the right trade when the alternative is that nobody reaches it at all. It expires
on its own; it does not need you to come back.

Nothing trips fail-open automatically, on purpose: the gate makes no network calls and cannot
observe the origin, and an alarm wired to open the gate would dump the entire queue onto the
origin the first time it flapped.

### If the AWS region is impaired

The queue, the API and the dashboard all live in the deployment region; the gate does not
([ADR-0034](adr/0034-single-region-failure-domain.md)). Visitors already admitted keep going, but
nobody new is let through, and **Fail open** on the dashboard is unreachable because the dashboard
is in the impaired region too.

Engage fail-open by writing the gate's config directly. This goes through CloudFront's global
control plane, not the region. The AWS CLI needs `awscrt` for the KeyValueStore's SigV4A signing
(`pip install awscrt`, or the CLI v2 installer, which bundles it).

```bash
KVS_ARN=$(terraform -chdir=infra/environments/dev output -raw gate_kvs_arn)
ETAG=$(aws cloudfront-keyvaluestore describe-key-value-store --kvs-arn "$KVS_ARN" --query ETag --output text)
aws cloudfront-keyvaluestore get-key --kvs-arn "$KVS_ARN" --key c --query Value --output text
# → {"v":1,"s":0,"f":0,"r":[...]}  Keep v, s and r exactly as printed; change only f.
UNTIL=$(( $(date +%s) + 30 * 60 ))  # 30 minutes; keep it bounded
aws cloudfront-keyvaluestore put-key --kvs-arn "$KVS_ARN" --if-match "$ETAG" --key c \
  --value '{"v":1,"s":0,"f":'"$UNTIL"',"r":[...the r array as printed...]}'
```

The window expires on its own. To end it early, put the document back with `"f":0`. The
dashboard does not show a fail-open engaged this way, and needs no cleanup afterwards. If the
region is still impaired when the window ends, write a new deadline. Queue state survives the
impairment: once the region returns, visitors whose pages stayed open keep their places.

### Opening early

**Open now** closes the pre-queue and opens the event immediately. It is the same operation the
schedule performs, guarded so that firing both does nothing twice — so a scheduled time that has
not arrived yet becomes a no-op rather than a second opening. It cannot be undone: everyone
waiting is assigned their place at that moment.

---

## The alarms, and what each one means

Each fires on a single occurrence, because none of these happen in normal operation. They are all
conditions the system would otherwise survive in silence.

| Alarm | What happened | What to do |
|---|---|---|
| `join_dropped` | A join reached SQS and was discarded: a malformed body, or an `event_id` that does not match this deployment. The visitor was told 200 and the queue stayed empty | Check `event_id` in `terraform.tfvars` against the event item. A mismatch drops **every** join this way |
| `join-dlq-not-empty` | A join was accepted, retried to exhaustion, and lost | Check `assign_position`'s logs for the failure. The messages are in the dead-letter queue and can be redriven |
| `arrival_record_failed` | An admitted visitor's arrival went uncounted | Cumulative and silent. Each one inflates the measured no-show rate, and the controller answers by releasing more people than the origin agreed to serve. Consider lowering the rate |
| `arrival_shard_draw_failed` | Same, one step earlier | As above |
| `admission_claim_failed` | The write that makes an admission count once did not land | Harmless for one visitor. If it is firing continuously, every poll of the waiting page counts another arrival, the measured no-show rate collapses toward zero, and the controller stops correcting |
| `admission_control_unreadable` | The stored admission control could not be parsed, so the controller is holding admission | The queue has stopped moving. Pause and Resume to rewrite the attribute |
| `rules_audit_failed` | The gate's ruleset changed but the audit stamp did not | The gate is correct; the dashboard's "last changed by" is stale. No visitor impact |
| `fail_open_audit_lost` | Fail-open was engaged or cleared but the audit stamp lost a race to a newer writer | The fail-open change took effect; the dashboard's "last changed by" is stale. No visitor impact |
| `open_event_error` | The open failed: most often the schedule fired while the event was not in the pre-queue phase, so nothing was opened | Check the event's phase. The scheduler retries for up to 10 minutes; once the phase is pre-queue a retry opens the event, or use **Open now** |
| `open-dlq-not-empty` | A scheduled open failed on every retry and was given up | The event is **not open**, and the one-time schedule has been used. Fix the cause (usually the phase), then **Open now** |

---

## After the event

1. **Move the phase to `post_event`** from the Phase control. Visitors see the event is over
   rather than a queue that never moves.
2. **Take what you need out of CloudWatch** before the stack goes. Log groups have 30-day
   retention, but they go with the deployment.
3. **`make destroy`.** The deployment exists for one event.

---

## What this system will not do for you

Stated here so it is not discovered mid-event:

- **Nothing trips fail-open automatically.** See above.
- **Nothing fails over to another region.** A regional impairment freezes the queue; fail-open
  from the command line is the way around it. See above.
- **Nothing moves the phase on its own** except the open, which runs at the scheduled time.
- **Sessions are not renewed.** A visitor still on the origin when `session_ttl_seconds` lapses
  is returned to the queue. Set it longer than the worst realistic time on the origin; there is
  no other control over this.
- **A session cookie is a bearer credential unless IP binding is on,** and it cannot be revoked
  either way. Binding stops a pass being redistributed across networks, not shared behind one
  NAT (an office, some carriers). Turning it on sends visitors whose address changes (Wi-Fi to
  mobile) back through the waiting page once; a visitor whose address changes constantly cannot
  stay admitted at all and is told to stay on one connection. Leave it off unless passes are
  being resold.
- **A request id alone no longer admits anyone:** redeeming one also needs the secret the
  visitor's browser joined with. A visitor who hands over both hands over their place.
- **Regenerating the signing key invalidates every session already issued.** Do it before an
  event opens, never during one.
