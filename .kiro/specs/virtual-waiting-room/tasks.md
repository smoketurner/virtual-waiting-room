# Tasks: Virtual Waiting Room

The build-state record for requirements.md. Requirement IDs in brackets; decisions in
[`docs/adr/`](../../../docs/adr/). An item only partly delivered stays unchecked with a
`Partial:` note saying what is missing.

---

## Phase 1 — Core

### 1a. Data and counter

- [x] Four tables (`Counters`, `PreQueue`, `Positions`, `Tokens`), on-demand, PITR, `warm_throughput_*` as variables [O1]
- [x] `Counters` event item: sequences, `calls`, `phase` (incl. `maintenance`), `origin_concurrency`, `shuffle_seed`, `message`, `admission_control` + `fail_open_until`; `prequeue_counter` and the deferred-join count striped ×10, and the arrivals counters as 26 shelves ×10, all as separate items (ADR-0015, ADR-0043)
- [x] Batch range claim via one `UpdateItem ADD` / `ALL_NEW`, incremented by the **valid** count only [F2.2, F2.6]
- [x] Every position write guarded by `attribute_not_exists(request_id)` [F2.5]
- [ ] `max_throughput_*` table variables (DESIGN §5.5) — Partial: not built.

### 1b. Event lifecycle and modes

- [x] Phase state machine: idle → pre-queue → active → post-event, plus maintenance [F0.1, F0.8]
- [x] Scheduled open at a configured start time; `/_wr/admin/open_now` opens early through `open_event` (ADR-0025) [F0.3]
- [x] Protection rules (path, header, cookie, user agent) evaluated per request by the edge gate; an empty ruleset passes everything through (dormancy) — [#60](https://github.com/smoketurner/virtual-waiting-room/issues/60), [#66](https://github.com/smoketurner/virtual-waiting-room/issues/66) [F0.6]
- [ ] Operator-authored page per phase, CDN-cached [F0.2] — Partial: one waiting page whose text follows the phase; no operator-authored page per phase.
- [ ] Standby mode: CloudWatch alarm on `AWS/CloudFront` `Requests` → EventBridge → phase Lambda, ~125 s worst-case activation [F0.4, F0.7]
- [ ] Scheduled and standby coexisting on one origin [F0.5]

### 1c. Pre-queue

- [x] Registration writes `PreQueue {r, s, l, t}` with `attribute_not_exists(r)`; striped local-index counter for the ~10,000/s registration ceiling (ADR-0015) [C1, F2.5]
- [x] Open at T−0 in one conditional `UpdateItem` (seed, prefix offsets, cohort size, phase) guarded by `attribute_not_exists(shuffle_seed) AND phase = pre_queue`; position is `PRP(seed, offset[s] + l, N)` on read (ADR-0002) [F1.3, F1.4, F1.5, C2]
- [x] Feistel PRP with property tests: bijectivity (exhaustive for N < 2000), in-domain, determinism, uniformity; contiguous shard assembly; burned index; straggler racing the open, judged by its shard's own issued count [F1.5, F2.3]
- [x] `/status` carries the phase, and after T−0 the seed, `participant_count` and `prequeue_offsets` (Min TTL 1 s, no cookies forwarded)
- [ ] Countdown page served from CDN cache with zero origin calls per view [F1.1, F1.2] — Partial: the countdown to `starts_at` renders on the polled waiting page ([#128](https://github.com/smoketurner/virtual-waiting-room/issues/128)), not on a separate cached page.

### 1d. Live join

- [x] REST API `AWS` integration → SQS `SendMessage`, JSON Schema validator, DLQ (`maxReceiveCount` 10), `ReportBatchItemFailures`, visibility timeout ≥ 6× function timeout + window [F2.1, F2.4, C5]
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` as variables — Partial: hardcoded at 100 / 1 s.
- [ ] SQS ESM Provisioned Mode as an opt-in variable, default off
- [ ] Per-event partition: one queue and one function per event, each with reserved concurrency (ADR-0008) [N9] — Partial: `assign_position` has reserved concurrency; a deployment serves one `event_id`.

### 1e. Read path

- [x] `/_wr/v1/status` (phase, serving position, origin concurrency and session length, message in one payload) and `/_wr/v1/queue_num`, served by the `read` Lambda [F3.1]
- [x] Adaptive poll interval from a Terraform-set `poll_policy` — [#69](https://github.com/smoketurner/virtual-waiting-room/issues/69) [N10, ADR-0023]

### 1f. Admission, session, and outflow control

- [x] Signing key from `random_bytes.signing_key`, written by Terraform to both the SSM SecureString and the gate's KeyValueStore `k` (issue #71)
- [x] `/_wr/v1/generate_token`: checks position against `serving_counter` (via `resolve`), claims the admission once per visitor (ADR-0033), takes the visitor's held spot from their call's shelf, answers a missed turn 410 `turn_missed`, mints the HMAC-SHA256 session [F3.3, F3.8]
- [x] Gate as a CloudFront Function at viewer-request on the protected behaviour, configured from one KeyValueStore; shapes its own refusals (302 navigation, 403 XHR) — [#64](https://github.com/smoketurner/virtual-waiting-room/issues/64) [F3.4, ADR-0021]
- [x] Session scoped per event, optional IP binding — [#61](https://github.com/smoketurner/virtual-waiting-room/issues/61) (ADR-0036); `request_id` redeemable only with its possession secret — [#62](https://github.com/smoketurner/virtual-waiting-room/issues/62) (ADR-0035) [F3.5]
- [x] Cross-language credential and rule conformance vectors checked against the shipped gate function [ADR-0021 §6]
- [x] `StoredControl` + `fail_open_until`; `/_wr/admin/fail_open` and `/_wr/admin/recover` mirrored to the gate (ADR-0009, ADR-0019)
- [x] Controller metering origin concurrency from an admission log, 10 s cadence as a durable function (ADR-0022, ADR-0041) [F3.2, F3.8]
- [x] Held turns, a 15 s draw window, and join order after it (ADR-0043): each pass calls as many places as there are free spots and holds a spot for each on its own shelf for 2 minutes (longer at the open); a missed turn's spot is called for the next place and the waiting page takes a new place at the back; a join is in the draw by its `SentTimestamp` against `starts_at + 15 s`; a join after the cutoff that arrives before the open is sent back for 2 s doubling and counted for the open's drain; live batches are numbered by `SentTimestamp`; Open now moves `starts_at` to the press. Checked by `make sim` [F1.3, F2.1, F3.8]
- [ ] Automatic fail-open on a backend outage — [#58](https://github.com/smoketurner/virtual-waiting-room/issues/58) [F4.1, F4.2, F4.3] — Partial: the time-limited mechanism is built and operator-engaged; nothing trips it automatically.
- [ ] Revoke an admission — [#63](https://github.com/smoketurner/virtual-waiting-room/issues/63) — no design chosen (ADR-0021 §5.2).
- [ ] Concurrency term in the control law — [#65](https://github.com/smoketurner/virtual-waiting-room/issues/65) — Partial: the controller keeps at most `origin_concurrency` sessions live, counting each as live for its whole length (ADR-0041); nothing reports completion or abandonment, so a visitor who leaves early holds their spot until the session ends.
- Retired: separately signed token (F3.6), sliding session (F3.7), session completion (F3.10) — ADR-0032; position expiry (F3.9) — ADR-0031.

### 1g. Abuse mitigation

- [ ] Bound how many positions one visitor can hold — [#59](https://github.com/smoketurner/virtual-waiting-room/issues/59) — Partial: server-drawn shards, reload dedupe and the edge web ACL's per-token join limit, a one-hour token life and a required token cookie raise the price per place; WAF has no lifetime quota per token, so nothing bounds volume per person.
- Retired: entry tickets (F6.1, F6.2) — ADR-0028; open-time demotion (F6.3) — ADR-0030.

### 1h. Operator surface

- [x] Operator message in the `/status` payload [F5.3]
- [x] Position and estimated wait from the measured speed of now serving, falling back to origin concurrency per session length, on the waiting page [F5.4]
- [x] Admin actions: phase, concurrency, message, start_time, open_now, force_maintenance, pause, resume, fail_open, recover, rules, ip_binding
- [ ] Every operator action scriptable, no console dependency [F5.5] — Partial: every action is an HTTP endpoint, but the only auth is the browser OIDC session, and every POST needs that session's `csrf` form field and a same-origin request (#243); no machine-caller path (ADR-0016).
- [x] Admin UI: one Axum Lambda, askama templates, one self-contained stylesheet (ADR-0018), no React [F7.1, F7.2, F7.5]
- [x] UI is a thin client over the same `/_wr/admin/*` actions [F7.3]
- [x] OIDC Authorization Code + PKCE sessions in DynamoDB, enforced in the Lambda (ADR-0016) [F7.4]
- [x] HTML forms work with JavaScript disabled; a small poller refreshes current state [F7.5]
- [x] askama template tests; no SPA bundle emitted
- [x] Pre-event Readiness panel, generating the RUNBOOK checklist — [#70](https://github.com/smoketurner/virtual-waiting-room/issues/70) [O1, O2, N7] — Not built by choice: WAF and GovCloud rows.
- [ ] Live metrics via EMF → CloudWatch: queue depth, admitted, no-show rate; inflow from `AWS/CloudFront` `Requests` [F5.1]
- [ ] Admin dashboard metrics view (inflow, outflow, queue depth, admitted, no-show rate) [F7.6, F5.1]
- [ ] Brandable waiting page from client-supplied assets [F5.2]
- [ ] `enforce_from` settable from the admin API — Partial: Terraform-only.

### 1i. No-JavaScript queue

- [x] `<noscript>` form posts to `/_wr/v1/enter`; `/_wr/v1/wait` is a server-rendered, self-refreshing page admitting through `generate_token` — [#67](https://github.com/smoketurner/virtual-waiting-room/issues/67) (ADR-0037)

**Exit:** endpoints correct; pre-queue assigns fairly and reproducibly; a visitor browses many pages on one session; operator steers a live event from API and UI; fail-open engages and the edge honours it.

---

## Phase 2 — Terraform module

- [x] `modules/core` — DynamoDB, SQS, Lambdas, IAM, REST API + validator, the gate's KeyValueStore [N5]
- [x] REST API answers only CloudFront: every method requires the origin API key (ADR-0038) [N7]
- [x] Flat-rate plan subscription via `flat_rate_plan_tier` (ADR-0038) [O6]
- [ ] `modules/edge` — three cache behaviours (ADR-0013), the gate, and WAF [N7, O5, C4] — Partial: all built, with the web ACL behind `flat_rate_plan_tier` and managed rules in Count; ASN matching is not built.
- [ ] CloudWatch alarms and a shipped dashboard — Partial: alarms on log-metric filters, both DLQs, join-queue age, function errors and throttles, table write throttles, API 5XX and the controller schedule are built, with an optional SNS topic; no dashboard.
- [ ] `var.enable_vpc` for ATO-constrained clients — Partial: not built; no VPC variable exists.
- [ ] OpenAPI specification for public and admin surfaces [N8]
- [ ] `examples/` and generated variable reference
- [ ] Verify idle monthly cost under $5 [N1]
- [ ] Confirm no component runs outside the client's account [N3]

**Exit:** `terraform apply` from a clean account to a working deployment [N2].

---

## Phase 3 — Load validation

- [ ] Repeatable load harness [O3] — Partial: `make stack` runs the shipped binaries, gate and page against the AWS Emulator and `make sim` runs the functions' logic on simulated time at real scale, both checking the design's claims, origin requests per waiter included ([LOCAL_STACK.md](../../../docs/LOCAL_STACK.md)) (C4, N10); nothing generates load against a deployment yet.
- [ ] Pre-queue: 1M registrations; bijectivity over the full cohort, no correlation with registration time, seed absent before T−0 [F1.3, F1.4, C1, C2]
- [ ] Live join: 10K/s at default quotas, 40K/s with increases; zero duplicates; gap rate measured [F2.2, C3]
- [ ] Raise quotas and pre-warm before testing above defaults [O1, O2]
- [ ] `/status` origin RPS flat from 10K to 1M waiters [C4]
- [ ] Fail-open: waiting room returning 5xx, origin reachable [F4.1]
- [ ] Session continuity across N pages [F3.5]
- [ ] No-show: with 30% no-shows the origin's spots stay nearly full [F3.8]
- [ ] Standby activation within ~125 s; unprotected paths unqueued [F0.4, F0.6]
- [ ] Spike in < 5 s drops no joins [C5]
- [ ] Event isolation under one event at its ceiling [N9]
- [ ] Cold-start P50 with `opt-level` 3 vs `z` (the jitter-entropy flag is already set — `.kiro/steering/tech.md`)

**Exit:** reproducible report.

---

## Phase 4 — Operational product

- [ ] Pre-event readiness checklist [O1, O2, O3] — Partial: `docs/RUNBOOK.md` carries it, generated from the Readiness panel; no rollback plan.
- [x] Reference client (`waiting.js`): countdown, position, ETA, auto-advance, backoff on a refused join, re-join on 404 [F4.4, F4.5]
- [ ] Operator runbook [O4, O5] — Partial: `docs/RUNBOOK.md` is written; never rehearsed.
- [ ] Single-region failure domain — [#68](https://github.com/smoketurner/virtual-waiting-room/issues/68) — Partial: documented (ADR-0034, `docs/DEPLOY.md`, break-glass in `docs/RUNBOOK.md`); not rehearsed against a deployment.
- [ ] Client integration guide: CloudFront/ALB/CDN placement
- [ ] Per-client cost model, including the flat-rate vs PAYG crossover and pre-warming [O6]

---

## Phase 5 — GovCloud variant

No CloudFront, no edge compute, no VPC origins; priced separately.

- [ ] A gate for an internal ALB; origin access via security groups and IAM [N4]
- [ ] Replacement for CDN cache collapse on `/status`
- [ ] Document the commercial-CloudFront-fronting-GovCloud data-boundary question for the AO
- [ ] Validate deploy in a real GovCloud account

---

## Deferred

Out of scope for this release (REQUIREMENTS §5).

- Invite-only rooms with MFA gating
- Proof-of-Work and CAPTCHA softblock before queue entry
- Native SDKs (iOS, Android, React Native)
- Connector breadth beyond the CloudFront gate
- OpenID identity-provider adapter
- Multi-region and global tables
- CloudFront SaaS Manager variant for many branded domains

---

## Risks

| Risk | Mitigation |
|---|---|
| Pre-queue randomization disputed as unfair | Recorded seed makes it auditable [F1.5] |
| Poll interval drives cost | Adaptive (ADR-0023); modelled per client [N10, O6] |
| Harness cannot generate 1M participants from one source | Distributed harness, budgeted in Phase 3 |
| GovCloud variant larger than estimated | Phase 5, priced separately |
| On-call burden during a live event | Price as incident-critical; cap concurrent engagements |
