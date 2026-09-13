# Tasks: Virtual Waiting Room

Delivers requirements.md per design.md; requirement IDs in brackets; phases ordered by dependency and risk; decisions in ../../../docs/adr/.

---

## Phase 0 — Spike

Throwaway code. Measures what documentation cannot settle.

- [ ] REST API `type: aws` → SQS with a request validator; confirm client UUIDv7 survives to the Lambda and malformed bodies are rejected with 400 [F2.4]
- [ ] Concurrent `UpdateItem ADD :n` / `ALL_NEW` against one item: assert zero duplicate positions across allocated ranges; measure gap rate under induced 5xx [F2.2, F2.3]
- [ ] Rust cold start on `provided.al2023` arm64 with an AWS SDK client — decides whether provisioned concurrency is needed. Measure the `aws-lc-rs` CPU-jitter-entropy init tax (aws-lc-rs ≥ 1.14.1 adds several ms–~1 s at process init) and compare mitigations: an **init-phase warm-up TLS handshake** (a cheap call like `list_tables` so the tax lands on boosted Init CPU, no entropy source dropped) versus building with `AWS_LC_SYS_NO_JITTER_ENTROPY=1` (removes it outright but drops one defense-in-depth entropy source — a security trade-off we lean against given the GovCloud/FIPS posture). Also A/B `opt-level` 3 vs `z` on cold-init P50 — scenario-dependent, decide by measurement, not a priori. Sources in the knowledge base (lambdabench.dev/rust, smithy-rs #4541)

**Exit:** findings written down. Nothing else kept.

---

## Phase 1 — Core

### 1a. Data and counter

- [x] Four tables — `Counters`, `PreQueue`, `Positions`, `Tokens` — on-demand, PITR, `warm_throughput_*` and optional `max_throughput_*` as variables (DESIGN §6.7) [O1] — Note: no `max_throughput_*` variables yet.
- [x] `Counters` attributes: `queue_counter`, `serving_counter`, `max_expired_position`, `arrivals#0..9`, `phase`, `phase_override`, `target_rate`, `shuffle_seed`, `operator_message` — Note: `phase_override` is instead the `Phase::Maintenance` variant, and `admission_control` (ADR-0019) replaces the `admission_paused`/`fail_open` pair.
- [x] Batch range allocation via `UpdateItem ADD` / `ALL_NEW`; increment by **valid** count only [F2.2, F2.6]
- [x] `PutItem` with `attribute_not_exists(request_id)` on every position write [F2.5]

### 1b. Event lifecycle and modes

- [x] Phase state machine: idle → pre-queue → active → post-event, plus maintenance [F0.1, F0.8]
- [ ] Operator-authored static page per phase, CDN-cached [F0.2]
- [ ] Protection rules (path, header, cookie, user agent), evaluated locally at the authorizer [F0.6] — Partial: path-prefix matching only (`PROTECTED_PATH_PREFIXES`); header, cookie, and user-agent matching outstanding.
- [ ] Standby mode: CloudWatch alarm on `AWS/CloudFront` `Requests` (60 s period, `us-east-1`) → EventBridge → phase Lambda. Activation latency ~125 s worst case [F0.4, F0.7]
- [ ] Scheduled and standby coexisting on one origin [F0.3, F0.5]

### 1c. Pre-queue

- [ ] Static countdown page, CDN-cached, zero origin calls per *view* — registration (one `POST /join` per visitor) is separate from viewing and is not zero-call; see the amended F1.1/F1.2 acceptance [F1.1, F1.2]
  - *Partial: the countdown itself is built (issue #128). The operator sets the start time and its timezone on the dashboard, `/status` publishes `starts_at`, and `waiting.js` counts down to it during `pre_queue`. What is outstanding is the framing: the countdown renders on the existing waiting page, which is polled, not on a separate CDN-cached static page with zero origin calls per view.*
- [x] Pre-queue registration (identity only, spread across the window); striped counter for ~10,000/s registration ceiling [C1]
- [x] `/status` carries phase, so the countdown page polls one endpoint (Min TTL 1 s, no cookies forwarded — DESIGN §8); after T−0 also carries `shuffle_seed`, `participant_count`, `prequeue_offsets`
- [x] Registration writes `PreQueue {r, s, l, t}` with `attribute_not_exists(r)`; shard `s = hash(request_id) % 10`, local index `l` claimed per-shard with one `SET s = :shard ADD n :count` per batch shard group (DESIGN §4.1, ADR-0015) [F2.5]
- [x] Seeded permutation (DESIGN §4.2, ADR-0002): at T−0 one `UpdateItem` on `Counters` that reads the 10 shard counts, computes `prequeue_offsets` (prefix sums) and `participant_count = ΣΣcounts`, and sets `shuffle_seed`, `participant_count`, `prequeue_offsets`, `phase`, guarded by `attribute_not_exists(shuffle_seed)`. Global index `i = offset[s] + l`; position derived on read as `PRP(seed, i, N)` [F1.3, F1.4, F1.5, C2]
- [x] Pseudorandom permutation (PRP): 4-round balanced Feistel, `HMAC-SHA256(seed, round || x)` round function, cycle-walking into `[0, N)`. Property tests for bijectivity over the full domain at N ≤ 10⁶, uniformity by chi-square, determinism across processes; assert the assembled global index space is exactly contiguous `[0, N)` across all 10 shards [F1.5] — Note: proptest bijectivity covers N < 2000 exhaustively; the 10⁶ cohort is Phase 3.
- [x] Property test — **burned slot** (ADR-0015, F2.3): with an injected registration-write failure rate (counter incremented, `PreQueue` row absent), assert (a) the assembled index space is still a contiguous `[0, N)` where `N` = Σ shard counts, (b) `PRP` remains bijective over `[0, N)`, (c) a burned index resolves to a valid position that maps to no `PreQueue` row, and (d) the serving counter advancing past it admits nobody — no duplicate, no panic, no gap in the permutation [F1.5, F2.3]
- [x] Property test — **straggler join racing the seal** (ADR-0015): for a join whose local index was claimed after the seal counted its shard, assert it is a straggler **by that shard's own issued count** (not by the reconstructed global index `i = offset[s] + l`, which can still land inside `[0, N)` on an interior shard) and that `/queue_num` never evaluates `PRP` out of domain for it, falling through to the `Positions` row instead — a live-join position if one has landed, 404 if not [F1.5]

### 1d. Live join

- [x] REST API `AWS` integration → SQS `SendMessage`, request validator with JSON Schema, DLQ with `maxReceiveCount` 5, `FunctionResponseTypes: [ReportBatchItemFailures]`, visibility timeout ≥ 6× function timeout + batching window [F2.1, F2.4, C5]
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` variables, default 100 / 1s — Partial: `BatchSize` 100 and a 1s window are hardcoded on the event-source mapping, not variables.
- [ ] SQS ESM Provisioned Mode as an opt-in variable, default off — mutually exclusive with the maximum-concurrency setting
- [ ] Per-event partition: one SQS queue and one Lambda function per event, each with reserved concurrency (ADR-0008) [N9]

### 1e. Read path

- [ ] `/status` (phase, serving position, rate, operator message — one payload), `/queue_num`, `/queue_pos_expiry` [F3.1] — Partial: `/status` and `/queue_num` are served by the read Lambda; `/queue_pos_expiry` is not routed at all, and neither is `/public_key`.
- [x] Adaptive poll interval: `/status` publishes a Terraform-set `poll_policy` (floor/ceiling/divisor); `waiting.js` clamps its interval to it, scaling with distance to the front instead of polling at a fixed interval [N10, ADR-0023]

### 1f. Admission, session, and outflow control

- [x] Deploy-time signing key into an SSM SecureString parameter, generated and rotated out of band so the key never lands in the repo or in Terraform state. Not Secrets Manager: a standard SecureString is free where a secret is $0.40/mo, which N1 (idle cost) does not allow
- [x] `/v1/generate_token` — the `generate_token` Lambda checks the position against `serving_counter` (resolved from `StoredControl` + `fail_open_until`, issue #71), records the arrival, and mints an HMAC-SHA256 session cookie (`wr_common::crypto::Session`) [F3.3, ADR-0021]
- [x] **The gate is a CloudFront Function** (issue #71, supersedes the trusted-key-group gate, ADR-0020): `infra/modules/edge/functions/gate.js.tftpl`, associated at viewer-request with the protected behaviour only, reads its ruleset and the signing secret from one CloudFront KeyValueStore (`modules/core`'s `gate_kvs_arn`, consumed by `modules/edge`). `event_id` and the session cookie name are templated into the function's own source [F3.4, ADR-0021]
- [x] Gate scope and lifetime: the session credential is scoped by `event_id` — the gate refuses a credential minted for another event — closing [#61](https://github.com/smoketurner/virtual-waiting-room/issues/61) on the CloudFront path. [#63](https://github.com/smoketurner/virtual-waiting-room/issues/63) (revocation) remains open; no design chosen (ADR-0021 §5.2)
- [x] Cross-language credential and rule conformance: `crates/wr-common/tests/vectors.rs` generates vectors (positives minted by the real `Session::sign`, negatives hand-encoded independently, `(rule, request) → bool` cases); `infra/modules/edge/tests/gate.conformance.test.js` checks the **shipped** function against them under `node:vm` [ADR-0021 §6]
- [x] HMAC admission-token minting also exists in the authorizer crate (`token.rs`) for the authorizer gate; it is not what the CloudFront path uses
- [x] Authorizer decision tree: session → token → protection match → 302, sharing `wr_common::rules::ProtectionRule` with the edge gate's config writer [F3.4]
- [x] Session cookie set after token validation (ADR-0011), signed over different inputs from the token, scoped per event, token stripped from the URL [F3.5, F3.6]
- [x] Sliding and fixed session validity modes [F3.7]
- [x] `StoredControl` (`Open`/`Paused`) + `Counters.fail_open_until` epoch (issue #71) replace the three-valued stored `AdmissionControl`: `wr_common::resolve(stored, fail_open_until, now)` is the only thing that produces the resolved `FailOpen`, so the string `"fail_open"` can no longer be written to storage. `/admin/fail_open` and `/admin/recover` engage/clear the epoch, mirrored to the edge gate's KeyValueStore (admin-writer-first entering, DynamoDB-first leaving) [ADR-0009, ADR-0019]
- [ ] Fail open with a time-limited epoch (ADR-0009) [F4.1, F4.2, F4.3] — Partial: the *mechanism* is built (`fail_open_until`, evaluated by the CloudFront Function against its own clock) and an operator can engage it via `/admin/fail_open`, but nothing trips it **automatically** on a backend outage — engaging fail-open still depends on a human, or a watchdog that does not exist yet, noticing DynamoDB is down ([#58](https://github.com/smoketurner/virtual-waiting-room/issues/58))
- [x] No-show compensating outflow controller: measure arrivals against releases, smooth, bound the correction, adjust `serving_counter` on a 10 s interval (`rate(1 minute)` × six passes, the scheduler's floor being one minute; the gaps are durable waits, so the controller is not billed for them — ADR-0022). Arrival counter sharded ×10 (`arrivals#0..9`) [F3.2, F3.8]
- [ ] Concurrency term in the control law: `/update_session` is a stub, so completions and abandonments never close the loop and origin concurrency drifts with session duration ([#65](https://github.com/smoketurner/virtual-waiting-room/issues/65))
- [x] Signing-key generation: `random_bytes.signing_key` generates the secret at apply and Terraform writes it to both the SSM SecureString the Lambdas read and the edge gate's KeyValueStore `k`. One value from one source, so the two copies cannot diverge and there is no bootstrap step whose absence has to be detected (issue #71)

### 1g. Entry gating and abuse mitigation

- [ ] Deferred bot enforcement: admit to pre-queue, block at randomization [F6.3]
      `Partial:` join-time telemetry (viewer address, ASN, country, JA4 fingerprint, user
      agent) is captured on every registration row, which is the input a deferred decision
      needs. No classifier, no operator action and no seal-time mitigation exist, and nothing
      reads the telemetry. Deferred on cost — the specified mechanism depends on WAF Bot
      Control, which the deployment does not enable.
- [ ] Bound how many positions one visitor can hold
      `Partial:` nothing does. `request_id` is client-supplied, so volume converts into share
      of the front of the queue linearly and every deployment is a bare raffle. The entry
      tickets that bounded identifier *minting* were removed (ADR-0028) — they needed the
      customer to build a signing endpoint, and they never bounded volume. Proof of work at
      registration, or behavioural classification over the telemetry above, is what would
      bound it.

### 1h. Operator surface

- [ ] Live metrics via EMF logs → CloudWatch: queue depth, admitted, no-show rate, expiry rate. Inflow from the `AWS/CloudFront` `Requests` metric, not counted in our code [F5.1]
- [ ] Brandable waiting page — client supplies assets, no module fork [F5.2]
- [x] Operator message as a `Counters` attribute, delivered in the existing `/status` payload — one `UpdateItem`, zero additional requests [F5.3]
- [ ] Position and estimated wait derived from measured admission rate [F5.4]
- [ ] Every operator action available via API; no console dependency [F5.5]

### 1i. Control plane

- [ ] Admin API: `/admin/phase`, `/admin/rate`, `/admin/message`, `/admin/reset`, `/admin/rules`, `/metrics`, `/update_session` [F3.10, F5.5] — Partial: phase, rate, message, reset, pause, resume, fail_open, recover, and rules are live; `/metrics` and `/update_session` return the deferred stub. `/admin/rules` (issue #71) replaces the edge gate's whole ruleset from a one-rule-per-line form, validated and encoded through `wr_common::rules::validate_rule_fields` + `encode_gate_config`, written to the KeyValueStore (the sole store for `rules`) and then audited on `Counters` (`rules_digest`, `rules_count`, `AdminAction::SetRules`) — a failed audit write is logged and swallowed, not surfaced to the operator, since the KeyValueStore write already landed by then. `enforce_from` is still Terraform-only; no admin route sets it. A rejected ruleset is reported as a
plain-text 400 naming the offending line and reason (works with JavaScript disabled, the
requirement that mattered); it is not re-rendered inline on the form beside the offending field.
- [x] Position expiry in the controller (ADR-0006): query `expires_at` past due with `status = issued`, mark expired, advance `max_expired_position`. Time to live (TTL) enabled only for post-event storage reclamation, with `FilterExpression` on reads that could see a pending-delete item [F3.9]

### 1j. Operator web interface (Axum Lambda, Vouch design language per ADR-0018)

- [x] Vendor one plain-CSS stylesheet into the admin Lambda — NO runtime npm dependency, NO React [F7.2, F7.5]. ADR-0018 replaced the Cloudscape design tokens with the Vouch design language, so the vendored `tokens.css` and its `extract_tokens.py` build step are deleted and the stylesheet is self-contained
- [x] Single Axum Lambda (Rust, arm64, `provided.al2023`) serving the admin UI via askama compile-time templates; semantic HTML laid out to the ADR-0018 conventions (top bar, content header, stat tiles, status pills, card grid, forms, a dominant emergency card for the andon cord) [F7.1, F7.2]
- [x] Reuse existing admin Lambda logic — UI is a thin server-rendered client over the SAME `/admin/*` actions; add no capability the API lacks [F7.3, F5.5]
- [x] One auth path on every admin UI request, with no second weaker one [F7.4]. ADR-0016 replaced SigV4 with an OIDC Authorization Code + PKCE login session, enforced in the admin Lambda and stored in DynamoDB; API Gateway auth is NONE because the Lambda is the enforcement point
- [x] HTML-form interactivity; core actions work with JS disabled; tiny vanilla-JS poller refreshes metrics within a 60s window [F7.5, F7.6]
- [ ] Admin dashboard view: live metrics (inflow, outflow, queue depth, admitted, no-show rate, expiry rate) as an HTML view over `/metrics` JSON + CloudWatch EMF [F7.6, F5.1]
- [x] askama template unit tests + rendered-HTML snapshot/accessibility check; verify no React/SPA bundle emitted

**Exit:** all endpoints correct; every lifecycle phase serves its page; pre-queue assigns fairly and reproducibly; a visitor browses multiple pages on one session; standby activates on threshold; entry gating rejects unsigned identifiers; operator can see and steer a live event via both API and the web interface; authorizer fails open.

---

## Phase 2 — Terraform module

- [x] `modules/core` — DynamoDB, SQS, Lambdas, IAM, regional REST API + validator [N5]
- [ ] `modules/edge` — CloudFront with three cache behaviours per ADR-0013: polled endpoints (Min TTL 1 s, no cookie forwarding), write endpoints (uncached), protected origin (uncached, session cookie forwarded, gated by the CloudFront Function — issue #71, ADR-0021). Web Application Firewall (WAF) with Bot Control, Autonomous System Number (ASN) match and anti-DDoS in Count mode [N7, O5, C4] — Partial: the three cache behaviours and the gate are built; the WAF web ACL is not.
- [x] `modules/authorizer` — origin authorizer plus optional CloudFront VPC origin. Note VPC origins require an internet gateway present but unused, forbid Lambda@Edge origin triggers, and are unavailable in GovCloud (DESIGN §12)
- [x] `var.enable_vpc` for ATO-constrained clients — design the seam now, do not retrofit
- [ ] Flat-rate plan subscription as a variable [O6]
- [x] Add the admin-UI Lambda + its route/cache behaviour to the module; confirm it stays within the ~80-resource budget [N6] — Note: the resource-count confirmation is the separate item below.
- [ ] CloudWatch alarms and a shipped dashboard — the metrics an operator acts on, not every metric available
- [ ] Publish the OpenAPI specification for public and admin surfaces [N8]
- [ ] `examples/` and generated variable reference
- [ ] Verify resource count ≤ 80 [N6] and idle monthly cost under $5 [N1]
- [ ] Confirm no component runs outside the client's account [N3]

**Exit:** `terraform apply` from a clean account to a working deployment [N2].

---

## Phase 3 — Load validation

The one place where being wrong is unrecoverable in production.

- [ ] Repeatable load harness as a deliverable, not a test script [O3]
- [ ] Pre-queue path: 1M registrations, assignment as a single write. Assert bijectivity across the full cohort, no correlation between registration time and assigned position, and that the seed is absent before T−0 [F1.3, F1.4, C1, C2]
- [ ] Live-join path: 10K/s at default quotas and 40K/s with increases filed; zero duplicates at both; gap rate measured [F2.2, C3]
- [ ] Raise quotas and pre-warm *before* testing the live-join path above defaults, or the test measures throttling rather than the design. Pre-queue assignment needs neither, since it is one write [O1, O2]
- [ ] `/status` origin requests per second (RPS) flat from 10K to 1M waiters: assert origin fetches ≈ elapsed/TTL, not a function of waiter count [C4]
- [ ] Fail-open verified: waiting room returning 5xx, origin still reachable [F4.1]
- [ ] Session continuity: a visitor browses N pages after admission without being re-queued [F3.5]
- [ ] No-show compensation: with an injected 30% no-show rate and a 500/min target, measured origin arrivals converge on 500/min [F3.8]
- [ ] Standby activation: inflow crossing the threshold queues new visitors within the ~125 s worst case; unprotected paths stay unqueued [F0.4, F0.6]
- [ ] Spike arriving in <5s does not drop joins [C5]
- [ ] Event isolation: drive one event to its throughput ceiling; a second event in the same deployment sees no change in join latency or error rate [N9]

**Exit:** reproducible report. Demonstrating a million assigned positions is itself the primary sales asset.

---

## Phase 4 — Operational product

What makes this a service rather than a repository.

- [ ] Pre-event readiness checklist [O1, O2, O3] — API Gateway RPS increase filed, DynamoDB per-table write request unit (WRU) increase filed, tables pre-warmed, Provisioned Mode enabled, load test executed, rollback plan
- [ ] Operator runbook [O4, O5] — rate adjustment, reset, pause, incident response, COUNT-then-BLOCK promotion, post-event flat-rate plan cancellation
- [ ] Waiting-room reference client — countdown, position, estimated time of arrival (ETA), auto-advance; 429 retry with jitter [F4.5]; 404 means re-join with a fresh universally unique identifier version 7 (UUIDv7) [F4.4]; UUIDv7 via the `uuid` package
- [ ] Client integration guide: CloudFront/ALB/CDN placement
- [ ] Per-client cost model [O6] — poll interval is the dominant variable; compute the flat-rate versus pay-as-you-go (PAYG) crossover and include pre-warming

---

## Phase 5 — GovCloud variant

Ships second, priced separately. No CloudFront, no edge compute, no VPC origins.

- [ ] Internal Application Load Balancer (ALB) gating with the token authorizer; origin access via security groups and Identity and Access Management (IAM) [N4]
- [ ] Replace CDN cache collapse for `/status` — the read-scaling story differs materially inside the boundary
- [ ] Document the commercial-CloudFront-fronting-GovCloud-origin data-boundary question for the client's Authorizing Official (AO)
- [ ] Validate deploy in a real GovCloud account

---

## Gaps from the architecture audit

Filed as issues so they carry discussion; listed here because tasks.md is the build-state
record and these are the things that stand between the current MVP and a waiting room an
operator can run an event on.

Reliability — the waiting room must not be the reason the site is down:

- [ ] [#58](https://github.com/smoketurner/virtual-waiting-room/issues/58) Gate fails closed: no fail-open path [F4.1]
- [ ] [#60](https://github.com/smoketurner/virtual-waiting-room/issues/60) Standby mode unreachable through the CloudFront gate [F0.4, F0.5, F0.7]
- [ ] [#64](https://github.com/smoketurner/virtual-waiting-room/issues/64) Origin 403s replaced by the waiting page
- [ ] [#67](https://github.com/smoketurner/virtual-waiting-room/issues/67) Visitors without JavaScript can never join
- [ ] [#68](https://github.com/smoketurner/virtual-waiting-room/issues/68) Single-region failure domain undocumented and untested
- [ ] [#70](https://github.com/smoketurner/virtual-waiting-room/issues/70) Pre-event readiness as a command, not a runbook [O1, O2, N7]

Fairness and abuse — nothing bounds how many places one visitor takes:

- [ ] [#59](https://github.com/smoketurner/virtual-waiting-room/issues/59) No one-position-per-visitor control [F6.3]
      `Partial:` server-drawn shards, the reload dedupe and join telemetry are built. The entry
      tickets are not — they were removed (ADR-0028). Nothing bounds volume; see the unchecked
      items in §1g.
- [ ] [#61](https://github.com/smoketurner/virtual-waiting-room/issues/61) Admission cookies wildcard-scoped and transferable
- [ ] [#62](https://github.com/smoketurner/virtual-waiting-room/issues/62) `request_id` is both a public cache key and the bearer credential
- [ ] [#63](https://github.com/smoketurner/virtual-waiting-room/issues/63) No way to revoke an admission

Control and cost:

- [ ] [#65](https://github.com/smoketurner/virtual-waiting-room/issues/65) Outflow control has no concurrency term [F3.10]
- [ ] [#66](https://github.com/smoketurner/virtual-waiting-room/issues/66) Per-request protection rules unavailable at the gate [F0.6]
- [ ] [#69](https://github.com/smoketurner/virtual-waiting-room/issues/69) Adaptive poll interval — the dominant cost driver [O6]

---

## Deferred

Out of scope for this release; see REQUIREMENTS §5.

- Invite-only waiting rooms with multi-factor authentication (MFA) gating
- Proof-of-Work challenges and CAPTCHA softblock
- Native application SDKs (iOS, Android, React Native)
- Platform connector breadth beyond the CloudFront/origin authorizer
- OpenID identity-provider adapter
- Multi-region and global tables
- CloudFront SaaS Manager variant for a client with many branded domains

---

## Risks

| Risk | Mitigation |
|---|---|
| Pre-queue randomization disputed as unfair | Recorded seed makes it auditable and reproducible [F1.5]; document the fairness model up front |
| Client poll interval drives cost more than any infrastructure choice | Adaptive since [#69](https://github.com/smoketurner/virtual-waiting-room/issues/69) — scales with distance to the front, floor/ceiling/divisor Terraform-configurable, modelled per client [N10, O6, ADR-0023] |
| Load harness cannot generate 1M participants from one source | Distributed harness; budget for it in Phase 3 |
| GovCloud variant larger than estimated — no CloudFront, no virtual private cloud (VPC) origins | Phase 5, priced separately; no date until commercial ships |
| On-call burden during a live event | Price as incident-critical infrastructure; cap concurrent engagements |
| Connector breadth | Product scope decision; one authorizer covers content delivery network (CDN) fronted origins |
