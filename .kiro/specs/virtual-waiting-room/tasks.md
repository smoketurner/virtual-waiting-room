# Tasks: Virtual Waiting Room

Delivers requirements.md per design.md; requirement IDs in brackets; phases ordered by dependency and risk; decisions in ../../../docs/adr/.

---

## Phase 0 — Spike

Throwaway code. Measures what documentation cannot settle.

- [ ] REST API `type: aws` → SQS with a request validator; confirm client UUIDv7 survives to the Lambda and malformed bodies are rejected with 400 [F2.4]
- [ ] Concurrent `UpdateItem ADD :n` / `ALL_NEW` against one item: assert zero duplicate positions across allocated ranges; measure gap rate under induced 5xx [F2.2, F2.3]
- [ ] Rust cold start on `provided.al2023` arm64 with an AWS SDK client — decides whether provisioned concurrency is needed

**Exit:** findings written down. Nothing else kept.

---

## Phase 1 — Core

### 1a. Data and counter

- [ ] Four tables — `Counters`, `PreQueue`, `Positions`, `Tokens` — on-demand, PITR, `warm_throughput_*` and optional `max_throughput_*` as variables (DESIGN §6.7) [O1]
- [ ] `Counters` attributes: `queue_counter`, `serving_counter`, `max_expired_position`, `arrivals#0..9`, `phase`, `phase_override`, `target_rate`, `shuffle_seed`, `operator_message`
- [ ] Batch range allocation via `UpdateItem ADD` / `ALL_NEW`; increment by **valid** count only [F2.2, F2.6]
- [ ] `PutItem` with `attribute_not_exists(request_id)` on every position write [F2.5]

### 1b. Event lifecycle and modes

- [ ] Phase state machine: idle → pre-queue → active → post-event, plus maintenance [F0.1, F0.8]
- [ ] Operator-authored static page per phase, CDN-cached [F0.2]
- [ ] Protection rules (path, header, cookie, user agent), evaluated locally at the authorizer [F0.6]
- [ ] Standby mode: CloudWatch alarm on `AWS/CloudFront` `Requests` (60 s period, `us-east-1`) → EventBridge → phase Lambda. Activation latency ~125 s worst case [F0.4, F0.7]
- [ ] Scheduled and standby coexisting on one origin [F0.3, F0.5]

### 1c. Pre-queue

- [ ] Static countdown page, CDN-cached, zero origin calls per view [F1.1, F1.2]
- [ ] Pre-queue registration (identity only, spread across the window); striped counter for ~10,000/s registration ceiling [C1]
- [ ] `/status` carries phase, so the countdown page polls one endpoint (Min TTL 1 s, no cookies forwarded — DESIGN §8); after T−0 also carries `shuffle_seed`, `participant_count`, `prequeue_offsets`
- [ ] Registration writes `PreQueue {r, s, l, t}` with `attribute_not_exists(r)`; shard `s = hash(request_id) % 10`, local index `l` from `ADD prequeue_counter#s :1` / `ALL_NEW` (DESIGN §4.1, ADR-0015) [F2.5]
- [ ] Seeded permutation (DESIGN §4.2, ADR-0002): at T−0 one `UpdateItem` on `Counters` that reads the 10 shard counts, computes `prequeue_offsets` (prefix sums) and `participant_count = ΣΣcounts`, and sets `shuffle_seed`, `participant_count`, `prequeue_offsets`, `phase`, guarded by `attribute_not_exists(shuffle_seed)`. Global index `i = offset[s] + l`; position derived on read as `PRP(seed, i, N)` [F1.3, F1.4, F1.5, C2]
- [ ] Pseudorandom permutation (PRP): 4-round balanced Feistel, `HMAC-SHA256(seed, round || x)` round function, cycle-walking into `[0, N)`. Property tests for bijectivity over the full domain at N ≤ 10⁶, uniformity by chi-square, determinism across processes; assert the assembled global index space is exactly contiguous `[0, N)` across all 10 shards [F1.5]

### 1d. Live join

- [ ] REST API `AWS` integration → SQS `SendMessage`, request validator with JSON Schema, DLQ with `maxReceiveCount` 5, `FunctionResponseTypes: [ReportBatchItemFailures]`, visibility timeout ≥ 6× function timeout + batching window [F2.1, F2.4, C5]
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` variables, default 100 / 1s
- [ ] SQS ESM Provisioned Mode as an opt-in variable, default off — mutually exclusive with the maximum-concurrency setting
- [ ] Per-event partition: one SQS queue and one Lambda function per event, each with reserved concurrency (ADR-0008) [N9]

### 1e. Read path

- [ ] `/status` (phase, serving position, rate, operator message — one payload), `/queue_num`, `/queue_pos_expiry` [F3.1]

### 1f. Admission, session, and outflow control

- [ ] Deploy-time signing key into Secrets Manager
- [ ] `/generate_token` — single-use admission token, short expiry [F3.3]
- [ ] Authorizer decision tree: session → token → protection match → 302 [F3.4]
- [ ] Session cookie set after token validation (ADR-0011), signed over different inputs from the token, scoped per event, token stripped from the URL [F3.5, F3.6]
- [ ] Sliding and fixed session validity modes [F3.7]
- [ ] Fail open with a time-limited bypass cookie, configurable (ADR-0009) [F4.1, F4.2, F4.3]
- [ ] No-show compensating outflow controller: measure arrivals against releases, smooth, bound the correction, adjust `serving_counter` on a 10 s schedule. Arrival counter sharded ×10 (`arrivals#0..9`) [F3.2, F3.8]
- [ ] Decide signing-key rotation (open question 2)

### 1g. Entry gating and abuse mitigation

- [ ] Client-signed identifier verification at join — membership ID, promo code, order reference; verified but never stored [F6.1, F6.2]
- [ ] Deferred bot enforcement: admit to pre-queue, block at randomization [F6.3]

### 1h. Operator surface

- [ ] Live metrics via EMF logs → CloudWatch: queue depth, admitted, no-show rate, expiry rate. Inflow from the `AWS/CloudFront` `Requests` metric, not counted in our code [F5.1]
- [ ] Brandable waiting page — client supplies assets, no module fork [F5.2]
- [ ] Operator message as a `Counters` attribute, delivered in the existing `/status` payload — one `UpdateItem`, zero additional requests [F5.3]
- [ ] Position and estimated wait derived from measured admission rate [F5.4]
- [ ] Every operator action available via API; no console dependency [F5.5]

### 1i. Control plane

- [ ] Admin API: `/admin/phase`, `/admin/rate`, `/admin/message`, `/admin/reset`, `/admin/rules`, `/metrics`, `/update_session` [F3.10, F5.5]
- [ ] Position expiry in the controller (ADR-0006): query `expires_at` past due with `status = issued`, mark expired, advance `max_expired_position`. Time to live (TTL) enabled only for post-event storage reclamation, with `FilterExpression` on reads that could see a pending-delete item [F3.9]

### 1j. Operator web interface (Cloudscape-styled Axum Lambda)

- [ ] Build-time Cloudscape token extraction: token values from `@cloudscape-design/design-tokens` `index-visual-refresh.json` into a plain CSS custom-properties stylesheet (style-dictionary or small script); vendor the generated CSS into the admin Lambda — NO runtime npm dependency, NO React [F7.2, F7.5]
- [ ] Single Axum Lambda (Rust, arm64, `provided.al2023`) serving the admin UI via askama compile-time templates; semantic HTML laid out to Cloudscape conventions (top nav, side nav, containers, tables, forms, status indicators) [F7.1, F7.2]
- [ ] Reuse existing admin Lambda logic — UI is a thin server-rendered client over the SAME `/admin/*` actions; add no capability the API lacks [F7.3, F5.5]
- [ ] SigV4 auth on every admin UI request, identical to the admin API; no second weaker auth path [F7.4]
- [ ] HTML-form interactivity; core actions work with JS disabled; tiny vanilla-JS poller refreshes metrics within a 60s window [F7.5, F7.6]
- [ ] Admin dashboard view: live metrics (inflow, outflow, queue depth, admitted, no-show rate, expiry rate) as an HTML view over `/metrics` JSON + CloudWatch EMF [F7.6, F5.1]
- [ ] askama template unit tests + rendered-HTML snapshot/accessibility check; verify no React/SPA bundle emitted

**Exit:** all endpoints correct; every lifecycle phase serves its page; pre-queue assigns fairly and reproducibly; a visitor browses multiple pages on one session; standby activates on threshold; entry gating rejects unsigned identifiers; operator can see and steer a live event via both API and the Cloudscape-styled web interface; authorizer fails open.

---

## Phase 2 — Terraform module

- [ ] `modules/core` — DynamoDB, SQS, Lambdas, IAM, regional REST API + validator [N5]
- [ ] `modules/edge` — CloudFront with three cache behaviours per ADR-0013: polled endpoints (Min TTL 1 s, no cookie forwarding), write endpoints (uncached), protected origin (uncached, session cookie forwarded). Web Application Firewall (WAF) with Bot Control, Autonomous System Number (ASN) match and anti-DDoS in Count mode [N7, O5, C4]
- [ ] `modules/authorizer` — origin authorizer plus optional CloudFront VPC origin. Note VPC origins require an internet gateway present but unused, forbid Lambda@Edge origin triggers, and are unavailable in GovCloud (DESIGN §12)
- [ ] `var.enable_vpc` for ATO-constrained clients — design the seam now, do not retrofit
- [ ] Flat-rate plan subscription as a variable [O6]
- [ ] Add the admin-UI Lambda + its route/cache behaviour to the module; confirm it stays within the ~80-resource budget [N6]
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
| Client poll interval drives cost more than any infrastructure choice | Configurable, default 10s, modelled per client [O6] |
| Load harness cannot generate 1M participants from one source | Distributed harness; budget for it in Phase 3 |
| GovCloud variant larger than estimated — no CloudFront, no virtual private cloud (VPC) origins | Phase 5, priced separately; no date until commercial ships |
| On-call burden during a live event | Price as incident-critical infrastructure; cap concurrent engagements |
| Connector breadth | Product scope decision; one authorizer covers content delivery network (CDN) fronted origins |
