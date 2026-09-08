# Implementation Plan

Delivers [`REQUIREMENTS.md`](./REQUIREMENTS.md) per [`DESIGN.md`](./DESIGN.md).
Requirement IDs in brackets.

Phases are ordered by dependency and risk, not by calendar. Phase 0 exists to kill
unknowns before they cost real work; Phase 3 is protected because it is the one place
where being wrong is unrecoverable in production.

---

## Phase 0 — Spike

Throwaway code. Measures what documentation cannot settle.

- [ ] REST API `type: aws` → SQS with a request validator; confirm client UUIDv7 survives
      to the Lambda and malformed bodies are rejected with 400 [F2.4]
- [ ] Concurrent `UpdateItem ADD :n` / `ALL_NEW` against one item: assert zero duplicate
      positions across allocated ranges; measure gap rate under induced 5xx [F2.2, F2.3]
- [ ] Rust cold start on `provided.al2023` arm64 with an AWS SDK client — decides whether
      provisioned concurrency is needed

**Exit:** findings written down. Nothing else kept.

---

## Phase 1 — Core

### 1a. Data and counter
- [ ] Four tables — `Counters`, `PreQueue`, `Positions`, `Tokens` — on-demand, PITR,
      `warm_throughput_*` and optional `max_throughput_*` as variables (DESIGN §6.7) [O1]
- [ ] `Counters` attributes: `queue_counter`, `serving_counter`, `max_expired_position`,
      `arrivals#0..9`, `phase`, `phase_override`, `target_rate`, `shuffle_seed`,
      `operator_message`
- [ ] Batch range allocation via `UpdateItem ADD` / `ALL_NEW`; increment by **valid** count
      only [F2.2, F2.6]
- [ ] `PutItem` with `attribute_not_exists(request_id)` on every position write [F2.5]

### 1b. Event lifecycle and modes
- [ ] Phase state machine: idle → pre-queue → active → post-event, plus maintenance
      [F0.1, F0.8]
- [ ] Operator-authored static page per phase, CDN-cached [F0.2]
- [ ] Protection rules (path, header, cookie, user agent), evaluated locally at the
      authorizer [F0.6]
- [ ] **Standby mode**: CloudWatch alarm on `AWS/CloudFront` `Requests` (60 s period,
      `us-east-1`) → EventBridge → phase Lambda. Worst-case activation latency ~125 s;
      document that standby does not protect against sub-2-minute spikes.
      [F0.4, F0.7]
- [ ] Scheduled and standby coexisting on one origin [F0.3, F0.5]

### 1c. Pre-queue
- [ ] Static countdown page, CDN-cached, zero origin calls per view [F1.1, F1.2]
- [ ] Pre-queue registration (identity only, spread across the window) [C1]
- [ ] `/status` carries phase, so the countdown page polls one endpoint (Min TTL 1 s,
      no cookies forwarded — DESIGN §8)
- [ ] EventBridge-triggered batch assignment (DESIGN §4.2): 256-bit seed to `Counters`,
      one `UpdateItem ADD`/`ALL_NEW` to claim the range, **parallel `Scan` projecting
      `request_id` only** (1 MB pages, ~286 pages at 1M, 50 segments), Fisher-Yates,
      then `PutItem` with `attribute_not_exists(request_id)` per position — **not
      `BatchWriteItem`, which cannot express conditions**. Checkpoint
      `{seed, offset, range_start}` and re-invoke beyond the 900 s Lambda timeout.
      [F1.3, F1.4, F1.5, C2]

### 1d. Live join
- [ ] REST API `AWS` integration → SQS `SendMessage`, request validator with JSON Schema,
      DLQ with `maxReceiveCount` 5, `FunctionResponseTypes: [ReportBatchItemFailures]`,
      visibility timeout ≥ 6× function timeout + batching window [F2.1, F2.4, C5]
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` variables, default 100 / 1s
- [ ] SQS ESM Provisioned Mode as an opt-in variable, default off — mutually exclusive with
      the maximum-concurrency setting
- [ ] **Per-event partition**: one SQS queue and one Lambda function per event, each with
      **reserved concurrency** so a runaway event cannot drain the shared account pool.
      Not shuffle sharding — serverless resources are free at rest, so full partitioning
      beats partial isolation (DESIGN §12) [N9]

### 1e. Read path
- [ ] `/status` (phase, serving position, rate, operator message — one payload),
      `/queue_num`, `/queue_pos_expiry` [F3.1]

### 1f. Admission, session, and outflow control
- [ ] Deploy-time signing key into Secrets Manager
- [ ] `/generate_token` — single-use admission token, short expiry [F3.3]
- [ ] Authorizer decision tree: session → token → protection match → 302 [F3.4]
- [ ] **Session minting after token validation**, signed over different inputs from the
      token, scoped per event, token stripped from the URL [F3.5, F3.6]
- [ ] Sliding and fixed session validity modes [F3.7]
- [ ] **Fail-open with time-limited bypass; configurable** [F4.1, F4.2, F4.3]
- [ ] **No-show compensating outflow controller** — measure arrivals against releases,
      smooth, bound the correction, adjust `serving_counter` on a 10 s schedule.
      **Arrival counter sharded ×10** (`arrivals#0..9`), since a 60,000/min admission rate
      is 1,000 writes/s and would hit the single-item ceiling (DESIGN §5). [F3.2, F3.8]
- [ ] Decide signing-key rotation (open question 2)

### 1g. Entry gating and abuse mitigation
- [ ] **Client-signed identifier verification at join** — membership ID, promo code, order
      reference; signed by the client, verified by us, stored by neither [F6.1, F6.2]
- [ ] Deferred bot enforcement: admit to pre-queue, block at randomization [F6.3]

### 1h. Operator surface
- [ ] Live metrics via EMF logs → CloudWatch: queue depth, admitted, no-show rate, expiry
      rate. Inflow from the `AWS/CloudFront` `Requests` metric, not counted in our code
      [F5.1]
- [ ] Brandable waiting page — client supplies assets, no module fork [F5.2]
- [ ] Operator message as a `Counters` attribute, delivered in the existing `/status`
      payload — one `UpdateItem`, zero additional requests [F5.3]
- [ ] Position and estimated wait derived from measured admission rate [F5.4]
- [ ] Every operator action available via API; no console dependency [F5.5]

### 1i. Control plane
- [ ] Admin API: `/admin/phase`, `/admin/rate`, `/admin/message`, `/admin/reset`,
      `/admin/rules`, `/metrics`, `/update_session` [F3.10, F5.5]
- [ ] **Deterministic position expiry in the controller** — query `expires_at` past due
      with `status = issued`, mark expired, advance `max_expired_position`. **DynamoDB TTL
      cannot drive this**: it deletes "within a few days" and expired items stay readable
      until deleted (DESIGN §5). TTL is enabled only for post-event storage reclamation,
      with `FilterExpression` on reads that could see a pending-delete item. [F3.9]

**Exit:** all endpoints correct; every lifecycle phase serves its page; pre-queue assigns
fairly and reproducibly; a visitor browses multiple pages on one session; standby activates
on threshold; entry gating rejects unsigned identifiers; operator can see and steer a live
event; authorizer fails open.

---

## Phase 2 — Terraform module

- [ ] `modules/core` — DynamoDB, SQS, Lambdas, IAM, regional REST API + validator [N5]
- [ ] `modules/edge` — CloudFront with **three separate cache behaviours**: polled
      endpoints (Min TTL 1 s, **no cookie forwarding**), write endpoints (uncached),
      protected origin (uncached, session cookie forwarded). **Min TTL must be >0 and
      cookies must not be forwarded on polled behaviours or request collapsing is
      disabled** and C4 fails (DESIGN §8). WAF: Bot Control + ASN match + Anti-DDoS in
      Count. [N7, O5, C4]
- [ ] `modules/authorizer` — origin authorizer plus optional CloudFront VPC origin.
      Note VPC origins require an internet gateway present but unused, forbid Lambda@Edge
      origin triggers, and are unavailable in GovCloud (DESIGN §12)
- [ ] `var.enable_vpc` for ATO-constrained clients — design the seam now, do not retrofit
- [ ] Flat-rate plan subscription as a variable [O6]
- [ ] CloudWatch alarms and a shipped dashboard — the metrics an operator acts on, not
      every metric available
- [ ] Publish the OpenAPI specification for public and admin surfaces [N8]
- [ ] `examples/` and generated variable reference
- [ ] Verify resource count ≤ 80 [N6] and idle monthly cost under $5 [N1]
- [ ] Confirm no component runs outside the client's account [N3]

**Exit:** `terraform apply` from a clean account to a working deployment [N2].

---

## Phase 3 — Load validation

The one place where being wrong is unrecoverable in production.

- [ ] Repeatable load harness as a deliverable, not a test script [O3]
- [ ] **Pre-queue path**: 1M participants, batch assignment within window, zero duplicates,
      randomization shows no correlation with arrival time [F1.3, F1.4, C1, C2]
- [ ] **Live-join path**: 10K/sec at default quotas and 40K/sec with increases filed; zero
      duplicates at both; gap rate measured [F2.2, C3]
- [ ] Raise quotas and pre-warm *before* testing above defaults, or the test measures
      throttling rather than the design [O1, O2]
- [ ] `/status` origin RPS flat from 10K to 1M waiters — **this validates request
      collapsing**, so assert origin fetches ≈ elapsed/TTL and not a function of waiter
      count [C4]
- [ ] Fail-open verified: waiting room returning 5xx, origin still reachable [F4.1]
- [ ] **Session continuity**: a visitor browses N pages after admission without being
      re-queued [F3.5]
- [ ] **No-show compensation**: with an injected 30% no-show rate and a 500/min target,
      measured origin arrivals converge on 500/min [F3.8]
- [ ] **Standby activation**: inflow crossing the threshold queues new visitors without
      operator action within the ~125 s worst case; unprotected paths stay unqueued
      [F0.4, F0.6]
- [ ] Spike arriving in <5s does not drop joins [C5]
- [ ] **Event isolation**: drive one event to its throughput ceiling and assert a second
      event in the same deployment sees no change in join latency or error rate [N9]

**Exit:** reproducible report. Demonstrating a million assigned positions is itself the
primary sales asset.

---

## Phase 4 — Operational product

What makes this a service rather than a repository.

- [ ] **Pre-event readiness checklist** [O1, O2, O3] — API Gateway RPS increase filed,
      DynamoDB per-table WRU increase filed, tables pre-warmed, Provisioned Mode enabled,
      load test executed, rollback plan. Billable.
- [ ] Operator runbook [O4, O5] — rate adjustment, reset, pause, incident response,
      COUNT-then-BLOCK promotion, post-event flat-rate plan cancellation
- [ ] Waiting-room reference client — countdown, position, ETA, auto-advance; **429 retry
      with jitter** [F4.5]; **404 means re-join with a fresh UUIDv7** [F4.4]; UUIDv7 via the
      `uuid` package
- [ ] Client integration guide: CloudFront/ALB/CDN placement
- [ ] **Per-client cost model** [O6] — poll interval is the dominant variable; compute
      flat-rate vs PAYG crossover and include pre-warming

---

## Phase 5 — GovCloud variant

Ships second, priced separately. No CloudFront, no edge compute, no VPC origins.

- [ ] Internal ALB gating with the token authorizer; origin access via security groups and
      IAM [N4]
- [ ] Replace CDN cache collapse for `/status` — the read-scaling story differs
      materially inside the boundary
- [ ] Document the commercial-CloudFront-fronting-GovCloud-origin data-boundary question
      for the client's AO
- [ ] Validate deploy in a real GovCloud account

---

## Deferred

Queue-it ships these; we do not yet. Recorded as decisions, not oversights.

- **Invite-only waiting rooms** (identifier + MFA gating). F6.1 is the primitive; the full
  flow is post-v1.
- **Proof-of-Work challenges** and **CAPTCHA softblock** — WAF challenge actions cover much
  of this initially.
- **Native app SDKs** (iOS, Android, React Native). A genuine gap for ticketing clients,
  who see heavy app traffic.
- **Connector breadth.** Queue-it ships 25+ connectors across edge, server-side, native app
  and ecommerce platforms, with a published version and support policy. This is their actual
  moat. We ship a CloudFront/origin authorizer covering CDN-fronted origins.
- OpenID identity-provider adapter
- Hi/Lo leasing and strided sequences — documented escape hatches, unbuilt
- Multi-region / global tables
- Additional platform connectors beyond the origin authorizer
- CloudFront SaaS Manager variant for a single client with many branded domains

---

## Risks

| Risk | Mitigation |
|---|---|
| Pre-queue randomization is disputed as unfair by a client's users | Recorded seed makes it auditable and reproducible [F1.5]; document the fairness argument up front |
| Client poll interval drives cost more than any infrastructure choice | Configurable, default 10s, modelled per client [O6] |
| Load harness cannot generate 1M participants from one source | Distributed harness; budget for it in Phase 3 |
| GovCloud variant larger than estimated — no CloudFront, no VPC origins | Phase 5, priced separately; no date until commercial ships |
| On-call burden — failure during an on-sale is career-ending for the client | Price as incident-critical infrastructure, not a care plan; cap concurrent engagements |
| Connector breadth versus Queue-it's 25+ | Product scope decision; one authorizer covers CDN-fronted origins |
