# Implementation Plan

Companion to [`DESIGN.md`](./DESIGN.md). Sequenced to kill the riskiest unknowns first
and to reach something demonstrable before something complete.

**Estimate: 6–8 weeks to a sellable commercial-region product.** GovCloud follows.

---

## Phase 0 — Spike (3–5 days)

Purpose: prove the two assumptions the whole build rests on, in throwaway code.
If either fails, the design changes before any real work is spent.

- [ ] **HTTP API → SQS.** Stand up `AWS_PROXY` / `SQS-SendMessage` and confirm the
      design in DESIGN §8a end to end: client-supplied UUIDv4 `request_id` in the body
      survives to the Lambda, WAF rate-based rules replace the public API key, and no
      response body mapping is needed. Desk research resolved the mechanism; this is
      confirmation, not investigation. Fallback if something surprises us: REST API
      (~71% higher per-request cost, no architectural change).
- [ ] **Atomic range allocation under concurrency.** Hammer one `Counters` item with
      concurrent `UpdateItem ADD :n` / `ALL_NEW`. Assert: zero duplicate positions
      across all allocated ranges. Measure gap rate under induced 5xx.
- [ ] **Rust Lambda cold start**, `provided.al2023` on arm64, with an AWS SDK client.
      Establishes whether provisioned concurrency is needed at all.

**Exit:** a scratch repo and a paragraph of findings. Nothing here is kept.

---

## Phase 1 — Core (3–4 weeks)

### 1a. Counter and ingest
- [ ] `Counters` / `Positions` / `Tokens` tables, `PAY_PER_REQUEST`, PITR on
- [ ] `assign_queue_num`: parse/validate client UUIDv7, partition valid from invalid,
      **increment the counter by the valid count only** (DESIGN §8a — incrementing by
      `records.len()` lets malformed payloads burn queue positions), batch range
      allocation, `attribute_not_exists(request_id)` on write (fixes upstream's
      redelivery double-assign)
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` as variables, default 100 / 1s
- [ ] DLQ + `ReportBatchItemFailures` partial-batch responses

### 1b. Read path
- [ ] `get_queue_num`, `get_serving_num`, `get_waiting_num`, `get_queue_position_expiry_time`

### 1c. Tokens
- [ ] RSA keypair generation at deploy time, private key in Secrets Manager
- [ ] `generate_token` — RS256, claims `{sub, aud=event_id, iss, exp, token_use}`
- [ ] `get_public_key` — JWKS endpoint
- [ ] `token_authorizer` — verify sig/exp/aud/iss, JWKS in a `OnceCell`
- [ ] **Decide JWKS rotation** (open question §10.2)

### 1d. Control plane
- [ ] `increment_serving_counter`, `update_session`, `reset_initial_state`,
      `get_list_expired_tokens`, `get_num_active_tokens`
- [ ] Queue-position expiry sweeper (EventBridge scheduled)

**Exit:** all 13 endpoints answer correctly against a locally-driven deployment.

---

## Phase 2 — Terraform module (1.5–2 weeks)

- [ ] `modules/core` — DynamoDB, SQS, Lambdas, IAM, HTTP API
- [ ] `modules/edge` — CloudFront, cache policies (`/queue_num` 24h,
      `/serving_num` 5s, `/public_key` 24h), WAF + Bot Control
- [ ] `modules/authorizer` — API Gateway authorizer for the client's protected origin
- [ ] `var.enable_vpc` — conditional `vpc_config` + endpoints for ATO-constrained
      clients (§5). Design the seam now; do not retrofit it.
- [ ] CloudWatch alarms — port the useful subset of upstream's 35, not all of them
- [ ] `examples/` — minimal deployment + protected-origin sample
- [ ] `terraform-docs` generated variable/output reference

**Exit:** `terraform apply` from clean account to working waiting room.

---

## Phase 3 — Load validation (1 week)

Non-negotiable. Everything else is mechanical; queue-position correctness under
concurrency is the entire product, and the failure mode only appears under exactly the
traffic the client hired us to survive.

- [ ] Repeatable load harness as a **first-class deliverable**, not a test script
- [ ] Verify at 10K, 50K, 100K joins/sec: **zero duplicate positions**, gap rate
      within tolerance, ordering preserved
- [ ] Confirm `/serving_num` cache collapse — origin RPS must stay flat as waiters scale
- [ ] Tune `BatchSize` against measured reality; publish the table
- [ ] Cold-start / ramp behavior for a spike arriving in <5s

**Exit:** a reproducible report. *Demonstrating 100K concurrent queue positions is
itself the primary sales asset.*

---

## Phase 4 — Operational product (1 week)

The part that makes this a service rather than a repo.

- [ ] **Pre-event readiness checklist** (§8) — API Gateway quota increase filed with
      lead time, Lambda concurrency raised, provisioned concurrency warmed, load test
      at target rate, rollback plan. Billable deliverable.
- [ ] Operator runbook: mid-event rate adjustment, reset, incident response
- [ ] Waiting-room page reference implementation (position, ETA, auto-advance).
      **Must treat HTTP 429 as expected and retry with jittered backoff** — API
      Gateway's burst bucket will shed a few requests at t=0 of any large on-sale, and
      a page that fails closed turns a smoothing event into an outage (DESIGN §8).
      **Must treat a 404 from `/queue_num` as "re-join with a fresh UUIDv7"** — this is
      the recovery path for a malformed or lost message (DESIGN §8a). Generates UUIDv7
      via the `uuid` package; `crypto.randomUUID()` is v4-only.
- [ ] Client integration guide: CloudFront/ALB/CDN placement
- [ ] Cost model per event size

---

## Phase 5 — GovCloud variant (1–1.5 weeks)

Ships second, priced separately.

- [ ] ALB/origin gating to replace edge gating (no CloudFront in GovCloud)
- [ ] Document the commercial-CloudFront-fronting-GovCloud-origin data-boundary
      question for the client's AO
- [ ] Validate deploy in a real GovCloud account — the artifact an agency buyer wants
      to see before signing

---

## Deferred

- OpenID adapter (618 LOC upstream, lowest value — port only on demand)
- Hi/Lo leasing and strided sequences (§4.5 — documented, unbuilt)
- Multi-region / global tables

---

## Sequencing rationale

Phase 0 exists to confirm the ingest design end-to-end and to measure what desk research
cannot. Phase 3 is protected because it is the one place where being wrong is
unrecoverable in production. Phase 4 is what converts a
GitHub repo into revenue — the code is the credential, the deployment and the
pre-event operations are the product.

## Risks

| Risk | Mitigation |
|---|---|
| HTTP API ingest surprises us in practice | Phase 0 confirmation; fall back to REST API |
| Load test can't reach 100K/sec from one source | Distributed harness; budget for it in Phase 3 |
| On-call burden — a failure during an on-sale is career-ending for the client | Price as incident-critical infrastructure, not a $150/mo care plan. Cap concurrent engagements. |
| AWS ships a replacement | Unlikely — they just deprecated theirs and pointed at Marketplace |
