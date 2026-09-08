# Implementation Plan

Companion to [`DESIGN.md`](./DESIGN.md). Sequenced to kill the riskiest unknowns first
and to reach something demonstrable before something complete.

**Estimate: 6–8 weeks to a sellable commercial-region product.** GovCloud follows.

---

## Phase 0 — Spike (3–5 days)

Purpose: prove the two assumptions the whole build rests on, in throwaway code.
If either fails, the design changes before any real work is spent.

- [ ] **REST API → SQS ingest.** Stand up the regional REST API with `type: aws` direct
      SQS integration, a request validator on the body, and client-supplied UUIDv7
      `request_id`. Confirm the ID survives to the Lambda and that a malformed body is
      rejected with a 400 at the gateway. (API flavor settled in DESIGN §8a — the HTTP
      API saving was $7.50 per million-visitor event and cost us validation, API keys
      and VTL.)
- [ ] **Atomic range allocation under concurrency.** Hammer one `Counters` item with
      concurrent `UpdateItem ADD :n` / `ALL_NEW`. Assert: zero duplicate positions
      across all allocated ranges. Measure gap rate under induced 5xx.
- [ ] **Rust Lambda cold start**, `provided.al2023` on arm64, with an AWS SDK client.
      Establishes whether provisioned concurrency is needed at all.

**Exit:** a scratch repo and a paragraph of findings. Nothing here is kept.

---

## Phase 1 — Core (3–4 weeks)

### 1a. Counter and ingest
- [ ] `Counters` / `Positions` / `Tokens` tables, `PAY_PER_REQUEST`, PITR on,
      `warm_throughput_*` and optional `max_throughput_*` as variables (DESIGN §4.3a/b)
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` as variables, default 100 / 1s
- [ ] SQS ESM **Provisioned Mode** as an opt-in variable (default off) — note it is
      mutually exclusive with the maximum-concurrency setting (DESIGN §8)
- [ ] `assign_queue_num`: parse/validate client UUIDv7, partition valid from invalid,
      **increment the counter by the valid count only** (DESIGN §8a — incrementing by
      `records.len()` lets malformed payloads burn queue positions), batch range
      allocation, `attribute_not_exists(request_id)` on write (fixes upstream's
      redelivery double-assign)
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

### 1e. Pre-queue (DESIGN §3.0 / DECISIONS D1)
- [ ] Static countdown page, CloudFront-cached, zero backend calls
- [ ] Pre-queue registration (lighter than a queue position — identity only)
- [ ] Randomized batch assignment at T-0; must be verifiably fair and auditable
- [ ] `/pre_queue_status` for the countdown page to poll (globally cached)

### 1f. Fail-open (DESIGN principle 6 / DECISIONS D2)
- [ ] Authorizer failure mode configurable, **defaulting to open** with a time-limited
      bypass cookie
- [ ] Client-side retry in the background while bypassed

**Exit:** all endpoints answer correctly, pre-queue assigns fairly, and the authorizer fails open, against a locally-driven deployment.

---


---

## Phase 2 — Terraform module (1.5–2 weeks)

- [ ] `modules/core` — DynamoDB, SQS, Lambdas, IAM, regional REST API + request validator
- [ ] `modules/edge` — CloudFront, cache policies (`/queue_num` 24h,
      `/serving_num` 5s, `/public_key` 24h), WAF: Bot Control + ASN match + Anti-DDoS
      rule group **in Count mode by default** (DESIGN §3.1a)
- [ ] `modules/authorizer` — API Gateway authorizer for the client's protected origin,
      plus optional **CloudFront VPC origin** so the origin has no public IP (DESIGN §9)
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
- [ ] Verify at 10K, 50K, 100K joins/sec on the **live-join path**: **zero duplicate
      positions**, gap rate within tolerance, ordering preserved. **Note the `Positions`
      table quota (~40,000 WRU/s default) is the real ceiling — file the increase and
      pre-warm before testing above it, or the test measures DynamoDB throttling rather
      than the design (DESIGN §4.3a/b).**
- [ ] **Verify the pre-queue batch assignment separately** (DESIGN §3.0) — this is the
      path a real scheduled on-sale uses, and it is a *scheduled* write rate we control
      (~3,333/s for 1M over 5 min), not a burst. Assert fairness of the randomization and
      zero duplicate positions across the whole assigned set.
- [ ] Confirm `/serving_num` cache collapse — origin RPS must stay flat as waiters scale
- [ ] Tune `BatchSize` against measured reality; publish the table
- [ ] Cold-start / ramp behavior for a spike arriving in <5s

**Exit:** a reproducible report. *Demonstrating 100K concurrent queue positions is
itself the primary sales asset.*

---

## Phase 4 — Operational product (1 week)

The part that makes this a service rather than a repo.

- [ ] **Pre-event readiness checklist** (§8) — API Gateway quota increase filed with
      lead time, **DynamoDB per-table WRU quota increase filed (§4.3a)**, **tables
      pre-warmed via warm throughput (§4.3b — billable line item)**, Lambda concurrency
      raised, **SQS ESM Provisioned Mode enabled**, provisioned concurrency warmed, load
      test at target rate, rollback plan. Billable deliverable.
- [ ] Operator runbook: mid-event rate adjustment, reset, incident response,
      **COUNT-then-BLOCK promotion discipline for the Anti-DDoS rule group (§3.1a)**
- [ ] Waiting-room page reference implementation (position, ETA, auto-advance).
      **Must treat HTTP 429 as expected and retry with jittered backoff** — API
      Gateway's burst bucket will shed a few requests at t=0 of any large on-sale, and
      a page that fails closed turns a smoothing event into an outage (DESIGN §8).
      **Must treat a 404 from `/queue_num` as "re-join with a fresh UUIDv7"** — this is
      the recovery path for a malformed or lost message (DESIGN §8a). Generates UUIDv7
      via the `uuid` package; `crypto.randomUUID()` is v4-only.
- [ ] Client integration guide: CloudFront/ALB/CDN placement
- [ ] Cost model per event size. **CloudFront request volume dominates** — it is ~17× the
      API Gateway bill — and the client poll interval is the single largest lever
      (DESIGN §8a). Model it explicitly rather than leaving it at the upstream 5s.
      **CloudFront flat-rate vs pay-as-you-go (AUDIT §8)** — default to a flat-rate plan
      sized to the client's event profile. PAYG must buy WAF ($5 ACL + $1/rule +
      $0.60/M) and Bot Control ($10/mo + $1/M Common, $10/M Targeted) separately; the
      flat-rate plan bundles them. Crossover depends on event size, poll interval and
      Common vs Targeted, so compute per client — do not assume.
- [ ] Per-event pre-warming cost model (DESIGN §4.3b) — this is billed, and it is the
      difference between a working on-sale and a throttled one.

---

## Phase 5 — GovCloud variant (1–1.5 weeks)

Ships second, priced separately.

- [ ] ALB/origin gating to replace edge gating (no CloudFront in GovCloud, **and no VPC
      origins either** — verified unavailable, AUDIT §9). Origin protection is internal
      ALB + token authorizer + security groups/IAM, built from primitives.
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
| Client poll interval drives cost more than any infra choice | Make it configurable; default 10s not 5s; model it in the Phase 4 cost model |
| GovCloud variant is more work than estimated — no CloudFront, no VPC origins, origin protection built from primitives | Keep it Phase 5, priced separately; do not promise a GovCloud date until the commercial module ships |
| Load test can't reach 100K/sec from one source | Distributed harness; budget for it in Phase 3 |
| On-call burden — a failure during an on-sale is career-ending for the client | Price as incident-critical infrastructure, not a $150/mo care plan. Cap concurrent engagements. |
| AWS ships a replacement | Unlikely — they just deprecated theirs and pointed at Marketplace |
