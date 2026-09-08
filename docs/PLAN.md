# Implementation Plan

Delivers [`REQUIREMENTS.md`](./REQUIREMENTS.md) per [`DESIGN.md`](./DESIGN.md).
Requirement IDs in brackets.

**~7–9 weeks to a sellable commercial-region product.** GovCloud follows.

---

## Phase 0 — Spike (3–5 days)

Throwaway code. Measures what documentation cannot settle.

- [ ] REST API `type: aws` → SQS with a request validator; confirm client UUIDv7 survives
      to the Lambda and malformed bodies are rejected with 400 [F2.4]
- [ ] Concurrent `UpdateItem ADD :n` / `ALL_NEW` against one item: assert zero duplicate
      positions across allocated ranges; measure gap rate under induced 5xx [F2.2, F2.3]
- [ ] Rust cold start on `provided.al2023` arm64 with an AWS SDK client — decides whether
      provisioned concurrency is needed

**Exit:** findings written down. Nothing else kept.

---

## Phase 1 — Core (4–5 weeks)

### 1a. Data and counter
- [ ] `Counters`, `Positions`, `Tokens` — on-demand, PITR, `warm_throughput_*` and optional
      `max_throughput_*` as variables [O1]
- [ ] Batch range allocation; increment by **valid** count only [F2.2, F2.6]
- [ ] `attribute_not_exists(request_id)` on position writes [F2.5]

### 1b. Pre-queue
- [ ] Static countdown page, CDN-cached, zero origin calls per view [F1.1, F1.2]
- [ ] Pre-queue registration (identity only, spread across the window) [C1]
- [ ] `/pre_queue_status`, globally cached
- [ ] EventBridge-triggered batch assignment: seeded shuffle, recorded seed, paced
      `BatchWriteItem` within the configured window [F1.3, F1.4, F1.5, C2]

### 1c. Live join
- [ ] REST API → SQS integration, request validator, DLQ,
      `ReportBatchItemFailures` [F2.1, F2.4, C5]
- [ ] `BatchSize` / `MaximumBatchingWindowInSeconds` variables, default 100 / 1s
- [ ] SQS ESM Provisioned Mode as an opt-in variable, default off — mutually exclusive with
      the maximum-concurrency setting

### 1d. Read path
- [ ] `/queue_num`, `/serving_num`, `/waiting_num`, `/queue_pos_expiry` [F3.1]

### 1e. Tokens and authorizer
- [ ] Deploy-time RSA keypair, private key in Secrets Manager
- [ ] `/generate_token` RS256; `/public_key` JWKS [F3.3]
- [ ] Rust authorizer: verify sig/exp/aud/iss, JWKS in `OnceCell` [F3.4]
- [ ] **Fail-open with time-limited bypass cookie; configurable** [F4.1, F4.2, F4.3]
- [ ] Decide JWKS rotation (open question 2)

### 1f. Control plane
- [ ] `/increment_serving_counter`, `/update_session`, `/reset_initial_state`,
      `/expired_tokens`, `/num_active_tokens` [F3.2, F3.6]
- [ ] Scheduled position-expiry sweeper [F3.5]

**Exit:** all endpoints correct; pre-queue assigns fairly and reproducibly; authorizer
fails open.

---

## Phase 2 — Terraform module (2 weeks)

- [ ] `modules/core` — DynamoDB, SQS, Lambdas, IAM, regional REST API + validator [N5]
- [ ] `modules/edge` — CloudFront, cache policies, WAF (Bot Control + ASN match + Anti-DDoS
      in Count) [N7, O5]
- [ ] `modules/authorizer` — origin authorizer plus optional CloudFront VPC origin
- [ ] `var.enable_vpc` for ATO-constrained clients — design the seam now, do not retrofit
- [ ] Flat-rate plan subscription as a variable [O6]
- [ ] CloudWatch alarms — the useful subset, not all 35 from the deprecated solution
- [ ] `examples/` and generated variable reference
- [ ] Verify resource count ≤ 80 [N6] and idle monthly cost under $5 [N1]
- [ ] Confirm no component runs outside the client's account [N3] and the endpoint
      contract matches the deprecated solution [N8]

**Exit:** `terraform apply` from a clean account to a working deployment [N2].

---

## Phase 3 — Load validation (1.5 weeks)

The one place where being wrong is unrecoverable in production.

- [ ] Repeatable load harness as a deliverable, not a test script [O3]
- [ ] **Pre-queue path**: 1M participants, batch assignment within window, zero duplicates,
      randomization shows no correlation with arrival time [F1.3, F1.4, C1, C2]
- [ ] **Live-join path**: 10K/sec at default quotas and 40K/sec with increases filed; zero
      duplicates at both; gap rate measured [F2.2, C3]
- [ ] Raise quotas and pre-warm *before* testing above defaults, or the test measures
      throttling rather than the design [O1, O2]
- [ ] `/serving_num` origin RPS flat from 10K to 1M waiters [C4]
- [ ] Fail-open verified: waiting room returning 5xx, origin still reachable [F4.1]
- [ ] Spike arriving in <5s does not drop joins [C5]

**Exit:** reproducible report. Demonstrating a million assigned positions is itself the
primary sales asset.

---

## Phase 4 — Operational product (1 week)

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

## Phase 5 — GovCloud variant (2 weeks)

Ships second, priced separately. No CloudFront, no edge compute, no VPC origins.

- [ ] Internal ALB gating with the token authorizer; origin access via security groups and
      IAM [N4]
- [ ] Replace CDN cache collapse for `/serving_num` — the read-scaling story differs
      materially inside the boundary
- [ ] Document the commercial-CloudFront-fronting-GovCloud-origin data-boundary question
      for the client's AO
- [ ] Validate deploy in a real GovCloud account

---

## Deferred

- OpenID adapter (618 LOC upstream, lowest value)
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
