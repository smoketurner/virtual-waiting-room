# Virtual Waiting Room — High-Level Design

**Status:** Draft
**License:** Apache-2.0
**Origin:** Clean-room reimplementation of the concepts in the deprecated
[`aws-solutions/virtual-waiting-room-on-aws`](https://github.com/aws-solutions/virtual-waiting-room-on-aws)
(archived 2025-11-03), rewritten in Rust + Terraform with a substantially simpler
infrastructure footprint.

---

## 1. Problem

When demand for a website briefly exceeds what its origin can serve — a ticket on-sale,
a limited product drop, an exam registration window, a benefits enrollment deadline —
the origin degrades or fails for *everyone*. Autoscaling does not solve this: the spike
arrives faster than instances boot, and the database tier usually cannot scale at all.

A virtual waiting room sits in front of the origin and **meters** visitors into it at a
rate the origin can survive, holding the remainder in a fair, transparent, first-come
first-served queue.

### Goals

| # | Goal |
|---|---|
| G1 | Absorb an arbitrarily large arrival burst without dropping or misordering visitors |
| G2 | Assign each visitor a unique, monotonically increasing queue position |
| G3 | Admit visitors to the origin at an operator-controlled rate |
| G4 | Prove admission cryptographically, so the origin can reject queue-jumpers |
| G5 | Cost approximately nothing when no event is running |
| G6 | Deploy into a *client's own* AWS account, commercial or GovCloud |

### Non-goals

- Physical-location queueing (restaurants, clinics) — different product entirely.
- Being the origin's CDN or WAF. We integrate with those; we don't replace them.
- Multi-tenant SaaS. See §9.

---

## 2. Design principles

1. **Gaps are free; duplicates are not.** A skipped queue position is invisible to
   users. Two users holding position 40,001 is a correctness failure. Every design
   decision follows from this asymmetry.
2. **No compute in the ingest path.** The burst hits a service integration, not a
   function. Cold starts and concurrency limits must not exist at the front door.
3. **Zero cost at rest.** No always-on infrastructure. An idle waiting room should bill
   pennies per month.
4. **The client owns the data and the account.** We ship infrastructure-as-code, not a
   hosted service.
5. **Boring where it counts.** The counter is the product. It gets the least clever
   implementation available that meets the throughput target.

---

## 3. Architecture

### 3.1 Request flow — joining the queue

```
                    ┌──────────────────────────────────────────┐
                    │  AWS WAF + Bot Control                   │
                    │  (blocks scripted/scalper traffic)       │
                    └────────────────────┬─────────────────────┘
                                         │
                    ┌────────────────────▼─────────────────────┐
                    │  CloudFront                              │
                    │  cache: /queue_num 24h (per request_id)  │
                    │         /serving_num 5s (global)         │
                    │         /public_key 24h                  │
                    └────────────────────┬─────────────────────┘
                                         │
                    ┌────────────────────▼─────────────────────┐
                    │  API Gateway HTTP API                    │
                    │  AWS_PROXY / SQS-SendMessage             │
                    │  ← no Lambda in the burst path           │
                    └────────────────────┬─────────────────────┘
                                         │
                    ┌────────────────────▼─────────────────────┐
                    │  SQS standard queue  (+ DLQ)             │
                    │  the shock absorber                      │
                    └────────────────────┬─────────────────────┘
                                         │ BatchSize 100 / window 1s
                    ┌────────────────────▼─────────────────────┐
                    │  assign_queue_num  (Rust, arm64)         │
                    │  1× UpdateItem ADD :n → claims a range   │
                    │  N× PutItem (idempotent per request_id)  │
                    └────────────────────┬─────────────────────┘
                                         │
                    ┌────────────────────▼─────────────────────┐
                    │  DynamoDB (on-demand)                    │
                    │  Counters │ Positions │ Tokens           │
                    └──────────────────────────────────────────┘
```

### 3.2 Request flow — being admitted

```
  Operator / inlet strategy
        │  increments serving_counter (rate control)
        ▼
  DynamoDB: serving_counter = 5,000
        │
  Visitor polls /serving_num  ──► sees 5,000 ≥ own position 4,812
        │
  POST /generate_token ──► Rust Lambda signs RS256 JWT
        │
  Visitor → origin with Bearer token
        │
  ┌─────▼──────────────────────────────────────┐
  │ token_authorizer (Rust)                    │
  │ verifies sig / exp / aud / iss             │
  │ JWKS cached in-process (OnceCell)          │
  └────────────────────────────────────────────┘
```

---

## 4. The counter (the critical design decision)

### 4.1 Why not write sharding

The reflexive DynamoDB answer to a hot key is write sharding. **It does not apply
here.** Sharded counters are *sum-only*: they answer "how many?" but cannot issue a
unique ordered position. Reading shard 3 at 1,847 tells you nothing about what number
to hand the next visitor, and two concurrent readers could compute the same total.
Sharding destroys the global ordering that *is* the product.

The ~1,000 WCU/sec single-partition ceiling is therefore real and unavoidable for a
true sequence. Adaptive capacity and split-for-heat cannot split a single key.

### 4.2 Why an atomic counter is nonetheless correct and sufficient

Per AWS's own guidance
([Implement auto-increment with Amazon DynamoDB](https://aws.amazon.com/blogs/database/implement-auto-increment-with-amazon-dynamodb/)):

> There are no race conditions with this design because all writes to a single item in
> DynamoDB are applied serially. This ensures that each counter value will never be
> returned more than once.

`UpdateItem` + `ADD` + `ReturnValues: ALL_NEW` is a correct atomic sequence generator.
No transactions, no OCC, no experimental validation required.

### 4.3 Batch range allocation

One increment claims a whole batch's worth of positions:

```rust
// one write per batch, not per visitor
let n = records.len() as i64;
let end = ddb.update_item()
    .update_expression("ADD queue_counter :n")
    .return_values(ReturnValue::AllNew)
    .send().await?;
let start = end - n + 1;          // this batch owns [start, end]
```

Throughput scales linearly with `BatchSize`. SQS standard queues support batches up to
10,000 (batches >10 require `MaximumBatchingWindowInSeconds ≥ 1`):

| BatchSize | Window | Counter ceiling |
|---|---|---|
| 10 | 0s | 10K/sec |
| **100** | **1s** | **100K/sec ← default** |
| 1,000 | 1s | 1M/sec |
| 5,000 | 1–2s | ~5M/sec (6 MB payload cap ≈ 10–12K records) |

The 1-second batching window adds ~1s to *joining* a queue in which users then wait
minutes. It is imperceptible. Both values are Terraform variables.

### 4.4 Gap tolerance

An SDK retry after a 5xx, or a function dying mid-batch, can burn positions without
issuing them. **This is acceptable and undetectable** — no user checks whether the queue
skipped a number. The upstream Redis implementation has identical gap behavior
(`INCR` then `put_item` is the same two-step), so this is not a regression.

This tolerance is what unlocks the cheapest approach and, if ever needed, Hi/Lo leasing
(§4.5). An inventory system could not make this trade.

### 4.5 Escape hatches (documented, not built)

- **Hi/Lo block leasing** — each execution environment leases N positions and serves
  them from memory. DynamoDB sees one write per N visitors, fully decoupled from
  ingest, with no batching-window latency. Cost: stranded blocks on environment death,
  i.e. more gaps.
- **Strided sequences** — N counters where counter *i* issues positions ≡ *i* (mod N).
  Uniqueness preserved, ordering approximate across counters, ceiling × N. Unlike
  sharding, reads do not scatter-gather.

Neither is built in v1. Both are recorded so the ceiling question has a known answer.

### 4.6 Sequences vs. statistics

| Counter | Type | Sharding |
|---|---|---|
| `queue_counter` | sequence | **never** |
| `serving_counter` | sequence | **never** |
| `token_counter` | statistic | permitted |
| `completed_counter` | statistic | permitted |
| `abandoned_counter` | statistic | permitted |
| `expired_queue_counter` | statistic | permitted |

---

## 5. What we deliberately removed

The upstream solution deploys **151 CloudFormation resources**. Twenty-six of them
exist solely because ElastiCache Redis requires VPC attachment:

```
Lambdas total = 20,  in-VPC = 12
VPC endpoints: sqs, dynamodb, secretsmanager, events, lambda
VPC/Redis-coupled resources: 26 of 151
```

Redis held eight plain integers. DynamoDB, SQS, Secrets Manager, EventBridge and
Lambda are all IAM-authenticated public-endpoint services — a function outside a VPC
reaches them with no NAT and no endpoints. Removing Redis removes the entire subtree.

| | Upstream | This design |
|---|---|---|
| Counters | ElastiCache `cache.r6g.large` MultiAZ | DynamoDB atomic counters |
| Networking | VPC, NAT, 5 endpoints, flow logs | none |
| Resources | 151 | ~60–70 |
| Idle cost | ~$300/mo Redis + ~$32/mo NAT | ~$0 |
| Runtime | Python 3 + Chalice | Rust (`provided.al2023`, arm64) |
| IaC | CloudFormation | Terraform |
| Ingest | REST API → SQS (`type: aws`) | HTTP API → SQS (`SQS-SendMessage`) |

**A VPC remains available as an opt-in Terraform variable** for clients whose ATO
boundary mandates private-subnet compute and VPC endpoints regardless of IAM. That is
a policy requirement, not an architectural one, and arguing SigV4 equivalence loses to
a written control. The Lambda code is identical either way; only `vpc_config` and the
endpoint resources become conditional.

---

## 6. API surface

Contract-compatible with upstream, so existing integrations port cleanly.

### Public (CloudFront-fronted)

| Method | Path | Cache | Purpose |
|---|---|---|---|
| POST | `/assign_queue_num` | none | Join the queue (→ SQS) |
| GET | `/queue_num` | 24h per `request_id` | Read own position |
| GET | `/serving_num` | 5s global | Read current serving position |
| GET | `/waiting_num` | 5s global | Count still waiting |
| POST | `/generate_token` | none | Exchange served position for JWT |
| GET | `/public_key` | 24h | JWKS for token verification |
| GET | `/queue_pos_expiry` | 5s | Seconds until position lapses |

### Private (IAM / API key)

| Method | Path | Purpose |
|---|---|---|
| POST | `/increment_serving_counter` | Admit N more visitors |
| POST | `/update_session` | Mark session completed/abandoned |
| POST | `/reset_initial_state` | Reset event |
| GET | `/expired_tokens` | List expired tokens |
| GET | `/num_active_tokens` | Active token count |

`/serving_num` is the highest-volume read — every waiter polls it. CloudFront's 5s
global cache collapses 100K pollers into one origin fetch per 5 seconds. Read hotspots
are solved with cache, not shards.

---

## 7. Data model

**`Counters`** — PK `event_id`. One item holding all counters as numeric attributes.
Atomic `ADD` per §4.

**`Positions`** — PK `request_id`. `{event_id, queue_position, entry_time, status}`.
Written with `attribute_not_exists(request_id)` so SQS at-least-once redelivery is
idempotent. *(Upstream lacks this condition and can double-assign on redelivery.)*

**`Tokens`** — PK `request_id`. Issued JWT metadata, session status, TTL for automatic
cleanup.

All tables `PAY_PER_REQUEST`. Point-in-time recovery on by default.

---

## 8. Real bottlenecks

The counter is not the constraint after §4.3. In order:

| # | Limit | Default | Adjustable |
|---|---|---|---|
| 1 | API Gateway account throttle | 10,000 RPS/region | Yes — Service Quotas, needs lead time |
| 2 | API Gateway **burst** quota | 5,000 | **No** — set by the service team |
| 3 | Lambda concurrency | 1,000 | Yes |
| 4 | SQS ESM poller ramp | +300/min → 1,250 max | No |
| 5 | DynamoDB counter | 100K/sec at defaults | Via `BatchSize` |

Item 2 matters most: an on-sale is pure burst, and that quota cannot be raised. This is
why **pre-event readiness is a first-class deliverable**, not an afterthought — quota
increases filed with lead time, provisioned concurrency warmed, load test executed at
target rate. It is also the work Queue-it's sales engineers perform for enterprise
accounts, and therefore billable.

---

## 9. Deployment model

**Single-tenant, deployed into the client's AWS account. Not multi-tenant SaaS.**

Hosting other organizations' waiting rooms in our own account would make us a Cloud
Service Provider, requiring our own FedRAMP authorization: $250K–$2M initial,
6–24 months, ~$500K/yr continuous monitoring, plus ~$50K per Significant Change
Request. That is not viable for a small firm, and FedRAMP guidance pushes toward
dedicated infrastructure anyway.

Deploying per-client means the client inherits AWS's existing GovCloud authorization
under their own ATO, and we are a systems integrator writing Terraform. Blast radius
is one client. Each client pays their own AWS bill. The reusable asset is the module.

### GovCloud variant

CloudFront, CloudFront Functions and Lambda@Edge **do not exist in GovCloud**. AWS's
own public-sector guidance places CloudFront in a commercial region pointing at
GovCloud origins. Two consequences:

1. Edge gating is unavailable inside the boundary; gate at ALB/origin instead.
2. A commercial-region CloudFront fronting a GovCloud origin means request metadata
   transits a non-GovCloud service — a conversation to have with the client's AO.

The GovCloud module is therefore a genuinely different topology, shipped second and
priced accordingly.

---

## 10. Open questions

1. `SQS-SendMessage` HTTP API integration: confirm `$context.requestId` can be mapped
   into `MessageAttributes` to replace REST's `apig_request_id`. Blocks §3.1 if not.
2. JWKS rotation story — upstream effectively has none.
3. Inlet strategy interface: port upstream's periodic/max-size Lambdas, or expose a
   plain API and let clients drive it?
4. Whether to port the OpenID adapter at all (618 LOC upstream, lowest value).
