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
3. **Near-zero cost at rest.** No always-on infrastructure; an idle waiting room should
   bill pennies per month. **With one deliberate exception:** DynamoDB on-demand tables
   scale from their previous peak, and an idle table has none, so tables are *pre-warmed*
   (billed) ahead of a known event. Cheap at rest, paid before it matters — see §4.3b.
4. **The client owns the data and the account.** We ship infrastructure-as-code, not a
   hosted service.
5. **Boring where it counts.** The counter is the product. It gets the least clever
   implementation available that meets the throughput target.

---

## 3. Architecture

### 3.1 Request flow — joining the queue

```
                    ┌──────────────────────────────────────────┐
                    │  AWS WAF + Bot Control + ASN match       │
                    │  + Anti-DDoS rule group (COUNT by        │
                    │    default — see §3.1a)                  │
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
                    │  API Gateway REST API (regional)         │
                    │  type: aws → SQS SendMessage             │
                    │  request validator rejects bad bodies    │
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

### 3.1a Bot mitigation, and why Anti-DDoS ships in Count mode

Three WAF layers, in increasing order of how much trouble they can cause:

1. **Bot Control** — the bot-vs-human discrimination layer. Safe to run in Block.
2. **ASN matching** (June 2025) — match on the Autonomous System Number of the source IP.
   Scalper infrastructure concentrates in a small number of hosting ASNs, making this
   cheap and effective for exactly this workload.
3. **`AWSManagedRulesAntiDDoSRuleSet`** (June 2025) — detects, labels and challenges
   requests suspected of participating in L7 DDoS. Adds a Challenge action alongside
   Block/Count and needs 50 WCU.

**The third ships in Count mode by default, deliberately.** It establishes a traffic
baseline, and this workload has a pathological one: a waiting room's "normal" is zero
traffic, and its legitimate peak is shaped exactly like a volumetric attack. AWS warns
that baselines established during an attack take two to three times as long to settle.
Running it in Block for a first on-sale risks challenging legitimate visitors at the
worst possible moment.

Promotion to Block is an explicit per-client decision after observing at least one real
event. The operator runbook documents the COUNT-then-BLOCK discipline.

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

The *counter* ceiling scales linearly with `BatchSize`. SQS standard queues support
batches up to 10,000 (batches >10 require `MaximumBatchingWindowInSeconds ≥ 1`):

| BatchSize | Window | Counter WCU/s | Counter ceiling |
|---|---|---|---|
| 10 | 0s | 1,000 | 10K/sec |
| **100** | **1s** | **1,000** | **100K/sec** |
| 1,000 | 1s | 1,000 | 1M/sec |
| 5,000 | 1–2s | 1,000 | ~5M/sec (6 MB payload cap ≈ 10–12K records) |

The 1-second batching window adds ~1s to *joining* a queue in which users then wait
minutes. It is imperceptible. Both values are Terraform variables.

### 4.3a The counter is not the real ceiling — `Positions` is

Batching amortizes the *counter* write. It does nothing for the position writes. Each
batch performs **one** `UpdateItem` against `Counters` and **N** `PutItem`s against
`Positions` — one per visitor, unavoidably.

`Positions` is keyed on a UUIDv7 `request_id`, so it is evenly distributed across
partitions and has no hot-key problem. But it is still bounded by the DynamoDB
**per-table** on-demand quota:

| Limit | Value | Adjustable |
|---|---|---|
| On-demand per-table write quota | 40,000 WRU/s | Yes — Service Quotas |
| **Brand-new / long-idle table** | **~4,000 writes/s** | Via pre-warming (§4.3b) |
| Single-partition write limit | 1,000 WCU/s | No — but irrelevant here (keys are distributed) |

**So the real ingest ceiling is ~40,000 joins/sec at default quotas**, regardless of
`BatchSize`. Raising `BatchSize` above ~40 buys counter headroom that `Positions` cannot
use. The quota increase must be requested *in advance* — it is a pre-event readiness item
(§8) alongside the API Gateway RPS increase.

### 4.3b Cold-start capacity: pre-warm before every event

On-demand capacity scales to roughly **double the previous peak**, and a new table starts
at ~4,000 writes/sec. **A waiting room is idle by definition — it has no meaningful
previous peak.** Left alone, a freshly deployed waiting room throttles at ~4,000
joins/sec at exactly the moment an on-sale begins. This is the most likely way a first
production deployment fails.

DynamoDB **warm throughput** (Nov 2024; GovCloud Jan 2025) addresses this directly:

> Warm throughput value isn't a maximum limit on your table's capacity — rather, it's the
> minimum throughput that your table is prepared to handle instantaneously. If you
> pre-warm a table to support 100,000 write requests per second, your table will be ready
> to handle that traffic immediately.

Reading the warm throughput value is free; pre-warming is billed. `warm_throughput_*` is
therefore a Terraform variable on `Positions` and `Counters`, and **pre-warming is a
billable line item in the pre-event readiness engagement** — a real per-event cost with a
concrete failure it prevents.

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
| Ingest | REST API → SQS (`type: aws`) | REST API → SQS, client-supplied UUIDv7 (§8a) |

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
| POST | `/assign_queue_num` | none | Join the queue (client supplies UUIDv7 `request_id`; → SQS) |
| GET | `/queue_num` | 24h per `request_id` | Read own position; **404 = re-join with a fresh ID** |
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

**`Positions`** — PK `request_id` (client-supplied UUIDv7, validated in the Lambda).
`{event_id, queue_position, entry_time, status}`. Written with
`attribute_not_exists(request_id)` so SQS at-least-once redelivery *and* client retries
are both idempotent. *(Upstream lacks this condition and can double-assign on
redelivery.)* `entry_time` is stamped server-side and is authoritative — the UUIDv7
timestamp is client-supplied and must never be trusted (§8a).

**`Tokens`** — PK `request_id`. Issued JWT metadata, session status, TTL for automatic
cleanup.

All tables `PAY_PER_REQUEST`. Point-in-time recovery on by default.

---

## 8. Real bottlenecks

The counter is not the constraint after §4.3. In order:

| # | Limit | Default | Adjustable |
|---|---|---|---|
| 1 | API Gateway account throttle (refill rate) | 10,000 RPS/region | Yes — Service Quotas, needs lead time |
| 2 | API Gateway burst bucket | 5,000 requests | Not directly — derived from the RPS quota |
| 3 | **DynamoDB `Positions` per-table write quota** | **40,000 WRU/s** | **Yes — Service Quotas (§4.3a)** |
| 4 | **DynamoDB cold-start capacity** | **~4,000 writes/s** | **Yes — pre-warm (§4.3b)** |
| 5 | Lambda concurrency | 1,000 | Yes |
| 6 | SQS ESM poller ramp | +300/min → 1,250 max | Yes — Provisioned Mode (below) |
| 7 | DynamoDB counter | ≥100K/sec at defaults | Via `BatchSize` — not binding |

### Lambda Provisioned Mode for SQS ESM

The default ESM ramp (+300 concurrent/minute) is too slow for a spike that arrives in
under five seconds. Provisioned Mode (Nov 2025) scales **3× faster** (up to 1,000
concurrent executions per minute) and supports **16× higher concurrency** (up to 20,000),
configured as min (2–200) and max (2–2000) event pollers. Each poller handles up to
1 MB/s, 10 concurrent invokes, or 10 SQS polling calls per second. Billed in Event Poller
Units.

**Constraint for the Terraform module:** provisioned mode cannot be combined with the
maximum-concurrency setting — concurrency is controlled through poller count instead.

It costs money at rest, so it is an opt-in variable defaulting to off, enabled as part of
pre-event readiness.

### How the throttle actually behaves

API Gateway uses a token bucket. Tokens refill at the account RPS quota and the bucket
holds at most 5,000. **Steady-state capacity is governed by the refill rate, not the
bucket size.** The bucket only absorbs instantaneous submissions arriving faster than
the refill rate can service; when it empties, clients receive `429 Too Many Requests`.

The burst quota is
[not directly adjustable](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html) —
"determined by the API Gateway service team based on the overall RPS quota for the
account in the Region." Raising the RPS quota is the only lever that influences it.

**This is a smoothing buffer, not a ceiling on event size.** A 200,000-visitor on-sale
against a 50,000 RPS quota drains the bucket in the first instant and refills it within
seconds; a small number of visitors see a 429 at t=0 and succeed on retry. The failure
mode is a brief burst of retries, not a capped event.

Two consequences, both of which the product must handle explicitly:

1. **Client retry behavior matters as much as the quota.** The waiting-room page must
   treat a 429 as expected and retry with jittered backoff rather than surfacing an
   error. A client that fails closed converts a smoothing event into an outage. This is
   a requirement on the reference implementation, not a nicety.
2. **File the RPS increase early.** It is the only way to grow the burst bucket, and
   Service Quotas requests above the default open a support case rather than
   auto-approving.

This is why **pre-event readiness is a first-class deliverable**, not an afterthought —
quota increases filed with lead time, provisioned concurrency warmed, load test executed
at target rate. It is also the work Queue-it's sales engineers perform for enterprise
accounts, and therefore billable.

---

## 8a. Ingest API flavor and request identity

### Decision: REST API (regional), not HTTP API

An earlier draft of this design specified an HTTP API for the public routes, on the
grounds that HTTP APIs cost $1.00/M requests against REST's $3.50/M — a ~71% saving on
the highest-volume endpoint. **That reasoning was wrong, because it priced the wrong
volume.**

API Gateway only ever sees CloudFront *cache misses*. `/queue_num` is cached for 24h per
`request_id` and `/serving_num` is cached globally for 5s, so a visitor generates roughly
three origin requests regardless of how long they wait or how often they poll:

| | Requests | Cost |
|---|---|---|
| CloudFront | 243,000,000 | $182.25 |
| API Gateway (cache misses only) | 3,001,440 | |
| → REST API @ $3.50/M | | **$10.51** |
| → HTTP API @ $1.00/M | | **$3.00** |

*(1,000,000 visitors, 20-minute average wait, 5-second poll interval, 2-hour event.)*

**The entire saving is $7.50 per million-visitor event.** CloudFront costs ~17× the API
Gateway bill either way. The real cost lever is the client poll interval, not the API
flavor — moving from 5s to 10s polling saves $90 on the same event, twelve times more
than the API choice:

```
poll every  2s -> 600 polls/visitor -> CloudFront $452.25
poll every  5s -> 240 polls/visitor -> CloudFront $182.25
poll every 10s -> 120 polls/visitor -> CloudFront $ 92.25
poll every 30s ->  40 polls/visitor -> CloudFront $ 32.25
```

For $7.50 an HTTP API would cost us **request validation** (REST-only), **API keys and
usage plans** (REST-only), and **response body mapping via VTL** (REST-only) — and would
require workarounds invented solely to route around those gaps. Asynchronous ingest
already carries irreducible complexity (§8a below); adding avoidable complexity on top
of it to save $7.50 is a bad trade.

Use a **regional** REST API. CloudFront already fronts it; edge-optimized would stack a
second CDN.

*(HTTP APIs are available in GovCloud — only private integrations are restricted, which
this design does not use — so region availability is not a factor either way.)*

### The HTTP API constraints, recorded

Kept for the record so the question is not relitigated. Porting the upstream
`SQS-SendMessage` integration to an HTTP API hits four constraints:

1. **Message attributes — supported.** `$context` variables are documented mapping
   values; the bracket form is required inside a JSON string:
   `{"apig_request_id": {"DataType": "String", "StringValue": "${context.requestId}"}}`
2. **Response body transformation — not supported.** HTTP API response parameters accept
   only `append|overwrite|remove:header.name` and `overwrite:statuscode`. No VTL, no body
   mapping.
3. **API keys — not supported.** Neither API keys nor usage plans exist on HTTP APIs.
4. **Request validation — not supported.** Request validators
   (`x-amazon-apigateway-request-validator`) are REST-only.

Constraints 2 and 4 interlock: with no response body mapping, a server-generated
fallback ID could never be returned to the client, so "generate one if the client didn't"
is not an available behavior on an HTTP API.

### Request identity: client-generated UUIDv7 (retained)

The upstream REST integration mints the visitor's identity from `$context.requestId`,
stamping it onto the SQS message and returning it in the response body. **We keep the
REST API but do not keep this pattern**, because a client-supplied ID is better on its
own merits:

> With `$context.requestId`, a client retry mints a *fresh* ID and burns a second queue
> position. With a client-generated ID, the retry carries the same value and is absorbed
> by the `attribute_not_exists(request_id)` condition in §7.

The browser generates an [RFC 9562](https://www.rfc-editor.org/rfc/rfc9562.html) UUIDv7
and sends it in the request body.

**Why v7 rather than v4** — for modest but real reasons. DynamoDB hashes the partition
key, so v7's time ordering yields *no* locality benefit on `Positions` (and no
hot-partition penalty either). The gains are debuggability — join time is recoverable
from the ID during incident reconstruction — and headroom if a sort key or GSI is added
later.

**The embedded timestamp is not trustworthy.** It comes from the client's clock and is
trivially spoofed. `entry_time` is stamped server-side in the Lambda and remains the
sole source of truth for ordering and expiry. The v7 timestamp is a debugging
convenience, never a data source.

Note that browser-native `crypto.randomUUID()` emits v4 only; the reference client needs
the `uuid` package (≥ v11 exports `uuidv7()`) or a short hand-rolled generator.

### Handling a missing or malformed request ID

On a REST API this gets **two** layers.

**Gateway validation (first layer).** A request validator with a JSON Schema model
rejects a malformed or missing `request_id` with a 400 before it ever reaches SQS —
synchronously, so the client learns immediately.

**Lambda validation (backstop).** Schema validation cannot enforce UUIDv7 version bits,
and defense in depth is cheap here:

```rust
let (valid, invalid): (Vec<_>, Vec<_>) = records.iter()
    .partition(|r| parse_uuid_v7(&r.body).is_ok());

// increment by the VALID count only
let end = ddb.update_item()
    .update_expression("ADD queue_counter :n")
    .expression_attribute_values(":n", N(valid.len().to_string()))
    .return_values(ReturnValue::AllNew).send().await?;
```

**Incrementing by `records.len()` would be a bug with security consequences**: a flood of
malformed payloads would consume queue positions without producing queue members — a
cheap denial-of-fairness attack. Allocate only for messages that parse. Gateway
validation makes this path rare; it does not make it unnecessary.

Invalid messages are reported through `ReportBatchItemFailures` and land in the DLQ. The
client's subsequent `GET /queue_num?request_id=…` returns **404**, which the waiting-room
page treats as "generate a fresh ID and re-join." This is self-healing and doubles as the
recovery path for a genuinely lost message.

The residual asymmetry is inherent to asynchronous ingest: API Gateway returns 200 for a
request that may never yield a position. The 200 means *accepted into the queue*, not
*position assigned*. The 404-and-retry loop is what makes that safe, and it must be
explicit in the client contract.

### On the public API key

The upstream public API key is not a security control — it ships in client-side
JavaScript and is trivially extracted. It functions as a throttling handle, and WAF
rate-based rules serve that purpose better.

Because we are on a REST API, API keys and usage plans **remain available** as an
optional extra (e.g. a client who wants a revocable handle for a partner integration).
They are simply not the primary mechanism. WAF is.

The **private** API is unaffected: it uses SigV4 and carries genuine authorization.

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

### Protecting the origin with CloudFront VPC origins

CloudFront **VPC origins** (Nov 2024) serve content from ALBs, NLBs, or EC2 instances in
*private* subnets, making CloudFront the sole ingress and removing the need for a public
IP on the origin.

This matters more here than it does for a typical site. The waiting room's entire purpose
is ensuring nobody reaches the origin without a token. VPC origins make that
*architecturally* enforceable rather than merely policy-enforced — the origin is not
reachable from the internet at all, so bypassing the queue is not a matter of guessing a
URL. The token authorizer stops being the only line of defense.

It is also the natural fit for the GovCloud variant's ALB gating, **subject to verifying
VPC origins availability in the target GovCloud region** — the supported-region list is
explicit and this has not yet been confirmed.

### On CloudFront SaaS Manager (multi-tenant distributions)

CloudFront SaaS Manager (April 2025) offers multi-tenant distributions with reusable
templates, per-tenant parameters and ACM integration. **It does not apply to this
deployment model** — it is tooling for exactly the multi-tenant architecture this section
rejects. We deploy into the client's account, where a client has one domain and their own
certificate.

There is one narrower case where it genuinely fits: a *single* client running many
concurrent branded events — a ticketing company with dozens of venue domains, a retailer
with several brands. There, one client account holds many waiting-room front-ends and
SaaS Manager removes real per-domain toil. That is a Phase 5+ variant for a specific
customer profile, not a change to the core module. Note multi-tenant distributions
support **only WAF V2 web ACLs**.

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

1. ~~`SQS-SendMessage` HTTP API integration: confirm `$context.requestId` can be mapped
   into `MessageAttributes`.~~ **Resolved — see §8a.** Investigated, then reversed: the
   HTTP API saving is ~$7.50 per million-visitor event because API Gateway sees only
   CloudFront cache misses. Staying on a regional REST API keeps request validation, API
   keys and VTL. Client-generated UUIDv7 request IDs are retained on their own merits
   (retry idempotency).
2. JWKS rotation story — upstream effectively has none.
3. Inlet strategy interface: port upstream's periodic/max-size Lambdas, or expose a
   plain API and let clients drive it?
4. Whether to port the OpenID adapter at all (618 LOC upstream, lowest value).
