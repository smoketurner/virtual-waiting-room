# Design

Implementation of [`REQUIREMENTS.md`](./REQUIREMENTS.md). A maintained replacement for
the deprecated [`aws-solutions/virtual-waiting-room-on-aws`](https://github.com/aws-solutions/virtual-waiting-room-on-aws)
(archived November 2025), rebuilt in Rust and Terraform.

Every quantitative claim is sourced in §11.

---

## 1. Approach

A virtual waiting room meters visitors into an origin at a rate it can survive. The hard
part is not the queue; it is the arrival burst.

**The central design decision is that we do not absorb the burst — we remove the incentive
that creates it.** If queue position is assigned by arrival order, arriving early is an
advantage, so everyone arrives in the same second. Randomizing position assignment among
everyone present at the scheduled start removes that advantage, and with it two orders of
magnitude of peak load:

| Approach | Write rate at T−0 for 1M visitors |
|---|---|
| Live arrival order | 200,000–1,000,000/sec |
| Pre-queue, randomized, assigned over 5 min | 3,333/sec |

The second fits inside default AWS quotas. The first does not fit inside raised ones. This
follows Queue-it's published design, which randomizes pre-queue visitors "like a raffle" to
neutralize "any advantage to arriving early."

Everything else follows from three further rules:

1. **Gaps are free; duplicates are not.** A skipped position is invisible to users. Two
   users holding position 40,001 is a correctness failure. This asymmetry permits the
   cheapest correct counter implementation.
2. **No compute in the ingest path.** The burst reaches a service integration, not a
   function. Cold starts and concurrency limits must not exist at the front door.
3. **Fail open.** A waiting room that fails closed converts our outage into the client's
   outage — worse than having no waiting room.

---

## 2. Architecture

### 2.1 Scheduled event (primary path)

```
  T−n           Visitor → CloudFront → static countdown page
                (cached; zero origin requests per view)
                        │
                        │ register intent (once per visitor)
                        ▼
                  Pre-queue set in DynamoDB

  T−0           EventBridge → assign_queue_num_batch (Rust)
                  ├─ shuffle participant set with a recorded seed
                  ├─ single UpdateItem ADD claims the whole range
                  └─ paced BatchWriteItem into Positions
                        │
                        ▼
  T+            Visitors poll /queue_num → position assigned
```

### 2.2 Live join (walk-up arrivals after opening)

```
  WAF (Bot Control + ASN match + Anti-DDoS in Count)
        │
  CloudFront   /queue_num 24h per request_id · /serving_num 5s global · /public_key 24h
        │
  API Gateway REST (regional)
    request validator rejects malformed bodies
    type: aws → SQS SendMessage          ← no Lambda in the burst path
        │
  SQS standard + DLQ                      ← the shock absorber
        │ BatchSize 100 / window 1s
  assign_queue_num (Rust, arm64)
    partition valid/invalid → ADD :valid_count → BatchWriteItem
        │
  DynamoDB  Counters · Positions · Tokens   (on-demand, pre-warmed)
```

### 2.3 Admission

```
  Operator or inlet strategy increments serving_counter
        │
  Visitor polls /serving_num, sees serving ≥ own position
        │
  POST /generate_token → RS256 JWT
        │
  Origin request with Bearer token
        │
  token_authorizer (Rust) verifies sig/exp/aud/iss; JWKS cached in-process
        │
  Origin — private subnet behind CloudFront VPC origin (commercial regions)
```

---

## 3. Pre-queue

Satisfies F1.1–F1.5, C1, C2.

**Countdown page.** Static HTML and JavaScript served from CloudFront with a long TTL.
Visitor count does not generate origin load (F1.2). The page polls a single globally-cached
`/pre_queue_status` endpoint for the transition to open.

**Registration.** A visitor arriving during the pre-queue registers a UUIDv7 into a
pre-queue set. This is one write per visitor, spread across the entire pre-queue window
(typically minutes to hours), not concentrated at T−0.

**Assignment at T−0.** An EventBridge schedule triggers a Rust function that:

1. Reads the participant set.
2. Shuffles it using a seed recorded to the `Counters` item, making the result reproducible
   and auditable (F1.5).
3. Claims the full position range with one `UpdateItem ADD :n`.
4. Writes positions with paced `BatchWriteItem`, rate-limited to the configured window
   (F1.4).

Because the write rate is scheduled rather than driven by arrivals, it is a parameter we
choose rather than a burst we survive.

**Fairness.** Randomization is the fairness model for scheduled events: everyone present at
T−0 has equal probability of any position, regardless of connection speed or geography.
First-come-first-served applies to live joins after opening (F2.1).

---

## 4. Counter

Satisfies F2.2, F2.3, C3.

### 4.1 Atomic sequence, not sharding

Write sharding is the reflexive answer to a hot DynamoDB key, and it does not apply.
Sharded counters are sum-only: they answer "how many" but cannot issue a unique ordered
position. Sharding destroys the global ordering that is the product.

`UpdateItem` + `ADD` + `ReturnValues: ALL_NEW` is a correct sequence generator. AWS
documents the guarantee: writes to a single item are applied serially, and each value is
returned exactly once. No transactions, no optimistic concurrency control, no experiment
needed.

### 4.2 Batch range allocation

One increment claims a whole batch's worth of positions:

```rust
let n = valid.len() as i64;
let end = ddb.update_item()
    .update_expression("ADD queue_counter :n")
    .expression_attribute_values(":n", N(n.to_string()))
    .return_values(ReturnValue::AllNew)
    .send().await?;
let start = end - n + 1;              // this batch owns [start, end]
```

Increment by the count of **valid** messages, never `records.len()` — otherwise a flood of
malformed payloads consumes positions without producing queue members, a cheap
denial-of-fairness attack (F2.6).

### 4.3 The real ceiling is `Positions`, not the counter

Batching amortizes the counter write. It does nothing for position writes, which are one
per visitor.

| Limit | Value | Adjustable |
|---|---|---|
| DynamoDB per-table write, on-demand | 40,000 WRU/s | Yes — Service Quotas; explicitly not a maximum |
| Cold or long-idle table | ~4,000 writes/s | Yes — pre-warming |
| Single-partition write | 1,000 WCU/s | No — irrelevant; `request_id` keys are distributed |
| Counter item | 1,000 WCU/s ÷ batch size | Not binding at batch ≥ 10 |

So the live-join ceiling is ~40,000/sec at default quotas. `BatchSize` above ~40 buys
counter headroom that `Positions` cannot use. Default `BatchSize` is 100 with a 1-second
window, which costs one second on join in a queue where users then wait minutes.

### 4.4 Cold-start capacity

On-demand tables serve ~4,000 writes/sec when new and grow to twice their previous peak. A
waiting room is idle by definition and has no meaningful previous peak, so an unprepared
deployment throttles at ~4,000/sec exactly when an on-sale starts.

DynamoDB warm throughput fixes this: pre-warming sets the throughput a table can absorb
instantaneously. Reading the value is free; pre-warming is billed. This is O1 — a
contractual pre-event step, not an optimization.

### 4.5 Sequences versus statistics

| Counter | Kind | Sharding |
|---|---|---|
| `queue_counter`, `serving_counter` | sequence | never |
| `token_counter`, `completed_counter`, `abandoned_counter`, `expired_queue_counter` | statistic | permitted if hot |

### 4.6 Gap tolerance

An SDK retry after a 5xx, or a function dying mid-batch, burns positions without issuing
them. Nobody checks whether a queue skipped a number (F2.3). This tolerance is what permits
the cheapest approach; an inventory system could not make the same trade.

---

## 5. Ingest

Satisfies F2.4–F2.6, C5.

**Regional REST API with a direct SQS integration.** No Lambda in the burst path. SQS
standard queues are documented as supporting a "nearly unlimited number of API calls per
second," so ingest is not a constraint; every real limit is downstream.

**REST, not HTTP API.** HTTP APIs cost $1.00/M against REST's $3.50/M, but API Gateway only
ever sees CloudFront cache misses — roughly three per visitor regardless of wait length.
For a million-visitor event that is ~3M billable requests, so the saving is **$7.50**. HTTP
APIs would cost request validation, API keys, and VTL response mapping, all REST-only. Not
a trade worth making.

**Client-supplied UUIDv7.** The client generates its own request identifier rather than
receiving one minted by API Gateway. With a server-minted ID, a client retry produces a new
ID and burns a second position; with a client-supplied one the retry carries the same value
and is absorbed by the conditional write (F2.5). v7 over v4 for debuggability — join time is
recoverable from the ID — and future sort-key headroom. The embedded timestamp is
client-supplied and never trusted; `entry_time` is stamped server-side.

Browser `crypto.randomUUID()` emits v4 only, so the reference client uses the `uuid`
package.

**Two validation layers.** A gateway request validator rejects malformed bodies
synchronously with 400 (F2.4). The Lambda re-validates because schema validation cannot
check UUIDv7 version bits, and because the increment-by-valid-count rule depends on it.

**Recovery.** Invalid messages go to the DLQ via `ReportBatchItemFailures`. The client's
subsequent `GET /queue_num` returns 404, which the client treats as "re-join with a fresh
ID" (F4.4). This is also the recovery path for a genuinely lost message. API Gateway
returning 200 means *accepted into the queue*, not *position assigned*; the 404 loop makes
that safe.

---

## 6. Read path and caching

Satisfies F3.1, C4.

| Path | TTL | Cache key |
|---|---|---|
| `/queue_num` | 24h | `event_id` + `request_id` |
| `/public_key` | 24h | `event_id` |
| `/serving_num` | 5s | global |
| `/queue_pos_expiry` | 5s | `event_id` + `request_id` |
| `/pre_queue_status` | 5s | global |
| `/assign_queue_num` | none | — |

A position never changes once assigned, so it is cached per visitor for a day.
`/serving_num` is the endpoint every waiter polls; a 5-second global cache collapses a
million pollers into one origin fetch per interval (C4). Read hotspots are solved with
cache, not sharding.

**The client poll interval is the dominant cost variable in the whole system** — it
multiplies CloudFront requests, WAF inspections, and Bot Control charges simultaneously.
Default is 10 seconds, not the 5 the deprecated solution used.

---

## 7. Security and abuse mitigation

Satisfies F3.3, F3.4, N7, O5.

**Tokens.** RS256 JWT with claims `{sub, aud=event_id, iss, exp, token_use}`. The private
key is generated at deploy time into Secrets Manager; the public JWKS is served and cached
for 24 hours. The Rust authorizer holds the JWKS in a `OnceCell` rather than re-fetching.

**WAF, three layers:**

1. **Bot Control** — bot-versus-human discrimination. Safe in Block.
2. **ASN matching** — scalper infrastructure concentrates in a small number of hosting
   ASNs, making this cheap and effective here.
3. **Anti-DDoS managed rule group** — ships in **Count mode by default** (O5). It learns a
   traffic baseline, and a waiting room's legitimate peak is shaped exactly like a
   volumetric attack. AWS documents that baselines formed during an attack take two to
   three times longer to settle. Promotion to Block is a per-client decision after
   observing one real event.

**No public API key.** The deprecated solution's public API key ships in client-side
JavaScript and is trivially extracted; it is a throttling handle, not a control. WAF
rate-based rules do the job properly. API keys remain available on REST if a client wants a
revocable handle for a partner integration.

**Origin protection.** In commercial regions, CloudFront VPC origins place the origin in a
private subnet with CloudFront as the sole ingress, making "nobody reaches the origin
without a token" architecturally enforceable rather than policy-enforced.

**WAF is a first-order cost, not a footnote.** It bills per request inspected on top of Bot
Control's per-request fee, against polling volume — frequently exceeding the CloudFront
bill. See §10.

---

## 8. Failure behaviour

Satisfies F4.1–F4.5.

**Fail open by default.** If the waiting room API is unreachable, the authorizer admits the
visitor with a time-limited bypass cookie while the client retries in the background. This
mirrors Queue-it's Direct Pass. Configurable to fail closed for clients who prefer it, with
the tradeoff documented (F4.3).

**Gateway throttling is expected, not exceptional.** API Gateway's account throttle is a
token bucket: tokens refill at the RPS quota, the bucket holds at most 5,000. Steady-state
capacity comes from the refill rate; the bucket absorbs instantaneous arrivals above it and
sheds 429s when empty. The burst quota is not directly adjustable — AWS derives it from the
RPS quota — so raising RPS is the only lever.

This is a smoothing buffer, not a ceiling on event size. A 200,000-visitor burst against a
50,000 RPS quota drains and refills within seconds. The requirement is that the client
retries with jitter (F4.5); a client that fails closed on 429 turns a brief smoothing event
into a visible outage.

---

## 9. Deployment

Satisfies N2, N3, N4, N5.

**Single-tenant, in the client's account.** Hosting other organizations' waiting rooms
would make us a Cloud Service Provider requiring our own FedRAMP authorization
($250K–$2M initial, 6–24 months, ~$500K/yr continuous monitoring). Deploying per-client
means the client inherits AWS's existing authorization under their own ATO and we are a
systems integrator writing Terraform. Blast radius is one client; the reusable asset is the
module.

CloudFront SaaS Manager is tooling for the multi-tenant architecture this rejects. It fits
one narrow case — a single client running many branded domains — as a later variant.

**What the deprecated solution deployed, and why we do not.** It used ElastiCache Redis for
eight integer counters. Redis requires VPC attachment, so 12 of its 20 Lambdas ran in a VPC,
which cost them five VPC endpoints, three subnets, a NAT gateway, an EIP, route tables, and
flow logs — 26 of 151 resources existing solely to reach eight integers, plus roughly
$330/month idle.

DynamoDB, SQS, Secrets Manager, EventBridge and Lambda are IAM-authenticated public-endpoint
services. Functions outside a VPC reach them with no NAT and no endpoints.

| | Deprecated solution | This design |
|---|---|---|
| Counters | ElastiCache Redis, MultiAZ | DynamoDB atomic counters |
| Networking | VPC, NAT, 5 endpoints, flow logs | none |
| Resources | 151 | target ≤ 80 |
| Idle cost | ~$330/mo | ~$0 plus pre-warming |
| Runtime | Python 3 + Chalice | Rust, `provided.al2023`, arm64 |
| IaC | CloudFormation | Terraform |

A VPC remains available as an opt-in variable for clients whose ATO boundary mandates
private-subnet compute regardless of IAM. That is policy, not architecture; the Lambda code
is identical either way.

### GovCloud variant

CloudFront, CloudFront Functions, Lambda@Edge, and CloudFront VPC origins are **all
unavailable in GovCloud** — the VPC origins supported-region list covers 34 commercial
regions and neither GovCloud region appears. AWS's own public-sector guidance places
CloudFront in a commercial region pointing at GovCloud origins, which raises a data-boundary
question for the client's Authorizing Official.

Consequences: no edge gating, no managed origin protection, and no CDN cache collapse for
`/serving_num` inside the boundary. Origin protection is built from primitives — internal
ALB, token authorizer, security groups and IAM. This is a materially different topology, not
a configuration flag, and is priced separately.

---

## 10. Cost model

Satisfies O6.

Ordered by size for a million-visitor event at 10-second polling:

| Component | Driver | Approximate |
|---|---|---|
| CloudFront requests | poll interval × visitors × wait | $92 |
| WAF + Bot Control | same request volume | $87 + $123 (Common) or $1,230 (Targeted) |
| DynamoDB pre-warming | target write rate | billed per event |
| API Gateway | cache misses only, ~3/visitor | $10 |
| SQS, Lambda, DynamoDB writes | joins | negligible |

**Two decisions must be computed per client rather than assumed:**

*Flat-rate versus pay-as-you-go.* Flat-rate plans (Free $0 / Pro $15 / Business $200 /
Premium $1,000 per distribution per month) bundle CDN, WAF, DDoS protection, Bot Control,
Route 53 and TLS. Pay-as-you-go buys those separately. The crossover is non-monotonic
because WAF has a ~$23/month fixed floor plus a per-request component while plans have
monthly allowances — flat-rate wins at small and large events, PAYG in the middle band.
Bias toward flat-rate where the client needs a not-to-exceed number, since under PAYG a
volumetric attack bills WAF and Bot Control per-request on attack traffic.

*Bot Control Common versus Targeted.* Targeted costs ten times more per request and is
designed for bots that mimic human behaviour, which is what scalpers do. At 123M requests
the difference is $123 against $1,230 per event. There is no data yet on whether it catches
materially more for this workload. Resolve by running Targeted in Count mode during a real
event and measuring, not from the price sheet.

---

## 11. Sources

| Claim | Source |
|---|---|
| DynamoDB per-table on-demand quota 40,000 RRU/WRU, adjustable, "not maximum limits" | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| No account-level throughput quota in on-demand mode | same |
| New tables 4,000 writes/s, 12,000 reads/s; growth to 2× previous peak | [On-demand capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) |
| Single-partition 1,000 WCU/s | [Partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html) |
| Warm throughput and pre-warming | [Pre-warming DynamoDB tables](https://aws.amazon.com/blogs/database/pre-warming-amazon-dynamodb-tables-with-warm-throughput/) |
| Atomic counter serialization; each value returned once | [Implement auto-increment with DynamoDB](https://aws.amazon.com/blogs/database/implement-auto-increment-with-amazon-dynamodb/) |
| Counter approaches and failure modes | [Implement resource counters with DynamoDB](https://aws.amazon.com/blogs/database/implement-resource-counters-with-amazon-dynamodb/) |
| API Gateway 10,000 RPS, 5,000 burst not customer-adjustable | [API Gateway quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html) |
| Request validators are REST-only | [Request validation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-method-request-validation.html) |
| HTTP API response mapping limited to headers and status code | [HTTP API parameter mapping](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-parameter-mapping.html) |
| SQS standard "nearly unlimited API calls per second, per action" | [SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html) |
| ESM BatchSize to 10,000; window ≥1s above 10; 6 MB payload; +300/min to 1,250 | [SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
| Provisioned Mode: 2–200/2–2000 pollers, 1,000 concurrent/min, 20,000 max | [Provisioned Mode for SQS ESM](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-lambda-provisioned-mode-sqs-esm) |
| Anti-DDoS rule group; baselines during attack take 2–3× longer | [Anti-DDoS managed rule group](https://docs.aws.amazon.com/waf/latest/developerguide/waf-anti-ddos-rg-using.html) |
| WAF pricing: $5 ACL, $1 rule, $0.60/M; Bot Control $10/mo + $1/M or $10/M | [AWS WAF pricing](https://aws.amazon.com/waf/pricing) |
| CloudFront flat-rate tiers and allowances | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| VPC origins; supported regions exclude GovCloud | [Restrict access with VPC origins](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-vpc-origins.html) |
| CloudFront unavailable in GovCloud | [Setting up CloudFront with GovCloud resources](https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/setting-up-cloudfront.html) |
| Pre-queue randomization; redirect-and-token integration; Direct Pass fail-open | [How Queue-it Works](https://www.queue-it.com/developers/how-queue-it-works), [What's New August 2025](https://queue-it.com/blog/whats-new-august-2025/) |
| FedRAMP cost and timeline | Published 3PAO and FedRAMP advisory pricing, cross-checked across sources |
| Deprecated solution: 151 resources, 4,866 LOC, 26 VPC/Redis-coupled | Read directly from the archived repository |

---

## 12. Open questions

1. Pre-queue randomization algorithm — must be verifiably fair and auditable from a
   recorded seed.
2. JWKS rotation. The deprecated solution has no rotation story.
3. Bot Control Common versus Targeted (§10) — resolve by measurement.
4. Inlet strategy interface: port the deprecated periodic and max-size Lambdas, or expose
   the API and let clients drive it.
5. Connector breadth. Queue-it ships 25+ platform connectors; we ship one authorizer.
   Product scope decision.
6. Whether to port the OpenID adapter at all — 618 LOC upstream, lowest value.
