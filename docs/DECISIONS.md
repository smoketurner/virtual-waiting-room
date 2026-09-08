# Decision Record

Every quantitative claim in `DESIGN.md` traced to a primary source, with each decision
marked **FIRM** (settled by a published quota, price sheet, or shipped product) or
**PROVISIONAL** (depends on a measurement we have not yet taken).

Purpose: stop re-litigating settled questions, and make the unsettled ones visibly
unsettled rather than asserted.

**Rule for this document:** no conclusion without a citation and an explicit statement of
what was included in the comparison. Three earlier reversals in this project all had the
same cause — a single-variable comparison presented as a settled answer:

| Reversal | Variable omitted |
|---|---|
| HTTP API → REST API | API Gateway sees only CloudFront cache misses (~3M, not 243M) |
| PAYG → flat-rate | WAF + Bot Control are bundled in flat-rate, billed separately in PAYG |
| Counter ceiling → `Positions` ceiling | Batching amortizes counter writes, not per-visitor position writes |

---

## Part 1 — Constraints (primary sources)

### DynamoDB

| Constraint | Value | Adjustable | Source |
|---|---|---|---|
| Per-table throughput, on-demand | 40,000 RRU + 40,000 WRU/s | **Yes** | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| Account-level quota, on-demand | **none** | — | same — "No account-level read and write throughput quotas are applied to tables in on-demand mode" |
| New-table instant capacity | 4,000 writes/s, 12,000 reads/s | Via pre-warm | [On-demand capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) |
| Growth rate | 2× previous peak, instantly | Via pre-warm | same |
| Single-partition write | 1,000 WCU/s | **No** | [Partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html) |
| Atomic counter correctness | serialized per item, each value returned once | — | [Implement auto-increment](https://aws.amazon.com/blogs/database/implement-auto-increment-with-amazon-dynamodb/) (Hunter, DynamoDB Principal SA) |

The quota page states explicitly: *"You can request any number of read capacity units
(RCU) or write capacity units (WCU) for your DynamoDB tables through a service quota
increase. The values listed in the following table represent the initial default quotas.
These are not maximum limits for your tables."*

**Consequence:** 40,000 WRU/s is a paperwork constraint, not an engineering one. It still
must be raised *in advance* — a support ticket during an on-sale is not a mitigation.

### API Gateway

| Constraint | Value | Adjustable | Source |
|---|---|---|---|
| Account throttle (refill rate) | 10,000 RPS per region | **Yes** | [API Gateway quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html) |
| Burst bucket | 5,000 requests | **No** — derived from RPS quota | same |
| REST-only: request validators | — | — | [Request validation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-method-request-validation.html) |
| REST-only: API keys / usage plans | — | — | HTTP API docs list neither |
| REST-only: VTL response mapping | — | — | [HTTP API parameter mapping](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-parameter-mapping.html) — only `header.*` and `statuscode` |

### SQS

| Constraint | Value | Source |
|---|---|---|
| Standard queue throughput | *"nearly unlimited number of API calls per second, per action"* | [SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html) |
| `SendMessage` batch (API) | 10 messages | same |
| Lambda ESM `BatchSize` (standard) | 10,000 (>10 requires window ≥1s) | [SQS ESM config](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
| Invocation payload | 6 MB | same |
| Default ESM scaling | +300 concurrent/min → 1,250 max | same |
| Provisioned Mode | 2–200 min / 2–2000 max pollers; 1,000 concurrent/min ramp; 20,000 max | same + [launch post](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-lambda-provisioned-mode-sqs-esm) |

**SQS is not a constraint in this design.** Standard queue ingest is effectively
unbounded; every downstream limit is elsewhere.

### Pricing (per million requests unless noted)

| Item | Rate | Source |
|---|---|---|
| CloudFront requests | ~$0.75/M | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| API Gateway REST | $3.50/M | same page family |
| API Gateway HTTP | $1.00/M | same |
| WAF web ACL | $5/month | [WAF pricing](https://aws.amazon.com/waf/pricing) |
| WAF rule / rule group | $1/month each | same |
| WAF request inspection | $0.60/M | same |
| Bot Control Common | $10/mo + $1.00/M (first 10M/mo free) | same |
| Bot Control Targeted | $10/mo + $10.00/M (first 1M/mo free) | same |
| Flat-rate plans (per distribution/mo) | Free $0 / Pro $15 / Business $200 / Premium $1,000 | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| Flat-rate allowances | 1M / 10M / 125M / 500M requests | same |

---

## Part 2 — Prior art

### Queue-it (the incumbent, ~$10M ARR, published architecture)

Source: [How Queue-it Works](https://www.queue-it.com/developers/how-queue-it-works),
[How Does Queue-it Work](https://queue-it.com/how-does-queue-it-work),
[What's New August 2025](https://queue-it.com/blog/whats-new-august-2025/).

| Pattern | Queue-it | This design |
|---|---|---|
| Integration | 302 redirect + signed token, *not* a reverse proxy | same (token + authorizer) ✅ |
| Connector library | 25+ (CDN, proxy, app-layer, Lua/Fastly/SFCC/Magento…) | one authorizer ⚠️ gap |
| **Pre-queue with randomization** | **yes** | **absent** ❌ **gap — see D1** |
| Fail-open on waiting-room outage | "Direct Pass… fails open: visitors continue to your site with a time-out cookie" | **absent** ❌ **gap — see D2** |
| Post-queue phase | yes (informational page after event) | absent — minor |

The redirect-plus-token architecture matching independently is a good signal. The two
gaps are not cosmetic.

### AWS Virtual Waiting Room (deprecated Nov 2025)

Read directly from the archived repo: 151 CloudFormation resources, 4,866 Python LOC,
Redis for 8 integer counters, 12 of 20 Lambdas forced into a VPC to reach it. Contributed
the API contract we keep, and the batch-range-allocation idea (`rc.incr(QUEUE_COUNTER,
num_msg)`) that survives into our DynamoDB implementation.

---

## Part 3 — Decisions

### D1. Pre-queue with randomized position assignment — **ADOPT** (FIRM)

**This is the most significant gap found.** Queue-it gathers early visitors on a
countdown page, then *randomizes* them into queue positions when the timer hits zero,
explicitly "neutralizing any advantage to arriving early."

Our current design assigns positions in live arrival order. That makes arriving early an
advantage, which *guarantees* everyone arrives at t=0 — we manufacture the thundering
herd we then spend the whole design absorbing.

| Approach | Load at T-0 |
|---|---|
| Live arrival order (current) | 1M visitors / 1–5s = **200,000–1,000,000 writes/s** |
| Pre-queue + batch assign over 5 min | **3,333 writes/s** — within the *default* 40,000 quota |

The pre-queue page is a static countdown served entirely from CloudFront cache: no API
Gateway, no DynamoDB, no SQS.

**Why FIRM:** it is the market leader's published design, the arithmetic is decisive
(two orders of magnitude), and it is *more* fair rather than a fairness tradeoff — a fast
connection stops being an advantage.

**Consequence:** the live-join path remains, but for walk-up arrivals after the event
opens, not for the scheduled peak. This changes what Phase 3 must load-test.

### D2. Fail-open on waiting-room unavailability — **ADOPT** (FIRM)

Queue-it's Direct Pass "fails open": if the waiting room is unreachable, visitors proceed
to the site with a time-out cookie while the connector retries in the background.

Our design has no defined behavior here, which means it fails *closed* by default — if
the waiting room breaks, nobody reaches the client's site. **A waiting room that fails
closed is worse than no waiting room**: it converts our outage into the client's outage.

The authorizer must have an explicit, configurable failure mode, defaulting to open.

### D3. Regional REST API — **FIRM**

Settled: HTTP API saves $2.50/M on ~3M billable requests = **$7.50 per million-visitor
event**, and costs request validation, API keys, and VTL (all REST-only per docs above).
Not revisited.

### D4. DynamoDB atomic counter, batch range allocation — **FIRM**

Correctness is documented by AWS, not inferred. Gaps are acceptable in this domain
(§4.4). No transactions, no OCC, no sharding — sharding is sum-only and destroys the
ordering that *is* the product.

### D5. `Positions` per-table quota is the ingest ceiling — **FIRM**

40,000 WRU/s default, raisable to any value on request. With D1 adopted, the *scheduled*
peak no longer approaches it; the quota increase remains a pre-event item for live-join
headroom.

### D6. Pre-warm before every event — **FIRM**

4,000 writes/s cold vs 2× previous peak growth, against a workload that is idle by
definition. Documented behavior, not speculation.

### D7. CloudFront flat-rate vs PAYG — **PROVISIONAL, compute per client**

The honest answer is that **there is no single winner**, and asserting one was the
mistake. The crossover is non-monotonic because WAF has a ~$23/month fixed floor and a
per-request component, while plans have monthly allowances:

| Event (10s polling) | PAYG w/ Bot Common | PAYG w/ Bot Targeted | Cheapest plan |
|---|---|---|---|
| 10K visitors | $25 | $27 | Pro $15 → **flat** |
| 50K | $31 | $83 | Pro $15 → **flat** |
| 100K | $42 | $153 | Business $200 → **PAYG** |
| 400K | $129 | $571 | Business $200 → PAYG / **flat** |
| 1M @10s | $302 | $1,409 | Business $200 → **flat** |
| 1M @5s | $584 | $2,771 | Premium $1,000 → PAYG / **flat** |

**Decision: the Terraform module supports both; the cost model computes the crossover
from the client's actual event profile.** Bias to flat-rate where the client needs a
not-to-exceed number, because under PAYG a volumetric attack bills WAF inspection and Bot
Control per-request on attack traffic — an unbounded liability on a workload that
attracts attacks by design.

**PROVISIONAL because** the inputs (event size, poll interval, Common vs Targeted) are
client-specific and the Bot Control tier choice is itself unsettled — see D8.

### D8. Bot Control Common vs Targeted — **PROVISIONAL, needs measurement**

Targeted costs 10× per request ($10/M vs $1/M) and is designed for bots that mimic human
behavior — which is exactly what scalpers do. But we have no data on whether it actually
improves outcomes for this workload, and at 123M requests the difference is $123 vs
$1,230 per event.

**Do not decide this from a price sheet.** Resolve it in Phase 3/4 by running Targeted in
Count mode during a real event and measuring what it catches that Common does not.

### D9. Anti-DDoS rule group in Count mode — **FIRM**

AWS documents that the rule group learns a traffic baseline and that baselines formed
during an attack take 2–3× longer to settle. A waiting room's legitimate peak is shaped
like a volumetric attack. Count first, promote per client after observing a real event.

### D10. Not multi-tenant — **FIRM**

FedRAMP CSP obligations ($250K–$2M initial, 6–24 months, ~$500K/yr) make hosting other
organizations' waiting rooms unviable for a small firm. CloudFront SaaS Manager is tooling
for the architecture we rejected.

---

## Part 4 — Open, and honestly open

| # | Question | Resolves how |
|---|---|---|
| 1 | Bot Control Common vs Targeted (D8) | Measure in Count mode during a real event |
| 2 | Flat-rate vs PAYG per client (D7) | Compute from the client's event profile |
| 3 | Pre-queue randomization algorithm | Design; must be verifiably fair and auditable |
| 4 | JWKS rotation | Design; upstream has none |
| 5 | Connector breadth vs Queue-it's 25+ | Product scope decision, not technical |
| 6 | Inlet strategy interface | Port upstream's, or expose API only |
| 7 | Rust cold start with pre-warmed ESM | Measure in Phase 0 |

Anything not in this table is decided. Anything in it will not be asserted as decided
until the stated resolution method has been carried out.
