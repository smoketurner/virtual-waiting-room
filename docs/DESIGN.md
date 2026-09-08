# Design

Implementation of [`REQUIREMENTS.md`](./REQUIREMENTS.md). An AWS-native virtual waiting
room, deployed into the operator's own account.

Every quantitative claim is sourced in §14.

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

  T−0           EventBridge → assign_positions_batch (Rust)
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
  CloudFront   /status 5s global · /queue_num 24h per request_id · /public_key 24h
        │
  API Gateway REST (regional)
    request validator rejects malformed bodies
    type: aws → SQS SendMessage          ← no Lambda in the burst path
        │
  SQS standard + DLQ                      ← the shock absorber
        │ BatchSize 100 / window 1s
  assign_position (Rust, arm64)
    partition valid/invalid → ADD :valid_count → BatchWriteItem
        │
  DynamoDB  Counters · Positions · Tokens   (on-demand, pre-warmed)
```

### 2.3 Admission and session

Admission is a two-credential design, following Queue-it's published model. The admission
token proves the visitor cleared the line; the session cookie proves they already did so on
a previous request.

```
  Outflow controller increments serving_counter
    target rate adjusted for measured no-show rate      ← closed loop, §5
        │
  Visitor polls /status, sees serving ≥ own position
        │
  POST /generate_token → single-use admission token, short expiry
        │
  Origin request carrying the token
        │
  authorizer (Rust, at CloudFront or the origin)
    ├─ session cookie present and valid?  → continue
    ├─ admission token present and valid? → mint session cookie, strip token, continue
    └─ neither, and request is protected? → 302 to the waiting room
        │
  Origin — private subnet behind a CloudFront VPC origin (commercial regions)
```

**Why the session cookie is mandatory, not an optimization.** The admission token travels
on the URL. The URL changes the moment the visitor clicks anything. Without a session, the
visitor loses their credential on their second page view and is bounced back to the queue —
a correctness failure, not a performance one. The connector validates the token once, mints
the cookie, and strips the token from the URL.

The two credentials are signed over **different inputs** so neither can be replayed as the
other.

Every authorizer decision — protection match, session check, token check, expiry — is local.
No call to the waiting-room backend on the hot path. This is what allows the authorizer to
run at the edge and add only the cost of a signature verification per request.

---

## 3. Event lifecycle and operating modes

Satisfies F0.1–F0.8.

### 3.1 Phases

Every event moves through four phases, following Queue-it's model:

```
  IDLE ──────────► PRE-QUEUE ──────────► ACTIVE ──────────► POST-EVENT
  info page        countdown page        queue + admission   outcome page
  (static)         (static, cached)      (metered)           (static)
                                                │
                                    MAINTENANCE ┘  (any phase, operator-forced)
```

Three of the four phases serve a **static, operator-authored page from CDN cache**. Only
`ACTIVE` involves the queueing machinery. This is why a waiting room costs almost nothing
when nothing is happening, and why the phases that appear to be "nothing" are cheap to
support.

The idle and post-event pages are not decoration. They are where an operator tells visitors
what is coming, that stock is available, or where to go next when it has sold out — the
communication that determines whether a queue feels fair or feels broken.

**Maintenance mode** parks every visitor on an operator page regardless of phase or
capacity. Given the state machine it is nearly free to implement, and it is the control an
operator reaches for when something has gone wrong downstream.

### 3.2 How the lifecycle is implemented

| Concern | AWS mechanism |
|---|---|
| Phase state | An attribute on the `Counters` item in DynamoDB. One conditional `UpdateItem` transitions a phase. |
| Scheduled transitions | EventBridge Scheduler rules, one per transition, invoking a Rust Lambda. |
| Manual transitions | The same Lambda behind the admin API, so scheduled and manual paths share one code path. |
| Phase pages | Client-supplied HTML in S3, served through CloudFront with a long TTL. Phase changes swap the cache behaviour target, not the page content. |
| Current phase for clients | `/status`, a globally cached 5-second endpoint carrying phase, serving position, and operator message in one payload. |
| Maintenance mode | A phase override attribute checked before the normal phase; set and cleared through the admin API. |

The phase page and the `/status` endpoint together mean a visitor in idle or post-event
generates **one CloudFront cache hit and nothing else**. No Lambda, no DynamoDB read, no
API Gateway request.

### 3.3 Modes

**Scheduled** — a known start time. Idle → pre-queue → randomized assignment at T−0 →
active. §4.

**Standby** — dormant. The authorizer evaluates every request but its action is *continue*
until measured inflow crosses a threshold, at which point new visitors are queued **FIFO**.
Insurance against unplanned spikes: a social post, a news mention, an unannounced restock.

**Fairness differs by mode, deliberately.** Scheduled events randomize because everyone
knows the start time, so arrival order measures connection speed rather than intent.
Standby activation is FIFO because the spike was unplanned and nobody was waiting for a
starting gun. This is Queue-it's model and the reasoning holds independently.

Both modes run simultaneously on one origin. The configuration Queue-it's architect cites
from a real customer: a scheduled room with a deliberately low admission rate on the
high-demand path, plus standby across the whole site to catch visitors who flood the
homepage instead of the product page.

**Protection rules** declare which requests are subject to queueing, matching on path,
header, cookie, or user agent, distributed to the authorizer and evaluated locally.
Unmatched requests are never queued, in any phase or mode. Because rules can match headers
and user agent, they double as coarse anti-automation; WAF (§9) does the actual scoring.

### 3.4 How standby is implemented

Standby needs an inflow measurement that does not itself become load, and an activation
decision that reaches the authorizer quickly.

| Concern | AWS mechanism |
|---|---|
| Inflow measurement | CloudFront standard logs already count requests per behaviour. A CloudWatch metric math alarm on request rate is the trigger — no counting in our own code, no per-request write. |
| Activation decision | The alarm targets EventBridge, which invokes the phase Lambda to flip the event to `ACTIVE`. |
| Reaching the authorizer | The authorizer reads phase from the same `/status` payload it already fetches, cached 5 seconds. Activation therefore propagates in one cache TTL. |
| Deactivation | A second alarm on sustained low inflow, with a longer evaluation period so the queue does not flap. |
| Manual override | Admin API sets a forced phase that suppresses both alarms. |

Using CloudFront's own request metrics rather than counting requests ourselves is the point:
the measurement is free, already aggregated, and cannot become a bottleneck at exactly the
moment traffic spikes.

---

## 4. Pre-queue

Satisfies F1.1–F1.5, C1, C2.

**Countdown page.** Static HTML and JavaScript served from CloudFront with a long TTL.
Visitor count does not generate origin load (F1.2). The page polls a single globally-cached
`/status` endpoint for the transition to open.

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

## 5. Outflow control

Satisfies F3.2, F3.8, F3.10.

The operator declares a capacity — say 500 arrivals per minute — and the system releases
visitors at that rate. Naively this is "increment `serving_counter` by 500 each minute."

**That undercounts, because of no-shows.** A fraction of visitors whose turn arrives never
click through: they switched tabs, closed the browser, or gave up. Releasing exactly 500
positions delivers materially fewer than 500 real arrivals, so the origin runs below the
capacity the client is paying to use, and everyone still waiting waits longer than
necessary. Queue-it's architect names this as one of the genuinely hard parts of the
problem.

The fix is a closed loop. `/update_session` already reports completions and abandonments;
those figures feed back into the release rate:

```
observed_arrival_rate = arrivals in the last interval
no_show_rate          = 1 − (observed_arrival_rate / released_last_interval)
release_next          = target_rate / (1 − smoothed_no_show_rate)
```

The no-show rate is smoothed across intervals to avoid oscillation, and the correction is
bounded so a transient measurement error cannot release a damaging burst.

**How it runs.** An EventBridge Scheduler rule invokes the controller Lambda on a fixed
interval (default 10 seconds). The controller reads release and arrival counters from the
`Counters` item, computes the correction, and writes the new `serving_counter` with a single
`UpdateItem`. Arrivals are counted by the authorizer at admission and reported through
`/update_session`; the aggregate lives on the same item, so the controller does one read and
one write per interval regardless of event size.

**Position expiry is the other half.** A released position that is never claimed within the
configured window expires, and the serving counter advances past it (F3.9). Expiry is driven
by DynamoDB TTL on the `Positions` item plus a scheduled sweeper for positions whose expiry
must advance the counter. Expiry reclaims capacity from no-shows after the fact; the
controller compensates for them in advance. Both are needed — expiry alone reacts too slowly
to hold the origin at target during a short event.

---

## 6. Counter

Satisfies F2.2, F2.3, C3.

### 6.1 Atomic sequence, not sharding

Write sharding is the reflexive answer to a hot DynamoDB key, and it does not apply.
Sharded counters are sum-only: they answer "how many" but cannot issue a unique ordered
position. Sharding destroys the global ordering that is the product.

`UpdateItem` + `ADD` + `ReturnValues: ALL_NEW` is a correct sequence generator. AWS
documents the guarantee: writes to a single item are applied serially, and each value is
returned exactly once. No transactions, no optimistic concurrency control, no experiment
needed.

### 6.2 Batch range allocation

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

### 6.3 The real ceiling is `Positions`, not the counter

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

### 6.4 Cold-start capacity

On-demand tables serve ~4,000 writes/sec when new and grow to twice their previous peak. A
waiting room is idle by definition and has no meaningful previous peak, so an unprepared
deployment throttles at ~4,000/sec exactly when an on-sale starts.

DynamoDB warm throughput fixes this: pre-warming sets the throughput a table can absorb
instantaneously. Reading the value is free; pre-warming is billed. This is O1 — a
contractual pre-event step, not an optimization.

### 6.5 Sequences versus statistics

| Counter | Kind | Sharding |
|---|---|---|
| `queue_counter`, `serving_counter` | sequence | never |
| `token_counter`, `completed_counter`, `abandoned_counter`, `expired_queue_counter` | statistic | permitted if hot |

### 6.6 Gap tolerance

An SDK retry after a 5xx, or a function dying mid-batch, burns positions without issuing
them. Nobody checks whether a queue skipped a number (F2.3). This tolerance is what permits
the cheapest approach; an inventory system could not make the same trade.

---

## 7. Ingest

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

## 8. API surface and caching

Satisfies F3.1, F5.5, C4.

### Public

| Path | TTL | Cache key | Purpose |
|---|---|---|---|
| `/status` | 5s | global | Phase, serving position, admission rate, operator message — **one payload, one poll** |
| `/queue_num` | 24h | `event_id` + `request_id` | Own position; 404 means re-join |
| `/queue_pos_expiry` | 5s | `event_id` + `request_id` | Seconds until position lapses |
| `/public_key` | 24h | `event_id` | Signature verification material |
| `/join` | none | — | Join the queue or pre-queue; client-supplied UUIDv7 |
| `/generate_token` | none | — | Exchange a served position for an admission token |

**`/status` is deliberately one endpoint, not four.** Every waiting visitor polls it, so
collapsing phase, serving position, rate and operator message into a single globally-cached
payload means one CloudFront request per visitor per interval instead of several. Since the
poll interval is the dominant cost variable in the system (§13), the shape of this endpoint
is a cost decision as much as an API decision.

A position never changes once assigned, so `/queue_num` is cached per visitor for a day. The
5-second global cache on `/status` collapses a million pollers into one origin fetch per
interval (C4). Read hotspots are solved with cache, not sharding.

### Admin (SigV4)

| Path | Purpose |
|---|---|
| `/admin/phase` | Transition phase; force or clear maintenance mode |
| `/admin/rate` | Set target admission rate |
| `/admin/message` | Publish an operator message to waiting visitors |
| `/admin/reset` | Reset event state |
| `/admin/rules` | Update protection rules |
| `/metrics` | Event metrics as JSON for the client's own tooling |
| `/update_session` | Report completion or abandonment; feeds the outflow controller |

Every operator action is here, and the scheduled paths call the same Lambdas. There is no
capability available through a console that is unavailable through the API.

---

## 9. Security and abuse mitigation

Satisfies F3.3, F3.4, N7, O5.

**Credentials.** Two artifacts, signed with the same key over **different inputs** so
neither can be replayed as the other (F3.6):

- *Admission token* — proves the visitor cleared the line. Carries event id, queue id, and
  expiry. Travels on the URL, so it is short-lived and validated once.
- *Session* — minted by the authorizer after the token validates, scoped per event so a
  visitor can hold sessions for several waiting rooms concurrently. Supports both a sliding
  window extended on activity and a hard cap from issue time (F3.7); a hard cap is what you
  want for a ticket on-sale, where a session should not live indefinitely because someone
  keeps clicking.

The signing key is per-deployment and lives in Secrets Manager, distributed to the
authorizer. **Its compromise permits minting admission for every event in that
deployment**, so it is rotated on a schedule and treated as the deployment's most sensitive
material.

The authorizer holds keys and protection rules in memory, so every decision is local with no
backend round-trip (§2.3).

**Gating queue entry on a client-issued identifier.** The strongest anti-bot lever available
is refusing entry to anyone the client cannot vouch for. The client signs an identifier it
already holds — a membership number, promo code, order reference — with a shared key, and
the waiting room verifies the signature at join time (F6.1, F6.2). The waiting room stores
none of that data and cannot mint identifiers itself.

This inverts the usual bot problem. Instead of trying to detect automation from request
signatures, the queue admits only visitors the client has already established a
relationship with. Queue-it ships this as their Queue Token SDK, and their published
customer results — a Japanese gaming company keeping out 225,000 bots and non-members
during an invite-only drop — reflect the difference between detecting bots and never
letting them in.

**Timing of enforcement.** Where an operator can identify likely bots during the pre-queue,
they may choose to admit them to the pre-queue and block at randomization rather than at
arrival (F6.3). Blocking early reveals the detection and gives operators of automated
clients time to retool and rejoin before the sale starts. Queue-it sells this as Hype Event
Protection; for us it is a configuration choice that costs nothing to support.

**WAF, three layers:**

1. **Bot Control** — bot-versus-human discrimination. Safe in Block.
2. **ASN matching** — scalper infrastructure concentrates in a small number of hosting
   ASNs, making this cheap and effective here.
3. **Anti-DDoS managed rule group** — ships in **Count mode by default** (O5). It learns a
   traffic baseline, and a waiting room's legitimate peak is shaped exactly like a
   volumetric attack. AWS documents that baselines formed during an attack take two to
   three times longer to settle. Promotion to Block is a per-client decision after
   observing one real event.

**No public API key.** A public API key on a page served to browsers ships in client-side
JavaScript and is trivially extracted, so it is a throttling handle rather than a control.
WAF rate-based rules do that job properly. API keys remain available on REST if a client
wants a revocable handle for a partner integration.

**Origin protection.** In commercial regions, CloudFront VPC origins place the origin in a
private subnet with CloudFront as the sole ingress, making "nobody reaches the origin
without a token" architecturally enforceable rather than policy-enforced.

**WAF is a first-order cost, not a footnote.** It bills per request inspected on top of Bot
Control's per-request fee, against polling volume — frequently exceeding the CloudFront
bill. See §10.

---

## 10. Operator surface

Satisfies F5.1–F5.5.

An event that cannot be observed and adjusted while it runs is not usable in production.
Queue-it sells traffic intelligence and custom themes as separate products; both are table
stakes.

| Capability | AWS mechanism |
|---|---|
| Live metrics | Lambdas emit EMF-formatted logs; CloudWatch derives inflow, outflow, queue depth, admitted, no-show rate, expiry rate without a separate metrics pipeline. A dashboard ships with the module. |
| Metrics for the client's own tooling | `/metrics`, a cached JSON endpoint reading the same `Counters` item. |
| Branding | Client HTML, CSS and assets in S3, served through CloudFront. The module ships a reference theme; the client overrides the bucket contents. No fork, no rebuild. |
| Operator messaging | A string attribute on the `Counters` item, published through the admin API, delivered in the existing `/status` payload. Zero additional requests. |
| Position and estimated wait | `/queue_num` returns position; the client computes wait from the measured admission rate in `/status`. Recomputed as the operator changes the rate. |
| Operator actions | Admin REST API with SigV4, backed by the same Lambdas as the scheduled paths. |

**Two design points worth stating.**

The operator message rides in the `/status` payload the waiting page already polls every
five seconds. Broadcasting to a million waiting visitors therefore costs one DynamoDB write
and no additional requests — the message reaches everyone within a cache TTL. This is the
control that turns an incident into a *communicated* incident.

Estimated wait is derived from the measured admission rate, not a static assumption, so it
degrades gracefully when the operator changes the rate mid-event rather than showing a
number that has quietly become fiction.

**API-first.** Every operator action — rate change, phase transition, reset, pause,
maintenance mode, message publish — is an API call. There is no console, and no action that
requires one. Clients drive it from their own tooling or Terraform.

---

## 11. Failure behaviour

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

## 12. Deployment

Satisfies N2, N3, N4, N5.

**Single-tenant, in the client's account.** Hosting other organizations' waiting rooms
would make us a Cloud Service Provider requiring our own FedRAMP authorization
($250K–$2M initial, 6–24 months, ~$500K/yr continuous monitoring). Deploying per-client
means the client inherits AWS's existing authorization under their own ATO and we are a
systems integrator writing Terraform. Blast radius is one client; the reusable asset is the
module.

CloudFront SaaS Manager is tooling for the multi-tenant architecture this rejects. It fits
one narrow case — a single client running many branded domains — as a later variant.

**Why there is no VPC.** DynamoDB, SQS, Secrets Manager, EventBridge and Lambda are all
IAM-authenticated public-endpoint services. Functions outside a VPC reach them over the AWS
network with no NAT gateway and no VPC endpoints. Nothing in this design needs private
networking, so nothing pays for it.

This is worth stating explicitly because the obvious alternative — a Redis or Memcached tier
for the counters — forces the opposite. A cache tier requires VPC attachment, which forces
every function that touches a counter into private subnets, which then needs VPC endpoints
for every AWS service those functions call, plus a NAT gateway, subnets, route tables and
flow logs. Choosing DynamoDB for eight integers avoids that entire subtree and roughly
$330/month of idle cost.

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
`/status` inside the boundary. Origin protection is built from primitives — internal
ALB, token authorizer, security groups and IAM. This is a materially different topology, not
a configuration flag, and is priced separately.

---

## 13. Cost model

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

## 14. Sources

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
| Pre-queue randomization; redirect-and-token integration; Direct Pass fail-open; connector taxonomy and security tradeoffs | [How Queue-it Works](https://www.queue-it.com/developers/how-queue-it-works), [What's New August 2025](https://queue-it.com/blog/whats-new-august-2025/) |
| Distributed FIFO, open-window outflow control, no-show compensation, DynamoDB backbone at "a couple hundred thousand TPS", Safety Net, simultaneous scheduled + standby configuration | [Virtual Waiting Room System Design, Smooth Scaling ep. 17](https://queue-it.com/smooth-scaling-podcast/ep017-virtual-waiting-room-architecture/) — Mojtaba Sarooghi, Distinguished Product Architect, Queue-it |
| Two-credential model: single-use URL token validated once, then a separately-signed per-event session cookie; sliding vs fixed session validity; triggers matched on URL, headers, cookies, user agent; local validation with no backend round-trip | [Queue-it's architecture: the queue token, the cookie, and safety-net mode](https://blog.crawlex.net/blog/queue-it-architecture/) — teardown of Queue-it's open-source connector implementations |
| FedRAMP cost and timeline | Published 3PAO and FedRAMP advisory pricing, cross-checked across sources |
| Cost of a Redis/Memcached counter tier: VPC attachment, NAT gateway, VPC endpoints, ~$330/mo idle | Measured from an existing AWS reference deployment of this pattern |

---

## 15. Open questions

1. Pre-queue randomization algorithm — must be verifiably fair and auditable from a
   recorded seed.
2. Signing key rotation. Compromise permits minting admission for every event in the
   deployment, so rotation cannot be an afterthought.
3. Session credential format. Whether to follow Queue-it's HMAC-over-concatenation or use a
   JWT — the security property required is only that it signs different inputs from the
   admission token.
4. Standby inflow measurement. Where the threshold is evaluated (authorizer-local versus
   centrally aggregated) and how quickly activation must occur to be useful.
5. No-show controller tuning — smoothing window and correction bounds (§5), which need a
   real event's data.
6. Bot Control Common versus Targeted (§13) — resolve by measurement.
7. Connector breadth. Queue-it ships 25+ platform connectors across CDNs and application
   frameworks; we ship a CloudFront/origin authorizer. Product scope decision.
8. Whether an OpenID identity-provider adapter is worth building, or whether entry
   gating on a client-signed identifier (§9) covers the same need more simply.
