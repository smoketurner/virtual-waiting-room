# Design

How the virtual waiting room works. Requirements are in [`REQUIREMENTS.md`](./REQUIREMENTS.md);
the reasoning behind each choice is in [`adr/`](./adr/). Constraints and their sources are in
§14.

---

## 1. Overview

A virtual waiting room meters visitors into an origin at a rate the origin can sustain.

Two operating modes, which may run simultaneously on one origin:

- **Scheduled** — a known start time. Early arrivals are held on a countdown page and
  assigned randomized positions at the start ([ADR-0001](adr/0001-randomize-pre-queue-assignment.md)).
- **Standby** — dormant until measured inflow crosses a threshold, then queues new visitors
  first-in, first-out (FIFO).

Admission is a signed token, exchanged once for a session cookie, verified locally at the
authorizer without a call to the waiting-room backend.

---

## 2. Architecture

### 2.1 Scheduled event

```
  T−n   Visitor → CloudFront → static countdown page (S3, cached)
                                    │
                                    │ POST /join — one PreQueue item per visitor,
                                    │ spread across the pre-queue window
                                    ▼
                              DynamoDB PreQueue

  T−0   EventBridge Scheduler → seal_event (Rust, arm64)
          ONE UpdateItem on Counters:
            SET shuffle_seed = :seed, participant_count = :n, phase = :active
            ConditionExpression: attribute_not_exists(shuffle_seed)
                                    │
                                    ▼
  T+    /queue_num reads (s, l) from PreQueue, seed/N/offsets from /status, returns
        PRP(seed, offset[s]+l, N) — a keyed permutation evaluated per read
```

### 2.2 Live join

```
  WAF          Bot Control · ASN match · anti-DDoS (Count mode, §8)
        │
  CloudFront   polled behaviour: Min TTL 1 s, no cookies forwarded
        │
  API Gateway  REST, regional
               request validator (JSON Schema) → 400 on malformed body
               type: aws → SQS SendMessage        ← no Lambda in the burst path
        │
  SQS          standard queue + DLQ (maxReceiveCount 5)
        │      ESM: BatchSize 100, MaximumBatchingWindowInSeconds 1,
        │      FunctionResponseTypes: [ReportBatchItemFailures]
        ▼
  assign_position (Rust, arm64)
        1. partition records into valid / invalid (UUIDv7 parse)
        2. UpdateItem ADD queue_counter :valid_count / ALL_NEW
        3. PutItem ConditionExpression: attribute_not_exists(request_id)
        4. return batchItemFailures for invalid or failed records
        │
        ▼
  DynamoDB     Counters · PreQueue · Positions · Tokens
```

### 2.3 Admission

```
  Outflow controller (EventBridge Scheduler, every 10 s)
        1. sum arrivals#0..9 and released count
        2. no_show_rate = 1 − (arrivals / released)
        3. release = target_rate / (1 − smoothed_no_show_rate), bounded
        4. UpdateItem serving_counter
        5. expire positions past expires_at, advance max_expired_position
        │
        ▼
  Visitor polls /status → serving_counter ≥ own position
        │
  POST /generate_token → admission token (short expiry, single use)
        │
        ▼
  authorizer (Rust — CloudFront VPC origin, or at the client origin)
     ├─ valid session cookie?        → forward to origin
     ├─ valid admission token?       → set session cookie, strip token from URL,
     │                                 ADD arrivals#(hash % 10), forward
     ├─ request not protected?       → forward
     ├─ waiting room unreachable?    → forward with bypass cookie (§10)
     └─ otherwise                    → 302 to the waiting room
        │
        ▼
  Client origin — private subnet, reachable only via the CloudFront VPC origin
```

---

## 3. Event lifecycle

```
  IDLE ──────────► PRE-QUEUE ──────────► ACTIVE ──────────► POST-EVENT
  info page        countdown page        queue + admission   outcome page
  (static)         (static, cached)      (metered)           (static)
                                                │
                                    MAINTENANCE ┘  (any phase, operator-forced)
```

Three of the four phases serve a static operator-authored page from content delivery network
(CDN) cache. Only `ACTIVE` runs the queueing machinery.

| Concern | Mechanism |
|---|---|
| Phase state | Attribute on the `Counters` item; one conditional `UpdateItem` transitions a phase |
| Scheduled transitions | EventBridge Scheduler rules invoking the phase Lambda |
| Manual transitions | The same Lambda behind the admin API |
| Phase pages | Client HTML in S3, served through CloudFront with a long time to live (TTL) |
| Current phase for clients | `/status`, cached 5 s globally |
| Maintenance mode | A phase override attribute checked before `phase` |

A visitor in `IDLE` or `POST-EVENT` generates one CloudFront cache hit and nothing else.

### Modes

**Scheduled** — idle → pre-queue → randomized assignment at T−0 → active (§4).

**Standby** — the authorizer evaluates every request but its action is *continue* until
inflow crosses a threshold, then new visitors are queued FIFO.

Fairness differs by mode: scheduled events randomize because the start time is published, so
arrival order measures connection latency rather than intent; standby is FIFO because the
spike is unplanned and arrival order carries information.

**Protection rules** declare which requests are subject to queueing, matching on path,
header, cookie, or user agent, evaluated locally at the authorizer. Unmatched requests are
never queued in any phase or mode.

### Standby activation

CloudFront publishes a `Requests` metric per distribution to CloudWatch in `us-east-1` at
1-minute granularity, at no additional charge and without counting against CloudWatch quotas.

| Concern | Mechanism |
|---|---|
| Inflow measurement | CloudWatch alarm on `AWS/CloudFront` `Requests`, `Sum`, 60 s period |
| Activation | Alarm → EventBridge rule → phase Lambda sets `ACTIVE` |
| Propagation | Authorizer reads phase from `/status`, cached 5 s |
| Deactivation | Second alarm on sustained low `Requests`, longer evaluation period |
| Manual override | Admin API sets a forced phase suppressing both alarms |

**Activation latency is bounded at approximately 125 seconds** — up to 60 s metric
publication, up to 60 s alarm evaluation, up to 5 s cache TTL. Standby protects against a
spike persisting for minutes. It does not protect against one that saturates an origin in
under two minutes; scheduled mode exists for events with a known start time.

---

## 4. Pre-queue and position assignment

### 4.1 Registration

A visitor arriving during the pre-queue calls `POST /join`, which follows the same path as a
live join (§6). The handler claims a per-shard local registration index from a striped atomic
counter and writes one `PreQueue` item
([ADR-0015](adr/0015-stripe-prequeue-counter.md)):

| Attribute | Value |
|---|---|
| `r` (partition key) | `request_id`, a client-supplied universally unique identifier, version 7 (UUIDv7) |
| `s` | shard, `hash(request_id) % 10` |
| `l` | local index within shard `s`, from `ADD prequeue_counter#s :1` / `ALL_NEW` |
| `t` | server-stamped registration time |

Written with `ConditionExpression: attribute_not_exists(r)`, so a duplicate join consumes no
index. `prequeue_counter` is striped across 10 shards (`prequeue_counter#0`–`#9`), raising the
registration ceiling from the ~1,000/s single-item limit to ~10,000/s. Each shard counts its
own local indices `0..count_s`; the contiguous global index `i` is assembled at T−0 (§4.2), not
at registration. Registration writes spread across the pre-queue window — minutes to hours —
rather than concentrating at T−0.

The countdown page is static HTML and JavaScript in S3 behind CloudFront, polling `/status`.
Page views generate no origin requests.

### 4.2 Assignment

Queue order is a bijection from registration index to queue position, realised as a keyed
**pseudorandom permutation (PRP)** rather than stored rows
([ADR-0002](adr/0002-seeded-permutation-not-materialised-shuffle.md)).

At T−0 the phase Lambda performs one `UpdateItem` on `Counters`. It reads the 10 shard counts
(`prequeue_counter#0`–`#9`), computes the prefix offsets `offset[s] = Σ counts[0..s)` and the
cohort size `N = Σ counts`, and writes them alongside the seed and phase in the same conditional
write ([ADR-0015](adr/0015-stripe-prequeue-counter.md)):

```
UpdateExpression: SET shuffle_seed = :seed, participant_count = :n,
                      prequeue_offsets = :offsets, phase = :active
ConditionExpression: attribute_not_exists(shuffle_seed)
```

Nothing else is written. Assignment is complete when that single conditional write succeeds,
so there is no interval during which some participants hold positions and others do not.

A participant's **global registration index** is `i = offset[s] + l`, where `s` and `l` are the
shard and local index in their `PreQueue` item. Because the shards partition the cohort and the
offsets are a prefix sum, the global indices are exactly the contiguous range `[0, N)` (where
`N` is the number of indices *issued*; a burned index maps to an unclaimed position, F2.3) —
the permutation domain is unchanged by striping.

Position is derived on read:

```
queue_position = PRP(shuffle_seed, i, participant_count)     // i = offset[s] + l
```

`/queue_num` reads the visitor's `PreQueue` item for `(s, l)` and the seed, count, and offsets
from `/status`; it reconstructs `i` with one addition. All three are already being fetched. A
join that raced the seal can produce `i ≥ participant_count`; `/queue_num` treats that as
"registered too late" and returns a live-join position behind the whole pre-queue cohort,
rather than evaluating `PRP` out of domain.

### 4.3 The permutation

A balanced Feistel network over a power-of-two domain, with cycle-walking to restrict output
to `[0, N)` — the standard small-domain construction underlying format-preserving encryption
(FPE).

```
b        = ceil(bit_length(N-1) / 2)         // half-width
domain   = 2^(2b)                            // smallest power of 4 >= N
F(r, x)  = HMAC-SHA256(seed, r || x)[0..4) & (2^b - 1)

enc(v):  L, R = v >> b, v & mask
         repeat 4 rounds:  L, R = R, L XOR F(round, R)
         return (L << b) | R

PRP(seed, i, N):  v = i
                  loop: v = enc(v); if v < N return v      // cycle-walk
```

#### Frozen wire encoding (audit contract)

The permutation is part of the audit contract: a third party must recompute every position
byte-for-byte. The following encoding is **frozen** — an implementation that deviates produces
different positions and breaks auditability (§4.4). All multi-byte integers are **big-endian**.

- **Domain.** `b = ceil(bit_length(N - 1) / 2)`; `domain = 2^(2b)`; `mask = 2^b - 1`. Both
  halves are exactly `b` bits. `N = 1` is a degenerate single-element identity (no rounds run).
- **Rounds.** Exactly **4** rounds, `round ∈ {0, 1, 2, 3}` in ascending order.
- **HMAC key.** The 256-bit `shuffle_seed`, used verbatim as the HMAC-SHA256 key. It is the key,
  never part of the message.
- **Message `r || x`.** The concatenation of exactly two fixed-width fields, total **5 bytes**:
  - `r` — the round number as **1 byte** (`0x00`–`0x03`).
  - `x` — the right half `R` as a **4-byte big-endian `u32`** (zero-padded; `R < 2^b ≤ 2^32`,
    since `b ≤ 32` for `N ≤ 2^64`).
- **Output → integer.** Take the **first 4 bytes** of the 32-byte HMAC output, interpret as a
  big-endian `u32`, then `& mask`. `F(r, x) = be_u32(HMAC-SHA256(seed, r ‖ x)[0..4]) & mask`.
- **Split / combine.** `L = v >> b`, `R = v & mask`; one round is `L, R = R, L XOR F(round, R)`;
  recombine `(L << b) | R`.
- **Cycle-walk.** Re-apply `enc` until the result is `< N`; return it.

A "determinism across processes" property test pins this encoding with fixed `(seed, i, N)` →
`position` vectors so any drift in field width, byte order, or the HMAC key/message split fails
the build.

| Property | Evidence |
|---|---|
| Bijective | Feistel networks are invertible by construction; cycle-walking preserves this on the restricted domain. Verified: 200,000 samples at N=1,000,000 gave 200,000 distinct positions |
| Uniform | Verified at N=10,000 across 10 deciles: exactly 1,000 each, χ² = 0.0 against a 16.9 critical value at p=0.05, df=9 |
| Deterministic | Same seed, index and N always yield the same position |
| Cheap | Expected cycle-walk iterations = `domain / N`, bounded by 4 and equal to 1.05 at N=1,000,000. Four hash-based message authentication code (HMAC) evaluations per iteration |

**The seed does not exist before T−0.** No participant can compute their position early or
select a favourable registration index. The seed is written at T−0 and published in `/status`
from that moment.

### 4.4 Auditability

Given `shuffle_seed`, `participant_count`, `prequeue_offsets`, and the `(request_id, s, l)`
tuples in `PreQueue`, any third party recomputes every global index `i = offset[s] + l` and
every position, and confirms the ordering.

Positions are written to `Positions` lazily, when a visitor is admitted, carrying
`entry_time`, `status` and `expires_at` for the outflow controller. Only visitors who reach
the front generate a row.

### 4.5 Live joins after opening

A visitor joining after T−0 has no pre-queue registration and takes a position from
`queue_counter`, which starts at `participant_count`. Live joins are ordered
first-come-first-served behind every pre-queue participant, using the batch range allocation
in §5.2. The permutation applies only to the pre-queue cohort.

---

## 5. Counters and data

### 5.1 Atomic sequence

`UpdateItem` with `ADD` and `ReturnValues: ALL_NEW` generates the position sequence. Writes
to a single item are applied serially, so each value is returned exactly once
([ADR-0003](adr/0003-dynamodb-counters-not-elasticache.md)).

### 5.2 Batch range allocation

One increment claims a batch's worth of positions:

```rust
let n = valid.len() as i64;
let end = ddb.update_item()
    .update_expression("ADD queue_counter :n")
    .expression_attribute_values(":n", N(n.to_string()))
    .return_values(ReturnValue::AllNew)
    .send().await?;
let start = end - n + 1;              // this batch owns [start, end]
```

Increment by the count of **valid** records, never `records.len()`; otherwise malformed
payloads consume positions without producing queue members.

### 5.3 Throughput

| Limit | Value | Adjustable |
|---|---|---|
| DynamoDB per-table write, on-demand | 40,000 write request units (WRU)/s | Yes — Service Quotas; explicitly not a maximum |
| Cold or long-idle table | ~4,000 writes/s | Yes — pre-warming |
| Single-partition write | 1,000 write capacity units (WCU)/s | No — irrelevant; `request_id` keys are distributed |
| Counter item | 1,000 WCU/s ÷ batch size | Not binding at batch ≥ 10 |

The live-join ceiling is ~40,000/s at default quotas. `BatchSize` above ~40 buys counter
headroom that `Positions` cannot use. Default is 100 with a 1-second window.

On-demand tables serve ~4,000 writes/s when new and grow to twice their previous peak. A
waiting room is idle by definition and has no meaningful previous peak, so **tables are
pre-warmed before every event** (O1). Pre-queue assignment needs no pre-warming, being one
write.

### 5.4 Sequences and statistics

| Counter | Kind | Write sharding |
|---|---|---|
| `queue_counter`, `serving_counter` | sequence | never — sharding destroys ordering |
| `prequeue_counter` | index (order-free) | sharded ×10 — reassembled to `[0, N)` at T−0 |
| `arrivals` | statistic | sharded ×10 — hot at high admission rates |
| `token_counter`, `completed_counter`, `abandoned_counter`, `expired_queue_counter` | statistic | permitted if hot |

A sequence must yield a unique ordered value; summing shards cannot produce one, so
`queue_counter` and `serving_counter` stay single-item. `prequeue_counter` is the exception: the
permutation needs registration indices only to be **unique and within `[0, N)`**, not to arrive
in order, so it is striped ×10 for throughput and reassembled into a contiguous range by prefix
offsets at T−0 ([ADR-0015](adr/0015-stripe-prequeue-counter.md)). Write sharding here is a
throughput technique, distinct from shuffle sharding, which is an isolation technique
([ADR-0008](adr/0008-partition-isolation-not-shuffle-sharding.md)).

Positions may contain gaps. A retry after a 5xx, or a function dying between the counter
increment and the position write, burns positions without issuing them. No user observes a
skipped number.

### 5.5 Tables

**`Counters`** — partition key `event_id`. One item per event:

| Attribute | Type | Purpose |
|---|---|---|
| `queue_counter` | N | Live-join position sequence |
| `prequeue_counter#0`–`#9` | N | Sharded registration index (local per shard; ADR-0015) |
| `prequeue_offsets` | L | Per-shard prefix offsets, written at T−0 to assemble `[0, N)` |
| `serving_counter` | N | Admission high-water mark |
| `max_expired_position` | N | Highest expired position |
| `arrivals#0`–`arrivals#9` | N | Sharded arrival count |
| `phase` | S | `idle` / `pre_queue` / `active` / `post_event` |
| `phase_override` | S | Maintenance mode, checked before `phase` |
| `target_rate` | N | Operator-set admissions per minute |
| `shuffle_seed` | B | 256-bit permutation key, written once at T−0 |
| `participant_count` | N | Pre-queue cohort size, the permutation domain |
| `operator_message` | S | Delivered in `/status` |

**`PreQueue`** — partition key `r`. Attributes `s` (shard), `l` (local index), `t`. Short
attribute names because the table is scanned during audit. The global registration index
`i = offset[s] + l` is derived on read, never stored. Read by `/queue_num` as a single
`GetItem`; never scanned on the hot path.

**`Positions`** — partition key `request_id`. Attributes `event_id`, `queue_position`,
`entry_time`, `status`, `expires_at`, `ttl`. Written with
`ConditionExpression: attribute_not_exists(request_id)`. `entry_time` is server-stamped and
authoritative; the UUIDv7 timestamp is client-supplied and untrusted.

**`Tokens`** — partition key `request_id`. Admission-token metadata and session status.

All tables use on-demand capacity with point-in-time recovery (PITR), and carry
`warm_throughput_write_units` sized to the event's target rate.

---

## 6. Ingest

Regional Representational State Transfer (REST) API with an `AWS` service integration to
Simple Queue Service (SQS) `SendMessage`. No Lambda in the burst path, so no cold start and
no concurrency ceiling at ingest ([ADR-0005](adr/0005-rest-api-not-http-api.md)).

| Limit | Value | Effect here |
|---|---|---|
| REST API integration timeout | 29 s, hard | Irrelevant: `SendMessage` is single-digit ms |
| REST API request payload | 10 MB | Join payload is ~100 bytes |
| SQS standard throughput | "nearly unlimited API calls per second, per action" | Not a constraint |
| Lambda ESM `BatchSize` | 10,000 (>10 requires window ≥1 s) | Default 100 / 1 s |
| Lambda sync invocation payload | 6 MB | ~500 B/record caps a batch near 10–12K records |

### Duplicates and failures

SQS standard queues are at-least-once with best-effort ordering. Both are acceptable:

- **Duplicates** fail the `attribute_not_exists(request_id)` condition and consume no
  position.
- **Ordering** does not matter; positions come from an atomic counter, not message sequence.

The event source mapping (ESM) sets `FunctionResponseTypes: [ReportBatchItemFailures]`, so
only failed record identifiers return to the queue. Visibility timeout is six times the
function timeout plus `MaximumBatchingWindowInSeconds`. `maxReceiveCount` is 5, after which
records move to the dead-letter queue (DLQ).

### Validation

A gateway request validator with a JSON Schema model rejects a malformed or missing
`request_id` with 400, synchronously. The Lambda re-validates, parsing the identifier and
checking the version nibble, which JSON Schema cannot express.

### Recovery

Invalid records reach the DLQ. The client's subsequent `GET /queue_num` returns 404, which
the client treats as "re-join with a fresh UUIDv7". API Gateway returning 200 means *accepted
into the queue*, not *position assigned*; the 404-and-rejoin loop makes that asymmetry safe
and is part of the client contract.

---

## 7. Outflow control

The operator declares a capacity — say 500 arrivals per minute. A fraction of visitors whose
turn arrives never click through, so releasing exactly 500 positions delivers fewer than 500
arrivals. The controller closes the loop:

```
observed_arrival_rate = arrivals in the last interval
no_show_rate          = 1 − (observed_arrival_rate / released_last_interval)
release_next          = target_rate / (1 − smoothed_no_show_rate)
```

The rate is smoothed across intervals to avoid oscillation and the correction is bounded, so
a transient measurement error cannot release a damaging burst.

An EventBridge Scheduler rule invokes the controller every 10 seconds. It sums the arrival
shards, reads the released count, computes the correction, and writes `serving_counter` with
one `UpdateItem`.

**Counting arrivals.** The authorizer increments an arrival counter when it converts an
admission token into a session — one write per admitted visitor. At a 60,000/minute admission
rate that is 1,000 writes/s, the single-item ceiling, so the counter is sharded across 10
items chosen by `hash(request_id) % 10`. Cost: 10 reads per interval, independent of event
size.

**Position expiry.** Each interval the controller queries positions whose `expires_at` has
passed with `status = issued`, marks them expired, and advances `max_expired_position`.
DynamoDB TTL is enabled on `Positions` for post-event storage reclamation only, never as the
expiry mechanism ([ADR-0006](adr/0006-controller-driven-expiry-not-ttl.md)). Reads that could
observe a TTL-pending item apply a `FilterExpression` on `expires_at`.

---

## 8. API surface, caching, and security

### Public endpoints

| Path | Min TTL | Cache key | Cookies | Purpose |
|---|---|---|---|---|
| `/status` | 1 s | path only | none | Phase, serving position, admission rate, operator message; after T−0 also `shuffle_seed`, `participant_count`, `prequeue_offsets` |
| `/queue_num` | 1 s | path + `event_id`, `request_id` | none | Own position; 404 means re-join |
| `/queue_pos_expiry` | 1 s | path + `event_id`, `request_id` | none | Seconds until position lapses |
| `/public_key` | 1 s | path + `event_id` | none | Signature verification material |
| `/join` | uncached | — | none | Join the queue or pre-queue |
| `/generate_token` | uncached | — | none | Exchange a served position for an admission token |

### Cache behaviours

| Behaviour | Path pattern | Caching | Cookies | Origin |
|---|---|---|---|---|
| Polled | `/status`, `/queue_num`, `/queue_pos_expiry`, `/public_key` | Min TTL 1 s | none | API Gateway |
| Write | `/join`, `/generate_token` | disabled | none | API Gateway |
| Protected origin | `/*` (default) | disabled | session cookie forwarded | Client origin, via VPC origin |

Minimum TTL must exceed zero and polled behaviours must forward no cookies, or CloudFront
disables request collapsing and every poll reaches the origin
([ADR-0013](adr/0013-cache-behaviour-separation.md)). `stale-while-revalidate` on `/status`
serves the previous value if the origin is slow.

### Admin endpoints (SigV4)

| Path | Purpose |
|---|---|
| `/admin/phase` | Transition phase; force or clear maintenance mode |
| `/admin/rate` | Set target admission rate |
| `/admin/message` | Publish an operator message to waiting visitors |
| `/admin/reset` | Reset event state |
| `/admin/rules` | Update protection rules |
| `/metrics` | Event metrics as JSON |
| `/update_session` | Report session completion or abandonment |

Every operator action is here; the scheduled paths call the same Lambdas. No capability
requires a console.

### Credentials

Two artifacts, signed with the same key over different inputs so neither can be replayed as
the other ([ADR-0011](adr/0011-session-cookie-after-token.md)):

- **Admission token** — carries event id, queue id, and expiry. Travels on the URL,
  short-lived, validated once.
- **Session cookie** — set by the authorizer after the token validates, scoped per event.
  Supports a sliding window extended on activity and a hard cap from issue time.

The signing key is per-deployment, held in Secrets Manager. Its compromise permits minting
admission for every event in that deployment.

### Entry gating

The client signs an identifier it already holds — membership number, promo code, order
reference — and the waiting room verifies the signature at join time, storing nothing. This
changes the problem from detecting automation to verifying a prior relationship.

Where an operator can identify likely bots during the pre-queue, blocking can be deferred to
randomization rather than applied on arrival, so detection is not revealed while there is
still time to modify a client and rejoin.

### Web Application Firewall (WAF)

1. **Bot Control** — bot-versus-human discrimination. Safe in Block.
2. **Autonomous System Number (ASN) matching** — scalper infrastructure concentrates in a
   small number of hosting ASNs.
3. **Anti-distributed-denial-of-service (anti-DDoS) managed rule group** — Count mode by
   default ([ADR-0012](adr/0012-anti-ddos-count-mode.md)).

There is no public API key. A key on a page served to browsers ships in client-side
JavaScript; WAF rate-based rules do that job properly.

WAF bills $0.60 per million requests inspected on top of Bot Control's per-request fee,
against the same request count CloudFront serves. At 1M visitors polling every 10 s for 20
minutes it exceeds the CloudFront bill (§12).

---

## 9. Operator surface

| Capability | Mechanism |
|---|---|
| Live metrics | Lambdas emit logs in Embedded Metric Format (EMF); CloudWatch derives queue depth, admitted, no-show rate, expiry rate. Inflow comes from the `AWS/CloudFront` `Requests` metric. A dashboard ships with the module |
| Metrics for client tooling | `GET /metrics` on the admin API |
| Branding | Client HTML, CSS and assets in S3 behind CloudFront; the module ships a reference theme |
| Operator messaging | A `Counters` attribute delivered in the existing `/status` payload |
| Position and estimated wait | `/queue_num` returns position; the client computes wait from the measured admission rate in `/status` |
| Operator actions | Admin REST API with SigV4, backed by the same Lambdas as the scheduled paths |

Broadcasting a message to 1,000,000 waiting visitors costs one `UpdateItem` and zero
additional requests; delivery completes within one cache TTL. Estimated wait is computed from
the measured admission rate, so it tracks operator rate changes during an event.

---

## 10. Failure behaviour

If the waiting room API is unreachable, the authorizer admits the visitor with a time-limited
bypass cookie while the client retries in the background
([ADR-0009](adr/0009-fail-open.md)). Configurable to fail closed per client.

API Gateway's account throttle is a token bucket: tokens refill at the requests per second
(RPS) quota, the bucket holds at most 5,000. Steady-state capacity comes from the refill
rate; the bucket absorbs instantaneous arrivals above it and sheds 429s when empty. The burst
quota is not directly adjustable — AWS derives it from the RPS quota — so raising RPS is the
only lever.

This is a smoothing buffer, not a ceiling on event size. A 200,000-visitor burst against a
50,000 RPS quota drains and refills within seconds. The client must retry 429s with jittered
backoff; a client that fails closed on 429 turns a brief smoothing event into a visible
outage.

---

## 11. Deployment

Single-tenant, deployed into the client's own AWS account
([ADR-0007](adr/0007-single-tenant-deployment.md)). No component runs anywhere else.

**No virtual private cloud (VPC).** DynamoDB, SQS, Secrets Manager, EventBridge and Lambda
are all Identity and Access Management (IAM) authenticated public-endpoint services, reached
with no network address translation (NAT) gateway and no VPC endpoints. A VPC is available as
an opt-in variable for clients whose Authority to Operate (ATO) boundary mandates
private-subnet compute regardless of IAM; the Lambda code is identical either way.

**Origin protection.** CloudFront VPC origins place the client origin in a private subnet
with CloudFront as the sole ingress. VPC origins forbid Lambda@Edge origin triggers, require
an internet gateway present but unused, and do not evaluate inbound network ACLs.

**Event isolation.** Each event gets its own SQS queue and its own Lambda function with
reserved concurrency ([ADR-0008](adr/0008-partition-isolation-not-shuffle-sharding.md)).
Without reserved concurrency, functions draw from the shared account pool and a runaway event
starves the others.

### GovCloud variant

CloudFront, CloudFront Functions, Lambda@Edge, and CloudFront VPC origins are all unavailable
in GovCloud — the VPC origins supported-region list covers 34 commercial regions and neither
GovCloud region appears. AWS public-sector guidance places CloudFront in a commercial region
pointing at GovCloud origins, which raises a data-boundary question for the client's
Authorizing Official (AO).

Consequences: no edge gating, no managed origin protection, and no CDN request collapsing for
`/status` inside the boundary. Origin protection is built from primitives — an internal
Application Load Balancer (ALB), the token authorizer, security groups and IAM. This is a
different topology, not a configuration flag, and is priced separately.

---

## 12. Cost

For a 1,000,000-visitor event at 10-second polling:

| Component | Driver | Approximate |
|---|---|---|
| CloudFront requests | poll interval × visitors × wait | $92 |
| WAF + Bot Control | same request volume | $87 + $123 (Common) or $1,230 (Targeted) |
| DynamoDB pre-warming | target write rate | billed per event |
| API Gateway | cache misses only, ~3/visitor | $10 |
| SQS, Lambda, DynamoDB writes | joins | negligible |

The client poll interval is the dominant variable, multiplying CloudFront, WAF and Bot
Control charges together.

Two choices are computed per client rather than assumed: CloudFront flat-rate versus
pay-as-you-go (PAYG) pricing, and Bot Control Common versus Targeted. Both are recorded as
open in [`adr/README.md`](adr/README.md).

---

## 13. Requirement coverage

Which section implements which requirement from [`REQUIREMENTS.md`](./REQUIREMENTS.md).

| Requirement | Section |
|---|---|
| F0.1, F0.2, F0.8 — lifecycle phases, phase pages, maintenance mode | §3 |
| F0.3, F0.5 — scheduled mode, coexistence | §3, §4 |
| F0.4, F0.7 — standby activation and override | §3 |
| F0.6 — protection rules | §3 |
| F1.1, F1.2 — countdown page, no origin load | §4.1 |
| F1.3, F1.4 — randomized assignment, prompt completion | §4.2, §4.3 |
| F1.5 — auditability | §4.4 |
| F2.1 — live joins ordered behind pre-queue | §4.5 |
| F2.2, F2.3 — unique positions, gaps permitted | §5.1, §5.2, §5.4 |
| F2.4, F2.5, F2.6 — client identifier, idempotent retry, malformed joins | §6 |
| F3.1 — read position and serving position | §8 |
| F3.2, F3.8 — admission rate control, no-show compensation | §7 |
| F3.3, F3.4, F3.6, F3.7 — token, origin rejection, distinct signing, session lifetime | §8 |
| F3.5 — session established after token validation | §2.3, §8 |
| F3.9 — position expiry | §7 |
| F3.10 — session completion and abandonment | §8 |
| F4.1, F4.2, F4.3 — fail open, bounded bypass, configurable | §10 |
| F4.4 — recovery from a lost join | §6 |
| F4.5 — client retries 429 with jitter | §10 |
| F5.1, F5.2, F5.3, F5.4, F5.5 — metrics, branding, messaging, wait estimate, API-first | §9 |
| F6.1, F6.2 — entry gating on a client-signed identifier | §8 |
| F6.3 — deferred bot enforcement | §8 |
| C1, C2 — pre-queue scale, atomic assignment | §4.2 |
| C3 — live-join throughput | §5.3 |
| C4 — polling load independent of visitor count | §8 |
| C5 — burst absorbed without dropping joins | §2.2, §6 |
| N1, N2, N3 — idle cost, client account, no shared infrastructure | §11 |
| N4 — commercial and GovCloud | §11 |
| N5, N6 — Terraform, deployment size | §11 |
| N7 — edge bot mitigation | §8 |
| N8 — OpenAPI specification | §8 |
| N9 — event isolation | §11 |
| O1 — pre-warming | §5.3 |
| O2, O3, O6 — quota increases, load test, cost model | §12 |
| O4 — mid-event operator control | §9 |
| O5 — Count-then-Block promotion | §8 |

---

## 14. Constraints and sources

| Claim | Source |
|---|---|
| DynamoDB per-table on-demand quota 40,000 read/write request units, adjustable, "not maximum limits" | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| No account-level throughput quota in on-demand mode | same |
| DynamoDB tables per region: 2,500 default, 10,000 on request | same |
| DynamoDB writes billed in 1 KB units rounded up, 1 WCU minimum | same |
| New tables 4,000 writes/s, 12,000 reads/s; growth to 2× previous peak | [On-demand capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) |
| Single-partition 1,000 WCU/s | [Partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html) |
| Warm throughput and pre-warming | [Pre-warming DynamoDB tables](https://aws.amazon.com/blogs/database/pre-warming-amazon-dynamodb-tables-with-warm-throughput/) |
| `UpdateItem` `ADD` atomic counter; serialized per item; each value returned once | [UpdateItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html), [Implement auto-increment with DynamoDB](https://aws.amazon.com/blogs/database/implement-auto-increment-with-amazon-dynamodb/) |
| `BatchWriteItem`: 25 items, 16 MB, no conditional expressions, `UnprocessedItems` | [BatchWriteItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) |
| `Scan`: 1 MB page limit, `LastEvaluatedKey` pagination, parallel segments | [Scanning tables in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html) |
| TTL deletes "within a few days"; expired items remain readable; use filter expressions | [Using time to live in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) |
| API Gateway 10,000 RPS account throttle; 5,000 burst not customer-adjustable | [API Gateway quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html) |
| REST API integration timeout 29 s hard; request payload 10 MB | [REST API quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-execution-service-limits-table.html) |
| Request validators are REST-only | [Request validation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-method-request-validation.html) |
| HTTP API response mapping limited to headers and status code | [HTTP API parameter mapping](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-parameter-mapping.html) |
| SQS standard "nearly unlimited API calls per second, per action"; at-least-once; best-effort ordering | [SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html), [Using Lambda with SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) |
| ESM `BatchSize` to 10,000; window ≥1 s above 10; +300/min to 1,250 pollers | [SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
| Provisioned Mode: 2–200/2–2000 pollers, 1,000 concurrent/min, 20,000 max | [Provisioned Mode for SQS ESM](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-lambda-provisioned-mode-sqs-esm) |
| Lambda timeout 900 s; sync invocation payload 6 MB; memory 10,240 MB | [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html) |
| CloudFront default metrics: 1-minute granularity, `us-east-1`, no additional charge | [Monitor CloudFront metrics](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/monitoring-using-cloudwatch.html) |
| Request collapsing disabled by Min TTL 0 or cookie forwarding | [DDoS resilience with HTTP caching](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront) |
| VPC origins: supported regions exclude GovCloud; no Lambda@Edge origin triggers; inbound NACLs not evaluated | [Restrict access with VPC origins](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-vpc-origins.html) |
| CloudFront unavailable in GovCloud | [CloudFront with GovCloud resources](https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/setting-up-cloudfront.html) |
| Anti-DDoS rule group; baselines during attack take 2–3× longer | [Anti-DDoS managed rule group](https://docs.aws.amazon.com/waf/latest/developerguide/waf-anti-ddos-rg-using.html) |
| WAF pricing: $5 access control list (ACL), $1 rule, $0.60/M; Bot Control $10/mo + $1/M or $10/M | [AWS WAF pricing](https://aws.amazon.com/waf/pricing) |
| CloudFront flat-rate tiers and allowances | [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| Small-domain FPE: Feistel construction, cycle-walking, domain size | [NIST SP 800-38G Rev. 1](https://csrc.nist.gov/pubs/sp/800/38/g/r1/2pd) |
| Pre-queue randomization; FIFO for safety-net; redirect-and-token model; Direct Pass fail-open | [How Queue-it Works](https://www.queue-it.com/developers/how-queue-it-works), [Queue-it virtual waiting room](https://queue-it.com/virtual-waiting-room) |
| Distributed FIFO, open-window outflow control, no-show compensation, DynamoDB backbone | [Smooth Scaling ep. 17](https://queue-it.com/smooth-scaling-podcast/ep017-virtual-waiting-room-architecture/) |
| Two-credential model; sliding vs fixed session validity; local validation | [Queue-it's architecture](https://blog.crawlex.net/blog/queue-it-architecture/) |
| Queue Token software development kit (SDK); Hype Event Protection | [Queue-it Connectors](https://queue-it.com/developers/connectors/), [bad bot protection](https://www.queue-it.com/bad-bot-protection) |
| Shuffle sharding requires an operator-assigned node pool | [Workload isolation using shuffle-sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding) |
| FedRAMP cost and timeline | Published third-party assessment organization and FedRAMP advisory pricing, cross-checked |
