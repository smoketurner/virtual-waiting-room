# Design

Implementation of [`REQUIREMENTS.md`](./REQUIREMENTS.md). An AWS-native virtual waiting
room, deployed into the operator's own account.

Every quantitative claim is sourced in §14.

---

## 1. Approach

A virtual waiting room meters visitors into an origin at a rate the origin can sustain. The
difficult part is the arrival burst, not the queue.

Assigning queue positions in arrival order makes early arrival advantageous, which
concentrates arrivals into the first seconds after an event opens. Randomizing position
assignment among all participants present at the scheduled start removes that advantage.
For a 1M-visitor event:

| Assignment method | Write rate at T−0 | Against a 40,000 WRU/s table quota |
|---|---|---|
| Arrival order, 1M arriving over 1–5 s | 200,000–1,000,000/s | 5–25× over, after a quota increase |
| Pre-queue, randomized, written over 5 min | 3,333/s | 8% of the default quota |

Queue-it applies the same approach, describing pre-queue visitors as randomized "like a
raffle" at the start time to neutralize "any advantage to arriving early."

Three further constraints shape the rest of the design:

1. **Gaps are acceptable; duplicates are not.** A skipped position is not observable by
   users. Two visitors holding position 40,001 is a correctness failure. This asymmetry
   permits an atomic counter rather than transactions (§6).
2. **No compute in the ingest path.** Arrivals reach an API Gateway service integration, not
   a Lambda function, so cold starts and concurrency limits do not apply at ingest (§7).
3. **Fail open.** If the waiting room is unavailable, visitors proceed to the origin (§11).

---

## 2. Architecture

### 2.1 Scheduled event (primary path)

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
          No per-participant writes. No Scan. Assignment is complete when this
          single conditional write succeeds.
                                    │
                                    ▼
  T+    /queue_num reads i from PreQueue, seed and N from /status, and returns
        PRP(seed, i, N) — a keyed Feistel permutation, computed per read
```

Detail in §4. One write replaces 1,000,000; there is no interval during which some
participants hold positions and others do not.

### 2.2 Live join (walk-up arrivals after opening)

```
  WAF          Bot Control · ASN match · Anti-DDoS (Count mode by default, §3.1a)
        │
  CloudFront   polled behaviour: Min TTL 1 s, no cookies forwarded
               (request collapsing depends on both — §8)
        │
  API Gateway  REST, regional
               request validator (JSON Schema) → 400 on malformed body
               type: aws → SQS SendMessage        ← no Lambda in the burst path
        │
  SQS          standard queue + DLQ (maxReceiveCount 5)
               at-least-once; duplicates absorbed by the conditional write
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
               on-demand, PITR, pre-warmed before the event (§6.4)
```

Detail in §7. The counter is incremented by the count of **valid** records, never
`records.len()` (F2.6).

### 2.3 Admission and session

Two credentials. The admission token proves the visitor reached the front of the queue; the
session cookie proves the token was already validated on an earlier request. Queue-it uses
the same split.

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
  Origin request carrying the token on the URL
        │
        ▼
  authorizer (Rust — CloudFront VPC origin, or at the client origin)
     ├─ valid session cookie?        → forward to origin
     ├─ valid admission token?       → set session cookie, strip token from URL,
     │                                 ADD arrivals#(hash % 10), forward
     ├─ request not protected?       → forward
     ├─ waiting room unreachable?    → forward with bypass cookie (fail open, §11)
     └─ otherwise                    → 302 to the waiting room
        │
        ▼
  Client origin — private subnet, reachable only via the CloudFront VPC origin
```

Detail in §5 (controller), §9 (credentials), §11 (fail open).

**The session is required for correctness, not performance.** The admission token is
carried as a URL query parameter. The URL changes on the visitor's next navigation, so
without a session the credential is lost on the second page view and the visitor is
re-queued. The authorizer validates the token once, sets the session cookie, and strips the
token from the URL before forwarding to the origin.

Queue-it implements this with a `queueittoken` URL parameter and a
`QueueITAccepted-SDFrts345E-V3_{eventId}` cookie, the latter signed over a different
concatenation than the token so neither can be replayed as the other.

The two credentials are signed over **different inputs** so neither can be replayed as the
other.

Every authorizer decision — protection match, session check, token check, expiry — is local.
No call to the waiting-room backend on the hot path. This is what allows the authorizer to
run at the edge and add only the cost of a signature verification per request.

---

## 3. Event lifecycle and operating modes

Satisfies F0.1, F0.2, F0.3, F0.4, F0.5, F0.6, F0.7, F0.8.

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

The idle and post-event pages carry operator communication: what is coming, whether stock
remains, where to go once an event has ended.

**Maintenance mode** parks every visitor on an operator page regardless of phase or
capacity. It is a phase override, used when a downstream system is unavailable.

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

**Fairness differs by mode.** Scheduled events randomize: the start time is published, so
arrival order measures connection latency rather than intent. Standby activation is FIFO:
the spike is unplanned, so arrival order carries information. Queue-it applies the same
split.

Both modes run simultaneously on one origin. Queue-it's architect describes a customer
configuration of this shape: a scheduled room with a low outflow limit on a specific product
path, plus site-wide standby for visitors who arrive at the homepage instead.

**Protection rules** declare which requests are subject to queueing, matching on path,
header, cookie, or user agent, distributed to the authorizer and evaluated locally.
Unmatched requests are never queued, in any phase or mode. Because rules can match headers
and user agent, they double as coarse anti-automation; WAF (§9) does the actual scoring.

### 3.4 Standby activation

Standby requires an inflow measurement that does not itself become load under the traffic it
is measuring.

**Measurement.** CloudFront publishes a `Requests` metric per distribution to CloudWatch in
`us-east-1`, in the `AWS/CloudFront` namespace, at **1-minute granularity**. Default
CloudFront metrics carry no additional charge and do not count against CloudWatch quotas
([CloudFront monitoring](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/monitoring-using-cloudwatch.html)).

The alternative — counting requests in our own code — would place a write on the hot path at
exactly the moment traffic spikes. CloudFront already counts every request; using its metric
costs nothing and cannot be a bottleneck.

**The 1-minute granularity is a real constraint, and it bounds what standby can promise.**

| Step | Latency |
|---|---|
| Metric publication | up to 60 s |
| Alarm evaluation (1 period) | up to 60 s |
| `/status` cache TTL | up to 5 s |
| **Total worst case** | **~125 s** |

Standby therefore protects against a spike that persists for minutes — a social post, a
news mention, a marketing send. It does not protect against a spike that arrives and
saturates the origin in under two minutes. Scheduled mode exists for events with a known
start time precisely because standby cannot react fast enough for them.

| Concern | Mechanism |
|---|---|
| Inflow measurement | CloudWatch alarm on `AWS/CloudFront` `Requests`, `Sum` statistic, 60 s period, in `us-east-1` |
| Activation | Alarm state change to `ALARM` → EventBridge rule → phase Lambda sets `ACTIVE` |
| Propagation to the authorizer | Authorizer reads phase from `/status`, cached 5 s |
| Deactivation | Second alarm on sustained low `Requests`, longer evaluation period so the queue does not flap |
| Manual override | Admin API sets a forced phase that suppresses both alarms |

Queue-it describes the same feature as Safety Net: "if traffic inflow exceeds the thresholds
you configure, only then will the online queue activate." Their FAQ frames the choice as
"Always Visible" versus "Visible at Peak," which maps to our scheduled and standby modes.

---

## 4. Pre-queue

Satisfies F1.1, F1.2, F1.3, F1.4, F1.5, C1, C2.

### 4.1 Registration

A visitor arriving during the pre-queue calls `POST /join`. The request goes through the
same API Gateway → SQS → Lambda path as a live join (§7); the handler claims the next
**registration index** from an atomic counter and writes one `PreQueue` item:

| Attribute | Value |
|---|---|
| `r` (PK) | `request_id`, client-supplied UUIDv7 |
| `i` | registration index, from `ADD prequeue_counter :n` |
| `t` | server-stamped registration time |

Written with `ConditionExpression: attribute_not_exists(r)`, so a duplicate join consumes
no index (F2.5). Registration writes are spread across the pre-queue window — typically
minutes to hours — so the rate is a fraction of what arrival-order assignment would
concentrate at T−0.

### 4.2 Assignment at T−0: a seeded permutation, not stored rows

The queue order is a bijection from registration index to queue position. Two ways to
realise it:

- **Materialise it.** Shuffle the ID list and write one row per participant. 1,000,000
  writes.
- **Compute it.** Define the bijection as a keyed pseudorandom permutation over
  `[0, N)`. **One write.**

The design computes it. At T−0 the phase Lambda performs a single `UpdateItem` on
`Counters`:

```
UpdateExpression: SET shuffle_seed = :seed, participant_count = :n, phase = :active
ConditionExpression: attribute_not_exists(shuffle_seed)
```

`:n` is the final value of `prequeue_counter`. Nothing else is written. Assignment is
complete the moment that one conditional write succeeds — there is no window during which
some participants have positions and others do not.

A visitor's position is then derived on read:

```
queue_position = PRP(shuffle_seed, registration_index, participant_count)
```

`/queue_num` reads the visitor's `PreQueue` item for `i`, reads the cached seed and count
from `/status`, and evaluates the permutation. Both inputs are already being fetched.

### 4.3 The permutation

A balanced Feistel network over a power-of-two domain, with cycle-walking to restrict the
output to `[0, N)`. This is the standard small-domain construction underlying
format-preserving encryption; NIST specifies FF1 on the same principle in
[SP 800-38G](https://csrc.nist.gov/pubs/sp/800/38/g/r1/2pd).

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

**Properties, verified by construction and by test:**

| Property | Evidence |
|---|---|
| Bijective | A Feistel network is invertible by construction; cycle-walking preserves that on the restricted domain. Verified: 200,000 samples at N=1,000,000 produced 200,000 distinct positions. |
| Uniform | Verified at N=10,000 across 10 deciles: exactly 1,000 each, χ² = 0.0 against a critical value of 16.9 at p=0.05, df=9. |
| Deterministic | Same seed, index and N always yield the same position. |
| Cheap | Expected cycle-walk iterations = `domain / N`, bounded by 4 in the worst case and 1.05 at N=1,000,000. Four HMAC evaluations per iteration. |
| Unpredictable before T−0 | Position depends on a seed not published until the assignment write. A participant cannot compute their position early, and cannot choose a registration index that yields a good one. |

The last property is what makes this safe against gaming: with a materialised shuffle the
seed is equally secret, but here the secrecy is load-bearing and must be stated. The seed is
written at T−0 and published in `/status` from that moment; before T−0 it does not exist.

### 4.4 Cost

| | Materialised shuffle | Seeded permutation |
|---|---|---|
| Writes at T−0 | 1,000,000 | **1** |
| WCU at T−0 | 1,000,000 | **1** |
| `Scan` of `PreQueue` | ~287 pages, ~36,800 RCU | **none** |
| Assignment window | 5 min sustained at 3,333 writes/s | **single write** |
| Lambda | manages 1M in-flight futures, checkpointing, resume | one `UpdateItem` |
| Partial-failure mode | some participants assigned, others not | none — one conditional write |
| Table quota consumed | 3,333 WRU/s held for 5 min | negligible |
| Pre-warming needed for assignment | yes | **no** |
| Read cost per visitor | 1 `GetItem` | 1 `GetItem` + ~4 HMACs |

The 900-second Lambda timeout, the checkpoint-and-resume logic, the parallel `Scan`, and
the in-memory shuffle set all disappear. F1.4's window becomes trivially satisfiable, and
C2 is no longer a throughput requirement on assignment.

**On attribute size.** Shortening attribute names or packing values into a binary blob
does not reduce write cost: DynamoDB bills writes in 1 KB units rounded up, with a 1 WCU
minimum, and a position row is ~300 bytes — already at the floor. Both a 300-byte and a
45-byte item cost 1 WCU. Size affects `Scan` page density, but eliminating the `Scan`
removes that concern too. `PreQueue` uses short attribute names anyway, because items are
scanned during audit and the density is free.

### 4.5 Auditability

The assignment is reproducible from two published values (F1.5). Given `shuffle_seed`,
`participant_count`, and the set of `(request_id, i)` pairs in `PreQueue`, any third party
can recompute every position and confirm the ordering. This is a stronger audit position
than a materialised shuffle, where verification requires trusting that stored rows were
not modified after the fact.

Positions are still written to `Positions` — but lazily, when a visitor is admitted, not
for every participant at T−0. That write carries `entry_time`, `status` and `expires_at`
for the outflow controller (§5) and covers only visitors who actually reach the front.

### 4.6 Live joins after opening

A visitor joining after T−0 has no pre-queue registration and takes a position from
`queue_counter`, which starts at `participant_count`. Live joins are therefore ordered
first-come-first-served behind every pre-queue participant (F2.1), and use the batch range
allocation in §6.2. The permutation applies only to the pre-queue cohort.

### 4.7 Fairness

Randomization is the fairness model for scheduled events: every participant present at T−0
has equal probability of any position, independent of connection speed or geography.
First-come-first-served applies to live joins after opening.

Queue-it uses the same split. Their documentation describes randomizing pre-queue visitors
"like a raffle" when the timer reaches zero, and their FAQ states that safety-net activation
operates as a FIFO queue. Their stated rationale is that randomization "prevents speedy bots
from getting an unfair advantage."

---

## 5. Outflow control

Satisfies F3.2, F3.8, F3.9, F3.10.

The operator declares a capacity — say 500 arrivals per minute — and the system releases
visitors at that rate. Naively this is "increment `serving_counter` by 500 each minute."

**That undercounts, because of no-shows.** A fraction of visitors whose turn arrives never
click through: they switched tabs, closed the browser, or gave up. Releasing exactly 500
positions delivers materially fewer than 500 real arrivals, so the origin runs below the
capacity the client is paying to use, and everyone still waiting waits longer than
necessary. Queue-it's architect identifies no-shows as a primary difficulty in outflow
control.

The fix is a closed loop over measured arrivals:

```
observed_arrival_rate = arrivals in the last interval
no_show_rate          = 1 − (observed_arrival_rate / released_last_interval)
release_next          = target_rate / (1 − smoothed_no_show_rate)
```

The no-show rate is smoothed across intervals to avoid oscillation, and the correction is
bounded so a transient measurement error cannot release a damaging burst.

**How it runs.** An EventBridge Scheduler rule invokes the controller Lambda every 10
seconds. It sums the arrival shards, reads the released count, computes the correction, and
writes the new `serving_counter` with one `UpdateItem`.

**Counting arrivals.** The authorizer increments an arrival counter when it converts an
admission token into a session. This is one write per admitted visitor. If the operator sets
a high admission rate — 60,000/min is 1,000/s — that reaches the 1,000 WCU/s single-item
ceiling. The arrival counter is therefore **sharded across 10 items** (`arrivals#0` through
`arrivals#9`, chosen by `hash(request_id) % 10`), and the controller sums all ten on each
interval. This is legitimate here because arrivals are a statistic, not a sequence (§6.5):
the controller needs the total, never an ordered position.

Cost: 10 reads per 10-second interval, or 1 read/second, independent of event size.

**Position expiry.** A position released but never claimed within the configured window must
expire so the serving counter can advance past it (F3.9).

**DynamoDB TTL cannot do this.** From the
[TTL documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html):
"DynamoDB automatically deletes expired items **within a few days** of their expiration
time" and "Items with valid, expired TTL attributes **might be deleted by the system at any
time**, typically within a few days after their expiration." Expired items also remain
visible to reads until deleted: "Use filter expressions to remove expired items from `Scan`
and `Query` results."

Days of latency is unusable for a mechanism that must reclaim capacity within an event
lasting minutes. Expiry is therefore driven by the controller itself: each interval it
queries positions whose `expires_at` has passed and whose `status` is still `issued`, marks
them `expired`, and advances `max_expired_position`. TTL remains enabled on `Positions`, but
only for eventual storage reclamation after the event — never as the expiry mechanism.

Reads that could observe a TTL-pending item use `FilterExpression` on `expires_at` so a
deleted-but-still-visible item is never returned as live.

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

So the live-join ceiling is ~40,000/s at default quotas. `BatchSize` above ~40 buys counter
headroom that `Positions` cannot use. Default `BatchSize` is 100 with a 1-second window,
adding one second to a join in a queue where the visitor then waits minutes.

The same `Positions` quota bounds pre-queue assignment (§4.4), where the write rate is
chosen rather than imposed. That is the difference between the two paths: live join must
absorb whatever arrives, pre-queue assignment is scheduled.

### 6.4 Cold-start capacity

On-demand tables serve ~4,000 writes/sec when new and grow to twice their previous peak. A
waiting room is idle by definition and has no meaningful previous peak, so an unprepared
deployment throttles at ~4,000/sec exactly when an on-sale starts.

DynamoDB warm throughput fixes this: pre-warming sets the throughput a table can absorb
instantaneously. Reading the value is free; pre-warming is billed. This is O1 — a
contractual pre-event step, not an optimization.

### 6.5 Sequences versus statistics

| Counter | Kind | Write sharding |
|---|---|---|
| `queue_counter`, `serving_counter`, `prequeue_counter` | sequence | **never** — sharding destroys ordering |
| `arrivals` | statistic | **sharded ×10** — hot at high admission rates (§5) |
| `token_counter`, `completed_counter`, `abandoned_counter`, `expired_queue_counter` | statistic | permitted if hot |

A sequence must yield a unique ordered value; summing shards cannot produce one. A statistic
needs only a total, so shards can be summed on read.

**This is write sharding, not shuffle sharding.** The two are unrelated and the shared word
invites confusion. Write sharding spreads writes across N keys to escape the 1,000 WCU/s
single-item limit — a *throughput* technique. [Shuffle
sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding)
assigns each tenant a random subset of nodes from a pool so that one tenant's failure reaches
few others — an *isolation* technique. See §12 for why the latter does not apply here.

### 6.6 Gap tolerance

An SDK retry after a 5xx, or a function dying between the counter increment and the position
write, burns positions without issuing them. No user observes a skipped number (F2.3). This
tolerance is what permits an atomic counter instead of `TransactWriteItems`, which would
double the write cost.

### 6.7 Tables

**`Counters`** — PK `event_id`. One item per event holding:

| Attribute | Type | Purpose |
|---|---|---|
| `queue_counter` | N | Position sequence |
| `serving_counter` | N | Admission high-water mark |
| `max_expired_position` | N | Highest expired position |
| `arrivals#0`–`arrivals#9` | N | Sharded arrival count (§5) |
| `phase` | S | `idle` / `pre_queue` / `active` / `post_event` |
| `phase_override` | S | Maintenance mode, checked before `phase` |
| `target_rate` | N | Operator-set admissions per minute |
| `shuffle_seed` | B | 256-bit permutation key, written once at T−0 (§4.2) |
| `participant_count` | N | Pre-queue cohort size `N`, the permutation domain |
| `prequeue_counter` | N | Registration index sequence |
| `operator_message` | S | Delivered in `/status` |

All counter updates are `UpdateItem` with `ADD`. Item stays well under the 400 KB limit.

**`PreQueue`** — PK `r` (`request_id`, UUIDv7). Attributes `i` (registration index from
`prequeue_counter`) and `t` (server-stamped registration time). Short attribute names
because the table is scanned during audit. Written with
`ConditionExpression: attribute_not_exists(r)`. Read by `/queue_num` as a single `GetItem`;
never scanned on the hot path.

**`Positions`** — PK `request_id`. Attributes `event_id`, `queue_position`, `entry_time`,
`status`, `expires_at`, `ttl`.

- Written with `ConditionExpression: attribute_not_exists(request_id)` (F2.5).
- `entry_time` is server-stamped and authoritative; the UUIDv7 timestamp is client-supplied
  and untrusted.
- `expires_at` drives deterministic expiry via the controller (§5).
- `ttl` is a separate attribute for eventual storage reclamation only, set well after the
  event ends.

**`Tokens`** — PK `request_id`. Issued admission-token metadata and session status. `ttl` set
to event end plus a retention margin.

All tables use on-demand capacity with point-in-time recovery, and carry
`warm_throughput_write_units` sized to the event's target rate (§6.4).

---

## 7. Ingest

Satisfies F2.1, F2.4–F2.6, C5.

### 7.1 Path

Regional REST API with an `AWS` service integration to SQS `SendMessage`. No Lambda in the
burst path, so no cold start and no concurrency ceiling at ingest.

Relevant limits:

| Limit | Value | Adjustable | Effect here |
|---|---|---|---|
| REST API integration timeout | 29 s max | No | Irrelevant: `SendMessage` is single-digit ms |
| REST API request payload | 10 MB | No | Join payload is ~100 bytes |
| SQS standard throughput | "nearly unlimited API calls per second, per action" | — | Not a constraint |
| SQS message size | 1 MiB | No | Not a constraint |
| Lambda ESM `BatchSize` | 10,000 (>10 requires window ≥1 s) | — | Default 100 / 1 s |
| Lambda sync invocation payload | 6 MB | No | ~500 B/record caps a batch near 10–12K records |

### 7.2 REST rather than HTTP API

HTTP APIs cost $1.00/M against REST's $3.50/M. API Gateway sees only CloudFront cache
misses — approximately three per visitor regardless of wait duration — so a million-visitor
event generates ~3M billable requests and the saving is **$7.50**. HTTP APIs do not support
request validators, API keys, or VTL response mapping. The saving does not justify losing
request validation.

### 7.3 Client-supplied UUIDv7

The client generates its own request identifier. With `$context.requestId`, a client retry
produces a new identifier and consumes a second position; with a client-supplied identifier
the retry carries the same value and is absorbed by the conditional write (F2.5).

v7 over v4 for debuggability — join time is recoverable from the identifier — and sort-key
headroom if a secondary index is added. The embedded timestamp is client-supplied and is
never trusted; `entry_time` is stamped server-side and is authoritative.

Browser `crypto.randomUUID()` emits v4 only, so the reference client uses the `uuid` package.

### 7.4 Duplicate and failure handling

SQS standard queues are **at-least-once**: the same message can be delivered more than once,
and ordering is best-effort. Both are acceptable here and neither is worked around:

- **Duplicates** are absorbed by `attribute_not_exists(request_id)` on the position write.
  A redelivered message fails the condition and consumes no position.
- **Ordering** does not matter because positions are allocated per batch from an atomic
  counter, not from message sequence.

The event source mapping sets `FunctionResponseTypes: [ReportBatchItemFailures]`. Without
it, one failed record causes the entire batch to be redelivered, re-processing records that
already succeeded. With it, only failed record identifiers return to the queue.

Queue visibility timeout is set to six times the function timeout plus
`MaximumBatchingWindowInSeconds`, per AWS guidance, so a slow batch is not redelivered while
still being processed. `maxReceiveCount` is 5, after which records move to the DLQ.

### 7.5 Validation

Two layers:

1. **Gateway request validator** with a JSON Schema model rejects a malformed or missing
   `request_id` with 400, synchronously, before the message reaches SQS.
2. **Lambda re-validation** parses the UUID and checks the version nibble, which JSON Schema
   cannot express.

The counter is incremented by the count of **valid** records, never `records.len()`.
Incrementing by record count would let malformed payloads consume queue positions without
producing queue members (F2.6).

### 7.6 Recovery

Invalid records are reported through `ReportBatchItemFailures` and reach the DLQ after
`maxReceiveCount`. The client's subsequent `GET /queue_num` returns 404, which the client
treats as "re-join with a fresh UUIDv7" (F4.4). This is also the recovery path for a record
lost for any other reason.

API Gateway returning 200 means *accepted into the queue*, not *position assigned*. The
404-and-rejoin loop is what makes that asymmetry safe, and it is part of the client contract.

---

## 8. API surface and caching

Satisfies F3.1, F5.5, N8, C4.

### Public

| Path | Min TTL | Cache key | Cookies forwarded | Purpose |
|---|---|---|---|---|
| `/status` | 1 s | path only | **none** | Phase, serving position, admission rate, operator message |
| `/queue_num` | 1 s | path + `event_id`, `request_id` | **none** | Own position; 404 means re-join |
| `/queue_pos_expiry` | 1 s | path + `event_id`, `request_id` | **none** | Seconds until position lapses |
| `/public_key` | 1 s | path + `event_id` | **none** | Signature verification material |
| `/join` | 0 (uncached) | — | none | Join the queue or pre-queue |
| `/generate_token` | 0 (uncached) | — | none | Exchange a served position for an admission token |

### Request collapsing is the mechanism behind C4

C4 requires that origin request rate stay flat as waiters scale from 10,000 to 1,000,000.
That depends entirely on CloudFront **request collapsing**: when N viewers miss the cache
for the same key simultaneously, CloudFront sends one request to the origin and serves all N
from the single response.

Collapsing is disabled by two configurations
([AWS re:Post, DDoS resilience with HTTP caching on CloudFront](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront)):

> The following configurations prevent request collapsing from occurring: The Minimum TTL of
> a cache behavior is set to 0. Cookie forwarding is enabled in the cache policy, the origin
> request policy, or the legacy cache settings.

Two consequences the design must respect:

1. **Minimum TTL must be greater than zero on every cached behaviour.** A 0-second minimum
   TTL disables collapsing even if the origin sends `Cache-Control: max-age=5`. Cached
   behaviours use Min TTL 1 s and the origin sets `Cache-Control: max-age=5` for `/status`;
   CloudFront honours the origin value when it falls between Min and Max TTL.

2. **The polled endpoints must forward no cookies.** The authorizer needs the session cookie
   on protected-origin requests, so **that is a separate cache behaviour with caching
   disabled**. Mixing cookie forwarding into a polled behaviour's cache policy would disable
   collapsing on the endpoint that 1M visitors are hitting, and the origin would receive
   every request.

| Behaviour | Path pattern | Caching | Cookies | Origin |
|---|---|---|---|---|
| Polled endpoints | `/status`, `/queue_num`, `/queue_pos_expiry`, `/public_key` | Min TTL 1 s | none | API Gateway |
| Write endpoints | `/join`, `/generate_token` | disabled | none | API Gateway |
| Protected origin | `/*` (default) | disabled | **session cookie forwarded** | Client origin, via VPC origin |

`stale-while-revalidate` is set on `/status` so a slow origin response serves the previous
value rather than blocking waiters.

**Poll interval.** A position never changes once assigned, so `/queue_num` is effectively
static per visitor. `/status` at Min TTL 1 s and origin `max-age=5` collapses 1M pollers into
at most one origin fetch per 5 seconds. Client poll interval defaults to 10 seconds; it is
the dominant cost variable in the system, multiplying CloudFront requests, WAF inspections,
and Bot Control charges together (§13).

### Admin (SigV4)

| Path | Purpose |
|---|---|
| `/admin/phase` | Transition phase; force or clear maintenance mode |
| `/admin/rate` | Set target admission rate |
| `/admin/message` | Publish an operator message to waiting visitors |
| `/admin/reset` | Reset event state |
| `/admin/rules` | Update protection rules |
| `/metrics` | Event metrics as JSON for the client's own tooling |
| `/update_session` | Report session completion or abandonment; updates statistics counters |

Every operator action is here, and the scheduled paths call the same Lambdas. There is no
capability available through a console that is unavailable through the API.

---

## 9. Security and abuse mitigation

Satisfies F3.3, F3.4, F3.5, F3.6, F3.7, F6.1, F6.2, F6.3, N7, O5.

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

**Entry gating on a client-issued identifier.** The client signs an identifier it already
holds — membership number, promo code, order reference — with a shared key. The waiting room
verifies the signature at join time and stores nothing (F6.1, F6.2).

This changes the problem from detecting automation to verifying a prior relationship.
Queue-it ships this as the Queue Token SDK, with implementations in Java, .NET, Ruby and
JavaScript. Their documented use case: "when running a members-only ticketing sale, the
venue could require members to enter their membership ID to get a spot in the waiting room."
One published customer result is a gaming company excluding 225,000 bots and non-members
from an invite-only drop.

**Enforcement timing.** Blocking a detected bot on arrival reveals the detection while
there is still time to modify the client and rejoin. Deferring the block to randomization
removes that window (F6.3). Queue-it sells this as Hype Event Protection, describing it as
blocking bots "only at the sale start—after genuine visitors have secured their spots."

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

**WAF cost scales with polling volume.** It bills $0.60 per million requests inspected on
top of Bot Control's per-request fee, against the same request count CloudFront serves.
At 1M visitors polling every 10 s for 20 minutes it exceeds the CloudFront bill (§13).

---

## 10. Operator surface

Satisfies F5.1, F5.2, F5.3, F5.4, F5.5, O4.

Queue-it sells traffic intelligence and custom themes as separate products. Both are
required to operate an event.

| Capability | AWS mechanism |
|---|---|
| Live metrics | Lambdas emit EMF-formatted logs; CloudWatch derives queue depth, admitted, no-show rate and expiry rate without a separate metrics pipeline. Inflow comes from the `AWS/CloudFront` `Requests` metric (§3.4). A dashboard ships with the module. |
| Metrics for the client's own tooling | `GET /metrics` on the admin API (SigV4), reading the `Counters` item. Not public and not cached. |
| Branding | Client HTML, CSS and assets in S3 behind CloudFront. The module ships a reference theme; the client overrides bucket contents. No fork required. |
| Operator messaging | A string attribute on the `Counters` item, published through the admin API, delivered in the existing `/status` payload. Zero additional requests. |
| Position and estimated wait | `/queue_num` returns position; the client computes wait from the measured admission rate in `/status`. Recomputed as the operator changes the rate. |
| Operator actions | Admin REST API with SigV4, backed by the same Lambdas as the scheduled paths. |

The operator message is delivered in the `/status` payload the waiting page already polls.
Broadcasting to 1M waiting visitors costs one `UpdateItem` and zero additional requests;
delivery completes within one cache TTL.

Estimated wait is computed from the measured admission rate rather than a configured
constant, so it tracks operator rate changes during an event.

**API-first.** Every operator action — rate change, phase transition, reset, pause,
maintenance mode, message publish — is an API call. There is no console, and no action that
requires one. Clients drive it from their own tooling or Terraform.

---

## 11. Failure behaviour

Satisfies F4.1, F4.2, F4.3, F4.4, F4.5.

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

Satisfies N1, N2, N3, N4, N5, N6, N9.

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

The alternative is a Redis or Memcached tier for the counters. ElastiCache requires VPC
attachment, which places every function touching a counter in a private subnet, which then
requires VPC endpoints for each AWS service those functions call, plus a NAT gateway,
subnets, route tables and flow logs. Measured cost of that topology in an existing reference
deployment: 26 additional resources and approximately $330/month idle.

A VPC remains available as an opt-in variable for clients whose ATO boundary mandates
private-subnet compute regardless of IAM. That is policy, not architecture; the Lambda code
is identical either way.

### Isolation between concurrent events

A single deployment may run several events at once — a scheduled room on one path plus
standby across the site (§3.3), or a client running multiple product drops. Those events
share Lambda concurrency, table throughput, and one SQS queue, so a single event generating
disproportionate load can degrade the others.

**Shuffle sharding does not apply.** The technique assigns each tenant a random subset of N
nodes from a pool of M, so two tenants rarely share every node and one tenant's failure
reaches few others. It requires three conditions, and this design satisfies none of them:

| Condition | Status here |
|---|---|
| Multiple tenants share infrastructure | No — single-tenant, in the client's own account |
| The operator assigns tenants to nodes | No — DynamoDB, Lambda, SQS and CloudFront each own their own fleets. There is no node pool to assign |
| Per-tenant dedication is too expensive | No — see below |

The third condition is the decisive one. Shuffle sharding exists to avoid provisioning M
dedicated resources when M is large and each costs money at rest. Serverless resources cost
nothing at rest, so **full partitioning is available and strictly better**:

| Approach | Resources for 20 concurrent events | Idle cost | Blast radius |
|---|---|---|---|
| Partition: one queue + one function per event | 20 queues, 20 functions | $0 | 1 event |
| Shuffle shard: M=8 queues, N=2 per event | 8 queues | $0 | ~5 events per node; 1 in 28 event pairs share both nodes |

Partitioning gives complete isolation for the same idle cost. Shuffle sharding gives partial
isolation and exists only to reduce a resource count that is already free.

**The design therefore isolates by partition:** each event gets its own SQS queue, its own
Lambda function with reserved concurrency, and its own item in `Counters`. Reserved
concurrency is what makes this real — without it, functions draw from the shared account
concurrency pool and a runaway event starves the others regardless of how many functions
exist.

This holds until table count becomes the constraint: DynamoDB allows 2,500 tables per region
by default and up to 10,000 on request. Events share tables and are separated by
`event_id`, so that ceiling is far away. If a deployment ever exceeded it, shuffle sharding
would become the right answer — and would need revisiting then.

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

Satisfies O2, O3, O6.

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
| Two-credential model: single-use URL token validated once, then a separately-signed per-event session cookie; sliding vs fixed session validity; triggers matched on URL, headers, cookies, user agent; local validation with no backend round-trip | [Queue-it's architecture: the admission token, the cookie, and safety-net mode](https://blog.crawlex.net/blog/queue-it-architecture/) — teardown of Queue-it's open-source connector implementations |
| Queue Token SDK: client-signed identifier gating queue entry; members-only ticketing use case | [Queue-it Connectors](https://queue-it.com/developers/connectors/) |
| Hype Event Protection: blocking bots at sale start rather than on arrival; 225,000 bots excluded from an invite-only drop | [Queue-it bad bot protection](https://www.queue-it.com/bad-bot-protection) |
| Safety Net activation on configured inflow threshold; "Always Visible" vs "Visible at Peak"; randomization for scheduled and FIFO for safety-net | [Queue-it virtual waiting room](https://queue-it.com/virtual-waiting-room) |
| `BatchWriteItem`: 25 items per call, 16 MB per call, no conditional expressions, `UnprocessedItems` partial-failure model | [BatchWriteItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) |
| Lambda timeout 900 s; synchronous invocation payload 6 MB | [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html) |
| CloudFront default metrics: 1-minute granularity, `us-east-1`, no additional charge, do not count against CloudWatch quotas | [Monitor CloudFront metrics with CloudWatch](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/monitoring-using-cloudwatch.html) |
| FedRAMP cost and timeline | Published 3PAO and FedRAMP advisory pricing, cross-checked across sources |
| `Scan`: 1 MB page limit, `LastEvaluatedKey` pagination, parallel `Segment`/`TotalSegments`, eventually consistent by default | [Scanning tables in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html) |
| Small-domain format-preserving encryption: Feistel construction, cycle-walking, minimum domain size | [NIST SP 800-38G Rev. 1](https://csrc.nist.gov/pubs/sp/800/38/g/r1/2pd) |
| DynamoDB writes billed in 1 KB units rounded up, 1 WCU minimum | [DynamoDB read/write capacity](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| TTL deletes "within a few days"; expired items remain readable until deleted; use filter expressions to exclude them | [Using time to live (TTL) in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) |
| `UpdateItem` `ADD` atomic counter; `ReturnValues: ALL_NEW`; `ConditionExpression` support | [UpdateItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) |
| Request collapsing disabled by Min TTL 0 or cookie forwarding; `stale-while-revalidate` | [DDoS resilience with HTTP caching on CloudFront](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront) |
| REST API integration timeout 29 s (hard), request payload 10 MB | [Quotas for configuring and running a REST API](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-execution-service-limits-table.html) |
| SQS standard at-least-once delivery, best-effort ordering; `ReportBatchItemFailures`; visibility timeout ≥ 6× function timeout | [Using Lambda with Amazon SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) |
| Shuffle sharding: random per-tenant node subsets to limit blast radius; requires a node pool the operator assigns | [Workload isolation using shuffle-sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding) — Colm MacCárthaigh, AWS Builders' Library |
| DynamoDB tables per region: 2,500 default, up to 10,000 on request | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
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
