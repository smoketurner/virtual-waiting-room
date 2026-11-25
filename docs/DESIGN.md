# Design

How the virtual waiting room works, as built. Requirements are in
[`REQUIREMENTS.md`](./REQUIREMENTS.md), the reasoning behind each choice is in [`adr/`](./adr/),
and [`DYNAMODB.md`](./DYNAMODB.md) covers every access pattern in detail. Constraints and their
sources are in §14.

## 1. Overview

The waiting room meters visitors into an origin at a rate the origin can sustain. It has two modes:

- **Scheduled.** The start time is known. Early arrivals register during a countdown, and at T−0
  they are assigned randomized positions
  ([ADR-0001](adr/0001-randomize-pre-queue-assignment.md)).
- **Standby.** The gate stays deployed with no protection rules, so every request passes
  through. Once an operator writes rules, matching visitors are queued first-in, first-out.

Admission is a signed session cookie. A CloudFront Function at the edge verifies it locally, with
no call to the backend.

## 2. Architecture

### 2.1 Scheduled event

```
  T−n   Visitor → CloudFront → waiting page (S3, cached)
                                    │ POST /v1/join — one PreQueue row per visitor,
                                    ▼ spread across the countdown
                              DynamoDB PreQueue {r, s, l, t, h}

  T−0   EventBridge Scheduler → open_event
          BatchGetItem the 10 shard counters, then ONE UpdateItem on Counters:
            SET shuffle_seed, participant_count = N, queue_counter = N,
                prequeue_offsets, phase = active
            ConditionExpression: attribute_not_exists(shuffle_seed) AND phase = pre_queue

  T+    /v1/queue_num → read Lambda: PRP(seed, offset[s] + l, N), computed per read
```

### 2.2 Live join

```
  CloudFront   /v1/join: uncached, x-api-key origin header added
        │
  API Gateway  REST, regional; JSON Schema request validator → 400 on a malformed body
               type: aws → SQS SendMessage          ← no Lambda in the burst path
        │
  SQS          standard queue + DLQ (maxReceiveCount 5)
        │      ESM: BatchSize 100, window 1 s, ReportBatchItemFailures
        ▼
  assign_position
        1. classify records; drop malformed ones (counted and logged, never retried)
        2. one strongly consistent GetItem of Counters picks the path for the whole batch
        3. live join: UpdateItem ADD queue_counter :valid_count / ALL_NEW
        4. PutItem per record, ConditionExpression attribute_not_exists(request_id)
        5. return batchItemFailures for records whose write failed
```

### 2.3 Admission

```
  controller (EventBridge Scheduler rate(1 minute); a durable execution of six 10 s passes)
        grant admission budget for the time elapsed (plus what went unused, capped)
        → UpdateItem serving_counter (elapsed + unused), then cap c on each arrivals shard
        │
  Visitor polls /v1/status until serving_position ≥ own position
        │
  POST /v1/generate_token (request_id + possession secret, ADR-0035)
        1. event admitting? (phase active, resolve(stored control, fail_open_until, now))
        2. position reached?
        3. take one unit of the admission budget (ADR-0039): conditional ADD n on a
           randomly drawn arrivals shard while n < c (up to 3 draws), 425 when spent;
           then mark Positions.status issued → admitted. An already-admitted visitor
           re-mints without taking a unit
        4. sign the session (HS256 JWS, wr_common::crypto) → Set-Cookie
           (Path=/, Secure, HttpOnly, SameSite=Lax, Max-Age)
        │
  CloudFront Function, viewer-request, protected behaviour only   ← the gate
        1. no rule matches                         → pass through (dormancy, #60)
        2. now < enforce_from                      → pass through, marked pending
        3. now < fail_open_until                   → pass through, marked failopen
        4. cookie missing / bad signature / other event / expired / IP mismatch
                                                   → refuse with a reason (#73):
                                                     302 to /_wr/waiting.html for
                                                     navigation, 403 JSON for XHR (#72)
        5. valid session                           → pass through
        6. the function throws                     → pass through, marked failed
```

The gate reads its configuration (`c`: version, `enforce_from`, `fail_open_until`, rules, IP
binding) and the signing secret (`k`) from one CloudFront KeyValueStore
([ADR-0021](adr/0021-edge-function-gate.md)); `event_id` and the cookie name are templated into
its source. Terraform seeds `c` from `gate_rules`, with `enforce_from = 0`. `/admin/rules` is the
ruleset's only writer after that: it validates and re-encodes through `wr_common::rules`, writes
the KeyValueStore (the ruleset's only store) by read-modify-write, and stamps `rules_digest` /
`rules_count` on `Counters`. No admin route writes `enforce_from`.

## 3. Event lifecycle

```
  IDLE ──────► PRE-QUEUE ──────► ACTIVE ──────► POST-EVENT
                                     │
                         MAINTENANCE ┘   (any phase, operator-forced)
```

| Concern | Mechanism |
|---|---|
| Phase state | The `phase` attribute on the event's `Counters` item. `maintenance` is one of its values, not a separate override |
| Seed | Terraform writes the item at `idle`, or at `pre_queue` when `starts_at` is set, then ignores changes to it |
| Scheduled open | An EventBridge Scheduler `at()` schedule invokes `open_event`. It is created disabled unless `starts_at` is set, and `/admin/start_time` arms it ([ADR-0025](adr/0025-operator-owned-seal-schedule.md)) |
| Manual transitions | `/admin/phase` makes any transition except to `active`, which only the open writes. `/admin/open_now` invokes `open_event` |
| Visitor-facing state | `/v1/status` carries `phase` and a derived `serving_state` ([ADR-0019](adr/0019-visitor-facing-serving-state.md)) |

**Protection rules** (path, header, cookie or user agent, evaluated at the gate) select what is
queued; an unmatched request never is.

Scheduled events randomize because a published start makes arrival order measure latency, not
intent; standby is FIFO because an unplanned spike's arrival order carries information.

**Not built:** automatic standby activation (a CloudWatch alarm on CloudFront `Requests` →
EventBridge; [#60](https://github.com/smoketurner/virtual-waiting-room/issues/60), [`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md)
Phase 1b). Today an operator leaves dormancy by writing rules.

## 4. Pre-queue and position assignment

### 4.1 Registration

A pre-queue join uses the ingest path (§6). `assign_position` decides per batch, on the open
outputs rather than phase alone: opened → live join (any phase); unopened and `pre_queue` →
pre-queue; unopened in any other phase → the batch fails and claims nothing.

On the pre-queue path, the batch draws one shard **at random** (`Shard::random`, issue #59) and
claims a contiguous block of local indices with one `UpdateItem ADD n :count` on that shard's
counter item, `EVT#{event_id}#PQ#{s}`. It then writes one `PreQueue {r, s, l, t, h}` row per
record (§5.5) with `attribute_not_exists(r)`, after deduplicating the batch. Each shard is a separate item because the 1,000-write/s ceiling is per partition key, so
ten items lift registration to about 10,000/s
([ADR-0015](adr/0015-stripe-prequeue-counter.md)).

**Stragglers.** A registration landing after `open_event` read its shard is invisible to the
open. The batch re-reads `Counters` after its writes and, if the event opened underneath it, gives
each row it wrote a live-join position; `/v1/queue_num` covers any it missed (§4.2).

The waiting page is static, served from S3 through CloudFront. Its only countdown write is one
`POST /v1/join` per visitor, deduplicated client-side across reloads where storage allows.

### 4.2 Assignment

Queue order is a keyed **pseudorandom permutation (PRP)** evaluated on read. The order is never
stored ([ADR-0002](adr/0002-seeded-permutation-not-materialised-shuffle.md)).

At T−0 `open_event` computes `offset[s] = Σ counts[0..s)` and `N = Σ counts` from the ten shard
counters and writes them in the one conditional `UpdateItem` of §2.1. `queue_counter = N` in the
same write numbers live joiners behind the cohort; the guard makes a double-fire open once.
Assignment is complete when that write lands, so no participant ever holds a position while
another does not (C2). A participant's index is `i = offset[s] + l`, exactly `[0, N)`; a burned
index maps to a position nobody holds (F2.3).

```
queue_position = PRP(shuffle_seed, i, participant_count)
```

`/v1/queue_num` reads the `PreQueue` row and `Counters` and computes the position (409 before
the open). The straggler test is **per shard**: `l` at or past shard `s`'s issued count
(`offset[s+1] − offset[s]`, or `N − offset[s]` for the last). A global `i ≥ N` test would miss an
over-count on an interior shard, whose index range belongs to a later shard, and give two
visitors one position. A straggler never reaches `PRP`: it falls through to `Positions` — 200
with a live-join position if one exists, else 404, which the client answers by re-joining with
the same `request_id` (§6).

### 4.3 The permutation

A balanced Feistel network over the smallest power-of-four domain ≥ N, cycle-walked into
`[0, N)` — the small-domain format-preserving encryption construction (NIST SP 800-38G).

```
b        = ceil(bit_length(N-1) / 2)
F(r, x)  = be_u32(HMAC-SHA256(seed, r || x)[0..4]) & (2^b - 1)
enc(v):  L, R = v >> b, v & mask;  4 rounds of  L, R = R, L XOR F(round, R);  (L << b) | R
PRP(seed, i, N):  v = i;  loop { v = enc(v); if v < N return v }
```

#### Frozen wire encoding (audit contract)

The permutation is part of the audit contract: a third party must be able to recompute every
position byte for byte. The following encoding is **frozen**. An implementation that deviates
produces different positions and breaks auditability (§4.4). All multi-byte integers are
**big-endian**.

- **Domain.** `b = ceil(bit_length(N - 1) / 2)`; `domain = 2^(2b)`; `mask = 2^b - 1`. Both
  halves are exactly `b` bits. `N ≤ 1` is the identity (no rounds run). An index `i ≥ N` is
  returned unchanged and never permuted.
- **Rounds.** Exactly **4** rounds, `round ∈ {0, 1, 2, 3}` in ascending order.
- **HMAC key.** The 256-bit `shuffle_seed`, used verbatim as the HMAC-SHA256 key. It is the key,
  never part of the message.
- **Message `r || x`.** Exactly two fixed-width fields, **5 bytes** in total:
  - `r`: the round number as **1 byte** (`0x00`–`0x03`).
  - `x`: the right half `R` as a **4-byte big-endian `u32`**, zero-padded (`R < 2^b ≤ 2^32`).
- **Output → integer.** Take the **first 4 bytes** of the 32-byte HMAC output, read them as a
  big-endian `u32`, then apply `& mask`. That is, `F(r, x) = be_u32(HMAC-SHA256(seed, r ‖ x)[0..4]) & mask`.
- **Split / combine.** `L = v >> b`, `R = v & mask`. One round is `L, R = R, L XOR F(round, R)`.
  Recombine as `(L << b) | R`.
- **Cycle-walk.** Re-apply `enc` until the result is `< N`, then return it.

`frozen_wire_encoding_vectors` and `frozen_round_function_encoding` pin the encoding with fixed
`(seed, i, N) → position` vectors. Any drift in field width, byte order or the key/message split
fails the build.

| Property | Evidence (`crates/wr-common/src/permutation/mod.rs`) |
|---|---|
| Bijective | Feistel networks are invertible by construction, and cycle-walking preserves that on `[0, N)`. `prop_bijective` checks the full image for random seeds and `N < 2000`; `bijective_over_small_domains` for fixed `N` up to 1,000 |
| In domain | `prop_in_domain`: every output `< N`, for `N < 100,000` |
| Deterministic | `prop_deterministic`, `deterministic_across_calls` |
| Seed-dependent | `distinct_seeds_generally_differ`, `seed_scrambles_the_identity` |
| Uniform | `prop_uniform_by_decile` asserts χ² < 27.88 (p = 0.001, df = 9) over deciles at N = 10,000. A bijection onto `[0, N)` fills every decile exactly, so this follows from bijectivity and says nothing further about randomness |
| Cheap | Expected cycle-walk iterations are `domain / N`: at most 4, and 1.05 at N = 1,000,000. Each iteration is four HMACs |

**The seed does not exist before T−0.** Nobody can compute a position early or choose a
favourable index. The open writes the seed, and `/v1/status` publishes it from that moment.

### 4.4 Auditability

Once the open has written them, `/v1/status` publishes `shuffle_seed` (lowercase hex),
`participant_count` and `prequeue_offsets`. With the `(r, s, l)` tuples in `PreQueue`, anyone can
recompute every position.

### 4.5 Live joins after opening

A post-open joiner takes the next `queue_counter` value, which starts at `participant_count`:
first-come-first-served behind the whole cohort. The permutation applies only to the cohort.

## 5. Counters and data

### 5.1 Atomic sequence

`UpdateItem ADD … ReturnValues: ALL_NEW` generates positions. DynamoDB serializes writes to one
item, so each value is returned exactly once
([ADR-0003](adr/0003-dynamodb-counters-not-elasticache.md)).

### 5.2 Batch range allocation

A batch claims `[end − n + 1, end]` with one increment by the count of **valid** records, never
`records.len()`, so malformed records consume nothing. Each row is then a `PutItem` with
`attribute_not_exists(request_id)` ([ADR-0004](adr/0004-putitem-not-batchwriteitem.md)).

### 5.3 Throughput

The live-join ceiling is the per-table on-demand quota: 40,000 WRU/s by default, adjustable. The
1,000 WCU/s per-partition-key ceiling does not bind, because `request_id` keys are spread and the
counter takes one write per batch. A new or long-idle table serves only ~4,000 writes/s, and a
waiting room has no previous peak to grow from, so **tables are pre-warmed before every event**
through the `warm_throughput_*` variables (O1). The open is one write and needs none.

### 5.4 Sequences and statistics

`queue_counter` and `serving_counter` are sequences and are never sharded: summing shards cannot
yield an ordered unique value. The pre-queue index (order-free, reassembled into `[0, N)` at T−0)
and `arrivals` (a statistic) are striped across ten items. This is write sharding for throughput,
not shuffle sharding for isolation ([ADR-0008](adr/0008-partition-isolation-not-shuffle-sharding.md)).

Positions may have gaps: a retry after a 5xx, or a function dying between increment and write,
burns positions. No user sees a skipped number.

### 5.5 Tables

There are four tables, all on-demand with PITR. Each has a single partition key and no sort key.
Keys are built only in `wr_common::expr`.

**`Counters`** (PK `event_id`). The event item `EVT#{event_id}` holds:

| Attribute | Purpose |
|---|---|
| `phase` | `idle` / `pre_queue` / `active` / `post_event` / `maintenance` |
| `queue_counter`, `serving_counter` | Live-join sequence; admission cursor |
| `shuffle_seed`, `participant_count`, `prequeue_offsets` | Open outputs, written once at T−0 |
| `admission_control` | The stored control, `open` or `paused` only, never `fail_open` (issue #71) |
| `fail_open_until` | Fail-open deadline in epoch seconds. `0` means no window |
| `target_rate` | Operator admission rate, in visitors per second |
| `message` | Operator broadcast, delivered in `/v1/status` |
| `starts_at` | Scheduled start, which the waiting page counts down to |
| `last_open_pass_at`, `rules_digest`/`rules_count` | The controller's last open pass; ruleset audit |

`wr_common::resolve(stored, fail_open_until, now)` is the only producer of
`AdmissionControl::FailOpen`. A pre-queue shard item `EVT#{event_id}#PQ#{s}` holds its count
`n`. An arrivals shard item `EVT#{event_id}#AR#{s}` holds `n`, the admissions taken, and `c`, the
cap they may be taken up to. `event_id` may not contain `#`.

**`PreQueue`** (PK `r`) has the attributes `s`, `l`, `t` and `h`. The names are short because
DynamoDB bills attribute names on every write and the table holds one row per visitor. The table
is never scanned.

**`Positions`** (PK `request_id`) has the attributes `queue_position`, `entry_time`
(server-stamped and authoritative), `status` (`issued` / `admitted`, [ADR-0033](adr/0033-admission-claimed-once-per-visitor.md)),
`h` and `ttl`. A live joiner gets a row at join. A pre-queue member gets one at admission.
`ttl` only reclaims storage. It is never an admission deadline
([ADR-0031](adr/0031-remove-controller-driven-expiry.md)).

**`Tokens`** (PK `request_id`) holds operator OIDC sessions (`SESS#`) and pending PKCE logins
(`PKCE#`), reclaimed by TTL on `expires_at`
([ADR-0016](adr/0016-admin-oidc-dynamodb-sessions.md)).

## 6. Ingest

A regional REST API with an `AWS` integration to SQS `SendMessage`: no Lambda in the burst path,
so no cold start and no concurrency ceiling at ingest
([ADR-0005](adr/0005-rest-api-not-http-api.md)). The 29 s REST timeout, SQS throughput and the
6 MB Lambda payload are far from binding; the ESM batch is 100 with a 1 s window, hard-coded.

**Duplicates and ordering.** SQS standard is at-least-once with best-effort ordering. A duplicate
fails `attribute_not_exists` and consumes nothing; order is irrelevant because positions come from
the counter. Visibility timeout is 6 × function timeout + batching window; after 5 receives a
record goes to the DLQ, which alarms.

**Validation.** The gateway's JSON Schema model requires `request_id`, `event_id` and `h`, and
returns 400 synchronously when any is missing or malformed. `assign_position` checks again: the
body must parse, the `request_id` must have the canonical UUID shape (no version-nibble check)
and the `event_id` must match. A record that fails is **dropped**: counted in `join_dropped`,
logged, and never retried or dead-lettered, because no redelivery can make it valid.

**Recovery (F4.4).** A 200 from `/v1/join` means `SendMessage` succeeded, not that a position
exists; any other SQS outcome maps to a 502, which the client retries with backoff. A
`/v1/queue_num` 404 means re-join with the **same** `request_id`: the guard passes because no row
exists, and a fresh id would abandon whatever the first attempt resolves to. The straggler
self-heal (§4.1) rides the same loop.

## 7. Outflow control

Admission is metered where it happens, not where it is offered
([ADR-0039](adr/0039-meter-admissions-not-the-cursor.md)). `generate_token` takes one unit of an
**admission budget** for each visitor it admits and answers 425 when there is none. Each pass the
controller grants budget and moves the cursor:

```
owed     = target_rate × seconds since the last open pass     (one interval if ≥ 20 s, or none)
unused   = Σ over shards of (c − n)                           (granted and not taken)
budget   = min(unused + owed, 2 intervals of target_rate)     → written as c = n + share, per shard
release  = owed + min(unused, owed)                           → serving_counter, up to queue_counter + 1
```

The budget bounds what the origin receives: at most two intervals' worth is ever outstanding, so
over any window of `t` seconds no more than `target_rate × t` plus that burst is admitted,
whoever turns up and whenever. Unused budget is the no-show correction: it is exactly the
admissions the origin was offered and did not get, so the cursor releases that many more people.
The cursor moves at most twice the target, which covers a no-show rate up to 50%. Nothing is
estimated or smoothed.

The controller is a Lambda durable function: Scheduler starts one execution a minute, which runs
six checkpointed passes separated by 10 s durable waits. A wait suspends the execution, so it is
not billed, and a replayed pass returns its checkpoint rather than advancing `serving_counter`
twice ([ADR-0022](adr/0022-durable-controller-cadence.md)). A pass runs only while the phase is
`active` and the resolved control is `Open`. Because `owed` follows the wall clock, an overlapping
execution's extra pass grants and releases only the seconds since the one before it
([#236](https://github.com/smoketurner/virtual-waiting-room/issues/236)); after a gap of two
intervals or more a pass is owed one interval and does not make up the gap. All arithmetic is
checked or saturating.

**Arrivals** are the budget taken: `generate_token` adds to one of ten shard items, once per
visitor (60,000 admissions a minute would otherwise hit the 1,000 writes/s item ceiling). A pass
reads the ten and writes each one's cap. **The controller expires nothing** and never scans; the
unused budget compensates for absentees ([ADR-0031](adr/0031-remove-controller-driven-expiry.md)).

**Not built:** a concurrency term. Nothing reports when a session completes, so origin
concurrency drifts with session length
([#65](https://github.com/smoketurner/virtual-waiting-room/issues/65)).

## 8. API surface, caching, and security

### Endpoints and cache behaviours

| Behaviour | Paths | Caching / cookies | Origin | Purpose |
|---|---|---|---|---|
| Polled | `/v1/status` | Min TTL 1 s, key is the path; no cookies | API Gateway → `read` | Phase, `serving_state`, `serving_position`, `target_rate`, message, `starts_at`, `poll_policy` ([ADR-0023](adr/0023-adaptive-poll-interval.md)); after T−0 the seed, count and offsets |
| Polled | `/v1/queue_num` | Min TTL 1 s, key adds `event_id` + `request_id`; no cookies | API Gateway → `read` | Own position. 404 means re-join; 409 means not yet open |
| Join | `/v1/join` | uncached; no cookies | API Gateway → SQS | Join the queue or the pre-queue |
| Write | `/v1/generate_token`, `/v1/enter`, `/v1/wait` | uncached; all cookies | API Gateway → `generate_token`, `nojs` | Mint the session; the no-JavaScript queue ([ADR-0037](adr/0037-no-javascript-queue.md)) |
| Admin | `/admin`, `/admin/*`, `/static/*` | uncached; all cookies | API Gateway → `admin` | Operator UI and API |
| Waiting page | `/_wr/*` | cached; no cookies | S3 | Ungated: where a refused visitor lands |
| Protected | `/*` (default) | uncached; allow-listed cookies | Client origin | Behind the gate |

The polled behaviours need a Min TTL above zero and no forwarded cookies. Otherwise CloudFront
disables request collapsing and every poll reaches the origin
([ADR-0013](adr/0013-cache-behaviour-separation.md)). The gate is associated with the default
behaviour only, because a Function bills per invocation.

### Admin endpoints

Served by `admin` (Axum, askama templates) behind an OIDC Authorization Code + PKCE session
([ADR-0016](adr/0016-admin-oidc-dynamodb-sessions.md), [ADR-0018](adr/0018-admin-vouch-design-language.md));
every action works without JavaScript and no action needs the AWS console. A state-changing
request is refused with a 403 unless it comes from the console's own origin (`Sec-Fetch-Site`,
falling back to `Origin`), because the console shares a site with the customer's pages
([#243](https://github.com/smoketurner/virtual-waiting-room/issues/243)).

| Path | Purpose |
|---|---|
| `/admin`, `/admin/state`, `/admin/login`, `/admin/callback`, `/admin/logout` | Dashboard with the readiness panel (issue #70), its state as JSON, the OIDC session |
| `/admin/phase`, `/admin/open_now`, `/admin/start_time` | Any transition except to `active`; open now; set T−0 and arm the schedule |
| `/admin/rate`, `/admin/message` | Target rate; broadcast message |
| `/admin/pause`, `/admin/resume`, `/admin/force_maintenance` | Hold or resume admission (queue and rate kept); emergency full stop ([ADR-0017](adr/0017-andon-cord-emergency-controls.md)) |
| `/admin/fail_open`, `/admin/recover` | Set (≤ 1,440 min) or clear the fail-open epoch; a pause still in force applies once it clears |
| `/admin/rules`, `/admin/ip_binding` | Replace the gate ruleset (§2.3); session IP binding on/off ([ADR-0036](adr/0036-optional-session-ip-binding.md)) |

### Credentials

One credential: the session cookie, an HS256 JWS (`aud` event id, `sub` request id, `iat`,
`exp`) signed under a key *derived* from the deployment secret, so a future second kind cannot
validate as a session ([ADR-0011](adr/0011-session-cookie-after-token.md),
[ADR-0024](adr/0024-jws-credentials.md)). `generate_token` mints it only for the holder of the
`request_id`'s possession secret (ADR-0035); the gate refuses one for another event, and, with IP
binding on, one whose network tag does not match. The admission token and origin authorizer are
gone ([ADR-0032](adr/0032-remove-the-origin-authorizer.md)). **Not built:** revocation
([#63](https://github.com/smoketurner/virtual-waiting-room/issues/63)).

`random_bytes.signing_key` is generated at apply and written to both readers: an SSM SecureString
for the Lambdas (free idle, unlike Secrets Manager) and KeyValueStore key `k` for the gate.
Regenerating it invalidates every issued cookie.

### Entry gating and bots

Nothing binds a position to a person: a client that mints N `request_id`s takes N places.
Entry tickets ([ADR-0028](adr/0028-remove-entry-tickets.md)) and open-time bot demotion
([ADR-0030](adr/0030-remove-open-time-demotion.md), F6.3) were removed; the edge web ACL below is
what remains. **Not built:** a per-visitor position limit
([#59](https://github.com/smoketurner/virtual-waiting-room/issues/59)).

### Web Application Firewall (WAF)

**The REST API answers only CloudFront** ([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)):
every method requires an API key (`core/origin_key.tf`) that only the distribution sends, as an
`x-api-key` origin header. A direct `execute-api` request gets an unbilled 403.

**The edge web ACL exists only with a flat-rate plan** (`flat_rate_plan_tier` = `BUSINESS` or
`PREMIUM`; `edge/waf.tf`, `edge/plan.tf`), which bundles its cost. On pay-as-you-go it would bill
per request and per rule between events, breaking N1.

| Rule | Paths | Action |
|---|---|---|
| Verify-page challenge | `/_wr/verify.html` | Challenge. Issues `aws-waf-token` and returns the visitor to the waiting page |
| Token required | `/v1/join`, `/v1/status`, `/v1/queue_num`, `/v1/generate_token` | Challenge: a 202 with `x-amzn-waf-action`, which `waiting.js` treats as "renew on the verify page" |
| Joins per token | `/v1/join` | Block (429) past 10 per 10 min |
| Requests per token | the four JS API paths | Block (429) past `waf_api_token_limit` per 5 min |
| Token requests + joins per IP | verify page, `/v1/join` | CAPTCHA past `waf_join_ip_limit` per 5 min. It escalates rather than blocks, because carrier NAT puts many real buyers behind one IP |
| Anonymous / hosting IP | verify page, join | CAPTCHA (Count until promoted) |
| No-JS joins per IP | `/v1/enter` | Block past `waf_nojs_ip_limit` per 5 min |
| Anonymous IP, no JS | `/v1/enter` | Block (Count until promoted) |
| No-JS closed | `/v1/enter`, `/v1/wait` | Block, only when `nojs_enabled = false` |
| Managed: Anti-DDoS, IP reputation, Bot Control common | ACL-wide or the room's paths | Count until `waf_managed_rules_mode = "enforce"` (O5, [ADR-0012](adr/0012-anti-ddos-count-mode.md)) |

The no-JavaScript paths and `waiting.html` are never challenged; a Terraform test fails if
`waiting.js` calls a `/v1` path the token rule misses. This raises the price of a place to one
browser session; it does not bind a place to a person.

## 9. Operator surface

| Capability | Mechanism |
|---|---|
| Alerting | Metric filters over the Lambdas' structured logs, each with an alarm at threshold zero, plus alarms on DLQ depth (`core/logging.tf`). No dashboard resource ships |
| Pre-event readiness | The dashboard's diagnostics panel, which makes read-only checks against the deployment (issue #70, [`RUNBOOK.md`](./RUNBOOK.md)) |
| Branding | Waiting-page HTML, CSS and JS in S3 behind CloudFront |
| Messaging | `/admin/message` makes one `UpdateItem`, and every waiter sees it within one `/v1/status` TTL |
| Position and wait | `/v1/queue_num` gives the position. The client estimates the wait from the measured movement of the cursor, falling back to `target_rate` |

`waiting.js` polls adaptively (ADR-0023): the interval scales with the visitor's distance to the
front, between a floor and a ceiling set in Terraform (`poll_floor_ms`, `poll_ceiling_ms`,
`poll_divisor`). A visitor near the front sees a pause or a fail-open within the floor. A
deep-queue visitor can see it up to about 39 s late (the ceiling plus jitter). The O4 runbook
accepts that delay.

**Not built:** live metrics for F5.1 (queue depth, admitted, no-show rate). See [`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md),
Phase 1h.

## 10. Failure behaviour

**Fail-open** ([ADR-0009](adr/0009-fail-open.md)): `/admin/fail_open` sets `fail_open_until` on
`Counters` and in the KeyValueStore, and the gate passes protected requests until its own clock
passes it. The gate also passes through when it throws (unreadable config, unknown version).
**Not built:** automatic fail-open on a backend outage
([#58](https://github.com/smoketurner/virtual-waiting-room/issues/58)); there is no fail-closed
setting. The failure domain is one region ([ADR-0034](adr/0034-single-region-failure-domain.md)).

**429s.** API Gateway's account throttle is a token bucket that refills at the RPS quota and
holds 5,000; raising the RPS quota is the only lever. It smooths rather than caps: a
200,000-visitor burst against 50,000 RPS drains in seconds. The client retries 429s with jittered
backoff (F4.5); treating one as fatal would turn smoothing into an outage.

## 11. Deployment

The system is single-tenant and deploys into the client's own AWS account
([ADR-0007](adr/0007-single-tenant-deployment.md)). `infra/environments/dev` is the only
deployable root, built from the `core` and `edge` modules plus an optional `demo-origin`. Every
function deploys a real build, and a missing zip fails at plan. The apply seeds the `Counters`
item, the gate ruleset and the open schedule, so a fresh stack works without manual setup steps.
See [`DEPLOY.md`](./DEPLOY.md).

- **No VPC.** DynamoDB, SQS, SSM, EventBridge and Lambda are reached over IAM-authenticated
  public endpoints, so there is no NAT gateway and no VPC endpoints.
- **Client origin.** A custom origin addressed by domain name. The distribution sends it an
  `x-wr-origin-verify` header carrying a generated secret
  ([#241](https://github.com/smoketurner/virtual-waiting-room/issues/241)); the origin has to
  refuse requests without it, or its hostname bypasses the room ([`DEPLOY.md`](./DEPLOY.md)).
  Network-level restriction, for example with VPC origins, is not built by this module.
- **Event isolation.** One deployment serves one event, so isolation between events is isolation
  between stacks ([ADR-0008](adr/0008-partition-isolation-not-shuffle-sharding.md)). Within a
  deployment, `assign_position` and `nojs` have reserved concurrency so that ingest cannot drain
  the account pool the control plane uses.
- **GovCloud** is out of scope (N4). It has no CloudFront, so it has no edge gate and no request
  collapsing. A GovCloud variant would be a different topology, tracked in
  [`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md) Phase 5.

## 12. Cost

Edge request volume dominates cost, and the poll interval sets it. The mechanism is
[ADR-0023](adr/0023-adaptive-poll-interval.md); the model lives here.

**Model.** 1,000,000 visitors; a 60-minute countdown with late arrival (mean 10 minutes on the
page before the open, drawn from `u^0.2`); a 20-minute drain at 833/s (mean wait 10 minutes).
One-shot requests (join, `queue_num`, `generate_token`, assets) add ~6M. The **baseline** is a
fixed-interval client whose hidden tabs poll at Chrome's throttled ~60 s, the assumption most
favourable to it. The adaptive client sends nothing while hidden, apart from one poll on return
and ~5 requests per iOS discard-and-reload.

| Hidden share | Baseline | Adaptive | Ratio | vs. Business 125M/month |
|---|---|---|---|---|
| 0 (foreground-only upper bound) | ~215M | ~55M | 3.9x | baseline 1.7x over; adaptive 56% headroom |
| 0.25 | ~168M | ~43M | 3.9x | baseline 1.3x over |
| 0.5 | ~120M | ~31M | 3.9x | baseline just under |

- **Scope every figure to its model.** "1.7x over" holds at hidden share 0, not at 0.5.
- **Components differ:** countdown polling saves 6.0x, queue polling 3.25x, so the blend depends
  on arrival shape. Early arrival (mean 50 minutes) gives ~632M → ~125M, 5.0x, exactly the
  Business allowance.
- **iOS** suspends hidden tabs, compressing the ratio to 3.62x at hidden share 0.5 and 3.19x at
  0.75. Mobile is plausibly the majority of a consumer drop.
- **Measured, the saving is smaller.** The harness (`crates/harness`, foreground only) measures
  4.60x on `/status` alone against 4.27x modelled (`--visitors 4000 --seconds 60 --countdown 30
  --target-rate 1 --arrival late`), but 2.66x (late) and 2.88x (uniform) end to end at 833/s with
  a 180 s countdown. That puts adaptive at ~75–81M, inside Business with 35–40% headroom. Short
  runs understate the saving: `make load VISITORS=1000000 SECONDS=90` reads 1.83x because the
  ~60 s first-ask spread dominates it. The harness cannot measure time-to-admission.
- **No T−0 herd.** Open discovery spreads over the ~39 s ceiling jitter plus `scheduleFirstAsk`'s
  60 s, so peak `/v1/queue_num` load is no higher than the 60 s spread alone
  ([#84](https://github.com/smoketurner/virtual-waiting-room/issues/84)).

**Pricing** (baseline, foreground-only bound, pay-as-you-go, US/Europe):

| Component | Baseline |
|---|---|
| CloudFront requests ($0.0100 per 10,000) | $215 |
| WAF + Bot Control Common (pay-as-you-go only) | $129 + $123; Targeted bot detection is Custom pricing only (~$1,230) |
| API Gateway (cache misses, ~3 per visitor) | $10 |
| DynamoDB pre-warming | per event |
| SQS, Lambda, DynamoDB writes | negligible |

Adaptive polling divides the CloudFront and WAF rows by the ratio above; do not assume more
than 3.9x. For a large event a **flat-rate plan** is usually cheaper and caps exposure: Business
is $200/month for 125M requests and bundles the web ACL, managed rules and WAF fees, with blocked
requests not counted. It is **not the default** because it bills monthly, which breaks N1 between
events, and a subscribed distribution outlives `make destroy` until the billing cycle ends. The
per-client choices are open in [`adr/README.md`](adr/README.md).

**Caveats.** The model assumes clients honour `policy.floorMs`; nothing server-side enforces it,
so abuse is the edge's job (§8). Clients keep the last good policy when one is withdrawn, so a bad
policy is rolled back by publishing corrected values, not by removing them.

## 13. Requirement coverage

| Requirements | Section |
|---|---|
| F0.1–F0.8: lifecycle, modes, protection rules, maintenance (standby auto-activation not built) | §2.3, §3 |
| F1.1–F1.5, C1, C2: pre-queue, randomized atomic assignment, auditability | §4 |
| F2.1–F2.6, C3, C5: live join, uniqueness, gaps, idempotent and malformed joins | §4.5, §5, §6 |
| F3.1–F3.5, F3.8: position, rate control, session, gate, no-show compensation | §2.3, §7, §8 |
| F4.1–F4.5: fail-open, lost-join recovery, 429 retry | §6, §10 |
| F5.1–F5.5, O4: operator surface | §8, §9 |
| C4, N7, N10, O5: polling load, edge mitigation, Count-then-Block | §8, §12 |
| N1–N5, N9, O1–O3, O6: deployment, isolation, pre-warming, cost | §5.3, §11, §12 |

Retired requirements are listed in [`REQUIREMENTS.md`](./REQUIREMENTS.md#retired-requirements).

## 14. Constraints and sources

| Claim | Source |
|---|---|
| DynamoDB on-demand 40,000 RU/WU per table, adjustable; new tables ~4,000 writes/s; 1,000 WCU/s per partition | [Quotas](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html), [On-demand](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html), [Partition keys](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html), [Pre-warming](https://aws.amazon.com/blogs/database/pre-warming-amazon-dynamodb-tables-with-warm-throughput/) |
| `UpdateItem ADD` is atomic and serialized per item; `BatchWriteItem` takes no conditions | [UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html), [BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) |
| API Gateway 10,000 RPS throttle, 5,000 burst not adjustable; REST timeout 29 s; validators REST-only | [Quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html), [REST quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-execution-service-limits-table.html), [Request validation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-method-request-validation.html) |
| SQS standard: nearly unlimited, at-least-once, best-effort order; ESM batch up to 10,000 | [SQS quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html), [SQS ESM](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
| Request collapsing disabled by Min TTL 0 or cookie forwarding | [DDoS resilience with HTTP caching](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront) |
| CloudFront unavailable in GovCloud | [CloudFront with GovCloud](https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/setting-up-cloudfront.html) |
| WAF and CloudFront flat-rate pricing | [WAF pricing](https://aws.amazon.com/waf/pricing), [CloudFront pricing](https://aws.amazon.com/cloudfront/pricing/) |
| Small-domain FPE: Feistel, cycle-walking | [NIST SP 800-38G Rev. 1](https://csrc.nist.gov/pubs/sp/800/38/g/r1/2pd) |
| Prior art: pre-queue randomization, FIFO safety net, no-show compensation | [How Queue-it Works](https://www.queue-it.com/developers/how-queue-it-works), [Smooth Scaling ep. 17](https://queue-it.com/smooth-scaling-podcast/ep017-virtual-waiting-room-architecture/) |
| Shuffle sharding needs an operator-assigned node pool | [Shuffle-sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding) |
