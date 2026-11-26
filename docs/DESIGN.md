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
                                    │ POST /_wr/v1/join — one PreQueue row per visitor,
                                    ▼ spread across the countdown
                              DynamoDB PreQueue {r, s, l, t, h}

  T−0   EventBridge Scheduler → open_event
          wait, up to 900 s, for the join queue to catch up twice running (#239)
          BatchGetItem the 10 shard counters, then ONE UpdateItem on Counters:
            SET shuffle_seed, participant_count = N, queue_counter = N,
                prequeue_offsets, phase = active
            ConditionExpression: attribute_not_exists(shuffle_seed) AND phase = pre_queue

  T+    /_wr/v1/queue_num → read Lambda: PRP(seed, offset[s] + l, N), computed per read
```

### 2.2 Live join

```
  CloudFront   /_wr/v1/join: uncached, x-api-key origin header added
        │
  API Gateway  REST, regional; JSON Schema request validator → 400 on a malformed body
               type: aws → SQS SendMessage          ← no Lambda in the burst path
        │
  SQS          standard queue + DLQ (maxReceiveCount 10)
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
        count live sessions from the admission log; grant the free slots,
        origin_concurrency − live, as admission budget
        → UpdateItem serving_counter (the slots due free by the next pass, plus a
          no-show correction), the log, then cap c on each arrivals shard
        │
  Visitor polls /_wr/v1/status until serving_position ≥ own position
        │
  POST /_wr/v1/generate_token (request_id + possession secret, ADR-0035)
        1. event admitting? (phase active, resolve(stored control, fail_open_until, now));
           the Counters item is held 1 s per execution environment
        2. position reached?
        3. take one unit of the admission budget (ADR-0039): conditional ADD n on a
           randomly drawn arrivals shard while n < c (up to 3 draws), 425 when spent;
           a shard seen spent is not asked again for 1 s;
           then mark Positions.status issued → admitted, stamping admitted_at. An
           already-admitted visitor re-mints without taking a unit
        4. sign the session (HS256 JWS, wr_common::crypto), expiring at
           admitted_at + ttl whenever it is asked for; 410 once that has passed (#237)
           → Set-Cookie
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
        6. the function throws before a rule match → pass through, marked failed
        7. it throws after one                     → refuse, plain 403, reason `error`
```

The gate reads its configuration (`c`: version, `enforce_from`, `fail_open_until`, rules, IP
binding) and the signing secret (`k`) from one CloudFront KeyValueStore
([ADR-0021](adr/0021-edge-function-gate.md)); `event_id` and the cookie name are templated into
its source. A session that fails under `k` is tried under the previous key `kp`, the signing
key's previous generation (#247). Terraform seeds `c` from `gate_rules`, with `enforce_from = 0`. `/_wr/admin/rules` is the
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
| Scheduled open | An EventBridge Scheduler `at()` schedule invokes `open_event`. It is created disabled unless `starts_at` is set, and `/_wr/admin/start_time` arms it ([ADR-0025](adr/0025-operator-owned-seal-schedule.md)) |
| Manual transitions | `/_wr/admin/phase` makes any transition except to `active`, which only the open writes. `/_wr/admin/open_now` invokes `open_event` |
| Visitor-facing state | `/_wr/v1/status` carries `phase` and a derived `serving_state` ([ADR-0019](adr/0019-visitor-facing-serving-state.md)) |

**Protection rules** (path, header, cookie or user agent, evaluated at the gate) select what is
queued; an unmatched request never is. A path rule also claims any decoded path with a `.` or
`..` segment, a `;` or a backslash, which origins canonicalise differently, and a prefix that
could never match (`%`, `//`, a dot segment, `;`, backslash) is rejected when the rule is written
([#242](https://github.com/smoketurner/virtual-waiting-room/issues/242)).

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

**Draining before the open.** A join answered 200 before T−0 is only in the cohort once
`assign_position` has claimed its index, and arrivals bunch in the last minute, when the consumers
are furthest behind. So the open first reads the join queue's waiting count
(`sqs:GetQueueAttributes`) and the ten shard counts once a second, until twice running no more is
waiting than the consumers counted since the last read. It waits as long as its 900 s timeout
allows, so how fast joins arrive decides when the event opens, not who is in the cohort
([ADR-0042](adr/0042-the-open-waits-out-the-join-backlog.md)). An empty queue is not the test:
joins keep arriving after T−0, and under steady traffic the queue rarely reads empty twice
([local stack](LOCAL_STACK.md#findings)). Everyone who joins during the wait is in the cohort.
In-flight batches are not waited for: they are the straggler case. A wait that reaches the
deadline, or a queue that cannot be read, opens anyway and logs `open_drain_timed_out` or
`open_drain_unreadable` ([#239](https://github.com/smoketurner/virtual-waiting-room/issues/239)).

A join refused with a 429 is not queued at all. During the countdown the page retries it at the poll
floor, about every 5 s, without backing off, so a registration throttled in the last seconds lands
inside the drain wait and is in the cohort. With the arrival model in §12, 44% of a cohort arrives
in the last second of the countdown, which at the default join throttle is more than 7,000/s.

**Stragglers.** A registration landing after `open_event` read its shard is invisible to the
open. The batch re-reads `Counters` after its writes and, if the event opened underneath it, gives
each row it wrote a live-join position; `/_wr/v1/queue_num` covers any it missed (§4.2).

The waiting page is static, served from S3 through CloudFront. Its only countdown write is one
`POST /_wr/v1/join` per visitor, deduplicated client-side across reloads where storage allows.

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

`/_wr/v1/queue_num` reads the `PreQueue` row and `Counters` and computes the position (409 before
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
favourable index. The open writes the seed, and `/_wr/v1/status` publishes it from that moment.

### 4.4 Auditability

Once the open has written them, `/_wr/v1/status` publishes `shuffle_seed` (lowercase hex),
`participant_count` and `prequeue_offsets`. With a registration's `(s, l)` its position can be
recomputed; with the `PreQueue` table, every position can. This makes the order *recomputable*. It
does not prove the seed was drawn fairly: the operator's own function draws it, after the indices
are fixed, and nothing in the account can prove otherwise to someone who does not trust the
operator ([ADR-0040](adr/0040-what-the-published-seed-proves.md)).

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
waiting room has no previous peak to grow from. A cold table still takes every join, because the
open waits for the queue to drain (§4.1); pre-warming through the `warm_throughput_*` variables
(O1) only shortens that wait. The open is one write and needs none; its wall time is that write
plus the wait.

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
| `origin_concurrency` | How many visitors the origin takes at once |
| `message` | Operator broadcast, delivered in `/_wr/v1/status` |
| `starts_at` | Scheduled start, which the waiting page counts down to |
| `rules_digest`/`rules_count` | Ruleset audit |

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
the counter. Visibility timeout is 6 × function timeout + batching window; after 10 receives a
record goes to the DLQ, which alarms. Ten, not five, so a table throttling at the open does not
dead-letter joins that a later receive would have written.

**Validation.** The gateway's JSON Schema model requires `request_id`, `event_id` and `h`, and
returns 400 synchronously when any is missing or malformed. `assign_position` checks again: the
body must parse, the `request_id` must have the canonical UUID shape (no version-nibble check)
and the `event_id` must match. A record that fails is **dropped**: counted in `join_dropped`,
logged, and never retried or dead-lettered, because no redelivery can make it valid.

**Recovery (F4.4).** A 200 from `/_wr/v1/join` means `SendMessage` succeeded, not that a position
exists; any other SQS outcome maps to a 502, which the client retries with backoff. A
`/_wr/v1/queue_num` 404 means re-join with the **same** `request_id`: the guard passes because no row
exists, and a fresh id would abandon whatever the first attempt resolves to. The straggler
self-heal (§4.1) rides the same loop.

## 7. Outflow control

The origin takes `origin_concurrency` visitors at once, and that is what the room meters: the most
sessions live at any moment ([ADR-0041](adr/0041-meter-origin-concurrency.md),
[#65](https://github.com/smoketurner/virtual-waiting-room/issues/65)). Admission is metered where it
happens ([ADR-0039](adr/0039-meter-admissions-not-the-cursor.md)): `generate_token` takes one unit of
an **admission budget** for each visitor it admits and answers 425 when there is none.

A session ends exactly `session_ttl_seconds` after its admission, and minting it again keeps that
end ([#237](https://github.com/smoketurner/virtual-waiting-room/issues/237)), so the sessions live
now are the admissions of the last session length. The controller keeps an **admission log**, the
total admitted at each pass, to count them. Each pass:

```
admitted = Σ over shards of n                                  (logged at the second the shards were read)
horizon  = now − session_ttl_seconds
live     = admitted − log[horizon]                             (the latest entry at or before it)
budget   = origin_concurrency − live                           → written as c = n + share, per shard
provided = origin_concurrency + log[horizon + 10 s]            (every slot, and each due free by the next pass)
release  = provided − log.provided + ⌈min(unused, budget) / 2⌉ → serving_counter, up to queue_counter + 1
```

The budget is the free slots, so no more than `origin_concurrency` sessions are ever live, whoever
turns up and whenever. The log reads the latest entry at or before the horizon, so an admission
counts as live until the first pass after its session has certainly ended: a slot is seen free up
to one pass (10 s) late, and a log that misses a pass counts its admissions live for longer, never
shorter. The entry is stamped with the whole second at which the shards were read, not the pass's
start: a session starts at its request's whole second, and the request took its unit before the
read, so no session the entry counts ends later than its stamp plus the session length.

The cursor moves once for every slot: the limit at the open, then each session as it is due to end,
counted a pass ahead so the people released for it are already asking when the slot is granted.
Without that lead, sessions that started together end together and the origin sits empty while
their replacements poll in. Unused budget is the no-show correction: a slot granted at the last
pass and still untaken had someone released for it who did not come, so half as many again are
released, which refills a slot nobody takes within a few passes without drawing a crowd for one
whose visitor is merely slow. Lowering the limit below the live sessions grants nothing and releases
nobody until enough sessions have ended. Throughput is `origin_concurrency` per session length:
at the 900 s default, a limit of 100 admits about 400 an hour.

A refusal has to cost almost nothing, because while the budget is spent everyone whose turn has
come keeps asking. Each `generate_token` execution environment remembers for one second which
shards it saw spent, and holds the event's `Counters` item for one second, so a refusal makes no
conditional write and does not read the hot item; a pause therefore takes effect up to a second
late. The page asks again at its floor after a 425: the people asking are only those released for
the next slots, about one turnover's worth, and a slot left waiting for a backed-off visitor is
throughput lost. A visitor within `origin_concurrency` of the front polls at the floor too, because
sessions ending release them without the cursor's measured speed saying so in advance.

The controller is a Lambda durable function: Scheduler starts one execution a minute, which runs
six checkpointed passes separated by 10 s durable waits. A wait suspends the execution, so it is
not billed, and a replayed pass returns its checkpoint rather than advancing `serving_counter`
twice ([ADR-0022](adr/0022-durable-controller-cadence.md)). A pass runs only while the phase is
`active` and the resolved control is `Open`. The budget and the release are both absolute, computed
from the shards and the log rather than from the time since the last pass, so an overlapping
execution's extra pass grants nothing twice and releases only what came due since
([#236](https://github.com/smoketurner/virtual-waiting-room/issues/236)). A pass that loses the
cursor race writes neither the log nor the caps. All arithmetic is checked or saturating.

A pass that fails is skipped (`controller_pass_failed`) and the cadence carries on; the next pass
computes the same free slots from the same state. An execution whose first pass finds the phase
`idle` or `post_event` ends there, since such an event only becomes active through `pre_queue` and
an open; that is most of the controller's cost between events (N1), and it means an operator who
goes idle → pre_queue → open inside one minute waits for the next execution before admission
starts.

**Arrivals** are the budget taken: `generate_token` adds to one of ten shard items, once per
visitor. A pass reads the ten, the log and the event item, and writes the cursor, the log and each
shard's cap. **The controller expires nothing** and never scans; the unused budget compensates for
absentees ([ADR-0031](adr/0031-remove-controller-driven-expiry.md)).

**Not built:** a completion signal. A visitor who finishes early holds their slot until the session
ends, because nothing reports a departure
([#65](https://github.com/smoketurner/virtual-waiting-room/issues/65)).

## 8. API surface, caching, and security

### Endpoints and cache behaviours

| Behaviour | Paths | Caching / cookies | Origin | Purpose |
|---|---|---|---|---|
| Polled | `/_wr/v1/status` | Min TTL 1 s, key is the path; no cookies | API Gateway → `read` | Phase, `serving_state`, `serving_position`, `origin_concurrency`, `session_seconds`, message, `starts_at`, `poll_policy` ([ADR-0023](adr/0023-adaptive-poll-interval.md)); after T−0 the seed, count and offsets |
| Polled | `/_wr/v1/queue_num` | Min TTL 1 s, key adds `event_id` + `request_id`; no cookies | API Gateway → `read` | Own position. 404 means re-join; 409 means not yet open |
| Join | `/_wr/v1/join` | uncached; no cookies | API Gateway → SQS | Join the queue or the pre-queue |
| Write | `/_wr/v1/generate_token`, `/_wr/v1/enter`, `/_wr/v1/wait` | uncached; all cookies | API Gateway → `generate_token`, `nojs` | Mint the session; the no-JavaScript queue ([ADR-0037](adr/0037-no-javascript-queue.md)) |
| Admin | `/_wr/admin`, `/_wr/admin/*`, `/_wr/static/*` | uncached; all cookies | API Gateway → `admin` | Operator UI and API |
| Waiting page | `/_wr/*`, matched after the rows above | cached; no cookies | S3 | Ungated: where a refused visitor lands |
| Protected | `/*` (default) | uncached; allow-listed cookies | Client origin | Behind the gate |

Everything the room serves is under `/_wr/`, the one path prefix it reserves on the customer's
hostname (issue #251).

The polled behaviours need a Min TTL above zero and no forwarded cookies. Otherwise CloudFront
disables request collapsing and every poll reaches the origin
([ADR-0013](adr/0013-cache-behaviour-separation.md)). The gate is associated with the default
behaviour only, because a Function bills per invocation.

### Admin endpoints

Served by `admin` (Axum, askama templates) behind an OIDC Authorization Code + PKCE session
([ADR-0016](adr/0016-admin-oidc-dynamodb-sessions.md), [ADR-0018](adr/0018-admin-vouch-design-language.md));
every action works without JavaScript and no action needs the AWS console. A state-changing
request is refused with a 403 unless it comes from the console's own origin (`Sec-Fetch-Site`,
falling back to `Origin`) and carries the session's `csrf` form field, because the console shares
a site with the customer's pages
([#243](https://github.com/smoketurner/virtual-waiting-room/issues/243)). Logout is a POST. The
session cookie is `__Host-` prefixed and `SameSite=Strict`, a login needs a verified email, and the
allowlist is re-checked on every request.

| Path | Purpose |
|---|---|
| `/_wr/admin`, `/_wr/admin/state`, `/_wr/admin/login`, `/_wr/admin/callback`, `/_wr/admin/logout` | Dashboard with the readiness panel (issue #70), its state as JSON, the OIDC session |
| `/_wr/admin/phase`, `/_wr/admin/open_now`, `/_wr/admin/start_time` | Any transition except to `active`; open now; set T−0 and arm the schedule |
| `/_wr/admin/concurrency`, `/_wr/admin/message` | Origin concurrency; broadcast message |
| `/_wr/admin/pause`, `/_wr/admin/resume`, `/_wr/admin/force_maintenance` | Hold or resume admission (queue and rate kept); emergency full stop ([ADR-0017](adr/0017-andon-cord-emergency-controls.md)) |
| `/_wr/admin/fail_open`, `/_wr/admin/recover` | Set (≤ 1,440 min) or clear the fail-open epoch; a pause still in force applies once it clears |
| `/_wr/admin/rules`, `/_wr/admin/ip_binding` | Replace the gate ruleset (§2.3); session IP binding on/off ([ADR-0036](adr/0036-optional-session-ip-binding.md)) |

### Credentials

One credential: the session cookie, an HS256 JWS (`aud` event id, `sub` request id, `iat`,
`exp`) signed under a key *derived* from the deployment secret, so a future second kind cannot
validate as a session ([ADR-0011](adr/0011-session-cookie-after-token.md),
[ADR-0024](adr/0024-jws-credentials.md)). `generate_token` mints it only for the holder of the
`request_id`'s possession secret (ADR-0035); the gate refuses one for another event, and, with IP
binding on, one whose network tag does not match. A session asked for again expires when the
first one did, so one admission is one visit however often, or by whomever, it is redeemed
([#237](https://github.com/smoketurner/virtual-waiting-room/issues/237)). The admission token and origin authorizer are
gone ([ADR-0032](adr/0032-remove-the-origin-authorizer.md)). **Not built:** revocation
([#63](https://github.com/smoketurner/virtual-waiting-room/issues/63)).

`random_bytes.signing_key` is generated at apply, one key per generation, and the current one is
written to both readers: an SSM SecureString for the Lambdas (free idle, unlike Secrets Manager)
and KeyValueStore key `k` for the gate. Raising `signing_key_generation` rotates it: the previous
generation becomes `kp`, which the gate still accepts, so sessions already issued last until they
expire rather than ending at once; a second rotation drops them
([`DEPLOY.md`](./DEPLOY.md#rotating-keys)). The functions that read the key (`generate_token`,
`nojs`) read it once at init, so a non-secret `SIGNING_KEY_FINGERPRINT` in their environment changes
with the key: a rotation publishes new configuration and retires the warm environments still
holding the old one ([#247](https://github.com/smoketurner/virtual-waiting-room/issues/247)).

### Entry gating and bots

Nothing binds a position to a person: a client that mints N `request_id`s takes N places.
Entry tickets ([ADR-0028](adr/0028-remove-entry-tickets.md)) and open-time bot demotion
([ADR-0030](adr/0030-remove-open-time-demotion.md), F6.3) were removed; the edge web ACL below is
what remains. **Not built:** a per-visitor position limit
([#59](https://github.com/smoketurner/virtual-waiting-room/issues/59)).

### Web Application Firewall (WAF)

**The REST API answers only CloudFront** ([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)):
every method requires an API key (`core/origin_key.tf`) that only the distribution sends, as an
`x-api-key` origin header. A direct `execute-api` request gets an unbilled 403. Per-method stage
throttles (`api_join_throttle_*`, `api_throttle_*`) apply with or without the web ACL, so one path
cannot drain the account's shared bucket.

**The edge web ACL exists only with a flat-rate plan** (`flat_rate_plan_tier` = `BUSINESS` or
`PREMIUM`; `edge/waf.tf`, `edge/plan.tf`), which bundles its cost. On pay-as-you-go it would bill
per request and per rule between events, breaking N1.

| Rule | Paths | Action |
|---|---|---|
| Verify-page challenge | `/_wr/verify.html` | Challenge. Issues `aws-waf-token` and returns the visitor to the waiting page |
| Token required | `/_wr/v1/join`, `/_wr/v1/status`, `/_wr/v1/queue_num`, `/_wr/v1/generate_token` | Challenge: a 202 with `x-amzn-waf-action`, which `waiting.js` treats as "renew on the verify page" |
| Joins per token | `/_wr/v1/join` | Block (429) past 10 per 10 min |
| Requests per token | the four JS API paths | Block (429) past `waf_api_token_limit` per 5 min |
| Token requests + joins per IP | verify page, `/_wr/v1/join` | CAPTCHA past `waf_join_ip_limit` per 5 min. It escalates rather than blocks, because carrier NAT puts many real buyers behind one IP |
| Anonymous / hosting IP | verify page, join | CAPTCHA (Count until promoted) |
| Token cookie required | the four JS API paths | Block a request with no `aws-waf-token` cookie, so a token sent only by header cannot escape the per-token limits. Count when `waf_require_token_cookie = false` |
| No-JS joins per IP | `/_wr/v1/enter` | Block past `waf_nojs_ip_limit` per 5 min |
| No-JS waits per IP | `/_wr/v1/wait` | Block (429) past `waf_nojs_wait_ip_limit` per 5 min |
| Anonymous IP, no JS | `/_wr/v1/enter` | Block (Count until promoted) |
| No-JS closed | `/_wr/v1/enter`, `/_wr/v1/wait` | Block, only when `nojs_enabled = false` |
| Managed: Anti-DDoS, IP reputation, Bot Control common | ACL-wide or the room's paths | Count until `waf_managed_rules_mode = "enforce"` (O5, [ADR-0012](adr/0012-anti-ddos-count-mode.md)) |

Every path match decodes and normalises the URI first (`URL_DECODE`, `NORMALIZE_PATH`), so an
encoded path does not slip past a rule. The no-JavaScript paths and `waiting.html` are never challenged; a Terraform test fails if
`waiting.js` calls a `/v1` path the token rule misses. This raises the price of a place to one
browser session; it does not bind a place to a person.

## 9. Operator surface

| Capability | Mechanism |
|---|---|
| Alerting | Metric filters over the Lambdas' structured logs, each with an alarm at threshold zero, plus metric alarms on DLQ depth, join-queue age, each function's `Errors` and `Throttles`, table write throttles, API 5XX and the controller schedule's target errors (`core/logging.tf`). They notify the SNS topic in `alarm_sns_topic_arn` when one is set, and nobody otherwise. No dashboard resource ships |
| Pre-event readiness | The dashboard's diagnostics panel, which makes read-only checks against the deployment (issue #70, [`RUNBOOK.md`](./RUNBOOK.md)) |
| Branding | Waiting-page HTML, CSS and JS in S3 behind CloudFront |
| Messaging | `/_wr/admin/message` makes one `UpdateItem`, and every waiter sees it within one `/_wr/v1/status` TTL |
| Position and wait | `/_wr/v1/queue_num` gives the position. The client estimates the wait from the measured movement of the cursor, falling back to `origin_concurrency / session_seconds` |

`waiting.js` polls adaptively (ADR-0023): the interval scales with the visitor's distance to the
front, between a floor and a ceiling set in Terraform (`poll_floor_ms`, `poll_ceiling_ms`,
`poll_divisor`). A visitor near the front sees a pause or a fail-open within the floor. A
deep-queue visitor can see it up to about 39 s late (the ceiling plus jitter). The O4 runbook
accepts that delay. Consecutive failed polls, and consecutive 429s or 5xx from a redemption, double
the interval up to the ceiling and the first success resets it
([#251](https://github.com/smoketurner/virtual-waiting-room/issues/251)). A 410 from
`/_wr/v1/generate_token` drops the stored identity, so the next load takes a new place (#237).

**Not built:** live metrics for F5.1 (queue depth, admitted, no-show rate). See [`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md),
Phase 1h.

## 10. Failure behaviour

**Fail-open** ([ADR-0009](adr/0009-fail-open.md)): `/_wr/admin/fail_open` sets `fail_open_until` on
`Counters` and in the KeyValueStore, and the gate passes protected requests until its own clock
passes it. The gate also passes through when it throws before a rule has matched (unreadable
config, unknown version); a throw after a match refuses (#242).
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
  between stacks ([ADR-0008](adr/0008-partition-isolation-not-shuffle-sharding.md)). To run
  another event on the same stack, change `event_id` and apply: Terraform re-seeds the event item
  and recreates the `PreQueue` and `Positions` tables, whose rows carry no event
  ([#248](https://github.com/smoketurner/virtual-waiting-room/issues/248)). Within a deployment,
  `assign_position` and `nojs` have reserved concurrency so that ingest cannot drain the account
  pool the control plane uses, and the controller and `open_event` have their own so they are
  never starved by it.
- **Least privilege.** Each function's `UpdateItem` on `Counters` is scoped by leading key to the
  items it writes: the event item, the pre-queue shards or the arrivals shards
  ([#250](https://github.com/smoketurner/virtual-waiting-room/issues/250)). The admin's
  read of the distribution's configuration, for the readiness panel, is granted by the `edge`
  module on its own distribution, since `core` cannot name a distribution built from its outputs.
- **Error caching.** The distribution caches a 404, 429 or 5xx for one second, not CloudFront's
  default ten, so one failed poll does not pin an error on a whole edge location.
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
- **Measured, the saving is smaller.** The ratios against the fixed-interval baseline were
  measured with the load program at commit `788bf62`, since removed: 4.60x on `/status` alone
  against 4.27x modelled, and 2.66x (late) to 2.88x (uniform) end to end at 833/s with a 180 s
  countdown, which puts adaptive at ~75–81M.
- **A whole sale, measured.** `make sim` (all visitors in the foreground) with 500,000 visitors,
  a 30-minute late-arrival countdown, 1,000 at once for 15 minutes and 20,000 tickets: 7,997
  buyers sold out 2.3 h after the first arrival, and the edge answered 103M `/status` polls and
  about 2.7M other requests, ~210 a visitor, inside Business's 125M. The origin saw about one
  `/status` fetch a second throughout. A quarter of the crowd (115,851) joined behind the cohort:
  the last minute's joins exceed API Gateway's 7,000/s join limit (203,292 refusals), and a retry
  accepted after the open is a live join.
- **No T−0 herd.** Open discovery spreads over the ~39 s ceiling jitter plus `scheduleFirstAsk`'s
  window of up to 120 s, so peak `/_wr/v1/queue_num` load is no higher than that spread alone:
  about 8,500/s at a million, under API Gateway's 10,000/s default
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

### Load at a million

What a 1,000,000-visitor scheduled event asks of API Gateway and DynamoDB. **Modelled from the
per-request costs in the code, not measured**: the arrival model above (a 60-minute countdown with
arrival fraction `u^0.2`, so joins peak at `5N / 3600` at T), and admission at `origin_concurrency`
per session length (§7).

| Phase | API Gateway | DynamoDB writes | DynamoDB reads |
|---|---|---|---|
| Countdown, 60 min | `/_wr/v1/join` ~280/s average, ~1,400/s peak at T. `/_wr/v1/status` origin fetches are bounded by CloudFront cache nodes, not audience: tens to low hundreds a second | ~1 unit per join on `PreQueue`, plus one shard-counter write per batch of up to 100: ~1,400/s peak | ~1 unit per join (the batch's consistent duplicate check): ~1,400/s peak |
| The open | one invoke | one conditional write | ten shard counters a second while the queue drains, then once |
| T to about T+160 s | 1,000,000 `/_wr/v1/queue_num`, ~8,500/s peak | none | one eventually consistent `PreQueue` read each: ~4,300 units/s peak |
| Admission | `/_wr/v1/generate_token` at `origin_concurrency / session_ttl_seconds` a second, plus the refusals of those whose turn has come | 2 units per admission (the shard take, the `Positions` mark) | ~1.5 units per admission (`Positions` consistent, `PreQueue` eventual; `Counters` is held per environment) |
| Controller, per pass | none | 12 (the cursor, the log, ten caps) | ~12 units (the event item, the log, ten shards) |

- **Most of a million are not admitted.** At the 900 s session a limit of 100 admits 400 an hour,
  and a limit of 10,000 admits 40,000. Everyone further back keeps a position and polls a cached
  `/_wr/v1/status`, which costs the origin nothing and CloudFront a request per poll; the page's
  wait estimate is what tells them.

- **Joins scale with the shape of arrival, not with this model.** Everyone in the last five
  minutes is ~3,300/s; everyone in the last minute is ~16,700/s, above API Gateway's 10,000/s
  default account limit and above the ~4,000 writes/s an un-warmed table serves. The table's
  ceiling delays the open, which waits for the backlog (§4.1); the account limit refuses joins,
  and one accepted only after the open is behind the cohort.
- **The position lookup is the largest request peak**, and the client's spread sets it: the
  first ask is spread over up to 120 s. At 60 s the same cohort peaked at ~16,600/s.
- **A spent budget costs no writes.** A refused `generate_token` reads two per-visitor rows and
  nothing shared (§7), so the admission rows above do not grow with the number of visitors asking.
- **The per-key ceilings that matter** are the ten arrivals shards, 1,000 writes/s each, so
  10,000 admissions/s, and the event's own item, which takes one `queue_counter` claim per ingest
  batch and one cursor write per pass.
- Nothing here has been run against a deployment; the load tests in
  [`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md) are still open.

### Worst case: nobody comes early

The model above assumes people arrive during the countdown. If nobody does, the open writes a
cohort of zero and the event is a live-join queue from its first second: all 1,000,000 visitors
post `/_wr/v1/join` when they arrive. Run on the [local stack](LOCAL_STACK.md#findings) at
one-tenth scale (100,000 visitors within 10 s of T, every API Gateway limit at a tenth), which keeps
the crowd's size relative to the throttles:

| | Measured, default quotas at one-tenth scale |
|---|---|
| Half the crowd has a position by | 2.7 min |
| 90% / 99% have a position by | 4.4 / 6.3 min |
| Join requests answered 429 | 3.4 per visitor |
| Positions | Nobody lost, no duplicate position |

Raising the account's API Gateway quota and the join throttle shortens the first two rows in
proportion.

- **The ceiling is API Gateway, and it is a rate, not a capacity.** The join path has no compute
  to exhaust, so the crowd is taken at the join throttle (7,000/s by default, kept under the
  account's 10,000/s so admission, lookups and the console are not starved alongside it) and
  everyone else is told 429. A million people need at least 143 s at that rate.
- **Nobody is turned away.** A 429 never counts towards the page giving up; the retry backs off
  with jitter so the gateway is not idle between waves. No position is asked for until a join has
  been accepted, or the lookups, which the edge cannot collapse, outnumber the joins they wait on.
- **Order is arrival order only loosely.** Within such a burst a place is decided by which retry
  got through, not by who clicked first. The pre-queue exists to avoid exactly this: it randomizes
  deliberately instead of by accident.
- **Admission is unaffected**: it fills the origin's slots from the first pass, from whoever has a
  position by then.
- To take such a crowd faster, raise the account's API Gateway quota and
  `api_join_throttle_rate_limit` with it.

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
