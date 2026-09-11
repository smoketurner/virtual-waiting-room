# Architecture

A virtual waiting room holds visitors during a traffic spike and releases them into an origin at
a rate the origin can sustain. This document describes what the deployed system does, read from
`crates/` and `infra/`.

Where [`DESIGN.md`](./DESIGN.md) and [`REQUIREMENTS.md`](./REQUIREMENTS.md) describe something the
code does not do, §10 lists the difference. [`DYNAMODB.md`](./DYNAMODB.md) covers the data layer.

---

## 1. Summary

The system deploys into the customer's own Amazon Web Services (AWS) account. It runs no
always-on compute and no cache tier. One deployment serves one event: `event_id` is a Terraform
variable, baked into every Lambda's environment as `EVENT_ID`.

Three decisions carry the design.

**The burst never touches compute.** `POST /v1/join` is an API Gateway `AWS` service integration
that calls Simple Queue Service (SQS) `SendMessage` directly, through a Velocity template. No
function of ours runs in that path.

**Queue order is never stored.** A visitor's position is the output of a keyed permutation applied
to their registration index. One conditional `UpdateItem` assigns positions to the whole
pre-queue cohort. The permutation is a 4-round Feistel network keyed by a 256-bit seed, and its
byte encoding is pinned by frozen test vectors.

**The gate is the content delivery network (CDN).** The CloudFront default cache behaviour names
a trusted key group. CloudFront verifies signed cookies at the edge and refuses an un-admitted
visitor before the origin receives the request. No code of ours runs in that path either.

The core Terraform module holds 65 managed resources.

---

## 2. The moving parts

Six Lambda functions, four DynamoDB tables, one SQS queue with a dead-letter queue, one REST
API, one CloudFront distribution.

| Function | Trigger | Timeout | What it does |
|---|---|---|---|
| `assign_position` | SQS event source mapping | 30 s | Consumes join batches; claims indices or positions; writes rows |
| `seal_event` | EventBridge Scheduler, one-shot | 10 s | Folds shard counts into offsets; writes the seal |
| `read` | API Gateway | 10 s | Serves `GET /v1/status` and `GET /v1/queue_num` |
| `controller` | EventBridge Scheduler, `rate(1 minute)` | 30 s | Meters admission; expires positions |
| `generate_token` | API Gateway | 30 s | Checks the position; records the arrival; signs the cookies |
| `admin` | API Gateway | 10 s | Axum operator UI and control plane |

Every function runs `provided.al2023` at 256 MB on the architecture named in
`terraform.tfvars`. Every function deploys a real build. There is no placeholder artifact and no
fallback path.

`authorizer` is a seventh crate with its own Terraform module. It is an origin-side gate for a
customer who controls their origin. It is built and deployable. Nothing in the CloudFront path
invokes it.

---

## 3. Ingest

```
POST /v1/join
  → API Gateway REST, regional
      request validator: JSON Schema "JoinRequest"
        required: request_id, event_id
        additionalProperties: false
        request_id maxLength 36
      integration type AWS, credentials: an IAM role
        Action=SendMessage&MessageBody=$util.urlEncode($input.body)
  → SQS standard queue
      visibility timeout 181 s, maxReceiveCount 5
      DLQ retention 14 days
  → assign_position
      BatchSize 100, MaximumBatchingWindowInSeconds 1
      FunctionResponseTypes: [ReportBatchItemFailures]
```

The event source mapping is `enabled = true` unconditionally. The comment above it claims it is
enabled only for a real artifact, which the argument beneath it contradicts.

`assign_position` validates every record twice. The gateway's JSON Schema checks that
`request_id` and `event_id` are present and that no extra field rides along. The function then
parses `request_id` as a UUID version 7 — 36 bytes, hyphens at positions 8, 13, 18 and 23, a `7`
version nibble at position 14, and a variant nibble in `8..=b` — and checks `event_id` against
its own `EVENT_ID`. A record that fails either check, or belongs to another event, is returned as
a batch failure and never claims anything.

Each invocation makes exactly one `ADD queue_counter` call regardless of how many joins it
carries, and that counter is a single DynamoDB item with a write ceiling near 1,000 per second. At
10,000 joins per second, batching by 100 costs 100 counter writes per second. Batching by 10 costs
1,000 and sits on the ceiling.

The batch size costs latency. A batch size above 10 requires a batching window of at least one
second, and AWS documents that any window lets Lambda wait up to 20 seconds before invoking on a
quiet queue. A lone join during testing can take about 20 seconds to get a position.

---

## 4. Position assignment

### 4.1 One read decides the whole batch

`assign_position` reads the event's `Counters` item once per batch with a strongly consistent
`GetItem`. Every valid record in that batch then takes the same path:

```rust
let live_path = counters
    .as_ref()
    .is_none_or(|c| c.sealed().is_some() || c.phase != Phase::PreQueue);
```

The branch is on the seal outputs, never on the phase alone. An operator can walk the phase back
to `pre_queue` after a seal without unsealing the index space, and a record arriving then is
still a live join. A missing `Counters` item takes the live path.

### 4.2 Pre-queue registration claims an index, not a position

Each record hashes to one of ten shards with FNV-1a over the request id bytes, reduced modulo 10.
The function groups the batch by shard and issues one `SET s = :shard ADD n :count` with
`ALL_NEW` per non-empty shard group. That claims a whole group's local indices in one round trip.
It then writes one `PreQueue` row per record: `{r, s, l, t}` — request id, shard, local index,
registration time.

Hashing rather than round-robin makes a retry safe. A retried join lands on the same shard, the
`attribute_not_exists(r)` guard rejects the row, and the burned index belongs to whichever
invocation wrote the authoritative row.

The block's first local index is `checked_sub`, not saturating. A saturated pre-queue index
would be a duplicate global index that hands two visitors the same position, where a saturated
live-join position would only be a gap.

### 4.3 The seal writes five values in one guarded update

```
SET shuffle_seed = :seed, participant_count = :n, queue_counter = :n,
    prequeue_offsets = :offsets, phase = :active
ConditionExpression: attribute_not_exists(shuffle_seed)
```

`seal_event` first gathers the ten shard counts with one strongly consistent `BatchGetItem`, then
folds them: `offset[s]` is the sum of counts `0..s`, and `N` is the total. An unprocessed key
aborts the seal rather than under-counting the cohort. A shard with no registrations has no item
at all, and `BatchGetItem` returns nothing for it, so the fold counts it as zero. A shard item
that exists but whose index cannot be read raises an error, because zeroing it silently would
unadmit every registrant in it.

A double-fire or a retry finds the seed present, fails the condition, and returns `AlreadySealed`
without changing anything.

`queue_counter = :n` rides in the same update because a separate write could be lost between the
seal and the first live join.

### 4.4 Position is computed on read

```
i        = prequeue_offsets[s] + l
position = PRP(shuffle_seed, i, participant_count)
```

`PRP` is a 4-round balanced Feistel network over the smallest power of four at or above `N`, with
cycle-walking to restrict the output to `[0, N)`. The round function is
`F(r, x) = be_u32(HMAC-SHA256(seed, r ‖ x)[0..4]) & mask`, with a 5-byte message: the round as one
byte, then the right half as a 4-byte big-endian integer. `aws-lc-rs` supplies the HMAC.

A third party recomputing a position has to reproduce these bytes exactly, so two tests pin them.
`frozen_wire_encoding_vectors` pins 14 `(seed, N) → [(index, position)]` cases, including
`prp(SEED_COUNTING, 0, 1_000_000) == 890_568`. `frozen_round_function_encoding` pins three raw
round-function outputs separately, so a failure separates a change in one round from a change in
the network around it. The vectors were computed from a separate implementation of the documented
encoding, not captured from this code.

Four rounds is fewer than the ten that NIST Special Publication 800-38G specifies for FF1. The
seed is not a long-term secret here. It does not exist before the seal, so nobody can predict
their position; after the seal it is published so a third party can recompute the ordering. The
construction has to be bijective and uniform, and both are tested directly.

`prp` returns `i` unchanged when `i >= n` or `n <= 1`. It is never evaluated outside its domain.

### 4.5 Live joins

One `ADD queue_counter :n` with `ALL_NEW` claims a block for the whole valid set. The block is
`[end - n + 1, end]`, computed with saturating arithmetic because the release profile has no
overflow checks. Each row is then written with `attribute_not_exists(request_id)`.

Because the block starts at `end - n + 1` and the seal leaves `queue_counter` at `N`, the first
live joiner after a seal receives position `N + 1`. Position `N` is never issued. The pre-queue
cohort holds `[0, N)`, live joins hold `[N + 1, …)`, and the single position between them is a
permitted gap.

### 4.6 A join that races the seal

A shard claim can land after `seal_event` read that shard's count. The row is then invisible to
the seal, and `SealedOffsets::assign` resolves it to `Assignment::LiveJoin`.

The test is per shard, against `offset[s+1] - offset[s]`, never against a global `i >= N`. A
global test would let an over-count on an interior shard reconstruct into an index range a later
shard legitimately owns, and two visitors would hold the same position.

`assign_position` handles this itself. After its pre-queue writes land, it re-reads `Counters`
consistently. Every row this invocation actually wrote — never a duplicate, whose authoritative
row belongs to another invocation — that now resolves to `LiveJoin` gets a real live position from
the same `queue_counter`. Every failure inside that fix-up is swallowed rather than reported as a
batch failure, because redelivery would take the live path and mint a second position for a
visitor already counted into the cohort.

One case is left open. A pre-queue write that times out on the caller side but actually lands is
indistinguishable from one that failed. It is reported as a batch failure, redelivery finds the
event sealed, and the visitor ends up with a `Positions` row as well as a counted `PreQueue` row.
Closing it costs a `PreQueue` `GetItem` on every live join.

---

## 5. Waiting

### 5.1 What `/v1/status` returns

```json
{
  "event_id": "...",
  "phase": "active",
  "serving_state": "running",
  "serving_position": 41234,
  "participant_count": 1000000,
  "prequeue_offsets": [0, 99873, ...],
  "message": "...",
  "target_rate": 500
}
```

`serving_state` is derived from `(phase, admission_control)` and never stored, so it cannot drift.
`target_rate` is **visitors per second**. `message`, `participant_count`, `prequeue_offsets` and
`target_rate` are omitted when unset.

The seed is **not** published here. A client cannot compute its own position; it asks
`/v1/queue_num`.

### 5.2 The client asks for its number once

`waiting.js` polls every 5,000 milliseconds plus up to 1,500 milliseconds of jitter. Each tick
fetches `/v1/status`. It fetches `/v1/queue_num` only while it does not yet know its position.

A position never changes once known. A pre-queue registrant's comes from the sealed permutation
and a live joiner's from a claimed row. Only the cursor moves, and `/v1/status` carries it.

`/v1/status` is cached with a path-only key and no cookies forwarded, so CloudFront collapses
concurrent misses into one origin fetch. `/v1/queue_num` is keyed on `event_id` and `request_id`,
so it cannot collapse — and it is asked once per visitor rather than once per poll. Origin load
stays flat as the room grows.

Everyone learns their number at the same moment, so the client waits a random slice of
`min(60_000, participants / 5000 × 1000)` milliseconds before asking. At 1,000,000 participants
the spread saturates at 60 seconds, which is about 16,700 requests per second — above the default
API Gateway account throttle of 10,000.

A 404 from `/v1/queue_num` means the row has not landed. The client counts eight consecutive
misses before dropping its joined flag and re-joining with the same request id. Eight is chosen to
clear the event source mapping's 20-second worst case, not its typical one.

---

## 6. Admission

### 6.1 The controller

`EventBridge Scheduler` fires `rate(1 minute)` against the universal
`arn:aws:scheduler:::aws-sdk:lambda:invoke` target with `InvocationType: Event` and a qualified
function name. The templated Lambda target invokes synchronously, which holds the invocation open
through the waits and bills the full minute of cadence. A durable function cannot be invoked
through an unqualified name, because an execution is pinned to the version that started it.

Each execution runs six passes ten seconds apart. The gaps are durable waits, so the execution
suspends rather than holding an invocation open. `durable_config` sets `execution_timeout = 120`
and `retention_period = 7`.

The schedule is created unconditionally. A controller nothing fires leaves the queue to form and
never drain.

A pass runs only when `phase == Active` **and** `admission_control == Open`. Any other admission
control returns the whole pass, so neither the cursor nor the expiry advances — a visitor cannot
lose a position to expiry during a hold they had no way to act through.

```
observed  = sum(arrivals) − last_arrivals_total
released  = serving_counter − last_serving_counter
no_show   = 1 − clamp(observed / released, 0, 1)
smoothed  = 0.3 × no_show + 0.7 × previous          // EWMA
release   = min(target_rate × 10 / (1 − smoothed), target_rate × 10 × 2)
```

With nothing released last interval there is nothing to measure, so the release falls back to the
raw target and the smoothed state is carried unchanged. Every subtraction saturates: arrivals and
the cursor come from separate updates and can read below the stored baseline.

The cursor is then clamped to `queue_counter + 1` and forced monotonic:

```rust
let ceiling = inputs.queue_counter.saturating_add(1);
let next = inputs.serving_counter
    .saturating_add(release)
    .min(ceiling)
    .max(inputs.serving_counter);
```

Advancing past the end of the line banks admission credit against an empty queue, and the next
burst walks straight through every position already released. The decision reports the distance
the cursor moved, not the distance requested, so the next interval measures arrivals against the
people who were let through.

The write is guarded on `serving_counter = :expected`, so two overlapping executions cannot
double-advance. A failed condition is logged and skipped, not retried.

### 6.2 Expiry

Grace is expressed in time and applied positionally. The cutoff is
`serving_counter − target_rate × 120`, because at `target_rate` per second the cursor covers that
many positions in 120 seconds. This needs no per-position write when a position is reached, and it
stops on its own when admission is paused, because a paused cursor does not move. A target rate of
zero yields a cutoff of zero and expires nothing.

`Positions` has no secondary index, so finding those rows is a `Scan` filtered on
`queue_position < :cutoff AND #s = :issued`. Each row is then marked expired with an
`UpdateItem` guarded on it still being `issued`, one round trip at a time. `max_expired_position`
advances to the highest position expired — the highest position, not a count, because the
attribute names a position.

### 6.3 Minting the cookies

`POST /v1/generate_token` takes `request_id` from the query string or the JSON body. It reads
`Positions` first; a live-join row is authoritative and the `PreQueue` lookup is skipped when one
answers.

`decide` refuses in this order: admission control not `Open`, phase not `Active`, no registration,
position not yet reached. A `Positions` row whose status is `completed`, `abandoned` or `expired`
is `Spent` — permanent, so the client stops rather than keeps polling. The cursor is exclusive:
`position >= serving_counter` means still queued.

Refusals map to statuses the waiting page acts on: 425 still queued, 409 not admitting or not
sealed, 404 not registered, 410 spent, 500 corrupt.

The arrival is recorded before the cookies are signed. A visitor counted but not admitted
understates the no-show rate. One admitted but not counted makes the controller over-release for
every later interval. A failed arrival write is logged at error under the stable event name
`arrival_record_failed` and the visitor is admitted anyway.

The cookies are a CloudFront **custom** policy over `https://*`, signed RSA PKCS#1 v1.5 over
SHA-256, defaulting to a one-hour lifetime:

```
CloudFront-Policy=<base64>; Path=/; Max-Age=3600; Secure; HttpOnly; SameSite=Lax
CloudFront-Signature=<base64>; ...
CloudFront-Key-Pair-Id=<id>; ...
CloudFront-Hash-Algorithm=SHA256; ...
```

The base64 is CloudFront's variant, with `+/=` replaced by `-~_`.
`CloudFront-Hash-Algorithm=SHA256` must be sent or CloudFront assumes SHA-1 and rejects every
signature. `Path=/` with no `Domain` must be used or the browser never sends the cookies back on
the protected request.

The signing key is read from SSM Parameter Store once at cold start, inside the boosted init
phase, so neither the TLS handshake nor the RSA key parse lands on a visitor's request.

---

## 7. The gate

The CloudFront default cache behaviour names `trusted_key_groups`. CloudFront verifies the signed
cookies itself on every request to the protected origin. Caching is disabled there and an origin
request policy forwards the session cookie, all viewer headers, and all query strings.

A refused visitor gets a 403, which a distribution-wide `custom_error_response` turns into:

```hcl
custom_error_response {
  error_code            = 403
  response_code         = 200
  response_page_path    = "/_wr/waiting.html"
  error_caching_min_ttl = 0
}
```

The waiting page is the correct answer to "you are not admitted yet", and a 403 body keeps
browsers from rendering it as a normal page. `error_caching_min_ttl` must stay 0: CloudFront
caches its own error responses, and a cached refusal keeps showing the waiting page to a visitor
who has since been admitted.

`/_wr/*` is its own behaviour against a private S3 bucket, outside the gate. Gating it would make
the refusal loop.

The client handles the loop that remains. A visitor holding cookies CloudFront refuses would
bounce between the origin and the waiting page, so `waiting.js` counts bounces, retries the same
pass up to five times, and then stops with an explanation rather than cycling.

### The cache behaviours

| Behaviour | Path | Cache policy | Cookies | Origin |
|---|---|---|---|---|
| Default | `/*` | CachingDisabled | session cookie forwarded | Protected origin, trusted key group |
| Waiting page | `/_wr/*` | Cached | none | S3, ungated |
| Polled | `/v1/status` | Min TTL 1 s, key: path | none | API Gateway |
| Polled | `/v1/queue_num` | Min TTL 1 s, key: path + `event_id` + `request_id` | none | API Gateway |
| Write | `/v1/join`, `/v1/generate_token` | CachingDisabled | AllViewerExceptHostHeader | API Gateway |
| Admin | `/admin`, `/admin/*`, `/static/*` | CachingDisabled | AllViewerExceptHostHeader | API Gateway |

`polled_min_ttl_seconds` is validated `> 0`. A minimum TTL of zero, or any forwarded cookie,
disables request collapsing and every poll reaches the origin.

CloudFront strips `Set-Cookie` response headers from a behaviour that forwards no cookies.
`/v1/generate_token`'s whole job is to return cookies, so the write behaviours carry an origin
request policy. Without it the visitor is admitted, receives nothing, and waits forever.

---

## 8. The operator surface

The admin Lambda serves an Axum router behind an API Gateway greedy proxy:

| Route | Method | State |
|---|---|---|
| `/admin` | GET | Dashboard |
| `/admin/state` | GET | JSON state |
| `/admin/login`, `/admin/callback`, `/admin/logout` | GET | OIDC Authorization Code with PKCE |
| `/admin/phase` | POST | Phase transition, guarded on the expected prior phase |
| `/admin/rate` | POST | Target rate, guarded on the expected prior rate |
| `/admin/message` | POST | Operator broadcast |
| `/admin/reset` | POST | Reset event state |
| `/admin/pause`, `/admin/resume` | POST | Admission control transitions |
| `/admin/rules` | POST | 501 Not Implemented |
| `/update_session` | POST | 501 Not Implemented |
| `/metrics` | GET | Routed by API Gateway, **no handler in the router** |

API Gateway authorization is `NONE` on every admin route. The Lambda enforces access with an OIDC
login session in DynamoDB and an email allowlist from `OIDC_ALLOWED_EMAILS`. An unset allowlist
denies everyone.

Every mutation writes four audit attributes alongside the change — `last_action`,
`last_action_by`, `last_action_at`, `last_action_epoch_ms` — in the same `UpdateItem`.

Every mutation is guarded twice. An optimistic-concurrency guard on the expected prior value makes
a transition another operator already applied a 409 rather than a second apply. A 2,000-millisecond
debounce guard on `last_action_epoch_ms` makes a double-submitted form a no-op. Forcing maintenance
mode is guarded on the expected phase but **not** debounced. An emergency stop must always apply.

Rendering is askama compile-time templates with design-system tokens as plain CSS. There is no
React, no bundler, and no runtime npm dependency.

---

## 9. What the tests prove

Read from the test bodies.

| Property | Test | What it asserts |
|---|---|---|
| Wire encoding is frozen | `frozen_wire_encoding_vectors` | 14 `(seed, N)` cases, 2 seeds, N from 2 to 1,000,000, computed from an independent implementation |
| Round function is frozen | `frozen_round_function_encoding` | Three raw big-endian words, unmasked, so a byte-order change cannot hide |
| Bijective | `prop_bijective` | Arbitrary seed, `n` in `1..2000`: the sorted image set equals `[0, n)` exactly |
| Bijective, fixed cases | `bijective_over_small_domains` | `n` in {2, 3, 5, 8, 15, 16, 100, 256, 999, 1000} |
| In domain | `prop_in_domain` | Arbitrary seed, `n` and `i` up to 100,000 |
| Uniform | `prop_uniform_by_decile` | At `n = 10,000`, χ² across 10 deciles stays below 27.88 — the p = 0.001 critical value at 9 degrees of freedom |
| Shards are distinct keys | `shard_keys_are_distinct_partition_keys` | Ten distinct partition keys; pre-queue and arrival families disjoint |
| No expression inlines a `#` | `no_expression_inlines_an_attribute_name_containing_a_hash` | Pins the bug that broke every arrival write |
| Token kinds do not collide | `the_tokens_table_key_space_does_not_collide_across_kinds` | `TKN#`, `SESS#`, `PKCE#` are three rows |
| Seal starts the live sequence | `seal_starts_the_live_join_sequence_at_the_cohort_size` | `queue_counter = :n` is in the seal update |
| Release is bounded | `near_total_no_show_is_bounded_at_cap`, `zero_arrivals_is_bounded_at_cap_not_infinite` | A no-show rate at 1.0 yields the cap, not a division by zero |

Two figures that appear in `DESIGN.md` have no test behind them: a 200,000-sample bijectivity run
at N = 1,000,000, and "χ² = 0.0 against a 16.9 critical value at p = 0.05". The committed
uniformity test uses a different threshold and does not assert a specific χ².

---

## 10. Where the code and the narrative documents disagree

| `DESIGN.md` says | The code does |
|---|---|
| `/status` publishes `shuffle_seed` after the seal | `StatusResponse` has no seed field. Only `/v1/queue_num` resolves a position |
| `target_rate` is admissions per minute | `target_rate` is visitors per **second**; the interval target is `rate × 10` |
| `/queue_pos_expiry` and `/public_key` exist, unrouted | Neither is declared. An endpoint with no implementation is not declared at all |
| WAF with Bot Control and ASN matching is deployed by default | `modules/edge` creates no WAF |
| Each event gets its own SQS queue and reserved concurrency | One queue, one event per deployment, no `reserved_concurrent_executions` anywhere |
| An empty artifact path leaves a placeholder binary | Every artifact path points at a real build |
| The join event source mapping is enabled when the artifact is real | `enabled = true`, unconditional. The comment above it is stale |
| The controller schedule is created when the controller is | Always created |
| The operator message attribute is `operator_message` | The attribute is `message` |
| `GET /metrics` returns event metrics | API Gateway routes it to the admin Lambda, whose router has no `/metrics` handler |
| A refused visitor gets a 403 mapped to the waiting page | The mapping rewrites the status to **200** |

---

## 11. Known gaps in the code

**The gate verifies, it does not decide.** CloudFront checks a signature. It cannot be told to
stand down when the backend is unreachable ([#58](https://github.com/smoketurner/virtual-waiting-room/issues/58))
or to pass traffic through while the event is dormant ([#60](https://github.com/smoketurner/virtual-waiting-room/issues/60)).
`AdmissionControl::FailOpen` is a storable state that holds the controller, but nothing enforces
it at the edge.

**The cookies are unbound bearer credentials.** The policy resource is `https://*` with no visitor
binding ([#61](https://github.com/smoketurner/virtual-waiting-room/issues/61)) and no revocation
([#63](https://github.com/smoketurner/virtual-waiting-room/issues/63)). `generate_token` takes a
request id from a query string and authenticates nothing else, so anyone holding a request id can
mint a set.

**`generate_token` is replayable and inflates the arrival count.** It never marks a position
spent. A visitor who calls it twice records two arrivals against one release. That understates the
no-show rate, so the controller under-releases — the safe direction, but the measurement is wrong.

**Standby mode is not implemented.** There is no inflow alarm, no dormant state, and no automatic
phase transition. `Phase::Idle` and `Phase::PostEvent` exist and an operator sets them by hand.

**Protection rules are not wired.** `authorizer` implements `ProtectionRule` matching on path,
header, cookie and user agent. `/admin/rules` returns 501, and the CloudFront path has no rule
evaluation at all.

**Sessions cannot be completed or abandoned.** `PositionStatus` has `Completed` and `Abandoned`
variants that only the controller's expiry path and `generate_token`'s refusal ever read.
`/update_session` returns 501, so nothing writes them.

[ADR-0021](adr/0021-edge-function-gate.md) is accepted and addresses the first two by replacing the
key-group gate with a CloudFront Function that decides at the edge. `infra/` contains no CloudFront
Function and no KeyValueStore, so none of it is deployed.

---

## 12. Frequently asked questions

**Why not store the shuffled order?**
Writing a million rows at the scheduled start is not atomic. There would be an interval in which
some participants hold positions and others do not. The seal is one conditional `UpdateItem`, so a
reader sees either the unsealed state or all five values together.

**Why DynamoDB rather than a cache tier?**
A cache tier is always-on compute, and idle cost is the constraint the whole design is built
around. `UpdateItem` with `ADD` and `ReturnValues: ALL_NEW` serializes per item and returns each
value once, which is the property a position sequence needs.

**Why a REST API rather than an HTTP API?**
REST supports request validators, which reject a malformed body with 400 before anything
downstream runs, and the `AWS` service integration to SQS `SendMessage`, which keeps Lambda out of
the burst path. HTTP APIs support neither.

**Why is `queue_counter` not sharded when the pre-queue counter is?**
A sequence must yield a unique ordered value, and summing shards cannot produce one. The
permutation needs registration indices to be unique and inside `[0, N)`; it does not need them
ordered. The prefix offsets written at the seal reassemble ten shards into that exact range.

**Why is the straggler check per shard rather than against `N`?**
An over-count on an interior shard can reconstruct into an index that still falls inside `[0, N)`,
because that index legitimately belongs to a later shard. Resolving it as pre-queue would hand two
visitors the same position.

**Can one deployment run two events?**
No. `event_id` is a Terraform variable and every Lambda carries it as `EVENT_ID`. The key builders
are written so a second event could share the tables, and a test pins the one collision that would
cause — an `event_id` containing `#` — but nothing else in the deployment is parameterized per
event.

**What happens when the signing key is compromised?**
Everything. One per-deployment key signs the CloudFront policies, and `authorizer` uses the same
parameter for its HMAC tokens and sessions with a kind-byte domain separation. Rotation is listed
as an open decision in [`adr/README.md`](adr/README.md).

---

## Appendix — Request paths

```
join:      CloudFront [/v1/join, uncached]
             → API Gateway REST (type: aws, SQS SendMessage)
             → SQS  → assign_position → DynamoDB

poll:      CloudFront [/v1/status, Min TTL 1 s, path-only key, no cookies]
             → API Gateway → read → DynamoDB (1 s in-process cache)

number:    CloudFront [/v1/queue_num, Min TTL 1 s, keyed per visitor]
             → API Gateway → read → DynamoDB   (once per visitor)

admit:     CloudFront [/v1/generate_token, uncached, AllViewerExceptHostHeader]
             → API Gateway → generate_token
             → DynamoDB (check position, record arrival)
             → 4 Set-Cookie headers

protected: CloudFront [/*, trusted key group]
             ├─ valid cookies    → customer origin (or the demo fixture)
             └─ missing/expired  → 403 → rewritten 200 → /_wr/waiting.html

seal:      EventBridge Scheduler at(seal_start_time)   [only if set]
             → seal_event → BatchGetItem ×10 → one guarded UpdateItem

meter:     EventBridge Scheduler rate(1 minute)        [always]
             → controller, six durable passes at 10 s
             → DynamoDB (sum arrivals, advance cursor, scan and expire)
```
