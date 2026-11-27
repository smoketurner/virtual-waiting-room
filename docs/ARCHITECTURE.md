# Architecture

A virtual waiting room holds visitors during a traffic spike and lets them into an origin
no faster than the origin can serve them: never more at once than `origin_concurrency`. This document is a tour of the deployed system as built, read from
`crates/` and `infra/`. [`DESIGN.md`](./DESIGN.md) gives the design, [`DYNAMODB.md`](./DYNAMODB.md)
the data layer, and [`adr/`](./adr/) the reasoning behind each decision.

## 1. Summary

The system deploys into the customer's own AWS account. It runs no always-on compute and no
cache tier. One deployment serves one event: `event_id` is a Terraform variable, baked into each
Lambda's environment as `EVENT_ID`, except `open_event`, which takes it from the invocation payload.

Three decisions carry the design:

- **The burst never touches compute.** `POST /_wr/v1/join` is an API Gateway `AWS` service
  integration that calls SQS `SendMessage` directly. No function runs in that path.
- **Queue order is never stored.** A pre-queue visitor's position is a keyed permutation of their
  registration index, computed on read. One conditional `UpdateItem` opens the whole cohort
  ([ADR-0002](adr/0002-seeded-permutation-not-materialised-shuffle.md)).
- **The gate is a CloudFront Function.** It decides at viewer-request from a KeyValueStore and
  never calls a backend ([ADR-0021](adr/0021-edge-function-gate.md)).

## 2. The moving parts

Seven Lambda functions, four DynamoDB tables, one SQS join queue with a dead-letter queue, one
REST API, one CloudFront distribution with a CloudFront Function and its KeyValueStore, and, on a
flat-rate plan only, a WAF web ACL.

| Function | Trigger | Timeout | What it does |
|---|---|---|---|
| `assign_position` | SQS event source mapping | 30 s | Consumes join batches; claims pre-queue indices or live positions; writes rows |
| `open_event` | EventBridge Scheduler one-shot, or `/_wr/admin/open_now` | 900 s | Folds the shard counts into offsets and writes the open |
| `read` | API Gateway | 10 s | `GET /_wr/v1/status`, `GET /_wr/v1/queue_num` |
| `controller` | EventBridge Scheduler `rate(1 minute)` | 30 s | Durable function; meters admission in six 10 s passes |
| `generate_token` | API Gateway | 10 s | Checks the place in line and its turn, takes the held spot, marks the visitor admitted, signs the session cookie |
| `nojs` | API Gateway | 10 s | The queue for a visitor without JavaScript: `POST /_wr/v1/enter`, `GET /_wr/v1/wait` ([ADR-0037](adr/0037-no-javascript-queue.md)) |
| `admin` | API Gateway | 10 s | Axum operator UI and control plane |

Every function runs `provided.al2023` at 256 MB on the architecture named in `terraform.tfvars`,
and every function deploys a real build; there is no placeholder artifact. `assign_position` and
`nojs` carry reserved concurrency as a cap (`nojs` defaults to 5, so a flood of form posts
throttles there rather than in `generate_token`); `open_event` (1) and `controller` carry it as a
guarantee, so the burst cannot throttle the open or a pass.

## 3. Request paths

```
join:      CloudFront [/_wr/v1/join, uncached, forwards content-type only]
             → API Gateway REST (type: aws, SQS SendMessage) → SQS → assign_position → DynamoDB

poll:      CloudFront [/_wr/v1/status, Min TTL 1 s, path-only key, no cookies]
             → API Gateway → read → DynamoDB (1 s in-process cache)

number:    CloudFront [/_wr/v1/queue_num, Min TTL 1 s, keyed on event_id + request_id]
             → API Gateway → read → DynamoDB            (asked once per visitor)

admit:     CloudFront [/_wr/v1/generate_token, uncached, cookies + viewer address forwarded]
             → API Gateway → generate_token → DynamoDB → Set-Cookie (signed session)

no-JS:     CloudFront [/_wr/v1/enter, /_wr/v1/wait, as admit]
             → API Gateway → nojs → SQS (join) / generate_token::admit (wait)

protected: CloudFront [/*, CloudFront Function at viewer-request]
             ├─ no rule matches, or a valid session cookie → customer origin (or the demo fixture)
             └─ otherwise → 302 to /_wr/waiting.html, or 403 JSON for XHR

open:      EventBridge Scheduler at(start time)   [always created; disabled unless `starts_at` is set, or until the operator sets it]
             → open_event → wait for the draw to close (starts_at + 15 s) and the join queue to catch up
             → BatchGetItem ×10 shards → one guarded UpdateItem

meter:     EventBridge Scheduler rate(1 minute)   [always created]
             → controller, six durable passes 10 s apart → DynamoDB
```

### Cache behaviours

| Path | Cache policy | Forwarded to origin | Origin |
|---|---|---|---|
| `/*` (default) | CachingDisabled | Session cookie, all viewer headers and query strings (none for the demo fixture) | Protected origin, gated |
| `/_wr/v1/status` | Min TTL 1 s, key: path | Nothing | API Gateway |
| `/_wr/v1/queue_num` | Min TTL 1 s, key: path + `event_id` + `request_id` | Those two query strings | API Gateway |
| `/_wr/v1/join` | CachingDisabled | `content-type` | API Gateway |
| `/_wr/v1/generate_token`, `/_wr/v1/enter`, `/_wr/v1/wait` | CachingDisabled | All cookies and query strings; `content-type`, `referer`, `CloudFront-Viewer-Address` | API Gateway |
| `/_wr/admin`, `/_wr/admin/*`, `/_wr/static/*` | CachingDisabled | AllViewerExceptHostHeader | API Gateway |
| `/_wr/*` (everything else under the prefix) | Cached | Nothing | Private S3 bucket, ungated |

`polled_min_ttl_seconds` is validated `> 0`: a zero minimum TTL, or any forwarded cookie, disables
request collapsing and every poll reaches the origin
([ADR-0013](adr/0013-cache-behaviour-separation.md)). The minting paths forward cookies because
CloudFront strips `Set-Cookie` from a behaviour that forwards none. `/_wr/*` sits outside the gate
so a refusal cannot loop, and a `check` block asserts the waiting page falls under it.

`/_wr/` is the only path prefix the room occupies on the customer's hostname (issue #251); every
other path reaches the protected origin. CloudFront takes the first ordered behaviour that
matches, so `/_wr/*` is declared last: ahead of the API and console behaviours it would send their
requests to S3.

## 4. Join

API Gateway validates the body against a JSON Schema before anything runs: `request_id` (≤ 36
characters), `event_id` (≤ the length of this deployment's id) and `h`, the 43-character base64url
digest of the visitor's possession secret ([ADR-0035](adr/0035-request-id-proof-of-possession.md)),
all required, no other properties. The integration sends the body as the SQS message; a 2xx from
SQS maps to 200, anything else to a fixed 502.

The queue has a 181 s visibility timeout (6 × 30 s + the 1 s window) and moves a message to the DLQ
after 10 receives. The event source mapping uses `BatchSize` 100, a 1 s batching window and
`ReportBatchItemFailures`, with `maximum_concurrency` capped at the function's reserved
concurrency.

Batch size 100 is load-bearing: each invocation makes one `ADD queue_counter`, and that single
item's write ceiling is about 1,000/s (arithmetic in [DYNAMODB.md §3](./DYNAMODB.md#3-amortise-the-sequence-across-a-batch)).
The cost is latency on a quiet queue: any batching window lets Lambda wait up to about 20 s before
invoking, so a lone test join can take that long to get a position.

`assign_position` drops, without retry, a record whose body does not parse, whose `event_id` is
not this deployment's, or whose `request_id` is not UUID-shaped (36 characters, hyphens at 8, 13,
18, 23). Dropped records claim nothing.

## 5. Position assignment

`assign_position` reads the event's `Counters` item once per batch, strongly consistent, and every
valid record in the batch takes the same path:

| State | Path |
|---|---|
| The open has run (`shuffle_seed`, `participant_count`, `prequeue_offsets` all present), in any phase | Live join |
| Not opened, phase `pre_queue`, accepted before the draw closes | Pre-queue registration |
| Not opened, phase `pre_queue`, accepted after the draw closes | Sent back for 2 s (doubling per delivery) and counted on `EVT#{event_id}#DF#{s}` |
| Not opened, any other phase, or no `Counters` item | Every record fails and retries |

The branch is on the open outputs, not the phase: an operator can walk the phase back after an
open without undoing the index space. Refusing joins before setup matters because the open's
`SET queue_counter = N` would overwrite any earlier live increment and collide positions.

**The draw closes 15 s after `starts_at`.** Which path a join takes before the open is decided by
its `SentTimestamp`, when SQS accepted it, against `starts_at + 15 s`, not by when it is processed
([ADR-0043](adr/0043-held-turns-and-join-order.md)). A join sent back calls
`ChangeMessageVisibility` (`JOIN_QUEUE_URL`, `sqs:ChangeMessageVisibility`); ten deliveries cover
about 34 minutes, so none is dead-lettered while the open runs. A live batch is sorted by
`SentTimestamp` before its block is claimed.

**Pre-queue.** The invocation draws one shard uniformly at random (issue #59), drops duplicates
within the batch and any id that already has a `PreQueue` row, and claims a contiguous block of
local indices on that shard with one `ADD n`. Each row `{r, s, l, t, h}` is written with
`attribute_not_exists(r)`. Shard striping is [ADR-0015](adr/0015-stripe-prequeue-counter.md).

**The open.** `open_event` writes nothing before the draw closes. It then waits for the join
consumers to catch up: once a second it reads the queue's waiting count, the pre-queue shard counts
and the deferred-join counts, until twice running no more is waiting than was cleared since the
last read, for as long as its 900 s timeout allows, and opens regardless (#239;
`open_drain_timed_out`, `open_drain_unreadable`, `open_before_draw_cutoff`). It then reads the ten
shard counts with one consistent `BatchGetItem`, computes
`offset[s] = Σ counts[0..s)` and `N`, and writes seed, `N`, offsets, `queue_counter = N` and
`phase = active` in one `UpdateItem` guarded by `attribute_not_exists(shuffle_seed) AND phase =
:pre_queue`. A double-fire or retry fails the guard and reports `AlreadyOpen`. Both the schedule
and `/_wr/admin/open_now` go through this one function; `open_now` first moves `starts_at` to the
press, so the draw closes 15 s after it.

**Position on read.** `position = PRP(shuffle_seed, offset[s] + l, N)`. The PRP is a 4-round
balanced Feistel network over the smallest power of four ≥ `N`, cycle-walked into `[0, N)`, with
round function `be_u32(HMAC-SHA256(seed, r ‖ be_u32(x))[0..4]) & mask` (`aws-lc-rs`). The byte
encoding is pinned by frozen vectors computed from an independent implementation, so a third party
can recompute any position from the published seed. Four rounds is enough because the seed is not
a long-term secret: it does not exist before the open and is published after it.

**Live join.** One `ADD queue_counter :n` with `ALL_NEW` claims `[end − n + 1, end]` for the `n`
valid records, then each `Positions` row is written with `attribute_not_exists(request_id)`. The
open leaves `queue_counter` at `N`, so the first live joiner gets `N + 1`; position `N` is a
permitted gap.

**A join that races the open.** A shard claim can land after `open_event` read that shard. The row
then lies past its shard's opened count (checked per shard, never against a global `N`, or an
over-count on an interior shard would alias a later shard's range) and resolves as a live join.
After its pre-queue writes, `assign_position` re-reads `Counters` and gives every row it wrote
that now resolves that way a real live position. Failures in that fix-up are swallowed rather than
retried, because redelivery would mint a second position.

One case stays open: a `PreQueue` write that times out client-side but landed is reported as a
failure; redelivery takes the live path, and the visitor holds both a counted `PreQueue` row and a
`Positions` row.

## 6. Waiting

`GET /_wr/v1/status` returns `event_id`, `phase`, `serving_state`, `serving_position`, and, when set,
`participant_count`, `prequeue_offsets`, `shuffle_seed` (hex, only after the open), `message`,
`origin_concurrency` (how many visitors the origin takes at once), `starts_at` and `poll_policy`,
and always `session_seconds`. `serving_state` is derived from
phase and admission control on every read ([ADR-0019](adr/0019-visitor-facing-serving-state.md)).
Publishing the seed with the offsets and `N` lets anyone audit the whole ordering; a visitor's own
shard and index come from `GET /_wr/v1/queue_num`.

`waiting.js`:

- asks `/_wr/v1/queue_num` only until it knows its position, since a position never changes; every
  later tick fetches `/_wr/v1/status` alone, which CloudFront collapses;
- delays that first ask by a random slice of `min(120 s, participants / 5000 s⁻¹)` so the cohort
  does not ask at once (at 1,000,000 participants that is about 8,500 requests/s, under the
  default 10,000/s API Gateway account throttle), and asks at the moment drawn, not a poll later;
- before the open, polls at the ceiling but never sleeps past `starts_at` plus the 15 s draw
  window, jitter included, and polls at the floor once that moment has passed, so a page does not
  learn its number late behind people who joined after it;
- asks again at the floor while a redemption is answered 425, a held spot not yet written, and
  backs off, doubling to the ceiling, on a 429 or a 5xx;
- on a 410 with `reason: "turn_missed"`, drops its place, takes a new one at the back and says
  why ([ADR-0043](adr/0043-held-turns-and-join-order.md));
- polls at the floor while within `origin_concurrency` of the front, since it is called as soon
  as the next sessions end;
- polls at `clamp(floor, ceiling, secondsAhead × 1000 / divisor)` plus up to 30 % jitter, from the
  Terraform-set `poll_policy` ([ADR-0023](adr/0023-adaptive-poll-interval.md)): by default a 5 s
  floor, a 30 s ceiling and a divisor of 10. A `/status` that carries no policy gets a fixed 5 s;
- stops polling while the tab is hidden and catches up when it is visible, never faster than the
  floor;
- re-joins with the same request id after eight consecutive 404s from `/_wr/v1/queue_num`, enough to
  clear the event source mapping's 20 s worst case;
- estimates the wait from how fast it has measured `serving_position` moving, falling back to
  `origin_concurrency / session_seconds` before it has a measurement;
- treats any response carrying `x-amzn-waf-action` as "renew the WAF token on `/_wr/verify.html`",
  never as success.

## 7. Admission

### 7.1 The controller

EventBridge Scheduler fires `rate(1 minute)` through the universal `aws-sdk:lambda:invoke` target
with `InvocationType: Event` and a qualified function name; a durable execution is pinned to the
version that started it. Each execution runs six passes ten seconds apart, and each gap is a
durable wait that suspends the execution instead of billing it
([ADR-0022](adr/0022-durable-controller-cadence.md)); `execution_timeout = 60`, retention 7 days. The schedule and the
async invoke both retry zero times and drop an invoke older than 60 s, so a late start is skipped
rather than bunched with the next; the function has reserved concurrency
(`controller_reserved_concurrency`).

A pass runs only when `phase = active` and the resolved admission control is `Open`; otherwise it
writes nothing. A pass that fails is skipped (`controller_pass_failed`) and the
cadence carries on; the next pass computes the same free spots from the same state. An
execution ends at the first pass that finds the phase `idle` or `post_event`, since such an event
only becomes active through `pre_queue` and an open; that is most of the controller's cost
between events (N1), and it means an operator who goes idle → pre_queue → open inside one minute
waits for the next execution before admission starts.

The origin takes `origin_concurrency` visitors at once, its spots, and that is the most sessions
the controller lets be live ([ADR-0041](adr/0041-meter-origin-concurrency.md)). A session ends
exactly `session_ttl_seconds` after the visitor was let in, so the spots in use are the people let
in during the last session length, counted from the **admission log** `EVT#{event_id}#LOG`: the
total let in at each recent pass. Each open pass calls as many places in line as there are free
spots and holds a spot for each ([ADR-0043](adr/0043-held-turns-and-join-order.md)):

```
admitted = Σ over 26 shelves × 10 counters of n                // logged at the read's second
horizon  = now − session_ttl_seconds
in use   = admitted − log[horizon]                             // latest entry at or before it
held     = Σ calls not past their turn + 10 s, of Σ (c − n) on the call's shelf
free     = origin_concurrency − in use − held
called   = min(free, queue_counter + 1 − serving_counter)
next     = serving_counter + called                            // with a new call on the next shelf
caps     = n + an even share of called, on that shelf's ten counters
```

Spots in use plus spots held never exceed `origin_concurrency`. A spot is seen free up to one pass
(10 s) after its session ends. Each call is recorded in the event item's `calls` — its first place
in line, when its turns pass, and its shelf — in the same update as now serving, guarded on
`serving_counter = :expected`; a pass that loses the race writes neither the log nor the caps. A
turn is held for 2 minutes (`TURN_HOLD`); a call made less than 2 minutes (`OPEN_SETTLE`) after
the draw closes is held until 4 minutes after it closed, since the waiting pages spread their first
ask over those 2 minutes. A spot not taken in time stops being counted one pass (`TURN_MARGIN`,
10 s) after the turn passes and is called for the next place in line: that is how the spot of a
visitor who never came is filled. A shelf still counted is never reused; that pass calls nobody.
Lowering the limit below the spots in use calls nobody until enough sessions have ended. Throughput
is `origin_concurrency` per session length. All arithmetic saturates, and now serving never passes
`queue_counter + 1`. The log is then one `UpdateItem`, and the caps one `UpdateItem` per counter,
absolute values, so writing them twice holds nothing twice.

The controller expires nothing. A place in line lives until DynamoDB TTL reclaims its row 24 hours
after its last write, and a missed turn frees an absentee's spot
([ADR-0031](adr/0031-remove-controller-driven-expiry.md)).

### 7.2 Minting the session

`POST /_wr/v1/generate_token` takes `request_id` and the possession `secret`. `generate_token::admit`,
which `nojs` also calls, reads both `PreQueue` and `Positions`. A `PreQueue` row inside its
shard's opened count is the visitor's place whatever `Positions` holds, the same order
`/_wr/v1/queue_num` resolves in; a `Positions` row answers only for a live joiner or a straggler
([#238](https://github.com/smoketurner/virtual-waiting-room/issues/238)). It then decides:

| Check, in order | Refusal |
|---|---|
| `request_id` or `secret` missing or malformed | 400 |
| No `Positions` or `PreQueue` row | 404 |
| The secret's digest does not match the row's `h` | 403 |
| Resolved admission control not `Open`, or phase not `active` | 409 |
| Event not opened | 409 |
| A straggler row with no live position | 404 |
| Corrupt row | 500 |
| `position ≥ serving_counter` | 425, keep polling |
| Called, and the turn has passed | 410 with `reason: "turn_missed"`, take a new place at the back |
| Called, and the held spot is not written yet | 425, keep polling |
| Already admitted, and `admitted_at + session_ttl_seconds` has passed | 410, take a new place |

Admission control is resolved by `wr_common::resolve(stored, fail_open_until, now)`; `FailOpen` is
never stored. The `Counters` item is held for one second per execution environment, as `read`
holds it, so the admission path does not put a read per attempt on one partition key; a pause
takes effect up to a second late.

A visitor not yet admitted then **takes their held spot**: an `UpdateItem ADD n 1` on a counter of
their own call's shelf, conditional on `n < c`, trying each of the ten from a random one. The
environment does not ask a counter it saw empty for another second, so a counter already taken
costs no write. A failed draw or write is also "no spot" (`arrival_shard_draw_failed`,
`arrival_record_failed`): an admission that cannot be metered is not made. The `Positions` row is then marked `status = admitted`, guarded by
`attribute_not_exists(request_id) OR status = issued`, which creates a pre-queue member's row. A
visitor whose row is already admitted (reload, second tab, lost response) gets their session
again without taking a spot. It is the same visit, not a new one: the mark stamps `admitted_at`
once, every session for the row expires at `admitted_at + session_ttl_seconds`, and after that the
answer is 410 ([#237](https://github.com/smoketurner/virtual-waiting-room/issues/237)). Otherwise a
place that had reached the front could be handed around and mint full-length sessions for as long
as its row lived. A failed mark (`admission_claim_failed`) still admits; a later call takes a
second spot, which admits fewer people, never more.

The session is a compact JWS (HS256) signed under a key derived from the deployment secret with a
per-kind label ([ADR-0024](adr/0024-jws-credentials.md)), carrying the event as `aud`, the expiry,
and a keyed tag of the viewer's IPv4 address or IPv6 /64 from `CloudFront-Viewer-Address`:

```
<session_cookie_name>=<jws>; Path=/; Max-Age=<session_ttl_seconds>; Secure; HttpOnly; SameSite=Lax
```

`session_ttl_seconds` defaults to 900, and each session holds one of the origin's spots for that
long. Terraform passes it as `SESSION_TTL_SECS` to `generate_token`, `nojs`, `controller` and
`read`, which all require it, so the session the gate enforces is the one the controller counts.
The signing key is read from SSM once at cold start.
Terraform generates it (`random_bytes.signing_key`) and writes it to both SSM and the gate's
KeyValueStore in the same apply.

## 8. The gate

A `cloudfront-js-2.0` function on the default behaviour at viewer-request reads its config (`c`)
and the signing secret (`k`) from one KeyValueStore. `event_id`, the cookie name and the waiting
path are templated into its source. It first strips any inbound `x-wr-gate`/`x-wr-gate-failed`
header, then:

1. No rule in the ruleset matches → pass through. An empty ruleset is dormancy. A path rule also
   matches any decoded path with a `.`/`..` segment, a `;` or a backslash, since origins
   canonicalise those differently (#242).
2. Now is before `enforce_from` → pass, `x-wr-gate: pending`. Before `fail_open_until` → pass,
   `x-wr-gate: failopen`.
3. Missing cookie, bad signature, wrong event, expired, or — with IP binding on — a different
   network → refuse with reason `none`/`signature`/`event`/`expired`/`ip`: a 302 to
   `waiting.html?r=<reason>&next=<destination>` for navigation, 403 JSON with `x-wr-reason` for
   XHR.
4. Otherwise pass through.

A session that fails under `k` is tried under the previous key `kp`, read only then. Terraform
writes it: `kp` is the signing key's previous generation (#247).

If the function throws before a rule has matched (unreadable config, unknown config version) it
passes the request with `x-wr-gate-failed: true`. If it throws after one has — an unreadable key,
a failure while verifying or building the refusal — it answers a plain 403 with
`x-wr-reason: error` (#242). Neither is a backend fail-open: the gate makes no network calls and
cannot observe the backend. Cross-language conformance with `wr_common::crypto` and
`wr_common::rules` is checked by `infra/modules/edge/tests/*.conformance.test.js` against vectors
from `crates/wr-common/tests/vectors.rs`. The function must stay under CloudFront's 10,240-byte
limit (a precondition), with a warning past 9,000.

## 9. Edge protection

**Origin key** ([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)). Every REST method sets
`api_key_required`, and only the distribution holds the key, sent as an `x-api-key` origin custom
header. A request straight to `execute-api` is refused with 403 before any integration runs, and
is not billed. A terraform test fails if a method does not require it.

**Web ACL.** Created only when `flat_rate_plan_tier` is set; `edge/plan.tf` subscribes the
distribution, the ACL and the KeyValueStore to the plan, which bundles their cost (on pay-as-you-go
the ACL would bill between events, breaking N1). It requires a WAF token on the JavaScript API
paths (`/_wr/v1/join|status|queue_num|generate_token`), earned by a challenge on `/_wr/verify.html`;
rate-limits per token and per IP (per-IP join limits escalate to CAPTCHA); and runs the AWS
managed rules (Anti-DDoS, IP reputation, Bot Control common, anonymous IP) in Count until
`waf_managed_rules_mode = "enforce"`. The no-JavaScript paths and `waiting.html` are never
challenged; they get a strict per-IP limit, anonymous-network blocking (counted, not blocked,
until the managed rules are enforced), and `nojs_enabled = false` to close them for an event.

**No-JavaScript queue** ([ADR-0037](adr/0037-no-javascript-queue.md)). `waiting.html`'s
`<noscript>` form posts to `/_wr/v1/enter`, which mints the same UUIDv7 and possession secret the
script would, keeps them in an `HttpOnly` cookie scoped to `/_wr/v1/`, and sends the same `{request_id,
event_id, h}` message to the join queue. `/_wr/v1/wait` is a self-refreshing server-rendered page that
calls `generate_token::admit`, so there is one way a session is minted. The cookie also carries
when the join was sent: one that has not registered after 60 s is resent, up to three times, and
then the visitor is shown the form again (`nojs_join_resent`, `nojs_join_lost`, #251).

## 10. The operator surface

The admin Lambda serves an Axum router behind an API Gateway greedy proxy:

| Route | Method | Does |
|---|---|---|
| `/_wr/admin`, `/_wr/admin/state` | GET | Dashboard (with the readiness panel, whose read of the distribution the `edge` module grants on its own distribution, #250) and JSON state |
| `/_wr/admin/login`, `/_wr/admin/callback` | GET | OIDC Authorization Code + PKCE ([ADR-0016](adr/0016-admin-oidc-dynamodb-sessions.md)). The callback answers a meta-refresh page, so the `SameSite=Strict` session cookie it sets is sent on the way to the dashboard |
| `/_wr/admin/logout` | POST | Ends the session. Like every POST, needs the session's `csrf` form field |
| `/_wr/admin/phase` | POST | Phase transition from an expected prior phase; refuses `active` for an event the open has not opened, so it only returns an opened event from maintenance |
| `/_wr/admin/start_time` | POST | Sets or clears the start time and arms the open schedule |
| `/_wr/admin/open_now` | POST | Invokes `open_event` now |
| `/_wr/admin/concurrency`, `/_wr/admin/message` | POST | Origin concurrency (guarded on the prior value), operator broadcast |
| `/_wr/admin/pause`, `/_wr/admin/resume` | POST | Admission control `open` ↔ `paused` |
| `/_wr/admin/fail_open`, `/_wr/admin/recover` | POST | Sets and clears `fail_open_until`, mirrored to the KeyValueStore |
| `/_wr/admin/force_maintenance` | POST | Emergency stop: maintenance from any phase, not debounced |
| `/_wr/admin/rules` | POST | Replaces the gate's ruleset in the KeyValueStore, audited on `Counters` |
| `/_wr/admin/ip_binding` | POST | Turns session IP binding on or off ([ADR-0036](adr/0036-optional-session-ip-binding.md)) |

API Gateway authorization is `NONE` (plus the origin key); the Lambda enforces an OIDC session
stored in `Tokens` and the `OIDC_ALLOWED_EMAILS` allowlist, which denies everyone when empty, and
the apply fails if it is. Every mutation is guarded on its expected prior value (a concurrent
change is a 409) and debounced 2 s on `last_action_epoch_ms`, and writes its audit attributes in
the same `UpdateItem` ([DYNAMODB.md §5](./DYNAMODB.md#5-conditional-writes)). Rendering is askama
templates with one plain-CSS stylesheet; no React, no bundler
([ADR-0018](adr/0018-admin-vouch-design-language.md)).

## 11. Tests

[`.kiro/steering/testing.md`](../.kiro/steering/testing.md) lists the properties that must stay
proven. The core ones:

| Property | Test |
|---|---|
| Wire encoding frozen | `frozen_wire_encoding_vectors` (14 `(seed, N)` cases up to N = 1,000,000), `frozen_round_function_encoding` |
| Bijective | `prop_bijective` (arbitrary seed, `n < 2000`), `bijective_over_small_domains` |
| In domain | `prop_in_domain` |
| Uniform | `prop_uniform_by_decile` (χ² over 10 deciles at `n = 10,000`, below the p = 0.001 critical value) |
| Keys distinct | `shard_keys_are_distinct_partition_keys`, `the_tokens_table_key_space_does_not_collide_across_kinds` |
| Concurrency bounded | `no_more_visitors_are_ever_on_the_site_than_the_origin_takes`, `lowering_the_limit_below_the_spots_in_use_calls_nobody`, `a_pass_never_panics_moves_back_or_calls_past_the_limit` |
| Held turns | `spots_held_for_an_earlier_call_are_not_called_again`, `a_missed_turn_frees_its_spot_one_pass_after_the_hold`, `a_shelf_a_counted_call_still_uses_is_never_reused`, `a_call_soon_after_the_draw_closes_holds_until_every_page_knows_its_place`, `the_origin_stays_nearly_full_despite_no_shows` |
| Sessions replaced | `sessions_ending_are_replaced_at_their_rate`, `a_session_frees_its_spot_once_its_length_has_passed` |
| Gate conformance | `infra/modules/edge/tests/gate.conformance.test.js` |

## 12. Known gaps

[`tasks.md`](../.kiro/specs/virtual-waiting-room/tasks.md) is the authoritative list. The ones that
shape how the built system behaves:

- **Fail-open and standby need an operator.** Nothing sets `fail_open_until` or a ruleset
  automatically; the gate cannot see the backend (#58).
- **Nothing bounds how many places one visitor holds.** `request_id` is client-minted, so volume
  converts into share of the front (#59). The WAF token and rate limits raise the cost but do not
  bound it.
- **Sessions are bearer credentials** unless IP binding is on, and nothing revokes one (#63).
  Minting one requires the possession secret, so a leaked request id admits no one.
- **Sessions are not extended.** A visitor still on the origin when `session_ttl_seconds` lapses
  returns to the queue; set it longer than the worst realistic visit.
- **The admin Lambda can read the signing key.** Its KeyValueStore grant covers `k` as well as the
  config, and IAM cannot narrow it.
- **No completion signal** (#65). A visitor who finishes early holds their spot until the session
  ends, because nothing reports a departure.
- **GovCloud has no gate.** CloudFront Functions are not offered there (N4).

## 13. FAQ

**Why a REST API rather than an HTTP API?** Only REST supports request validators and the `AWS`
service integration to SQS, which keep malformed bodies and Lambda out of the burst path
([ADR-0005](adr/0005-rest-api-not-http-api.md)).

**Can one deployment run two events?** No. `event_id` is a deployment variable. The key builders
would let two events share tables (`event_id` may not contain `#`), but nothing else is
parameterised per event, and `Positions` rows carry no `event_id`.

**What if the signing key is compromised?** It mints admission for the whole event. Rotate by
raising `signing_key_generation` and applying. The gate goes on accepting the previous
generation as `kp`, so a single rotation keeps sessions minted under the compromised key valid
until they expire; raise it twice to drop that key at once, which sends every live session back
to the waiting page.
