# Design: Virtual Waiting Room

This is the agent-facing summary. The design narrative is
[`docs/DESIGN.md`](../../../docs/DESIGN.md) (cited as "DESIGN §n"), requirements are in
[`requirements.md`](requirements.md), build state is in [`tasks.md`](tasks.md), and rationale is
in [`docs/adr/`](../../../docs/adr/). This file names the components, the interfaces between
them, and the invariants a change must not break. It does not repeat the narrative.

## Components

| Component | Role | Narrative |
|---|---|---|
| CloudFront (`infra/modules/edge`) | Separate cache behaviours for polled, join, write, admin, waiting-page and protected paths ([ADR-0013](../../../docs/adr/0013-cache-behaviour-separation.md)) | DESIGN §8 |
| Gate: CloudFront Function, `edge/functions/gate.js.tftpl` | Viewer-request on the protected behaviour only. Reads config `c` and secret `k` from one KeyValueStore ([ADR-0021](../../../docs/adr/0021-edge-function-gate.md)) | DESIGN §2.3 |
| API Gateway REST (`core/main.tf`, `core/api.tf`) | `/_wr/v1/join` goes straight to SQS. Every method requires the origin API key ([ADR-0038](../../../docs/adr/0038-edge-web-acl-on-flat-rate-plan.md)) | DESIGN §6, §8 |
| `assign_position` | SQS consumer: pre-queue registration or live-join claim | DESIGN §4.1, §5.2 |
| `open_event` | T−0: the one conditional open write | DESIGN §4.2 |
| `read` | `/_wr/v1/status`, `/_wr/v1/queue_num` | DESIGN §4.2, §8 |
| `generate_token` | Admission check, one admission claim, arrival count, session mint | DESIGN §2.3, §8 |
| `nojs` | `/_wr/v1/enter`, `/_wr/v1/wait`. Admits only through `generate_token::admit` ([ADR-0037](../../../docs/adr/0037-no-javascript-queue.md)) | DESIGN §8 |
| `controller` | Durable function: six 10 s passes per `rate(1 minute)` ([ADR-0022](../../../docs/adr/0022-durable-controller-cadence.md)), metering origin concurrency ([ADR-0041](../../../docs/adr/0041-meter-origin-concurrency.md)) | DESIGN §7 |
| `admin` | Axum + askama operator UI/API behind OIDC + PKCE. The only writer of the gate KeyValueStore after apply ([ADR-0016](../../../docs/adr/0016-admin-oidc-dynamodb-sessions.md), [ADR-0018](../../../docs/adr/0018-admin-vouch-design-language.md)) | DESIGN §8, §9 |
| `wr-common` | `permutation`, `ids`, `items`, `expr`, `crypto`, `rules`, re-exported flat | DESIGN §4.3, §5.5 |
| `harness` | Load harness for the polling and cost model | DESIGN §12 |
| Waiting page (`edge/pages/`) | `waiting.html` + `waiting.js` in S3 under `/_wr/*`, ungated | DESIGN §8, §9 |

Every Lambda crate is split into `lib.rs` (pure logic, generic over a `Store` trait), `dynamo.rs`
(the SDK-backed `Store`) and `main.rs` (wiring). Tests mock the `Store`, never the logic.

## Interfaces

| Interface | Contract |
|---|---|
| `POST /_wr/v1/join` | `{request_id, event_id, h}` with `h = base64url(SHA-256(secret))`, schema-validated (400). 200 means enqueued, not assigned. A non-success SQS outcome returns 502 |
| `GET /_wr/v1/status` | Phase, `serving_state`, `serving_position`, `origin_concurrency` (absent when unset), `session_seconds`, message, `starts_at`, `poll_policy`. After the open it adds `shuffle_seed` (hex), `participant_count` and `prequeue_offsets`. Min TTL 1 s, path-only cache key, no cookies |
| `GET /_wr/v1/queue_num` | 200 position, 404 re-join with the **same** `request_id`, 409 not yet open |
| `POST /_wr/v1/generate_token` | `request_id` + possession secret ([ADR-0035](../../../docs/adr/0035-request-id-proof-of-possession.md)) → `Set-Cookie` session |
| Session cookie | HS256 JWS: `aud` event, `sub` request id, `iat`, `exp`, plus an optional IP tag ([ADR-0024](../../../docs/adr/0024-jws-credentials.md), [ADR-0036](../../../docs/adr/0036-optional-session-ip-binding.md)) |
| Gate config `c` | `{v: 1, s: enforce_from, f: fail_open_until, r: rules, b?: bind_ip}`. Under ~1 KB. Terraform seeds it. `admin` read-modify-writes it under the read-time ETag |
| Gate refusal | 302 to `/_wr/waiting.html` for navigation, 403 JSON for XHR, with a reason: `none` / `signature` / `event` / `expired` / `ip` |
| Admin | `/_wr/admin/*` routes (DESIGN §8). Every action works without JavaScript, and the UI adds nothing the API lacks |

## Data

Four on-demand tables with PITR. DESIGN §5.5 lists the attributes and
[`docs/DYNAMODB.md`](../../../docs/DYNAMODB.md) lists every access pattern.

- `Counters` (PK `event_id`): the event item `EVT#{id}`, plus shard items `EVT#{id}#PQ#{s}` and
  `EVT#{id}#AR#{s}`, each holding `n` (an arrivals shard also holds its cap `c`), and the
  controller's admission log `EVT#{id}#LOG` (`h`, `p`).
- `PreQueue` (PK `r`): `{s, l, t, h}`.
- `Positions` (PK `request_id`): `{queue_position, entry_time, status, h, ttl}`.
- `Tokens` (PK `request_id`): `SESS#` / `PKCE#` admin rows.

Every key is built in `wr_common::expr`. `event_id` may not contain `#`. A tag goes on a key only
in a table that holds more than one kind of item.

## Invariants

A change that breaks one of these is a fairness or safety failure, not a refactor.
[`../../steering/testing.md`](../../steering/testing.md) lists the test that proves each one.

1. **The queue order is never stored.** Position = `PRP(seed, offset[s] + l, N)`, computed on
   read ([ADR-0002](../../../docs/adr/0002-seeded-permutation-not-materialised-shuffle.md)). The
   PRP's wire encoding is frozen: DESIGN §4.3, "Frozen wire encoding (audit contract)". Changing
   it changes every position.
2. **The open is one conditional write**, guarded by `attribute_not_exists(shuffle_seed) AND
   phase = pre_queue`. It sets the seed, `N`, the offsets, `queue_counter = N` and the phase
   together. It reads the ten shard counters and never the rows, after waiting up to 15 s for
   the join queue to read empty (#239).
3. **No seed before T−0.** Nothing can compute a position before the open.
4. **Contiguous assembly.** The shard offsets are a prefix sum, so the indices are exactly
   `[0, N)`. A burned index is a gap nobody holds.
5. **The straggler test is per shard** (`l ≥` that shard's issued count), never a global `i ≥ N`.
   A straggler never reaches `PRP`.
6. **Zero duplicate positions; gaps allowed.** A live claim increments by the count of *valid*
   records, and every row write carries `attribute_not_exists`. Malformed records are dropped and
   never retried.
7. **The pre-queue shard is a server-side random draw** per batch (`Shard::random`, issue #59),
   never derived from `request_id`. The arrivals shard an admission takes its unit of budget from
   (`n < c`, ADR-0039) is random per admission.
8. **Sequences are never sharded.** `queue_counter` and `serving_counter` stay on the event item.
   Striped counters are separate items, not attributes
   ([ADR-0015](../../../docs/adr/0015-stripe-prequeue-counter.md)).
9. **Admission is counted once per visitor.** `Positions.status` `issued` → `admitted`
   ([ADR-0033](../../../docs/adr/0033-admission-claimed-once-per-visitor.md)). A repeat still
   gets a session but is not counted again.
10. **`FailOpen` is never stored.** Storage holds `StoredControl` (`open`/`paused`) and the
    `fail_open_until` epoch. Only `wr_common::resolve` produces `FailOpen`.
11. **Controller arithmetic is checked or saturating.** The budget is never more than
    `origin_concurrency` minus the live sessions, and the cursor never passes `queue_counter + 1`.
    The controller expires nothing ([ADR-0031](../../../docs/adr/0031-remove-controller-driven-expiry.md)).
12. **A session is signed under a derived key**, never the raw secret. Rust and the gate agree
    byte for byte, as proven by the vectors in `crates/wr-common/tests/vectors/session.json`
    against `infra/modules/edge/tests/gate.conformance.test.js`.
13. **The gate is associated with the protected behaviour only**, never distribution-wide
    (`edge/tests/gate_association.tftest.hcl`). It passes through when no rule matches, before
    `enforce_from`, inside a fail-open window, and when it throws before a rule has matched. A
    throw after a match refuses with a plain 403 (#242).
14. **The signing secret has one source.** `random_bytes.signing_key` is written to both the SSM
    parameter and KeyValueStore key `k`.
15. **Every REST method requires the origin API key.** A Terraform test enforces this. The
    no-JavaScript paths and `waiting.html` are never WAF-challenged.
16. **Idle cost stays under $5/month (N1).** No always-on compute, and no web ACL without a
    flat-rate plan.

## Not built

These are tracked in [`tasks.md`](tasks.md):

- automatic standby activation (#60)
- automatic fail-open (#58)
- a per-visitor position limit (#59)
- session revocation (#63)
- a completion signal that frees a slot when a visitor leaves early (#65)
- live metrics for F5.1
- an OpenAPI spec (N8)
- GovCloud
