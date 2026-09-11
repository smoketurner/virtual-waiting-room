# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A virtual waiting room that deploys into a customer's own AWS account: it holds visitors
during a traffic spike and meters them into the origin at a controlled rate. Rust Lambdas
plus Terraform; no always-on compute. Status is MVP — the scheduled pre-queue and live-join
paths are implemented and deployable.

## Commands

The Cargo workspace root is the repository root; member crates live under `crates/`.

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings   # warnings are errors
cargo test --workspace
cargo test -p wr-common prop_bijective                     # one crate / one test
cargo deny check                                           # advisories, licenses, bans
```

Terraform and packaging run from the repository root via `make` (see `docs/DEPLOY.md`):

```bash
make build           # cargo lambda build --release --output-format zip, per crate
make plan            # terraform plan against the built artifacts
make validate        # terraform validate the dev root
make fmt             # terraform fmt -recursive across infra/
make apply           # real deploy — only when explicitly asked
prek run             # pre-commit hooks (fmt, actionlint, zizmor, shellcheck)
```

`make apply` and `make destroy` create and delete real AWS resources. Per
`.kiro/steering/conventions.md`, `validate` and `plan` are the default verification —
never run apply or destroy unless explicitly asked.

Deployment configuration (region, `aws_profile`, `event_id`, `lambda_architecture`,
`seal_start_time`, artifact paths) lives in `infra/environments/dev/terraform.tfvars` and is
authoritative — the Makefile deliberately passes no `-var`, since a command-line `-var` would
override the file. `make build` reads `lambda_architecture` out of that file to pick its cross-compile
target, so the binaries cannot be built for a different architecture than the functions are
deployed with. `ARCH=` still overrides it for a one-off build.

CI runs the same checks: `.github/workflows/rust-ci.yml` (fmt, clippy, test, cargo-deny,
pinned to Rust 1.98.0) and `terraform-ci.yml` (fmt -check, init -backend=false, validate).

## Architecture

The load-bearing idea is that **the burst never touches compute and the queue order is never
stored**. Read `docs/DESIGN.md` before changing any of the mechanisms below; `docs/adr/`
holds the reasoning for each.

Request path:

```
join:      CloudFront → API Gateway REST (type: aws, direct SQS SendMessage) → SQS
               → assign_position Lambda → DynamoDB
protected: CloudFront [gate: CloudFront Function, viewer-request] ─ valid session cookie → client origin
                                                                   └ missing/expired → 302/403 → /_wr/waiting.html
```

Three separate CloudFront cache behaviours (ADR-0013). `/status` is Min TTL 1 s with no
cookies forwarded, so CloudFront collapses simultaneous misses into one origin fetch and
origin load is independent of how many people are waiting.

Position assignment has two paths:

- **Pre-queue (scheduled).** Registration writes `PreQueue {r, s, l, t}`: shard
  `s = hash(request_id) % 10`, local index `l` from a striped counter (ADR-0015). At T−0
  `seal_event` folds the ten shard counts into prefix offsets and a cohort size and writes
  seed + offsets + count + phase in **one conditional `UpdateItem`** guarded by
  `attribute_not_exists(shuffle_seed)`, so a double-fire seals exactly once. A visitor's
  position is `PRP(seed, offset[s] + l, N)`, computed on read — never stored (ADR-0002).
- **Live join.** `assign_position` claims a contiguous block with one
  `UpdateItem ADD queue_counter :n / ALL_NEW`, incrementing by the count of **valid** records
  only, then writes each row with `attribute_not_exists(request_id)`. Gaps are acceptable;
  duplicates are not.

Admission is closed-loop: `controller` runs six passes per `rate(1 minute)` execution
(10 s cadence), measures arrivals against what it released, smooths the no-show rate with an
EWMA, and advances `serving_counter` by a bounded correction. It is a **Lambda durable
function** (ADR-0022): each pass is a checkpointed durable step and each 10 s gap a durable
wait that suspends the execution instead of holding the invocation open, so the waiting is not
billed. The SDK (`aws-durable-execution-sdk`) is an experimental preview, pinned exactly. It also expires positions and
advances `max_expired_position` (ADR-0006 — expiry is controller-driven, not DynamoDB TTL).

**The gate is a CloudFront Function** (ADR-0021, issue #71), associated at viewer-request with the
protected behaviour only — never distribution-wide, since Functions bill per invocation and a
distribution-wide association would bill every `/status` poll from every waiter. It decides
locally, reading its whole configuration and the HMAC signing secret from one CloudFront
KeyValueStore (`infra/modules/edge/functions/gate.js.tftpl`): no rule matches → pass through
(dormancy, #60); a valid session cookie → pass through; otherwise refuse with a reason (#73), a
302 to the waiting page for navigation and 403 JSON for XHR (#72). This replaces ADR-0020's
trusted-key-group gate, which could verify a signature but not decide, closing #58's mechanism,
#60, #64, #66, #72 and #73. `event_id` and the session cookie name are templated into the
function's own source rather than carried in the KeyValueStore value, so Terraform stays their
single source of truth.

`generate_token` mints the session cookie the gate verifies: it checks the visitor's position
against `serving_counter` (resolving the operator's admission control through
`wr_common::resolve(StoredControl, fail_open_until, now)` — `FailOpen` is never a stored string,
only an epoch), records the arrival, and signs an HMAC-SHA256 session credential
(`wr_common::crypto`, the same wire format the authorizer already used). It is the only writer of
`arrivals#*` while admission is Open, so the controller's no-show correction depends on it.

The signing key is generated by `random_bytes.signing_key` during apply. Terraform writes that one
value to both readers: the SSM SecureString the Lambdas read, and the KeyValueStore key `k` the
gate reads. There is no bootstrap step and no placeholder, so the two copies cannot diverge.
Regenerating the key invalidates every session cookie already issued.

`authorizer` is the alternative gate for a customer who *does* control their origin and wants
per-request rules the edge cannot express (header, cookie, user agent). It decides locally
with no backend call: session cookie → admission token → protection-rule match → 302. Tokens
and sessions are both HMAC-SHA256 from one per-deployment key but domain-separated by a
leading kind byte (`0x01` token, `0x02` session), so neither validates as the other
(ADR-0011). It is built and deployable but is not in the CloudFront path.

### Crates

`wr-common` is the single shared library, laid out as `permutation` (Feistel PRP + shard
assembly), `ids` + `items` (newtypes, `Phase`, `StoredControl`/`AdmissionControl`/`resolve`,
DynamoDB item shapes), `expr` (update/condition fragments), `crypto` (token and session signing),
and `rules` (`ProtectionRule` + its compact KeyValueStore-sized wire encoding, shared by
`authorizer` and `admin`'s edge-gate writer, and mirrored a third time by the CloudFront
Function). Its whole public surface is re-exported flat, so callers write `wr_common::Phase`
rather than a path that encodes which layer a type lives in.
`crates/wr-common/tests/vectors.rs` generates the cross-language conformance vectors
`infra/modules/edge/tests/*.conformance.test.js` check the CloudFront Function against.

`assign_position`, `seal_event`, `read`, `controller`, `authorizer`, `admin`, and
`generate_token` are the Lambdas.

Every Lambda crate follows the same three-file split, and new ones should:

- `lib.rs` — pure logic, generic over a `Store` trait (the persistence port). AWS-free, so
  the logic is tested without AWS.
- `dynamo.rs` — the SDK-backed `Store` implementation.
- `main.rs` — runtime wiring, environment variables, handler.

`admin` is an Axum Lambda rendering askama compile-time templates with Cloudscape design
tokens as plain CSS — no React, no bundler, no runtime npm dependency (ADR-0014, ADR-0018).
Auth is OIDC Authorization Code + PKCE with sessions in DynamoDB (ADR-0016). Core operator
actions must work with JavaScript disabled, and the UI adds no capability the admin API lacks.

### State

Four DynamoDB tables, all on-demand with PITR: `Counters` (PK `event_id`), `PreQueue` (PK `r`),
`Positions` (PK `request_id`), `Tokens` (PK `request_id`).

The event's own `Counters` item holds the sequences, phase, seed, rate, message, and the
operator's admission override: `admission_control` (the `StoredControl` wire string, `open` or
`paused` only — never `fail_open`, issue #71) and `fail_open_until` (an epoch-seconds deadline,
`0` = no window). `wr_common::resolve(stored, fail_open_until, now)` is the only thing that
produces the third, resolved value, `AdmissionControl::FailOpen`; nothing parses it back out of
storage. `queue_counter` and `serving_counter` must stay on the item — sharding a sequence
destroys ordering — and both are low-rate: one claim per ingest batch, one advance per controller
pass.

Keys are tagged only where a table holds more than one kind of item. `Counters` holds the
event plus its shards, and `Tokens` holds admission-token reservations (`TKN#`), operator OIDC
sessions (`SESS#`), and pending PKCE logins (`PKCE#`) — without the tag a session id and a
token for the same string would be one row. `Positions` and `PreQueue` hold one kind each and
take bare ids: a tag there disambiguates nothing and costs bytes in the partition key of every
row, of which there is one per visitor.

`prequeue_counter` and `arrivals` are order-free and striped ×10, **as separate items** keyed
`EVT#{event_id}#PQ#{shard}` and `EVT#{event_id}#AR#{shard}`, each holding one attribute `n`.
The event's own item is `EVT#{event_id}`. Every key is built by `wr_common::expr`, never at
the call site, and `event_id` may not contain `#` — the separator would let one event's shard
key collide with another event's item. The write
ceiling is 1,000/s per partition key, so striping across attribute names on one item would
share a single budget and distribute nothing (ADR-0015 amendment). Attribute names are billed
on every write too, which is why the shard attribute is one letter.

### Infrastructure

`infra/environments/dev` is the only deployable Terraform root; `apply` never runs inside a
module. Modules are `core` (tables, SQS, Lambdas, IAM, REST API), `edge` (CloudFront, WAF),
and `authorizer`. An **empty** artifact path leaves a function on the vendored placeholder
binary, which lets the infrastructure plane stand up before any crate is built.

Behaviour follows the artifact rather than a separate toggle, because a stack that looks
complete and meters nobody is worse than one that plainly is not built yet: the join
event-source mapping is enabled when `assign_position` is real, and the controller's
schedule is created when the controller is. `make build` compiles every Lambda crate, reading
its target architecture from `terraform.tfvars`.

`core` owns the edge gate's CloudFront KeyValueStore (issue #71) — the store's only *writer* is
the admin Lambda, which lives in `core`, so putting the store in `edge` would need `edge` to
export its ARN back to `core`, a module cycle. `core` exports `gate_kvs_arn`; `edge` consumes it
and owns the `aws_cloudfront_function` resource, which reads its ARN and templates `event_id` +
the session cookie name into the function's own source. Writing to the KeyValueStore data plane
needs SigV4A, which the Rust SDK signs with RustCrypto (`p256`/`hmac`/`sha2`, the
`aws-sdk-cloudfrontkeyvaluestore` crate's `sigv4a` feature). That path is control-plane only. The
admission path mints and verifies with `aws-lc-rs`.

Keep the `core` module at or under 80 Terraform resources (requirement N6); justify additions.
Currently 65.

## Conventions specific to this repo

`.kiro/steering/` (`conventions.md`, `tech.md`, `testing.md`, `structure.md`, `product.md`) is
the authoritative rule set and is worth reading before non-trivial work. The points that bite
most often:

- **Exact version pins only** in `[workspace.dependencies]` — `= "=x.y.z"`, never `^` or `~`.
  `default-features = false` on everything, and **no `features = [...]` at the workspace
  root**: each member crate opts into the features it uses.
- **The admission path is `aws-lc-rs` only.** `ring` and `openssl` are banned outright
  (`deny.toml`); RustCrypto is a scoped, deliberate exception on two control-plane paths only
  (admin's OIDC login, admin's edge-gate KeyValueStore writer — `.kiro/steering/tech.md`), never
  the paths that mint or verify a credential. This constrains dependency selection (see the
  `openidconnect`/`reqwest`/`rustls` comments in `Cargo.toml`): a crate whose defaults pull in a
  ring-backed provider must have defaults disabled.
- `allow_attributes = "deny"` means you cannot silently `#[allow]` a lint. Fix the cause.
  `unwrap_used`, `panic`, `todo`, `print_stdout` are denied workspace-wide.
- Controller and permutation arithmetic must be checked or saturating — the release profile
  has no overflow checks, and a wrapped subtraction there releases a damaging burst.
- Newtypes over primitives, enums over boolean flags. `StoredControl` (`Open`/`Paused`) plus a
  `fail_open_until` epoch replaced the three-valued `AdmissionControl` as the *stored* form
  (issue #71): `resolve()` is the only thing that produces the third value, `FailOpen`, so it can
  never be written to storage as a string — the storage codec has two values where the display
  type has three, and that asymmetry is the invariant. Exhaustive `match` with no `_` arms.
- Pin GitHub Actions to a SHA with a version comment, set `persist-credentials: false`, and
  run `actionlint` and `zizmor` before committing.

## Testing

Correctness is the product here: a duplicate position or a non-uniform permutation is a
visible fairness failure at a million people. `.kiro/steering/testing.md` lists the mechanisms
that must stay proven — bijectivity and uniformity of the PRP, the frozen wire encoding
vectors, contiguous shard assembly, burned slots, the straggler-racing-the-seal case, zero
duplicate positions under concurrency, token/session non-interchangeability, cross-language
credential and rule conformance with the edge gate (issue #71), and no-show convergence. Use
`proptest` for the permutation and counter, and mock the AWS boundary via the `Store` trait
rather than mocking logic.

## Documentation layers

`docs/` is the human narrative (`REQUIREMENTS.md`, `DESIGN.md`, `PLAN.md`, `adr/`).
`.kiro/specs/virtual-waiting-room/` is the agent-facing SDD (EARS requirements, design,
tasks). Requirement IDs (`F*`, `C*`, `N*`, `O*`) are the join key: when a requirement changes,
update both layers and keep the ID stable. Neither layer restates a decision's rationale —
that lives in an ADR. The checkboxes in `.kiro/specs/.../tasks.md` track what is built; an
item that is only partly delivered stays unchecked and carries a `Partial:` note saying what
is missing. Keep it that way when you finish a task.
