# Technology

Rows marked **not built** are design intent with no code or Terraform behind them yet. Keep the
mark accurate: a steering file that describes an unbuilt thing as present is the drift that
costs the most, because it is loaded into every session.

## Runtime and language

- **Rust** on AWS Lambda, `provided.al2023`, **arm64**. Every Lambda in the system is Rust. The
  one exception is the admission gate itself (issue #71): a CloudFront Function, which only runs
  JavaScript (`cloudfront-js-2.0`) — `infra/modules/edge/functions/gate.js.tftpl`.
- No Lambda in the ingest (burst) path — API Gateway REST integrates directly with SQS
  `SendMessage`. Lambda appears only downstream of SQS and on the control/admin plane.
- Async with `tokio`; AWS SDK for Rust.

## The seven Lambdas

| Function | Trigger | Job |
|---|---|---|
| `assign_position` | SQS event source mapping | Claims a contiguous position range per batch, writes `Positions` rows |
| `seal_event` | EventBridge Scheduler, one-time `at()` set by the operator on the dashboard (issue #128) | One conditional `UpdateItem` at T−0: seed, offsets, count, phase |
| `read` | API Gateway | `GET /v1/status`, `GET /v1/queue_num` |
| `generate_token` | API Gateway | Checks the position against `serving_counter`, records the arrival, mints a signed session cookie the edge gate's CloudFront Function verifies (issue #71) |
| `controller` | EventBridge Scheduler, `rate(1 minute)` | Durable function: six 10-second passes per execution — no-show correction, `serving_counter`, position expiry. Waits between passes suspend the execution rather than being billed |
| `admin` | API Gateway | Axum app: operator UI and `/admin/*` actions, OIDC-authenticated |
| `authorizer` | ALB / API Gateway at the operator's origin | The alternative gate, for an origin the operator controls. Deployed by `modules/authorizer`, not in the CloudFront path |

## Cargo workspace and crates

Edition 2024, `resolver = "3"`, one crate per Lambda plus `wr-common` under `crates/`, deps
declared once in `[workspace.dependencies]`. Pinning and lint rules are in `conventions.md`.

The actual dependency set (each pinned exactly, `default-features = false`, features enabled
only in the member crate that uses them):

| Purpose | Crate |
|---|---|
| Lambda runtime | `lambda_runtime`, `lambda_http`, `aws_lambda_events` |
| Web framework (admin UI) | `axum`, `tower`, `tower-http` |
| Async runtime | `tokio` |
| AWS SDK | `aws-config`, `aws-sdk-dynamodb`, `aws-sdk-ssm`, `aws-smithy-types`, `aws-sdk-cloudfrontkeyvaluestore` (admin only — writes the edge gate's config, issue #71), `aws-sdk-scheduler` (admin only — sets the seal schedule's time, issue #128) |
| Templating + static assets (admin UI) | `askama`, `rust-embed`, `mime_guess` |
| Admin OIDC login (ADR-0016) | `openidconnect`, `jsonwebtoken`, `reqwest`, `rustls`, `cookie` |
| Serialization | `serde`, `serde_json`, `serde_dynamo`, `base64` |
| Crypto | **`aws-lc-rs`** — signing/HMAC backend for tokens, sessions, the CloudFront cookie signature, and the permutation HMAC |
| Errors and logging | `thiserror` (libs), `anyhow` (binaries), `tracing`, `tracing-subscriber` |
| Dev | `proptest` |

There is no SQS SDK dependency: API Gateway writes to the queue and `assign_position` receives
the batch as a Lambda event. There is no Secrets Manager SDK dependency either — secrets are read
from SSM.

Terraform creates every schedule and owns its target, retry policy and role. The one schedule
whose *time* is not Terraform's is the seal (issue #128): the operator sets it on the dashboard,
so `admin` carries `aws-sdk-scheduler` to rewrite the expression and state, and those two fields
alone sit under `ignore_changes`. `UpdateSchedule` replaces rather than patches, so that writer
reads the schedule and resends the whole definition.

Timezone conversion uses `jiff` with the tz database compiled in (`tzdb-bundle-always`), not read
from the runtime image — `provided.al2023` does not guarantee one. It is a control-plane
dependency in `admin` only: the operator picks the zone their event opens in, and a stored offset
would be wrong on the far side of a daylight-saving change.

- **The admission path is `aws-lc-rs` only.** Credentials are minted and verified with `aws-lc-rs`
  throughout. Select the AWS SDK's `aws-lc-rs`-backed TLS/crypto path and disable defaults so no
  second crypto stack is pulled in by accident.
- **`ring` and `openssl` are banned outright** (`deny.toml` `[bans].deny`, issue #71/#74). `ring`
  reached the tree only via `crates/harness`'s `reqwest` feature selection; switching it to the
  same `rustls-tls-webpki-roots-no-provider` pattern `admin` already used, plus installing the
  `aws-lc-rs` rustls provider there too, removed it entirely — `cargo tree --workspace -i ring`
  prints nothing.
- **RustCrypto is accepted, scoped to two control-plane paths, never the admission path**: the
  admin OIDC login (`openidconnect`'s `p256`/`rsa` dependencies, below) and the admin's edge-gate
  KeyValueStore writer (`aws-sdk-cloudfrontkeyvaluestore`'s `sigv4a` feature, which signs via
  RustCrypto's `p256`/`hmac`/`sha2` — SigV4A has no `aws-lc-rs`-backed implementation). Neither
  crate is present in any Lambda that mints or verifies a credential.
- AWS SDK crates take `default-features = false` deliberately: it drops the legacy rustls 0.21
  connector; each crate then enables the current hyper-1 HTTPS client explicitly.

### The OIDC dependency chain, and why it is pinned the way it is

The admin login is where the single-crypto-backend rule is relaxed on purpose (previous
paragraph), so the feature selection is deliberate and fragile. Manifests carry no comments, so
the reasoning lives here:

- `openidconnect`'s default `rustls-tls` feature forces `reqwest`'s **ring**-backed provider,
  which this project forbids on every path. Defaults are therefore off and only the `reqwest`
  transport feature is enabled.
- `openidconnect` re-exports the `oauth2` 5 / `reqwest` 0.12 types that its `request_async`
  signature expects, so `reqwest` is pinned to 0.12 or the admin crate does not typecheck.
- `reqwest` uses `rustls-tls-webpki-roots-no-provider`, so no provider is chosen for it, and the
  admin binary installs rustls's **`aws-lc-rs`** `CryptoProvider` at startup. `crates/harness`
  does the same for the same reason.
- `jsonwebtoken` verifies JWKS signatures with its `aws_lc_rs` feature.

Changing any one of these can silently pull `ring` back into the tree — `deny.toml` now catches
it (`cargo deny check bans` fails with `error[banned]` on a crate that is present), closing
[#74](https://github.com/smoketurner/virtual-waiting-room/issues/74). `ring` still has no
`wrappers` exemption and gets none: a list that passed would have to name `rustls`, the crate that
wanted `ring` in the first place, which would permit exactly what the rule exists to forbid.

## Release profile (Lambda size / cold start)

```toml
[profile.release]
lto = true
codegen-units = 1
strip = true
```

Smaller artifact and a marginally faster cold start, at the cost of slower builds.
**Deliberately not set:** `panic = "abort"` — `lambda_runtime` catches an unwinding handler
panic and turns it into a 5xx while keeping the sandbox warm; `abort` would torch the execution
environment on every panic. `opt-level = "z"` — the permutation HMAC and JSON parsing are billed
CPU time, so size-over-speed is the wrong trade; `opt-level` 3-vs-`z` for cold start is
scenario-dependent and is A/B-measured in the Phase 0 spike, not assumed.

### aws-lc-rs cold-start (jitter entropy)

`aws-lc-rs` ≥ 1.14.1 seeds its RNG with CPU jitter entropy, collected once per process init
(unoptimized, SHA3) — several ms to ~1 s on a cold start, worst on small-memory / frequently-cold
functions ([smithy-rs #4541](https://github.com/smithy-lang/smithy-rs/discussions/4541),
[lambdabench.dev/rust](https://lambdabench.dev/rust.html)). This system is idle by definition, so
cold starts are the common case. **Preferred mitigation: force one real TLS handshake in the Init
phase** (a cheap warm-up call on the client the handler uses), so the tax lands on boosted Init
CPU rather than the first invoke — and no entropy source is dropped. The build flag
`AWS_LC_SYS_NO_JITTER_ENTROPY=1` removes the tax outright but drops one of two defense-in-depth
entropy sources; given the GovCloud/FIPS posture we lean **against** it. Settle both in the Phase 0
spike.

## AWS services (core, commercial regions)

| Concern | Service |
|---|---|
| Edge / CDN / request collapsing | CloudFront — polled, write, waiting-page and protected behaviours (ADR-0013) |
| **The admission gate** | CloudFront Function (`cloudfront-js-2.0`) at viewer-request on the protected behaviour only, deciding locally from a KeyValueStore (ADR-0021, issue #71); `generate_token` signs an HMAC-SHA256 session cookie the function verifies. Sub-millisecond compute at the edge, not zero, but no round trip to the origin |
| Bot & abuse mitigation | WAF: Bot Control, ASN matching, anti-DDoS in Count mode — **not built** (N7, #59, #70) |
| Ingest | API Gateway **REST** (regional) with request validator → SQS |
| Buffer | SQS standard queue + DLQ (`maxReceiveCount` 5), ESM `ReportBatchItemFailures` |
| Compute | Lambda (Rust, arm64) — the seven functions above |
| State | DynamoDB on-demand + PITR: `Counters`, `PreQueue`, `Positions`, `Tokens` |
| Scheduling | EventBridge Scheduler (T−0 seal, controller every minute × six passes via durable waits) |
| Secrets | **SSM Parameter Store SecureString** — the per-deployment HMAC signing key (Terraform generates it and writes the same value to the edge gate's CloudFront KeyValueStore, issue #71) and the OIDC client secret. Not Secrets Manager: a SecureString is free where a secret is $0.40/mo, which N1 does not allow |
| Metrics | CloudWatch. EMF emission and the shipped dashboard are **not built** (F5.1) |

**No VPC by default** — all services are IAM-authenticated public endpoints (no NAT, no VPC
endpoints). VPC is an opt-in variable for ATO-constrained operators; Lambda code is identical.

### Public API surface as deployed

`POST /v1/join` (direct SQS integration), `GET /v1/status`, `GET /v1/queue_num`,
`POST /v1/generate_token`, plus `/admin`, `/admin/{proxy+}`, `/metrics`, `/update_session` and
`/static/{proxy+}` fronting the admin Lambda. `/queue_pos_expiry` and `/public_key` appear in
`docs/DESIGN.md` §8 but are **not routed**.

## Infrastructure as code

- **Terraform**, no manual console steps (N5). Modules: `core`, `edge`, `authorizer`,
  `demo-origin` (a fixture standing in for an operator origin in the dev root).
- Target ≤ 80 Terraform-managed resources for the core module (N6). Every new resource counts
  against this budget — justify additions.
- API documented as **OpenAPI** (N8) — **not built**.

## Admin web interface

A **single Axum-based Rust Lambda** rendering server-side HTML with **askama** compile-time
templates.

- Styled with the **Vouch design language** as a self-contained plain-CSS stylesheet embedded in
  the binary via `rust-embed` (ADR-0018, which superseded the Cloudscape design tokens and their
  `extract_tokens.py` build step). No React, no bundler, no runtime npm dependency.
- Auth is **OIDC Authorization Code + PKCE** with sessions and pending logins in the `Tokens`
  table (ADR-0016, superseding SigV4). API Gateway auth is `NONE` because the Lambda is the
  enforcement point.
- Interactivity = plain HTML `<form>` POSTs to the same `/admin/*` actions plus a small
  vanilla-JS metrics poller. Core actions work with JavaScript disabled.
- The UI is a **thin server-rendered client over the admin Lambda's own logic** — it adds no
  capability the API lacks (F5.5 API-first still holds).

## Justify new dependencies

Each crate is attack surface and maintenance burden. The system is deliberately small; prefer
the standard library and the AWS SDK over new crates. A new dependency needs a stated reason.
