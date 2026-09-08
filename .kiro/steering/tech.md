# Technology

## Runtime and language

- **Rust** on AWS Lambda, `provided.al2023`, **arm64**. Every function in the system is Rust.
- No Lambda in the ingest (burst) path — API Gateway REST integrates directly with SQS
  `SendMessage`. Lambda appears only downstream of SQS and on the control/admin plane.
- Async with `tokio`; AWS SDK for Rust.

## Cargo workspace and crates

Follow the reference layout in
[`aws-messaging-webhook/Cargo.toml`](https://github.com/smoketurner/aws-messaging-webhook/blob/main/Cargo.toml):
edition 2024, `resolver = "3"`, one crate per Lambda plus shared libs under `crates/`, deps
declared once in `[workspace.dependencies]`. Pinning and lint rules are in `conventions.md`.

The intended crate set (each pinned exactly, `default-features = false`, features enabled only
in the member crate that uses them):

| Purpose | Crate |
|---|---|
| Lambda HTTP runtime | `lambda_http` |
| Web framework (admin UI + handlers) | `axum`, `tower`, `tower-http` |
| Async runtime | `tokio` |
| AWS SDK | `aws-config`, `aws-sdk-dynamodb`, `aws-sdk-sqs`, `aws-sdk-eventbridge`, `aws-sdk-secretsmanager`, `aws-smithy-types` |
| Typed Lambda event shapes | `aws_lambda_events`, `serde_dynamo` (DynamoDB Streams) |
| Serialization | `serde`, `serde_json` |
| Templating (admin UI) | `askama` |
| Crypto | **`aws-lc-rs`** — the signing/HMAC backend for tokens, sessions, and the permutation HMAC |
| Observability | `metrics`, `metrics_cloudwatch_embedded`, `tracing` |
| Errors | `thiserror` (libs), `anyhow` (binaries) |
| Dev | `proptest`, `wiremock` |

- **Crypto is `aws-lc-rs` only — never `ring` or `openssl`.** Select the AWS SDK's
  `aws-lc-rs`-backed TLS/crypto path and disable defaults so no second crypto stack (legacy
  rustls/`ring`) is pulled in. One FIPS-capable, AWS-maintained backend across the whole tree.
- AWS SDK crates take `default-features = false` deliberately: it drops the legacy rustls 0.21
  connector; each crate then enables the current hyper-1 HTTPS client explicitly.

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
CPU time, so size-over-speed is the wrong trade.

## AWS services (core, commercial regions)

| Concern | Service |
|---|---|
| Edge / CDN / request collapsing | CloudFront (three separate cache behaviours — see ADR-0013) |
| Bot & abuse mitigation | WAF: Bot Control, ASN matching, anti-DDoS rule group (Count mode by default) |
| Ingest | API Gateway **REST** (regional) with request validator → SQS |
| Buffer | SQS standard queue + DLQ (`maxReceiveCount` 5), ESM `ReportBatchItemFailures` |
| Compute | Lambda (Rust, arm64) — assign_position, controller, authorizer, phase, admin |
| State | DynamoDB on-demand + PITR: `Counters`, `PreQueue`, `Positions`, `Tokens` |
| Scheduling | EventBridge Scheduler (T−0 seal, 10 s outflow controller) |
| Secrets | Secrets Manager (per-deployment signing key) |
| Metrics | CloudWatch via Embedded Metric Format (EMF); inflow from `AWS/CloudFront` `Requests` |

**No VPC by default** — all services are IAM-authenticated public endpoints (no NAT, no VPC
endpoints). VPC is an opt-in variable for ATO-constrained clients; Lambda code is identical.

## Infrastructure as code

- **Terraform**, no manual console steps (N5). Modules: `core`, `edge`, `authorizer`.
- Target ≤ 80 Terraform-managed resources for the core module (N6). Every new resource counts
  against this budget — justify additions.
- API documented as **OpenAPI**; public and admin surfaces generated from the spec (N8).

## Admin web interface — Option A (accepted)

The operator dashboard is a **single Axum-based Rust Lambda** rendering server-side HTML with
**askama** compile-time templates, styled with the **Cloudscape design language**.

- **Cloudscape components are React-only** — there is no server-rendered HTML component
  library. We therefore use Cloudscape **design tokens**, not components.
- **No runtime dependency on `@cloudscape-design/design-tokens`** (it ships Sass/JS vars and
  presupposes the React components). At **build time**, extract token *values* from the
  blessed `index-visual-refresh.json` artifact (via `style-dictionary` or a small script)
  into a plain CSS custom-properties stylesheet (`:root { --color-…: … }`) vendored into the
  Lambda. Runtime stays React-free and dependency-free.
- Interactivity = plain HTML `<form>` POSTs to the same `/admin/*` actions + a tiny vanilla-JS
  poller for metrics. **No React, no bundler in the request path.** Core actions work with JS
  disabled.
- The UI is a **thin server-rendered client over the existing admin Lambda logic** — it adds
  no capability the API lacks (F5.5 API-first still holds) and uses the same **SigV4** auth.
- Tradeoff accepted: hand-author markup that Cloudscape-React would provide as components, in
  exchange for one React-free Rust Lambda that fits N1 (idle cost) and N6 (resource budget).

## Justify new dependencies

Each crate is attack surface and maintenance burden. The system is deliberately small; prefer
the standard library and the AWS SDK over new crates. A new dependency needs a stated reason.
