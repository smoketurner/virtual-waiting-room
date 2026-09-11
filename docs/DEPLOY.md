# Deploying the Virtual Waiting Room

This is the MVP deploy path: the scheduled pre-queue and live-join happy paths,
deployed into your own AWS account. It covers what builds the Lambda binaries,
how they are packaged and deployed, the `make` targets that wrap the workflow,
and a first-deploy walkthrough.

## The build → package → deploy chain

The four Rust functions `make build` produces (`assign_position`, `seal_event`,
`read`, `admin`) are **`provided.al2023` custom-runtime** Lambdas — plain zipped
binaries, not container images. There are no Dockerfiles by design. Two distinct
steps take source to a running function:

1. **Build + package** — `cargo lambda build --release --output-format zip`
   cross-compiles each crate to a static Linux binary named `bootstrap` and
   packages it into a ready-to-deploy zip, one per function at
   `.artifacts/<crate>/bootstrap/bootstrap.zip` (`make build`). Each crate is
   built separately: all four bins are named `bootstrap` (required by
   `provided.al2023`), so a single `--output-format zip` invocation would
   collide them — the per-crate `--lambda-dir` keeps them apart. This runs
   *outside* Terraform, so a plan stays hermetic — it never triggers a compile.
   The cross-link uses `zig`; no Docker is involved.

2. **Deploy** — each `aws_lambda_function` uploads its zip directly (`filename`
   points at the cargo-lambda zip) with `runtime = "provided.al2023"`,
   `handler = "bootstrap"`, `architectures = [var.lambda_architecture]`, and
   `source_code_hash = filebase64sha256(<zip>)` so a rebuilt zip redeploys
   automatically. No Terraform re-zip step.

The seam between build and deploy is a **path variable per function**
(`assign_position_artifact_path`, `seal_event_artifact_path`,
`read_artifact_path`, `admin_artifact_path`). You build the zips out-of-band,
point the variables at them in `terraform.tfvars`, and Terraform deploys them
directly.

The `controller` crate is not in the `make build` loop and the dev root exposes
no `controller_artifact_path`, so the outflow controller currently deploys as
the placeholder.

### The placeholder fallback

An **empty** artifact path leaves that function pointing at the vendored
`infra/modules/core/placeholder-lambda/bootstrap` instead. This lets the whole
infrastructure plane — API Gateway, SQS, IAM, DynamoDB tables — stand up before
any crate is built, and lets functions land one at a time. The join queue's
event-source mapping is enabled **only** when `assign_position` is real, so the
placeholder never consumes the queue.

## Prerequisites

- **Rust** with the target for your chosen Lambda architecture
  (`x86_64-unknown-linux-gnu` or `aarch64-unknown-linux-gnu`).
- **cargo-lambda** (`cargo install cargo-lambda`) and **zig** — the release
  build was verified with cargo-lambda 1.9.2 and Terraform 1.16.1.
- **Terraform** ≥ 1.16.
- **uv** — runs the smoke test; its PEP 723 inline metadata declares `boto3`,
  so `uv run` provisions an ephemeral virtualenv (no manual venv or pip).
- **AWS credentials** in the environment with rights to create the stack
  (Lambda, DynamoDB, SQS, API Gateway, IAM, EventBridge Scheduler). Sign in
  with `aws sso login` or `aws configure`; the CLI picks the credentials up
  from the environment.
- **Remote state backend.** State lives in S3 (bucket and key are in
  `infra/environments/dev/versions.tf`); `make init` wires it up.
- **A `terraform.tfvars`.** Copy `infra/environments/dev/example.tfvars` to
  `infra/environments/dev/terraform.tfvars` and edit it. It is gitignored and
  holds all deployment configuration (see below).

## Make targets

All targets run from the repo root and operate on the `infra/environments/dev`
Terraform root.

| Target          | What it does                                                             |
| --------------- | ------------------------------------------------------------------------ |
| `make build`    | Compile the four Lambdas and stage their bootstraps under `.artifacts/`.  |
| `make init`     | `terraform init` (safe, idempotent).                                     |
| `make plan`     | `terraform plan` against the built artifacts.                            |
| `make apply`    | `terraform apply` — the real deploy (needs AWS credentials).             |
| `make destroy`  | Tear the stack down (Terraform prompts for confirmation).                |
| `make fmt`      | `terraform fmt` across the infra tree.                                   |
| `make validate` | `terraform validate` the dev root.                                       |
| `make clean`    | Remove the staged Lambda artifacts.                                      |
| `make help`     | List the targets.                                                        |

### Where configuration lives

All deployment configuration is in
`infra/environments/dev/terraform.tfvars`, which Terraform auto-loads from the
`-chdir` root. **The Makefile passes no `-var`** — a command-line `-var` would
override the file, so the file stays the single source of truth. Copy
`example.tfvars` for the full shape; the values you will normally set:

| Variable                    | Default     | Meaning                                                                 |
| --------------------------- | ----------- | ----------------------------------------------------------------------- |
| `region`                    | `us-east-1` | AWS region.                                                             |
| `aws_profile`               | *(empty)*   | Named AWS profile to authenticate with. Empty uses the default credential chain — environment, active SSO session, or instance role. |
| `event_id`                  | `default`   | The single event id this deployment serves.                             |
| `lambda_architecture`       | `arm64`     | Lambda CPU architecture (`arm64` or `x86_64`). **Must match the built binaries.** |
| `seal_start_time`           | *(empty)*   | One-time UTC seal time as an EventBridge `at()` value, e.g. `2026-09-10T18:00:00`. Empty = seal invoked manually. |
| `client_origin_domain_name` | *(none)*    | **Required.** Bare domain of the protected origin CloudFront fronts. Host only — no scheme, no path. |
| `*_artifact_path`           | *(empty)*   | The four built zips. Empty = that function stays on the placeholder.     |
| `oidc_*`                    | *(varies)*  | Admin login (ADR-0016). The client secret is not here — it goes in an SSM SecureString out of band. |

The one `make`-level override is the build target:

| Variable | Default  | Meaning                                                    |
| -------- | -------- | ---------------------------------------------------------- |
| `ARCH`   | `x86_64` | `cargo lambda build` target: `x86_64` or `arm64`.           |

> **Architecture must match.** `ARCH` selects only what you *build*;
> `lambda_architecture` in `terraform.tfvars` selects what the function *runs*,
> and the two must agree. `ARCH` defaults to `x86_64` because that is what the
> standard host toolchain builds, while `lambda_architecture` defaults to
> `arm64` — so shipping the default `arm64` function means installing the
> `aarch64-unknown-linux-gnu` Rust target and running `make build ARCH=arm64`.
> A mismatch fails at invoke time, not at deploy.

## First deploy

```bash
# 1. Authenticate to AWS (in your shell).
aws sso login          # or: aws configure

# 2. Configure the deployment: event id, region, origin domain, artifact paths.
cp infra/environments/dev/example.tfvars infra/environments/dev/terraform.tfvars
$EDITOR infra/environments/dev/terraform.tfvars

# 3. Build the four Lambda binaries and stage them under .artifacts/.
make build ARCH=arm64          # must match lambda_architecture

# 4. Review the plan, then deploy. The gate's signing key is generated and
#    written to both of its homes by this apply — there is no separate
#    bootstrap step.
make plan
make apply
```

`make apply` prints the stack outputs, including the API invoke URL and the
table names. To exercise the deployed stack end to end — register, seal, and
verify no two visitors get the same position — run the smoke test. It reads the
API URL, table names, seal function, and event id from `terraform output` and
never touches Terraform state, so it needs nothing but credentials. `uv`
provisions boto3 from the script's inline metadata on first run:

```bash
AWS_PROFILE=dev-admin uv run scripts/smoke_test.py
```

## The edge gate's signing secret (issue #71)

The admission gate is a CloudFront Function that verifies session cookies with a per-deployment
HMAC key. **There is no bootstrap step.** Terraform generates the key with `random_bytes` at apply
and writes it to both of its homes — the SSM SecureString `/<name_prefix>/signing-key` that
`generate_token` reads, and the CloudFront KeyValueStore key `k` that the gate reads. One value
from one source, so the two cannot disagree.

The key is in Terraform state, which the S3 backend encrypts. The admin OIDC client secret is
different: the identity provider issues it, so Terraform creates a placeholder under
`ignore_changes` and it is written out of band.

Changing the key — a `terraform taint` on `random_bytes.signing_key`, or anything else forcing it
to regenerate — invalidates every live session immediately. Do it between events, not during one.

### Choosing `SESSION_TTL_SECS`

The CloudFront-path session has a fixed lifetime, not a sliding one: `generate_token` mints a
session valid for `SESSION_TTL_SECS` (`modules/core`'s `session_ttl_seconds`, default 3600) and
nothing re-issues it — a visitor still on the protected origin when it expires is logged out and
has to rejoin the queue, checkout included (ADR-0021 §5.3). This differs from `authorizer`'s
`SessionMode::Sliding`, which extends the idle window on activity up to a hard cap; the two gates
never run in the same deployment, so this is a choice between them, not an inconsistency a visitor
could observe. Set `session_ttl_seconds` comfortably longer than the worst realistic time on the
protected origin — cart to confirmation, not the median — or a visitor can lose their admission to
nothing more than a slow checkout.

## Tear down

```bash
make destroy
```

Terraform prompts for confirmation before deleting anything (the target does
not pass `-auto-approve`). It reads the same `terraform.tfvars` you deployed
with, so leave that file in place until the stack is gone.
