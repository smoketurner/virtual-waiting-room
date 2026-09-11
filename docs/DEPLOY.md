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
- **uv** — runs the smoke test and `scripts/bootstrap_edge_gate.py`; each
  script's PEP 723 inline metadata declares `boto3`, so `uv run` provisions
  an ephemeral virtualenv (no manual venv or pip).
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

# 4. Review the plan, then deploy.
make plan
make apply

# 5. Write the signing secret to both places the edge gate reads it from.
#    Required before the first POST /v1/generate_token of every deployment —
#    see "The edge gate's signing secret" below.
AWS_PROFILE=dev-admin uv run scripts/bootstrap_edge_gate.py
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
HMAC key. Terraform creates that key's two homes — the SSM SecureString
`/<name_prefix>/signing-key` and the CloudFront KeyValueStore key `k` — with the literal
placeholder `PLACEHOLDER-overwrite-out-of-band` and then ignores their value
(`lifecycle { ignore_changes = [value] }`), the same pattern already used for the admin OIDC
client secret. `scripts/bootstrap_edge_gate.py` is what writes the real secret to both, in one
run: generate (or reuse) the secret, write SSM, then mirror it to the KeyValueStore. Run it once
after every `make apply` that creates a fresh stack, before serving any traffic — the script
itself is idempotent (a second run against an already-bootstrapped stack does nothing but read
and re-mirror, unless `--force` is passed to regenerate).

**Two failure modes if this step is skipped or misunderstood, both silent:**

- **The bootstrap never ran.** SSM and the KeyValueStore both still hold the literal
  `PLACEHOLDER-overwrite-out-of-band` — which means they *agree*, so the gate verifies correctly
  against a secret published in this repository. Nothing is refused, and nothing looks wrong,
  until an operator writes a non-empty ruleset (`r` in the KeyValueStore config), at which point
  anyone who has read this repo and knows the event id can mint a valid session. `generate_token`
  and `authorizer` both refuse to start if the key they read is this placeholder
  (`wr_common::PLACEHOLDER_SIGNING_KEY`) — that refusal, visible as a Lambda Init failure in
  CloudWatch, is the only symptom this failure mode produces, so alarm on it.
- **The bootstrap ran but the two writes diverged** — a failure between the SSM write and the
  KeyValueStore write, or a stack whose KeyValueStore was seeded independently. `generate_token`
  mints cookies signed with one key; the edge verifies against the other; every visitor is
  refused at the edge with `x-wr-reason=signature` and loops between the waiting page and
  `generate_token`. This is silent and delayed: `ADMISSION_GRACE_SECS = 120` means the lockout
  becomes *permanent* (the controller expires the position and `generate_token` starts answering
  `Denied::Spent`) roughly two minutes after the cursor passes each position. It is also
  misleading on the dashboard: `generate_token`'s `record_arrival` fires on every looping
  attempt even though the edge is the one refusing, so `arrivals#*` inflates and the measured
  no-show rate reads near 0 for the duration — an operator watching arrivals sees a healthy
  number while every visitor is stuck. Re-run `scripts/bootstrap_edge_gate.py` (add `--force` if
  it reports the SSM value is already a real secret) to converge both sides again.

Regenerating the secret (a second bootstrap run, or `--force`) invalidates every live session
immediately — treat it as a flag day, not a routine operation.

## Tear down

```bash
make destroy
```

Terraform prompts for confirmation before deleting anything (the target does
not pass `-auto-approve`). It reads the same `terraform.tfvars` you deployed
with, so leave that file in place until the stack is gone.
