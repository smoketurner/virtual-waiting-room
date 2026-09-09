# Deploying the Virtual Waiting Room

This is the MVP deploy path: the scheduled pre-queue and live-join happy paths,
deployed into your own AWS account. It covers what builds the Lambda binaries,
how they are packaged and deployed, the `make` targets that wrap the workflow,
and a first-deploy walkthrough.

## The build → package → deploy chain

The three Rust functions (`assign_position`, `seal_event`, `read`) are
**`provided.al2023` custom-runtime** Lambdas — plain zipped binaries, not
container images. There are no Dockerfiles by design. Two distinct steps take
source to a running function:

1. **Build + package** — `cargo lambda build --release --output-format zip`
   cross-compiles each crate to a static Linux binary named `bootstrap` and
   packages it into a ready-to-deploy zip, one per function at
   `.artifacts/<crate>/bootstrap.zip` (`make build`). cargo-lambda namespaces
   each function into its own directory, so the shared `bootstrap` name never
   collides. This runs *outside* Terraform, so a plan stays hermetic — it never
   triggers a compile. The cross-link uses `zig`; no Docker is involved.

2. **Deploy** — each `aws_lambda_function` uploads its zip directly (`filename`
   points at the cargo-lambda zip) with `runtime = "provided.al2023"`,
   `handler = "bootstrap"`, `architectures = [var.lambda_architecture]`, and
   `source_code_hash = filebase64sha256(<zip>)` so a rebuilt zip redeploys
   automatically. No Terraform re-zip step.

The seam between build and deploy is a **path variable per function**
(`assign_position_artifact_path`, `seal_event_artifact_path`,
`read_artifact_path`). You build the zips out-of-band, point the variables at
them, and Terraform deploys them directly.

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
- **AWS credentials** in the environment with rights to create the stack
  (Lambda, DynamoDB, SQS, API Gateway, IAM, EventBridge Scheduler). Sign in
  with `aws sso login` or `aws configure`; the CLI picks the credentials up
  from the environment.

## Make targets

All targets run from the repo root and operate on the `infra/environments/dev`
Terraform root.

| Target          | What it does                                                             |
| --------------- | ------------------------------------------------------------------------ |
| `make build`    | Compile the three Lambdas and stage their bootstraps under `.artifacts/`. |
| `make init`     | `terraform init` (safe, idempotent).                                     |
| `make plan`     | `terraform plan` against the built artifacts.                            |
| `make apply`    | `terraform apply` — the real deploy (needs AWS credentials).             |
| `make destroy`  | Tear the stack down (Terraform prompts for confirmation).                |
| `make fmt`      | `terraform fmt` across the infra tree.                                   |
| `make validate` | `terraform validate` the dev root.                                       |
| `make clean`    | Remove the staged Lambda artifacts.                                      |
| `make help`     | List the targets.                                                        |

### Overridable variables

Pass these on the `make` command line; each has a default.

| Variable     | Default     | Meaning                                                                 |
| ------------ | ----------- | ----------------------------------------------------------------------- |
| `ARCH`       | `x86_64`    | Lambda CPU architecture (`x86_64` or `arm64`). **Must match the built binaries.** |
| `EVENT_ID`   | `default`   | The single event id this deployment serves.                             |
| `SEAL_START` | *(empty)*   | One-time UTC seal time as an EventBridge `at()` value, e.g. `2026-09-10T18:00:00`. Empty = seal invoked manually. |
| `REGION`     | `us-east-1` | AWS region.                                                             |
| `PROFILE`    | *(empty)*   | Named AWS profile to authenticate with (sets the provider's `profile`). Empty uses the default credential chain — environment, active SSO session, or instance role. |

> **Architecture must match.** `ARCH` defaults to `x86_64` because that is what
> the standard host toolchain builds. To ship `arm64` you need the
> `aarch64-unknown-linux-gnu` Rust target installed, then build and deploy with
> `ARCH=arm64` on both `make build` and `make apply`. A mismatch between the
> binary and the function's `architectures` fails at invoke time, not deploy.

## First deploy

```bash
# 1. Authenticate to AWS (in your shell).
aws sso login          # or: aws configure

# 2. Build the three Lambda binaries and stage them under .artifacts/.
make build

# 3. Review the plan, then deploy. Set an event id and (optionally) a seal time.
make plan  EVENT_ID=launch
make apply EVENT_ID=launch SEAL_START=2026-09-10T18:00:00
```

`make apply` prints the stack outputs, including the API invoke URL and the
table names. To exercise the deployed stack end to end — register, seal, and
verify no two visitors get the same position — run the smoke test. It reads the
API URL, table names, seal function, and event id from `terraform output` and
never touches Terraform state, so it needs nothing but credentials:

```bash
AWS_PROFILE=dev-admin ./scripts/smoke_test.py
```

## Tear down

```bash
make destroy EVENT_ID=launch
```

Terraform prompts for confirmation before deleting anything (the target does
not pass `-auto-approve`). Pass the same `EVENT_ID` / `ARCH` you deployed with
so the plan resolves the same resources.
