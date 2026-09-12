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

The gate verifies session cookies with a per-deployment HMAC key. There is no bootstrap step.
`random_bytes` generates the key during `make apply`. Terraform writes that one value to two
places: the SSM SecureString `/<name_prefix>/signing-key`, which `generate_token` reads, and the
KeyValueStore key `k`, which the gate reads. The two cannot diverge because both come from the
same resource.

The key is stored in Terraform state. The S3 backend encrypts it.

The admin OIDC client secret works differently. The identity provider issues it, so Terraform
creates a placeholder under `ignore_changes` and an operator writes the real value out of band.

Regenerating the key invalidates every session cookie already issued. The gate refuses those
visitors with `x-wr-reason=signature` and redirects them to the waiting page. Visitors whose
positions the controller has expired cannot rejoin. The deployment accepts one key at a time, so
there is no overlap period. Regenerate only before an event opens.

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

## Entry tickets (issue #59)

Optional. Leave `entry_ticket_public_key` empty and the deployment behaves as it always has: any
client that can mint a `request_id` can take a place in line, and registration volume converts
linearly into share of the front of the queue. **That is the correct setting for a public onsale**,
where there is no prior relationship to sign about — but know that it is what you are choosing.
See [ADR-0026](adr/0026-entry-tickets.md).

### What you build

One authenticated redirect handler on your own domain. Authenticate the visitor however you
already do, then:

1. Derive an opaque subject — `base64url(HMAC-SHA256(pepper, identity ‖ event_id))` is the
   recommended recipe. **Never put the member id, email or order reference in `sub` directly.**
   It must be 22–256 base64url characters; a raw email or member number is rejected, but the
   check is on shape, not entropy, so a weak subject passes.
2. Mint a compact ES256 JWS with `aud` = the event id, a short `exp`, and that `sub`.
3. Redirect the visitor to the waiting page carrying the ticket.

Set `entry_ticket_public_key` to the matching P-256 public key as a JSON JWK
(`{"kty":"EC","crv":"P-256","x":"…","y":"…"}`). We never hold your private key, so we cannot mint
tickets.

### Delivering the ticket

**With a custom domain** (`aliases` + `acm_certificate_arn`, the expected setup) set a cookie
named by `entry_ticket_cookie_name` with `Domain=` your registrable domain. It re-presents itself
on every load and never appears in a URL.

**Without one**, redirect to
`https://<distribution>/_wr/waiting.html#wrt=<jws>`. The fragment is never sent to a server. Two
things to get right: the `Location` must be `https://`, and **tickets sent by email should use the
cookie path instead** — link rewriters such as Outlook SafeLinks re-encode the whole URL, fragment
included, into a query parameter on their own host, which hands the ticket to a third party.

### Two things that will bite you

**A ticket is a bearer credential.** Whoever reads one takes that identity's position; nothing
binds it to a browser. Keep `exp` short.

**Never add a third-party tag to the waiting page.** Not analytics, not a tag manager, not a
session recorder. The ticket is same-origin readable for the whole visit, so any script on that
page can harvest every visitor's credential. This applies to both delivery paths and is invisible
until it is violated.

### What it does not do

A farm holding N legitimate identities still gets N positions. Entry tickets move the constraint
from minting identifiers to obtaining identities, so they are worth exactly what your identity
system is worth. If your accounts are free, instant and unverified, this buys you very little.

## Tear down

```bash
make destroy
```

Terraform prompts for confirmation before deleting anything (the target does
not pass `-auto-approve`). It reads the same `terraform.tfvars` you deployed
with, so leave that file in place until the stack is gone.
