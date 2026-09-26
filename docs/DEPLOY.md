# Deploying the Virtual Waiting Room

This is the MVP deploy path: the scheduled pre-queue and live-join happy paths,
deployed into your own AWS account. It covers what builds the Lambda binaries,
how they are packaged and deployed, the `make` targets that wrap the workflow,
and a first-deploy walkthrough.

## The build → package → deploy chain

The four Rust functions `make build` produces (`assign_position`, `open_event`,
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

The seam between build and deploy is a **path variable per function**. `make
build` compiles all six Lambda crates — `assign_position`, `open_event`, `read`,
`admin`, `controller`, `generate_token` — into `.artifacts/`, and the dev root
points each variable at the matching zip.

**There is no placeholder fallback.** Every function deploys a real build, and
the join event-source mapping and the controller schedule are created
unconditionally. A stack that stands up with stub binaries looks deployed and
serves nobody, which is the failure this system is most prone to and least able
to report; an incomplete apply now fails at plan instead, on the missing zip.

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
| `client_origin_domain_name` | *(empty)*   | Bare domain of the protected origin CloudFront fronts. Host only — no scheme, no path. Empty protects the built-in demo origin instead, which is a working stack that guards nothing real. |
| `admission_rate`            | `5`         | Visitors per second, seeded onto the event item. Zero would release nobody, forever, so this has a real default. Changed live from the dashboard. |
| `gate_rules`                | *(empty)*   | Which requests the gate covers, one rule per line — same grammar as the dashboard's Set rules form. Empty is dormant: every request passes through. |
| `starts_at`                 | *(empty)*   | When the event opens, local date-time with no zone. Set, the first apply seeds the event in the pre-queue phase so visitors register and the scheduled open can run; the waiting page shows no countdown until the operator also sets the time on the dashboard. Empty leaves the schedule disabled and the event idle — it then opens when an operator presses **Open now** on the dashboard, or never. |
| `starts_at_timezone`        | `UTC`       | IANA zone `starts_at` is evaluated in. |
| `aliases`                   | `[]`        | The hostnames viewers actually use, e.g. `["waiting.example.com"]`. Empty deploys on the distribution's own `*.cloudfront.net` name. |
| `acm_certificate_arn`       | *(empty)*   | ACM certificate in **us-east-1** covering every name in `aliases`. Set with `aliases` or not at all — the pair is validated together. |
| `session_ttl_seconds`       | `3600`      | How long an admitted visitor's session cookie lasts. See *Choosing the session lifetime* below. |
| `oidc_client_id`, `oidc_redirect_uri`, `oidc_allowed_emails` | *(empty)* | **Required — the apply fails without them.** See the two-pass note below. The client secret is not here; it goes in an SSM SecureString out of band. |

The one `make`-level override is the build target:

| Variable | Default | Meaning                                                              |
| -------- | ------- | -------------------------------------------------------------------- |
| `ARCH`   | read from `terraform.tfvars` | `cargo lambda build` target. The Makefile takes `lambda_architecture` out of `terraform.tfvars`, falling back to `arm64` if the file is absent, so the binaries cannot be built for an architecture the functions are not deployed with. Override for a one-off build only. |

> **A manual override must still match.** `ARCH` selects what you *build*;
> `lambda_architecture` selects what the function *runs*. They agree by default
> because one is derived from the other — but `make build ARCH=x86_64` against
> an `arm64` deployment still deploys cleanly and fails at first invoke.

## First deploy

```bash
# 1. Authenticate to AWS (in your shell).
aws sso login          # or: aws configure

# 2. Configure the deployment: event id, region, origin domain.
cp infra/environments/dev/example.tfvars infra/environments/dev/terraform.tfvars
$EDITOR infra/environments/dev/terraform.tfvars

# 3. Build the Lambda binaries and stage them under .artifacts/.
make build

# 4. Review the plan, then deploy. The gate's signing key is generated and
#    written to both of its homes by this apply — there is no separate
#    bootstrap step. This apply also seeds the event item, the gate ruleset and
#    the open schedule, so what comes up is a stack that serves.
make plan
make apply
```

### The one sequencing step: OIDC

`oidc_redirect_uri` is a path on the host the dashboard is served from, and
`core` cannot derive it: `edge` consumes `core`'s KeyValueStore ARN, so `core`
depending on `edge` would be a module cycle.

**With a custom domain there is no sequencing step.** You already know the
hostname, so set `aliases`, `acm_certificate_arn` and
`oidc_redirect_uri = "https://waiting.example.com/admin/callback"` together and
apply once. Point the DNS record at the distribution afterwards.

Without one, the `*.cloudfront.net` name does not exist until the first apply,
so a brand-new deployment is a two-pass operation:

```bash
make apply                              # fails: the admin Lambda has no OIDC config
terraform -chdir=infra/environments/dev output cloudfront_domain_name

# Register the OIDC application with that host's /admin/callback, then:
aws ssm put-parameter --name /<name_prefix>/oidc-client-secret \
  --type SecureString --value '<secret>' --overwrite

$EDITOR infra/environments/dev/terraform.tfvars   # set the three oidc_* values
make apply
```

The apply **fails** on the first pass rather than succeeding and leaving an
admin Lambda that dies at Init on every invoke — a dead control plane whose only
symptom was a 502. The error names the value it needs and where to find it.

`make apply` prints the stack outputs, including the API invoke URL and the
table names. To exercise the deployed stack end to end — register, open, and
verify no two visitors get the same position — run the smoke test. It reads the
API URL, table names, open function, and event id from `terraform output` and
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
positions cannot rejoin. The deployment accepts one key at a time, so
there is no overlap period. Regenerate only before an event opens.

### Choosing the session lifetime

The CloudFront-path session has a fixed lifetime, not a sliding one: `generate_token` mints a
session valid for `session_ttl_seconds` (default 3600, set in `terraform.tfvars`) and
nothing re-issues it — a visitor still on the protected origin when it expires is logged out and
has to rejoin the queue, checkout included (ADR-0021 §5.3). There is no sliding alternative: the
one that existed lived in the origin authorizer, which was removed (ADR-0032), so this value is
the whole of the policy. Set `session_ttl_seconds` comfortably longer than the worst realistic time on the
protected origin — cart to confirmation, not the median — or a visitor can lose their admission to
nothing more than a slow checkout.

## Edge protection on a flat-rate plan (ADR-0038)

Two things stand between a script and the queue, and only one is optional.

**The API answers only CloudFront, always.** Every REST API method requires an API key that
Terraform generates and that only the distribution sends, as an `x-api-key` origin custom header.
A request straight to the `execute-api` URL is refused with a 403 and not billed. There is nothing
to configure. Apply the change that introduces it between events: the stage requires the key
the moment it deploys, while CloudFront takes minutes to add the header at every edge, so API
calls fail with 403 in that window. Anything you run against the API directly needs the key:
`terraform -chdir=infra/environments/dev output -raw api_origin_key`. The smoke test reads it
itself, and checks that a keyless request is refused.

**The edge web ACL is for a deployment on a CloudFront flat-rate plan.** The plan (Business,
$200/month, or above) bundles WAF and requires a web ACL. On pay-as-you-go the same ACL bills per
request inspected and a monthly fee per rule between events, so it is off by default. To run an
event under it:

1. Set `waf_enabled = true` in `terraform.tfvars` and `make apply`. This creates
   `<name_prefix>-edge` in us-east-1 and attaches it to the distribution. The rules and what each
   one does are in `docs/DESIGN.md` §8.
2. Subscribe the distribution to the plan in the CloudFront console (the distribution's pricing
   plan settings). The Terraform provider cannot do this yet (terraform-provider-aws#45450,
   PR #49235). The distribution already carries the Terraform-managed web ACL, so keep that one.
   Do not create or edit rules in the console: the next apply reverts them.
3. Load the waiting page in a private window. On the first load the page detours through
   `/_wr/verify.html` for a brief browser check, comes back, and the join goes through. The smoke test's
   CloudFront step now reports the join as challenged, which is the design: only a browser can
   solve the challenge.

Plan for the subscription's constraints before you start:

- **It is monthly.** While the distribution is subscribed, AWS refuses to delete it or remove its
  web ACL, even after you cancel, until the billing cycle ends. `make destroy`, or an apply with
  `waf_enabled = false`, fails until then (see Tear down).
- **Managed rules start in Count** (`waf_managed_rules_mode = "count"`, O5). Promote them to
  `enforce` only after reviewing an event's worth of their CloudWatch metrics (`docs/RUNBOOK.md`).
- **Browsers that refuse cookies cannot queue with JavaScript.** The WAF token is a cookie, and
  the waiting page says so rather than reloading in a loop. The no-JavaScript queue needs no
  token.

## Availability

A deployment lives in one region ([ADR-0034](adr/0034-single-region-failure-domain.md)). The
queue is only as available as that region's DynamoDB, SQS, API Gateway and Lambda. There is no
replica and no automatic failover, and one cannot be added as a setting: the queue's sequences
need a single writer. The gate runs at the edge, so during a regional impairment admitted visitors
keep their access. The operator can open the origin to everyone else by writing a fail-open window
straight to the gate's KeyValueStore (`docs/RUNBOOK.md`, "If the AWS region is impaired"). A hosted
waiting room would carry this risk for you; here it is yours, and that procedure is how you
manage it.

## Tear down

```bash
make destroy
```

Terraform prompts for confirmation before deleting anything (the target does
not pass `-auto-approve`). It reads the same `terraform.tfvars` you deployed
with, so leave that file in place until the stack is gone.

A distribution subscribed to a CloudFront flat-rate plan cannot be deleted, or shed its web ACL,
until the billing cycle in which you cancel ends (ADR-0038). Cancel the plan in the console after
the event, and run `make destroy` once the cycle has closed. Until then the stack stays up, and
the plan covers its edge. The rest of the stack costs what an idle deployment costs (N1).
