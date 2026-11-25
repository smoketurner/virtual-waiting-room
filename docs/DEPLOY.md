# Deploying the Virtual Waiting Room

How to build the Lambdas, configure a deployment, and stand the stack up in your own AWS
account. [`RUNBOOK.md`](./RUNBOOK.md) covers running an event on it.

## Build and deploy

The seven Lambdas (`assign_position`, `open_event`, `read`, `admin`, `controller`,
`generate_token`, `nojs`) are `provided.al2023` custom-runtime functions: zipped static
binaries, no containers.

`make build` runs `cargo lambda build --release --output-format zip` once per crate (every
binary is named `bootstrap`) and writes `.artifacts/<crate>/bootstrap/bootstrap.zip`, linking with
`zig`. Terraform uploads each zip directly, and `source_code_hash` redeploys a rebuilt one. There
is no placeholder binary: a missing zip fails the plan rather than standing up a stack that admits
nobody.

## Prerequisites

- **Rust**, **cargo-lambda** (`cargo install cargo-lambda`) and **zig**. Verified with
  cargo-lambda 1.9.2.
- **Terraform** >= 1.9 (verified with 1.16.1).
- **uv**, to run the smoke test. It installs `boto3` from the script's inline metadata.
- **AWS credentials** with rights to create the stack: Lambda, DynamoDB, SQS, API Gateway, IAM,
  CloudFront, SSM, CloudWatch, S3 and EventBridge Scheduler. `aws sso login` or `aws configure`.
- **The S3 state backend** named in `infra/environments/dev/versions.tf`. `make init` wires it up.
- **`infra/environments/dev/terraform.tfvars`**, copied from `example.tfvars` (it is gitignored).

## Make targets

All run from the repository root against `infra/environments/dev`. `plan`, `apply` and
`validate` run `build` and `init` first.

| Target          | What it does                                                          |
| --------------- | --------------------------------------------------------------------- |
| `make build`    | Cross-compile every Lambda to a zip under `.artifacts/<crate>/`.      |
| `make init`     | `terraform init` (idempotent).                                        |
| `make plan`     | Build, then `terraform plan`.                                         |
| `make apply`    | Build, then `terraform apply`. The real deploy.                       |
| `make destroy`  | Tear the stack down. Terraform prompts for confirmation.              |
| `make validate` | Build, then `terraform validate` the dev root.                        |
| `make fmt`      | `terraform fmt -recursive` across `infra/`.                           |
| `make clean`    | Remove `.artifacts/`.                                                 |
| `make load`     | Run the local load harness (`VISITORS`, `SECONDS`, `POLLING`).        |
| `make stack-emulator` | Build and start the AWS Emulator the local stack runs against.  |
| `make stack`    | Run the whole system locally and check the design ([`LOCAL_STACK.md`](LOCAL_STACK.md)). |
| `make help`     | List the targets.                                                     |

## Configuration

All deployment configuration lives in `infra/environments/dev/terraform.tfvars`. The Makefile
passes no `-var`, because a command-line `-var` would override the file. `example.tfvars` shows
the full shape. The values you will normally set:

| Variable | Default | Meaning |
| --- | --- | --- |
| `region` | `us-east-1` | AWS region for the regional resources. |
| `aws_profile` | `""` | Named AWS profile. Empty uses the default credential chain. |
| `name_prefix` | `vwr-dev` | Prefix for every resource name. Change it to run a second deployment in one account. |
| `event_id` | `default` | The one event this deployment serves. Must not contain `#`. |
| `lambda_architecture` | `arm64` | `arm64` or `x86_64`. Must match the built binaries (see `ARCH` below). |
| `client_origin_domain_name` | `""` | Bare host of the protected origin, no scheme or path. Empty protects the built-in demo origin, which guards nothing real. See [Locking the origin to the distribution](#locking-the-origin-to-the-distribution). |
| `aliases`, `acm_certificate_arn` | `[]`, `""` | Hostnames viewers use, and a **us-east-1** ACM certificate covering them. Set both or neither. Empty uses the `*.cloudfront.net` name. |
| `oidc_issuer` | `https://us.vouch.sh` | OIDC issuer for the admin login. |
| `oidc_client_id`, `oidc_redirect_uri`, `oidc_allowed_emails` | `""` | **Required; the plan fails without them.** See [OIDC](#oidc-the-one-sequencing-step). The client secret goes in SSM, not here. |
| `oidc_require_email_verified` | `true` | A login needs the provider's `email_verified` claim to be true. Set `false` only for a provider that omits the claim; an explicit `false` is still refused. |
| `alarm_sns_topic_arn` | `""` | SNS topic every alarm notifies on alarm and on recovery. Empty leaves the alarms changing state with nobody told. |
| `table_deletion_protection` | `false` | Deletion protection on the four tables. Turn on for a real event; it must be off to change `event_id` or to destroy. |
| `controller_reserved_concurrency` | `2` | Concurrency reserved for the controller so ingest cannot starve it. |
| `api_join_throttle_rate_limit`, `api_join_throttle_burst_limit` | `7000`, `3500` | Stage throttle on `POST /_wr/v1/join`. Below the account's 10,000/s on purpose, so a crowd of joins cannot starve admission and the console. Raise it with the account quota, keeping it under that quota. |
| `api_throttle_rate_limit`, `api_throttle_burst_limit` | `10000`, `5000` | Stage throttle on every other method. Lower to cap what the read and admission functions can be asked for. |
| `admission_rate` | `5` | Visitors per second, seeded onto the event. Changed live from the dashboard. |
| `gate_rules` | `""` | Which requests the gate covers, one rule per line, same grammar as the dashboard's rules form (`p /checkout`, `c <cookie>`, `u <agent substring>`, `h <header> <value>`). Empty is dormant: everything passes through. |
| `starts_at`, `starts_at_timezone` | `""`, `UTC` | When the event opens, local date-time with no zone, and its IANA zone. Set, the first apply seeds the event in the pre-queue phase and arms the open schedule. Empty leaves the event idle until an operator presses **Open now**. |
| `session_ttl_seconds` | `3600` | Lifetime of an admitted visitor's session. See [below](#choosing-the-session-lifetime). |
| `warm_throughput_write_units`, `warm_throughput_read_units` | `0` | Table pre-warm ahead of an event; 0 is none. See the runbook's readiness checklist. |
| `poll_floor_ms`, `poll_ceiling_ms`, `poll_divisor` | `5000`, `30000`, `10` | Adaptive poll interval published on `/status` ([ADR-0023](adr/0023-adaptive-poll-interval.md)). |
| `flat_rate_plan_tier` | `null` | `BUSINESS` or `PREMIUM` subscribes to a CloudFront flat-rate plan and attaches the edge web ACL. See [Edge protection](#edge-protection-on-a-flat-rate-plan). |
| `waf_managed_rules_mode` | `count` | `count` or `enforce` for the managed rules (plan only). |
| `waf_join_ip_limit` | `100` | Token requests plus joins per IP per 5 minutes before a CAPTCHA (plan only). |
| `waf_nojs_wait_ip_limit` | `300` | No-JavaScript waiting-page requests per IP per 5 minutes before a 429 (plan only). |
| `waf_require_token_cookie` | `true` | A JavaScript API request holding a WAF token must carry it as the cookie, or it is blocked; `false` counts instead (plan only). |
| `signing_key_generation`, `origin_key_generation` | `1`, `1` | Raise by one to rotate the session signing key or the CloudFront-to-API key. See [Rotating keys](#rotating-keys). |
| `expected_join_rate` | `0` | Peak joins per second expected, 0 if unknown. Above 4,000 the plan fails unless `warm_throughput_write_units` covers it. |
| `nojs_enabled` | `true` | `false` closes the no-JavaScript queue at the edge (plan only). |

Terraform seeds `admission_rate`, `gate_rules` and the open schedule once; after that the
dashboard owns them.

**Running another event on the same stack.** Change `event_id` and apply. Terraform re-seeds the
event item and recreates the `PreQueue` and `Positions` tables empty, since their rows carry no
event and a returning browser's old row would otherwise resolve against the new cohort. Turn
`table_deletion_protection` off first. `scripts/reset-env.py` remains a test convenience.

**`ARCH`** is the one `make`-level override. `make build` reads `lambda_architecture` from
`terraform.tfvars` (falling back to `arm64`), so the build and the deployed functions agree by
default. `make build ARCH=x86_64` against an `arm64` deployment applies cleanly and fails at
first invoke; use it only for a one-off build.

### The path the room reserves

`/_wr/` is the one path prefix the room occupies on the customer's hostname: the waiting page
(`/_wr/waiting.html`), the public API (`/_wr/v1/*`) and the operator console (`/_wr/admin`,
`/_wr/static/*`). Every other path is forwarded to the origin, so the origin's own `/v1`, `/admin`
or `/static` paths are untouched. A path the origin serves under `/_wr/` is not reachable through
the distribution.

### Locking the origin to the distribution

The gate only protects requests that pass through CloudFront. An origin that also answers on its
own hostname can be reached directly, skipping the queue, and that hostname is discoverable from
certificate transparency logs and DNS history.

CloudFront sends every request to the client origin an `x-wr-origin-verify` header carrying a
secret generated at apply. A viewer cannot supply it: CloudFront overwrites a header of that name.
Read the value with:

```bash
terraform -chdir=infra/environments/dev output -raw client_origin_secret
```

Configure the origin (its load balancer, web server or application) to refuse any request whose
`x-wr-origin-verify` does not equal that value. The stack cannot do this for you, and until it is
done the header protects nothing. Restricting the origin's network to the
`com.amazonaws.global.cloudfront.origin-facing` prefix list as well is stronger still.

## First deploy

```bash
aws sso login                                   # or: aws configure
cp infra/environments/dev/example.tfvars infra/environments/dev/terraform.tfvars
$EDITOR infra/environments/dev/terraform.tfvars
make plan
make apply
```

The apply generates the gate's signing key and seeds the event item, the gate ruleset and the
open schedule, so the stack serves without any further setup step. Do not run
`scripts/reset-env.py` as setup: it is a destructive test script.

### OIDC: the one sequencing step

`oidc_redirect_uri` is `https://<host>/_wr/admin/callback`, a path on the distribution, and the
admin Lambda fails at plan (a `precondition`) if it, `oidc_client_id` or `oidc_allowed_emails`
is empty.

**With a custom domain**, set `aliases`, `acm_certificate_arn` and
`oidc_redirect_uri = "https://waiting.example.com/_wr/admin/callback"` together and apply once. Point
DNS at the distribution afterwards.

**Without one**, the `*.cloudfront.net` name does not exist until the distribution does. Because
the precondition is checked at plan, an apply with the OIDC values empty creates nothing. Apply
first with placeholder values, then the real ones:

```bash
# terraform.tfvars, first pass: real allowlist, placeholder client
#   oidc_client_id      = "pending"
#   oidc_redirect_uri   = "https://pending.invalid/_wr/admin/callback"
#   oidc_allowed_emails = "you@example.com"
make apply
terraform -chdir=infra/environments/dev output cloudfront_domain_name

# Register the OIDC application with https://<that host>/_wr/admin/callback, then:
aws ssm put-parameter --name /<name_prefix>/oidc-client-secret \
  --type SecureString --value '<secret>' --overwrite

$EDITOR infra/environments/dev/terraform.tfvars   # the real oidc_client_id and oidc_redirect_uri
make apply
```

### Smoke test

The smoke test registers visitors, opens the event, checks that no two visitors get the same
position, and checks that a request without the API key is refused. It reads everything it needs
from `terraform output` and never modifies Terraform state:

```bash
AWS_PROFILE=dev-admin uv run scripts/smoke_test.py
```

## Secrets

- **Gate signing key.** `random_bytes` generates it during apply, and Terraform writes the same
  value to the SSM SecureString `/<name_prefix>/signing-key` (read by `generate_token`) and the
  KeyValueStore key `k` (read by the gate). It lives in Terraform state, which the S3 backend
  encrypts. There is one key per generation; see [Rotating keys](#rotating-keys).
- **CloudFront-to-API key.** Generated the same way, sent by the distribution as `x-api-key`
  ([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)).
- **OIDC client secret.** Terraform creates `/<name_prefix>/oidc-client-secret` as a placeholder
  under `ignore_changes`; you write the real value with `aws ssm put-parameter` as above.

### Rotating keys

Both keys rotate by raising a number in `terraform.tfvars` and applying. Each keeps its previous
generation valid, so a rotation is not an outage.

- **`signing_key_generation`.** The new key signs from the apply on. The previous generation
  becomes the gate's `kp`, which it tries when `k` refuses, so sessions already issued stay good
  until they expire. A `SIGNING_KEY_FINGERPRINT` in the minting functions' environment changes
  with the key, which retires warm environments still holding the old one. Rotating twice within
  one `session_ttl_seconds` drops the oldest sessions: the generation before the previous one is
  destroyed.
- **`origin_key_generation`.** API Gateway honours the new key at once and CloudFront takes
  minutes to start sending it. The previous key stays attached to the usage plan until the next
  rotation, so the API keeps answering while the distribution deploys.

After a suspected compromise, rotate twice (waiting out one session lifetime between them if
visitors are inside) so the exposed generation is gone.

### Choosing the session lifetime

A session lasts exactly `session_ttl_seconds` from the visitor's first admission and nothing
renews it: asking the waiting page for it again returns one that expires at the same moment, and
after that the place is used up (410) and they take a new one
([#237](https://github.com/smoketurner/virtual-waiting-room/issues/237)). A visitor still on the
origin when it lapses is sent back to the queue, checkout included
([ADR-0021](adr/0021-edge-function-gate.md), [ADR-0032](adr/0032-remove-the-origin-authorizer.md)).
Set it longer than the worst realistic time on the origin (cart to confirmation, not the median).

## Edge protection on a flat-rate plan

See [ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md) and `docs/DESIGN.md` §8 for the rules.

**The API answers only CloudFront, always.** Every REST API method requires an API key that only
the distribution sends. A direct request to `execute-api` gets a 403 and is not billed. Nothing to
configure, but:

- When first applying this to an existing stack, do it between events. The stage requires the
  key immediately, while CloudFront takes minutes to add the header at every edge, so API calls
  fail with 403 in that window.
- Anything you call the API with directly needs the key:
  `terraform -chdir=infra/environments/dev output -raw api_origin_key`.

**The edge web ACL needs a flat-rate plan**, which bundles WAF; the default is neither, because
both bill between events. To run an event under a plan:

1. Set `flat_rate_plan_tier = "BUSINESS"` (or `"PREMIUM"`) and `make apply`. This creates the web
   ACL `<name_prefix>-edge` in us-east-1 and subscribes the distribution, ACL and KeyValueStore to
   the plan. **Billing starts when you confirm the apply.** AWS approval can take up to 30 minutes;
   `terraform -chdir=infra/environments/dev output flat_rate_plan_status` reads `ACTIVE` when done.
   Do not edit the rules or plan in the console: the next apply reverts them.
2. Load the waiting page in a private window. It detours through `/_wr/verify.html` for a brief
   browser check, returns, and the join goes through. The smoke test now reports its CloudFront
   join as challenged; that is expected, since only a browser can solve the challenge.

Know the constraints first:

- **It is monthly.** AWS refuses to delete a subscribed distribution or remove its web ACL until
  the billing cycle in which the plan is cancelled ends. See [Tear down](#tear-down).
- **Upgrading is immediate; downgrading** (Premium to Business) takes effect at the end of the
  billing period.
- **Managed rules start in Count.** Promote to `enforce` only after reviewing an event's worth of
  their CloudWatch metrics ([`RUNBOOK.md`](./RUNBOOK.md), After the event).
- **Browsers that refuse cookies cannot use the JavaScript queue**, because the WAF token is a
  cookie. The waiting page says so. The no-JavaScript queue needs no token.

## Availability

A deployment lives in one region with no failover
([ADR-0034](adr/0034-single-region-failure-domain.md)). In a regional impairment admitted visitors
keep access but nobody new is admitted; the procedure is in
[`RUNBOOK.md`](./RUNBOOK.md#if-the-aws-region-is-impaired).

## Tear down

```bash
make destroy
```

Terraform prompts before deleting anything. Keep `terraform.tfvars` until the stack is gone. On a
flat-rate plan the first run cancels the plan and then fails deleting the distribution; run it
again after the billing cycle closes.
