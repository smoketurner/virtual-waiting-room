# ADR-0020: CloudFront signed cookies are the admission gate

**Status:** Superseded by [ADR-0021](0021-edge-function-gate.md).

**Supersedes:** the origin-authorizer half of [ADR-0009](0009-fail-open.md)
as the *primary* gate. The authorizer remains for origins the customer controls.

## Context

A waiting room is advisory unless something refuses an un-admitted visitor at the protected
origin. Until now that something was `modules/authorizer`: a Rust Lambda invoked with the
ALB / API Gateway request shape, sitting at the origin, answering 200 to serve or 302 to send
the visitor to wait.

That design assumes the customer lets us run code in their request path — via a CloudFront VPC
origin, or an ALB in their VPC. Three problems surfaced when we tried to deploy it:

1. **It does not work for an origin we do not own.** The dev deployment fronts
   `www.google.com`. There is no seam to insert a Lambda into, so the authorizer cannot be
   exercised end to end at all. That is not a quirk of the test origin: any customer whose
   origin is SaaS, a third party, or simply not in a VPC we can reach has the same problem.
2. **VPC origins carry hard constraints.** They forbid Lambda@Edge origin triggers, require an
   internet gateway present but unused, and do not exist in GovCloud. The topology was
   load-bearing and unproven.
3. **It puts compute on the highest-RPS path in the system.** The authorizer sees every request
   to a protected path from every admitted visitor — origin scale, not queue scale. At a
   500/s admission rate with ten-minute sessions that is on the order of 10⁴ requests per
   second, each one paying a Lambda invocation.

The alternative gates considered were a CloudFront Function at viewer-request (custom JS
verifying the existing HMAC credential) and Lambda@Edge. The Function is cheap and origin-
agnostic, but needs the symmetric signing key readable at the edge and cannot record arrivals.
Lambda@Edge is neither cheap nor available alongside VPC origins.

## Decision

**CloudFront itself is the gate.** The protected behaviour names a trusted key group. A
request carrying valid signed cookies reaches the origin; one without gets a 403 from the edge,
mapped by `custom_error_response` to the waiting page. No custom code runs per request.

`generate_token` mints the cookies. It checks the visitor's position against `serving_counter`,
records the arrival, and signs a custom policy with **RSA PKCS#1 v1.5 over SHA-256**.

Consequences that are not obvious and are load-bearing:

- **SHA-256, not SHA-1.** CloudFront defaults to SHA-1 and accepts SHA-256 only when the
  `CloudFront-Hash-Algorithm=SHA256` cookie is sent. This matters because `aws-lc-rs` — the
  sole crypto backend (`tech.md`) — exposes no RSA-PKCS1-SHA1 *signing* encoding, only SHA-1
  *verification* parameters. Sending the hash-algorithm cookie is what keeps the gate inside
  the crypto policy instead of forcing in the `rsa` crate or unsafe FFI.
- **`Path=/`, no `Domain`.** A narrower path means the browser never returns the cookies on the
  protected request, which presents as every admitted visitor getting a 403.
- **`error_caching_min_ttl = 0`.** CloudFront caches its own error responses. A cached 403
  would keep serving the waiting page to a visitor who has since been admitted.
- **`Resource: "https://*"`.** The key group is bound to one distribution's behaviour, so only
  that distribution honours these cookies and the meaningful limits are the expiry and the key.
  Naming the distribution's own host would make the edge module feed its domain back to the
  core module that signs — a dependency cycle between the two.
- **The key pair and key group live in `modules/core`,** not `modules/edge`. They are global
  CloudFront resources referencing no distribution, so owning them beside the Lambda that signs
  with them gives it the key-pair id without the cycle above.

**Arrivals move to token issuance.** The authorizer was the only writer of `arrivals#*`, which
tied the outflow controller's no-show correction to the gate being deployed. `generate_token`
writes them now: a visitor asking for a credential is a visitor showing up. This slightly
overcounts — someone who fetches cookies and never uses them reads as an arrival — and in
exchange the controller works regardless of which gate a deployment runs.

## Consequences

The gate now works against any origin, costs nothing per request, needs no VPC, and is
exercisable end to end in a dev stack pointed at a third-party site.

What is given up: **per-request protection rules.** The authorizer could match on header,
cookie, and user agent; CloudFront matches on path pattern via cache behaviours only. A
deployment needing the finer rules runs the authorizer at its own origin, which is why the
crate and module are kept rather than deleted.

The private key is generated at apply time and lives in Terraform state. That is the trade for
a stack that stands up in one command; a deployment whose threat model excludes state supplies
the pair out of band and points the SSM parameter at it.

A 403 is a poor answer for a visitor whose cookies merely expired mid-session — they see the
waiting page again and rejoin the back of the queue rather than being told what happened.
Refining that needs the expiry to be visible to the page, which is follow-on work.
