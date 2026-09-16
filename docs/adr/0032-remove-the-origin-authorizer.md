# ADR-0032: Remove the origin authorizer

**Status:** Accepted. Amends [0011](0011-session-cookie-after-token.md) and
[0024](0024-jws-credentials.md), which described a token-then-session handoff that no longer
has a second half. Narrows [0021](0021-edge-function-gate.md) from "the gate for CloudFront
deployments" to "the gate".

## 1. Context

The `authorizer` crate was the alternative gate: a Lambda the customer attaches at their own
origin, which decides locally with no backend call — session cookie, then admission token, then
protection-rule match, then a 302 to the waiting room. It was built and deployed on every
apply.

Nothing invoked it. The repository said so in three places of its own accord
(`infra/modules/authorizer/main.tf`, that module's `outputs.tf`, and `docs/DESIGN.md`):
*"The function is built and deployed, but nothing in this account invokes it: it attaches at
the customer's own origin."*

**As wired, it also protected nothing.** `protected_path_prefixes` defaulted to `[]` and its
description read "Empty means the whole origin is protected", but the module always set
`PROTECTED_PATH_PREFIXES = join(",", [])` — the empty string. The Rust read that as `Ok("")`,
split it, filtered out the empty element, and produced an empty rule vector; the
`vec![PathPrefix("/")]` fallback ran only when the variable was *unset*, which Terraform never
left it. `matches_any` on an empty slice is `false`, so `decide` took the unprotected
early-pass and forwarded every request. The description stated the exact inverse of the
behaviour.

Six of its environment variables — `SESSION_COOKIE_NAME`, `BYPASS_COOKIE_NAME`, `FAIL_CLOSED`,
`SESSION_MODE`, `SESSION_IDLE_SECS`, `SESSION_CAP_SECS` — were never set by Terraform at all,
so their code defaults were load-bearing and changing `session_cookie_name` at the root
silently desynchronized it.

It cost 1,481 lines of Rust, 273 of Terraform across six deployed AWS resources, a build
target, 685 lines of tests on every CI run, and **a standing SSM `GetParameter` grant on the
signing key for a function nothing calls**.

## 2. Decision

Remove it: the crate, the Terraform module, the module block and outputs in the dev root, the
build target, and the admission-token credential it was the only consumer of.

[ADR-0021](0021-edge-function-gate.md)'s CloudFront Function is now the gate, not *a* gate.

## 3. What stays, and why

- **`wr_common::rules` in full, including `matches_any` and `RequestView`.** The Rust matcher
  is the reference implementation the conformance vectors are generated from
  (`crates/wr-common/tests/vectors.rs`), and `gate.conformance.test.js` checks the shipping
  CloudFront Function against those vectors. That chain is the only thing pinning the gate's
  matching semantics. The authorizer was a *consumer* of `rules`, not its reason for existing.
- **`wr_common::crypto::Session`**, which `generate_token` mints and the gate verifies.
- **`Kind` as a one-variant enum.** `Kind::Token` and its `vwr/jws/token/v1` label go with
  `AdmissionToken`, but the enum does not collapse into a constant: a second credential kind
  must be forced to carry a label of its own, because two kinds sharing a derived key is
  precisely the confusion the type exists to prevent. `Kind::Session`'s label stays
  byte-identical — `gate.js.tftpl` hardcodes `vwr/jws/session/v1` and derives the same key.

## 4. Consequences

- **Six deployed AWS resources destroyed**, and the signing key loses a reader.
- **`Key::AdmissionToken` (`TKN#`) goes**, so the `Tokens` table holds two tagged kinds rather
  than three: operator OIDC sessions (`SESS#`) and pending PKCE logins (`PKCE#`). The
  key-collision test still guards them, at two kinds.
- **`/update_session` goes with it.** [ADR-0016](0016-admin-oidc-dynamodb-sessions.md) defers
  that route explicitly "with the authorizer"; it returned 501 and nothing wrote
  `PositionStatus::Completed` or `Abandoned`. **F3.10 is retired.** `/metrics` is removed from
  the API Gateway route table in the same change — it was routed there with no handler in the
  axum router, so a call to the documented endpoint got a bare 404. Between them that is six
  API Gateway resources fewer.
- **F3.6 is retired** — there is no admission token to sign separately from a session.
- **F3.7 is retired.** `SessionMode::Sliding` was the only sliding-session implementation, and
  the edge does not re-issue a cookie, so a visitor still on the origin when
  `SESSION_TTL_SECS` lapses returns to the queue. Retiring the requirement records that the
  product does not do this, rather than leaving a `MUST` with no mechanism. Renewing at the
  edge (a viewer-response function that re-signs a near-expiry cookie under a hard cap) is the
  design that would re-raise it.
- **F3.3, F3.4 and F3.5 survive, reworded against the gate.** The properties they name —
  verify without a backend call, refuse a request with no valid credential, establish a
  session once a position is reached — all still hold; only the component changed.
- **N4 (GovCloud) is restated, not retired.** CloudFront Functions do not exist in GovCloud and
  this was the gate that did. Rather than drop a requirement the product may still want, N4 now
  says commercial regions are supported and GovCloud is a topology with no shipped gate. This
  is the one consequence that is a product decision rather than a code one, and it is stated
  here so it is not buried in a diff.
