# ADR-0016: Authenticate the admin UI with OIDC, backed by DynamoDB

**Status:** Accepted

## Context

The admin control plane is a single Axum Rust Lambda (ADR-0014) whose routes are
`authorization = "AWS_IAM"` — the caller signs each request with SigV4. That is the cheapest
secure control-plane auth for a machine caller, and it is correct for the JSON API (F5.5).

It is also **unusable from a browser**. A browser cannot SigV4-sign a navigation or a
stylesheet `<link>`, so the rendered dashboard from ADR-0014 cannot be opened by the operator
it was built for. The dashboard works only through a signing proxy or a scripted client — not
the thing an operator points a browser at during an event.

Making it browser-usable needs an auth mechanism a browser can complete. The options:

- **A — OIDC Authorization Code + PKCE**, session in a cookie, on the existing admin Lambda.
  Standard browser login against the operator's identity provider (IdP). Per-operator identity.
- **B — CloudFront + WAF gate** (IP allowlist, or a shared signed cookie checked at the edge),
  origin auth `NONE`. Browser-openable, but IP ≠ identity and it authenticates the network
  location, not the person.
- **C — Move the admin UI to a long-running container (ECS)** so a normal session-based web
  framework applies directly. Browser-usable and WebSocket-capable, but a standing task
  reverses N1 (near-zero idle cost) — the system's founding principle.

Two facts shape option A on Lambda:

- **In-memory session state does not survive.** The reference `openidconnect` example holds the
  PKCE verifier + nonce in an in-process `HashMap` and sessions in a `tower-sessions`
  `MemoryStore`. On Lambda the login and the callback can land in different execution
  environments, so the verifier written at `/admin/login` is absent at `/admin/callback`. The
  transaction state must live outside the process.
- **The session plane was deferred.** ADR-0014 and the admin design note explicitly deferred
  `/update_session` and the authorizer/session machinery to post-MVP. OIDC un-defers a session
  concept for the admin surface specifically.

## Decision

Option A. Authenticate the admin UI with **OIDC Authorization Code + PKCE** on the existing
admin Lambda, storing the login transaction and the session in **DynamoDB** rather than in
process memory.

- **Routes.** Add `GET /admin/login` (redirect to the IdP), `GET /admin/callback` (exchange
  code, verify the ID token against the issuer's published JWKS, create a session), and
  `GET /admin/logout`. `GET /admin` and every `/admin/*` action become **session-cookie-gated**:
  no valid session → 302 to `/admin/login`.
- **This replaces SigV4 as the admin gate.** The `/admin*` API Gateway routes flip
  `AWS_IAM → NONE`; the Lambda enforces auth via the session cookie. A CloudFront behaviour
  fronts `/admin*` (verified separately). The JSON API's SigV4 story for machine callers is a
  separate surface and is out of scope here.
- **State in DynamoDB, TTL-expired.** Reuse the existing `Tokens` table (PK `request_id`),
  which already holds session-adjacent metadata, rather than adding a table (N6):
  - **Login transaction** `pkce#<state>` → PKCE verifier + nonce, DynamoDB TTL ~10 min. Survives
    the login→callback round-trip across cold starts — the case the in-memory `HashMap` cannot.
  - **Session** `session#<id>` → subject + claims + expiry, DynamoDB TTL ~8 h. The cookie carries
    only the opaque session id; the record lives in DynamoDB. TTL auto-expires both — no cleanup
    job (consistent with the controller-driven-expiry stance of ADR-0006 applies to positions,
    not here; sessions genuinely are TTL-expirable because nothing reconstructs them).
- **Crypto.** `jsonwebtoken` with the `rust_crypto` feature (pure-Rust `RustCrypto`, not `ring`)
  verifies the ID/access token signature; `reqwest` uses `rustls-tls` with the **aws-lc-rs**
  provider for discovery/JWKS/token calls. The aws-lc-rs-only rule (tech.md) governs the
  TLS/HMAC/signing backend; JWT signature verification via RustCrypto is compatible and does not
  introduce `ring`.
- **Client secret** in an encrypted (SecureString) SSM Parameter Store parameter, read at Init
  via `ssm:GetParameter` with decryption — the same pattern the authorizer module already uses
  for the per-deployment signing key. No Secrets Manager (avoids adding a service; Parameter
  Store SecureString is already in the tree).
- **Provider-agnostic.** Issuer, client id, and redirect URI are configuration; the flow is
  standard OIDC discovery. Dev wiring defaults to the reference IdP used by the example.

## Consequences

- The dashboard becomes **operable by a human in a browser** with per-operator identity — the
  gap that made ADR-0014's UI unusable is closed.
- **N1 is preserved.** No standing compute: still one idle Lambda. This is the reason option C
  (ECS) was rejected despite being the more natural home for sessions and WebSockets.
- **The session plane is partially un-deferred**, but only for admin login — not the visitor
  authorizer/session machinery, which stays deferred. `/update_session` remains out of scope.
- **New dependencies** (`openidconnect`, `oauth2`, `jsonwebtoken`, `tower-sessions`-style
  session handling, `reqwest`) roughly double the admin crate's tree. Justified: browser auth is
  not something to hand-roll, and these are the maintained, standard crates for it.
- **Reverses the admin=SigV4 decision** for the browser surface. Machine callers that relied on
  signing the admin API must move to the session flow or a separate signed API surface; this ADR
  does not preserve a dual auth path (conventions.md: replace, don't deprecate).
- Live updating, if wanted later, is a `GET /admin/state` JSON endpoint the existing vanilla-JS
  poller reads — no WebSocket, no new plane. Recorded here so the transport choice is not
  re-litigated: polling, not push, for an operator control readout.

Additive to ADR-0014; reverses its SigV4 gate for the browser surface. Satisfies the
operator-usable-dashboard requirement; keeps N1 and N6.
