# ADR-0024: Carry credentials as a JWS

**Status:** Accepted

**Amends:** [ADR-0011](0011-session-cookie-after-token.md), which established the two credentials
and their domain separation. The separation stands; how it is achieved changes.

## 1. Context

Admission tokens and session cookies were a bespoke encoding: `base64url(payload).base64url(mac)`,
where the payload was `u16`-length-prefixed UTF-8 fields followed by big-endian `u64` timestamps,
and the MAC covered a one-byte kind tag prepended to that payload.

It worked and it was compact. Two things argued against keeping it.

Anyone who has to validate a credential has to implement the format first. Today that is only our
own code, so the cost is hidden. It stops being hidden the moment a customer's origin wants to
check the cookie itself, or read which visitor it belongs to — at which point we are shipping a
specification instead of naming a standard.

And it is undiagnosable. Reading a credential means decoding length prefixes by hand. A JWT goes
into any JWT tool.

## 2. Decision

Both credentials are **JSON Web Signatures in compact serialization**, `HS256` — a JWT.

Claims are registered names, so the payload reads the same to any tool: `aud` the event id, `sub`
the request id, `exp` the hard expiry, and `iat` on a session.

The Rust side uses the `jsonwebtoken` crate, already in the tree for OIDC verification, on
`aws-lc-rs`. The `CloudFront` Function hand-rolls verification because its runtime has no library
to call.

### 2.1 Domain separation moves from a tag to a key

ADR-0011 required that a token never validate as a session. That was a kind byte prepended to the
MAC input, so the wrong kind failed the signature rather than a check.

The property is kept and the mechanism changes: **each kind signs under its own derived key**,
`HMAC-SHA256(secret, label)` with distinct constant labels. A token presented as a session fails
`BadSignature`, exactly as before.

The alternative — a `typ` claim — is worse in a way worth recording. It is read *after* verifying,
so admitting the wrong credential becomes a check someone can forget. A derived key cannot be
forgotten, because nothing else validates.

### 2.2 HS256 is forced, not chosen

The `CloudFront` Functions `crypto` module exposes `createHash` and `createHmac` over `md5`, `sha1`
and `sha256`. Nothing else. So `RS256`, `ES256`, `HS384` and `HS512` cannot be verified at the
edge, and **JWE cannot be decrypted there at all** — an encrypted credential would force the gate
onto `Lambda@Edge`, which cannot read a `KeyValueStore` and puts billable compute back in the
request path, undoing [ADR-0021](0021-edge-function-gate.md).

JWE would also protect nothing. The claims are the event id, the visitor's own request id, and two
timestamps. The visitor already knows all of it.

### 2.3 The algorithm is pinned, never read

`alg` is compared against the one value this deployment issues and never used to select a verifier,
so `alg: none` and a downgrade are both refused.

The gate parses the header rather than matching it as an encoded string. Matching the string would
be smaller and would tie the edge to the exact field order the Rust library emits — a library
upgrade that reordered `typ` and `alg` would reject every credential in flight. A test builds its
credentials with the fields in the opposite order for that reason.

## 3. Consequences

- **Symmetric signing keeps the key-exposure property unchanged.** The edge holds a secret that can
  mint, not merely check. That was true before this change and remains true; it is not what this
  ADR addresses.
- The edge derives the session key per request — one extra HMAC, against a measured budget of 11 of
  100. Terraform has no HMAC function, so it cannot write a reduced key at apply time; the edge
  holds the deployment secret.
- The gate's verifier got **smaller**: the length-prefixed walk and its cursor are gone, replaced by
  a split, an HMAC and a `JSON.parse`.
- Credentials are longer. JSON claims with registered names cost more than packed binary. Nothing
  here is near a cookie size limit.
- The conformance vectors are regenerated. They still come from the real Rust signer and still run
  against the shipped `gate.js` under `node:vm`, in both directions.
- A deployment does not outlive its event, so there is no migration: credentials minted under the
  old format never meet a verifier that expects the new one.
