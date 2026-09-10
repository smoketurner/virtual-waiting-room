# ADR-0021: Move the gate to a CloudFront Function

**Status:** Proposed — spike output, revised after architecture review and measured against a real
function and key value store. The direction is sound, §4.1 is settled and the gate is demonstrated
to work; §4.2 and §5 carry the remaining open decisions, and §7 lists what is still unmeasured.

**Supersedes if accepted:** [ADR-0020](0020-cloudfront-signed-cookie-gate.md) entirely. It does
**not** restore [ADR-0009](0009-fail-open.md) on its own — see §5.1.

## 1. Context

ADR-0020 made CloudFront's trusted key group the gate. That bought two things worth keeping: the
gate costs nothing per request, and it works against an origin we cannot put code near. What it
gave up is that CloudFront **verifies a signature; it does not decide**.

| Issue | What a decision point restores |
|---|---|
| [#58](https://github.com/smoketurner/virtual-waiting-room/issues/58) | The *mechanism* to bypass the gate. Not the detection of when to — see §5.1 |
| [#60](https://github.com/smoketurner/virtual-waiting-room/issues/60) | A dormant state, so standby passes traffic through untouched |
| [#63](https://github.com/smoketurner/virtual-waiting-room/issues/63) | Nothing yet — revocation needs a design this ADR does not have (§5.2) |
| [#64](https://github.com/smoketurner/virtual-waiting-room/issues/64) | Origin 403s stop being rewritten as the waiting page |
| [#66](https://github.com/smoketurner/virtual-waiting-room/issues/66) | Header, cookie and user-agent rules |
| [#72](https://github.com/smoketurner/virtual-waiting-room/issues/72) | XHR refused with a machine-readable answer, not an HTML page |
| [#73](https://github.com/smoketurner/virtual-waiting-room/issues/73) | A reason attached to every refusal |

ADR-0020 rejected this option because a CloudFront Function "needs the symmetric signing key
readable at the edge and cannot record arrivals." Both objections dissolve: arrivals are recorded
one step earlier by `generate_token` (§3.1), and the key at the edge is unavoidable in any design
that verifies at the edge, so the question is which edge store holds it (§4.1).

## 2. Verified constraints

Checked against AWS documentation, not assumed.

**The runtime can do the work.** JavaScript runtime 2.0 provides a `crypto` module with
`crypto.createHmac(algorithm, key)` for `sha256`, and `hmac.digest()` in `hex`, `base64` or
`base64url`. `Buffer` (including `base64url`), `atob`/`btoa`, `TextEncoder`/`TextDecoder` and
`querystring` are all available.

**The runtime cannot do anything else.** No network access of any kind — "XHR, HTTP(S), and socket
are not supported." No environment variables; configuration must come from a KeyValueStore. No
timers, and the function must run synchronously to completion. `Date` returns the function's start
time for the whole run.

**The gate cannot see the request body.** "CloudFront Functions can't access the body of the HTTP
request." Queue-it's connectors match triggers against up to 2 KB of body. Ours cannot, so edge
rules are bounded to path, header, cookie and user agent — exactly what `ProtectionRule` already
implements in `crates/authorizer/src/lib.rs`, and exactly what F0.6 asks for.

**There is no timing-safe comparison.** The `crypto` module offers `createHash`, `createHmac`,
`update` and `digest` and nothing else; neither `Buffer.prototype.equals` nor `Buffer.compare`
carries a constant-time guarantee. This is a real regression against `aws_lc_rs::hmac::verify`,
whose constant-time property `crates/wr-common/src/crypto.rs` relies on by comment. The mitigation
is a double-HMAC compare — MAC both the received and computed digests under the same key and
compare those — which costs two further HMAC operations and must be included in the compute budget.

**Execution time is observable and has a name.** *Compute utilization*, 0–100, "the amount of time
that the function took to run as a percentage of the maximum allowed time," reported by
`aws cloudfront test-function` and published to CloudWatch.

**The budgets are tight and shape the design:**

| Limit | Value | Consequence here |
|---|---|---|
| Function size | 10 KB, not adjustable | Rules and state live in the store, not the code |
| KeyValueStore per function | **1** | One store holds everything the edge reads |
| **Value size** | **1 KB** | Binding. ~45 rules per value (§6); 100 rules needs 3 keys |
| Store size | 5 MB | Not binding |
| Keys per update call | 50, or 3 MB | A multi-key ruleset writes atomically, but readers at different edges may still observe a partial ruleset mid-propagation |

Sources: [CloudFront quotas](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/cloudfront-limits.html),
[JavaScript runtime 2.0](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/functions-javascript-runtime-20.html),
[KeyValueStore](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/kvs-with-functions.html),
[Function restrictions](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/cloudfront-function-restrictions.html).

**AWS publishes no propagation-time commitment for a KeyValueStore update.** That number is the
standby activation latency (#60) and the revocation delay (#63) simultaneously, and it is the
window during which the edge and DynamoDB disagree about serving state (§4.2).

## 3. Decision (proposed)

A CloudFront Function at **viewer-request** is the gate. It reads its configuration from a
KeyValueStore and decides locally. The credential is HMAC-SHA256 under the per-deployment key
already in SSM, in the `wr_common::crypto` encoding, with the existing kind-byte domain separation
(ADR-0011). The RSA key pair, the CloudFront public key, the key group and the
`custom_error_response` mapping all go away.

### 3.1 One credential, delivered as a cookie

ADR-0011 introduced the token-then-session exchange for a stated reason: *"The URL changes on the
visitor's next navigation, so without a session the credential is lost on the second page view."*
That reason is specific to a credential carried on a URL. Our waiting page is a behaviour on the
same distribution, so `generate_token` already returns credentials as `Set-Cookie` on a response
the visitor receives — see `crates/generate_token/src/main.rs`.

**So `generate_token` mints the session credential directly.** There is no admission credential at
the edge, no exchange step, and no 302 to strip anything. The edge verifies one credential kind.
That is one fewer state to implement identically in two languages.

This is not a fix for #62. Nothing on the CloudFront path has ever put a credential in a URL;
#62 is about `request_id` being a cacheable query parameter on `/v1/queue_num` *and* the sole input
to `/v1/generate_token`, which this ADR does not change. The exposure is only closed if the
authorizer path converts too, which is not proposed here.

`generate_token` stays a Lambda and remains the only writer of `arrivals#*` **while admission is
Open**, so the controller's no-show correction is untouched in the normal case. Across a fail-open
or dormancy transition nobody calls `generate_token` at all, arrivals stop, and the reconciliation
in §4.2 is what keeps `serving_counter` from advancing against arrivals that are no longer being
recorded.

### 3.2 What the credential carries

The edge cannot call back, so everything the decision needs is in the credential or the store.
Starting from today's `Session` (`event_id`, `request_id`, `issued_at`, `expires_at`) and adding
only what has a requirement behind it:

| Field | Status | Why |
|---|---|---|
| kind byte | exists | Domain separation, verified across languages (§6) |
| **version byte** | **add** | A frozen format with no version cannot be revised without a flag day, and §7 says the propagation window — the thing a flag day would need — is unknown |
| event id | exists | Refuse a credential minted for another event (#61) |
| request id | exists | Correlates an arrival with the position it came from |
| issued at, expires at | exists | Expiry check against the function's start time |
| ~~session validity, session mode~~ | **dropped** | Justified as "the backend decides per visitor", but nothing varies per visitor: `authorizer::Config` holds one `session_mode` for the whole deployment. Per-visitor lifetime is what #65 may want later. Until then this belongs in the KVS value the gate already reads |
| ~~hashed IP~~ | **dropped** | No requirement asks for IP binding, so it falls under "no speculative features". If it returns it must be fixed-width with a zero sentinel, never an optional trailing field, and the consequence recorded: it breaks every visitor moving between wifi and cellular, and every visitor behind CGNAT |

### 3.3 The decision tree

```
viewer-request:
  1. no rule matches            -> pass through untouched            (#66, dormant #60)
  2. serving state is FailOpen  -> pass through, mark the request     (§5.1)
  3. valid session credential   -> pass through
  4. invalid credential         -> refuse with a reason               (#73)
  5. no credential              -> refuse: navigation gets the waiting page,
                                   XHR gets JSON plus a header        (#72)
  6. anything throws            -> pass through, mark the request
```

Step 6 handles *the gate being broken*. It is not the same thing as #58 — see §5.1.

## 4. Open decisions

### 4.1 The signing key lives in the KeyValueStore

An earlier draft treated this as blocked. It is not, and the alternative it proposed was worse.

**Terraform is not the obstacle.** The repo already solves this problem for this exact key:
`infra/modules/core/main.tf` creates the signing parameter with the literal value
`PLACEHOLDER-overwrite-out-of-band` under `lifecycle { ignore_changes = [value] }`, and the real
value is written out of band. `ignore_changes` is a core lifecycle meta-argument, so the same
pattern applies unchanged to `aws_cloudfrontkeyvaluestore_key`: Terraform creates the store and a
placeholder, and the bootstrap overwrites it. This is the established shape here, used twice
already (the signing key and the OIDC client secret).

**Embedding the key in the function code is worse, not better.** `aws cloudfront get-function`
"Gets the code of a CloudFront function," so a key in the code is readable by anyone holding
`cloudfront:GetFunction` — a permission nobody treats as secret-bearing — and CloudFront retains
function versions, so every rotated key persists in history. The KeyValueStore has a purpose-built
data-plane permission (`cloudfront-keyvaluestore:GetKey`) and a value that is replaced rather than
versioned.

**Neither option avoids the edge.** The key has to be at every edge location or the gate cannot
verify anything. The question was never "edge or not" but "which edge-distributed store has the
better permission boundary and rotation story," and that is the KeyValueStore.

**RustCrypto is accepted.** Writing to the KeyValueStore data plane requires SigV4A, and in the
Rust SDK that means `sigv4a = ["dep:p256", "dep:crypto-bigint", "dep:subtle", "dep:zeroize"]`, with
`sign/v4a.rs` signing via RustCrypto's `p256`, `hmac` and `sha2`. That is a deliberate, recorded
exception to `tech.md`'s single-backend rule rather than a violation of it, and it must be scoped:

- The **admission path is untouched** — credentials are still minted and verified with `aws-lc-rs`.
  RustCrypto enters only on the control-plane path that writes configuration to the edge.
- The exception has a real boundary in the **GovCloud variant** (N4). CloudFront, CloudFront
  Functions and KeyValueStore do not exist in GovCloud (DESIGN §11), so the edge gate is a
  commercial-only feature by construction and the FIPS posture is not compromised there. But the
  **admin Lambda ships to both**, so the KeyValueStore writer must sit behind a Cargo feature or in
  its own crate, or the GovCloud build carries a non-FIPS crypto stack it never calls.
- `deny.toml` must be updated to say this explicitly, since today it enforces nothing either way
  ([#74](https://github.com/smoketurner/virtual-waiting-room/issues/74)).

### 4.2 Which copy of the serving state is authoritative

The edge does **not** need `serving_counter` — `generate_token` already gates on it before minting,
so the credential's existence *is* the admission decision, and KVS writes stay rare rather than
once per controller pass. That assumption survived review.

What the ADR must add is that a second copy of the *coarse* serving state now lives at the edge
with no stated reconciliation. `Counters.admission_control` is authoritative; the KVS value is a
derived cache. During the propagation window they disagree: the edge may pass everyone through
while DynamoDB still reads `Open` and the controller keeps advancing `serving_counter` against
arrivals that have stopped being recorded. The controller already refuses to act in either
non-`Open` state — `crates/controller/src/lib.rs` returns `PassOutcome::Held` for both `Paused` and
`FailOpen` — which closes the hole once both sides agree, but not during the window. The ADR needs
to say what the edge does when it cannot tell.

With RustCrypto accepted (§4.1), the writer is settled: the **admin Lambda writes both** — DynamoDB
first as the authority, then the KeyValueStore as the derived cache — so an operator action has one
origin and the two copies converge within the propagation window rather than being reconciled by a
separate component. What remains open is only the behaviour *during* that window.

### 4.3 Cost depends on which behaviours carry the association

CloudFront Functions bill **per invocation**, at a flat rate — compute utilization does not enter
into it. The measured 9-versus-11 gap between an unprotected and a gated request is therefore
irrelevant to cost; it says only that both fit the time budget comfortably.

What decides the bill is invocation *count*. Associated with the **protected behaviour only**, that
is protected-origin requests. Associated distribution-wide it also bills every `/status` poll from
every waiter, which at a million waiters is the dominant term and would swamp the O6 model. The
association is on the protected behaviour alone, and that is a load-bearing configuration detail,
not a default.

## 5. What this ADR does not deliver

### 5.1 Fail-open (#58) is not restored by step 6

Step 6 catches *the gate throwing*. #58 is about the **backend** being unreachable, and the edge
makes no network calls, so it cannot observe that at all. When DynamoDB is unavailable,
`generate_token` fails, nobody receives a credential, already-admitted visitors keep flowing, and
new visitors sit on the waiting page indefinitely — while the gate is perfectly healthy and
correctly refusing them.

Reaching step 2 requires something to have written `FailOpen` into the store, and that writer needs
the backend that is down. **Fail-open still depends on the failing system working.** Naming the
watchdog, and showing it does not share a failure domain with what it watches, is measurement 5.

One concrete improvement regardless: store a `fail_open_until` epoch rather than a state flag. The
edge has `Date`, so a timestamp self-heals if the control plane dies after tripping it, mirroring
the authorizer's existing `bypass_ttl_secs`. A flag left by a control plane that then dies leaves
the deployment open indefinitely.

### 5.2 Revocation (#63) has no design here

Bearer credentials verified with no state are unrevocable by construction. The options are a
generation counter in the credential plus a floor in the store, which revokes everyone at once; or
a per-visitor deny list in the store, which is targeted and fits 5 MB but is gated by the same
unmeasured propagation window. Neither is chosen, so #63 is listed in §1 as unresolved rather than
claimed.

## 6. Measured in the spike

**The existing wire format verifies unchanged in JavaScript.** A `Session` minted by the Rust was
verified by the gate's own `verify()` under Node: fields round-trip, wrong key rejected, tampered
payload rejected, tampered MAC rejected, and the same credential presented under the token kind
byte rejected — domain separation holds across the language boundary.

The scope of that result matters. It proves the *machinery* agrees: base64url without padding,
HMAC-SHA256, the `u16`-length-prefixed UTF-8 walk, big-endian `u64`, and the kind byte. It exercised
exactly today's `Session` fields. The version byte in §3.2 is not covered and is still new format.

**The complete gate is 4,759 bytes against the 10 KB ceiling (46%)** — rule matching over all four
`ProtectionRule` kinds, XHR-versus-navigation response shaping, the double-HMAC compare, reason-coded
refusals and the fail-open `catch`. An earlier draft quoted 3,079 bytes for a skeleton that omitted
the first three; this figure is the whole decision tree. Multi-key ruleset reassembly is still not in
it.

**Compute utilization on the hot path is 11 of 100**, measured with `aws cloudfront test-function`
against a real function and a real KeyValueStore (`scripts/spike_edge_gate.py`). The breakdown:

| Case | Utilization | Outcome |
|---|---|---|
| Valid session on a protected path (2 KVS reads, rule match, 3 HMACs) | **11** | passed through |
| Unprotected path, no rule matched (1 KVS read) | 9 | passed through |
| No credential, navigation | 9 | 302 to the waiting page |
| No credential, XHR | 8 | 403 JSON with headers (#72) |
| Tampered credential | 8 | 302, `reason=signature` |

**89% of the time budget is unspent**, and the shape of the numbers matters as much as the total: a
function that throws immediately costs 6, so the entire gate — two store reads, rule matching and
three HMAC operations — costs about 5 points above the floor. Credential verification is
approximately free at this scale, which was the thing most in doubt.

**The Rust-minted credential verified in the real runtime, not just under Node.** The admitted case
passed through, which means `wr_common::crypto`'s output was accepted by a genuine CloudFront
Function. The cross-language risk is now closed empirically at both layers.

**Every refusal produced the right shape and the right reason**, so #72 and #73 are demonstrated
rather than asserted: a navigation gets a 302 to the waiting page, an XHR gets 403 with a JSON body
and `x-wr-reason`, and a tampered credential is distinguishable from a missing one.

**A realistic ruleset averages 23 bytes per rule**, so about 45 rules fit one 1 KB value and 100
rules needs 3 keys. The encoding measured was hand-written; the committed format must be
`ProtectionRule`'s own `serde_json` form, or Rust and JavaScript will drift.

## 7. What must still be measured

1. ~~Compute utilization at real credential size.~~ **Answered in §6: 11 of 100 on the hot path.**
2. **KeyValueStore propagation time**, end to end and at the tail: the standby activation latency,
   the revocation delay, and the edge/DynamoDB disagreement window.
3. ~~Ruleset shape against the 1 KB value ceiling.~~ **Answered in §6.**
4. ~~Whether viewer-response runs after a generated viewer-request response.~~ **Moot.** With the
   exchange step gone (§3.1) and per-visitor session lifetime deferred to #65, the second function
   has nothing to do. If sliding sessions return, a viewer-request function can re-issue by
   returning a 302 with a refreshed cookie late in the window — one function, no dependency on
   undocumented behaviour, at the cost of one redirect per slide interval.
5. **What trips fail-open, and whether it shares a failure domain with the backend it watches**
   (§5.1).
6. Whether there is a per-invocation limit on KeyValueStore reads, which bounds a multi-key ruleset.

Measurements 1, 2 and 5 need a CloudFront Function and a KeyValueStore to exist. They are blocked
on IAM, not effort: the `VouchAdmin` role in the dev account denies `cloudfront:CreateFunction` and
`cloudfront:CreateKeyValueStore` by session policy, while reads are permitted. Completing them
needs, scoped to a spike-named prefix:

```
cloudfront:CreateFunction, TestFunction, DescribeFunction, DeleteFunction
cloudfront:CreateKeyValueStore, DescribeKeyValueStore, DeleteKeyValueStore
cloudfront-keyvaluestore:PutKey, GetKey, DescribeKeyValueStore
```

Measurement 2 additionally needs the function associated with a distribution. Doing that outside
Terraform puts the dev stack into drift, so it should be a `/_spike/*` cache behaviour added through
Terraform rather than a console or CLI edit.

Writing to the store with temporary credentials requires a **regional** STS endpoint; the global
endpoint issues v1 session tokens that SigV4A rejects, and it fails as an unexplained
authentication error.

## 8. Consequences

**`generate_token` loses its RSA path.** `RESOURCE_WILDCARD`, the policy types, `policy_json`,
`cf_base64`, `SignedCookies`, `Signer` and their tests all go, and the crate drops `aws-lc-rs` and
`base64` as direct dependencies. `aws-lc-rs` stays in the tree via `wr-common`, so the
jitter-entropy cold-start note in `tech.md` is unchanged. The genuine win is narrower than "less
crypto": HMAC needs no RNG, so a `SystemRandom` construction per signature and an RSA-2048 signing
operation both leave the admission path.

**The `Store` trait and `decide` are untouched.** The whole change is what `main.rs` does with the
returned `Grant` — the three-file split working as intended.

**One key, one rotation.** `generate_token` stops reading the CloudFront signer parameter and reads
`/signing-key`, the parameter the authorizer already uses.

**Terraform: −3 resources, +1 KVS, +1 function, +1 per seeded key.** `modules/core` is at 65 of the
80 budget (N6). The §6 ruleset result is what keeps this affordable — every additional ~45 rules is
one more Terraform resource.

**A JavaScript toolchain returns, dev-only.** ADR-0018 removed one deliberately. A Node-based
conformance runner with no npm dependencies preserves the letter of that, but `structure.md`,
`testing.md` and CI all need updating — in particular `testing.md`'s "Not currently provable /
fail-open" paragraph, which this ADR rewrites rather than leaves stale.

**Delete on adoption, per "replace, don't deprecate":** the bounce guard in
`infra/modules/edge/pages/waiting.js` exists solely to survive the 403-to-waiting-page loop that
#64 and this ADR remove.
