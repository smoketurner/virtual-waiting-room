# ADR-0021: Move the gate to a CloudFront Function

**Status:** Accepted. The spike measured everything that could have invalidated the approach and
none of it did: the runtime verifies an unchanged `wr_common::crypto` credential, the gate fits in
4,759 of 10,240 bytes, the hot path costs 11 of 100 compute utilization, and a configuration change
reaches an edge in a median of 31 seconds. What remains in §5 are design decisions — what trips
fail-open, and which revocation design — not open questions about feasibility.

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

**AWS publishes no propagation-time commitment for a KeyValueStore update, so it was measured:
a median of 31 seconds** (§6). That single number is the standby activation latency (#60), the
revocation delay (#63), and the window during which the edge and DynamoDB disagree about serving
state (§4.2).

## 3. Decision

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
| event id | exists | Refuse a credential minted for another event (#61) |
| request id | exists | Correlates an arrival with the position it came from |
| issued at, expires at | exists | Expiry check against the function's start time |
| ~~session validity, session mode~~ | **dropped** | Justified as "the backend decides per visitor", but nothing varies per visitor: `authorizer::Config` holds one `session_mode` for the whole deployment. Per-visitor lifetime is what #65 may want later. Until then this belongs in the KVS value the gate already reads |
| ~~hashed IP~~ | **dropped** | No requirement asks for IP binding, so it falls under "no speculative features". If it returns it must be fixed-width with a zero sentinel, never an optional trailing field, and the consequence recorded: it breaks every visitor moving between wifi and cellular, and every visitor behind CGNAT |

No version byte: the kind byte already domain-separates and the MAC already covers the tag, so a
future format change fails cleanly as `BadSignature` under a v1 verifier with zero wire-format
change today, at the cost of one extra HMAC at a future flag day. A deployment does not outlive its
event, so Rust and JavaScript ship from one commit and no v1 verifier ever meets a v2 credential in
practice.

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

**The placeholder is a live secret, not just a missing one.** Both the SSM parameter and the
KeyValueStore's `k` are seeded with the literal `PLACEHOLDER-overwrite-out-of-band`
(`infra/modules/core/main.tf`, `infra/modules/core/edge_gate.tf`). If `scripts/bootstrap_edge_gate.py`
is never run, both sides *agree* on that literal — the gate verifies correctly, against a secret
published in this repository — and nothing looks wrong until an operator writes a non-empty
ruleset, at which point anyone who has read the repo and knows the event id mints a valid session.
`generate_token` and `authorizer` both refuse to start if the key they read is the placeholder
(`wr_common::PLACEHOLDER_SIGNING_KEY`); that is the only guard that catches the bootstrap never
having run at all, since the bootstrap script cannot detect its own absence. The bootstrap's two
writes (SSM, then the KeyValueStore) are not atomic, so the honest claim is "divergence cannot
arise from a *successful* run," not "cannot arise" — a failed run is loud (it exits non-zero) and
must be re-run before serving traffic. See `docs/DEPLOY.md` for both failure modes in full.

**RustCrypto is accepted.** Writing to the KeyValueStore data plane requires SigV4A, and in the
Rust SDK that means `sigv4a = ["dep:p256", "dep:crypto-bigint", "dep:subtle", "dep:zeroize"]`, with
`sign/v4a.rs` signing via RustCrypto's `p256`, `hmac` and `sha2`. That is a deliberate, recorded
exception to `tech.md`'s single-backend rule rather than a violation of it, and it must be scoped:

- The **admission path is untouched** — credentials are still minted and verified with `aws-lc-rs`.
  RustCrypto enters only on the control-plane path that writes configuration to the edge.
- RustCrypto is already present in the `admin` build via `openidconnect`'s `p256`/`rsa` dependencies
  (the OIDC login path), so the KeyValueStore writer adds a dependency the build already carries
  rather than a new exposure.
- `deny.toml` now says this explicitly: it bans `ring` outright (closing
  [#74](https://github.com/smoketurner/virtual-waiting-room/issues/74) for the crate the project's
  single-backend rule actually cared about) and leaves `p256`/`crypto-bigint`/`hmac`/`sha2` alone,
  since they have many legitimate parents already in the tree and a `wrappers` entry on them would
  be brittle rather than protective.

### 4.2 The edge keeps no copy of the serving state

The edge needs neither `serving_counter` nor `admission_control`. `generate_token` gates on
`serving_counter` before minting, so the credential's existence *is* the admission decision.
`admission_control` is likewise not needed: `Paused` holds new admissions without revoking anyone,
so the edge enforces identically under `Open` and `Paused`, and `generate_token` reads DynamoDB
directly for that check. There is therefore no second copy of the serving state and nothing to
reconcile during the propagation window.

What the store does hold is a ruleset and two epochs. `FailOpen` at the edge is not the DynamoDB
flag but `fail_open_until` — a different fact ("this edge is open until T") from the flag itself
("the machinery is in break-glass"). `Counters` carries the same epoch as `fail_open_until` (issue
#71): `AdmissionControl::FailOpen` is never stored, it is resolved by `wr_common::resolve(stored,
fail_open_until, now)` from `StoredControl` (`Open`/`Paused` only) plus the epoch, so the string
`"fail_open"` cannot exist in `Counters.admission_control` at all.

The governing rule is that **every change making the gate more permissive is expressed as a
timestamp the edge evaluates against its own clock, and every change making it more restrictive
takes effect on receipt**, so propagation skew can only delay relief, never extend exposure. The
restrictive change that matters is dormant-to-enforcing at go-live, where skew would leave some
edges ungated for ~31 seconds at the onset of a spike. `enforce_from` closes it: the writer stamps
`now + settle_secs`, every edge compares that stamp to its own `Date`, and every edge that received
the value flips at the same instant. `enforce_from = 0` is the break-glass path: enforcement on
receipt, non-simultaneously, within about a minute.

The admin Lambda writes both stores. The ordering is per action, not fixed: write whichever side
leaves the system self-consistent if the second write never lands. Entering fail-open writes the
KeyValueStore first — a crash between the two leaves the edge open with the machinery still minting
and counting, whereas DynamoDB-first would stop `generate_token` while the edge still enforced.
Leaving fail-open writes DynamoDB first, so a crash leaves the machinery running and the edge open
only until the stamped expiry.

The control plane cannot confirm that a value reached an edge; `meta()`'s `lastUpdatedDateTime` is
readable only from inside a function. The admin therefore reports the scheduled effective time and
the fact that it is at least `settle_secs` away. It does not claim confirmation.

### 4.3 Cost depends on which behaviours carry the association

CloudFront Functions bill **per invocation**, at a flat rate — compute utilization does not enter
into it. The measured 9-versus-11 gap between an unprotected and a gated request is therefore
irrelevant to cost; it says only that both fit the time budget comfortably.

What decides the bill is invocation *count*. Associated with the **protected behaviour only**, that
is protected-origin requests. Associated distribution-wide it also bills every `/status` poll from
every waiter, which at a million waiters is the dominant term and would swamp the O6 model. The
association is on the protected behaviour alone, and that is a load-bearing configuration detail,
not a default — asserted by `infra/modules/edge/tests/gate_association.tftest.hcl` rather than left
to review, so a future behaviour added without thinking about this fails CI instead of shipping.

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

One concrete improvement regardless, and now specified rather than suggested: `Counters` stores a
`fail_open_until` epoch, not a stored `FailOpen` flag — `StoredControl` (issue #71) has only `Open`
and `Paused`, so the string `"fail_open"` cannot be written at all, and `wr_common::resolve` is the
only thing that produces `AdmissionControl::FailOpen`, from the epoch against a clock. The edge has
`Date`, so the timestamp self-heals if the control plane dies after tripping it, mirroring the
authorizer's existing `bypass_ttl_secs`. A flag left by a control plane that then dies would leave
the deployment open indefinitely; an epoch cannot.

### 5.2 Revocation (#63) has no design here

Bearer credentials verified with no state are unrevocable by construction. The options are a
generation counter in the credential plus a floor in the store, which revokes everyone at once; or
a per-visitor deny list in the store, which is targeted and fits 5 MB. Both are gated by the
31-second propagation floor, so revocation is honestly described as "takes effect within about a
minute," never as immediate. That rules it out as a response to something happening right now, and
it means a short credential TTL remains the primary defence with revocation as a second line.
Neither design is chosen, so #63 is listed in §1 as unresolved rather than claimed.

### 5.3 Sliding sessions are not delivered at the edge

`generate_token` mints one fixed-TTL session (`SESSION_TTL_SECS`) and the gate only verifies or
refuses it — there is no `SessionMode` on this path and no re-issue. This was already flagged as
moot/deferred in §7 measurement 4 before the CloudFront Function existed, and it stays deferred now
that it does: CloudFront Functions can set cookies on a generated response, so a re-issue-on-activity
step is possible in principle, but nothing implements it.

The `authorizer` path is not the same: `SessionMode::Sliding` re-issues the session cookie on any
request that still carries a valid one, extending the idle window up to a hard cap from first issue.
The two gates never run in the same deployment, so this is not an inconsistency a visitor could
observe within one event — but it is a real difference in what the two gates give an operator. A
visitor admitted through the CloudFront path is logged out and must rejoin the queue if their
session outlives `SESSION_TTL_SECS`, checkout included; the same visitor through the authorizer path
with sliding configured is not. An operator choosing between the two gates has to decide whether
`SESSION_TTL_SECS` comfortably exceeds the worst realistic checkout time — see `docs/DEPLOY.md`.

## 6. Measured in the spike

**A Rust-minted credential verifies; the JS verifier accepts a strict superset of what Rust
accepts.** A `Session` minted by the Rust was verified by the gate's own `verify()` under Node:
fields round-trip, wrong key rejected, tampered payload rejected, tampered MAC rejected, and the
same credential presented under the token kind byte rejected — domain separation holds across the
language boundary. The missing property was directionality, not validity: the spike's naive
`verify()` accepted trailing payload bytes and read a 64-bit field's low half only, both of which
Rust rejects; the shipped `infra/modules/edge/functions/gate.js.tftpl` closes both, proven by
`crates/wr-common/tests/vectors.rs`'s generated conformance vectors (positives: Rust-minted ->
JS-verified; negatives: both verifiers reject the same set — JS never mints, so there is no reverse
direction to prove).

The scope of that result matters. It proves the *machinery* agrees: base64url without padding,
HMAC-SHA256, the `u16`-length-prefixed UTF-8 walk, big-endian `u64`, and the kind byte. It exercised
exactly today's `Session` fields; the test credential's high words were zero and its payload had no
trailing bytes, so neither bug was exercised by the spike itself — only by the vectors added
afterward.

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

**KeyValueStore propagation to an edge is a median of 31 seconds** — 29.4s min, 32.4s max, five
of five trials observed. Measured by publishing a probe function against a throwaway distribution
and polling it over HTTPS after each write (`scripts/spike_edge_gate.py --propagation`).

The tightness matters as much as the value: a three-second spread across five trials looks like a
fixed sync interval rather than variable propagation, which makes it a number you can design
against rather than a distribution with a bad tail.

Two caveats bound it. One client observes one point of presence, so **31 seconds is a floor** —
global convergence is at least that and probably longer. And propagation was measured on a
single-key write; a multi-key ruleset (100 rules, 3 keys) writes atomically but may still be read
partially at an edge mid-propagation.

## 7. What must still be measured

1. ~~Compute utilization at real credential size.~~ **Answered in §6: 11 of 100 on the hot path.**
2. ~~KeyValueStore propagation time.~~ **Answered in §6: 31s median to one edge.**
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

**The `Store` trait is untouched; `decide` gains one parameter.** `decide` needed `now: u64` to
resolve `StoredControl` against `fail_open_until` (issue #71) — the rest of the change is what
`main.rs` does with the returned `Grant`, the three-file split working as intended.

**One key, one rotation.** `generate_token` stops reading the CloudFront signer parameter and reads
`/signing-key`, the parameter the authorizer already uses.

**Terraform: −4 resources, +3 for the KeyValueStore and its two seeded keys.** `modules/core` nets
65 → 64, under the 80 budget (N6): `tls_private_key`, `aws_ssm_parameter.cf_signer_key`,
`aws_cloudfront_public_key.signer` and `aws_cloudfront_key_group.signer` go (`tls_private_key` is
itself a resource, hence −4, not −3); `aws_cloudfront_key_value_store.gate` and two
`aws_cloudfrontkeyvaluestore_key` resources (`c`, `k`) replace them. `modules/edge` nets 14 → 15
(+1 for the function). The §6 ruleset result is what keeps room in the budget for the KVS resources
— every additional ~45 rules would be one more Terraform resource, but none are added by this issue,
since no rules-editing admin action ships with it.

**A JavaScript toolchain returns, dev-only.** ADR-0018 removed one deliberately; it had already come
back for `waiting.js`'s adaptive-poll tests before this ADR. A Node-based conformance runner with no
npm dependencies preserves the letter of that — `crates/wr-common/tests/vectors.rs` generates the
vectors, `infra/modules/edge/tests/gate.conformance.test.js` and
`gate.decision-tree.test.js` consume them under `node:vm`, run by the existing `client-ci.yml`
job. `structure.md` and `testing.md` are updated to match; in particular `testing.md`'s
"Not currently provable / fail-open" paragraph, which this ADR rewrites rather than leaves stale.

**Delete on adoption, per "replace, don't deprecate":** the bounce guard in
`infra/modules/edge/pages/waiting.js` exists solely to survive the 403-to-waiting-page loop that
#64 and this ADR remove — replaced by the gate's own `next=` query parameter, which preserves the
visitor's destination through the redirect without needing a client-side loop guard at all.

**ADR-0020 is retired in one change, with no dual-gate window.** A transition period would require
knowing whether a viewer-request function runs before or after trusted-key-group validation, which
AWS does not document; if validation runs first, the two gates cannot coexist at all. The `custom_
error_response` block and `trusted_key_groups` are removed and the gate's `function_association`
added in the same apply that deploys `generate_token`'s HMAC-only `main.rs`, so no request ever
meets both gates and there is nothing to roll back through. A visitor holding an already-issued RSA
cookie at cutover is refused with reason `none` and rejoins the queue; nothing can be done for them,
and nothing needs to be — ephemerality (no cookie survives past its `Max-Age`) means the population
affected is bounded by the cutover instant, not by how long the cookie type is retired.
