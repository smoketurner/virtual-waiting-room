# ADR-0038: A place in the queue costs a WAF challenge, and the API answers only CloudFront

**Status:** Accepted. Amends DESIGN §8's "No web ACL is created" and reverses N7's default.

## 1. Context

Joining cost a client nothing. `request_id` is client-supplied (ADR-0010), and nothing bounded
how many ids one client could mint. The pre-queue shuffle (ADR-0001) then turned that volume
straight into a share of the front of the queue, so every deployment was a raffle that a script
wins. Entry tickets (ADR-0028) and open-time demotion (ADR-0030) had both been removed, and
nothing replaced them.

Two things kept the edge from helping.

- **The regional API was reachable directly.** CloudFront's origin is the public
  `execute-api` hostname. A script could post `/v1/join` straight to it and skip every
  control at the edge. That is also why ADR-0030's telemetry could not see the traffic it
  was aimed at.
- **A web ACL was priced out.** On pay-as-you-go WAF charges per request inspected, against
  a request volume that is mostly the waiting page's own polling. At a million visitors that
  exceeded the CloudFront bill (DESIGN §12). An ACL left in place between events also bills
  a monthly fee per ACL and per rule, which N1 does not allow.

CloudFront flat-rate plans change the second point. The Business plan ($200/month) and the plans
above it include the web ACL, its rules, AWS managed rules and WAF request fees, with no overage
charges. Requests WAF blocks do not count against the plan's allowance. The plan also includes
the JavaScript challenge, CAPTCHA, rate-based rules, Bot Control at the common inspection level
and the Anti-DDoS managed rules. A plan can't use Targeted Bot Control, the account-takeover and
account-creation fraud groups, partner managed rules or custom rule groups. It requires a web ACL
on the distribution.

## 2. Decision

**Every REST API method requires an API key that only the distribution holds. On a deployment
subscribed to a flat-rate plan, a web ACL makes a place in the queue cost a browser challenge,
and caps what one challenge buys.**

### The API answers only CloudFront

Every method (public, admin and static) sets `api_key_required`. Terraform generates the key and
binds it to the stage with a usage plan. The distribution sends it to the API origin as the
`x-api-key` origin custom header, which replaces any value a viewer sends. A request straight to
`execute-api` gets a 403 before any integration runs. API Gateway does not bill requests it
rejects for a missing key, and no Lambda runs, so the join path still has no compute in it.

This is not the public API key DESIGN §8 rules out. That rule is about a key served to browsers,
which ships in client JavaScript. This key never reaches a browser. It is in Terraform state
and the distribution's configuration, so it keeps out the internet, not the account's own
operators.

The key is unconditional, with no opt-in. It costs nothing and closes the bypass on every
deployment, flat-rate or not.

### The web ACL (opt-in, `waf_enabled`)

The web ACL is opt-in. A deployment on a flat-rate plan turns it on. On pay-as-you-go it bills
between events, so the default is off.

- **The token.** Every JavaScript API path requires an `aws-waf-token`. The paths are listed
  explicitly and a Terraform test checks them against the paths `waiting.js` calls. Without a
  token, WAF answers with a 202 challenge (or a 405 for a CAPTCHA). `waiting.js` then navigates
  to `/_wr/verify.html`, and that navigation gets the Challenge action: a silent interstitial
  that runs once per immunity period, sets the token and returns the visitor to the waiting page.
  The visitor keeps their request id, so they keep their place. The challenge is on its own page
  because `waiting.html` also serves the no-JavaScript form, and a browser without JavaScript
  can't pass an interstitial.
- **What one token buys.** Rate limits keyed on the token cookie cap it at ten joins per ten
  minutes (WAF's minimum) and at a request rate far above what an honest page polls. More
  places mean more challenge solves.
- **Per address, escalate rather than block.** Token requests (verify-page loads) plus joins
  from one IP are limited. Past the limit, each further visitor on that address solves a CAPTCHA once rather
  than being blocked, because a mobile carrier's NAT puts many real buyers behind one address.
- **The no-JavaScript queue** (ADR-0037) can't run a challenge or a CAPTCHA, so it is never
  challenged. Instead it gets a strict per-IP limit that refuses past ten joins in five minutes,
  a refusal for anonymous and hosting-provider addresses, and a `nojs_enabled` switch that
  closes it at the edge for an event where it is being abused. Cloudflare Turnstile was
  considered for this path and rejected: it also needs JavaScript, and it sends visitor telemetry
  to a third party, which breaks ADR-0007's posture.
- **Managed rules** (Anti-DDoS, Amazon IP reputation, Bot Control common, and the anonymous-IP
  labels) are scoped to the waiting room's own paths, except Anti-DDoS, which is ACL-wide as AWS
  intends. They run in Count until `waf_managed_rules_mode = "enforce"`, per O5 and ADR-0012.
  The token and rate-limit rules act from the start. They are the mechanism, not a classifier
  with an unknown false-positive rate, and they can't lock out a browser that runs the page:
  at worst it sees a CAPTCHA.

Everything used is within the Business plan's allowance: individual rules only, no rule groups
of our own, 13–14 rules against its 50.

## 3. Consequences

- **A place now costs something, but only a little.** Taking N places needs N/10 challenge
  solves from a real browser engine, spread across addresses that each pass a CAPTCHA past the
  per-IP limit. That lifts the cost from nothing to a browser session per few places. It still
  doesn't bind a place to a person. A farm running real browsers through residential proxies
  pays more, but it still wins places. One place per identity needs a customer-side identity,
  which ADR-0028 declined to require.
- **The cost is fixed per month, not per event.** The plan is a monthly subscription. While a
  distribution is subscribed, AWS refuses to delete it or remove its web ACL, even after
  cancellation, until the billing cycle ends. A stack that was torn down after each event
  (ADR-0008) now waits for month end.
- **Subscribing is manual.** The Terraform provider cannot create the subscription yet
  (terraform-provider-aws#45450, PR #49235), so it is a console step after `apply`
  (`docs/DEPLOY.md`).
- **Browsers that refuse cookies cannot queue on the JavaScript path.** The token is a cookie.
  The page detects a token that won't stick and says so instead of reloading in a loop. The
  no-JavaScript path doesn't need the token.
- **Introducing the key is a between-events change.** The stage starts requiring the key as
  soon as its deployment updates. CloudFront takes minutes to carry the new header to every
  edge, and until it does, API calls through those edges get a 403. Once both are in place,
  later applies don't reopen that window, because the key doesn't change.
- **Anything calling the API directly needs the key.** `scripts/smoke_test.py` reads it from
  `terraform output`. The load harness (`crates/harness`) runs against a local stub and is
  unaffected.
- **The model's request allowance holds.** DESIGN §12's adaptive-polling model puts a
  million-visitor event at roughly 55–80M requests, inside the Business plan's 125M. The plan
  also absorbs a one-time spike of up to three times the allowance, and blocked requests don't
  count.
