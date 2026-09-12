# ADR-0027: Deliver the entry ticket by cookie, or by URL fragment — never a query string

**Status:** Accepted

## 1. Context

[ADR-0026](0026-entry-tickets.md) has the customer's own system mint a ticket. That ticket has to
reach `waiting.js`, which runs on the waiting room's host. The issuer runs on the customer's host.

The obvious answer, a cookie, does not work by itself. A cookie set by `shop.example.com` is not
readable at `d111abcdef.cloudfront.net`. This is ordinary cross-origin scope, not the Public
Suffix List — host-only cookies work fine on `*.cloudfront.net`, as `generate_token` already
demonstrates by setting the session cookie there and having the edge gate read it back.

So the question is how a value minted on one origin reaches a script on another.

## 2. Decision

**Two ingress paths, one stored form, ordered by ticket expiry.**

**On a custom domain — the expected deployment — the issuer sets a cookie.** With
`aliases` and `acm_certificate_arn` configured, the waiting room and the issuer share a
registrable domain, so the issuer sets `Domain=example.com` and the ticket is simply there on
every load, re-presenting itself without ever appearing in a URL. This path gets the fuller test
coverage because it is the one that ships.

**Without one, the issuer redirects with the ticket in the URL fragment:**
`https://<host>/_wr/waiting.html#wrt=<jws>`. `waiting.js` reads `location.hash` on first load.
Fragments survive a 302 whose `Location` carries one, so the issuer needs nothing on our side to
land on.

**Not a query string.** A ticket is a bearer credential, so the fewer places it travels the
better. A fragment is never sent to a server at all; a query string is sent to ours, and — because
`waiting.html` loads its CSS and JS before any script of ours can run — to the `Referer` of those
subresources too, which `history.replaceState` cannot get ahead of. The fragment costs nothing and
avoids the question entirely.

**Precedence is by `exp`, not by source.** Each candidate's payload is decoded (not verified — that
is `assign_position`'s job) and the one with the latest unexpired `exp` wins. Ordering by source
cannot express this: the fragment is deliberately left in place for a visitor whose storage all
failed, so they can bookmark that URL, later collect a fresher ticket into the cookie, and return
through the stale link. A source-ordered rule would pick the older ticket and have it rejected as
expired while a valid one sat one tier down.

**The fragment is stripped only once the ticket is stored durably.** Durability is a read-back,
not the absence of a thrown exception: the storage chain ends in an in-memory tier that always
succeeds and never survives a reload, so trusting it would strip the ticket and strand exactly the
visitors the chain exists to protect.

## 3. Consequences

- A deployment without a custom domain works. The fragment is not a degraded stopgap; it is a
  complete path.
- **For a ticketed deployment the storage chain is a convenience, not a correctness
  requirement** — `sub` is stable per identity, so re-entering through the customer's link
  re-derives the same `request_id` with no storage at all. This property depends on the strip being
  conditional. Making it unconditional silently makes storage load-bearing again, which is why the
  coupling is stated in the code rather than left to be rediscovered.
- **A ticket is a bearer credential, not a hash.** Whoever reads one derives the same `request_id`
  and claims that identity's position; nothing binds it to the browser it was issued to. That is a
  sharper class than the identifier exposure driving ADR-0026's opaque subject — a leaked
  identifier is a privacy loss, a leaked ticket is an impersonation. It is bounded by `exp` and by
  being one identity per ticket, so an exposure is one victim rather than a cohort.
- Where the ticket remains exposed, and why each is accepted: the visitor's own address bar and
  history (narrow); a copied or shared URL (the real one — storage-denied visitors keep a working
  credential in the address bar by design and are the people most likely to paste "the queue
  link"); and any script on the waiting page.
- **The waiting page must stay same-origin-only, permanently.** This is a property of the whole
  design and not a fragment caveat: after the strip the ticket sits in `localStorage`, the cookie
  and `sessionStorage`, all same-origin readable for the whole visit, so an analytics or
  tag-manager tag would read every visitor's ticket on the cookie path too. Stated in
  `docs/DEPLOY.md` because it is invisible until violated.
- **A link rewriter inverts the property the fragment was chosen for.** Outlook SafeLinks and
  similar wrappers percent-encode the original URL, fragment included, into a query parameter of
  their own host — so the ticket is transmitted to and logged by a third party. Tickets delivered
  into an email channel should use the custom-domain cookie path.
- The integration guide must require an `https://` `Location`. A redirect whose `Location` omits
  the fragment inherits the original's, which happens to save the chain when an issuer emits
  `http://` and a redirect-to-https hop is inserted — but that leans on inheritance rather than on
  anything the issuer controls.

## 4. Alternatives considered

**Query parameter with `history.replaceState` stripping.** Rejected above: the subresource
`Referer` leak happens before any script runs, and `replaceState` cannot undo a log write.

**Require a custom domain.** Strongest privacy posture and the simplest client, but it makes a
validated ACM certificate a precondition for standing up an event at all.

**An iframe or `postMessage` bridge from the issuer's origin.** Works, and it needs the customer to
host and maintain a page whose only job is relaying a credential — more integration surface than a
302 with a fragment, for no additional property.
