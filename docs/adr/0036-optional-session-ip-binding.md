# ADR-0036: Sessions can be bound to the visitor's network, off by default

**Status:** Accepted

## 1. Context

The session cookie is a bearer credential (issue #61). The gate checks the signature, the event
(its `aud` claim, which already closed the cross-event half of #61) and the expiry, and nothing
ties the cookie to the client it was issued to. So one automated client can clear the queue and
hand the cookie to a fleet for `session_ttl_seconds`, and for a hype event that cookie is the
scarce good.

`docs/ARCHITECTURE.md` had recorded why no binding was built: every binding available at the edge
breaks real visitors. An IP binding breaks mobile handoff and carrier NAT; a TLS fingerprint
breaks on a browser update; a device key needs JavaScript crypto and key storage. That is still
true. It argues for making the binding optional, not for leaving it out. Queue-it ships IP binding
per waiting room for the same reason.

## 2. Decision

**`generate_token` tags every session with the visitor's network, and the gate enforces the tag
only while the operator has turned IP binding on.**

- **The tag.** `HMAC-SHA256(ip_key, network)`, base64url, truncated to 22 characters, carried as
  the private claim `cip`. `ip_key` is derived from the deployment secret with its own label
  (`vwr/ip/v1`), the same way the session key is. `network` is the dotted address for IPv4 and
  the first four hextets for IPv6: privacy addresses rotate within a /64, so binding the exact
  IPv6 address would break visitors the binding is not aimed at. The tag is keyed so that the
  cookie never holds a value an IPv4 address could be brute-forced back out of. Queue-it's
  connector hashes the IP for the same reason.
- **The address.** API Gateway sees CloudFront, not the visitor, so `/v1/generate_token` gets its
  own origin request policy that forwards `CloudFront-Viewer-Address` (and cookies, and no
  `Host`). The gate reads `event.viewer.ip`. A session minted without the header, which only
  happens to a request that bypassed CloudFront (#177), carries no tag.
- **The switch.** `b` in the gate's KeyValueStore config document, set from the dashboard
  (`/admin/ip_binding`) and audited as `set_ip_binding`. It is absent, meaning off, in every
  document written before this, including the Terraform seed. Every session is tagged whether or
  not binding is on, so turning it on mid-event also covers sessions issued earlier.
- **The refusal.** A mismatched or missing tag is refused with its own reason, `ip`, as a 302 to
  the waiting page or a 403 for XHR, like the other reasons (#73). The waiting page runs its
  normal flow: the visitor's position is still served, so it redeems again and gets a session
  tagged for the new network. That one extra round trip is the whole cost of a legitimate
  network change. A second `ip` refusal within a minute means the new session is not holding
  either, so the page stops and tells the visitor to stay on one connection instead of bouncing.

Rust (`SigningKey::ip_tag`, `ip_network`) and the gate (`ipTag`) compute the tag independently.
Conformance vectors generated from the Rust side pin that they agree, including compressed,
uppercase and IPv4-mapped IPv6 forms and non-addresses.

## 3. Consequences

- **With binding on, a copied cookie does not work from another network.** The fleet has to
  redeem for each network it uses, and that needs the request id *and* its possession secret
  ([ADR-0035](0035-request-id-proof-of-possession.md)). Sharing both hands over the place itself,
  which no edge-side binding can prevent.
- **Visitors behind the same NAT share a network,** so binding does not separate them. A cookie
  shared inside one office or one carrier NAT still works. The binding stops wide
  redistribution, not neighbours.
- **A visitor whose address changes** (Wi-Fi to mobile, some carrier NATs) bounces through the
  waiting page once and is let straight back in. A visitor whose address changes on every
  request cannot hold a session and is told why. This is why binding is off by default, and why
  the runbook names the trade-off next to the switch.
- **The gate does two more HMACs,** only for a request that matches a rule, only while binding is
  on. The function source stays under its size budget; the gate was re-indented to two spaces,
  which also matches `waiting.js`, to keep the headroom the budget check asks for.
- **Revocation (#63) is unchanged.** A bound session still cannot be revoked before it expires.
