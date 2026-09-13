# ADR-0026: One position per identity, from a customer-signed entry ticket

**Status:** Superseded by [0028](0028-remove-entry-tickets.md) — entry tickets were removed.

## 1. Context

`request_id` was a client-supplied UUIDv7 and the only guard on it was
`attribute_not_exists`, which rejects a *repeat* of the same id. Minting a fresh one defeats it
entirely. Nothing else in the system bounded how many places in line one person could hold.

That matters more here than it would elsewhere, because randomization is the marquee mechanism.
[ADR-0001](0001-randomize-pre-queue-assignment.md) removes the speed advantage automated clients
hold, and the permutation in [ADR-0002](0002-seeded-permutation-not-materialised-shuffle.md) is
provably uniform — but uniform over whoever registered. Volume converts into expected share
linearly, so ten times the registrations is roughly ten times the front of the queue, drawn
perfectly fairly from a poisoned population.

The cost asymmetry ran the wrong way too. Registration is deliberately compute-free
([ADR-0005](0005-rest-api-not-http-api.md), direct API Gateway → SQS), which makes it free for the
attacker and billable for the operator.

The same defect had a benign form. `waiting.js` kept the id in `localStorage`; a browser that
denies storage minted a fresh id on every page load. Before the pre-queue shipped that visitor
merely lost their place. After it, each reload also claimed and burned a pre-queue index, so the
cohort size `N` grew with their reload count and those indices resolved to positions nobody held.

## 2. Decision

**The customer signs an entry ticket; the waiting room derives `request_id` from it.**

The ticket is a compact ES256 JWS the customer's own system mints, carrying `aud` (the event id),
`exp`, and `sub` — an opaque per-identity value. `assign_position` verifies it against a
configured P-256 public key and derives

```
request_id = uuid_shape(SHA-256("vwr/rid/v1" || 0x00 || aud || 0x00 || sub)[0..16])
```

A record whose supplied `request_id` does not equal the re-derived value is dropped. The existing
`attribute_not_exists` guard then enforces one position per identity with no new table, no new
write, and no new index.

**`sub` is opaque, not the identifier.** The customer derives it themselves, e.g.
`base64url(HMAC-SHA256(pepper, identity || event_id))`, so the waiting room never receives a
membership number, email or order reference. That keeps the integration free of any agreement
about handling customer member data, and it means a `request_id` — which is a public value, used
as a cache key on `/v1/queue_num` — carries nothing derived from a real identifier.

**Verification happens in the consumer, not at the edge.** The join path stays compute-free: the
ticket rides in the message body, API Gateway forwards it to SQS unexamined, and `assign_position`
is the first thing to look at it. A visitor whose ticket is bad still receives 200 from the join
call, so a botter learns nothing at join time about which tickets work.

**An invalid ticket is a drop, not an error.** It is counted and logged, never retried and never
dead-lettered. No redelivery makes attacker-chosen input valid, and retrying it five times before
dead-lettering would deepen the cost asymmetry this ADR exists to fix.

**Tickets are optional.** With no public key configured the deployment behaves exactly as before.
Behaviour follows the configuration rather than a separate toggle.

## 3. Consequences

- One identity yields one position, however many browsers, devices or reloads it uses. The benign
  private-browsing case is fixed by the same mechanism, since a derived id needs no storage to
  survive a reload.
- `request_id` is no longer a time-ordered UUIDv7. Nothing depended on that ordering — no sort
  key, no GSI, the controller ranks by `queue_position`, and the edge gate reads only `aud` and
  `exp` — so [ADR-0010](0010-client-supplied-request-id.md) is amended rather than reversed and the
  id is now shape-checked only.
- **The residual is a farm with N legitimate accounts, which gets N positions.** Entry tickets
  move the constraint from "can you mint ids?" to "can you obtain identities?". That is a large
  improvement and it is not a guarantee. An operator whose identities are free to create has
  moved the problem, not solved it.
- **The opaque-`sub` property is a customer obligation the code cannot enforce.** Verification
  shape-checks `sub` (22–256 base64url characters), which fails closed on an email, a raw member
  number or a UUID — the mistakes a customer would actually make — but 22 `a`s pass. It checks
  shape, not entropy.
- A well-formed public key from the *wrong pair* passes startup validation and then silently drops
  every registration. `DecodingKey::from_ec_components` cannot detect an off-curve point, and the
  only library that could is banned on this path
  (`.kiro/steering/tech.md`). A drop-rate alarm is the compensating control.
- The signing relationship is one-directional: the customer holds the private key and the waiting
  room only a public one, so compromising the waiting room does not let anyone mint tickets.

## 4. Alternatives considered

**Hash the raw identifier.** Simplest, but it puts a value derived from a real member identifier
into a public cache key, and it requires the customer to send us their identifiers at all. An
opaque subject costs them one HMAC and avoids both.

**A separate identity-claim row.** Keep the client's UUIDv7 and write a conditional claim keyed on
a keyed hash of the identifier. Preserves UUIDv7 ordering and allows re-issuing a position to a new
device, at the cost of one extra write and one extra row per visitor — and it requires storing
something derived from the identity, which the derived-id approach avoids entirely.

**Verify at the edge.** A CloudFront Function cannot do ES256, and putting a Lambda in the join
path would give up the property that the burst never touches compute.

**Rate-limit registrations per IP.** Defeated by any distributed farm, and it penalises shared
egress — offices, universities, mobile carrier NAT — which is precisely the population least able
to complain.
