# ADR-0028: Remove entry tickets

**Status:** Accepted. Supersedes [0026](0026-entry-tickets.md) and
[0027](0027-ticket-delivery-fragment-not-query.md).

## 1. Context

[ADR-0026](0026-entry-tickets.md) gated queue entry on a ticket the customer's own system
signed, deriving `request_id` from its opaque subject so one identity held one position.
[ADR-0027](0027-ticket-delivery-fragment-not-query.md) settled how the ticket reached the page.
Both shipped, off by default.

## 2. Decision

Remove the mechanism: the ticket verification, the derived `request_id`, the public-key and
cookie-name variables, the client's ticket handling, and the drop-count alarm that existed to
catch a misconfigured key.

`request_id` is client-supplied again, checked for the canonical `8-4-4-4-12` hex shape and
nothing more, and every deployment is a bare raffle.

## 3. Why

**Nothing could use it without work we do not supply.** The waiting room only verified. The
customer had to already run a login, then build and host a signing endpoint against it and
hold a P-256 private key. No deployment could turn the feature on out of the box, and the
repository shipped no key-generation helper, no signing example, and no way to test the path.

**It bounded the wrong thing.** It moved the constraint from minting identifiers to obtaining
identities. A farm with N legitimate accounts still took N positions, so the guarantee was
inherited from the customer's identity system and worth little where accounts are free and
instant. The generic case this product serves — a public onsale open to anyone — has no prior
relationship to sign about and could not use it at all.

**It added a silent failure to a path we had just finished de-silencing.** A well-formed public
key from the wrong pair passes init validation and then discards every registration. The
drop-count alarm existed solely to notice that. Deleting the feature deletes the failure mode
and two of the core module's Terraform resources with it.

## 4. Consequences

- One person can hold N places in line, bounded by nothing. This is the state
  [ADR-0001](0001-randomize-pre-queue-assignment.md) already describes: randomization removes
  the speed advantage, not the volume advantage.
- Bounding volume now needs a mechanism that costs the client something rather than one that
  needs an identity — proof of work at registration, or behavioural classification over the
  join telemetry, which is
  [#145](https://github.com/smoketurner/virtual-waiting-room/issues/145). Neither is built.
- A visitor whose browser denies every storage tier mints a fresh identifier per reload and
  takes a new place rather than recovering their old one. The `PreQueue` dedupe read keeps that
  from burning an index in the common case; it does not make the visitor whole.
- The server-drawn pre-queue shard and the dedupe read arrived with 0026 and stay. Neither
  depended on tickets.
