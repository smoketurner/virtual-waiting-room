# ADR-0010: Client supplies the request identifier

**Status:** Accepted

## Context

The visitor needs an identifier that survives from join to position lookup. API Gateway can
mint one via `$context.requestId`.

## Decision

The client supplies the request identifier in the join body.

**Amended by [ADR-0026](0026-entry-tickets.md).** The version is no longer part of the
contract: an id is checked for the canonical `8-4-4-4-12` hex shape and nothing more. Where an
entry ticket is configured the id is not client-chosen at all — it is derived from the ticket's
subject and any other value is rejected.

## Consequences

- With a server-minted identifier, a client retry produces a new identifier and consumes a
  second position. With a client-supplied one, the retry carries the same value and is
  absorbed by the conditional write (ADR-0004).
- **The embedded timestamp is client-supplied and never trusted.** `entry_time` is stamped
  server-side and is authoritative for ordering and expiry.
- **Nothing depends on the id being time-ordered.** Version 7 was chosen for debuggability and
  for sort-key headroom should a secondary index ever be added. The headroom was never used:
  there is no sort key and no secondary index, the controller ranks by `queue_position`, and
  the edge gate reads only `aud` and `exp`. So dropping the version check to admit a derived
  id costs nothing that was being relied on.
- Clients control the identifier namespace **only on an unticketed deployment**, and a
  duplicate submission fails the conditional write, which is the correct outcome. What that
  conditional write never did was bound how many *distinct* ids one person could mint — see
  ADR-0026 for the mechanism that does, and for what it still does not cover.
