# ADR-0010: Client supplies the request identifier

**Status:** Accepted

## Context

The visitor needs an identifier that survives from join to position lookup. API Gateway can
mint one via `$context.requestId`.

## Decision

The client generates a UUIDv7 and sends it in the join body.

## Consequences

- With a server-minted identifier, a client retry produces a new identifier and consumes a
  second position. With a client-supplied one, the retry carries the same value and is
  absorbed by the conditional write (ADR-0004).
- Version 7 rather than 4 for debuggability — join time is recoverable from the identifier —
  and sort-key headroom if a secondary index is added later.
- **The embedded timestamp is client-supplied and never trusted.** `entry_time` is stamped
  server-side and is authoritative for ordering and expiry.
- Browser `crypto.randomUUID()` emits version 4 only, so the reference client depends on the
  `uuid` package.
- Clients control the identifier namespace. A duplicate submission fails the conditional
  write, which is the correct outcome.
