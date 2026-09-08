# ADR-0006: Drive position expiry from the controller, not DynamoDB TTL

**Status:** Accepted

## Context

A position released but never claimed must expire so the serving counter can advance past it
(F3.9). DynamoDB Time to Live is the obvious mechanism.

## Decision

Expire positions in the outflow controller on its 10-second interval. Retain TTL on
`Positions` only for post-event storage reclamation.

## Consequences

From the TTL documentation: DynamoDB "automatically deletes expired items **within a few
days** of their expiration time," and expired items "might be deleted by the system at any
time, typically within a few days after their expiration." Expired items remain readable
until deleted: "Use filter expressions to remove expired items from `Scan` and `Query`
results."

- Days of latency cannot reclaim capacity during an event lasting minutes.
- The controller queries positions whose `expires_at` has passed with `status = issued`,
  marks them expired, and advances `max_expired_position`.
- Reads that could observe a TTL-pending item apply a `FilterExpression` on `expires_at`, so
  a deleted-but-still-visible item is never returned as live.
- `expires_at` and `ttl` are separate attributes with different purposes.
