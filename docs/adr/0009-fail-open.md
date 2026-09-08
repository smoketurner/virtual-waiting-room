# ADR-0009: Fail open when the waiting room is unavailable

**Status:** Accepted

## Context

If the waiting room API is unreachable, the authorizer must either admit visitors or block
them.

## Decision

Admit the visitor with a time-limited bypass cookie while the client retries in the
background. Configurable to fail closed per client.

## Consequences

- A waiting room that fails closed converts our outage into the client's outage, which is
  worse than having no waiting room.
- The origin may briefly receive unmetered traffic, which is the condition the waiting room
  exists to prevent. That risk is accepted: an unprotected origin may degrade, whereas a
  fail-closed waiting room guarantees a total outage.
- Queue-it's Direct Pass behaves the same way: on service disruption, visitors "continue to
  your site with a time-out cookie while the Connector retries connection in the background."
- Clients who prefer fail-closed can configure it, with the tradeoff documented.
