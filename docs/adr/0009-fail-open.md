# ADR-0009: Fail open when the waiting room is unavailable

**Status:** Accepted. The mechanism below is not the one built: there is no bypass cookie and no
per-client fail-closed setting (both went with the origin authorizer,
[0032](0032-remove-the-origin-authorizer.md)). Fail-open is an operator-set deadline,
`fail_open_until`, which the gate honours ([0021](0021-edge-function-gate.md)); for a regional
impairment see [0034](0034-single-region-failure-domain.md).

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
