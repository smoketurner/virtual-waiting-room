# ADR-0013: Separate cache behaviours to preserve request collapsing

**Status:** Accepted

## Context

Requirement C4 states that origin request rate must stay flat as waiters scale from 10,000
to 1,000,000. This depends on CloudFront request collapsing: N simultaneous misses for the
same cache key become one origin fetch.

## Decision

Three cache behaviours: polled endpoints (Min TTL 1 s, no cookies forwarded), write endpoints
(uncached), protected origin (uncached, session cookie forwarded).

## Consequences

From AWS guidance on DDoS resilience: "The following configurations prevent request
collapsing from occurring: The Minimum TTL of a cache behavior is set to 0. Cookie forwarding
is enabled in the cache policy, the origin request policy, or the legacy cache settings."

- **Minimum TTL must be greater than zero** on every cached behaviour. A 0-second minimum
  disables collapsing even when the origin sends `Cache-Control: max-age=5`.
- **Polled endpoints must forward no cookies.** The authorizer needs the session cookie on
  protected-origin requests, so that must be a separate behaviour. Mixing cookie forwarding
  into a polled behaviour's cache policy would send every poll to the origin.
- `/status` combines phase, serving position, admission rate and operator message into one
  payload, so a waiting visitor makes one request per interval rather than four.
- The client poll interval is the dominant cost variable in the system, multiplying
  CloudFront requests, WAF inspections and Bot Control charges together. Default is 10
  seconds.
