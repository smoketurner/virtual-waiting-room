# ADR-0011: Establish a session after validating the admission token

**Status:** Accepted

## Context

The admission token proves a visitor reached the front of the queue. It travels as a URL
query parameter.

## Decision

The authorizer validates the token once, sets a per-event session cookie signed over
different inputs, and strips the token from the URL before forwarding to the origin.

## Consequences

- **This is a correctness requirement, not an optimization.** The URL changes on the
  visitor's next navigation, so without a session the credential is lost on the second page
  view and the visitor is re-queued.
- The two credentials are signed over different inputs, so neither can be replayed as the
  other.
- Session lifetime supports a sliding window extended on activity and a hard cap from issue
  time. A hard cap suits a ticket on-sale, where a session should not live indefinitely
  because someone keeps clicking.
- Every authorizer decision is local. No call to the waiting-room backend on the hot path.
- The signing key's compromise permits minting admission for every event in the deployment,
  so it is rotated on a schedule.
- Queue-it implements the same split: a `queueittoken` URL parameter and a
  `QueueITAccepted-SDFrts345E-V3_{eventId}` cookie signed over a different concatenation.
