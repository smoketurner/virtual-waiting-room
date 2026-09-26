# Product

A virtual waiting room for AWS. It absorbs traffic spikes that would take a site down
(ticket on-sales, product drops, registration windows) and meters visitors into the operator's
origin at a rate the origin can survive.

## Who it is for

Anyone running a large-scale event who does not want to pay a waiting-room vendor. That is the
whole positioning. It sets two bars, and both are testable:

- **Reliable.** The waiting room must never be the reason the site is down. An operator takes
  on the failure modes a vendor would have absorbed, so every one of them has to be named,
  bounded, and rehearsed — not discovered during an event.
- **Affordable.** Idle cost near zero (N1), and event cost modelled per event (O6). The poll
  interval is the dominant multiplier across CloudFront, WAF and Bot Control (DESIGN §12).

- **Single-tenant.** Deploys into the operator's own AWS account. No component runs anywhere
  else — no shared infrastructure we operate on their behalf (N2, N3).
- **Open source, Apache-2.0.** For organizations that cannot justify enterprise pricing or
  cannot route traffic through a third party (public sector, regulated industries).
- **Commercial regions and GovCloud.** The GovCloud variant is a different topology (no
  CloudFront, no edge compute, no VPC origins), ships second, and is priced separately (N4).

## Two modes, one origin

- **Scheduled** — a known start time. Early arrivals wait on a static countdown page and are
  assigned **randomized** positions at T−0. Randomization removes the incentive to arrive
  early (arrival order would otherwise measure connection latency, not intent).
- **Standby** — dormant year-round, activates automatically when inflow crosses an operator
  threshold, then queues new visitors **FIFO** (the spike is unplanned, so arrival order
  carries information).

Both are designed to run simultaneously on one origin. The gate supports dormancy (#60), but
**standby activation is not built** — nothing flips the phase when inflow crosses the threshold —
so only the scheduled mode is served end to end.

## Load-bearing principles (do not violate without an ADR)

- **The burst is removed, not absorbed.** Hold early arrivals and randomize at the start.
- **No compute in the ingest path.** API Gateway writes straight to SQS. No Lambda at the
  door means no cold start and no concurrency ceiling during a burst.
- **Randomization is one write, and it is auditable.** Queue order is a seeded pseudorandom
  permutation computed on read, not a million stored rows. The property that distinguishes us
  is not the write count — it is that a published seed, offsets and count let any third party
  recompute every position and prove the raffle was a raffle (F1.5).
- **Closed-loop admission.** The controller measures the no-show rate and compensates so the
  origin runs at the capacity the operator paid for.
- **Fails open.** If the waiting room is unavailable, visitors reach the site. A waiting room
  that fails closed turns its own outage into the operator's.
  **Only half-held today** (#58): the mechanism exists — an operator engages fail-open and the
  edge honours it at once — but nothing trips it automatically, because the gate makes no
  network calls and cannot observe the backend. Until something can, an outage of the token
  path 403s every visitor, and no document should claim the property unqualified.
- **Near-zero idle cost.** No always-on compute or cache tier. Idle bill under $5/mo (N1).
- **Your account, your data.** No visitor data leaves the operator's account — this rules out
  email/SMS position notifications and marketing data collection (see non-goals).

## Non-goals

Physical-location queueing; replacing the operator's CDN/WAF; multi-tenant SaaS; gapless
position sequences; sub-second join latency; visitor engagement/marketing widgets; email/SMS
notifications (would require collecting personal data).

## Relationship to Queue-it

We deliberately follow Queue-it's published architecture where they have learned something,
and the convergence is close: redirect-and-signed-token, pre-queue randomization with FIFO for
latecomers, a separately-signed session after first token validation, closed-loop outflow
control with no-show compensation on a 10-second interval, fail-open, DynamoDB as the
coordination backbone. The queueing mechanics here are not novel, and the ingest path
(API Gateway → SQS → Lambda → atomic counter → poll → exchange for a token) is close to the
AWS Virtual Waiting Room solution.

Two things are ours:

- **Position as a keyed permutation computed on read** (ADR-0002). Queue-it materializes queue
  numbers; the AWS solution stores them in ElastiCache; Cloudflare orders one-minute buckets
  and has no per-visitor position. Only ours makes the ordering independently verifiable.
- **A gate that decides at the edge** (ADR-0021). A CloudFront Function on the protected
  behaviour only, reading its rules and signing secret from a KeyValueStore — no round trip to
  any backend, and it works against an origin we cannot put code near.

What they have that we do not, and it is not only breadth:

- **Identity.** One position per person, enforced — visitor identification keys, invite-only
  rooms, enqueue tokens, IP binding, proof-of-work, reputation, deferred bot mitigation at
  randomization. Randomization converts volume into expected share, so a raffle without
  identity is a raffle a bot farm wins (#59). The edge web ACL (ADR-0038) makes each place cost
  a browser challenge solve, which prices out a script but not a farm running real browsers.
  This is still the gap that matters most.
- **A gate that can observe the backend.** Their connector and Cloudflare's Worker can call
  home; our CloudFront Function makes no network calls. That is why automatic fail-open (#58)
  and revocation (#63) are open, and the no-JavaScript path (#67) is open too.

The difference is therefore **architecture as well as deployment model**, and the consequences
above are the cost of ADR-0021.

Narrative source: `docs/`. SDD source of truth: `.kiro/specs/virtual-waiting-room/`. What is
actually built is tracked in `.kiro/specs/virtual-waiting-room/tasks.md`.
