# Product

A virtual waiting room for AWS. It absorbs traffic spikes that would take a site down
(ticket on-sales, product drops, registration windows) and meters visitors into the client's
origin at a rate the origin can survive.

## What it is

- **Single-tenant.** Deploys into the client's own AWS account. No component runs anywhere
  else — no shared infrastructure we operate on their behalf (N2, N3).
- **Open source, Apache-2.0.** A maintained alternative to enterprise waiting-room SaaS, for
  organizations that cannot justify enterprise pricing or cannot route traffic through a
  third party (public sector, regulated industries).
- **Commercial regions and GovCloud.** The GovCloud variant is a different topology (no
  CloudFront, no edge compute, no VPC origins), ships second, and is priced separately (N4).

## Two modes, one origin

- **Scheduled** — a known start time. Early arrivals wait on a static countdown page and are
  assigned **randomized** positions at T−0. Randomization removes the incentive to arrive
  early (arrival order would otherwise measure connection latency, not intent).
- **Standby** — dormant year-round, activates automatically when inflow crosses an operator
  threshold, then queues new visitors **FIFO** (the spike is unplanned, so arrival order
  carries information).

Both run simultaneously on one origin.

## Load-bearing principles (do not violate without an ADR)

- **The burst is removed, not absorbed.** Hold early arrivals and randomize at the start.
- **No compute in the ingest path.** API Gateway writes straight to SQS. No Lambda at the
  door means no cold start and no concurrency ceiling during a burst.
- **Randomization is one write.** Queue order is a seeded pseudorandom permutation computed
  on read, not a million stored rows.
- **Closed-loop admission.** The controller measures the no-show rate and compensates so the
  origin runs at the capacity the operator paid for.
- **Fails open.** If the waiting room is unavailable, visitors reach the site. A waiting room
  that fails closed turns its own outage into the client's.
- **Near-zero idle cost.** No always-on compute or cache tier. Idle bill under $5/mo (N1).
- **Your account, your data.** No visitor data leaves the client's account — this rules out
  email/SMS position notifications and marketing data collection (see non-goals).

## Non-goals

Physical-location queueing; replacing the client's CDN/WAF; multi-tenant SaaS; gapless
position sequences; sub-second join latency; visitor engagement/marketing widgets; email/SMS
notifications (would require collecting personal data).

## Relationship to Queue-it

We deliberately follow Queue-it's published architecture where they have learned something
(redirect-and-signed-token, pre-queue randomization + FIFO standby, separately-signed session
after first token validation, closed-loop outflow control, fail-open). The difference is
**deployment model, not architecture**: they are hosted SaaS with 25+ connectors; we deploy
into the client's account with one CloudFront/origin authorizer.

Narrative source: `docs/`. SDD source of truth: `.kiro/specs/virtual-waiting-room/`.
