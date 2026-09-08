# Virtual Waiting Room

A virtual waiting room for AWS. Absorbs traffic spikes that would otherwise take down
your site — ticket on-sales, product drops, registration windows — and meters visitors
into your origin at a rate it can survive.

**Status: design phase.** No runnable code yet.

- [`docs/REQUIREMENTS.md`](./docs/REQUIREMENTS.md) — numbered, testable requirements
- [`docs/DESIGN.md`](./docs/DESIGN.md) — architecture and rationale, every claim sourced
- [`docs/PLAN.md`](./docs/PLAN.md) — phased implementation plan

## Why this exists

Virtual waiting rooms are enterprise software. The established option starts around
$10K/year, is sales-led, and has no self-serve tier. AWS published a free reference
implementation in 2022 and [deprecated it in November 2025](https://github.com/aws-solutions/virtual-waiting-room-on-aws),
pointing customers at AWS Marketplace or at building their own.

That leaves a gap for organizations that need one but cannot justify enterprise pricing,
or cannot route their traffic through a third party at all — public sector, regulated
industries, anyone whose compliance boundary ends at their own AWS account.

This is a maintained, open-source implementation that deploys into that account.

## What it does

**Scheduled events.** Early visitors gather on a countdown page. At the start time they
are randomized into queue positions, then admitted to your origin at a rate you control.

**Standby protection.** The queue sits dormant year-round and activates automatically when
traffic crosses a threshold you set — insurance against a spike nobody planned for.

**Both at once.** A scheduled room on your product page with a deliberately low admission
rate, plus standby across the rest of the site for visitors who hit the homepage instead.

## How it works

```
  WAF ──► CloudFront ──► API Gateway ──► SQS ──► Lambda ──► DynamoDB
   │          │              (direct integration, no compute in the burst path)
   │          └─ /status: Min TTL 1s, no cookies forwarded — CloudFront collapses
   │             simultaneous misses into one origin fetch, so origin load is
   │             independent of how many people are waiting
   └─ Bot Control · ASN matching · Anti-DDoS
```

Admission is a signed token, validated once and exchanged for a session cookie, so the
origin never calls the waiting room on the hot path.

## Design highlights

- **The burst is removed, not absorbed.** Assigning positions by arrival order makes
  arriving early an advantage, so everyone arrives at once. Randomizing among everyone
  present at the start drops peak write load from ~1,000,000/sec to ~3,300/sec — inside
  default AWS quotas.
- **No compute in the ingest path.** API Gateway writes straight to SQS. The burst never
  touches a function, so there are no cold starts and no concurrency ceiling at the door.
- **Closed-loop admission.** Some admitted visitors never arrive. The controller measures
  the no-show rate and compensates, so your origin runs at the capacity you paid for.
- **Fails open.** If the waiting room is unavailable, visitors reach your site. A waiting
  room that fails closed turns its own outage into yours.
- **Near-zero idle cost.** No always-on compute or cache tier. Tables are pre-warmed before
  an event and cost nothing between them.
- **Your account, your data.** Commercial regions or GovCloud. Nothing runs anywhere else.

## Relationship to Queue-it

[Queue-it](https://queue-it.com) has run this problem since 2010 — 150+ billion visitors,
1,000+ organizations — and publishes a great deal about how their system works. This
project deliberately follows their architecture wherever they have learned something: the
redirect-and-signed-token model, pre-queue randomization for scheduled events with FIFO for
threshold-triggered ones, a separately-signed session after the first token validation,
closed-loop outflow control that compensates for no-shows, and failing open when the
waiting room is unreachable.

The difference is deployment model, not architecture. Queue-it is hosted SaaS with 25+
platform connectors; that breadth is their moat and this does not attempt to match it. If
you want a managed service with an SLA and connectors for every stack, buy theirs. This is
for the cases where the traffic cannot leave your account, or the price cannot be
enterprise.

## License

Apache-2.0. Portions are a clean-room reimplementation of concepts from
`aws-solutions/virtual-waiting-room-on-aws`, also Apache-2.0.
