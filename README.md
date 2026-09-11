# Virtual Waiting Room

A virtual waiting room for AWS. Absorbs traffic spikes that would otherwise take down
your site — ticket on-sales, product drops, registration windows — and meters visitors
into your origin at a rate it can survive.

**Status: MVP.** The scheduled pre-queue and live-join happy paths are implemented and
deployable — see [`docs/DEPLOY.md`](./docs/DEPLOY.md).

- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md) — start here: the shape of the system and why
- [`docs/DEPLOY.md`](./docs/DEPLOY.md) — build → package → deploy, and the `make` targets
- [`docs/REQUIREMENTS.md`](./docs/REQUIREMENTS.md) — numbered, testable requirements
- [`docs/DESIGN.md`](./docs/DESIGN.md) — how the system works, with sourced constraints
- [`docs/DYNAMODB.md`](./docs/DYNAMODB.md) — the data layer: key design, scaling techniques, ceilings
- [`docs/adr/`](./docs/adr/) — architecture decision records: why it works that way
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
  Web Application
  Firewall (WAF) ──► CloudFront ──► API Gateway ──► SQS ──► Lambda ──► DynamoDB
   │          │              (direct integration, no compute in the burst path)
   │          └─ /status: Min TTL 1s, no cookies forwarded — CloudFront collapses
   │             simultaneous misses into one origin fetch, so origin load is
   │             independent of how many people are waiting
   └─ Bot Control · Autonomous System Number (ASN) matching · anti-DDoS
```

Admission is a signed token, validated once and exchanged for a session cookie, so the
origin never calls the waiting room on the hot path.

## Design highlights

- **The burst is removed, not absorbed.** Assigning positions by arrival order makes
  arriving early an advantage, so everyone arrives at once. Holding early arrivals on a
  countdown page and randomizing them at the start removes that incentive.
- **Randomization is one database write.** The queue order is a seeded pseudorandom
  permutation (PRP) computed on read, not a million stored rows. Assignment for a million-person
  cohort is a single conditional write, so there is no window where some people have
  positions and others do not.
- **No compute in the ingest path.** API Gateway writes straight to SQS. The burst never
  touches a function, so there are no cold starts and no concurrency ceiling at the door.
- **Closed-loop admission.** Some admitted visitors never arrive. The controller measures
  the no-show rate and compensates, so your origin runs at the capacity you paid for.
- **Fails open** — the intent, not yet the behaviour. A waiting room that fails closed turns
  its own outage into yours. The authorizer gate does this; the CloudFront gate does not, and
  an outage of the token path currently refuses every visitor
  ([#58](https://github.com/smoketurner/virtual-waiting-room/issues/58)). Read that issue
  before running an event on this.
- **Near-zero idle cost.** No always-on compute or cache tier. Tables are pre-warmed before
  an event and cost nothing between them.
- **Your account, your data.** Commercial regions or GovCloud. Nothing runs anywhere else.

## Relationship to Queue-it

[Queue-it](https://queue-it.com) has run this problem since 2010 — 150+ billion visitors,
1,000+ organizations — and publishes a great deal about how their system works. This
project deliberately follows their architecture wherever they have learned something: the
redirect-and-signed-token model, pre-queue randomization for scheduled events with
first-in-first-out (FIFO) for threshold-triggered ones, a separately-signed session after
the first token validation, closed-loop outflow control that compensates for no-shows, and
failing open when the waiting room is unreachable.

Two things here are not theirs. Queue position is a keyed permutation computed on read, so a
published seed lets anyone recompute every position and **prove the raffle was a raffle** —
Queue-it materializes queue numbers and asks you to trust them. And the gate is CloudFront's
own trusted key group rather than code on the request path, so admission costs nothing per
request and works against an origin you cannot run code near.

Two things of theirs are missing, and they are not breadth. **Identity**: they enforce one
position per person (visitor identification keys, invite-only rooms, IP binding, proof of work,
bot mitigation deferred until randomization). Randomization turns volume into expected share,
so a raffle without identity is a raffle a bot farm wins
([#59](https://github.com/smoketurner/virtual-waiting-room/issues/59)). And **a decision point
in the request path**, which is what their connector is: removing it is what makes this free
per request, and it is why fail-open, standby activation, admission revocation and per-request
rules are all open issues.

Their 25+ platform connectors are a real moat and this does not attempt to match them. If you
want a managed service with a service level agreement (SLA) and connectors for every stack, buy
theirs. This is for the cases where the traffic cannot leave your account, or the price cannot
be enterprise.

## License

Apache-2.0. Portions are a clean-room reimplementation of concepts from
`aws-solutions/virtual-waiting-room-on-aws`, also Apache-2.0.
