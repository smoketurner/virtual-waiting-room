# Virtual Waiting Room

A virtual waiting room that deploys into your own AWS account. It holds visitors during a
traffic spike (a ticket on-sale, a product drop, a registration window) and lets them into
your origin, never more at once than it can take.

**Status: MVP.** The scheduled pre-queue and live-join paths are implemented and deployable.
Standby mode (a dormant queue that activates on a traffic threshold) is specified but not
built. [`tasks.md`](./.kiro/specs/virtual-waiting-room/tasks.md) is the record of what is built.

## Documentation

- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md): start here. The shape of the system and why.
- [`docs/DESIGN.md`](./docs/DESIGN.md): how each mechanism works, with sourced constraints.
- [`docs/DYNAMODB.md`](./docs/DYNAMODB.md): the data layer: key design, scaling, ceilings.
- [`docs/REQUIREMENTS.md`](./docs/REQUIREMENTS.md): numbered, testable requirements.
- [`docs/DEPLOY.md`](./docs/DEPLOY.md): build, configure and deploy; the `make` targets.
- [`docs/LOCAL_STACK.md`](./docs/LOCAL_STACK.md): run the shipped functions locally (`make stack`)
  and on simulated time at real scale (`make sim`).
- [`docs/RUNBOOK.md`](./docs/RUNBOOK.md): running an event: controls, readiness checklist,
  alarms, what to do when the queue stops moving.
- [`docs/adr/`](./docs/adr/): architecture decision records.
- [`.kiro/specs/virtual-waiting-room/tasks.md`](./.kiro/specs/virtual-waiting-room/tasks.md):
  build status, item by item.

## What it does

Early visitors to a scheduled event register on the waiting page. At the start time they are
randomized into queue positions in one step, and visitors who arrive after that join behind
them in arrival order. You set the origin's concurrency, the most visitors it takes at once,
and every ten seconds a controller calls as many places in line as there are free spots.
Admitted visitors carry a signed session cookie, valid for a fixed session length, that a
CloudFront Function checks on every request to the protected origin.

Operators run the event from a dashboard at `/_wr/admin` (OIDC login): change the origin
concurrency, pause, resume, open early, post a message, fail open for a bounded window, edit the
protected paths, or take the event down.

## How it works

```
join:      CloudFront → API Gateway (direct SQS integration) → SQS → assign_position Lambda → DynamoDB
status:    CloudFront (Min TTL 1 s, no cookies) → read Lambda       one origin fetch per second, however many wait
protected: CloudFront Function gate ── valid session cookie → your origin
                                    └─ otherwise → 302 / 403 → waiting page
```

- **No compute in the burst.** API Gateway writes joins straight to SQS, so there are no cold
  starts or concurrency limits at the door.
- **The queue order is never stored.** It is a seeded pseudorandom permutation computed on read,
  so opening a million-person pre-queue is one conditional DynamoDB write. A published seed lets
  anyone recompute every position.
- **Admission is bounded by origin concurrency.** Every session lasts the same fixed length, so
  the spots in use are the visitors let in during the last session length; the controller counts
  them and calls only the free spots. A called visitor's spot is held for them alone for two
  minutes, longer just after the open. A spot nobody takes in time is a missed turn and goes to
  the next place in line, so no-shows do not leave the origin empty and nobody takes another
  visitor's turn.
- **The gate makes no network calls.** It reads its rules and signing key from a CloudFront
  KeyValueStore. Requests that match no rule pass through untouched.
- **Fail-open is manual.** An operator can open the gate for a bounded window, but nothing trips
  it automatically ([#58](https://github.com/smoketurner/virtual-waiting-room/issues/58)).
- **The REST API answers only CloudFront**, through an API key only the distribution sends. On a
  CloudFront flat-rate plan, an edge web ACL adds a browser challenge and rate limits
  ([ADR-0038](./docs/adr/0038-edge-web-acl-on-flat-rate-plan.md)).
- **Near-zero idle cost.** No always-on compute or cache tier; tables are on-demand. The target is
  under $5/month between events.

## Relationship to Queue-it

[Queue-it](https://queue-it.com) is the established commercial product, and this project follows
its published architecture where it applies: pre-queue randomization for scheduled events, a
session issued after admission, closed-loop outflow control, and failing open. It differs in
two ways: positions are a verifiable permutation rather than stored numbers, and the gate runs
at the edge with no per-request call to the waiting room. It lacks Queue-it's visitor identity
controls ([#59](https://github.com/smoketurner/virtual-waiting-room/issues/59)) and its platform
connectors. If you want a managed service with an SLA, buy theirs. This is for when the traffic
cannot leave your AWS account or the budget cannot be enterprise.

## License

Apache-2.0. Portions are a clean-room reimplementation of concepts from
`aws-solutions/virtual-waiting-room-on-aws`, also Apache-2.0.
