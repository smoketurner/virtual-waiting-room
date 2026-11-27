# Requirements: Virtual Waiting Room

EARS form of [`docs/REQUIREMENTS.md`](../../../docs/REQUIREMENTS.md), the source of record.
MUST = SHALL, SHOULD = SHOULD. IDs match that document exactly; build state is in
[`tasks.md`](tasks.md), rationale in [`docs/adr/`](../../../docs/adr/).

## Introduction

The virtual waiting room meters visitors into an AWS origin no faster than the origin can serve them,
holding overflow on operator pages and admitting visitors under operator control. It runs
scheduled events (known start time) and standby protection (dormant until inflow crosses a
threshold). Every deployment is single-tenant, inside the client's own AWS account.

---

## 1. Functional

### 1.1 Event lifecycle

**F0.1** — As an operator, I want a defined lifecycle so that behaviour is predictable.
- THE SYSTEM SHALL progress an event through **idle → pre-queue → active → post-event** phases.
- Acceptance: Each phase is observable and the transitions are scheduled or manual.

**F0.2** — As a visitor, I want a relevant page at each phase.
- THE SYSTEM SHALL serve an operator-authored page for each phase.
- Acceptance: Idle shows event information before the pre-queue opens; post-event shows outcome and next steps.

**F0.3** — As an operator, I want a room to open at a known start time.
- WHERE an event is **scheduled**, THE SYSTEM SHALL open it at its scheduled time.
- Acceptance: A configured event opens at its scheduled time.

**F0.4** — As an operator, I want surges absorbed without my intervention.
- WHERE **standby** is configured, THE SYSTEM SHALL activate automatically WHEN inflow crosses an operator-configured threshold.
- Acceptance: Below threshold, visitors pass through untouched. Above it, new visitors are queued without operator action.

**F0.5** — As an operator, I want scoped rooms and site-wide protection to coexist.
- THE SYSTEM SHALL run scheduled and standby modes simultaneously on one origin.
- Acceptance: A scheduled room on `/product/x` with a low origin concurrency coexists with standby protection across the whole site.

**F0.6** — As an operator, I want to declare what is protected.
- THE SYSTEM SHALL allow the operator to declare protected requests by path, header, cookie, or user agent.
- Acceptance: An unprotected path is never queued, in any mode.

**F0.7** — As an operator, I want standby state visible and overridable.
- THE SYSTEM SHALL make standby activation observable and manually overridable.
- Acceptance: Operator can force-activate or force-dormant; state is visible in metrics.

**F0.8** — As an operator, I want to hold every visitor on demand.
- WHEN maintenance mode is enabled, THE SYSTEM SHALL park all visitors on an operator page.
- Acceptance: Enabling it holds every visitor regardless of mode or capacity.

**Fairness by mode.** Scheduled events randomize among pre-queue participants; standby
activation queues FIFO.

### 1.2 Pre-queue (scheduled events)

**F1.1** — As an early visitor, I want a countdown rather than a position, registered with one bounded write.
- WHILE an event is in the pre-queue phase, THE SYSTEM SHALL hold visitors on a countdown page rather than assign a position, and SHALL register each with one row write plus a share of one amortised shard-counter claim per batch.
- Acceptance: A visitor arriving at T−10min sees a countdown and is registered; no `Positions` item and no position exists until the event opens.

**F1.2** — As an operator, I want countdown views served from cache.
- WHILE an event is in the pre-queue phase, THE SYSTEM SHALL serve the countdown page entirely from CDN cache, with zero calls to API Gateway, DynamoDB or SQS per view; registration SHALL be a separate, one-time direct write from the edge to the ingest queue, with no compute in the path.
- Acceptance: Origin requests from page views are independent of visitor count; each visitor registers once, deduplicated across reloads wherever the browser allows client-side storage; no Lambda in the write's path. Known residuals: a storage-denied visitor takes a new place on each reload, and two reloads in one batching window can each claim.

**F1.3** — As an operator, I want ordering that early registration cannot game.
- WHEN T−0 is reached, THE SYSTEM SHALL assign pre-queue positions in **randomized** order, and SHALL NOT make the ordering predictable before that moment.
- THE SYSTEM SHALL treat a join accepted within 15 s after T−0 as a pre-queue participant (ADR-0043).
- Acceptance: No correlation between position and registration time; positions uniformly distributed; the permutation key does not exist before T−0; membership is decided by when SQS accepted the join, not when it was processed.

**F1.4** — As an operator, I want assignment not to delay opening.
- WHEN T−0 is reached, THE SYSTEM SHALL complete pre-queue position assignment promptly.
- Acceptance: 1,000,000 participants assigned in one write; elapsed time independent of cohort size.

**F1.5** — As an auditor, I want reproducible ordering.
- THE SYSTEM SHALL make the randomization recomputable after the fact. This checks the arithmetic, not that the seed was drawn fairly (ADR-0040).
- Acceptance: A third party given the published seed, participant count and registration indices recomputes every position exactly.

### 1.3 Queue join (live arrivals)

**F2.1** — As a live arrival, I want a position behind the pre-queue.
- WHEN a visitor joins after the event opens, THE SYSTEM SHALL assign a position in arrival order.
- Acceptance: A visitor joining at T+5min receives a position after all pre-queue participants; joins after the draw closes are numbered in the order SQS accepted them, to within a few seconds, and let in in line order, to within one poll, while the API is not saturated.

**F2.2** — As a visitor, I want a unique position.
- THE SYSTEM SHALL NOT issue any queue position twice.
- Acceptance: Under concurrent load, the set of issued positions contains zero duplicates.

**F2.3** — As an operator, I want gaps measured, not fought.
- THE SYSTEM SHALL allow queue positions to contain gaps.
- Acceptance: Not a defect. Gap rate is measured and reported.

**F2.4** — As the system, I want a client-supplied identifier so joins are idempotent.
- WHEN a client joins, THE SYSTEM SHALL require a client-supplied request identifier (UUIDv7).
- Acceptance: A join with no request id, or one longer than a UUID, is rejected at the gateway with 400. One that is not UUID-shaped is accepted and dropped by `assign_position` without a position; the version is not checked.

**F2.5** — As a retrying client, I want idempotent joins.
- WHEN a join is repeated with the same request ID, THE SYSTEM SHALL NOT consume an additional position.
- Acceptance: Duplicate submission returns the original position.

**F2.6** — As the system, I want malformed joins to cost nothing.
- WHEN a join is malformed, THE SYSTEM SHALL NOT consume a queue position.
- Acceptance: Sending N malformed payloads leaves the counter unchanged.

### 1.4 Waiting and admission

**F3.1** — As a waiting visitor, I want to see my progress.
- THE SYSTEM SHALL let a visitor read their own position and the current serving position.
- Acceptance: `GET /_wr/v1/queue_num` and `GET /_wr/v1/status` return correct values.

**F3.2** — As an operator, I want to throttle admission during the event.
- WHEN the operator changes the origin concurrency, THE SYSTEM SHALL keep no more admitted visitors' sessions live at once than the new limit.
- Acceptance: `POST /_wr/admin/concurrency` changes the limit; no more sessions than it are ever live, and the change takes effect within one controller pass.

**F3.3** — As an admitted visitor, I want a credential that needs no backend call.
- WHEN a visitor is admitted, THE SYSTEM SHALL issue a cryptographically verifiable session credential.
- Acceptance: The session cookie is signed; the edge gate verifies it locally.

**F3.4** — As the origin, I want only admitted visitors to reach me.
- IF a request has no valid session, THEN THE SYSTEM SHALL refuse it at the edge.
- Acceptance: A request with no credential, an expired one, or one for another event is refused at the edge.

**F3.5** — As an admitted visitor, I want not to be re-checked on every request.
- WHEN a visitor's position is reached, THE SYSTEM SHALL establish a **session**.
- Acceptance: A visitor navigates to a second page without re-presenting anything and is not re-queued.

**F3.8** — As an operator, I want the origin kept full despite no-shows.
- THE SYSTEM SHALL compensate admission control for **no-shows**, called visitors who never arrive at the origin, without letting anyone take another visitor's turn.
- THE SYSTEM SHALL count each admitted visitor once however many times they call `/_wr/v1/generate_token`.
- Acceptance: A called visitor's spot is held for them alone for 2 minutes (longer at the open) and then called for the next place in line; with a 30% no-show rate and the default 900 s session the origin's spots stay above 80% full; three reloads take one spot, not three.

### 1.5 Failure behaviour

**F4.1** — As a visitor, I want an outage of the waiting room not to block me.
- IF the waiting room is unavailable, THEN THE SYSTEM SHALL let visitors proceed to the origin.
- Acceptance: With the waiting room API returning 5xx, the origin remains reachable.

**F4.2** — As the system, I want fail-open bounded.
- WHERE fail-open is in effect, THE SYSTEM SHALL time-limit the bypass and the client SHALL retry in the background.
- Acceptance: Bypass expires; normal queueing resumes without user action.

**F4.3** — As a strict client, I want to choose fail-closed.
- WHERE a client requires fail-closed, THE SYSTEM SHALL allow fail-open to be overridden per client.
- Acceptance: A client requiring fail-closed can configure it, with the tradeoff documented.

**F4.4** — As a client, I want a lost join to be recoverable.
- IF a join is lost downstream, THEN THE SYSTEM SHALL allow the client to recover it.
- Acceptance: `GET /_wr/v1/queue_num` returns 404; the client re-joins with the same request id and succeeds because no row exists for it yet.

**F4.5** — As a client, I want throttling handled gracefully.
- WHEN the client receives HTTP 429, THE client SHALL retry with jittered backoff.
- Acceptance: Under gateway throttling, no user-visible error; joins succeed on retry.

### 1.6 Operator experience

**F5.1** — As an operator, I want live metrics.
- THE SYSTEM SHALL show live event metrics: inflow, outflow, queue depth, admitted, no-show rate.
- Acceptance: Metrics visible in CloudWatch within one 60 s metric period.

**F5.2** — As a client, I want to brand the waiting page without forking.
- THE SYSTEM SHALL allow the waiting page to be branded by the client without forking the module.
- Acceptance: Client supplies template assets; no code change required.

**F5.3** — As an operator, I want to message waiting visitors.
- WHEN the operator publishes a message, THE SYSTEM SHALL display it to waiting visitors.
- Acceptance: Message appears on the waiting page within the cache TTL.

**F5.4** — As a waiting visitor, I want my position and estimated wait.
- THE SYSTEM SHALL show waiting visitors their position and an estimated wait time.
- Acceptance: Both displayed and updated as the queue advances.

**F5.5** — As an operator, I want every action scriptable.
- THE SYSTEM SHALL make every operator action available through an API, not only a console.
- Acceptance: Concurrency change, reset, pause, message publish, and mode override all scriptable.

### 1.7 Abuse mitigation

No requirement here is currently met: nothing binds a place to a person. See
[`docs/REQUIREMENTS.md`](../../../docs/REQUIREMENTS.md) §1.7 and ADR-0038 for what the edge
web ACL does and does not bound. F6.1–F6.3 are retired below.

### 1.8 Operator web interface

A thin server-rendered client over the operator API (F5.1–F5.5); it adds no capability the API lacks.

**F7.1** — As an operator, I want a web interface so that I need not script every action.
- THE SYSTEM SHALL serve the operator interface from one Axum Lambda rendering server-side HTML with askama templates.
- Acceptance: Reachable at `/_wr/admin`; one Lambda; no browser SPA bundle.

**F7.2** — As an operator, I want a consistent, dependency-free look.
- THE SYSTEM SHALL style the interface in the Vouch design language as one self-contained stylesheet (ADR-0018).
- Acceptance: No runtime npm dependency and no build-time token extraction.

**F7.3** — As an operator, I want parity with the admin API.
- THE interface SHALL expose every operator capability in the admin API and no other.
- Acceptance: Each `/_wr/admin/*` action has a UI control; F5.5 still holds.

**F7.4** — As a security owner, I want one auth path.
- WHEN an operator makes any interface request, THE SYSTEM SHALL authenticate it through one path, with no weaker second one.
- Acceptance: OIDC Authorization Code + PKCE session enforced in the Lambda (ADR-0016); unauthenticated requests are redirected to login.

**F7.5** — As an operator, I want core actions without a client-side runtime.
- THE interface SHALL be server-rendered with no React runtime; interactivity is HTML forms plus minimal progressive-enhancement JavaScript.
- Acceptance: Core actions work with JavaScript disabled.

**F7.6** — As an operator, I want live metrics in the interface.
- THE interface SHALL display live metrics: inflow, outflow, queue depth, admitted, no-show rate.
- Acceptance: Metrics render and refresh within one 60 s metric period.

---

## 2. Capacity

Requirements on the deployed system, not on AWS defaults. Every figure requires the
pre-event preparation in §4.

**C1** — THE SYSTEM SHALL support at least 1,000,000 concurrent pre-queue participants.
- Acceptance: Load test sustains 1M countdown-page holders.

**C2** — WHEN the pre-queue cohort is assigned, THE SYSTEM SHALL make the assignment atomic.
- Acceptance: A single conditional write; a reader sees either no participant assigned or all of them.

**C3** — THE SYSTEM SHALL sustain ≥ 10,000 joins/sec at default quotas, and ≥ 40,000/sec with quota increases filed.
- Acceptance: Load test at both levels; zero duplicates at each.

**C4** — THE SYSTEM SHALL keep origin polling load independent of visitor count.
- Acceptance: Origin RPS for `/status` stays flat as waiters scale from 10K to 1M.

**C5** — WHEN a spike arrives in under 5 seconds, THE SYSTEM SHALL NOT drop joins.
- Acceptance: Joins are durably enqueued even when compute has not yet scaled.

---

## 3. Non-functional

**N1** — THE SYSTEM SHALL approach zero idle cost, with no always-on compute or cache tier.
- Acceptance: Monthly bill for an idle deployment is under $5 excluding pre-warming.

**N2** — THE SYSTEM SHALL deploy into the client's own AWS account.
- Acceptance: `terraform apply` from a clean account produces a working deployment.

**N3** — THE SYSTEM SHALL NOT require the vendor to operate shared infrastructure on clients' behalf.
- Acceptance: No component runs in a Smoke Turner account.

**N4** — THE SYSTEM SHALL support commercial AWS regions; GovCloud (US) is out of scope until a gate exists for it.
- Acceptance: The commercial variant deploys and passes functional tests. GovCloud offers no CloudFront Function, so it has no gate (ADR-0032).

**N5** — THE SYSTEM SHALL express infrastructure as Terraform.
- Acceptance: No manual console steps in the deployment path.

**N7** — Edge abuse mitigation.
- THE SYSTEM SHALL refuse every request without a valid session at the edge, before the origin.
- THE SYSTEM SHALL answer API requests from its own distribution only: every REST API method requires an API key only CloudFront holds.
- WHERE `flat_rate_plan_tier` is set, THE SYSTEM SHALL attach the edge web ACL: a challenge on every JavaScript API call, rate limits per token and per IP, managed rules in Count until promoted (O5).
- THE SYSTEM SHALL NOT create a web ACL by default, since on pay-as-you-go it bills against N1 (ADR-0038).
- Acceptance: The gate runs at viewer-request on the protected behaviour; the join burst reaches SQS with no compute in the path; a request straight to `execute-api` is refused with 403; with the web ACL on, a join without a WAF token is challenged and the no-JavaScript queue never is (ADR-0037).

**N8** — THE SYSTEM SHALL document the API as an OpenAPI specification.
- Acceptance: Spec published; client and admin surfaces generated from it.

**N9** — THE SYSTEM SHALL isolate concurrent events in one deployment from each other.
- Acceptance: One event at its throughput ceiling does not increase join latency or error rate for another.

**N10** — THE SYSTEM SHALL make client polling cost scale with distance to the front, not with waiting visitors × a fixed interval.
- Acceptance: Poll count is O(log) in the starting wait; the client's request total is materially below a fixed-interval client's at identical settings.

---

## 4. Operational

Contractual deliverables. Without these the capacity requirements are not met.

**O1** — THE SYSTEM SHALL have DynamoDB tables pre-warmed before each event.
- Acceptance: Warm throughput (`warm_throughput_write_units`) ≥ the event's target write rate, verified before T−0 by the dashboard's Readiness panel.

**O2** — THE SYSTEM SHALL have service quota increases filed with lead time.
- Acceptance: API Gateway RPS and DynamoDB per-table WRU confirmed raised before T−0; the Readiness panel shows the limits in force.

**O3** — THE SYSTEM SHALL have a load test at the event's origin concurrency executed before the event.
- Acceptance: Report produced and reviewed with the client.

**O4** — THE SYSTEM SHALL let the operator adjust the origin concurrency, hold admission, or stop the event mid-event, and SHALL document what each control does and what a waiting visitor sees.
- Acceptance: Every control is a dashboard form that works with JavaScript disabled; `docs/RUNBOOK.md` documents each.

**O5** — WHEN a new WAF rule is introduced, THE SYSTEM SHALL observe it in Count mode before promoting it to Block.
- Acceptance: No rule enters Block without one event's worth of Count data.

**O6** — THE SYSTEM SHALL have cost modelled per client before the event.
- Acceptance: Written estimate covering CloudFront, WAF, Bot Control, DynamoDB, and pre-warming.

---

## 5. Out of scope

Not required for the current release; rationale in `docs/adr/README.md`.

- Invite-only waiting rooms with MFA gating.
- Proof-of-Work challenges and CAPTCHA softblock before queue entry.
- Native application SDKs for iOS, Android and React Native.
- Platform connector breadth beyond the CloudFront gate.

## 6. Explicit non-goals

- Physical-location queueing.
- Replacing the client's CDN or WAF.
- Multi-tenant SaaS.
- Gapless position sequences.
- Sub-second join latency.
- Visitor engagement widgets and marketing data collection.
- Email or SMS notification of queue position.

---

## Retired requirements

Never reused or renumbered; kept in step with `docs/REQUIREMENTS.md`.

| ID | Required | Record |
|---|---|---|
| F3.6 | Session signed separately from the admission token | ADR-0032 |
| F3.7 | Sliding session window plus a hard cap | ADR-0032 |
| F3.9 | Unused positions expire after an operator-configured period | ADR-0031 |
| F3.10 | Sessions markable as completed or abandoned | ADR-0032 |
| F6.1 | Queue entry gated on a client-signed entry ticket | ADR-0028 |
| F6.2 | Entry ticket signed by the client, opaque subject | ADR-0028 |
| F6.3 | Bot-blocking enforceable at event start | ADR-0030 |
| N6 | ≤ 80 Terraform resources in `core` | — (superseded by N1) |
