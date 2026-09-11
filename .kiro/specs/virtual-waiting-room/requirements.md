# Requirements: Virtual Waiting Room

EARS-formatted requirements for the virtual waiting room. Source of record: `docs/REQUIREMENTS.md`.
Modal mapping: MUST = SHALL, SHOULD = SHOULD. Every original ID (F0.x, F1.x–F6.x, C1–C5,
N1–N9, O1–O6) is preserved verbatim, with each original acceptance criterion carried through
as a testable `Acceptance:` line. Section groupings, the "Fairness by mode" note, and the
Out-of-scope + Non-goals sections are preserved.

## Introduction

The virtual waiting room meters visitors into an AWS origin at a rate the origin can survive,
holding overflow on operator-authored pages and admitting visitors under operator control. It
runs in two modes at once — scheduled events with a known start time, and standby protection
that stays dormant until inflow crosses a threshold. Every deployment is single-tenant, running
entirely inside the client's own AWS account with no shared infrastructure.

---

## 1. Functional

### 1.1 Event lifecycle

**F0.1** — As an operator, I want events to move through a defined lifecycle so that behaviour is predictable at every stage.
- THE SYSTEM SHALL progress an event through **idle → pre-queue → active → post-event** phases.
- Acceptance: Each phase is observable and the transitions are scheduled or manual.

**F0.2** — As a visitor, I want a relevant page at each phase so that I always know the event's status.
- THE SYSTEM SHALL serve an operator-authored page for each phase.
- Acceptance: Idle shows event information before the pre-queue opens; post-event shows outcome and next steps.

**F0.3** — As an operator, I want scheduled events so that a room opens at a known start time.
- WHERE an event is configured as **scheduled** with a known start time, THE SYSTEM SHALL open it at its scheduled time.
- Acceptance: A configured event opens at its scheduled time.

**F0.4** — As an operator, I want standby protection so that surges are absorbed without my intervention.
- WHERE an event is configured as **standby** (dormant year-round), THE SYSTEM SHALL activate automatically WHEN inflow crosses an operator-configured threshold.
- Acceptance: Below threshold, visitors pass through untouched. Above it, new visitors are queued without operator action.

**F0.5** — As an operator, I want both modes on one origin so that scoped rooms and site-wide protection coexist.
- THE SYSTEM SHALL run scheduled and standby modes simultaneously on one origin.
- Acceptance: A scheduled room on `/product/x` with a low admission rate coexists with standby protection across the whole site.

**F0.6** — As an operator, I want to declare what is protected so that unprotected traffic is never queued.
- THE SYSTEM SHALL allow the operator to declare which requests are protected, by path, header, cookie, or user agent.
- Acceptance: An unprotected path is never queued, in any mode.

**F0.7** — As an operator, I want visible, overridable standby state so that I retain control.
- THE SYSTEM SHALL make standby activation observable and manually overridable.
- Acceptance: Operator can force-activate or force-dormant; state is visible in metrics.

**F0.8** — As an operator, I want a maintenance mode so that I can hold every visitor regardless of mode or capacity.
- WHEN maintenance mode is enabled, THE SYSTEM SHALL park all visitors on an operator page.
- Acceptance: Enabling it holds every visitor regardless of mode or capacity.

**Fairness by mode.** Scheduled events randomize among pre-queue participants; standby
activation queues first-in, first-out (FIFO).

### 1.2 Pre-queue (scheduled events)

**F1.1** — As a visitor arriving early, I want a countdown rather than a queued position so that registering costs nothing more than a bounded write.
- WHILE an event is in the pre-queue phase, THE SYSTEM SHALL hold visitors on a countdown page rather than assign a queue position, and SHALL register each visitor's place with one row write, plus a share of one amortised shard-counter claim per batch.
- Acceptance: A visitor arriving at T−10min sees a countdown and is registered; no `Positions` item — and no queue position — exists until the event opens.

**F1.2** — As an operator, I want page views served from cache so that origin load from browsing is independent of visitor count, with registration kept to one bounded write per visitor.
- WHILE an event is in the pre-queue phase, THE SYSTEM SHALL serve the countdown page entirely from content delivery network (CDN) cache, so page views make zero calls to API Gateway, DynamoDB, or Simple Queue Service (SQS); registering a visitor's place SHALL be a separate, single direct write from the edge to the ingest queue, with no compute in the path.
- Acceptance: Origin request count from page views is independent of visitor count; each visitor registers exactly once, deduplicated across reloads wherever the browser permits persistent client-side storage (a browser that denies it, e.g. private browsing, re-registers on every reload — a known gap, not a guarantee this requirement makes), with no Lambda in that write's path.

**F1.3** — As an operator, I want fair, unpredictable ordering so that early registration confers no advantage.
- WHEN the scheduled start (T−0) is reached, THE SYSTEM SHALL assign queue positions to pre-queue participants in **randomized** order, and THE SYSTEM SHALL NOT make the ordering predictable before that moment.
- Acceptance: Assigned position shows no correlation with registration time; positions are uniformly distributed; the permutation key does not exist before T−0.

**F1.4** — As an operator, I want prompt assignment so that start-time scale does not delay opening.
- WHEN the scheduled start is reached, THE SYSTEM SHALL complete position assignment for pre-queue participants promptly.
- Acceptance: 1,000,000 participants assigned in one write; elapsed time independent of cohort size.

**F1.5** — As an auditor, I want reproducible ordering so that fairness can be verified after the fact.
- THE SYSTEM SHALL make the randomization auditable after the fact.
- Acceptance: A third party given the published seed, participant count, and registration indices recomputes every position and reproduces the ordering exactly.

### 1.3 Queue join (live arrivals)

**F2.1** — As a live-arriving visitor, I want a position after the event opens so that late arrivals are ordered after the pre-queue.
- WHEN a visitor joins after the event opens, THE SYSTEM SHALL assign a position in arrival order.
- Acceptance: A visitor joining at T+5min receives a position after all pre-queue participants.

**F2.2** — As a visitor, I want a unique position so that no two visitors collide.
- THE SYSTEM SHALL issue each visitor a unique queue position and SHALL NOT issue any position twice.
- Acceptance: Under concurrent load, the set of issued positions contains zero duplicates.

**F2.3** — As an operator, I want gaps treated as acceptable so that they are measured, not fought.
- THE SYSTEM SHALL allow queue positions to contain gaps.
- Acceptance: Not a defect. Gap rate is measured and reported, not eliminated.

**F2.4** — As the system, I want a client-supplied identifier so that joins are idempotent and traceable.
- WHEN a client joins, THE SYSTEM SHALL require the client to supply its own request identifier (UUIDv7).
- Acceptance: A join without a valid UUIDv7 is rejected at the gateway with 400.

**F2.5** — As a client retrying, I want idempotent joins so that a repeat does not consume a position.
- WHEN a join is repeated with the same request ID, THE SYSTEM SHALL NOT consume an additional position.
- Acceptance: Duplicate submission returns the original position.

**F2.6** — As the system, I want malformed joins rejected so that the counter is not polluted.
- WHEN a join is malformed, THE SYSTEM SHALL NOT consume a queue position.
- Acceptance: Sending N malformed payloads leaves the counter unchanged.

### 1.4 Waiting and admission

**F3.1** — As a waiting visitor, I want to read my position and the serving position so that I know my progress.
- THE SYSTEM SHALL allow a visitor to read their own position and the current serving position.
- Acceptance: `GET /queue_num` and `GET /status` return correct values.

**F3.2** — As an operator, I want to control admission rate so that I can throttle the origin during the event.
- WHEN the operator changes the admission rate during an event, THE SYSTEM SHALL apply the new target rate.
- Acceptance: `POST /admin/rate` changes the target rate; effect visible within the cache time to live (TTL).

**F3.3** — As an admitted visitor, I want a verifiable token so that admission needs no backend call.
- WHEN a visitor is admitted, THE SYSTEM SHALL issue a cryptographically verifiable token.
- Acceptance: Token is signed; signature verifies at the authorizer without a backend call.

**F3.4** — As the origin, I want unauthenticated requests denied so that only admitted visitors get through.
- IF a request has no valid token or session, THEN THE SYSTEM SHALL reject it at the origin.
- Acceptance: A request with no credential, an expired one, or one for another event is denied.

**F3.5** — As an admitted visitor, I want a session so that I am not re-checked on every request.
- WHEN an admission token is validated for the first time, THE SYSTEM SHALL establish a **session** so the visitor is not re-checked against a single-use token on every subsequent request.
- Acceptance: A visitor navigates to a second page without re-presenting the admission token and is not re-queued.

**F3.6** — As the system, I want distinct signing so that a token and a session cannot be swapped.
- THE SYSTEM SHALL sign the session separately from the admission token, over different inputs.
- Acceptance: A captured admission token cannot be replayed as a session credential, or vice versa.

**F3.7** — As an operator, I want configurable session lifetime so that both sliding and hard-cap policies are supported.
- THE SYSTEM SHALL support both a sliding-window session lifetime (extended on activity) and a hard cap from issue time.
- Acceptance: Both modes configurable per event; hard cap does not extend regardless of activity.

**F3.8** — As an operator, I want no-show compensation so that actual origin arrivals hit the target.
- THE SYSTEM SHALL compensate admission rate control for **no-shows** — admitted visitors who never arrive at the origin.
- Acceptance: With a 30% no-show rate and a target of 500/min, actual origin arrivals converge on 500/min, not 350.

**F3.9** — As an operator, I want unused positions to expire so that the queue does not stall on absentees.
- IF a queue position is unused within an operator-configured period, THEN THE SYSTEM SHALL expire it.
- Acceptance: Position expires; the serving counter advances past it.

**F3.10** — As an operator, I want session outcomes recorded so that completion and abandonment are measurable.
- THE SYSTEM SHALL allow sessions to be marked as completed or abandoned.
- Acceptance: `POST /update_session` updates the completion and abandonment counters.

### 1.5 Failure behaviour

**F4.1** — As a visitor, I want to reach the origin if the waiting room is down so that an outage does not block me.
- IF the waiting room is unavailable, THEN THE SYSTEM SHALL allow visitors to proceed to the origin rather than being blocked.
- Acceptance: With the waiting room API returning 5xx, the origin remains reachable.

**F4.2** — As the system, I want fail-open bounded so that normal queueing resumes automatically.
- WHERE fail-open is in effect, THE SYSTEM SHALL time-limit the bypass and the client SHALL retry in the background.
- Acceptance: Bypass cookie expires; normal queueing resumes without user action.

**F4.3** — As a client with strict needs, I want fail-open overridable so that I can choose fail-closed.
- WHERE a client requires fail-closed, THE SYSTEM SHALL allow fail-open to be overridden per client.
- Acceptance: A client requiring fail-closed can configure it, with the tradeoff documented.

**F4.4** — As a client, I want to recover a lost join so that a downstream drop is not user-visible.
- IF a join is lost downstream, THEN THE SYSTEM SHALL allow the client to recover it.
- Acceptance: `GET /queue_num` returns 404; the client re-joins with the same request id, and the retry succeeds because no row exists for it yet.

**F4.5** — As a client, I want throttling handled gracefully so that joins succeed on retry.
- WHEN the client receives HTTP 429, THE SYSTEM (client) SHALL treat it as expected and retry with jittered backoff.
- Acceptance: Under gateway throttling, no user-visible error; joins succeed on retry.

### 1.6 Operator experience

**F5.1** — As an operator, I want live metrics so that I can see event health in near real time.
- THE SYSTEM SHALL show the operator live event metrics: inflow, outflow, queue depth, admitted, no-show rate, expiry rate.
- Acceptance: Metrics visible in CloudWatch within one 60 s metric period.

**F5.2** — As a client, I want to brand the waiting page so that I need not fork the module.
- THE SYSTEM SHALL allow the waiting room page to be branded by the client without forking the module.
- Acceptance: Client supplies template assets; no code change required.

**F5.3** — As an operator, I want to publish a message so that I can inform waiting visitors mid-event.
- WHEN the operator publishes a message during an event, THE SYSTEM SHALL display it to waiting visitors.
- Acceptance: Message appears on the waiting page within the cache TTL.

**F5.4** — As a waiting visitor, I want my position and estimated wait so that I know how long I have.
- THE SYSTEM SHALL show waiting visitors their position and an estimated wait time.
- Acceptance: Both displayed and updated as the queue advances.

**F5.5** — As an operator, I want every action scriptable so that operations are API-first, not console-only.
- THE SYSTEM SHALL make every operator action available through an API, not only a console.
- Acceptance: Rate change, reset, pause, message publish, and mode override all scriptable.

### 1.7 Abuse mitigation

**F6.1** — As an operator, I want to gate entry on a signed identifier so that only holders can join.
- THE SYSTEM SHALL support gating **queue entry** on a client-issued signed identifier — a membership ID, promo code, or order reference.
- Acceptance: A visitor without a valid identifier cannot join the queue.

**F6.2** — As a client, I want to sign identifiers myself so that the waiting room stores no such data.
- THE SYSTEM SHALL require the identifier to be signed by the client, not by the waiting room.
- Acceptance: The waiting room verifies a signature over data it never stores.

**F6.3** — As an operator, I want bot-blocking deferrable to start so that suspected bots can be dropped at randomization.
- WHERE the operator elects to defer bot-blocking, THE SYSTEM SHOULD enforce bot-blocking decisions at event start rather than during the pre-queue.
- Acceptance: An operator can choose to admit suspected bots to the pre-queue and block them at randomization.

### 1.8 Operator web interface

New requirements. These build on the operator API surface (F5.1–F5.5); the web interface is a
thin server-rendered client over that API and adds no capability the API lacks.

**F7.1** — As an operator, I want an admin web interface so that I can drive the system without scripting every action.
- THE SYSTEM SHALL serve an operator admin web interface from a single Axum-based Lambda rendering server-side HTML via askama templates. (Relates to F5.5.)
- Acceptance: Reachable at `/admin`, one Lambda, no browser SPA bundle.

**F7.2** — As an operator, I want AWS-native styling so that the interface feels like a native AWS service.
- THE SYSTEM SHALL style the admin interface with the Cloudscape design language so it presents as an AWS-native service. (Relates to F5.1–F5.5.)
- Acceptance: Uses Cloudscape design-token values (color/spacing/typography) extracted at build time; matches AWS console conventions (top nav, side nav, containers, tables).

**F7.3** — As an operator, I want full parity with the admin API so that the UI exposes every operator capability.
- THE admin interface SHALL expose every operator capability already in the admin API (phase incl. maintenance, rate, message, reset, rules, metrics view, session update); it is a thin server-rendered client adding no capability the API lacks. (Relates to F5.5 API-first; F5.2, F5.3.)
- Acceptance: Each `/admin/*` action has a UI control; F5.5 API-first still holds.

**F7.4** — As a security owner, I want the UI to use the same auth as the API so that there is no weaker path.
- WHEN an operator submits an admin action via the web interface, THE SYSTEM SHALL authenticate with AWS SigV4 (same as the admin API). (Relates to F5.5.)
- Acceptance: Unauthenticated admin UI requests rejected; no second weaker auth path.

**F7.5** — As an operator, I want a server-rendered UI so that core actions do not depend on a client-side runtime.
- THE admin interface SHALL be server-rendered with no client-side React runtime; interactivity uses HTML forms + minimal progressive-enhancement JS only. (Relates to F7.1, F7.3.)
- Acceptance: Core actions work with JS disabled; no React/SPA bundle shipped.

**F7.6** — As an operator, I want live metrics in the UI so that I can watch event health from the admin interface.
- THE admin interface SHALL display live metrics (inflow, outflow, queue depth, admitted, no-show rate, expiry rate) from `/metrics` + CloudWatch EMF. (Relates to F5.1.)
- Acceptance: Metrics render and refresh within one 60 s metric period.

---

## 2. Capacity

Requirements on the deployed system, not on AWS defaults. Every figure requires the
pre-event preparation in the Operational section.

**C1** — As an operator, I want the pre-queue to hold a large cohort so that mass-registration events are supported.
- THE SYSTEM SHALL support at least 1,000,000 concurrent pre-queue participants.
- Acceptance: Load test sustains 1M countdown-page holders.

**C2** — As an operator, I want atomic assignment so that no reader ever sees a partially assigned cohort.
- WHEN the pre-queue cohort is assigned positions, THE SYSTEM SHALL make the assignment atomic — no interval in which some participants hold positions and others do not.
- Acceptance: Assignment completes in a single conditional write; a reader either sees the pre-queue unassigned or sees every participant assigned.

**C3** — As an operator, I want a high live-join throughput so that surges are absorbed at scale.
- THE SYSTEM SHALL sustain ≥ 10,000 joins/sec on the live-join path at default quotas, and ≥ 40,000/sec with quota increases filed.
- Acceptance: Load test at both levels; zero duplicates at each.

**C4** — As an operator, I want origin polling load flat so that scale does not overwhelm the origin.
- THE SYSTEM SHALL keep polling load independent of visitor count at the origin.
- Acceptance: Origin requests per second (RPS) for `/status` stays flat as waiters scale from 10K to 1M.

**C5** — As an operator, I want sudden spikes absorbed so that fast surges do not drop joins.
- WHEN a spike arrives in under 5 seconds, THE SYSTEM SHALL handle it without dropping joins.
- Acceptance: Joins are durably enqueued even when compute has not yet scaled.

---

## 3. Non-functional

**N1** — As a client, I want near-zero idle cost so that dormant deployments are cheap.
- THE SYSTEM SHALL approach zero idle cost, with no always-on compute or cache tier.
- Acceptance: Monthly bill for an idle deployment is under $5 excluding pre-warming.

**N2** — As a client, I want deployment into my own account so that I retain full ownership.
- THE SYSTEM SHALL deploy into the client's own AWS account.
- Acceptance: `terraform apply` from a clean account produces a working deployment.

**N3** — As a vendor, I want no shared infrastructure so that clients operate independently.
- THE SYSTEM SHALL NOT require the vendor to operate shared infrastructure on clients' behalf.
- Acceptance: No component runs in a Smoke Turner account.

**N4** — As a client, I want commercial and GovCloud support so that both partitions are covered.
- THE SYSTEM SHALL support commercial AWS regions and AWS GovCloud (US).
- Acceptance: Both variants deploy and pass functional tests.

**N5** — As an operator, I want everything in Terraform so that deployment has no manual steps.
- THE SYSTEM SHALL express infrastructure as Terraform.
- Acceptance: No manual console steps in the deployment path.

**N6** — As a maintainer, I want a small deployment so that it can be reasoned about in one sitting.
- THE SYSTEM SHOULD keep a full deployment small enough to read and reason about in one sitting.
- Acceptance: Target ≤ 80 Terraform-managed resources for the core module.

**N7** — As an operator, I want edge abuse mitigation so that bots are handled before the origin.
- THE SYSTEM SHALL provide bot and abuse mitigation at the edge.
- Acceptance: A Web Application Firewall (WAF) with Bot Control and Autonomous System Number (ASN) matching is deployed by default.

**N8** — As an integrator, I want an OpenAPI spec so that client and admin surfaces are generated.
- THE SYSTEM SHALL document the API as an OpenAPI specification.
- Acceptance: Spec published; client and admin surfaces generated from it.

**N9** — As an operator, I want event isolation so that one busy event does not degrade another.
- THE SYSTEM SHALL isolate concurrent events in one deployment from each other.
- Acceptance: One event driven to its throughput ceiling does not increase queue-join latency or error rate for another event in the same deployment.

**N10** — As an operator, I want client polling cost to scale with distance to the front so that a large waiting cohort does not multiply request volume by a fixed interval.
- THE SYSTEM SHALL make client polling cost scale with distance to the front, not with waiting visitors × a fixed interval.
- Acceptance: Poll count is O(log) in the starting wait, and the harness client-request total under `--polling backoff` is materially below `--polling hold-position` at identical settings.

---

## 4. Operational

Contractual deliverables. Without these the capacity requirements are not met.

**O1** — As an operator, I want tables pre-warmed so that write throughput is ready at start.
- THE SYSTEM SHALL have DynamoDB tables pre-warmed before each event.
- Acceptance: Warm throughput ≥ the event's target write rate, verified before T−0.

**O2** — As an operator, I want quota increases filed early so that limits are not hit at start.
- THE SYSTEM SHALL have service quota increases filed with lead time.
- Acceptance: API Gateway RPS and DynamoDB per-table write request units (WRU) confirmed raised before T−0.

**O3** — As an operator, I want a load test before the event so that capacity is proven.
- THE SYSTEM SHALL have a load test at the event's target rate executed before the event.
- Acceptance: Report produced and reviewed with the client.

**O4** — As an operator, I want mid-event controls so that I can adjust, reset, or pause during the event.
- THE SYSTEM SHALL allow the operator to adjust admission rate, reset, or pause mid-event.
- Acceptance: Documented runbook procedures, exercised in rehearsal.

**O5** — As an operator, I want WAF rules observed before blocking so that legitimate traffic is not dropped.
- WHEN a new WAF rule is introduced, THE SYSTEM SHALL observe it in Count mode before promoting it to Block.
- Acceptance: No rule enters Block without one event's worth of Count data.

**O6** — As a client, I want a per-event cost model so that spend is understood beforehand.
- THE SYSTEM SHALL have cost modelled per client before the event.
- Acceptance: Written estimate covering CloudFront, WAF, Bot Control, DynamoDB, and pre-warming.

---

## 5. Out of scope

Not required for the current release. Rationale in `adr/README.md`.

- Invite-only waiting rooms with multi-factor authentication (MFA) gating. F6.1 is the primitive it would build on.
- Proof-of-Work challenges and CAPTCHA softblock before queue entry.
- Native application software development kits (SDKs) for iOS, Android and React Native.
- Platform connector breadth beyond the CloudFront/origin authorizer.

## 6. Explicit non-goals

- Physical-location queueing (restaurants, clinics, service counters).
- Replacing the client's CDN or WAF. We integrate with them.
- Multi-tenant software as a service (SaaS). We are not a Cloud Service Provider.
- Gapless position sequences.
- Sub-second join latency. Queue join is latency-insensitive by nature.
- Visitor engagement widgets and marketing data collection. Not infrastructure.
- Email or Short Message Service (SMS) notification of queue position. Requires collecting personal data, which conflicts with the posture that no visitor data leaves the client's account.

---

## Traceability

- Design: `design.md` (same spec folder) elaborates how these requirements are realized.
- Tasks: `tasks.md` (same spec folder) breaks the design into implementation work items.
- Narrative source: the authoritative prose requirements remain in `docs/REQUIREMENTS.md`; this document re-expresses them in EARS form for the Kiro SDD workflow.
