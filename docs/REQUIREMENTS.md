# Requirements

Numbered, testable requirements for the virtual waiting room, each with an acceptance
criterion. `MUST` = mandatory; `SHOULD` = strong default, overridable per client. IDs are stable
and never reused; the EARS form is in
[`.kiro/specs/virtual-waiting-room/requirements.md`](../.kiro/specs/virtual-waiting-room/requirements.md),
and what is built is tracked in its `tasks.md`.

---

## 1. Functional

### 1.1 Event lifecycle

| ID | Requirement | Acceptance |
|---|---|---|
| F0.1 | An event MUST progress through **idle → pre-queue → active → post-event** phases. | Each phase is observable and the transitions are scheduled or manual. |
| F0.2 | Each phase MUST serve an operator-authored page. | Idle shows event information before the pre-queue opens; post-event shows outcome and next steps. |
| F0.3 | The system MUST support **scheduled** events with a known start time. | A configured event opens at its scheduled time. |
| F0.4 | The system MUST support **standby** mode: dormant year-round, activating automatically when inflow crosses an operator-configured threshold. | Below threshold, visitors pass through untouched. Above it, new visitors are queued without operator action. |
| F0.5 | Both modes MUST run simultaneously on one origin. | A scheduled room on `/product/x` with a low admission rate coexists with standby protection across the whole site. |
| F0.6 | The operator MUST be able to declare which requests are protected, by path, header, cookie, or user agent. | An unprotected path is never queued, in any mode. |
| F0.7 | Standby activation MUST be observable and manually overridable. | Operator can force-activate or force-dormant; state is visible in metrics. |
| F0.8 | The system MUST support a **maintenance mode** that parks all visitors on an operator page. | Enabling it holds every visitor regardless of mode or capacity. |

**Fairness by mode.** Scheduled events randomize among pre-queue participants; standby
activation queues first-in, first-out (FIFO).

### 1.2 Pre-queue (scheduled events)

| ID | Requirement | Acceptance |
|---|---|---|
| F1.1 | During the pre-queue phase, visitors MUST be held on a countdown page rather than assigned a queue position. Registering MUST cost one row write per visitor plus a share of one amortised shard-counter claim per batch. | A visitor arriving at T−10min sees a countdown and is registered; no `Positions` item and no queue position exists until the event opens. |
| F1.2 | The countdown page MUST be servable entirely from CDN cache, so repeated views make zero calls to API Gateway, DynamoDB or SQS. Registration MUST be a separate, one-time direct write from the edge to the ingest queue, with no compute in the path. | Origin requests from page views are independent of visitor count; each visitor registers once, deduplicated across reloads wherever the browser allows client-side storage; no Lambda in the write's path. Known residuals: a storage-denied visitor takes a new place on each reload, and two reloads in one batching window can each claim. |
| F1.3 | At T−0 the system MUST assign queue positions to pre-queue participants in **randomized** order, and the ordering MUST NOT be predictable before that moment. | Assigned position shows no correlation with registration time; positions are uniformly distributed; the permutation key does not exist before T−0. |
| F1.4 | Position assignment for pre-queue participants MUST complete promptly at the scheduled start. | 1,000,000 participants assigned in one write; elapsed time independent of cohort size. |
| F1.5 | The randomization MUST be recomputable after the fact. | A third party given the published seed, participant count and registration indices recomputes every position exactly. This checks the arithmetic, not that the seed was drawn fairly ([ADR-0040](adr/0040-what-the-published-seed-proves.md)). |

### 1.3 Queue join (live arrivals)

| ID | Requirement | Acceptance |
|---|---|---|
| F2.1 | The system MUST accept joins after the event opens, assigning positions in arrival order. | A visitor joining at T+5min receives a position after all pre-queue participants. |
| F2.2 | Each visitor MUST receive a unique queue position. No position may be issued twice. | Under concurrent load, the set of issued positions contains zero duplicates. |
| F2.3 | Queue positions MAY contain gaps. | Not a defect. Gap rate is measured and reported, not eliminated. |
| F2.4 | The client MUST supply its own request identifier (UUIDv7) when joining. | A join without a valid UUIDv7 is rejected at the gateway with 400. |
| F2.5 | Repeating a join with the same request ID MUST NOT consume an additional position. | Duplicate submission returns the original position. |
| F2.6 | Malformed joins MUST NOT consume queue positions. | Sending N malformed payloads leaves the counter unchanged. |

### 1.4 Waiting and admission

| ID | Requirement | Acceptance |
|---|---|---|
| F3.1 | A visitor MUST be able to read their own position and the current serving position. | `GET /queue_num` and `GET /status` return correct values. |
| F3.2 | The operator MUST be able to control admission rate during the event. | `POST /_wr/admin/rate` changes the target rate; effect visible within the cache TTL. |
| F3.3 | Admitted visitors MUST receive a cryptographically verifiable credential. | The session cookie is signed; the edge gate verifies it locally, with no backend call. |
| F3.4 | The protected origin MUST be unreachable without a valid session. | A request with no credential, an expired one, or one for another event is refused at the edge. |
| F3.5 | Once a visitor's position is reached, the system MUST establish a **session** so they are not re-checked on every subsequent request. | A visitor navigates to a second page without re-presenting anything and is not re-queued. |
| F3.8 | Admission rate control MUST compensate for **no-shows**, admitted visitors who never arrive at the origin. | With a 30% no-show rate and a target of 500/min, origin arrivals converge on 500/min, not 350. One visitor counts as one arrival however many times they call `/_wr/v1/generate_token`. |

### 1.5 Failure behaviour

| ID | Requirement | Acceptance |
|---|---|---|
| F4.1 | If the waiting room is unavailable, visitors MUST proceed to the origin rather than being blocked. | With the waiting room API returning 5xx, the origin remains reachable. |
| F4.2 | The fail-open bypass MUST be time-limited and the client MUST retry in the background. | Bypass expires; normal queueing resumes without user action. |
| F4.3 | Fail-open MUST be overridable per client. | A client requiring fail-closed can configure it, with the tradeoff documented. |
| F4.4 | A join lost downstream MUST be recoverable by the client. | `GET /queue_num` returns 404; the client re-joins with the same request id, and the retry succeeds because no row exists for it yet. |
| F4.5 | The client MUST treat HTTP 429 as expected and retry with jittered backoff. | Under gateway throttling, no user-visible error; joins succeed on retry. |

### 1.6 Operator experience

| ID | Requirement | Acceptance |
|---|---|---|
| F5.1 | The operator MUST see live event metrics: inflow, outflow, queue depth, admitted, no-show rate. | Metrics visible in CloudWatch within one 60 s metric period. |
| F5.2 | The waiting room page MUST be brandable by the client without forking the module. | Client supplies template assets; no code change required. |
| F5.3 | The operator MUST be able to publish a message to waiting visitors during an event. | Message appears on the waiting page within the cache TTL. |
| F5.4 | Waiting visitors MUST see their position and an estimated wait time. | Both displayed and updated as the queue advances. |
| F5.5 | Every operator action MUST be available through an API, not only a console. | Rate change, reset, pause, message publish, and mode override all scriptable. |

### 1.7 Abuse mitigation

No requirement in this section is currently met: nothing binds a place to a person.
`request_id` is client-supplied, so volume buys share of the front linearly
([ADR-0001](adr/0001-randomize-pre-queue-assignment.md)). Where the edge web ACL is attached,
each place costs a browser challenge solve and a per-IP CAPTCHA past a limit
([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)); a farm of real browsers still takes
places. Entry tickets ([ADR-0028](adr/0028-remove-entry-tickets.md)) and open-time demotion
([ADR-0030](adr/0030-remove-open-time-demotion.md)) were removed; F6.1–F6.3 are retired below.

### 1.8 Operator web interface

A thin server-rendered client over the operator API (F5.1–F5.5); it adds no capability the API
lacks.

| ID | Requirement | Acceptance |
|---|---|---|
| F7.1 | The system MUST serve an operator web interface from one Axum Lambda rendering server-side HTML with askama templates. | Reachable at `/_wr/admin`; one Lambda; no browser SPA bundle. |
| F7.2 | The interface MUST be styled in the Vouch design language as one self-contained stylesheet ([ADR-0018](adr/0018-admin-vouch-design-language.md)). | No runtime npm dependency and no build-time token extraction. |
| F7.3 | The interface MUST expose every operator capability in the admin API and no other. | Each `/_wr/admin/*` action has a UI control; F5.5 still holds. |
| F7.4 | Every interface request MUST pass one authentication path, with no weaker second one. | OIDC Authorization Code + PKCE session enforced in the Lambda ([ADR-0016](adr/0016-admin-oidc-dynamodb-sessions.md)); unauthenticated requests are redirected to login. |
| F7.5 | The interface MUST be server-rendered with no client-side React runtime; interactivity is HTML forms plus minimal progressive-enhancement JavaScript. | Core actions work with JavaScript disabled. |
| F7.6 | The interface MUST display live metrics: inflow, outflow, queue depth, admitted, no-show rate. | Metrics render and refresh within one 60 s metric period. |

---

## 2. Capacity

Requirements on the deployed system, not on AWS defaults. Every figure requires the
pre-event preparation in §4.

| ID | Requirement | Acceptance |
|---|---|---|
| C1 | The pre-queue MUST support at least 1,000,000 concurrent participants. | Load test sustains 1M countdown-page holders. |
| C2 | Position assignment for the pre-queue cohort MUST be atomic: no interval in which some participants hold positions and others do not. | Assignment is a single conditional write; a reader sees either no participant assigned or all of them. |
| C3 | The live-join path MUST sustain ≥ 10,000 joins/sec at default quotas, and ≥ 40,000/sec with quota increases filed. | Load test at both levels; zero duplicates at each. |
| C4 | Polling load MUST be independent of visitor count at the origin. | Origin RPS for `/status` stays flat as waiters scale from 10K to 1M. |
| C5 | The system MUST handle a spike arriving in under 5 seconds without dropping joins. | Joins are durably enqueued even when compute has not yet scaled. |

---

## 3. Non-functional

| ID | Requirement | Acceptance |
|---|---|---|
| N1 | Idle cost MUST approach zero. No always-on compute or cache tier. | Monthly bill for an idle deployment is under $5 excluding pre-warming. |
| N2 | The system MUST deploy into the client's own AWS account. | `terraform apply` from a clean account produces a working deployment. |
| N3 | The system MUST NOT require us to operate shared infrastructure on clients' behalf. | No component runs in a Smoke Turner account. |
| N4 | The system MUST support commercial AWS regions. GovCloud (US) is out of scope until a gate exists for it. | The commercial variant deploys and passes functional tests. GovCloud offers no CloudFront Function, so it has no gate ([ADR-0032](adr/0032-remove-the-origin-authorizer.md)). |
| N5 | Infrastructure MUST be expressed as Terraform. | No manual console steps in the deployment path. |
| N7 | Bot and abuse mitigation MUST be present at the edge. | The gate refuses every request without a valid session at viewer-request, and the join burst reaches SQS with no compute in the path. Every REST API method requires an API key only CloudFront holds. With `flat_rate_plan_tier` set, the edge web ACL challenges every JavaScript API call, rate-limits per token and per IP, and runs managed rules in Count until promoted; it is off by default because on pay-as-you-go it bills against N1 ([ADR-0038](adr/0038-edge-web-acl-on-flat-rate-plan.md)). |
| N8 | The API MUST be documented as an OpenAPI specification. | Spec published; client and admin surfaces generated from it. |
| N9 | Concurrent events in one deployment MUST be isolated from each other. | One event driven to its throughput ceiling does not increase queue-join latency or error rate for another. |
| N10 | Client polling cost MUST scale with distance to the front, not with waiting visitors × a fixed interval. | Poll count is O(log) in the starting wait; the harness client-request total under `--polling backoff` is materially below `--polling hold-position` at identical settings. |

---

## 4. Operational

Contractual deliverables. Without these the capacity requirements in §2 are not met.

| ID | Requirement | Acceptance |
|---|---|---|
| O1 | DynamoDB tables MUST be pre-warmed before each event. | Warm throughput ≥ the event's target write rate, verified before T−0 by the dashboard's Readiness panel ([RUNBOOK.md](RUNBOOK.md)). |
| O2 | Service quota increases MUST be filed with lead time. | API Gateway RPS and DynamoDB per-table WRU confirmed raised before T−0; the Readiness panel shows the limits in force. |
| O3 | A load test at the event's target rate MUST be executed before the event. | Report produced and reviewed with the client. |
| O4 | The operator MUST be able to adjust admission rate, hold, or stop mid-event. | Every control is a dashboard form; [RUNBOOK.md](RUNBOOK.md) says what each does and what a waiting visitor sees. |
| O5 | New WAF rules MUST be observed in Count mode before being promoted to Block. | No rule enters Block without one event's worth of Count data. |
| O6 | Cost MUST be modelled per client before the event. | Written estimate covering CloudFront, WAF, Bot Control, DynamoDB, and pre-warming. |

---

## 5. Out of scope

Not required for the current release. Rationale in [`adr/README.md`](./adr/README.md).

- Invite-only waiting rooms with multi-factor authentication (MFA) gating.
- Proof-of-Work challenges and CAPTCHA softblock before queue entry.
- Native application SDKs for iOS, Android and React Native.
- Platform connector breadth beyond the CloudFront gate.

## 6. Explicit non-goals

- Physical-location queueing (restaurants, clinics, service counters).
- Replacing the client's CDN or WAF. We integrate with them.
- Multi-tenant SaaS. We are not a Cloud Service Provider.
- Gapless position sequences.
- Sub-second join latency. Queue join is latency-insensitive by nature.
- Visitor engagement widgets and marketing data collection.
- Email or SMS notification of queue position: it requires collecting personal data, which
  conflicts with keeping all visitor data in the client's account.

---

## Retired requirements

Never reused or renumbered. The reasoning is in the linked record.

| ID | Required | Retired because | Record |
|---|---|---|---|
| F3.6 | The session is signed separately from the admission token. | There is no admission token. | [ADR-0032](adr/0032-remove-the-origin-authorizer.md) |
| F3.7 | Session lifetime supports a sliding window and a hard cap. | Sliding lived only in the removed origin authorizer; the edge does not re-issue a cookie. | [ADR-0032](adr/0032-remove-the-origin-authorizer.md) |
| F3.9 | Queue positions expire if unused within an operator-configured period. | Controller-driven expiry was unbounded and blind to the pre-queue cohort; DynamoDB TTL reclaims rows and the no-show correction compensates. | [ADR-0031](adr/0031-remove-controller-driven-expiry.md) |
| F3.10 | Sessions are markable as completed or abandoned. | `/update_session` returned 501 and nothing wrote either state. | [ADR-0032](adr/0032-remove-the-origin-authorizer.md) |
| F6.1 | Gate queue entry on a client-issued signed entry ticket, one position per identity. | Unusable without customer-side work the product does not supply. | [ADR-0028](adr/0028-remove-entry-tickets.md) |
| F6.2 | The entry ticket is signed by the client and its subject is opaque. | Retired with F6.1. | [ADR-0028](adr/0028-remove-entry-tickets.md) |
| F6.3 | Bot-blocking decisions enforceable at event start rather than during the pre-queue. | Built as open-time demotion, never enabled, and blind to clients that bypassed CloudFront. | [ADR-0030](adr/0030-remove-open-time-demotion.md) |
| N6 | A deployment small enough to reason about, ≤ 80 Terraform resources in `core`. | The count changed with formatting (`for_each` vs separate blocks) without measuring comprehensibility. N1, idle cost, is the constraint that survives. | — |
