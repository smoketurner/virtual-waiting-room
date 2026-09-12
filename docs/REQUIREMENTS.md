# Requirements

Numbered, testable requirements for the virtual waiting room. Each has an acceptance
criterion. `MUST` = mandatory; `SHOULD` = strong default, overridable per client.

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
| F1.1 | During the pre-queue phase, visitors MUST be held on a countdown page rather than assigned a queue position. Registering a visitor's place MUST cost one row write per visitor, plus a share of one amortised shard-counter claim per batch. | A visitor arriving at T−10min sees a countdown and is registered; no `Positions` item — and no queue position — exists until the event opens. |
| F1.2 | The pre-queue countdown page MUST be servable entirely from content delivery network (CDN) cache, so that repeated page views make zero calls to API Gateway, DynamoDB, or Simple Queue Service (SQS). Registration MUST be a separate, one-time direct write from the edge to the ingest queue, with no compute in the path. | Origin request count from page views during the pre-queue phase is independent of visitor count; each visitor registers exactly once, deduplicated across reloads: the client stores its identifier across a chain of localStorage, a first-party cookie and sessionStorage, and `assign_position` reads which identifiers already hold a row before claiming a pre-queue index, so a browser that denies every storage tier no longer burns an index per reload. Two residuals: with no entry ticket configured a storage-denied visitor still mints a fresh identifier and so takes a *new* place rather than recovering their old one, and two reloads landing in different invocations within the batching window can each claim. With a ticket the identifier is derived and needs no storage at all. No Lambda in the write's path. |
| F1.3 | At T−0 the system MUST assign queue positions to pre-queue participants in **randomized** order, and the ordering MUST NOT be predictable before that moment. | Assigned position shows no correlation with registration time; positions are uniformly distributed; the permutation key does not exist before T−0. |
| F1.4 | Position assignment for pre-queue participants MUST complete promptly at the scheduled start. | 1,000,000 participants assigned in one write; elapsed time independent of cohort size. |
| F1.5 | The randomization MUST be auditable after the fact. | A third party given the published seed, participant count, and registration indices recomputes every position and reproduces the ordering exactly. |

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
| F3.2 | The operator MUST be able to control admission rate during the event. | `POST /admin/rate` changes the target rate; effect visible within the cache time to live (TTL). |
| F3.3 | Admitted visitors MUST receive a cryptographically verifiable token. | Token is signed; signature verifies at the authorizer without a backend call. |
| F3.4 | The origin MUST reject requests without a valid token or session. | A request with no credential, an expired one, or one for another event is denied. |
| F3.5 | After validating an admission token once, the system MUST establish a **session** so the visitor is not re-checked against a single-use token on every subsequent request. | A visitor navigates to a second page without re-presenting the admission token and is not re-queued. |
| F3.6 | The session MUST be separately signed from the admission token, over different inputs. | A captured admission token cannot be replayed as a session credential, or vice versa. |
| F3.7 | Session lifetime MUST support both a sliding window (extended on activity) and a hard cap from issue time. | Both modes configurable per event; hard cap does not extend regardless of activity. |
| F3.8 | Admission rate control MUST compensate for **no-shows** — admitted visitors who never arrive at the origin. | With a 30% no-show rate and a target of 500/min, actual origin arrivals converge on 500/min, not 350. |
| F3.9 | Queue positions MUST expire if unused within an operator-configured period. | Position expires; the serving counter advances past it. |
| F3.10 | Sessions MUST be markable as completed or abandoned. | `POST /update_session` updates the completion and abandonment counters. |

### 1.5 Failure behaviour

| ID | Requirement | Acceptance |
|---|---|---|
| F4.1 | If the waiting room is unavailable, visitors MUST proceed to the origin rather than being blocked. | With the waiting room API returning 5xx, the origin remains reachable. |
| F4.2 | The fail-open bypass MUST be time-limited and the client MUST retry in the background. | Bypass cookie expires; normal queueing resumes without user action. |
| F4.3 | Fail-open MUST be overridable per client. | A client requiring fail-closed can configure it, with the tradeoff documented. |
| F4.4 | A join lost downstream MUST be recoverable by the client. | `GET /queue_num` returns 404; the client re-joins with the same request id, and the retry succeeds because no row exists for it yet. |
| F4.5 | The client MUST treat HTTP 429 as expected and retry with jittered backoff. | Under gateway throttling, no user-visible error; joins succeed on retry. |

### 1.6 Operator experience

| ID | Requirement | Acceptance |
|---|---|---|
| F5.1 | The operator MUST see live event metrics: inflow, outflow, queue depth, admitted, no-show rate, expiry rate. | Metrics visible in CloudWatch within one 60 s metric period. |
| F5.2 | The waiting room page MUST be brandable by the client without forking the module. | Client supplies template assets; no code change required. |
| F5.3 | The operator MUST be able to publish a message to waiting visitors during an event. | Message appears on the waiting page within the cache TTL. |
| F5.4 | Waiting visitors MUST see their position and an estimated wait time. | Both displayed and updated as the queue advances. |
| F5.5 | Every operator action MUST be available through an API, not only a console. | Rate change, reset, pause, message publish, and mode override all scriptable. |

### 1.7 Abuse mitigation

| ID | Requirement | Acceptance |
|---|---|---|
| F6.1 | The system MUST support gating **queue entry** on a client-issued signed entry ticket carrying an opaque per-identity subject, and MUST derive the visitor's `request_id` from that subject so one identity holds one position. | With a ticket configured, N registrations under one identity yield exactly one position, and a registration with a missing, invalid, expired or wrong-audience ticket is discarded without the client learning so at join time. |
| F6.2 | The ticket MUST be signed by the client, not by the waiting room, and the subject MUST be opaque — the waiting room never receives the underlying identifier. | The waiting room holds only a public key and verifies a signature over a subject it cannot reverse. The subject's opaqueness is shape-checked (22–256 base64url characters), which is a customer obligation the wire format cannot enforce. |
| F6.3 | Bot-blocking decisions SHOULD be enforceable at event start rather than during the pre-queue. | **Not built.** Join-time telemetry (viewer address, ASN, country, JA4 fingerprint, user agent) is captured on every registration row, which is the input such a decision would need, but nothing consumes it and no classification or seal-time mitigation exists. Deferred on cost: the mechanism depends on WAF Bot Control, which the deployment does not enable. |

**What F6.1 does and does not bound.** It moves the constraint from "how many identifiers can
you mint?" to "how many identities can you obtain?" — a farm holding N legitimate identities
still receives N positions. Its value is therefore inherited from the customer's identity
system, and it is inapplicable where there is no prior relationship to sign about: a public
onsale open to anyone has no party who can vouch that a visitor is distinct. Such a deployment
runs without a ticket and is a bare raffle, where registration volume converts linearly into
expected share of the front of the queue
([ADR-0001](adr/0001-randomize-pre-queue-assignment.md),
[ADR-0026](adr/0026-entry-tickets.md)). Bounding volume without an identity needs a different
mechanism — proof of work, or behavioural classification over the telemetry F6.3 describes —
and neither is built.


---

## 2. Capacity

Requirements on the deployed system, not on AWS defaults. Every figure requires the
pre-event preparation in §4.

| ID | Requirement | Acceptance |
|---|---|---|
| C1 | The pre-queue MUST support at least 1,000,000 concurrent participants. | Load test sustains 1M countdown-page holders. |
| C2 | Position assignment for the pre-queue cohort MUST be atomic — no interval in which some participants hold positions and others do not. | Assignment completes in a single conditional write; a reader either sees the pre-queue unassigned or sees every participant assigned. |
| C3 | The live-join path MUST sustain ≥ 10,000 joins/sec at default quotas, and ≥ 40,000/sec with quota increases filed. | Load test at both levels; zero duplicates at each. |
| C4 | Polling load MUST be independent of visitor count at the origin. | Origin requests per second (RPS) for `/status` stays flat as waiters scale from 10K to 1M. |
| C5 | The system MUST handle a spike arriving in under 5 seconds without dropping joins. | Joins are durably enqueued even when compute has not yet scaled. |

---

## 3. Non-functional

| ID | Requirement | Acceptance |
|---|---|---|
| N1 | Idle cost MUST approach zero. No always-on compute or cache tier. | Monthly bill for an idle deployment is under $5 excluding pre-warming. |
| N2 | The system MUST deploy into the client's own AWS account. | `terraform apply` from a clean account produces a working deployment. |
| N3 | The system MUST NOT require us to operate shared infrastructure on clients' behalf. | No component runs in a Smoke Turner account. |
| N4 | The system MUST support commercial AWS regions and AWS GovCloud (US). | Both variants deploy and pass functional tests. |
| N5 | Infrastructure MUST be expressed as Terraform. | No manual console steps in the deployment path. |
| N6 | A full deployment SHOULD be small enough to read and reason about in one sitting. | Target ≤ 80 Terraform-managed resources for the core module. |
| N7 | Bot and abuse mitigation MUST be present at the edge. | A Web Application Firewall (WAF) with Bot Control and Autonomous System Number (ASN) matching is deployed by default. |
| N8 | The API MUST be documented as an OpenAPI specification. | Spec published; client and admin surfaces generated from it. |
| N9 | Concurrent events in one deployment MUST be isolated from each other. | One event driven to its throughput ceiling does not increase queue-join latency or error rate for another event in the same deployment. |
| N10 | Client polling cost MUST scale with distance to the front, not with waiting visitors × a fixed interval. | Poll count is O(log) in the starting wait, and the harness client-request total under `--polling backoff` is materially below `--polling hold-position` at identical settings. |

---

## 4. Operational

Contractual deliverables. Without these the capacity requirements in §2 are not met.

| ID | Requirement | Acceptance |
|---|---|---|
| O1 | DynamoDB tables MUST be pre-warmed before each event. | Warm throughput ≥ the event's target write rate, verified before T−0. |
| O2 | Service quota increases MUST be filed with lead time. | API Gateway RPS and DynamoDB per-table write request units (WRU) confirmed raised before T−0. |
| O3 | A load test at the event's target rate MUST be executed before the event. | Report produced and reviewed with the client. |
| O4 | The operator MUST be able to adjust admission rate, reset, or pause mid-event. | Documented runbook procedures, exercised in rehearsal. |
| O5 | New WAF rules MUST be observed in Count mode before being promoted to Block. | No rule enters Block without one event's worth of Count data. |
| O6 | Cost MUST be modelled per client before the event. | Written estimate covering CloudFront, WAF, Bot Control, DynamoDB, and pre-warming. |

---

## 5. Out of scope

Not required for the current release. Rationale in [`adr/README.md`](./adr/README.md).

- Invite-only waiting rooms with multi-factor authentication (MFA) gating. F6.1 is the
  primitive it would build on.
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
- Email or Short Message Service (SMS) notification of queue position. Requires collecting personal data, which
  conflicts with the posture that no visitor data leaves the client's account.
