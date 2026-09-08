# Requirements

Numbered, testable requirements for the virtual waiting room. Each has an acceptance
criterion. `MUST` = mandatory; `SHOULD` = strong default, overridable per client.

---

## 1. Functional

### 1.1 Event lifecycle

Queue-it runs every waiting room through four phases. We adopt the same model: it is where
operators communicate with visitors, and the phases that look like "nothing is happening"
are the ones that carry the most operational value.

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
activation queues first-in-first-out. This mirrors Queue-it and is deliberate: randomization
neutralizes arrival-speed advantage when everyone knows the start time, while FIFO is the
fair model when a spike is unplanned and nobody was waiting.

### 1.2 Pre-queue (scheduled events)

| ID | Requirement | Acceptance |
|---|---|---|
| F1.1 | The system MUST support events with a scheduled start time, holding visitors who arrive before it on a countdown page. | Visitors arriving at T−10min see a countdown, not a queue position. |
| F1.2 | The pre-queue page MUST be servable entirely from CDN cache, making zero calls to API Gateway, DynamoDB, or SQS per view. | Origin request count during the pre-queue phase is independent of visitor count. |
| F1.3 | At T−0 the system MUST assign queue positions to pre-queue participants in **randomized** order. | Across repeated trials, arrival timestamp shows no correlation with assigned position. |
| F1.4 | Position assignment for pre-queue participants MUST complete within an operator-configured window. | 1,000,000 positions assigned within 5 minutes. |
| F1.5 | The randomization MUST be auditable after the fact. | A stored seed plus the participant set reproduces the exact assignment. |

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
| F3.1 | A visitor MUST be able to read their own position and the current serving position. | `GET /queue_num`, `GET /serving_num` return correct values. |
| F3.2 | The operator MUST be able to control admission rate during the event. | `POST /increment_serving_counter` admits N more visitors; effect visible within cache TTL. |
| F3.3 | Admitted visitors MUST receive a cryptographically verifiable token. | Token is signed; signature verifies at the authorizer without a backend call. |
| F3.4 | The origin MUST reject requests without a valid token or session. | A request with no credential, an expired one, or one for another event is denied. |
| F3.5 | After validating an admission token once, the system MUST establish a **session** so the visitor is not re-checked against a single-use token on every subsequent request. | A visitor navigates to a second page without re-presenting the admission token and is not re-queued. |
| F3.6 | The session MUST be separately signed from the admission token, over different inputs. | A captured admission token cannot be replayed as a session credential, or vice versa. |
| F3.7 | Session lifetime MUST support both a sliding window (extended on activity) and a hard cap from issue time. | Both modes configurable per event; hard cap does not extend regardless of activity. |
| F3.8 | Admission rate control MUST compensate for **no-shows** — admitted visitors who never arrive at the origin. | With a 30% no-show rate and a target of 500/min, actual origin arrivals converge on 500/min, not 350. |
| F3.9 | Queue positions MUST expire if unused within an operator-configured period. | Position expires; the serving counter advances past it. |
| F3.10 | Sessions MUST be markable as completed or abandoned. | Counters update; the figures feed F3.8. |

### 1.5 Failure behaviour

| ID | Requirement | Acceptance |
|---|---|---|
| F4.1 | If the waiting room is unavailable, visitors MUST proceed to the origin rather than being blocked. | With the waiting room API returning 5xx, the origin remains reachable. |
| F4.2 | The fail-open bypass MUST be time-limited and the client MUST retry in the background. | Bypass cookie expires; normal queueing resumes without user action. |
| F4.3 | Fail-open MUST be overridable per client. | A client requiring fail-closed can configure it, with the tradeoff documented. |
| F4.4 | A join lost downstream MUST be recoverable by the client. | `GET /queue_num` returns 404; the client re-joins with a fresh UUIDv7. |
| F4.5 | The client MUST treat HTTP 429 as expected and retry with jittered backoff. | Under gateway throttling, no user-visible error; joins succeed on retry. |

### 1.6 Operator experience

A waiting room that cannot be observed and adjusted mid-event is not usable in production.
Queue-it sells traffic intelligence and branded themes as products; both are table stakes
rather than extras.

| ID | Requirement | Acceptance |
|---|---|---|
| F5.1 | The operator MUST see live event metrics: inflow, outflow, queue depth, admitted, no-show rate, expiry rate. | Metrics visible within one polling interval of reality. |
| F5.2 | The waiting room page MUST be brandable by the client without forking the module. | Client supplies template assets; no code change required. |
| F5.3 | The operator MUST be able to publish a message to waiting visitors during an event. | Message appears on the waiting page within the cache TTL. |
| F5.4 | Waiting visitors MUST see their position and an estimated wait time. | Both displayed and updated as the queue advances. |
| F5.5 | Every operator action MUST be available through an API, not only a console. | Rate change, reset, pause, message publish, and mode override all scriptable. |

### 1.7 Abuse mitigation

| ID | Requirement | Acceptance |
|---|---|---|
| F6.1 | The system MUST support gating **queue entry** on a client-issued signed identifier — a membership ID, promo code, or order reference. | A visitor without a valid identifier cannot join the queue. |
| F6.2 | The identifier MUST be signed by the client, not by the waiting room. | The waiting room verifies a signature over data it never stores. |
| F6.3 | Bot-blocking decisions SHOULD be enforceable at event start rather than during the pre-queue. | An operator can choose to admit suspected bots to the pre-queue and block them at randomization. |

**Why F6.3.** Queue-it's Hype Event Protection blocks bots at sale start, after genuine
visitors have secured positions, specifically so operators do not reveal detection early and
give bots time to retool and rejoin. This is an operational posture, not a feature — it
costs nothing to support and materially changes outcomes.


---

## 2. Capacity

Stated as requirements on the deployed system, not on AWS defaults. Every figure below is
achievable only with the pre-event preparation in §4.

| ID | Requirement | Acceptance |
|---|---|---|
| C1 | The pre-queue MUST support at least 1,000,000 concurrent participants. | Load test sustains 1M countdown-page holders. |
| C2 | Batch position assignment MUST sustain ≥ 4,000 writes/sec. | 1M positions in ≤ 5 min, zero throttling. |
| C3 | The live-join path MUST sustain ≥ 10,000 joins/sec at default quotas, and ≥ 40,000/sec with quota increases filed. | Load test at both levels; zero duplicates at each. |
| C4 | Polling load MUST be independent of visitor count at the origin. | Origin RPS for `/serving_num` stays flat as waiters scale from 10K to 1M. |
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
| N6 | The deployed resource count SHOULD be materially lower than the deprecated AWS solution's 151. | Target ≤ 80 resources for the core module. |
| N7 | Bot and abuse mitigation MUST be present at the edge. | WAF with Bot Control and ASN matching is deployed by default. |
| N8 | The API contract SHOULD remain compatible with the deprecated AWS solution. | Existing integrations work against the documented endpoints. |

---

## 4. Operational

These are contractual deliverables, not implementation details. They are the difference
between a working on-sale and a throttled one.

| ID | Requirement | Acceptance |
|---|---|---|
| O1 | DynamoDB tables MUST be pre-warmed before each event. | Warm throughput ≥ the event's target write rate, verified before T−0. |
| O2 | Service quota increases MUST be filed with lead time. | API Gateway RPS and DynamoDB per-table WRU confirmed raised before T−0. |
| O3 | A load test at the event's target rate MUST be executed before the event. | Report produced and reviewed with the client. |
| O4 | The operator MUST be able to adjust admission rate, reset, or pause mid-event. | Documented runbook procedures, exercised in rehearsal. |
| O5 | New WAF rules MUST be observed in Count mode before being promoted to Block. | No rule enters Block without one event's worth of Count data. |
| O6 | Cost MUST be modelled per client before the event. | Written estimate covering CloudFront, WAF, Bot Control, DynamoDB, and pre-warming. |

---

## 5. Deliberately deferred

Queue-it ships these; we do not, yet. Listed so the gap is a decision rather than an
oversight.

| Capability | Why deferred |
|---|---|
| Invite-only waiting rooms (identifier + MFA gating) | Real revenue feature for loyalty and members-only sales. F6.1 is the primitive it builds on; the full flow is post-v1. |
| Proof-of-Work challenges | Raises bot compute cost. Needs client-side work; WAF challenge actions cover much of it initially. |
| CAPTCHA softblock before queue entry | WAF's CAPTCHA action covers the common case. |
| Native app SDKs (iOS, Android, React Native) | A genuine gap for ticketing clients, who see heavy app traffic. Post-v1. |
| Connector breadth — 25+ platform integrations | **This is Queue-it's actual moat.** We ship a CloudFront/origin authorizer, which covers CDN-fronted origins. Matching their breadth is a multi-year product commitment, not a v1 goal. |

## 6. Explicit non-goals

- Physical-location queueing (restaurants, clinics, service counters).
- Replacing the client's CDN or WAF. We integrate with them.
- Multi-tenant SaaS. We are not a Cloud Service Provider; see DESIGN §12.
- Gapless position sequences.
- Sub-second join latency. Queue join is latency-insensitive by nature.
- Visitor engagement widgets and marketing data collection. Not infrastructure.
- Email or SMS notification of queue position. Requires collecting personal data, which
  conflicts with the posture that no visitor data leaves the client's account.
