# Architecture Decision Records

One record per decision. Each states the context, the decision, and its consequences.
`DESIGN.md` describes the system as built; these explain why it is built that way.

| ADR | Decision |
|---|---|
| [0001](0001-randomize-pre-queue-assignment.md) | Randomize position assignment for scheduled events |
| [0002](0002-seeded-permutation-not-materialised-shuffle.md) | Derive queue position from a seeded permutation |
| [0003](0003-dynamodb-counters-not-elasticache.md) | Use DynamoDB atomic counters, not a cache tier |
| [0004](0004-putitem-not-batchwriteitem.md) | Write positions with `PutItem`, not `BatchWriteItem` |
| [0005](0005-rest-api-not-http-api.md) | Use a REST API, not an HTTP API |
| [0006](0006-controller-driven-expiry-not-ttl.md) | Drive position expiry from the controller, not DynamoDB TTL (superseded by 0031) |
| [0007](0007-single-tenant-deployment.md) | Deploy single-tenant into the client's account |
| [0008](0008-partition-isolation-not-shuffle-sharding.md) | Isolate concurrent events by partition, not shuffle sharding |
| [0009](0009-fail-open.md) | Fail open when the waiting room is unavailable |
| [0010](0010-client-supplied-request-id.md) | Client supplies the request identifier |
| [0011](0011-session-cookie-after-token.md) | Establish a session after validating the admission token (amended by 0032) |
| [0012](0012-anti-ddos-count-mode.md) | Ship the anti-DDoS rule group in Count mode |
| [0013](0013-cache-behaviour-separation.md) | Separate cache behaviours to preserve request collapsing |
| [0014](0014-admin-ui-askama-cloudscape-tokens.md) | Render the admin UI with askama compile-time templates (styling superseded by 0018) |
| [0015](0015-stripe-prequeue-counter.md) | Stripe the pre-queue registration counter across 10 partition keys |
| [0016](0016-admin-oidc-dynamodb-sessions.md) | Authenticate the admin UI with OIDC and DynamoDB sessions |
| [0017](0017-andon-cord-emergency-controls.md) | Give the operator an andon cord and guarded emergency controls |
| [0018](0018-admin-vouch-design-language.md) | Style the admin UI with the Vouch design language (supersedes 0014's styling) |
| [0019](0019-visitor-facing-serving-state.md) | Express the visitor-facing serving state as one enum |
| [0021](0021-edge-function-gate.md) | Gate the protected origin with a CloudFront Function |
| [0022](0022-durable-controller-cadence.md) | Build the controller's 10-second cadence from durable waits |
| [0023](0023-adaptive-poll-interval.md) | Scale the client poll interval with distance to the front |
| [0024](0024-jws-credentials.md) | Carry credentials as a JWS (amends 0011; amended by 0032) |
| [0025](0025-operator-owned-seal-schedule.md) | The operator owns the open schedule's time; Terraform owns the schedule (revised: Open now, and `active` is not operator-settable) |
| [0026](0026-entry-tickets.md) | One position per identity, from a customer-signed entry ticket (superseded by 0028) |
| [0027](0027-ticket-delivery-fragment-not-query.md) | Deliver the entry ticket by cookie, or by URL fragment — never a query string (superseded by 0028) |
| [0028](0028-remove-entry-tickets.md) | Remove entry tickets: nothing could use them without customer-side work we do not supply |
| [0029](0029-seal-time-demotion.md) | Demote telemetry groups to the tail at the open, matching on read (superseded by 0030) |
| [0030](0030-remove-open-time-demotion.md) | Remove open-time demotion: never enabled, sixteen concepts, and a room-wide refusal if one chunk item went unread |
| [0031](0031-remove-controller-driven-expiry.md) | Remove controller-driven expiry: an unbounded Scan six times a minute that could not see the pre-queue cohort |
| [0032](0032-remove-the-origin-authorizer.md) | Remove the origin authorizer: nothing invoked it, and as wired it forwarded every request |
| [0033](0033-admission-claimed-once-per-visitor.md) | Admission is claimed once per visitor, and a repeat still admits |

## Open

Decisions deferred until measurement or a client engagement supplies the input.

| Question | Resolves how |
|---|---|
| Bot Control Common versus Targeted | Run Targeted in Count mode during a real event and measure what it catches that Common does not. Ten times the per-request cost. |
| Flat-rate versus pay-as-you-go CloudFront pricing | Flat-rate is usually the better fit for a large planned event (predictable, caps exposure), but the Terraform provider cannot create a flat-rate distribution yet ([#45450](https://github.com/hashicorp/terraform-provider-aws/issues/45450), PR #49235). PAYG is the default until it lands; flat-rate is selected manually per event. The crossover is otherwise non-monotonic in event size, poll interval and Bot Control tier. |
| Signing key rotation | Compromise permits minting admission for every event in the deployment. |
| Standby inflow measurement placement | Authorizer-local versus centrally aggregated. |
| No-show controller tuning | Smoothing window and correction bounds need a real event's data. |
| Connector breadth | Product scope: the competitor ships 25+ platform connectors; we ship a CloudFront/origin authorizer. |
