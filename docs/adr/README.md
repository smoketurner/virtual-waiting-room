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
| [0006](0006-controller-driven-expiry-not-ttl.md) | Drive position expiry from the controller, not DynamoDB TTL |
| [0007](0007-single-tenant-deployment.md) | Deploy single-tenant into the client's account |
| [0008](0008-partition-isolation-not-shuffle-sharding.md) | Isolate concurrent events by partition, not shuffle sharding |
| [0009](0009-fail-open.md) | Fail open when the waiting room is unavailable |
| [0010](0010-client-supplied-request-id.md) | Client supplies the request identifier |
| [0011](0011-session-cookie-after-token.md) | Establish a session after validating the admission token |
| [0012](0012-anti-ddos-count-mode.md) | Ship the anti-DDoS rule group in Count mode |
| [0013](0013-cache-behaviour-separation.md) | Separate cache behaviours to preserve request collapsing |
| [0014](0014-admin-ui-askama-cloudscape-tokens.md) | Render the admin UI with askama and Cloudscape design tokens |
| [0015](0015-stripe-prequeue-counter.md) | Stripe the pre-queue registration counter across 10 shards |

## Open

Decisions deferred until measurement or a client engagement supplies the input.

| Question | Resolves how |
|---|---|
| Bot Control Common versus Targeted | Run Targeted in Count mode during a real event and measure what it catches that Common does not. Ten times the per-request cost. |
| Flat-rate versus pay-as-you-go CloudFront pricing | Flat-rate is usually the better fit for a large planned event (predictable, caps exposure), but the Terraform provider cannot create a flat-rate distribution yet ([#45450](https://github.com/hashicorp/terraform-provider-aws/issues/45450), PR #49235). PAYG is the default until it lands; flat-rate is selected manually per event. The crossover is otherwise non-monotonic in event size, poll interval and Bot Control tier. |
| Session credential format | Whether to follow an HMAC-over-concatenation scheme or a JWT. The required property is only that it signs different inputs from the admission token. |
| Signing key rotation | Compromise permits minting admission for every event in the deployment. |
| Standby inflow measurement placement | Authorizer-local versus centrally aggregated. |
| No-show controller tuning | Smoothing window and correction bounds need a real event's data. |
| Connector breadth | Product scope: the competitor ships 25+ platform connectors; we ship a CloudFront/origin authorizer. |
