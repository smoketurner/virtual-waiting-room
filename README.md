# Virtual Waiting Room

A virtual waiting room for AWS. Absorbs traffic spikes that would otherwise take down
your site — ticket on-sales, product drops, registration windows — and meters visitors
into your origin at a rate it can survive, in fair first-come first-served order.

**Status: design phase.** No runnable code yet. See [`docs/DESIGN.md`](./docs/DESIGN.md)
and [`docs/PLAN.md`](./docs/PLAN.md).

## Why this exists

AWS published [`virtual-waiting-room-on-aws`](https://github.com/aws-solutions/virtual-waiting-room-on-aws)
in 2022 and **deprecated it in November 2025**, directing customers to AWS Marketplace
or to build their own. Existing deployments no longer receive features, fixes, or CVE
patches.

Meanwhile the commercial option in this category starts around $10K/year with a
sales-led onboarding process and no self-serve tier.

This project is a maintained, open-source alternative — rebuilt rather than forked.

## How it differs from the deprecated AWS solution

| | AWS solution | This |
|---|---|---|
| Counters | ElastiCache Redis (MultiAZ) | DynamoDB atomic counters |
| Networking | VPC, NAT gateway, 5 VPC endpoints | none required |
| Resources deployed | 151 | ~60–70 |
| Cost when idle | ~$330/mo | ~$0 |
| Runtime | Python 3 + Chalice | Rust (arm64) |
| Infrastructure as code | CloudFormation | Terraform |
| Maintained | no | yes |

Redis held eight integers, and everything else — the VPC, the NAT gateway, the
endpoints, the Lambdas forced into private subnets — existed only to reach it.
Removing it removes all of that.

The API contract is kept compatible, so existing integrations port over.

## Design highlights

- **No compute in the ingest path.** API Gateway writes straight to SQS; the burst
  never touches a function.
- **One counter write per batch, not per visitor.** Throughput scales with batch size
  — 100K joins/sec at defaults, ~5M/sec if tuned.
- **Zero idle cost.** Nothing runs between events.
- **Deploys into your account**, commercial regions or GovCloud.

## License

Apache-2.0. Portions are a clean-room reimplementation of concepts from
`aws-solutions/virtual-waiting-room-on-aws`, also Apache-2.0.
