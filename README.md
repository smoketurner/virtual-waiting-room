# Virtual Waiting Room

A virtual waiting room for AWS. Absorbs traffic spikes that would otherwise take down
your site — ticket on-sales, product drops, registration windows — and meters visitors
into your origin at a rate it can survive.

**Status: design phase.** No runnable code yet.

- [`docs/REQUIREMENTS.md`](./docs/REQUIREMENTS.md) — numbered, testable requirements
- [`docs/DESIGN.md`](./docs/DESIGN.md) — architecture and rationale, every claim sourced
- [`docs/PLAN.md`](./docs/PLAN.md) — phased implementation plan

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
| Resources deployed | 151 | target ≤ 80 |
| Cost when idle | ~$330/mo | ~$0 plus per-event pre-warming |
| Runtime | Python 3 + Chalice | Rust (arm64) |
| Infrastructure as code | CloudFormation | Terraform |
| Scheduled-event handling | live arrival order | pre-queue with randomized assignment |
| Failure behaviour | undefined | fails open |
| Maintained | no | yes |

Redis held eight integers, and everything else — the VPC, the NAT gateway, the
endpoints, the Lambdas forced into private subnets — existed only to reach it.
Removing it removes all of that.

The API contract is kept compatible, so existing integrations port over.

## Design highlights

- **The burst is removed, not absorbed.** Assigning queue positions by arrival order
  makes arriving early an advantage, so everyone arrives at once. Randomizing among
  everyone present at the scheduled start drops peak write load from ~1,000,000/sec to
  ~3,300/sec — inside default AWS quotas.
- **No compute in the ingest path.** API Gateway writes straight to SQS; the burst never
  touches a function.
- **Fails open.** If the waiting room is unavailable, visitors reach your site. A waiting
  room that fails closed is worse than none.
- **Near-zero idle cost.** Nothing runs between events; tables are pre-warmed before one.
- **Deploys into your account**, commercial regions or GovCloud.

## License

Apache-2.0. Portions are a clean-room reimplementation of concepts from
`aws-solutions/virtual-waiting-room-on-aws`, also Apache-2.0.
