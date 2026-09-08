# ADR-0008: Isolate concurrent events by partition, not shuffle sharding

**Status:** Accepted

## Context

A deployment may run several events at once. Without isolation they share Lambda concurrency,
table throughput and one queue, so one event can degrade others.

[Shuffle sharding](https://builder.aws.com/content/3F06NpJ8YeoIGP8VHTw4n81pFn8/workload-isolation-using-shuffle-sharding)
assigns each tenant a random subset of N nodes from a pool of M, limiting blast radius.

## Decision

Give each event its own SQS queue and its own Lambda function with reserved concurrency.

## Consequences

Shuffle sharding requires three conditions; none hold here:

| Condition | Status |
|---|---|
| Multiple tenants share infrastructure | No — single-tenant (ADR-0007) |
| Operator assigns tenants to nodes | No — AWS owns every fleet; there is no pool to assign |
| Per-tenant dedication is too expensive | No |

The third is decisive. Shuffle sharding exists to avoid provisioning M dedicated resources
when each costs money at rest. Serverless resources cost nothing at rest:

| Approach | 20 concurrent events | Idle cost | Blast radius |
|---|---|---|---|
| Partition | 20 queues, 20 functions | $0 | 1 event |
| Shuffle shard, M=8 N=2 | 8 queues | $0 | ~5 events per node |

- Reserved concurrency is what makes partitioning real. Without it, functions draw from the
  shared account pool and a runaway event starves the others regardless of function count.
- Holds until DynamoDB's 2,500 tables per region (10,000 on request) binds. Events share
  tables and separate by `event_id`, so that ceiling is distant. If a deployment ever
  exceeded it, shuffle sharding becomes the right answer and this decision needs revisiting.
