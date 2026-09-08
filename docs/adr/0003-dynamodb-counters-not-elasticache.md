# ADR-0003: Use DynamoDB atomic counters, not a cache tier

**Status:** Accepted

## Context

The system needs a small number of counters — queue sequence, serving position, arrival and
completion statistics. An in-memory cache tier (ElastiCache for Redis or Memcached) is the
conventional choice for high-rate counters.

## Decision

Hold counters as attributes on a DynamoDB item, updated with `UpdateItem` using `ADD` and
`ReturnValues: ALL_NEW`.

## Consequences

- Writes to a single item are serialized, so each returned value is unique. AWS documents
  this guarantee, so no transaction or optimistic concurrency control is required.
- Single-item throughput is capped at 1,000 write capacity units per second. Sequences work
  around this with batch range allocation; statistics work around it with write sharding.
- **The whole VPC disappears.** ElastiCache requires VPC attachment, which places every
  function touching a counter in a private subnet, which then requires VPC endpoints for
  each AWS service those functions call, plus a NAT gateway, subnets, route tables and flow
  logs. Measured in an existing reference deployment: 26 additional resources and
  approximately $330/month idle.
- DynamoDB, SQS, Secrets Manager, EventBridge and Lambda are all IAM-authenticated
  public-endpoint services, so functions reach them with no private networking.
- A VPC remains available as an opt-in variable for clients whose ATO boundary mandates
  private-subnet compute regardless of IAM. That is policy, not architecture.
