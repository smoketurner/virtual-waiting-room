# ADR-0034: The waiting room is single-region, and fails open around a regional impairment

**Status:** Accepted

## 1. Context

Every stateful component lives in one AWS region: the four DynamoDB tables, the join queue, the
REST API, every Lambda, the scheduler, and the S3 bucket behind the waiting page. What is not
regional is the edge: the CloudFront distribution, the gate's CloudFront Function, and the
KeyValueStore it reads are global, and the KeyValueStore is replicated to every edge location.

Nothing recorded this as a choice (issue #68). A self-hosting operator takes on the regional risk
a hosted vendor would otherwise absorb, and they should know that before an event rather than
during one.

## 2. Decision

**The deployment is single-region. Multi-region is a different design, not a configuration
option.**

`queue_counter` and `serving_counter` are strict sequences. Every live join claims a block with a
conditional `ADD` on one item, and the open writes the seed, offsets and cohort size together
under `attribute_not_exists(shuffle_seed)`. DynamoDB global tables replicate asynchronously with
last-writer-wins resolution, so two regions claiming blocks at the same moment would issue the
same positions, and two opens could each write a seed. Both are the failures this system exists to
prevent (`docs/DESIGN.md` §5.4). A second region would need a single writer for the sequences,
which puts the failure domain back in one place.

What the deployment does instead is **degrade to fail-open**, and the path to that does not go
through the impaired region.

## 3. What a regional impairment does

With the deployment region impaired and the edge healthy:

- **Admitted visitors keep going.** The gate decides from its KeyValueStore and a session cookie
  with no network call, so every visitor holding an unexpired session passes through for the rest
  of `session_ttl_seconds`.
- **The queue freezes.** `/v1/join`, `/v1/status`, `/v1/queue_num` and `/v1/generate_token` are
  regional. Nobody new joins, nobody is admitted, and a waiting page whose script cannot reach
  the API stops advancing. The waiting page itself comes from CloudFront's cache while it holds a
  copy (`s-maxage` 60 s for the HTML, 300 s for the script and stylesheet), and from the regional
  bucket after that.
- **The dashboard is unreachable.** The admin Lambda is regional, so **Fail open** on the
  dashboard is not available either.
- **Everyone else gets the waiting page instead of the origin.** A request matching a protection
  rule without a valid session is refused, which with the queue frozen means the origin receives
  only already-admitted traffic.

The operator's site is therefore not down, but it is closed to everyone not already admitted, for
as long as the region is impaired.

## 4. The recovery path

The gate's fail-open epoch lives in the KeyValueStore, which is written through CloudFront's
global control plane rather than the deployment region. An operator engages fail-open by writing
`f` (an epoch-seconds deadline) into the gate's config document `c` directly, with the AWS CLI.
`docs/RUNBOOK.md` carries the procedure. The window expires on its own, like the dashboard's.

The direct write skips `Counters.fail_open_until`, the DynamoDB copy the dashboard displays. That
does not need reconciling: both copies are deadlines that resolve on their own (ADR-0021), and
the region-side copy only matters to `generate_token`, which is down anyway.

This path depends on CloudFront's control plane. If that is impaired too, the edge keeps serving
the last replicated config, and there is no lever left inside this deployment.

## 5. Consequences

- The queue's availability is the deployment region's availability for DynamoDB, SQS, API
  Gateway and Lambda. There is no automatic failover and no replica.
- The origin's availability during a regional impairment is bounded by how long the operator
  takes to run the break-glass write, plus KeyValueStore propagation to the edge (seconds).
- The queue state is not lost. The tables keep point-in-time recovery, and when the region returns
  the controller, reads and admissions resume from where they stopped. Visitors whose pages stayed
  open keep their places.
- The break-glass procedure has not been rehearsed against a real deployment. Until it has, the
  recovery time in the second point is an estimate, and issue #68 stays open.
