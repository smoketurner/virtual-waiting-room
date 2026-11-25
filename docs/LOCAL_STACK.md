# Local stack

`make stack` runs the whole waiting room on one machine and checks the design's claims against
what happens. Nothing is deployed and no AWS account is used. It complements a run against a
deployment; it does not replace one (see [What it cannot show](#what-it-cannot-show)).

```bash
make stack-emulator                                   # once: build and start the AWS Emulator
make stack                                            # 2,000 visitors, 50/s, a 30 s countdown
make stack STACK_ARGS="--visitors 20000 --rate 200 --countdown 60"
make stack STACK_ARGS="--visitors 100000 --rate 83 --scale 0.1 --arrival herd"
```

It needs Docker (the AWS Emulator has no macOS build) and Node.js (the gate and the page are
JavaScript). A run writes `report.txt`, `report.json` and every function's log under
`target/stack-runs/<run>/`. It exits 0 when every check passes, 1 when one fails, and 3 when the
harness could not keep up and the result is inconclusive.

## What runs, and what stands in

| Part | In the local stack |
|---|---|
| `read`, `generate_token`, `assign_position`, `open_event` | The shipped binaries, built for this machine, each a pool of processes behind a Lambda Runtime API host. A pool grows when an invocation finds no idle environment, as Lambda scales out. Each process keeps its own memory, so the per-environment caches behave as on Lambda |
| `controller` | Its library's `run_pass` against the real `DynamoStore`, every 10 s. The durable-execution wrapper is not run |
| DynamoDB, SSM | The [AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/), pinned in `crates/harness/emulator/Dockerfile`. It enforces DynamoDB's 1,000 writes/s per partition key. `--dynamodb` points DynamoDB at another endpoint, such as [ExtendDB](https://extenddb.org/)'s dev image, to check the same run against a second implementation |
| SQS and the event source mapping | Modelled in the harness: at-least-once delivery (`--duplicate-rate`), the 181 s visibility timeout, the dead-letter queue after 10 receives, and batches of 100 over a 1 s window. The emulator's SQS holds at most 1,000 messages per queue, which would invent backpressure in exactly the crowd cases |
| CloudFront | Modelled: the cache behaviours, a 1 s minimum TTL on the polled paths, request collapsing, and 1 s error caching, for one edge location |
| API Gateway | Modelled: the account's 10,000/s token bucket, the stage throttles from `core/api.tf`, the join method's content-type rule and JSON schema, and the REST proxy event. `--scale` multiplies every limit, for a scaled-down model of a larger event |
| The gate | The shipped CloudFront Function, run under Node by the same loader its tests use |
| Visitors | A model of `waiting.js`'s protocol, at any count, plus a sample (`--page-visitors`, default 20) running the shipped `waiting.js` itself over HTTP |

## What a run checks

- No two visitors are told the same position.
- Every visitor who joins gets a position, nobody gives up, and everyone who stays is admitted.
- The open runs, and every cohort position recomputes from the seed, count and offsets `/status`
  publishes plus the `PreQueue` rows, distinct and inside `[0, N)`.
- A cohort position is uncorrelated with when the visitor registered.
- No window of admissions exceeds its time at the target rate plus the budget's burst.
- Steady admissions run within 10% of the target rate, with or without no-shows.
- `/status` reaches its origin at most about once a second, however many are waiting.
- The gate never passes a request because it failed, and the shipped page's visitors pass it.
- No accepted join is dead-lettered, no function fails or logs an error, and DynamoDB answers
  every call it does not throttle.

The report separates these from two checks on the harness itself: every invocation found an
environment within API Gateway's 29 s, and the gate sidecar answered every request. When either
fails the machine was saturated, and the run says it is inconclusive.

## What it cannot show

- **Quotas and capacity on AWS**: the Lambda account concurrency limit and its scaling rate, an
  on-demand table's cold write ceiling (about 4,000/s), and anything throttled by a service
  limit the emulator does not model.
- **Latency**, cold starts, and the behaviour of many CloudFront edge locations, each with its
  own cache.
- **IAM**, the WAF web ACL, and the Terraform itself.
- **Scale beyond one machine.** A laptop handles tens of thousands of visitors at full rate. For
  a larger event, scale the visitors, the admission rate and API Gateway's limits down together
  (`--scale`), which keeps the shape of the crowd against the throttles.
