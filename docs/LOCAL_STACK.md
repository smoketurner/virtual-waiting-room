# Local stack

`make stack` runs the waiting room's visitor path on one machine and checks the design's claims against
what happens. Nothing is deployed and no AWS account is used. It complements a run against a
deployment; it does not replace one (see [What it cannot show](#what-it-cannot-show)).

```bash
make stack-emulator                                   # once: build and start the AWS Emulator
make stack                                            # 2,000 visitors, 500 at once for 60 s each
make stack STACK_ARGS="--visitors 2000 --concurrency 400 --session 120 --countdown 30"
make stack STACK_ARGS="--visitors 100000 --concurrency 5000 --session 60 --scale 0.1 --arrival herd"
```

It needs Docker (the AWS Emulator has no macOS build) and Node.js (the gate and the page are
JavaScript). A run writes `report.txt`, `report.json` and every function's log under
`target/stack-runs/<run>/`. It exits 0 when every check passes, 1 when one fails, and 3 when the
harness could not keep up and the result is inconclusive.

## Simulated time: `make sim`

`make sim` runs the same scenario, edge, queue, visitors and checks with the functions as their
libraries instead of their binaries, in one process on tokio's paused clock: when every task is
waiting, the clock jumps to the next timer. A run takes as long as its work, not as long as the
event, so real session lengths and crowds fit: a 609 s run of 2,000 visitors takes under a
second, and 100,000 visitors over 2.8 hours of event take about four minutes.

```bash
make sim SIM_ARGS="--visitors 2000 --concurrency 400 --session 120 --countdown 30"
make sim SIM_ARGS="--visitors 500000 --concurrency 1000 --session 900 --countdown 1800 --tickets 20000"
```

- `read`, `generate_token` and `assign_position` answer through their library handlers, the open
  runs `open_event`'s drain wait and open, and the controller its `run_pass`, all over one
  in-memory store (`crates/harness/src/memory.rs`) that keeps each `dynamo.rs`'s write
  conditions and DynamoDB's 1,000 writes/s per key.
- The gate verifies the session cookie with `wr_common::Session`, which the conformance vectors
  hold to the CloudFront Function.
- `--tickets` sells 1 to 4 tickets to each admitted visitor (`--tickets-each` fixes the number);
  at the last one the operator sets a sold-out message and ends the event, and the visitors still
  waiting leave.
- `--after-open N` adds N visitors who arrive after the others, over the herd window starting
  `--after-open-from` seconds after the open, so the draw window and the order after it can be
  checked. Both are `make sim` flags only.
- It cannot show what only the shipped system can: the binaries, their `DynamoDB` expressions,
  the JavaScript gate and page, eventually consistent reads, and each environment's own cache.
  `make stack` checks those.

On the stack's calibration scenario (`--visitors 2000 --concurrency 400 --session 120
--countdown 30 --seed 41`) the sim holds 90% of the spots, at most 400 live against 400, with
arrival to admission 302 s at the median.

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
- Every join accepted before the draw closes (15 s after the start) is in the cohort, not
  numbered behind it.
- Nobody who joined after the draw closed takes a spot someone who joined before it needed, and
  those who joined after it are let in in the order they joined, to within a few seconds.
- No more sessions are ever live than `--concurrency`, counted from the session end each
  `generate_token` answer gives.
- The origin holds at least 85% of its spots from the first admission to the last.
- `/status` reaches its origin at most about once a second, however many are waiting.
- The gate never passes a request because it failed, and, in `make stack`, the visitors running
  the shipped `waiting.js` are admitted and pass it.
- No accepted join is dead-lettered and no function fails or logs an error; in `make stack`,
  DynamoDB also answers every call it does not throttle.

The report separates these from two checks on the harness itself: every invocation found an
environment within API Gateway's 29 s, and the gate sidecar answered every protected request.
When either fails the machine was saturated, and the run says it is inconclusive.

## What it cannot show

- **Quotas and capacity on AWS**: the Lambda account concurrency limit and its scaling rate, an
  on-demand table's cold write ceiling (about 4,000/s), and anything throttled by a service
  limit the emulator does not model.
- **Latency**, cold starts, and the behaviour of many CloudFront edge locations, each with its
  own cache.
- **IAM**, the WAF web ACL, and the Terraform itself.
- **The operator console and the no-JavaScript queue.** `admin` (with its OIDC login and its
  KeyValueStore writer) and `nojs` are not run, by `make stack` or by `make sim`.
- **Scale beyond one machine.** A laptop handles tens of thousands of visitors at full rate. For
  a larger event, scale the visitors, the concurrency and API Gateway's limits down together
  (`--scale`), which keeps the shape of the crowd against the throttles.
- **A real session length in a short run.** A run lasts about `visitors / concurrency` session
  lengths, so runs use sessions of a minute or two. The turnover lag is a fixed few seconds, so a
  short session overstates its share; at 900 s it is about a tenth of what a 120 s run shows. A
  no-show's spot is held for 2 minutes and a pass whatever the session length, so a short session
  overstates what no-shows cost too.

## Findings

Each row is one `make sim` run unless marked, reproducible with its seed.

| Run | Result |
|---|---|
| `--visitors 2000 --concurrency 400 --session 120 --countdown 30 --seed 31` | Every check passes. At most 400 sessions live against 400, and the origin holds 90% of its spots; at each turnover it dips for about a pass while the ended sessions are seen free and the visitors called for them come in |
| `--visitors 2000 --concurrency 400 --session 120 --countdown 30 --no-show 0.3 --duplicate-rate 0.05 --seed 32` | Fails the 85% check: with 30% no-shows the origin holds 62% of its spots, at most 301 live. A no-show's spot is held for 2 minutes and a pass, longer than this 120 s session. 94 duplicate deliveries, no duplicate position |
| `--visitors 5000 --concurrency 500 --session 900 --countdown 600 --no-show 0.3 --seed 32` | Every check passes. At the default session length the same no-show share holds 93% of the spots (F3.8) |
| `--visitors 500000 --concurrency 1000 --session 900 --countdown 1800 --tickets 20000` | Every check passes. 433,100 in the cohort, 8,063 buyers, sold out 9,120 s after the countdown began; 98% of the spots held; about one `/status` origin fetch a second; 341 s of wall clock |
| `--visitors 100000 --concurrency 5000 --session 60 --scale 0.1 --arrival herd --herd-window 10 --seed 11` | A one-tenth model of a million arriving at the open. Half had a position in 2.0 min, 90% in 4.2, 99% in 5.7, at 3.9 join 429s a visitor; 10,849 joined in the draw window and are in the cohort. Fails two checks: 4,579 of the later joiners were let in while someone who joined over 5 s before them still waited (at most 28 s out of order), and the origin held 83% of its spots |
| `make stack`, `--visitors 500 --dynamodb http://127.0.0.1:18080 --seed 22`, at commit `2d6cf3a` | The same checks with `ExtendDB` as the DynamoDB backend |
