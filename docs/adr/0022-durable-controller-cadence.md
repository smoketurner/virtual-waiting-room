# ADR-0022: Build the controller's 10-second cadence from durable waits

> **Not yet delivering this in the deployed stack.** The durable configuration is applied and the
> code uses `ctx.wait`, but `vwr-dev-controller` bills ~50 seconds per invocation: all six passes
> run under one RequestId, so the waits are not suspending. The cause is the schedule, not this
> decision — EventBridge Scheduler's templated Lambda target invokes synchronously, and a
> synchronous durable invocation is held open for the whole execution. The target ARN is also
> unqualified, which the durable functions documentation forbids. Tracked and diagnosed in
> [#76](https://github.com/smoketurner/virtual-waiting-room/issues/76); the reasoning below stands
> once that is fixed.

**Status:** Accepted. The SDK it depends on is an experimental preview (§5).

## 1. Context

The outflow controller has to run every 10 seconds. That interval is load-bearing in two ways.
It sets the release granularity: advancing `serving_counter` is what admits people, so a pass
every 60 seconds would release a whole minute's worth at once and the only thing spreading
those arrivals across the origin would be the visitors' own poll interval, which we do not
control. It also sets the sample period of a closed loop — measuring arrivals against releases
once a minute makes the EWMA no-show correction sluggish to converge.

No AWS scheduler fires that often. EventBridge Scheduler rate expressions take
`minutes | hours | days`, and the service invokes targets "with 60 second precision"; the older
`aws_cloudwatch_event_rule` has the same one-minute floor.

The original workaround was to absorb the gap inside the function: the schedule fired
`rate(1 minute)` and the handler ran six passes with `tokio::time::sleep` between them. It
worked, and it billed the way you would expect — roughly 50 seconds of a 256 MB function
sleeping, 1,440 times a day, for a few milliseconds of DynamoDB work per pass.

## 2. Options

| Option | Cadence | Cost of waiting |
|---|---|---|
| `tokio::time::sleep` in the handler | 10 s | Full Lambda duration billing while idle |
| Step Functions **Standard** + `Wait` loop | 10 s | ~4 state transitions per pass at $25/M — the most expensive option |
| Step Functions **Express** + `Wait` loop | 10 s | Duration billing at 64 MB instead of 256 MB — cheaper, same model |
| **Lambda durable functions** | 10 s | None: a durable wait suspends the execution |

Express is the interesting near-miss. It is genuinely cheaper, but only because it meters the
same wall-clock waiting at a lower unit rate. It also moves the loop into ASL, adds a state
machine, an IAM role and a log group, and swaps an exactly-once sequence inside one invoke for
an at-least-once execution model — which is the wrong direction for the component whose job is
to *not* release twice.

Durable functions are the only option where waiting is free rather than discounted: the
execution suspends and, for on-demand functions, "does not incur duration charges until
execution resumes."

## 3. Decision

Keep the `rate(1 minute)` schedule and the six-pass structure. Replace the sleeps with durable
waits and make each pass a durable step.

```rust
for pass in 0..PASSES_PER_INVOKE {
    ctx.step(...).name(format!("pass-{pass}")).await?;   // checkpointed
    ctx.wait(Duration::from_secs(INTERVAL_SECS)).await?; // suspends, unbilled
}
```

Each wait ends the invocation. Lambda invokes the function again to resume, and the SDK replays
the handler from the start, returning completed steps from their checkpoints instead of
re-running them. One execution still covers one minute of cadence; it now spans several
invocations rather than holding one open.

Three properties of the controller drive the configuration:

- **A pass is not idempotent.** It advances `serving_counter`. Steps therefore use
  `StepSemantics::AtMostOncePerRetry`, so a pass interrupted after its `UpdateItem` landed is
  treated as failed rather than replayed into a second release.
- **A failed pass should be skipped, not retried.** The SDK default retries six times with
  exponential backoff, suspending for each delay — which would drag the remaining passes off
  the 10-second cadence. The strategy is `RetryDecision::Stop`: the execution ends and the next
  scheduled one starts a clean minute, exactly what a failed pass did before.
- **Executions stay short.** `ExecutionTimeout` is 120 s, bounding one minute of cadence with
  headroom. Replay re-runs the handler from the beginning, so an unbounded loop would
  accumulate checkpoint history without limit; one minute per execution keeps it at six steps.

The function timeout drops from 90 s to 30 s, because an invocation now covers replay plus a
single pass rather than a whole minute.

## 4. Consequences

The controller stops being billed for waiting. What remains is the real work — a few
milliseconds per pass — plus per-durable-operation and checkpoint-data charges.

The cadence is unchanged, so nothing downstream moves: `serving_counter` advances on the same
10-second interval, and the closed loop samples at the same rate.

`PassOutcome` is now checkpointed, so it has to round-trip through serde. Because executions
live about a minute, a change to its shape cannot strand an in-flight execution for long.

Enabling `durable_config` forces a replacement of the Lambda function, and destroying a durable
function stops its in-flight executions first — the provider documents that as taking up to an
hour, so the resource carries a 60-minute delete timeout.

The execution role needs `lambda:CheckpointDurableExecution` and
`lambda:GetDurableExecutionState`. A durable execution is a sub-resource of a function
*version*, so the policy resource must carry a qualifier; an unqualified function ARN never
matches one and every checkpoint is denied.

## 5. The risk we are taking

`aws-durable-execution-sdk` is version 0.1.0, published two weeks before this decision, and its
repository is labelled "Experimental preview. NOT FOR PRODUCTION USE." The README warns the API
may change without notice. AWS's own runtime table does not yet list `provided.al2023` as
supported, though the SDK's conformance suite deploys onto exactly that runtime with
`cargo lambda`, which is how this crate is built.

This is the admission rate limiter — the component where a defect releases a burst at the
customer's origin. Adopting a preview SDK here is a deliberate acceptance of that risk, not an
oversight. The mitigations are that the dependency is pinned exactly, the blast radius is one
crate whose pure logic remains AWS-free and covered by its existing tests, and reverting means
restoring a sleep loop that is a dozen lines.

Revisit when the SDK reaches GA.
