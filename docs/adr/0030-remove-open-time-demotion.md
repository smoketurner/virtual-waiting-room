# ADR-0030: Remove open-time demotion

**Status:** Accepted. Supersedes [0029](0029-seal-time-demotion.md).

## 1. Context

[ADR-0029](0029-seal-time-demotion.md) classified the pre-queue cohort when the event opened,
grouped it by the join-time telemetry, and moved every group over its `signal:max` threshold to
a tail `[N, 2N)` behind the rest of the cohort. It shipped as issue #145.

It was never switched on. `demotion_rules` defaulted to `""`, which disabled the scan entirely,
and `demotion_mode` defaulted to `observe`, which demotes nobody even when rules are set. No
deployment has enforced it.

What it cost while switched off:

- **About 2,930 lines of Rust**, 13% of the workspace, threaded through every crate except
  `assign_position`.
- **Sixteen concepts** a reader had to hold to follow one position lookup: signals, rules, modes,
  the classification, the demoted set, a per-run nonce, chunk items, chunks-before-election
  ordering, the `2N` tail, a per-environment cache keyed by nonce, the refuse-without-the-set
  invariant, straggler-before-demotion ordering, tiers, two densities, people-versus-positions
  conversion in three directions, and the report item. Eleven existed only to make the tail work
  and were the identity at `D = 0`.
- **A `dynamodb:Scan` grant on `PreQueue`** for `open_event`, and a Lambda sized at 300 seconds
  and 1 GB for a scan it never ran.
- **Nine follow-up pull requests in three days** (#150, #155, #157, #158, #159, #162, #163, #164,
  #165) after the feature landed.

And it carried a failure mode worse than the feature: `ResolveError::DemotionUnavailable` meant a
single unreadable chunk item made `/v1/queue_num` refuse for *every* pre-queue registrant in the
room. That was deliberate — answering from the primary slot would silently un-demote everyone —
but it means a control nobody had enabled owned a room-wide availability failure.

## 2. Decision

Remove the mechanism: the classifier, the demoted set and its chunk items, the per-run nonce, the
`2N` tail, the report item and its dashboard card, the `Tiers` position-space model in the
controller, and the two Terraform variables.

A pre-queue position is `PRP(seed, offset[s] + l, N)` and nothing else. `queue_counter` starts at
`N`, never `2N`. `open_event` is one `BatchGetItem` and one conditional `UpdateItem` again.

## 3. Consequences

- The controller's release and expiry arithmetic is plain subtraction. `people_in`,
  `positions_for_people` and `walk_back` were the identity at `D = 0`, so nothing that ran in any
  deployment changes behaviour.
- `open_event` drops to a 10-second timeout and 256 MB, and loses `dynamodb:Scan` on `PreQueue`
  along with `PutItem` and `DeleteItem` on `Counters`. It now reads counters, never rows.
- The `Counters` item loses `demoted_count`, `demotion_nonce` and `demotion_chunks`. One
  deployment serves one event, so there is no migration; a stack mid-event cannot take this
  upgrade.
- **Requirement F6.3 is retired.** Deferred bot enforcement has no mechanism again, which is where
  [ADR-0028](0028-remove-entry-tickets.md) left it before #145. That ADR's §4 named #145 as the
  surviving bounding mechanism; it no longer is.
- Issue #59 (a raffle that can be stuffed for free) is open and unmitigated. Demotion was not
  mitigating it: a client posting to the regional API Gateway URL rather than through CloudFront
  carries no viewer telemetry, and an untelemetered row never matched a group, so the control was
  structurally blind to exactly the traffic it was aimed at. Closing that bypass is worth more
  against #59 than this was.
- The join-time telemetry the classifier consumed has no remaining reader. It is removed
  separately, which is what takes the six conditional `MessageAttribute.N` entries out of the
  burst path's Velocity template.
