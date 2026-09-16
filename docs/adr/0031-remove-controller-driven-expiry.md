# ADR-0031: Remove controller-driven expiry; DynamoDB TTL is the only reclamation

**Status:** Accepted. Supersedes [0006](0006-controller-driven-expiry-not-ttl.md).

## 1. Context

[ADR-0006](0006-controller-driven-expiry-not-ttl.md) chose to expire positions from the
controller rather than from DynamoDB TTL, so that a position's liveness was decided by a
cursor the system controls rather than a deletion AWS performs whenever it gets to it. Every
10 seconds the controller scanned `Positions` for rows below a grace cutoff whose status was
still `issued`, flipped each to `expired`, and advanced `max_expired_position`.

Four things were wrong with it at once.

**The scan was unbounded and ran six times a minute.** `query_expired` built a `Scan` with a
filter expression and no `Limit`, then paginated the whole table — on the same table
`generate_token` reads consistently on every admission. At the capacity this product is sized
for, `Positions` is millions of rows within minutes of the open. Nothing in the architecture
document's known-gaps section mentioned it.

**It could only ever see live joiners.** A pre-queue registrant has a `PreQueue` row, not a
`Positions` row — their position is computed from the permutation, never stored. A `Scan` over
`Positions` therefore could not reach the pre-queue cohort at all, which is the dominant
population in the scheduled-onsale case the product is built around. `PositionStatus::Expired`
was unreachable for most of the queue.

**`max_expired_position` was written and read by nothing.** The controller advanced it on every
pass that expired anything. No other crate reads the attribute.

**The grace was a duration applied as a distance.** `ADMISSION_GRACE_SECS` was 120, but the
cutoff was `serving_counter − target_rate × 120` — a count of positions. When the no-show
correction released at its `2×` cap, the cursor covered those 120 seconds' worth of positions
in about 60 seconds of wall clock, and a visitor whose tab had been hidden lost their place
having done nothing wrong ([#97](https://github.com/smoketurner/virtual-waiting-room/issues/97)).

Meanwhile `Positions` already carried a 24-hour DynamoDB TTL, doing the reclamation half of the
job for free.

## 2. Decision

Remove controller-driven expiry entirely. DynamoDB TTL reclaims the row; until it does, a
position is live.

The no-show correction already compensates for people who never arrive — it measures arrivals
against releases and releases more to cover the gap. Expiry was a second control acting on the
same quantity, and the two were never reconciled.

## 3. Consequences

- **The controller performs no `Scan` anywhere**, and no longer touches the `Positions` table
  at all: its `POSITIONS_TABLE` environment variable and the `ExpirePositions` IAM statement
  (`dynamodb:Scan` plus `UpdateItem` on `Positions`) are both gone.
- **[#97](https://github.com/smoketurner/virtual-waiting-room/issues/97) closes as "mechanism
  removed".** No position expires, so no hidden tab loses its place.
- **Requirement F3.9 is retired.** A live joiner who never shows keeps their position for the
  life of the event, and can be admitted at any point within it. This is the one visible
  behaviour change. The alternative that preserves F3.9 is a sparse index on
  `status = issued` ordered by `queue_position` with a bounded `Limit` — one more Terraform
  resource, one more index to pay for, and more concepts. It was not taken.
- **`PositionStatus::Expired` and the `expired` wire value go with it**, and so does
  `PositionWrite::allow_expired_overwrite`. That flag existed "so a re-join can reclaim a row
  the controller expired"; with nothing writing `expired`, its widened condition could never
  match. Removing it also removes a `BatchGetItem` from every live-join batch — the read that
  computed it — and with that read goes the residual where a failed `BatchGetItem` degraded to
  "every id unknown" and re-enabled the resurrection the flag existed to prevent.
- A re-join under an existing `request_id` now fails `attribute_not_exists` and abandons its
  claimed position, as a duplicate. The visitor keeps the position they already hold, which is
  the correct answer — previously the row might have been expired out from under them.
- `ReleaseOutcome` **stays.** Its documentation was written entirely around expiry ("the caller
  must not derive an expiry cutoff from a non-persisted cursor"), but the lost-race distinction
  has a second, surviving use: a pass that lost the race released nobody, and reports `0` rather
  than claiming the release its guarded `UpdateItem` refused to land.
