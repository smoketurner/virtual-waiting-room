# ADR-0033: Admission is claimed once per visitor, and a repeat still admits

**Status:** Accepted

## 1. Context

`generate_token` records an arrival with an unconditional `ADD arrivals#<shard> :one`. Nothing
marked a visitor as having been admitted, so nothing stopped a second call from recording a second
arrival.

A second call is not an exotic case. `request_id` travels in a URL (issue #62), the waiting page
polls `/v1/generate_token` until it succeeds, and a visitor who reloads, opens a second tab, or
whose response is lost in flight arrives here again. Each of those counted another arrival against
a single release.

The consequence is not a visitor-visible error. The controller measures its no-show rate as
arrivals against what it released; over-counted arrivals drive that rate down, the correction it
applies shrinks, and it under-releases for the rest of the event. The queue moves more slowly than
the operator asked for, nothing logs an error, and no metric says why.

There was a half-built mechanism for this. `PositionStatus` had `Completed` and `Abandoned`
variants and `generate_token` refused an admission on either with `Denied::Spent`, mapped to a 410.
**Nothing in the repository ever wrote either value** — `assign_position` writes `issued` and no
other writer exists — so `Denied::Spent` was unreachable and the enum described a state machine
with no transitions.

## 2. Decision

**One conditional write per visitor claims their admission, and the claim decides whether the
arrival is counted — not whether they are admitted.**

After `decide` admits, `generate_token` issues a single `UpdateItem` against the visitor's
`Positions` row setting `status = admitted`, guarded by:

```
attribute_not_exists(request_id) OR status = "issued"
```

That disjunction is one expression for both populations. A live joiner has a row already, written
at registration with `status = issued`, so the second disjunct claims it. A pre-queue member has no
row at all — their position is derived from the seed on read and never stored (ADR-0002) — so the
first disjunct claims it and the same update creates the row, stamping the position that was
resolved for them. Every later call through either path fails the condition.

A successful claim is followed by `record_arrival`. A failed condition skips it. **Neither refuses
the visitor**: both return 200 with a freshly signed session.

`PositionStatus` is now `Issued | Admitted`. `Completed`, `Abandoned` and `Denied::Spent` are
deleted, along with the 410 they produced.

## 3. Why a repeat still admits

Refusing a second call would strand a visitor whose first response never reached them, for a
failure that was not theirs, while holding a position that is still theirs. The cookie they are
asking for is one they are entitled to, and re-signing it costs nothing the first signing did not.

This is not a new exposure. A pre-queue member could already re-mint indefinitely, because nothing
recorded that they had been admitted; live joiners could too, because nothing wrote `completed`.
The change makes both populations behave the same way deliberately rather than by omission. Binding
a credential to the visitor is issues #61 and #62's subject and needs a mechanism this does not
have; what this ADR fixes is the arithmetic the controller runs on, which was wrong for every
visitor who reloaded.

## 4. Which way a failure leans

A claim that fails with a store error leaves it unknown whether the arrival has already been
counted, so it is counted.

The two mistakes are not symmetric. An over-counted arrival understates the no-show rate, so the
controller corrects less and releases fewer people — the origin is protected and the queue is
slower. A missed arrival overstates it, so the controller releases more people than the origin
agreed to serve. The second is the damaging direction, so the uncertain case takes the first.

It is logged as `admission_claim_failed` with a metric filter and an alarm at threshold zero. A
claim failing for one visitor is harmless; a claim failing for everyone — a missing grant, a
throttled table — collapses the measured no-show rate toward zero while visitors are admitted
normally throughout, which is this system's characteristic failure and is otherwise silent.

## 5. Why not the alternatives

**Why not a separate claim item, in `Tokens`?** `Tokens` is the table that already carries tagged
keys for multiple item kinds, so an `ADM#` row would fit its shape. But it needs a fourth table
binding, an environment variable and an IAM grant on `generate_token`, to record something that is
a fact about the visitor's position — which is what the `Positions` row, keyed by exactly the right
id and already read on this path, is for.

**Why not make `record_arrival` itself conditional?** The arrivals shards are counters with no
per-visitor structure; making them idempotent would mean recording which visitors had been counted,
which is the claim by another name and in a hotter item.

**Why not keep `Completed` and refuse with 410?** That was the shape the code already implied, and
it strands real visitors — see §3. The status is now read for the count, not for the refusal, which
is the only thing it can honestly govern.

## 6. Consequences

- One conditional `UpdateItem` per admission, on the table `generate_token` already reads, at the
  admission rate rather than the join rate. Nothing scans that table (ADR-0031).
- A pre-queue member now leaves a `Positions` row behind, created at admission. It carries the
  standard 24-hour ttl, so it is reclaimed with everything else.
- `entry_time` is written with `if_not_exists`, so a live joiner keeps the timestamp from when they
  claimed their position rather than having it overwritten with the moment they were admitted. This
  added `Update::set_if_not_exists` to `wr_common::expr`.
- The position a pre-queue member is admitted at is stamped on their row, so later reads take it
  from the row rather than recomputing it. It is the same number either way.
- `PositionStatus` loses two variants and `Denied` loses one. `/v1/generate_token` no longer
  answers 410 under any condition.
