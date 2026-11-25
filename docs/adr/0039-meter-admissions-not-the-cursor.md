# ADR-0039: Meter admissions, not the cursor

**Status:** Accepted. Amends [0033](0033-admission-claimed-once-per-visitor.md) and
[0022](0022-durable-controller-cadence.md); the no-show estimator they describe is removed.

## 1. Context

The controller metered the room by moving `serving_counter`. Each pass it advanced the cursor by
the target, measured how many of the people it had released last interval had arrived, smoothed
that into a no-show rate, and released more to compensate. `generate_token` admitted anyone the
cursor had passed, at any time.

Three things were wrong with that, and they have one cause: the thing being controlled was the
offer, not the admission.

**Nothing bounded what the origin received.** A position the cursor has passed never expires
([ADR-0031](0031-remove-controller-driven-expiry.md)), so every released visitor who had not yet
arrived was banked credit, and the no-show correction grew the bank by releasing faster. A resume
after a pause, a crowd returning to hidden tabs, or one client holding many passed positions could
redeem all of it in a second
([#235](https://github.com/smoketurner/virtual-waiting-room/issues/235)).

**Latency was measured as absence.** After the open a visitor learns their position up to about
100 s late. The controller compared arrivals in one 10 s interval with releases in the one before,
read the lag as a no-show rate near 1, and ran at its 2× cap; the backlog then arrived on top. A
model of the old law with every visitor showing up peaks at 2.65× the target at a million visitors
([#234](https://github.com/smoketurner/virtual-waiting-room/issues/234)).

**The release was per pass, not per second**, so two executions whose passes interleaved doubled
the rate ([#236](https://github.com/smoketurner/virtual-waiting-room/issues/236)).

The estimator was also most of the controller: an EWMA, a staleness rule so that a pause was not
measured as a 100% no-show, a special case for the phantom position 0, a floor for the smallest
number DynamoDB can store, and three attributes of carried state. "No-show controller tuning" was
an open decision waiting on data from a real event.

## 2. Decision

**`generate_token` takes one unit of an admission budget for every visitor it admits, and refuses
when there is none. The controller grants the budget, and moves the cursor by what was granted plus
what went unused.**

The budget lives on the ten arrivals shards that already existed. Each holds `n`, the admissions
taken, and now `c`, the count it may be taken up to. An admission is one conditional write on a
randomly drawn shard:

```
ADD n 1    ConditionExpression: n < c
```

Each pass the controller reads the shards and computes:

```
owed    = target_rate × seconds since the last open pass     (one interval if that is ≥ 20 s ago, or never)
unused  = Σ (c − n)
budget  = min(unused + owed, 2 intervals)                     → written back as c = n + share
release = owed + min(unused, owed)                            → serving_counter advances by this
```

That is the whole law. Unused budget is, exactly, admissions the origin was promised and did not
receive, so it is what the cursor has to make up. There is no rate to estimate.

## 3. What this guarantees

- **The origin's intake is bounded whoever turns up.** At most two intervals of budget are ever
  outstanding, so over any window of `t` seconds no more than `target_rate × t` plus two intervals'
  worth can be admitted. Banked positions cannot be redeemed faster than that. The bound is tested
  (`no_window_admits_more_than_its_time_plus_the_burst`).
- **No-shows are still compensated**, up to a rate of one half: with a share `z` of released
  visitors never arriving, unused budget settles at `z/(1−z)` of an interval and admissions settle
  on the target (`admissions_converge_on_the_target_despite_no_shows`, F3.8). Past one half the
  cursor is at its cap of twice the target, as it was before.
- **Lag costs a bounded burst, not a sustained overshoot.** While visitors cannot yet know they
  were released, budget goes unused and the cursor runs ahead, as before. When they arrive they
  find at most two intervals of budget. The same model that gave 2.65× for about 40 s gives one or
  two 10 s windows at up to 2×, then the target.
- **Passes can overlap.** Budget and release both follow elapsed seconds, and caps are absolute
  values, so a duplicated or interleaved pass grants nothing twice.
- **An idle event banks nothing beyond the burst** (`an_idle_event_banks_no_more_than_the_burst`).

## 4. Consequences

- **A visitor whose turn has come can be told to wait a moment.** `/v1/generate_token` answers 425
  (`AtCapacity`) when the budget is spent; the page keeps polling at its floor, exactly as it does
  for `StillQueued`. Among visitors the cursor has passed, admission is in the order they ask, not
  position order. During steady running that pool is a few seconds deep.
- **A failed budget write refuses instead of admitting.** Before, an arrival that could not be
  recorded was admitted anyway and the miscount was alarmed. An admission that cannot be metered is
  now not made, and the visitor asks again. `arrival_record_failed` keeps its alarm: if it is
  continuous, nobody is being admitted.
- **The once-per-visitor claim ([ADR-0033](0033-admission-claimed-once-per-visitor.md)) becomes a
  mark.** A row already `admitted` re-mints its session without taking a unit. Otherwise the unit is
  taken first and the row marked second; if two calls for one visitor race, both take a unit, which
  admits one person fewer. `AdmissionClaim` is gone.
- **Controller state shrinks to one attribute**, `last_open_pass_at`. `no_show_rate`,
  `last_arrivals_total` and `last_serving_counter` are gone, with the EWMA, the phantom-position
  rule and the DynamoDB number floor.
- **A pass is ten more writes**: one `UpdateItem` per shard for its cap, on top of the cursor
  write, only while the event is active and admission is open.
- **Admissions inside one interval are not shaped.** The budget is granted in 10 s lumps, so a
  backlog can take an interval's worth in its first second. That was already true of the cursor's
  10 s steps. Smoothing within the interval would need the grant to ramp, which is not built.
- **"No-show controller tuning" is closed** in [the open list](README.md): there is one constant,
  the burst, and it is the origin's stated headroom rather than a smoothing factor.

## 5. Why not the alternatives

**Why not keep the estimator and add a rate limit at `generate_token`?** That is two controls on
one quantity, which is how ADR-0031 described expiry alongside the no-show correction. The budget
makes the estimate redundant: what the estimator inferred from a lagged ratio, the unused budget
states directly.

**Why not a single budget attribute on the event's item?** One item takes 1,000 writes a second
([ADR-0015](0015-stripe-prequeue-counter.md)), and the admission write would share it with every
`queue_counter` claim. The shards exist for that reason, and the conditional write costs what the
unconditional one did.

**Why not expire passed positions instead?** It bounds the bank but not the burst, and it takes a
place away from a visitor whose tab was hidden, which is what
[#97](https://github.com/smoketurner/virtual-waiting-room/issues/97) was.

**Why two intervals of burst?** One interval is what a pass grants; the second is the carry-over
that lets unused budget act as the correction. With none carried, the correction has no memory and
admissions settle below the target whenever anyone fails to show.
