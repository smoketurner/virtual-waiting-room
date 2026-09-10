# ADR-0019: The visitor-facing serving state is one enum

**Status:** Accepted

## Context

"Stop the event" is a fiction. Visitors keep arriving at the URL no matter what an operator
clicks; the burst lands on CloudFront and API Gateway regardless (there is no compute in the
ingest path, by design). An operator cannot stop arrivals. The only thing an operator controls
is **what an arriving visitor gets when they arrive.**

Until now that control was spread across two independently-set fields on the `Counters` item:

- `phase` — the event lifecycle: `idle -> pre_queue -> active -> post_event`, plus `maintenance`.
- `admission_paused` — a boolean andon-cord flag (ADR-0017).

The true operational state was the *product* `(phase, admission_paused)` — a 10-cell space in
which most cells are nonsense (paused + post_event? paused + maintenance?), and in which the
one thing that actually matters — what the arriving visitor sees — was not named anywhere. This
produced concrete bugs: Pause wrote a flag no reader served, so it was invisible to visitors;
"Force maintenance" was a one-way door; and the operator copy could not describe any control
without hedging, because the controls did not map to distinct visitor outcomes.

`conventions.md` is explicit: **"Enums for state machines, not boolean flags."** The stop-state
is a state machine wearing a boolean.

## Decision

Name the thing that matters. Define one enum, **`ServingState`**, that answers a single
question: *what does a visitor arriving right now experience?* It is the visitor-facing
projection returned by `/status`, and it is the vocabulary the admin console speaks.

| `ServingState` | Arriving visitor gets | Already-queued visitor | Operator meaning |
|---|---|---|---|
| **Running** | Enters the queue; admitted at the target rate when their turn comes. | Position holds; progresses toward admission. | Normal operation. |
| **Paused** | Enters the queue; held, not admitted. Page shows "you keep your place." | Position holds; does **not** progress (admission is stopped). | Hold the line. Reversible, no data lost. |
| **Closed** | Not queued. Page shows "this event is not open; come back later." | Position **held, frozen** (not discarded), so a reopen resumes the same queue. | The event is down / not running. Reversible. |
| **FailOpen** | Sent **straight to the origin**, no queue. | Also sent straight through. | The waiting room itself is the fault (ADR-0009): stop gating, let everyone in. |

Two things this makes explicit:

- **Arrivals are never "stopped."** Every state above still receives the arriving request; they
  differ only in the response. "Stop everything" was never achievable and is removed as a concept.
- **Closed vs FailOpen are opposites**, and were both hiding under "stop." Closed turns arrivals
  away; FailOpen lets them all through. Conflating them was the core confusion.

### Relationship to `Phase` (they are orthogonal, and both stay)

`ServingState` does **not** replace `Phase`. They answer different questions:

- `Phase` (`idle/pre_queue/active/post_event`) is the event's **timeline**: it governs the
  mechanism — when the seed is written (T-0 seal), when positions become computable, when the
  queue is draining. It is largely time/schedule-driven.
- `ServingState` is the **operator's live override of the visitor experience**, on top of
  wherever the timeline is.

`ServingState` is a **derived projection**, computed, not a stored field to keep in sync. The
operator's live intent is itself a small enum state machine, **`AdmissionControl`** (`Open`,
`Paused`, `FailOpen`), which replaces the former `admission_paused` + `fail_open` boolean pair —
so an illegal combination (paused *and* fail-open) is unrepresentable, and the transitions are
methods on the type (`pause`, `resume`, `fail_open`, `recover`) rather than free functions over
bools. `ServingState` is then a total function of `(Phase, AdmissionControl)`:

```
serving_state(phase, control) =
    match control:
        FailOpen                     -> FailOpen
        Paused if phase == Active    -> Paused
        Open | Paused =>
            match phase:
                Active               -> Running
                Idle | PreQueue      -> Closed   # nothing to admit yet
                PostEvent            -> Closed   # event over
                Maintenance          -> Closed   # operator took it down
```

So the operator sets `phase` and `admission_control`; `/status` publishes the single derived
`ServingState`, which is the authoritative visitor-facing signal. The admin console describes each
control by the `ServingState` it produces, not the field it writes. `maintenance` as a
*visitor-facing* word is retired — to a visitor it is simply `Closed`; `Maintenance` survives only
as an internal phase value that projects to `Closed`.

### Steady states only; `Pausing`/`Resuming` deferred

`ServingState` models **steady states only**. Transient states `Pausing`/`Resuming` are
deliberately omitted: pausing today is a single conditional write with no observable in-between —
a visitor sees `Running` then `Paused`, never a "pausing" interval. Adding them now would model a
drain that nothing performs (conventions.md: no phantom features). They become real once the
**outflow controller** exists and "pause" means "stop admitting *and* let in-flight admissions
settle" — a genuine, observable drain. At that point they slot in as `Running -> Pausing ->
Paused` (and `Paused -> Resuming -> Running`). Recorded here so a future reader knows they were
considered and deferred by design, not missed.

### Transitions

`ServingState` transitions are induced by the operator actions and the timeline, so its legal
moves are whatever the underlying `phase` machine (ADR-0017 revision: forward chain plus
`maintenance <-> {active, idle}`) and the two booleans permit. The machine is therefore
**self-consistent by construction**: there is no separate `ServingState` transition table to
drift from the phase table. `Paused` returns to `Running` by clearing the pause; `Closed` returns
to `Running`/`Paused` by moving `phase` back to `active`; `FailOpen` is the break-glass and
clears back to whatever the phase/pause imply.

## Consequences

- **The visitor is now first-class.** Every control and every help string is defined by the
  `ServingState` an arriving visitor lands in, satisfying the project principle that a control is
  judged by the waiting visitor's experience, not the field it writes.
- **`/status` gains a `serving_state` field** (the derived enum), alongside the existing `phase`
  and `admission_paused`. The waiting page switches on `serving_state`. Existing fields stay for
  now; `serving_state` is the authoritative visitor-facing signal.
- **Illegal combinations become unrepresentable at the boundary that matters.** The stored
  `(phase, paused, fail_open)` can still express odd tuples, but the *visitor-facing* state is a
  total function over them, so a visitor never sees an undefined state. A follow-up may tighten
  the stored representation; this ADR fixes the projection first because that is what the visitor
  and the operator see.
- **`FailOpen` is defined but its enforcement is post-MVP**, like `admission_paused`: it is
  published in `serving_state` and labeled pending its enforcer (the authorizer / edge), not
  faked (conventions.md: no phantom features).
- **Supersedes the visitor-facing half of ADR-0017's stop model**: the two-level Pause/Maintenance
  control is re-expressed as `ServingState` values. ADR-0017's andon-cord *decision* (graded,
  guarded, audited controls) stands; this ADR renames the states they produce and adds `Closed`
  and `FailOpen` as distinct, honest visitor outcomes.
- **Depends on** the outflow controller for `Paused`/`Running` admission enforcement and the
  authorizer for `FailOpen`, both post-MVP. The projection and the visitor-facing publication
  ship now.
