# ADR-0001: Randomize position assignment for scheduled events

**Status:** Accepted

## Context

Queue position can be assigned in arrival order or by randomizing among participants present
at a scheduled start.

Arrival order makes early arrival advantageous, so every visitor arrives in the first
seconds after an event opens. For a 1,000,000-visitor event arriving over 1–5 seconds, that
is 200,000–1,000,000 writes/second against a default per-table quota of 40,000 write request
units per second.

## Decision

For events with a scheduled start time, hold early arrivals on a countdown page and assign
positions in randomized order at the start. Live joins after opening remain first-come,
first-served.

## Consequences

- Peak assignment load becomes a scheduling parameter rather than an arrival property.
- Connection latency and geography stop conferring advantage, which also removes the speed
  advantage automated clients hold over browsers. It does **not** remove their volume
  advantage. Randomization converts registrations into expected share linearly, so a farm
  holding ten times the registrations of the genuine population takes roughly 91% of the
  front of the queue — and the permutation is perfectly uniform over that poisoned
  population. Uniformity is a property of the draw, not of who holds the tickets.
  Constraining volume is a separate mechanism and none is built
  ([0028](0028-remove-entry-tickets.md)), so the raffle is a bare raffle and this ADR's
  guarantee is only about speed.
- Two fairness models must be documented to visitors: randomized for scheduled events,
  first-in first-out for standby activation.
- Queue-it applies the same split, describing pre-queue randomization "like a raffle" and
  stating that safety-net activation operates as a FIFO queue.
