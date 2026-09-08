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
  advantage automated clients hold over browsers.
- Two fairness models must be documented to visitors: randomized for scheduled events,
  first-in first-out for standby activation.
- Queue-it applies the same split, describing pre-queue randomization "like a raffle" and
  stating that safety-net activation operates as a FIFO queue.
