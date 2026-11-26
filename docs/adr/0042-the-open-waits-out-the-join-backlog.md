# ADR-0042: The open waits out the join backlog

**Status:** Accepted. Amends [0025](0025-operator-owned-seal-schedule.md): Open now invokes the
open asynchronously.

## 1. Context

A join is in the cohort once `assign_position` has claimed its index, not when the API accepts it.
Arrivals bunch in the last minute, so at T−0 the join queue can hold more than the consumers clear
in seconds: at 7,000 accepted joins a second against a cold table's ~4,000 writes a second, a
minute of rush leaves a backlog of about 50 seconds. The open waited at most 15 s for the queue
to read empty, then numbered everything still queued behind the whole cohort
([#239](https://github.com/smoketurner/virtual-waiting-room/issues/239)).

Fitting the backlog inside 15 s needed the operator to declare the peak join rate and pre-warm the
tables to it. Nobody knows that rate before an event.

## 2. Decision

**The open waits until the join queue has caught up, for as long as its invocation allows (900 s,
Lambda's maximum).** Caught up is a queue holding no more than the consumers counted into the
shards since the last read, twice running: whatever is waiting then is cleared by the next read.
An empty queue is not the test, because joins keep arriving after T−0 and a queue under steady
traffic rarely reads empty twice. If the deadline comes first, the open goes ahead as before.

**Open now invokes the open asynchronously.** The console's request passes through API Gateway,
which waits 29 s, and a draining open can take longer. The console reports that the open was
requested; the phase shows when it lands. Lambda retries a failed asynchronous open, and the open's
error alarm reports one that keeps failing.

`expected_join_rate` is removed. Warm throughput remains, as a way to shorten the wait.

## 3. Consequences

- **The rate of joins decides when the event opens, not who is in the cohort.** A backlog at
  T−0 delays the open by the time it takes to clear, about 50 s in the example above. Everyone is
  waiting during it, so nobody loses a place.
- **Joins accepted during the wait are in the cohort too.** The cohort is everyone counted when
  the open reads the shards.
- **A join refused before T−0 is still behind.** API Gateway's join throttle (7,000/s) and the
  account limit it sits under are not covered: a visitor whose retry is accepted after the open is
  a live joiner. Raising the account quota is the remedy.
- **The console cannot say "already open".** The asynchronous invoke returns before the open runs.
  The audit stamp records that an operator asked for the open.
- **The open reads the ten shard counters once a second while it waits**, ten strongly consistent
  reads a second for at most 15 minutes.
