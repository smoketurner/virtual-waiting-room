# ADR-0043: Held turns, and join order after the open

**Status:** Accepted. Amends [0041](0041-meter-origin-concurrency.md) and the control law of
[0022](0022-durable-controller-cadence.md): held turns, in place of the no-show correction and
calling a pass early. Amends [0039](0039-meter-admissions-not-the-cursor.md) and
[0033](0033-admission-claimed-once-per-visitor.md): a visitor takes the spot held on their own
call's shelf. Amends [0042](0042-the-open-waits-out-the-join-backlog.md): the draw closes 15
seconds after the start, and the open waits for that and counts joins sent back to wait for it.
Amends [0025](0025-operator-owned-seal-schedule.md): Open now moves `starts_at` first. Amends
[0023](0023-adaptive-poll-interval.md): a missed turn takes a new place at once.

## Terms

- **Place in line:** the number each visitor gets (`position` in code and storage).
- **Now serving:** the number the room has reached; every place below it has been called
  (`serving_counter`).
- **Spot:** one of the `origin_concurrency` visitors the ticket site takes at once.
- **Turn:** when the room calls a visitor's number, it holds a spot for them. They take it, or
  they miss their turn.
- **Call:** the places in line one controller pass calls, and the spots it holds for them.
- **Shelf:** the ten arrivals counters a call's held spots are counted on.

## 1. Context

The rule the room has to keep:

- Everyone who joins before the start, or within 15 seconds after it, is in random order among
  themselves.
- Everyone who joins later is let in after all of them, close to the order they joined.

Queue-it and Ticketmaster state the same rule. Queue-it randomizes everyone who arrived before
the start "like a raffle" and adds later arrivals to the back, so "no one who arrives late can get
access to the site before someone who arrived on time"
([Queue-it](https://queue-it.com/how-does-queue-it-work)). Ticketmaster: "If you join the queue
after the sale starts, you'll be at the back of the line", and "when it's your turn, your spot will
be held for 10 minutes"
([Ticketmaster](https://help.ticketmaster.com/hc/en-us/articles/9781366115985-What-is-the-queue-and-how-do-I-join)).

Three things decide whether the room keeps it.

**Getting in has to be a turn, not a race.** If the controller calls more places than there are
free spots, whoever's page asks first takes each spot, and the visitors who lose keep asking:
places near the front can lose round after round while later places get in, and a visitor who
waited before the start can miss out entirely while people who joined later are let in.

**Draw membership has to follow when a join was accepted.** If a join goes into the draw whenever
it is processed before the open is written, then while the open waits for the queue, joins
accepted after the start are shuffled in with everyone who was on time.

**Joins after the start have to be numbered in the order they were accepted.** The join queue is a
standard SQS queue, which delivers in no particular order, and a join that comes back after a
retry arrives later still.

## 2. Decision

### 2.1 A called visitor's spot is held for them

**Each pass calls as many places as there are free spots, and holds one spot for each of them.
Nobody else can take a held spot. A visitor who does not take theirs within two minutes has missed
their turn: the spot is called for the next place in line, and the visitor takes a new place at
the back.**

- **Free spots** are `origin_concurrency`, less the spots in use (the admission log, unchanged),
  less the spots still held for earlier calls. A call holds its untaken spots until its turns pass
  and one pass more, so a request that took a spot just before the turn passed has landed before
  the spot is called again.
- **Each call has its own shelf.** Its spots are counted on ten arrivals counters,
  `EVT#{event_id}#AR#{shelf}#{shard}`, of 26 shelves used in turn. `generate_token` takes a
  visitor's spot from their own call's shelf, trying each of its counters from a random one. A
  shelf still held is never reused: that pass calls nobody rather than mix two calls' spots.
- **The `Counters` item keeps the recent calls**, each its first place, when its turns pass, and
  its shelf, written in the same update that moves now serving. `generate_token` finds a visitor's
  call from the item it already reads.
- **A missed turn** is answered 410 with `reason: "turn_missed"`. The waiting page takes a new
  place at the back and says why; the no-JavaScript page shows the same and offers the join form.
  A visitor already let in has their session back after their turn has passed.

**At the open, turns are held longer.** Each waiting page waits a random slice of up to two
minutes before it asks for its place in line, so the cohort does not ask at once. A visitor called
in that time cannot take a turn they do not yet know is theirs. A call made within two minutes of
the draw closing holds its spots until two minutes after those two minutes. That is why there are
26 shelves: a call as the draw closes is held for up to four minutes and ten seconds.

### 2.2 The draw closes 15 seconds after the start

**A join accepted before `starts_at + 15 s` is in the draw. The open does not write before then,
so nobody is let in during the window.**

Queue-it does the same: the first 15 seconds after the start are a collection window, randomized
with the pre-queue, and nobody is let in before numbers are assigned
([white paper](https://queue-it.com/media/qwpmrzxi/queue-it-technical-integration-white-paper.pdf)).
People who arrive in the first seconds of a sale, often the ones refreshing at the start exactly,
go into the draw rather than to the back.

- `assign_position` compares each join's `SentTimestamp` with the cutoff, not when it is processed.
- Opening early from the console moves `starts_at` to the press before invoking the open, so the
  cutoff is 15 seconds after the operator opened.

### 2.3 Joins after the cutoff are numbered close to the order they were accepted

- **Within a batch,** `assign_position` sorts the joins by `SentTimestamp` before numbering them.
- **A join that arrives before the open is written** is not numbered yet. It is sent back with
  `ChangeMessageVisibility`, for 2 seconds, doubling with each delivery to 1,024 s. Ten deliveries
  cover about 34 minutes, longer than the 900 s open, so it is never dead-lettered while waiting.
- **The open counts those as cleared.** `assign_position` adds each one to striped counters
  `EVT#{event_id}#DF#{shard}`, and the open's caught-up test (ADR-0042) counts them with the
  registrations. The backlog it waits for is only joins accepted before the cutoff.

Order after the cutoff is then right to within a few seconds. A FIFO queue would not do better: it
keeps order only within a message group, one group takes 300 messages a second, and high-throughput
mode reaches its 70,000 a second only by spreading messages over many groups with no order between
them ([SQS quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)).

## 3. What this guarantees

- **Nobody takes another visitor's turn.** A called place has a spot only it can take until its
  turn passes. A place not yet called has none.
- **The origin limit holds.** Spots in use plus spots held never exceed `origin_concurrency`. A
  failed pass calls nobody, and a call whose caps were not written holds fewer spots than it called.
- **On time is random, and later is behind it.** Membership of the draw is decided by when the join
  was accepted.
- **A called visitor is not refused for capacity.** Everyone called has a spot; a 425 to a called
  visitor means only that the spot is not written yet, for the moment between the call and its
  caps.

## 4. Consequences

- **A no-show's spot sits empty until their turn passes,** two minutes (four at the open) and a
  pass. At 30% no-shows and a 900 s session the sim keeps the site above 80% full; with a short
  session the held time is a larger share of each session, and the site is less full.
- **Early and late joiners can share a call.** When the draw is smaller than the free spots, the
  open's call includes the first later joiners too. Every one of them has their own spot; some later
  joiners step in seconds before some early ones.
- **The controller reads more:** 26 shelves of ten counters, three `BatchGetItem` calls a pass.
- **A pause does not stop the clock on a turn.** A call held through an operator pause can pass
  while nobody can be let in.
- **Open now moves `starts_at` before the open is invoked.** If the invoke fails, the start has
  still moved and the console records the press; the failure is reported to the operator.
