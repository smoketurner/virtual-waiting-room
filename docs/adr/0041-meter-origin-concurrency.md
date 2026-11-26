# ADR-0041: Meter origin concurrency, not a rate

**Status:** Accepted. Amends [0039](0039-meter-admissions-not-the-cursor.md): the budget and the
cursor law it describes are replaced; taking a unit at issuance is kept.

## 1. Context

The controller granted admission budget at `target_rate` visitors a second. That is the wrong
quantity for the origin this room protects. A ticketing or checkout origin is limited by how many
people are on it at once, each holding a cart for minutes, not by how many arrive each second.

A rate can only approximate that limit through the session length: concurrency is rate × session.
Two things made the approximation unusable:

- **The rate could not go low enough.** It was a whole number of visitors per second, so the
  least it could admit was 1/s. With the default one-hour session that is up to 3,600 people on
  the origin at once. An origin that takes 100 needed 100 per hour, about 0.03/s.
- **Nothing tied the two together.** The operator set the rate, Terraform set the session length,
  and nothing checked that their product fitted the origin
  ([#65](https://github.com/smoketurner/virtual-waiting-room/issues/65)).

## 2. Decision

**The operator sets `origin_concurrency`, how many visitors the origin takes at once. Each pass
the controller grants the free slots, `origin_concurrency` minus the sessions live now, as the
admission budget.** `generate_token` still takes one unit per admission and refuses at zero
(ADR-0039).

The room counts live sessions without anyone reporting a departure. A session ends exactly
`session_ttl_seconds` after its admission, and minting it again keeps that end
([#237](https://github.com/smoketurner/virtual-waiting-room/issues/237)). So the live sessions are
the admissions of the last session length. The controller keeps an **admission log** item, the
total admitted at each pass, stamped with the second its shards were read, and reads the entry one
session length back. No session that entry counts ends later than that: a session starts at its
request's whole second, and the request took its unit before the read.

The cursor moves once per slot. It moves by the limit at the open, then by each session as it
comes due to end. It counts those one pass ahead, so the people released for a slot are already
asking when it is granted. A slot granted last pass and still untaken releases half as many again:
that is the no-show correction.

The page meets that halfway. A visitor within `origin_concurrency` of the front polls at its floor,
because sessions ending release them without the cursor's measured speed saying so in advance. A
425 is a slot not yet free, so the page asks again at its floor rather than backing off; only a
429 or a 5xx backs off.

`session_ttl_seconds` is required by every function that mints or counts a session, and Terraform
gives them all the same value. Its default is 900 s, the time to complete a purchase.

## 3. What this guarantees

- **No more than `origin_concurrency` sessions are ever live.** The budget is the free slots. The
  log counts an admission as live until the first pass after its session has certainly ended.
  Every failure leaves fewer slots granted, never more: a lost log write counts its admissions as
  live for longer, and stale caps grant only slots that were free when they were written.
- **Throughput follows from the origin.** It is `origin_concurrency` per session length. Nobody
  sets a rate that could disagree with the limit.
- **Overlapping passes grant nothing twice.** The budget and the release are absolute, computed
  from the shards and the log, not from the time since the last pass.

## 4. Consequences

- **The open admits `origin_concurrency` visitors at once.** "N at a time" means exactly that, so
  the origin must take its limit in one burst. No smoothing is applied.
- **A slot is seen free late.** An admission is logged at the next pass, so a slot goes back into
  the budget up to one pass (10 s) after its session ends. At a 900 s session that is about 1% of
  throughput. Sessions admitted together end together, so the origin dips at each turnover.
  Releasing replacements a pass early, and the page asking at its floor, keep the dip to the
  detection lag rather than the lag plus a polling interval.
- **The front of the queue polls at the floor.** That is `origin_concurrency` visitors asking a
  cached `/status` every 5 s, and those released asking `/generate_token` every 5 s until their
  slot is granted. Both are bounded by the limit, not by the crowd.
- **A visitor who finishes early holds their slot until the session ends.** Nothing reports a
  departure. Freeing it early needs a completion signal from the origin, which is not built
  ([#65](https://github.com/smoketurner/virtual-waiting-room/issues/65)).
- **Lowering the limit admits nobody** until enough sessions have ended to make up the difference.
  Raising it admits the difference at the next pass.
- **Most of a large crowd is not admitted.** At 100 slots and 15-minute sessions the room admits
  400 an hour. The rest keep their positions and poll a cached `/status`. The page's wait estimate,
  `origin_concurrency / session_seconds` until it has measured the cursor, is what tells them.
- **One more item per event.** The log holds one entry per pass for a session length, 90 at the
  default (about 2 KB). It costs one read and one write per pass, and only the controller's
  scoped IAM can touch it.

## 5. Why not the alternatives

- **Keep the rate and derive it from a concurrency.** Setting `rate = concurrency / session`
  bounds the average, not the instant. Admissions bunch at the open and after a pause, and the
  burst the budget allowed is a multiple of the rate, so at any given moment the limit is not held.
- **Hold slots as lease items.** One item per slot, claimed by a conditional write, also bounds
  concurrency. But finding a free slot means probing at random, which fails more often the fuller
  the origin is, and that is the exact moment everyone is asking. The arrivals shards already
  absorb the takes; the log only adds one item the controller alone reads.
- **Free slots on a completion signal.** Freeing a slot when the visitor finishes would turn the
  origin over faster. But it needs the origin to call the room, a new trust boundary, and fixed
  sessions are needed anyway for visitors who never finish. It stays open as #65.
