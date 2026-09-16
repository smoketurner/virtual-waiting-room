# ADR-0029: Demote telemetry groups to the tail at the seal, matching on read

**Status:** Accepted

## 1. Context

Registration is compute-free and `request_id` is client-supplied, so nothing bounds how many
positions one party takes. Randomization removes the *speed* advantage an automated client holds
over a browser (ADR-0001); it does nothing about *volume*, which it converts into expected share
of the front of the queue linearly. A farm holding ten times the registrations of the genuine
population takes roughly 91% of the front. Every deployment is a bare raffle (issue #59).

Since #139, every registration row carries what the join reported on the way in: the viewer
address, its ASN, its country, the TLS stack's JA4 fingerprint and the user agent. A farm running
one toolkit across many addresses collapses onto a handful of JA4 values; one running many
processes behind a few addresses collapses onto those addresses; scalper infrastructure clusters
in a small number of hosting ASNs. Nothing read any of it (issue #145).

The mechanism requirement F6.3 originally assumed was WAF Bot Control's labels, forwarded to the
origin as inserted headers. That path is real but not free, needs a pricing tier or a
self-managed web ACL, and would still land on the same row and be acted on at the same moment.
The moment is the design decision here; the signal source is not.

## 2. Decision

**Classify at the seal, over the whole cohort; store the demoted groups once; match every row
against them on read.**

`seal_event` takes an operator-configured rule list, `signal:max` entries over `address`, `asn`,
`ja4` and `ua`. When any rule is set it scans the pre-queue after reading the shard counts and
before the seal write, groups the cohort's rows by each ruled signal, and marks every group whose
size exceeds its threshold as demoted. Every row in a demoted group is demoted.

Nothing is written per row. The seal writes the demoted group set — the `signal:value` pairs —
as chunk items keyed by a per-run nonce (`EVT#{event_id}#DG#{nonce}#{k}`), then the one
conditional seal write also carries `D` (the count of matched rows), the nonce and the chunk
count, and starts the live-join sequence at `2N`. A resolver (`read`, `generate_token`) loads the
set once per execution environment and matches a row's telemetry against it: a match resolves to
`N + PRP(seed, i, N)` instead of `PRP(seed, i, N)`, the same slot in a second copy of the index
space behind the whole cohort. Primary slots are below `N`, demoted slots are in `[N, 2N)`, live
joins start at `2N`: three disjoint ranges, each its own bijection, so no position is ever held
twice.

A demoted row does not lose its place; it resolves behind everyone the rules did not touch.

**The controller knows the density.** The tail is sparse: `D` people over `N` positions, and the
primary range is `N − D` people over `N`. The seal counted both, so the controller converts
rather than corrects: it releases the operator's target as a number of *people* per interval,
converting to positions at the density of the tier the cursor is in; it measures no-shows as
arrivals against people released, not positions; and it walks the expiry grace back as a count of
people, so 120 seconds stays 120 seconds inside the tail.

**`observe` is the default mode.** It runs the whole classification and writes the report
without demoting anyone. `enforce` acts. No rules means no scan, and the seal is the single write
it always was.

**The operator sees it.** The seal writes a report item, `EVT#{event_id}#DM`, holding the mode,
the rules, the cohort size, the demoted count, and the largest demoted groups with the threshold
each exceeded. The dashboard renders it. A fairness control that acts invisibly is not defensible
after the event, and a threshold cannot be tuned against a report nobody can read.

## 3. Why these choices

**Why the seal and not the join.** Acting at join reveals the classification while there is time
to retool and re-register; a group is also better judged on its whole pre-queue footprint than
on the window a rate rule sees. This is what F6.3 asked for, and it is Queue-it's Hype Event
Protection shape without a partner.

**Why demotion and not a block.** A rule that catches a farm also catches an office NAT, a campus
network or a carrier's CGNAT egress. Demoted, the office still gets in, after the people the
rules left alone. Blocked, it does not, and the operator finds out from the office. Demotion
bounds the damage a false positive can do to "later", which is the only bound that lets a
threshold be tried on a real event at all.

**Why the group set and not a per-row mark.** The first cut of this design wrote a compact tail
index on every demoted row after the seal: `D` conditional writes, a phase held at `pre_queue`
while they landed, a partial-failure story, and a seal whose duration grew with the attack — at
32 writes in flight, roughly 40 seconds per 100,000 demoted rows and a ceiling near three million
inside the Lambda limit. The dollars were trivial (a write unit per row); the shape was wrong.
The problem being defended against is one whose size the attacker chooses, and the mitigation's
cost should not scale with it. Storing the groups once costs a few items however large the farm,
the seal stays one atomic write with nothing to apply afterwards, there is no window in which a
demoted row is visible at its primary slot, and the tail is reproducible from the published set
rather than from marks on rows. The read-time cost is four hash lookups per resolution and one
read of the set per execution environment; the set is immutable once sealed, so that read never
repeats.

**Why the controller has to know.** Mapping demoted rows to `N + p` leaves `N − D` gaps in the
tail. The controller advances the cursor by a bounded multiple of the target rate and treats an
unclaimed position like a no-show, capped at twice the target; at a 1% demotion rate it would
admit 2% of the target while crossing the tail and spend `N / 2r` doing it, with every live
joiner stuck behind. The same cap made in positions would also shrink the expiry grace to seconds
of wall clock there. The densities are exact, not estimates, so converting at them is a
bookkeeping change rather than a heuristic, and it is property-tested: the people released per
interval never fall short of the target and never exceed it by more than the rounding a tier
boundary costs.

**Why the set is written before the election.** The seal write names the set by nonce; a reader
that finds the nonce must find the chunks. Writing them first means a winning seal never points
at something that does not exist yet. A losing double-fire deletes its own chunks; if that fails
they are orphans keyed under a nonce nothing names.

**Why a reader without the set refuses.** A sealed event with `D > 0` whose set cannot be loaded
could answer from the primary slot, and every demoted row would silently be un-demoted. The
resolver returns an error instead, `read` answers 503 and `generate_token` refuses with a
retryable status, and the next poll tries the load again.

**Why the rules are deploy-time settings.** The same reason the poll policy is (ADR-0023): a
fairness control that can be flipped from a dashboard mid-event can be flipped by mistake at the
moment it matters most, and the seal fires once. Terraform validates the syntax at plan time; a
rules string that still fails to parse at runtime is logged, recorded in the report, and sealed
past without demotion, because an event that never opens is worse than one that opened without
a control the operator can see did not apply.

**Why count thresholds and nothing cleverer.** Every honest user of one browser release shares
one JA4, so JA4 concentration separates tooling from browsers, not one visitor from another; a
farm on real headless Chrome collapses into the honest population. Address concentration catches
the trivial loop and also catches NAT. The signals are weak individually and the false-positive
rate is unknown until a real event's report exists, which is what `observe` is for. A rule the
operator can read and reason about, with a report that shows what it caught, is worth more here
than a classifier nobody can explain.

## 4. Consequences

- The event item gains `demoted_count`, `demotion_nonce` and `demotion_chunks`; the demotion set
  chunks and a report item join the `Counters` table. `PreQueue` rows are untouched. No new
  Terraform resources.
- `seal_event` needs `dynamodb:Scan` on `PreQueue` and `PutItem`/`DeleteItem` on `Counters`, a
  five-minute timeout and 1 GB of memory (a few words per cohort row plus the interned signal
  values). With no rules it uses none of it.
- The seal's duration becomes a function of the cohort when rules are set — a consistent parallel
  scan of a million rows takes seconds — and of nothing else.
- The scan cannot be scoped to the event: `PreQueue` rows carry no event id and the table name
  does not change when `event_id` does. A cohort wider than the event's own participant count is
  therefore another event's rows, left behind by a stack reused instead of destroyed. The seal
  reports that and demotes nobody rather than refusing to seal, on the same reasoning as the
  unparsable-rules path: an event that never opens is worse than one that opened without a
  control the operator can see, in the report, did not apply.
- Every resolver of a pre-queue row takes the demotion set as an argument. `read` and
  `generate_token` hold one per execution environment, keyed by nonce.
- The controller's `ReleaseInputs` carry `N` and `D`, and its release, no-show measurement and
  expiry cutoff convert between positions and people through `Tiers`. With `D = 0` every
  conversion is the identity and the control law is exactly what it was.
- A demoted visitor's `/queue_num` position is `N + p`, so the waiting page's wait estimate and
  poll interval treat the whole primary range as ahead of them, which overstates the wait by the
  tail's gaps. They are demoted; a pessimistic estimate is the least of it.
- Auditability (design §4.4): the published seed, offsets and cohort size reproduce every primary
  position; the stored group set and each row's telemetry reproduce which rows sit at `N + p`.
- Terraform gains `demotion_rules` and `demotion_mode`, defaulting to off and `observe`.
- Requirement F6.3 is built. The false-positive measurement it calls for still needs a real
  event, which is why nothing is enforced by default (`.kiro/specs/virtual-waiting-room/tasks.md`).
- The WAF signals (#59: Bot Control labels, the anonymous-IP and hosting-provider lists) are
  a later input to the same classification: another attribute on the row, another signal name
  in the rules, the same seal. Nothing here precludes them; nothing here needs them.

## Revision (2026-09-16): the count proxy is not a reliable contamination signal

The §4 claim that "a cohort wider than the event's own participant count is therefore another
event's rows" rests on the premise that *every issued index has a row*. That premise is false:
`assign_position` claims a pre-queue index (`ADD shard_count`) *before* the row write, and both
write outcomes that fail to land leave a **burned index** — the counter is incremented but no
`PreQueue` row exists (`Duplicate` on a `request_id` collision; `Err` on a transient `PutItem`
timeout/throttle/capacity). `N` therefore counts indices *issued*, not rows written, and a clean
event reads `cohort < N`. A foreign row that fills a burned slot does not push `cohort` above
`N`, so the `cohort > N` guard silently misses contamination that fits inside the burned gap and
`seal_event` enforces demotion on another event's rows while recording it as a clean demotion.

The guard now uses two signals instead of `cohort > N`:

- **`cohort > distinct`**: a single event issues each `(shard, local index)` at most once, so a
  slot observed more than once is another event's row duplicating a current one. This is a
  *definite* contamination signal — demotion is withheld and the report says so, exactly as the
  old guard did for the maximally contaminated (`cohort > N`) case.
- **`cohort < N`** (the burned-index regime): burned slots make a clean read indistinguishable
  from one that folded foreign rows into the gap, so contamination cannot be *ruled out*. The
  report flags it rather than recording a clean demotion; enforcement is **not** withheld,
  because burned indices are an expected, throughput-dependent residual and withholding here
  would disable demotion at any real scale.

The colliding half of the residual gap is closed (`cohort > distinct`). The complete fix —
distinguishing current-event rows from foreign rows that fill burned slots — still requires an
event identifier on `PreQueue` rows (or per-event isolation), since the scan carries no event
tag and no proxy built from `cohort`, `N`, or distinct slots can separate the two in that
shape. This is a design change the ADR has not made; until it is, the burned-index regime is
*surfaced* rather than *detected*.
