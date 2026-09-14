# ADR-0029: Demote telemetry groups to a compact tail at the seal

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

**Classify at the seal, over the whole cohort, and demote whole groups to a compact tail.**

`seal_event` takes an operator-configured rule list, `signal:max` entries over `address`, `asn`,
`ja4` and `ua`. When any rule is set it scans the pre-queue after reading the shard counts and
before the seal write, groups the cohort's rows by each ruled signal, and marks every group whose
size exceeds its threshold as demoted. Every row in a demoted group is demoted.

A demoted row does not lose its place; it resolves behind everyone the rules did not touch. The
seal writes `D`, the number of demoted rows, in the same conditional update as the seed, and
starts the live-join sequence at `N + D`. After winning that write, the seal writes a fresh tail
index `d` in `[0, D)` on each demoted row. A row carrying one resolves to `N + PRP(seed, d, D)`;
every other row resolves to `PRP(seed, i, N)` as before. Primary slots are below `N`, tail slots
are in `[N, N + D)`, live joins start at `N + D`: three disjoint ranges, each its own bijection,
so no position is ever held twice.

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

**Why a compact tail and not `N + PRP(seed, i, N)`.** Mapping demoted rows into a second copy of
the whole index space needs no per-row write, but it leaves a tail of `N` positions of which
`N − D` are gaps. The controller advances the cursor by a bounded multiple of the target rate
and treats an unclaimed position like a no-show; at a 1% demotion rate it would spend `N / 2r`
sweeping a tail that is 99% empty, with every live joiner stuck behind it. The compact tail
costs `D` conditional writes at the seal, which is proportional to the problem rather than to
the cohort.

**Why the phase is held during the tail writes.** The seal write is the election: exactly one
run wins it, and only the winner writes tail indices. Between that write and the last tail index
a demoted row would resolve to its primary slot. The waiting page never asks for a position while
the phase is `pre_queue`, so the winner seals with the phase held there and flips it to `active`
in a final guarded update once the tail is written. The window is seconds, and the client
already shows "opening now" past the start time for exactly this kind of gap.

**Why a partial failure is safe.** A demoted row that never receives its `d` — the winner died
part way — keeps its primary slot, which no other row holds. Partial application can misplace a
registration; it cannot duplicate a position. Each tail write is guarded by
`attribute_not_exists(d)`, so nothing overwrites one. A retry of the seal finds the event already
sealed and does nothing; if the phase is still held, the dashboard says so and the operator sets
it to `active` by hand. The event item records how many tail indices landed against `D`.

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

- `PreQueue` rows gain an optional `d`; the event item gains `demoted_count` and
  `demotion_applied`; a report item `EVT#{event_id}#DM` joins the `Counters` table. No new
  Terraform resources.
- `seal_event` needs `dynamodb:Scan` and `dynamodb:UpdateItem` on `PreQueue` and `PutItem` on
  `Counters`, a 15-minute timeout and 2 GB of memory. With no rules it uses none of it.
- The seal's duration becomes a function of the cohort when rules are set: a consistent parallel
  scan of a million rows takes seconds, and the tail writes take on the order of `D / 3,000`
  seconds. The phase flips to `active` when both are done, not at T−0 exactly.
- Auditability (design §4.4) now needs the tail: the published seed, offsets and cohort size
  reproduce every primary position, and the `d` values on the demoted rows reproduce the tail.
  `D` is on the event item; the `d` values are on the rows, as `s` and `l` are.
- The controller sees `D` gaps inside `[0, N)` and corrects for them as it does for no-shows.
  Above roughly half the cohort demoted, the bounded correction cannot keep the target rate; a
  threshold that demotes half a cohort is an operator error the report will show.
- Terraform gains `demotion_rules` and `demotion_mode`, defaulting to off and `observe`.
- Requirement F6.3 is built. The false-positive measurement it calls for still needs a real
  event, which is why nothing is enforced by default (`.kiro/specs/virtual-waiting-room/tasks.md`).
- The WAF signals (#59: Bot Control labels, the anonymous-IP and hosting-provider lists) are
  a later input to the same classification: another attribute on the row, another signal name
  in the rules, the same seal. Nothing here precludes them; nothing here needs them.
