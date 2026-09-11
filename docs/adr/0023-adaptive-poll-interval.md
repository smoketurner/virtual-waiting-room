# ADR-0023: The client's poll interval scales with distance to the front

**Status:** Accepted

## Context

`waiting.js` polled `/status` and `/queue_num` at a fixed interval (5 s, plus up to 1.5 s of
jitter) regardless of how far a visitor was from being admitted. A visitor 900,000 deep in a
million-person queue and a visitor about to be let through polled at the same rate, even though
the first one's position could not possibly change on any timescale under minutes. `docs/DESIGN.md`
§12 modelled this as the dominant cost driver in the whole design: it multiplies CloudFront, WAF
and Bot Control charges together, and at a full million-visitor event it exceeded the CloudFront
Business plan's request allowance ([#69](https://github.com/smoketurner/virtual-waiting-room/issues/69)).

N10 states the requirement directly: client polling cost must scale with distance to the front,
not with waiting visitors × a fixed interval.

## Decision

### The interval rule

`waiting.js` computes its next poll interval from the wait it can see, clamped to a
floor and ceiling and divided down by a configurable factor:

```
intervalFor(waitSeconds) = clamp(floorMs, ceilingMs, waitSeconds * 1000 / divisor)
```

With no wait to be proportional to — no admission rate published yet, or this visitor's position
not yet known — the client falls back to `floorMs`, never to the ceiling: a client that cannot
tell how far away the front is has no basis to poll less often, and the ceiling exists to save
requests from a visitor who is confidently far away, not to hide a visitor the client knows
nothing about. The full table of which branch applies in which `serving_state` lives in
`crates/read/src/lib.rs`'s and `infra/modules/edge/pages/waiting.js`'s own comments; the
degenerate-to-floor rule is what makes an unset admission rate or an out-of-domain wait safe
rather than silently pinning everyone to the ceiling.

A `paused` event is the one case worth calling out: the poll interval stays computed from the
visitor's own distance to the front rather than jumping to the ceiling outright. The controller's
cursor is frozen while paused, so that distance does not change during the hold, and the
operator's target rate is preserved across a pause, so the estimate is the same one the visitor
had a moment before. A near-front visitor keeps a short interval through a pause; a deep-queue
visitor still gets the ceiling's saving.

### Jitter is proportional, not flat

Jitter is a fraction of the interval being jittered (`interval * (1 + random() * 0.3)`) rather
than a fixed window added to it. A flat window sized for the old 5 s interval would be
negligible spread at the 30 s ceiling and would let a cohort that got out of sync re-correlate;
a proportional one keeps the same relative spread at every interval.

### Terraform, not an admin lever

The floor, ceiling and divisor are three Terraform variables on the `read` Lambda
(`poll_floor_ms`, `poll_ceiling_ms`, `poll_divisor`), published verbatim on `/status`, not a
value the operator sets from the admin console mid-event.

The reason is **not** that Terraform changes reach clients faster — it does not. A Terraform
redeploy is a Lambda environment variable update, and it reaches every waiting client on their
next 1-second `/status` cache miss, exactly as fast as an admin form submission would (both are
one write, read back on the same polled document). The real reasons:

1. **Build cost.** Three `variable` blocks and three environment reads, versus an admin `Store`
   method, an `/admin/poll` route, a form row, an `AdminAction` variant, and the debounce/audit
   surface that comes with every other mutating admin action — for a value nobody has a reason to
   turn mid-event.
2. **Zero Terraform resources against the N6 ceiling** (`core` must stay at or under 80 managed
   resources): three variables and three environment entries cost nothing there, where an admin
   route and its supporting `Store` method would.
3. **Criterion 4 of #69 (operator-configurable and published) is satisfied without it.** Deploy-time
   Terraform published on `/status` is exactly that: the same place the operator already sets
   `seal_start_time`, served on the same document as `target_rate`.

Changing the policy mid-event, if ever needed, is safe but not useful enough to build a lever
for: a change in interval *length* does not collapse phase offsets (see Consequences), so it
creates no herd — there is just no operational reason to reach for it while an event is running.

### Page Visibility: stop while hidden, catch up on return

A hidden tab costs polling requests and shows the visitor nothing. Browsers throttle a hidden
tab's timers only after several minutes and only some of them do it at all, so throttling alone
does not capture the saving; the client stops its own timer on `visibilitychange` while
`document.hidden` and restarts on return.

Two guards on the restart, both closing gaps a plain "poll immediately on return" would leave
open:

- **A floor-elapsed rate guard.** Every return to a visible tab polling immediately with no lower
  bound would let a visitor who is flapping between tabs — switching away and back every couple
  of seconds — poll faster than the floor, which is the one bound the whole design is argued to
  respect "by construction". The handler polls immediately only if at least `floorMs` has elapsed
  since the last poll; otherwise it schedules the remainder of that window.
- **An `inFlight` flag.** Firing a poll out of band from `visibilitychange` is the one path that
  can start a second poll chain while the first is still awaiting its own `/status` response — a
  hidden tab with a fetch in flight, then shown. Without a guard, both chains can reach `join()`
  before either has recorded success, taking two positions for one visitor. `inFlight` is set for
  the whole chain, not just the `/status` fetch, and a `visibilitychange` poll request while it is
  set is dropped; the running chain's own next `schedule()` picks up from where it left off.

**No herd from tabs returning together, even for a scheduled drop where many visitors look back
at the same instant.** `/status` is keyed on path alone regardless of how many tabs return
together, so CloudFront still collapses it to about one origin fetch a second. A pre-queue
registrant already holds the "joined" flag from registration, so returning from background never
re-triggers a join. And the one truly per-visitor request, `/queue_num`, is asked once and then
held — the spread that already protects it (`scheduleFirstAsk`) is set from each visitor's own
first *open* poll, whenever that lands, not from when their tab happens to become visible, so a
correlated return does not correlate the one request that cannot collapse.

**Not fixed here:** a visitor whose tab stays hidden longer than the admission grace loses their
place, because a hidden tab now polls zero times rather than the old throttled ~60 s that could
occasionally still catch a narrow grace window. This is a pre-existing gap — the grace (120
positions, ~60 s of wall clock at the correction cap) was already shorter than a realistic
tab-away period — that stopping-while-hidden sharpens rather than creates, and it is arguably the
right direction: today's throttled hidden polling can auto-redeem and navigate while nobody is
watching, which burns an admission slot and records an arrival for a visitor who is not there,
inflating the measured arrival rate and shrinking the controller's no-show correction. Tracked
separately as #97; not attempted in #69.

### Policy parse is all-or-nothing, and last-known-good on withdrawal

`read` publishes `floor_ms`, `ceiling_ms` and `divisor` together or omits the object entirely; it
never sends a partial one. The client mirrors that: any one field missing, non-numeric, or out of
its sanity bounds, or an inverted clamp (`ceiling_ms < floor_ms`), rejects the whole document
rather than repairing it field by field — a document that is wrong in one place is not trustworthy
in the places it looks right, and guessing at a repair (for example clamping ceiling up to floor)
would silently produce a different, unrequested policy.

A client that has never adopted a real policy falls back to a **fixed 5 s interval** —
`floorMs === ceilingMs === 5000, divisor === 1` — reproducing exactly what every client polled at
before this change. A client that **already adopted** a good policy keeps it if a later `/status`
omits or corrupts the field: reverting every already-adaptive client to the fixed interval
mid-event on one malformed response would be a worse failure mode than a stale-but-still-good
policy, and a transient bad read is far more likely than a deliberate withdrawal.

### Jitter and lockstep persistence

Proportional jitter preserves a cohort's relative spread from perfect lockstep as well as the old
flat jitter did (peak/mean of wake-ups 2.4x/1.7x/1.2x at polls 5/11/25, versus 2.5x/1.8x/1.3x
today) — but because the interval itself grows, an accidental lockstep now persists roughly six
minutes (poll 11 lands around 380 s at the ceiling) instead of about one minute today. This is not
a hazard: the only endpoint repeatedly hit is `/status`, which collapses on a path-only key no
matter how correlated the polls are.

## Cost model

`docs/DESIGN.md` §12 carries the full numbers: both the foreground-only upper bound and a
hidden-share sensitivity table, the desktop-Chrome-throttle invariance and the iOS-suspension
counter-case, and the per-component (countdown vs. queue) ratio range. Read it there rather than
duplicating it here — it is a model, not a mechanism, and belongs with the other cost modelling.

## Consequences

- `/status` gains an optional `poll_policy` object (`crates/read`). Absent when Terraform has not
  set one, which a client already treats identically to a fixed 5 s interval.
- Three new Terraform variables on `core`, wired through the `read` Lambda's environment and the
  dev root's variables, at zero net Terraform resources.
- `waiting.js` gains ~90 lines: the clamp/jitter functions, the visibility handler, and the
  `inFlight`/rate-guard state the handler needs. No new dependency and no new network request —
  everything still rides the existing `/status` document.
- New client-side test infrastructure: `infra/modules/edge/tests/waiting.vm.test.js`, loading the
  shipped `waiting.js` under Node's built-in `node:vm` with a stub `document`/`window`/`fetch`,
  run by `node --test` in `.github/workflows/client-ci.yml` and a local prek hook. This is the
  first JavaScript in the repo's test surface; `oxlint`/`oxfmt` are deliberately out of scope for
  now — the shipped file is ES5, written for maximum browser compatibility, and a linter tuned for
  modern syntax would flag most of it. A future pass can add linting once that tradeoff is
  revisited on its own.
- `crates/harness` gains `Polling::Backoff`, a `--target-rate` flag, and a moving `serving_position`
  in its built-in origin stub, so the client-request saving can be measured locally without a
  deployed stack.
