# ADR-0017: Andon-cord emergency controls for the admin plane

**Status:** Accepted

## Context

The admin dashboard (ADR-0014, ADR-0016) can set phase, admission rate, and the broadcast
message, and force maintenance. That is enough to run an event, but it is not an *andon cord*:
a set of controls an operator can pull the instant something goes wrong, designed so the safe
action is fast and unmistakable and the dangerous action is guarded.

"Off the rails" for a waiting room has specific shapes:

- **Admitting too fast** — the origin is melting because `target_rate` is too high (bad value,
  or the origin's capacity dropped).
- **Admitting into a dead origin** — the origin is down; every admitted visitor hits an error,
  and the queue is draining people into a wall.
- **The waiting room itself is the problem** — the room is misbehaving (bad deploy, stuck
  controller) and is *itself* the outage. The system's founding principle is to **fail open**
  here (ADR-0009), but nothing lets an operator force that on demand.
- **Fat-finger under pressure** — during an incident the operator is stressed; a one-click
  irreversible reset can turn a small problem into a bigger one.

Today the only emergency lever is **Force maintenance** — one unguarded click that slams the
phase to `Maintenance`. It is blunt (tears down the serving state), one-way, and has no
"hold, don't destroy" middle ground. There is no record of who pulled it.

## Decision

Add a graded set of andon-cord controls to the admin plane, ordered from *frictionless and
reversible* to *powerful and guarded*. All state lives on the single `Counters` item, applied
as guarded conditional `UpdateItem`s (the existing pattern); the controls are surfaced as a
distinct, visually-emphatic **Emergency** card on the dashboard.

### 1. Pause admission (the primary cord) — frictionless, reversible

- A single large **PAUSE ADMISSION** control that sets an explicit `admission_paused` flag on
  `Counters` (not merely `target_rate = 0`, so the operator's configured rate is preserved and
  restored on resume). While paused: the queue keeps forming and positions keep their meaning,
  but the controller admits no one.
- One click, no confirmation — pulling the cord must be instant. **Resume** restores the prior
  rate.
- New attribute: `admission_paused: bool`. The outflow controller (future) treats
  `paused || rate == 0` as "admit nobody"; until the controller exists the flag is authoritative
  and honestly labeled as taking effect when the controller ships.

### 2. Drain vs. hard stop — two distinct levers

- **Pause** (above) holds new admissions while preserving the queue.
- **Force maintenance** (existing) is the hard stop: phase → `Maintenance`, everything halts.
  These are deliberately separate so the operator does not reach for the destructive one when
  the reversible one suffices.

### 3. Break-glass fail-open — the ultimate cord

- A **FORCE FAIL-OPEN** control that sets a `force_fail_open` flag on `Counters`. The authorizer
  (ADR-0009 plane, deferred) reads it and admits every visitor straight to the origin with the
  time-limited bypass cookie — the waiting room takes itself out of the path. This is the answer
  to "the room is the outage."
- **Guarded** (see §5): it sends unmetered traffic to the origin, so it is a deliberate act.
- Because the authorizer is not in the MVP, this control is **rendered but honestly labeled as
  pending the authorizer** (no phantom feature): it writes the flag, and the flag does nothing
  until the authorizer reads it. The write + label ship now; the enforcement ships with the
  authorizer.

### 4. Guardrails on the values, not just the buttons

- **Rate ceiling.** `apply_rate` already rejects `0`; add an upper sanity bound
  (`target_rate <= max_admission_rate`, a per-deployment config) so a fat-fingered `900000`
  cannot be written. A ceiling is itself an andon guard.

### 5. Confirmation on the irreversible / high-blast-radius actions

- **Pause / Resume**: no confirmation — pulling the cord is frictionless by design.
- **Force maintenance** and **Force fail-open**: a confirmation step (a two-step submit, or
  typing the `event_id`) so a stressed mis-click does not escalate the incident. The asymmetry
  is the point: safe actions are frictionless, dangerous ones are guarded.

### 6. Audit trail — who pulled the cord

- Every admin action records the actor and time: the OIDC session (ADR-0016) already gives a
  verified operator email. Write `last_action`, `last_action_by`, `last_action_at` onto
  `Counters` on each mutating call, and surface "last changed by X at T" on the dashboard.
  During an incident the first question is "what changed and who changed it" — the andon cord is
  only trustworthy if pulls are attributable. This also covers the existing (currently
  unlogged) actions.

## Consequences

- The operator gets a real emergency-stop: a fast, reversible **Pause** that does not destroy
  queue state, distinct from the destructive maintenance stop, plus the break-glass fail-open
  for when the room itself is the fault.
- **New `Counters` attributes**: `admission_paused`, `force_fail_open`, `last_action*`. Small,
  and they ride the existing single-item conditional-write model — no new tables (N6).
- **Two flags are ahead of their enforcers.** `admission_paused` is authoritative only once the
  outflow controller reads it; `force_fail_open` only once the authorizer reads it. Both are
  written now and labeled as pending — deliberately not faked (conventions.md: no phantom
  features). This is a real seam, recorded here so a future reader knows the flag predates its
  reader by design.
- **Confirmation is a usability tension.** Guarding the destructive actions adds a click exactly
  when the operator is under stress; the mitigation is that the *frequent emergency* action
  (Pause) is unguarded and the *rare destructive* ones are guarded, so the friction lands where
  the blast radius is largest.
- The audit fields make every pull attributable but are last-writer-wins single values, not a
  full history; a full audit log is a later concern (CloudWatch/EMF already captures per-request
  actor via the session). Recorded as a known limitation.
- Additive to ADR-0014/0016; depends on ADR-0009's authorizer for §3 enforcement and the future
  outflow controller for §1 enforcement.
