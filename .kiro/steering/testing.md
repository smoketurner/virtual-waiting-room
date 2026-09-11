# Testing

Correctness here is load-bearing: a duplicate position or a non-uniform permutation is a
visible fairness failure at 1,000,000 people. Tests are the primary evidence.

## Discipline

- **Test edges and errors, not just the happy path.** Empty inputs, boundaries, malformed
  payloads, duplicate joins, 5xx retries, missing items. Every error path the code handles
  gets a test that triggers it.
- **Test behavior, not implementation.** If a refactor breaks a test but not the behavior, the
  test was wrong.
- **Verify tests catch failures.** Break the code, confirm the test fails, then fix. Use
  `cargo-mutants` to check this systematically on the counter and permutation.
- **Mock boundaries, not logic.** Only mock what is slow, non-deterministic, or an external
  service. Prefer a trait seam over the AWS SDK so the pipeline runs AWS-free in tests.

## The mechanisms that must be proven

- **Permutation (design §4.3).** Property tests with `proptest`:
  - **Bijective** over the full domain for N up to 10⁶ — no two indices map to the same
    position; every position in `[0, N)` is hit exactly once.
  - **Uniform** — chi-square across deciles within the critical value.
  - **Deterministic** — same `(seed, i, N)` yields the same position across processes. Pin the
    frozen wire encoding (design §4.3) with fixed `(seed, i, N)` → `position` vectors so any
    drift in field width, byte order, or the HMAC key/message split fails the build.
  - **Seed absence** — no position is computable before the seed is written at T−0.
  - **Contiguous assembly** — across the 10 pre-queue shards, the assembled global index space
    is exactly `[0, N)` with `N` = Σ shard counts.
  - **Burned slot** — with an injected registration-write failure (counter incremented, no
    `PreQueue` row), the space stays contiguous, `PRP` stays bijective, and the burned index
    resolves to a position that maps to no one (absorbed like a live-join gap, F2.3).
  - **Straggler race** — a join that raced the seal claims a local index at or past its own
    shard's issued count (a **per-shard** test, not a global `i ≥ participant_count`: an
    over-count on a shard that is not the last one can still reconstruct to an `i` inside
    `[0, N)`, because that index belongs to a later shard); `/queue_num` never calls `PRP` out
    of domain for it, falling through to the `Positions` row instead (a live-join position if
    one has landed, 404 — recoverable by re-join — if not).
- **Atomic counter (design §5).** Under concurrent load, the set of issued positions has
  **zero duplicates**; gaps are permitted and their rate is measured, not eliminated.
- **Idempotent join** — repeating a join with the same `request_id` consumes no extra
  position; malformed joins consume none.
- **Admission / session** — a captured admission token cannot be replayed as a session, or
  vice versa; a session survives a second page view without re-queueing.
- **Cross-language credential and rule conformance (ADR-0021, issue #71)** — a frozen wire
  contract with the edge, proven by generated vectors rather than hand-written assertions on
  either side. `crates/wr-common/tests/vectors.rs` generates
  `crates/wr-common/tests/vectors/session.json` (positive credentials minted by the real
  `Session::sign`, negatives hand-encoded independently of `wr_common`'s private wire helpers,
  and `(rule, request) → bool` vectors) and self-checks it against the real Rust implementation;
  `infra/modules/edge/tests/gate.conformance.test.js` loads the **shipped**
  `gate.js.tftpl` under `node:vm` and checks the same vectors against the JS implementation.
  Rule vectors are the only thing that proves rule agreement — `eq_ignore_ascii_case` (Rust) and
  `toLowerCase()` (JS) diverge on non-ASCII input even though both read the same wire encoding.
  Regenerate with `cargo test -p wr-common -- --ignored regenerate_vectors` after a wire-format
  change; the committed file drifting from the generator fails CI. `node:vm` exercises Node's
  `Buffer`/HMAC/base64url, not CloudFront's — it is a proxy for what
  `scripts/spike_edge_gate.py` proved against a real function, not a replacement for it.
- **Edge gate decision-tree branches (ADR-0021)** — `infra/modules/edge/tests/gate.decision-tree.test.js`
  covers every branch against the shipped function: dormancy (`r: []`), `enforce_from` pending,
  `fail_open_until` active and lapsed, each refusal reason (`none`/`signature`/`event`/`expired`),
  XHR-versus-navigation refusal shaping, spoofed `x-wr-gate*` headers stripped at entry, and the
  throw-and-pass-through path (an unrecognised config version, a failed KeyValueStore read).
- **No-show compensation** — with an injected no-show rate, measured origin arrivals converge
  on the target rate.

## Not currently provable, and why

- **Automatic fail-open on backend-unreachable (#58)** — the *mechanism* is proven (a
  `fail_open_until` epoch the gate evaluates against its own clock, `resolve()` producing
  `AdmissionControl::FailOpen`, `apply_fail_open` writing both stores in the safe order), but
  nothing trips it automatically. Fail-open still depends on a human (or a watchdog that does
  not exist yet) noticing the backend is down and calling `/admin/fail_open`. Do not write a
  test that asserts DynamoDB-unreachable automatically flips the gate open — nothing does that.

## To prove when the corresponding gap closes

Listed so they are not rediscovered late. Each is unbuilt today; do not write the test before
the mechanism exists.

- **One position per identity** (#59) — N registrations under one verified identifier yield one
  position; a pre-queue registration classified as a bot at join is mitigated at seal.
- **Concurrency control** (#65) — with injected session durations an order of magnitude apart,
  measured active sessions converge on the ceiling in both cases.
- **Revocation** (#63) — no design exists yet (ADR-0021 §5.2); do not write a test against a
  mechanism that has not been chosen.

## Load validation (Phase 3)

Correctness under scale is proven by the load harness, not unit tests alone: 1M assigned
positions in one write, ≥10K/s (≥40K/s with quotas filed) live joins with zero duplicates,
`/status` origin RPS flat from 10K to 1M waiters, event isolation. Pre-warm and raise quotas
*before* testing the live-join path or the test measures throttling, not the design.

## Admin UI

`askama` template unit tests plus a rendered-HTML snapshot / accessibility check. Verify core
operator actions work with JavaScript disabled and that no React/SPA bundle is emitted.
