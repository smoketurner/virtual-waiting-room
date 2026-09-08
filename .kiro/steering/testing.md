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
  - **Deterministic** — same `(seed, i, N)` yields the same position across processes.
  - **Seed absence** — no position is computable before the seed is written at T−0.
- **Atomic counter (design §5).** Under concurrent load, the set of issued positions has
  **zero duplicates**; gaps are permitted and their rate is measured, not eliminated.
- **Idempotent join** — repeating a join with the same `request_id` consumes no extra
  position; malformed joins consume none.
- **Admission / session** — a captured admission token cannot be replayed as a session, or
  vice versa; a session survives a second page view without re-queueing.
- **Fail-open** — with the waiting-room API returning 5xx, the origin stays reachable.
- **No-show compensation** — with an injected no-show rate, measured origin arrivals converge
  on the target rate.

## Load validation (Phase 3)

Correctness under scale is proven by the load harness, not unit tests alone: 1M assigned
positions in one write, ≥10K/s (≥40K/s with quotas filed) live joins with zero duplicates,
`/status` origin RPS flat from 10K to 1M waiters, event isolation. Pre-warm and raise quotas
*before* testing the live-join path or the test measures throttling, not the design.

## Admin UI

`askama` template unit tests plus a rendered-HTML snapshot / accessibility check. Verify core
operator actions work with JavaScript disabled and that no React/SPA bundle is emitted.
