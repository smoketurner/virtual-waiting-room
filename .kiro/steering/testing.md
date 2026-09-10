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
  - **Straggler race** — a join that raced the seal reconstructs `i ≥ participant_count`;
    `/queue_num` returns a live-join position and never calls `PRP` out of domain.
- **Atomic counter (design §5).** Under concurrent load, the set of issued positions has
  **zero duplicates**; gaps are permitted and their rate is measured, not eliminated.
- **Idempotent join** — repeating a join with the same `request_id` consumes no extra
  position; malformed joins consume none.
- **Admission / session** — a captured admission token cannot be replayed as a session, or
  vice versa; a session survives a second page view without re-queueing.
- **CloudFront cookie encoding (ADR-0020)** — a second frozen wire contract, this one with the
  edge rather than with an auditor. Pin it: custom-policy cookie set (`CloudFront-Policy`,
  never `CloudFront-Expires`), whitespace-free policy JSON, the `+/=` → `-~_` base64 alphabet,
  and `CloudFront-Hash-Algorithm=SHA256` present. Any drift presents as every admitted visitor
  getting a 403, which no unit test would otherwise catch.
- **No-show compensation** — with an injected no-show rate, measured origin arrivals converge
  on the target rate.

## Not currently provable, and why

- **Fail-open** — "with the waiting-room API returning 5xx, the origin stays reachable" holds
  for the authorizer gate only. The CloudFront gate fails closed by construction (#58), so this
  test must be scoped to the authorizer until that issue resolves. Do not write a test that
  passes by asserting the weaker behaviour and calling it fail-open.

## To prove when the corresponding gap closes

Listed so they are not rediscovered late. Each is unbuilt today; do not write the test before
the mechanism exists.

- **One position per identity** (#59) — N registrations under one verified identifier yield one
  position; a pre-queue registration classified as a bot at join is mitigated at seal.
- **Credential scope** (#61) — cookies minted for one event are refused on another event's
  behaviour in the same distribution.
- **Concurrency control** (#65) — with injected session durations an order of magnitude apart,
  measured active sessions converge on the ceiling in both cases.

## Load validation (Phase 3)

Correctness under scale is proven by the load harness, not unit tests alone: 1M assigned
positions in one write, ≥10K/s (≥40K/s with quotas filed) live joins with zero duplicates,
`/status` origin RPS flat from 10K to 1M waiters, event isolation. Pre-warm and raise quotas
*before* testing the live-join path or the test measures throttling, not the design.

## Admin UI

`askama` template unit tests plus a rendered-HTML snapshot / accessibility check. Verify core
operator actions work with JavaScript disabled and that no React/SPA bundle is emitted.
