# ADR-0002: Derive queue position from a seeded permutation

**Status:** Accepted
**Supersedes:** the materialised-shuffle approach in the first draft of ADR-0001

## Context

Randomized assignment (ADR-0001) requires a bijection from registration index to queue
position. Materialising it means shuffling the participant list and writing one row per
participant: 1,000,000 `PutItem` calls, a ~287-page `Scan` to read the participant set, five
minutes of sustained writing, and a window during which some participants hold positions and
others do not.

Two size optimizations were evaluated and rejected:

- **Shorter attribute names.** DynamoDB bills writes in 1 KB units rounded up with a one
  write capacity unit minimum. A 300-byte row and a 45-byte row both cost 1 WCU. Saving: zero.
- **Packing many mappings into one item.** Reduces writes to ~200 blocks, but positions
  become non-indexable. A visitor must fetch a 98 KB block to read one integer: ~100 GB of
  egress across 1,000,000 visitors.

## Decision

Do not store the mapping. Define queue order as a keyed pseudorandom permutation over
`[0, N)` and compute each position on read.

Construction: 4-round balanced Feistel network over the smallest power-of-four domain ≥ N,
`HMAC-SHA256(seed, round || x)` as the round function, cycle-walking to restrict output to
`[0, N)`. This is the standard small-domain construction underlying format-preserving
encryption; NIST SP 800-38G specifies FF1 on the same principle.

## Consequences

| | Materialised | Permutation |
|---|---|---|
| Writes at T−0 | 1,000,000 | 1 |
| `Scan` of participant set | ~287 pages | none |
| Assignment window | 5 min sustained | single conditional write |
| Partial-failure mode | possible | impossible |
| Pre-warming for assignment | required | not required |
| Read cost | 1 `GetItem` | 1 `GetItem` + ~4 HMAC evaluations |

- Verified bijective: 200,000 samples at N=1,000,000 produced 200,000 distinct positions.
- Verified uniform: N=10,000 across 10 deciles gave exactly 1,000 each, χ² = 0.0 against a
  16.9 critical value at p=0.05, df=9.
- Expected cycle-walk iterations = domain/N, bounded by 4 and equal to 1.05 at N=1,000,000.
- **The seed becomes security-relevant.** It does not exist before T−0, so no participant can
  compute their position early or select a favourable registration index. With a materialised
  shuffle the seed is equally secret but the secrecy is incidental; here it is load-bearing.
- Auditability improves: a third party with the seed, participant count and registration
  indices recomputes every position without trusting that stored rows were unaltered.
