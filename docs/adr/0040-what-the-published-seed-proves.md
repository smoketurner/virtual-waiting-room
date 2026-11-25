# ADR-0040: The published seed proves the arithmetic, not the draw

**Status:** Accepted

## 1. Context

After the open, `/v1/status` publishes `shuffle_seed`, `participant_count` and `prequeue_offsets`,
and the documentation said that with the `(r, s, l)` tuples "anyone can recompute every position".
The requirements call this auditability (F1.5).

That is true and it is less than it sounds
([#249](https://github.com/smoketurner/virtual-waiting-room/issues/249)). The published values let
a third party check that each position is `PRP(seed, offset[s] + l, N)`. They do not show that the
seed was drawn once and honestly. `open_event` draws it from its own CSPRNG at T−0, after every
registration's index is fixed and with no prior commitment. Whoever controls the deployment could
draw seeds until a favoured index landed at the front, and the published values would look the
same. The `(r, s, l)` tuples are not public either: they are rows in a table the operator owns.

The party an audit is for is someone who does not trust the operator. Against that party the
operator holds every input: the function that draws the seed, the table the registrations are in,
and the item the result is written to.

## 2. Decision

**State what is proved, and do not build a commitment scheme that the operator can also forge.**

The documentation now says the published values make the queue *recomputable*: a visitor can check
their own position from their own `(s, l)`, and the operator, or an auditor the operator gives
table access to, can check all of them. It no longer says they demonstrate that the draw was fair.

No in-account mechanism changes that:

- **Committing to the seed before registration** stops the seed being chosen after the indices are
  known, but the operator who knows the seed early can place chosen registrations at chosen
  indices, since it also controls the index counters.
- **Committing to the registration set before the draw** fixes the indices publicly, but the seed
  is still drawn privately after the commitment is seen.
- **Both together** leave one move: a draw that is not the operator's. That means a public
  randomness beacon, with the round named in advance and the seed derived from the beacon value and
  the registration commitment.

A beacon is the only version that proves anything to an outsider, and it is not adopted now:

- It puts a third-party network call on the open, the one write the whole event waits on. If the
  beacon is unreachable at T−0 the choices are to delay the open or to fall back to a private draw,
  and a fallback the operator can trigger is not a guarantee.
- It is the first component outside the client's account (N3).
- A full audit also needs every visitor to be able to check their own `(s, l)` against the
  commitment, which means publishing a Merkle root over a million rows that the open deliberately
  never reads ([ADR-0002](0002-seeded-permutation-not-materialised-shuffle.md)).

## 3. Consequences

- F1.5 is met as "recomputable", and the requirement text says so. A client whose buyers need proof
  against the operator needs the beacon design above, as a deliberate addition with its own failure
  policy.
- The seed still does not exist before T−0, and the open is still one conditional write that
  cannot be repeated: an honest operator cannot re-roll by accident, and a re-roll by a dishonest
  one requires deleting an attribute, which point-in-time recovery and CloudTrail data events (if
  enabled by the client) would show.
- The frozen wire encoding is unchanged.
