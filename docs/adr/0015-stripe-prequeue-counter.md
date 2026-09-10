# ADR-0015: Stripe the pre-queue registration counter across 10 shards

**Status:** Accepted, amended — the original striping did not work (see Amendment)

## Context

Pre-queue registration claims a registration index from `prequeue_counter` with one
`UpdateItem ADD` per visitor (design §4.1). A single DynamoDB item is limited to 1,000 write
capacity units per second (WCU/s) — the single-partition write ceiling. So one counter item
caps pre-queue registration at ~1,000 registrations/second.

For a 1,000,000-participant cohort this is only safe when the pre-queue window is long enough
that `1,000,000 / window < 1,000/s` — about 17 minutes. A shorter window (a 5-minute countdown
at ~3,300/s) throttles registration against the single-item ceiling. Requirement C1 speaks to
*holding* 1,000,000 participants on the countdown page (pure content-delivery-network fan-out,
genuinely flat), not to the *rate* at which they can register; the registration rate ceiling
was previously unstated.

The design's counter rules (§5.4) forbid sharding a *sequence* counter because summing shards
cannot yield a unique ordered value. But `prequeue_counter` is a special case: the permutation
`PRP(seed, i, N)` needs each registration index `i` to be **unique and within `[0, N)`** — it
does **not** need indices to arrive in order, and it does not use contiguity for ordering, only
for domain packing. That distinction is what makes striping this particular counter safe when
striping the others is not.

## Decision

Stripe `prequeue_counter` across a fixed **K = 10** shards, by design, for every deployment.

- **Registration.** Each `POST /join` during the pre-queue picks a shard
  `s = hash(request_id) % 10`, does `ADD prequeue_counter#s :1` / `ALL_NEW` to claim a
  **local index** within that shard, and writes `PreQueue {r, s, l, t}` — the shard `s` and
  the local index `l`, not a global index. Ceiling rises to ~10,000 registrations/second.
- **Seal (T−0).** The seal `UpdateItem` reads the 10 shard counts, computes **prefix offsets**
  `offset[s] = Σ counts[0..s)`, sets `participant_count = Σ counts` (= N), and stores the 10
  offsets on the `Counters` item — all in the same conditional write that sets `shuffle_seed`
  and `phase = active`. Still one write; still atomic.
- **Read.** A visitor's **global registration index** is `i = offset[s] + l`, reconstructed on
  read from the offsets published in `/status`. Because the shards partition the cohort and the
  offsets are a prefix sum, the set of global indices is exactly the contiguous range
  `[0, N)` — the permutation domain is unchanged. `queue_position = PRP(seed, i, N)` as before.

`queue_counter` and `serving_counter` remain single-item sequences (§5.4): they are read as
running values during the event, so a prefix-sum reconciliation at a single sealing moment does
not apply to them.

## Consequences

- Pre-queue registration throughput rises from ~1,000/s to ~10,000/s with no window-length
  assumption. The "window must exceed ~17 minutes for 1M" caveat is removed.
- **Contiguity is preserved where it matters.** The global index space stays exactly `[0, N)`
  where `N` is the number of local indices *issued* (Σ shard counts), so the
  Feistel-plus-cycle-walking permutation (ADR-0002) is unaffected — same domain, same
  bijection, same audit contract. `N` counts issued indices, **not** confirmed participants:
  an index whose `PreQueue` write failed after the counter incremented is a burned slot, so
  `PRP` maps it to a position that no one claims. This is the gaps-permitted property (F2.3),
  already true of a single counter; striping inherits it rather than introducing it. A burned
  pre-queue slot behaves like a live-join gap — the serving counter advances past an unclaimed
  position and the no-show controller (§7) absorbs it.
- **Shard by `hash(request_id) % 10`, not round-robin.** A retried join hashes to the same
  shard, so the `attribute_not_exists(r)` conditional rejects the duplicate and consumes no
  index — idempotency (F2.5) is preserved. UUIDv7 hashed is uniform mod 10, so shards stay
  balanced.
- `PreQueue` stores `{r, s, l, t}` instead of `{r, i, t}`. The global index is derived, never
  stored, so it cannot disagree with the offsets.
- The read path gains one addition (`offset[s] + l`) and reads 10 offsets from `/status`, which
  it already fetches — no extra input/output.
- Cost against the resource budget (N6) is zero: the shards are attributes on the existing
  `Counters` item (`prequeue_counter#0`–`#9`), not new tables or resources.
- **Auditability is preserved.** A third party recomputes every position from the published
  seed, participant count, per-shard offsets, and the `(request_id, s, l)` tuples in `PreQueue`.
- K is fixed at 10, matching the `arrivals` shard count. A single knob is not exposed: 10,000/s
  covers the stated 1,000,000-participant target across any plausible window, and an unused
  configuration variable is attack surface and cognitive load for no active need.

## Failure modes

- **Straggler join racing the seal.** The seal reads the 10 shard counts, then writes
  seed + offsets + `active` in one conditional `UpdateItem`. A `POST /join` in flight can
  increment a shard *after* the seal read it, producing a local index beyond the counted range,
  so its reconstructed `i ≥ participant_count` falls outside the permutation domain. `/queue_num`
  MUST treat any reconstructed `i ≥ participant_count` as "registered too late" and return a
  live-join position (via `queue_counter`, behind the whole pre-queue cohort) rather than
  calling `PRP` out of range. This degrades a straggler to exactly the live joiner it would have
  been a moment later, and keeps the seal a single write. (This race also exists for a single
  counter racing `participant_count`; it is not introduced by striping.)
- **Torn read at T−0 is impossible.** Seed, count, offsets and phase are set in one atomic
  single-item write, so a reader sees either the pre-`active` state (countdown) or all four
  together — never offsets without a count.


## Amendment: striping by attribute name distributes nothing

The decision above was implemented by striping across ten *attribute names*
(`prequeue_counter#0`..`prequeue_counter#9`) on the single `Counters` item. That
does not do what this record claims.

`DynamoDB` enforces its write ceiling per **partition key**, not per attribute:
"If your application drives consistently high traffic to a single item… DynamoDB
can deliver throughput up to the partition maximum of 3,000 RCUs and 1,000 WCUs
to that single item's primary key." Ten attributes on one item share one 1,000
writes-per-second budget. The stripe count bought nothing.

It was worse than neutral. `UpdateItem` is billed on the size of the whole item:
"Even if you update a subset of the item's attributes, `UpdateItem` will still
consume the full amount of provisioned throughput", rounded up to the next 1 KB.
Attribute names count toward that size, and `prequeue_counter#0` is eighteen
bytes of name before any value. Twenty such attributes — the pre-queue shards
plus the arrivals shards — made every write to `Counters` more expensive,
including the `queue_counter` claims and every controller pass, while
distributing nothing.

The same mistake was load-bearing at the target scale. At 10k joins per second
the arrivals counter alone runs at the admission rate, and it shared a budget
with the live-join sequence on the same item.

**The striping is now across partition keys.** Each shard is its own item, keyed
`{event_id}#pq#{shard}` and `{event_id}#ar#{shard}`, holding a single attribute
`n`. Ten shards are ten partition keys and ten budgets, and each item is small
enough that an increment always costs exactly one write unit rather than the
size of a growing shared item.

`queue_counter` and `serving_counter` stay on the event's own item. They are
sequences whose ordering is the point, and they are low-rate: one claim per
ingest batch, one advance per controller pass.

The cost is on the read side, and it is small: the seal and the controller each
gather their ten shards with one `BatchGetItem` instead of reading attributes
from an item they had already fetched. Both treat an incomplete batch as a
failure rather than a zero, because a missed shard would under-count the cohort
or over-release admission.
