# ADR-0015: Stripe the pre-queue registration counter across 10 partition keys

**Status:** Accepted

## Context

Pre-queue registration claims a registration index with one `UpdateItem ADD` per visitor
(design §4.1). DynamoDB's write ceiling is **1,000 write capacity units per second per partition
key** — "if your application drives consistently high traffic to a single item, DynamoDB can
deliver throughput up to the partition maximum of 3,000 RCUs and 1,000 WCUs to that single item's
primary key". One counter therefore caps pre-queue registration at ~1,000 registrations/second.

For a 1,000,000-participant cohort that is only safe when the pre-queue window is long enough that
`1,000,000 / window < 1,000/s` — about 17 minutes. A 5-minute countdown at ~3,300/s throttles
against the ceiling. Requirement C1 speaks to *holding* 1,000,000 participants on the countdown
page (content-delivery-network fan-out, genuinely flat), not to the *rate* at which they can
register; the registration rate ceiling was previously unstated.

The design's counter rules (§5.4) forbid sharding a *sequence* counter, because summing shards
cannot yield a unique ordered value. `prequeue_counter` is the exception: the permutation
`PRP(seed, i, N)` needs each registration index `i` to be **unique and within `[0, N)`**. It does
not need indices to arrive in order, and it uses contiguity only for domain packing, never for
ordering. That distinction is what makes striping this counter safe when striping the others is
not.

## Decision

Stripe `prequeue_counter` across a fixed **K = 10** shards, by design, for every deployment.

**Each shard is its own item**, keyed `EVT#{event_id}#PQ#{shard}` and holding a single attribute
`n`. The event's own item is `EVT#{event_id}`. `arrivals` is striped the same way,
`EVT#{event_id}#AR#{shard}`. Every key is built in one place (`wr_common::expr`) rather than at
each call site, and `event_id` may not contain `#`, or one event's shard key could collide with
another event's item.

- **Registration.** Each request during the pre-queue hashes to a shard
  `s = hash(request_id) % 10`. `assign_position` groups a batch's requests by shard and issues one
  `ADD n :count` / `ALL_NEW` per non-empty shard group — one round trip claims a whole group's
  **local indices** within that shard, not one round trip per registrant — then writes
  `PreQueue {r, s, l, t}` per request: the shard `s` and the local index `l`, never a global index.
  Ten partition keys are ten budgets, so the ceiling rises to ~10,000 registrations/second.
- **Seal (T−0).** The seal gathers the 10 shard counts, computes **prefix offsets**
  `offset[s] = Σ counts[0..s)`, sets `participant_count = Σ counts` (= N), and stores the 10
  offsets on the `Counters` item — in the same conditional write that sets `shuffle_seed` and
  `phase = active`. Still one write; still atomic.
- **Read.** A visitor's **global registration index** is `i = offset[s] + l`, reconstructed on read
  from the offsets published in `/status`. Because the shards partition the cohort and the offsets
  are a prefix sum, the set of global indices is exactly the contiguous range `[0, N)` — the
  permutation domain is unchanged. `queue_position = PRP(seed, i, N)` as before.

`queue_counter` and `serving_counter` stay on the event's own item. They are sequences whose
ordering is the point, and they are low-rate: one claim per ingest batch, one advance per
controller pass.

### Why separate items rather than ten attributes on one

Striping across attribute names on a single item — `prequeue_counter#0`..`#9` — distributes
nothing, because the ceiling is enforced per partition key. Ten attributes on one item share one
1,000-writes-per-second budget.

It is worse than neutral. `UpdateItem` is billed on the size of the whole item: "even if you update
a subset of the item's attributes, `UpdateItem` will still consume the full amount of provisioned
throughput", rounded up to the next 1 KB. Attribute names count toward that size, so twenty long
names on the shared item would make every write to `Counters` more expensive — including the
`queue_counter` claims and every controller pass — while distributing nothing. It is also why the
shard attribute is the single letter `n`.

## Consequences

- Pre-queue registration throughput rises from ~1,000/s to ~10,000/s with no window-length
  assumption. The "window must exceed ~17 minutes for 1M" caveat is removed.
- **Contiguity is preserved where it matters.** The global index space stays exactly `[0, N)` where
  `N` is the number of local indices *issued* (Σ shard counts), so the Feistel-plus-cycle-walking
  permutation (ADR-0002) is unaffected — same domain, same bijection, same audit contract. `N`
  counts issued indices, **not** confirmed participants: an index whose `PreQueue` write failed
  after the counter incremented is a burned slot, so `PRP` maps it to a position no one claims.
  This is the gaps-permitted property (F2.3), already true of a single counter; striping inherits
  it rather than introducing it. A burned pre-queue slot behaves like a live-join gap — the serving
  counter advances past an unclaimed position and the no-show controller (§7) absorbs it.
- **Shard by `hash(request_id) % 10`, not round-robin.** A retried join hashes to the same shard, so
  the `attribute_not_exists(r)` conditional rejects the duplicate and consumes no index —
  idempotency (F2.5) is preserved. UUIDv7 hashed is uniform mod 10, so shards stay balanced.
- `PreQueue` stores `{r, s, l, t}`. The global index is derived, never stored, so it cannot disagree
  with the offsets.
- **The read side pays a small cost.** The seal and the controller each gather their ten shards with
  one `BatchGetItem` rather than reading attributes from an item they had already fetched. Both
  treat an incomplete batch as a failure rather than a zero, because a missed shard would
  under-count the cohort or over-release admission.
- The visitor read path gains one addition (`offset[s] + l`) and reads 10 offsets from `/status`,
  which it already fetches — no extra input/output.
- Cost against the resource budget (N6) is zero: shards are items in an existing table, not new
  tables or Terraform resources. Each is small enough that an increment always costs exactly one
  write unit, rather than the size of a growing shared item.
- **Auditability is preserved.** A third party recomputes every position from the published seed,
  participant count, per-shard offsets, and the `(request_id, s, l)` tuples in `PreQueue`.
- K is fixed at 10, matching the `arrivals` shard count. A single knob is not exposed: 10,000/s
  covers the stated 1,000,000-participant target across any plausible window, and an unused
  configuration variable is attack surface and cognitive load for no active need.

## Failure modes

- **Straggler join racing the seal.** The seal reads the 10 shard counts, then writes seed +
  offsets + `active` in one conditional `UpdateItem`. A registration in flight can claim a local
  index in a shard *after* the seal read that shard's count, so its local index is beyond the range
  the seal counted for that shard. The straggler test MUST be **per shard**, not a global
  `i ≥ participant_count`: a shard's own issued count is `offset[s+1] - offset[s]` (or
  `N - offset[s]` for the last shard), and a local index at or past it is the straggler, regardless
  of where the reconstructed global index lands. A global `i ≥ N` test is not equivalent — an
  over-count on an interior shard can reconstruct to an `i` that still falls inside `[0, N)`,
  because that index belongs to a *later* shard, and resolving it as pre-queue hands two visitors
  the same position. `/queue_num` falls through to the straggler's `Positions` row rather than
  calling `PRP` out of range — answering with that row's live-join position, or 404 when none has
  landed yet so the client recovers by re-joining; `assign_position` applies the same check once its
  own writes land, to catch the case where the seal lands mid-batch. This degrades a straggler to
  exactly the live joiner it would have been a moment later, and keeps the seal a single write.
  (This race also exists for a single counter racing `participant_count`; it is not introduced by
  striping.)
- **Torn read at T−0 is impossible.** Seed, count, offsets and phase are set in one atomic
  single-item write, so a reader sees either the pre-`active` state (countdown) or all four
  together — never offsets without a count.
