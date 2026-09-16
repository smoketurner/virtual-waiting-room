# DynamoDB

Amazon DynamoDB is the only datastore in this system. This document covers the table design, every
access pattern, the scaling techniques, and the arithmetic behind each ceiling, read from
`crates/*/src/dynamo.rs`, `crates/wr-common/src/expr.rs`, `crates/wr-common/src/items.rs`,
`crates/admin/src/sessions.rs`, and `infra/modules/core/main.tf`.

[`ARCHITECTURE.md`](./ARCHITECTURE.md) describes the system around it.

Throughout: WCU is a write capacity unit, RCU a read capacity unit, WRU a write request unit, RRU
a read request unit. On-demand tables bill in request units; the partition-level ceilings are
expressed in capacity units. A write of 1 KB or less costs 1 unit. An eventually consistent read
of 4 KB or less costs 0.5 units. A strongly consistent read of the same item costs 1 unit.

---

## 1. The four tables

All four are `PAY_PER_REQUEST` with point-in-time recovery enabled. None has a sort key. None has
a global or local secondary index. No table has streams enabled, and none declares
`server_side_encryption` or `deletion_protection`.

| Table | Partition key | Holds | TTL attribute |
|---|---|---|---|
| `Counters` | `event_id` (S) | The event item, 10 pre-queue shard items, 10 arrival shard items | none |
| `PreQueue` | `r` (S) | One registration per pre-queue visitor | none |
| `Positions` | `request_id` (S) | One row per live joiner | `ttl` |
| `Tokens` | `request_id` (S) | Admission reservations, operator sessions, pending logins | `ttl` |

Only key attributes are declared in Terraform. DynamoDB needs nothing else at create time, and
every other attribute is schemaless.

### 1.1 `Counters` holds five kinds of item

```
EVT#{event_id}            the event
EVT#{event_id}#PQ#{0..9}  pre-queue registration shards
EVT#{event_id}#AR#{0..9}  arrival shards
```

The event item carries, by writer:

| Attribute | Written by | Meaning |
|---|---|---|
| `queue_counter` | `open_event`, `assign_position` | Live-join position sequence; set to `N` at the open |
| `serving_counter` | `controller` | Admission cursor, exclusive |
| `last_serving_counter`, `last_arrivals_total`, `no_show_rate` | `controller` | Measurement state carried across passes |
| `phase` | `open_event`, `admin` | `idle` / `pre_queue` / `active` / `post_event` / `maintenance` |
| `admission_control` | `admin` | `open` / `paused` |
| `fail_open_until` | `admin` | Epoch-seconds break-glass deadline; absent or `0` means not engaged |
| `target_rate` | `admin` | Visitors **per second** |
| `shuffle_seed` | `open_event` | 256-bit permutation key (B), written once |
| `participant_count` | `open_event` | Cohort size `N` |
| `prequeue_offsets` | `open_event` | Ten prefix offsets (L) |
| `message` | `admin` | Operator broadcast |
| `last_action`, `last_action_by`, `last_action_at`, `last_action_epoch_ms` | `admin` | Audit trail and debounce guard |

A shard item carries exactly two attributes: `s`, its own index, and `n`, its count. It records
its own index so a reader that fetched ten shards in one batch knows which is which without
taking the key apart.

### 1.2 Keys are tagged only where a table holds more than one kind of item

`Counters` holds the event plus its shards, so every key is tagged. `Tokens` holds three kinds —
`SESS#` for operator sessions and `PKCE#` for pending logins — so
every key there is tagged too. Without the tag, a session id and an admission reservation for the
same string would be one row. A test pins that all three are distinct.

`PreQueue` and `Positions` hold one kind each and take bare identifiers. A tag there disambiguates
nothing and costs bytes in the partition key of every row, of which there is one per visitor.

### 1.3 Every key is built in one place

`wr_common::expr` owns every key builder, and no call site formats a key string. The builders
return the complete primary key map rather than a string, so the key attribute's own name is
written once as well — call sites pass the result straight to `set_key` or to `BatchGetItem`.

The shard builders `assert!(shard < SHARDS)`. An out-of-range shard would write to a key no
reader ever gathers, which loses registrations silently.

### 1.4 Two hazards the tests pin

**An `event_id` containing `#` collides two keys.** `EVT#a#PQ#1` is both event `a`'s first
pre-queue shard and event `a#PQ#1`'s own item. The Terraform variable rejects a `#`, and one event
per deployment puts the collision out of reach.
`a_hash_in_an_event_id_would_collide_two_keys` asserts it.

**No expression string may contain a `#`.** In a DynamoDB expression, `#` opens an
expression-attribute-name placeholder, so an attribute whose name contains one cannot be written
literally. `ADD arrivals#4 :one` parses as the attribute `arrivals` plus an undefined placeholder
`#4`, and DynamoDB rejects the request.

`no_expression_inlines_an_attribute_name_containing_a_hash` asserts the rule across every
expression fragment. Key values are unaffected, because a key is a value and not expression text.

Where a reserved word is unavoidable, the code uses a placeholder properly: `status` is bound to
`#s` wherever a condition names it.

---

## 2. Every access pattern

| Caller | Table | Operation | Consistency | Rate |
|---|---|---|---|---|
| `assign_position` | `Counters` | `GetItem` event item | **Strong** | 1 per SQS batch |
| `assign_position` | `Counters` | `UpdateItem SET s ADD n`, `ALL_NEW`, on a pre-queue shard | — | ≤10 per batch |
| `assign_position` | `PreQueue` | `PutItem` if `attribute_not_exists(r)` | — | 1 per registrant |
| `assign_position` | `Counters` | `UpdateItem ADD queue_counter`, `ALL_NEW` | — | 1 per batch, +1 per straggler fix-up |
| `assign_position` | `Counters` | `GetItem` event item (fix-up re-read) | **Strong** | 1 per pre-queue batch |
| `assign_position` | `Positions` | `PutItem` if `attribute_not_exists(request_id)` | — | 1 per live joiner |
| `open_event` | `Counters` | `BatchGetItem` of ten pre-queue shards | **Strong** | Once per event |
| `open_event` | `Counters` | `UpdateItem` if `attribute_not_exists(shuffle_seed)` | — | Once per event |
| `read` | `Counters` | `GetItem` event item | Eventual, 1 s in-process cache | ≤1/s per execution environment |
| `read` | `PreQueue` | `GetItem` by `r` | Eventual | 1 per `/v1/queue_num` |
| `read` | `Positions` | `GetItem` by `request_id` | Eventual | 1 per `/v1/queue_num` with no `PreQueue` row |
| `generate_token` | `Counters` | `GetItem` event item | **Strong** | 1 per call |
| `generate_token` | `Positions` | `GetItem` by `request_id` | **Strong** | 1 per call |
| `generate_token` | `PreQueue` | `GetItem` by `r` | Eventual | 1 per call with no `Positions` row |
| `generate_token` | `Counters` | `UpdateItem SET s ADD n` on an arrival shard | — | 1 per admission |
| `controller` | `Counters` | `GetItem` event item | **Strong** | 1 per pass |
| `controller` | `Counters` | `BatchGetItem` of ten arrival shards | Eventual | 1 per pass |
| `controller` | `Counters` | `UpdateItem` cursor, guarded | — | 1 per pass |
| `admin` | `Counters` | `GetItem`, five `UpdateItem` forms | Eventual | Operator actions |
| `admin` | `Tokens` | `PutItem`, `GetItem`, `DeleteItem` | Eventual | Operator logins |

There is no `Scan` in the codebase and no `Query` (§11). Every access is a key lookup.

---

## 3. Technique: amortise the sequence across a batch

A position sequence must yield a unique ordered value. Only a single item can do that, and a
single item is capped at 1,000 WCU per second. One `UpdateItem` per joiner would sit ten times
over that ceiling at 10,000 joins per second.

So the batch claims a block:

```rust
let n = valid.len() as u64;
let end = store.claim_block(event_id, n).await?;   // ADD queue_counter :n, ALL_NEW
let start = end.saturating_sub(n).saturating_add(1);
```

One increment per SQS batch of 100. At 10,000 joins per second that is 100 counter writes per
second, 10% of the item ceiling. At 40,000 joins per second it is 400, 40%. The sequence stops
being the binding constraint; the table-level quota binds first.

Batching by 10 costs 1,000 counter writes per second at the same join rate and sits exactly on the
ceiling. Lowering the batch size to chase latency breaks the counter.

`n` is the count of **valid** records, never `records.len()`. Malformed payloads are rejected
before the claim, so they consume no positions.

The arithmetic saturates rather than wrapping. The release profile has no overflow checks, and a
wrapped `start` would hand out positions from the top of the `u64` range. `end >= n` always holds
for a counter that only moves forward, so the guard is against a counter that was reset.

---

## 4. Technique: stripe across partition keys, never attribute names

### 4.1 The ceiling applies per partition key

AWS documents the limit per item's primary key: up to 3,000 RCUs and 1,000 WCUs to a single item.
Ten attributes on one item share one budget. Ten items are ten partition keys and ten budgets.

Each shard is therefore its own item — `EVT#{event_id}#PQ#{s}` and `EVT#{event_id}#AR#{s}` — and
not an attribute on the event item.

`UpdateItem` bills the whole item rounded up to the next kilobyte even when it writes one
attribute, and attribute names count toward that size. Ten shard attributes on the event item
would make every write to it more expensive — including the `queue_counter` claims and every
controller pass — while distributing nothing. An attribute name of the form `prequeue_counter#0`
also cannot be written into an expression at all (§1.4).

### 4.2 Which counters are striped

| Counter | Kind | Striped | Why |
|---|---|---|---|
| `queue_counter` | Sequence | No | Summing shards cannot yield a unique ordered value |
| `serving_counter` | Sequence | No | Same, and it is read as a running value |
| Pre-queue index | Index | Yes, ×10 | The permutation needs indices unique and inside `[0, N)`, not ordered |
| `arrivals` | Statistic | Yes, ×10 | Runs at the admission rate; order carries no information |

Striping a sequence is forbidden. Striping the pre-queue counter is safe because `PRP(seed, i, N)`
needs `i` to be unique and in range and nothing else. The prefix offsets written at the open
reassemble ten shards into exactly `[0, N)`, so the permutation domain is unchanged.

`SHARDS` is `pub const SHARDS: usize = 10` and is not configurable. An unused configuration
variable is attack surface and cognitive load for no active need.

### 4.3 Shard by hash, not round-robin

```rust
pub fn shard_for(request_id: &[u8]) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;   // FNV-1a
    for &byte in request_id {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    usize::try_from(hash % SHARDS as u64).unwrap_or(0)
}
```

A retried join hashes to the same shard, so the `attribute_not_exists(r)` guard rejects it there.
Round-robin would send a retry to a different shard, where the guard would still reject the row
but a second index would already be burned.

The hash need not be cryptographic. It need only spread request ids uniformly modulo 10, and
UUIDv7 identifiers do.

### 4.4 The batched claim moves the ceiling further out

`assign_position` groups a batch by shard and issues one claim per non-empty group. With a batch
of 100 spread over 10 shards, each shard takes one write per batch. At a registration rate of
R per second, each shard sees R/100 writes per second, so a shard reaches 1,000 per second at
R = 100,000. The shard counters are not the binding constraint at this batch size; the 40,000
WRU per second table quota is.

### 4.5 The pre-queue claim errors rather than saturates

```rust
shard_count_after_add.checked_sub(count).ok_or_else(...)
```

The live-join block claim saturates in the same situation. A saturated live-join position is a
gap, which is permitted. A saturated pre-queue local index is a duplicate global index that hands
two visitors the same position.

The function is pulled out of the store method so it is unit-tested directly — the `Store` fake in
`lib.rs` computes the block start a different way, so the real subtraction would otherwise have no
test seam.

---

## 5. Technique: conditional writes carry idempotency and concurrency control

| Write | Guard | What it buys |
|---|---|---|
| `PreQueue` row | `attribute_not_exists(r)` | A duplicate registration consumes no index |
| `Positions` row | `attribute_not_exists(request_id)` | A duplicate join consumes no position |
| Open | `attribute_not_exists(shuffle_seed)` | A double-fire opens exactly once |
| Cursor advance | `attribute_not_exists(serving_counter) OR serving_counter = :expected` | Overlapping controller executions cannot double-advance |
| Admission reservation | `attribute_not_exists(request_id)` | A reservation is taken once |
| Phase change | `phase = :from` | A transition another operator applied is a 409 |
| Rate change | `target_rate = :exp`, or `attribute_not_exists(target_rate)` | Same, for the rate |
| Admission control | `admission_control = :from`, widened to allow absence when `:from` is `open` | Same, and absence reads as `open` |
| Every debounced admin write | `attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms <= :cutoff` | A double-submitted form is a no-op for 2,000 ms |

Forcing maintenance mode is guarded on the expected phase but **not** debounced. An emergency stop
must always apply.

`last_action_epoch_ms` is written from the instant the request was served at, and read back as an
instant. A stored value no instant can hold reads as no stamp at all, so it cannot hold the
debounce window open against every later action.

Every guarded write handles `ConditionalCheckFailedException` by name, and none treats it as an
error. `assign_position` reports a duplicate, `open_event` returns `AlreadyOpen`, the controller
logs the lost race and continues, and `admin` maps it to a conflict for the operator.

These guards are what let ingest run on an SQS standard queue, which delivers at least once with
best-effort ordering. A duplicate fails the condition and costs one rejected write. Ordering does
not matter, because positions come from a counter and not from message sequence.

---

## 6. Technique: derive state instead of storing it

A materialised shuffle for a million participants is a million row writes, and those writes are
not atomic. There would be an interval in which some participants hold positions and others do
not.

The open writes five values in one conditional `UpdateItem`. A reader sees the unopened state or
all five together; a torn read is impossible because it is one item. `Counters::opened()` returns
`Some` only when the seed, the cohort size, and the offsets are all present, and
`opened_is_none_before_every_open_output_is_present` pins that.

Position is then computed on read. The savings compound: a million fewer writes at the scheduled
start, no rows to fall out of sync with the offsets, no storage to reclaim afterwards, and no
stored global index that can disagree with the offsets — because it is not stored anywhere to
disagree.

`Counters::resolve_prequeue` is the one implementation. `read` and `generate_token` both call it,
so two Lambdas cannot interpret one stored row two different ways.

The same discipline applies to `serving_state`, which `/v1/status` publishes: it is derived from
`(phase, admission_control)` on every read and never stored.

Fail-open follows the same rule. `admission_control` stores what the operator chose: `open` or
`paused`. `fail_open_until` stores when the break-glass window ends. Callers match on the
three-valued control that `resolve(stored, until, now)` returns, computed on every read. A
fail-open window therefore expires at the same instant everywhere rather than in one component at
a time.

---

## 7. Technique: pay attention to bytes

`UpdateItem` bills the full item size rounded up to the next kilobyte, including attribute names,
on every write.

A shard item's attributes are `s` and `n`. A verbose name is paid for on every increment, forever,
and nothing queries by attribute name. `a_shard_increment_names_only_short_attributes` asserts the
two names, so a refactor cannot lengthen them quietly. A shard item stays far below 1 KB, so an
increment always costs exactly one write unit rather than the size of a growing shared item.

`PreQueue` uses short names for a different reason. Its attributes are `r`, `s`, `l`, `t`, and the
table is read across the whole cohort during an audit. A row is about 60 bytes.

`Positions` uses readable names — `request_id`, `queue_position`, `entry_time`, `status`, `ttl` —
because it is written once and read a handful of times, not incremented in a hot loop. A row is
about 110 bytes.

`status` is `issued` or `admitted`, and the transition between them is the conditional write that
makes an admission count once (ADR-0033). A live joiner's row starts at `issued`; a pre-queue
member has no row at all until the claim creates one at `admitted`, since their position is
derived from the seed rather than stored. A row already at `admitted` is not refused — the status
governs whether the arrival is counted, not whether the visitor is let in.

`queue_position` and `entry_time` are separate numeric attributes, and
`position_and_entry_time_are_separate_numeric_attributes` pins that. A reader looking for a
timestamp must not parse a position.

---

## 8. Consistency: where the code pays for it

There are exactly five `consistent_read(true)` call sites.

**Strong, because a stale answer is wrong:**

- `assign_position` reading `Counters`. The whole batch's routing decision — pre-queue or live —
  turns on this read, and so does the straggler fix-up.
- `open_event` reading the ten pre-queue shards. The open folds these into the cohort size. A
  registration missed here is a visitor with no position at all.
- `generate_token` reading `Counters`. A visitor at their turn must not be told to keep waiting
  because a replica lagged behind the controller.
- `generate_token` reading `Positions`. The position must be a live claim at the moment the
  cookies are minted.
- `controller` reading `Counters`. The release calculation reads the cursor it is about to
  advance, and the write is guarded on that value.

**Eventual, because a second of staleness costs nothing:**

- `read` for `/v1/status` and `/v1/queue_num`. Both answers are already cached at the edge for a
  second, so strong consistency would buy freshness CloudFront immediately discards.
- `controller` summing the arrival shards. The sum feeds an EWMA at alpha 0.3 with the release
  bounded at twice the target, so one interval of lag is absorbed.
- `generate_token` reading `PreQueue`. A registration row is immutable once written.
- `admin` reading `Counters`. Every mutation is separately guarded on the expected prior value, so
  a stale read produces a conflict rather than a wrong write.

**A partial batch is a failure, not a zero.** Both `BatchGetItem` call sites check
`unprocessed_keys` and return an error. A missed pre-queue shard under-counts the cohort and
strands every registration in it. A missed arrival shard under-counts arrivals, which reads as a
higher no-show rate and releases more people than the operator asked for.

**A missing shard is zero; a corrupt shard is an error.** A shard with no writes has no item, and
`BatchGetItem` returns nothing for it, so the fold counts zero and
`a_shard_with_no_registrations_has_no_item_and_counts_zero` pins that. A shard item that exists
but whose `s` cannot be read raises an error instead, because zeroing it would unadmit every
registrant in that shard.

---

## 9. Capacity, quotas, and pre-warming

### 9.1 On-demand tables start cold

A new on-demand table serves about 4,000 writes per second and 12,000 reads per second, and grows
to twice its previous peak. A waiting room is idle by definition and has no meaningful previous
peak, so every table is pre-warmed before an event.

Terraform exposes `warm_throughput_write_units` and `warm_throughput_read_units`. The
`warm_throughput` block is omitted entirely when both are zero, so an un-warmed table stays at the
on-demand cold baseline rather than pinning a floor. Any non-zero value must be at least 4,000 for
writes and 12,000 for reads — the AWS minimums — and the variables validate that with an explicit
error message.

### 9.2 The quota to raise is per-table

On-demand mode has no account-level throughput quota. The per-table quota is 40,000 write request
units and 40,000 read request units per second, and AWS documents both as adjustable and states
that they are not maximum limits.

### 9.3 Write budget for a 1,000,000-participant event

| Phase | Operation | Count | Write units |
|---|---|---|---|
| Pre-queue | `PreQueue` rows | 1,000,000 | 1,000,000 |
| Pre-queue | Shard block claims, batch 100 over 10 shards | ≤100,000 | ≤100,000 |
| Open | The conditional update | 1 | 1 |
| Admission | Arrival shard increments | 1 per admitted visitor | 1 each |
| Admission | Cursor advance | 6 per minute | 6 per minute |

Registration spread across a 20-minute window runs at about 917 writes per second, below the
cold-table floor. Compressed into 100 seconds it runs at 11,000 per second and needs both
pre-warming and a quota increase.

---

## 10. The read path

### 10.1 The client asks for its number once

`waiting.js` fetches `/v1/queue_num` only while `knownPosition` is null. Every later poll fetches
`/v1/status` alone. A position never changes once known, so there is nothing to re-fetch.

`/v1/status` is cached with a path-only key and no cookies, so CloudFront collapses concurrent
misses into roughly one origin fetch per second. `/v1/queue_num` is keyed on `event_id` and
`request_id` and cannot collapse — so it is asked once per visitor instead of once per poll.

### 10.2 The first-ask spike is the real read event

Everyone learns their number at the same moment, so the client spreads the ask over
`min(60_000, participants / 5000 × 1000)` milliseconds.

At 1,000,000 participants:

```
spread            = min(60,000 ms, 200,000 ms)      = 60 s
requests          = 1,000,000 / 60                  ≈ 16,700 /s
PreQueue GetItem  = 16,700 × 0.5 RRU                ≈  8,350 RRU/s   (21% of a 40,000 quota)
```

Those reads land on 1,000,000 distinct partition keys, so there is no hot partition. DynamoDB is
not the constraint here — API Gateway is. 16,700 requests per second exceeds the default account
throttle of 10,000. Past `FIRST_ASK_TARGET_RPS` times the 60-second cap, the spread stops being
enough on its own and the account throttle has to be raised.

### 10.3 The in-process cache protects the one hot item

Both read endpoints read the same `Counters` item, and a single partition serves at most 3,000
read units per second. `read` holds that item in process for one second, matched to the edge TTL,
so a reader is never staler than what CloudFront is already serving.

The cache stamps the time **after** the read returns, not before, so a slow read is not credited a
full TTL it already spent in flight. It caches a missing event too, so a flood of requests for an
event that does not exist costs one read per TTL rather than one each. A poisoned lock reports a
miss, because the fallback is a live read and that is always correct.

With the cache, `Counters` reads are one per execution environment per second:

```
environments = request_rate × mean_duration
at 16,700 /s and 20 ms    →   ~334 environments  →  ~167 RCU/s   (6% of the partition ceiling)
at 16,700 /s and 100 ms   →  ~1,670 environments →  ~835 RCU/s   (28%)
```

Without the cache, every one of those 16,700 requests reads the item directly — about 8,350 RCU
per second against a 3,000 RCU per second ceiling.

### 10.4 What a per-poll position fetch would cost

Re-fetching `/v1/queue_num` on every poll, at 1,000,000 waiters and a mean interval of 5.75
seconds, puts the read path outside quota:

```
requests          = 1,000,000 / 5.75        ≈ 174,000 /s
PreQueue GetItem  = 174,000 × 0.5 RRU       ≈  87,000 RRU/s   (2.2× the default quota)
```

---

## 11. No scan, and time to live is the only reclamation

There is no `Scan` anywhere in the codebase. The controller once ran an unbounded one over
`Positions` six times a minute to expire positions the admission cursor had passed; that
mechanism was removed ([ADR-0031](adr/0031-remove-controller-driven-expiry.md)) and with it the
only `dynamodb:Scan` grant in the deployment. Every access is now a key lookup.

DynamoDB deletes expired items within a few days, and expired items stay readable until the
deletion runs. That was the original objection to TTL as an expiry mechanism, and it still holds
— which is why nothing treats it as one. **A position is live until its row is gone.** TTL on
`Positions` is reclamation, set 86,400 seconds after the row is written by
`const POSITION_TTL_SECS: u64 = 86_400`, and nothing reads the attribute.

What compensates for people who never arrive is the controller's no-show correction: it measures
arrivals against releases and releases more to cover the gap. Expiry was a second control acting
on the same quantity, and the two were never reconciled.

`load_session` applies the same caution in the other direction: it checks `expires_at` on read
rather than trusting deletion, so an expired session is never served while its row is still
waiting to be reclaimed.

---

## 12. Known gaps

**The `Tokens` partition key is named `request_id`.** It now holds operator session ids and PKCE
states, neither of which is a request id. Renaming it changes the table's hash key, which replaces
the table, so the name stays and `expr.rs` documents the misnomer at the constant.

**`Positions` rows carry no `event_id`.** `PositionItem` has `request_id`, `queue_position`,
`entry_time`, `status`, and `ttl`. Isolation between events comes from the deployment being
single-event, not from the row. A second event sharing these tables would need the attribute, and
every reader of a row would need to check it.

---

## Appendix A — Ceilings

| Ceiling | Value | Adjustable | Where it binds |
|---|---|---|---|
| Single-partition write | 1,000 WCU/s | No | `queue_counter` below batch size ~40 |
| Single-partition read | 3,000 RCU/s | No | The `Counters` item, absorbed by the in-process cache (§10.3) |
| Per-table write, on-demand | 40,000 WRU/s | Yes | `Positions` writes at 40,000 joins/s |
| Per-table read, on-demand | 40,000 RRU/s | Yes | `PreQueue` at the first-ask spike, 21% used (§10.2) |
| Cold or long-idle table | ~4,000 writes/s, ~12,000 reads/s | Yes, by pre-warming | Every un-warmed event |
| Warm throughput minimum | 4,000 write units, 12,000 read units | No | The Terraform variable validation |
| Item size | 400 KB | No | Not binding; the largest item is under 1 KB |
| `BatchGetItem` | 100 items, 16 MB | No | Not binding; both call sites fetch 10 |
| API Gateway account throttle | 10,000 requests/s | Yes | The first-ask spike, before DynamoDB (§10.2) |

## Appendix B — Sources

| Claim | Source |
|---|---|
| Per-table on-demand quotas are adjustable and not maximum limits | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| No account-level throughput quota in on-demand mode | same |
| Writes billed in 1 KB units rounded up | same |
| New tables serve 4,000 writes/s and 12,000 reads/s; growth to 2× previous peak | [On-demand capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) |
| Partition maximum of 3,000 RCUs and 1,000 WCUs per item's primary key | [Partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html) |
| Warm throughput and pre-warming | [Pre-warming DynamoDB tables](https://aws.amazon.com/blogs/database/pre-warming-amazon-dynamodb-tables-with-warm-throughput/) |
| `UpdateItem` `ADD` is atomic; writes serialized per item; each value returned once | [UpdateItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) |
| `UpdateItem` consumes throughput for the whole item even when updating a subset | same |
| `Scan` pages at 1 MB; filters are applied after the read | [Scanning tables in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html) |
| TTL deletes within a few days; expired items remain readable | [Using time to live in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) |
| Request collapsing is disabled by a Min TTL of 0 or by cookie forwarding | [DDoS resilience with HTTP caching on CloudFront](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront) |
| SQS standard queues are at-least-once with best-effort ordering | [Using Lambda with SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) |
| A batch size above 10 requires a window of at least 1 second | [SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
