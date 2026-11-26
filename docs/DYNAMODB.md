# DynamoDB

DynamoDB is the only datastore. This document covers the tables, every access pattern, the
techniques that keep the hot items under their ceilings, and the consistency choices, read from
`crates/*/src/dynamo.rs`, `crates/read/src/main.rs`, `crates/admin/src/sessions.rs`,
`crates/wr-common/src/{expr,items}.rs` and `infra/modules/core/main.tf`.
[`ARCHITECTURE.md`](./ARCHITECTURE.md) describes the system around it.

Units: WCU/RCU are capacity units (the per-partition ceilings), WRU/RRU request units (on-demand
billing and the per-table quotas). A write of ≤ 1 KB costs 1 unit; a read of ≤ 4 KB costs 0.5
eventually consistent or 1 strongly consistent.

## 1. Tables and items

All four tables are `PAY_PER_REQUEST` with point-in-time recovery, a partition key only, no
secondary indexes and no streams. Terraform declares only the key attribute and the TTL.

| Table | Partition key | Holds | TTL attribute |
|---|---|---|---|
| `Counters` | `event_id` | The event item, 10 pre-queue shards, 10 arrival shards, the admission log | none |
| `PreQueue` | `r` | One registration per pre-queue visitor | none |
| `Positions` | `request_id` | One row per live joiner, plus one per admitted pre-queue member | `ttl` |
| `Tokens` | `request_id` | Operator OIDC sessions and pending PKCE logins | `expires_at` |

### 1.1 Keys

```
Counters   EVT#{event_id}             the event
           EVT#{event_id}#PQ#{0..9}   pre-queue registration shards
           EVT#{event_id}#AR#{0..9}   arrival shards
           EVT#{event_id}#LOG         admission log
Tokens     SESS#{session_id}          operator session
           PKCE#{state}               pending login
PreQueue   {request_id}               bare
Positions  {request_id}               bare
```

Keys are tagged only where a table holds more than one kind of item; in `PreQueue` and
`Positions` a tag would disambiguate nothing and cost bytes in one key per visitor. Every key is
built by `wr_common::expr::Key`, which returns the whole key map so the attribute name is written
once too; no call site formats a key. A `Shard` can only be constructed in `0..10`.

`event_id` may not contain `#` (Terraform validation): `EVT#a#PQ#1` would be both event `a`'s
shard 1 and event `a#PQ#1`'s item. The `Update` and `Condition` builders bind every attribute name
through a placeholder, so reserved words such as `status` cannot break an expression, and their
tests assert every placeholder is bound and used.

### 1.2 The event item

Terraform seeds it at apply (`phase` = `idle`, or `pre_queue` when `starts_at` is set; `open`;
both counters `0`; `origin_concurrency` from the variable of the same name) and then ignores it.

| Attribute | Writer | Meaning |
|---|---|---|
| `queue_counter` | `assign_position`, `open_event` | Live-join sequence; set to `N` at the open |
| `serving_counter` | `controller` | Admission cursor, exclusive |
| `phase` | `open_event`, `admin` | `idle` / `pre_queue` / `active` / `post_event` / `maintenance` |
| `shuffle_seed`, `participant_count`, `prequeue_offsets` | `open_event` | The open outputs: 256-bit seed, `N`, ten prefix offsets. Written once |
| `admission_control` | `admin` | `open` / `paused` only; absent reads as `open` |
| `fail_open_until` | `admin` | Epoch seconds; `0` or absent means no window |
| `origin_concurrency` | `admin` | How many visitors the origin takes at once |
| `message` | `admin` | Operator broadcast |
| `starts_at`, `starts_at_tz` | `admin` | Scheduled start (epoch) and the operator's IANA zone; absent means unscheduled |
| `rules_digest`, `rules_count` | `admin` | Audit of the gate ruleset; the ruleset itself lives only in the KeyValueStore |
| `last_action`, `last_action_by`, `last_action_at`, `last_action_epoch_ms` | `admin` | Audit trail and debounce anchor |

A shard item holds two attributes: `s`, its own index (so a batch read can tell shards apart
without parsing keys), and `n`, its count. An arrival shard also holds `c`, the cap `n` may be
taken up to.

The admission log `EVT#{event_id}#LOG` is written only by `controller`. It holds `h`, a flat list
of `[epoch seconds, total admitted]` pairs, one per recent pass, which is what live sessions are
counted from, and `p`, how many slots the cursor has been moved for so far
([DESIGN §7](DESIGN.md#7-outflow-control)).

### 1.3 Row shapes

| Item | Attributes |
|---|---|
| `PreQueue` | `r` request id, `s` shard, `l` local index, `t` registration time, `h` possession digest |
| `Positions` | `request_id`, `queue_position`, `entry_time`, `status` (`issued`/`admitted`), `admitted_at`, `h`, `ttl` |
| `Tokens` `SESS#` | `subject`, `email`, `expires_at` |
| `Tokens` `PKCE#` | `pkce_verifier`, `nonce`, `expires_at` |

`h` is the digest of the visitor's possession secret
([ADR-0035](adr/0035-request-id-proof-of-possession.md)); `generate_token` refuses a row without
one. A pre-queue member has no `Positions` row until the admission claim creates one at
`admitted`, copying `h` and stamping the derived position. `admitted_at` is written once, by that
same guarded mark; every session minted for the row expires from it (#237).

## 2. Access patterns

| Caller | Table | Operation | Consistency | Rate |
|---|---|---|---|---|
| `assign_position` | `Counters` | `GetItem` event | Strong | 1 per batch, +1 fix-up re-read per pre-queue batch |
| `assign_position` | `PreQueue` | `BatchGetItem`, projection `r` (dedupe) | Strong | 1 per pre-queue batch |
| `assign_position` | `Counters` | `UpdateItem SET s ADD n`, `ALL_NEW`, one random shard | — | 1 per pre-queue batch |
| `assign_position` | `PreQueue` | `PutItem` if `attribute_not_exists(r)` | — | 1 per registrant |
| `assign_position` | `Counters` | `UpdateItem ADD queue_counter`, `ALL_NEW` | — | 1 per live batch, +1 per straggler fix-up |
| `assign_position` | `Positions` | `PutItem` if `attribute_not_exists(request_id)` | — | 1 per live joiner |
| `open_event` | `Counters` | `BatchGetItem` of ten pre-queue shards, read once a second, with an SQS `GetQueueAttributes`, while the join queue drains (#239), then once for the open | Strong | Once per event |
| `open_event` | `Counters` | `UpdateItem` open, guarded | — | Once per event |
| `open_event` | `Counters` | `GetItem` projecting `shuffle_seed` | Eventual | Only when the open's guard fails, to tell "already open" from "wrong phase" |
| `read` | `Counters` | `GetItem` event | Eventual, 1 s in-process cache | ≤ 1/s per execution environment |
| `read` | `PreQueue` | `GetItem` | Eventual | 1 per `/_wr/v1/queue_num` |
| `read` | `Positions` | `GetItem` | Eventual | 1 per `/_wr/v1/queue_num` with no `PreQueue` row, or a straggler |
| `generate_token`, `nojs` | `Counters` | `GetItem` event | Strong | 1 per call |
| `generate_token`, `nojs` | `Positions` | `GetItem` | Strong | 1 per call |
| `generate_token`, `nojs` | `PreQueue` | `GetItem` | Eventual | 1 per call |
| `generate_token`, `nojs` | `Counters` | `UpdateItem ADD n 1` on a random arrival shard, guarded on `n < c` | — | 1–3 per first admission |
| `generate_token`, `nojs` | `Positions` | `UpdateItem` admitted mark, guarded | — | 1 per first admission |
| `controller` | `Counters` | `GetItem` event | Strong | 1 per pass |
| `controller` | `Counters` | `BatchGetItem` of ten arrival shards | Strong | 1 per pass |
| `controller` | `Counters` | `GetItem` admission log | Strong | 1 per pass |
| `controller` | `Counters` | `UpdateItem` cursor, guarded | — | 1 per open pass |
| `controller` | `Counters` | `UpdateItem` admission log | — | 1 per open pass |
| `controller` | `Counters` | `UpdateItem SET c` on each arrival shard | — | 10 per open pass |
| `admin` | `Counters` | `GetItem`; guarded `UpdateItem` per action | Eventual | Operator actions |
| `admin` | `Tokens` | `PutItem` session/login; `GetItem` session; `DeleteItem` logout; `DeleteItem ALL_OLD` to consume a login | Eventual | Operator logins |

There is no `Scan` and no `Query` anywhere, and no IAM grant for either. Every access is a key
lookup.

## 3. Amortise the sequence across a batch

A position sequence must yield unique ordered values, so it lives on one item, and one item takes
at most 1,000 WCU/s. One `UpdateItem` per joiner would be ten times over that at 10,000 joins/s.

Each SQS batch instead claims a block with one `ADD queue_counter :n` and `ALL_NEW`, and assigns
`[end − n + 1, end]`. At `BatchSize` 100 and 10,000 joins/s that is 100 counter writes/s (10 % of
the ceiling); at 40,000 joins/s it is 400 (40 %), and the 40,000 WRU/s table quota binds first. A
batch size of 10 would put 1,000 writes/s on the item, exactly the ceiling.

`n` counts valid records only, so malformed payloads consume no positions. The block start
saturates rather than wraps (the release profile has no overflow checks): a saturated live
position is only a gap.

## 4. Stripe across partition keys, never attribute names

The 1,000 WCU / 3,000 RCU ceiling is per partition key, so ten attributes on one item share one
budget and ten items get ten. Each shard is therefore its own item. Shard attributes on the event
item would also raise the billed size of every write to it, `queue_counter` claims and controller
passes included, while distributing nothing. The reasoning is
[ADR-0015](adr/0015-stripe-prequeue-counter.md).

| Counter | Kind | Striped | Why |
|---|---|---|---|
| `queue_counter` | Sequence | No | Summing shards cannot yield a unique ordered value |
| `serving_counter` | Sequence | No | Same, and it is read as a running value |
| Pre-queue index | Index | ×10 | The permutation needs indices unique and in `[0, N)`, not ordered; the open's prefix offsets reassemble them into exactly that range |
| `arrivals` | Budget meter | ×10 | Order carries no information; each shard holds `n` taken and `c`, its cap ([ADR-0039](adr/0039-meter-admissions-not-the-cursor.md)) |

`SHARDS` is a constant, 10. The shard is drawn uniformly at random per invocation (pre-queue) or per
admission (arrivals) rather than hashed from the client-supplied `request_id`, which would let a
client steer its shard (issue #59). The row stores the shard, so nothing needs to reproduce it. A
retry cannot burn a second index because the batch first drops ids that already have a `PreQueue`
row; if that read fails it degrades to "claim anyway", and `attribute_not_exists(r)` remains the
authoritative guard.

Each pre-queue batch makes one shard claim on one shard, so at a registration rate of R/s each
shard sees about R / 1,000 claims/s at batch 100. The shard counters reach their ceiling only near
R = 1,000,000/s; the table quota binds long before.

The pre-queue claim's first index is `checked_sub`, not saturating, and errors on underflow: a
saturated pre-queue index would be a duplicate global index and hand two visitors one position.

## 5. Conditional writes

| Write | Guard | What it buys |
|---|---|---|
| `PreQueue` row | `attribute_not_exists(r)` | A duplicate registration consumes no index |
| `Positions` row (live join) | `attribute_not_exists(request_id)` | A duplicate join consumes no position |
| Open | `attribute_not_exists(shuffle_seed) AND phase = :pre_queue` | Opens exactly once, and only from `pre_queue` |
| Cursor advance | `attribute_not_exists(serving_counter) OR serving_counter = :expected` | Overlapping controller executions cannot double-advance |
| Admission budget | `n < c` on the arrival shard | No admission past the cap the controller last wrote; a shard never granted one admits nobody ([ADR-0039](adr/0039-meter-admissions-not-the-cursor.md)) |
| Admitted mark | `attribute_not_exists(request_id) OR status = :issued` | A visitor takes budget once; a repeat re-mints without it |
| Phase change | `phase = :from` | A transition another operator applied is a 409 |
| Concurrency change | `origin_concurrency = :exp`, or `attribute_not_exists(origin_concurrency)` | Same, for the origin concurrency |
| Admission control | `admission_control = :from`, also allowing absence when `:from` is `open` | Same; absence reads as `open` |
| Debounced admin writes | `… AND (attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms <= :cutoff)` | A double-submitted form is a no-op for 2,000 ms |
| Audit stamps (rules, fail-open/recover, open now, IP binding) | `attribute_not_exists(last_action_epoch_ms) OR last_action_epoch_ms <= :ms` | An older action cannot overwrite a newer audit anchor |

`force_maintenance` is guarded on the expected phase but not debounced, and fail-open/recover
carry only the stamp guard: an emergency control must always apply. When a newer writer has
already advanced the anchor, fail-open falls back to writing `fail_open_until` alone and loses its
own audit entry.

Every guard's `ConditionalCheckFailedException` is handled by name as an outcome, not an error:
`assign_position` reports a duplicate, `open_event` disambiguates already-open from wrong-phase,
`generate_token` reads a spent shard as "try another, then 425", the controller logs the lost race, and admin
returns a conflict. These guards are what let ingest run on an at-least-once SQS standard queue:
a redelivery fails its condition, and ordering is irrelevant because positions come from a
counter.

## 6. Derive state instead of storing it

A materialised shuffle is a million non-atomic writes at the start
([ADR-0002](adr/0002-seeded-permutation-not-materialised-shuffle.md)). The open instead writes its
outputs in one `UpdateItem`, so a reader sees the unopened item or all of them; `open_outputs()`
returns `Some` only when seed, `N` and offsets are all present. The position is computed on read
by `Counters::resolve_prequeue`, the one implementation both `read` and `generate_token` call.

The same rule covers `serving_state` (derived from phase and admission control) and fail-open:
storage holds `open`/`paused` and an epoch, and `wr_common::resolve(stored, until, now)` produces
`FailOpen` on every read, so a window ends at the same instant everywhere.

## 7. Bytes

`UpdateItem` bills the whole item, attribute names included, rounded up to the next KB. Shard
items use `s` and `n` so every increment stays one write unit. `PreQueue` uses one-letter names
because it is the per-visitor table read across the cohort at audit; a row is about 100 bytes.
`Positions` is written once or twice per visitor and uses readable names. Every item is far below
1 KB.

## 8. Consistency

Six strongly consistent reads, each because a stale answer is wrong:

- `assign_position`'s `Counters` read: the batch's live/pre-queue routing and the straggler
  fix-up turn on it.
- `assign_position`'s `PreQueue` dedupe read: a replica that has not seen a row would burn an
  index.
- `open_event`'s shard read: a missed registration is a visitor with no position.
- `generate_token`'s `Positions` read: the admission status and digest must be current.
- `controller`'s `Counters` read: it computes from the cursor its write is guarded on.
- `controller`'s arrival-shard read: each cap is written as `n` plus a share, so a stale `n`
  would re-grant budget already spent.

The controller reads its admission log consistently too, though a log missing the latest pass
would only count more sessions live.

`generate_token`'s `Counters` read is consistent when made, but each execution environment holds
the item for one second (`MEMO_SECS`), as `read` does: every admission attempt, refused or not,
would otherwise be a read on one partition key. The cursor moves every ten seconds, and a pause
takes effect at most a second late. The same environment does not retry a shard it saw refuse a
take for one second, so a spent budget is not also a stream of failed conditional writes.

Everything else is eventual. `read`'s answers are cached at the edge for a second anyway;
`PreQueue` rows are immutable; and admin's writes are guarded on the prior value, so a stale read is a conflict, not a
wrong write.

**A partial batch is a failure, not a zero.** `open_event` and the controller check
`unprocessed_keys` and error: a missed pre-queue shard strands every registrant in it, and a
missed arrival shard would read as having admitted nobody and be capped below what it has taken. **A missing shard is zero; a corrupt one
is an error**: a shard with no registrations has no item, but one whose `s` cannot be read fails
the open rather than unadmitting its registrants.

## 9. Capacity and quotas

A new on-demand table serves about 4,000 writes/s and 12,000 reads/s and grows to twice its
previous peak. A waiting room has no meaningful previous peak, so tables are pre-warmed before an
event: `warm_throughput_write_units` / `warm_throughput_read_units` apply to all four tables, are
omitted when zero, and must otherwise be at least 4,000 / 12,000 (validated). On-demand has no
account-level throughput quota; the per-table quota is 40,000 WRU/s and 40,000 RRU/s, both
adjustable.

Write budget for a 1,000,000-participant event:

| Phase | Operation | Count |
|---|---|---|
| Pre-queue | `PreQueue` rows | 1,000,000 |
| Pre-queue | Shard claims, batch 100 | ~10,000 |
| Open | The guarded update | 1 |
| Admission | Budget take + admitted mark | 2 per admitted visitor |
| Admission | Cursor advance + log + ten shard caps | 72 per minute |

The pre-queue dedupe adds about 1,000,000 strongly consistent reads (1 RRU each) over the
registration window. Registration spread over 20 minutes runs at about 830 writes/s, under the cold
floor; compressed into 100 seconds it is about 10,000/s and needs pre-warming and a quota increase.

## 10. The read path

`/_wr/v1/status` is cached at the edge with a path-only key, so it costs about one origin fetch per
second whatever the crowd. `/_wr/v1/queue_num` is keyed per visitor and cannot collapse, so the client
asks it once.

**The first-ask spike** is the real read event. At 1,000,000 participants the client's spread
saturates at 120 s:

```
requests          = 1,000,000 / 120 s   ≈  8,350 /s
PreQueue GetItem  =  8,350 × 0.5 RRU    ≈  4,200 RRU/s   (10 % of the 40,000 quota)
```

Those reads hit a million distinct keys, so there is no hot partition, and the request rate
stays under API Gateway's default 10,000/s account throttle.

**The in-process cache protects the one hot item.** Both endpoints read the `Counters` item, and
one partition serves at most 3,000 RCU/s. `read` holds the item for one second per execution
environment, matching the edge TTL. The timestamp is taken after the read returns, a missing event
is cached too, and a poisoned lock is a miss. Reads then scale with environments, not requests:

```
environments ≈ request_rate × mean_duration
 8,350 /s × 20 ms   →    ~167 environments  →   ~84 RCU/s   (3 % of the partition ceiling)
 8,350 /s × 100 ms  →    ~835 environments  →  ~418 RCU/s   (14 %)
```

Without it the same spike is about 8,350 RCU/s against a 3,000 ceiling. Re-fetching
`/_wr/v1/queue_num` on every poll instead of once would be about 174,000 requests/s at 1,000,000
waiters and a 5.75 s mean interval: roughly 87,000 RRU/s, over twice the default quota.

## 11. Reclamation

There is no `Scan`. The controller's former `Positions` scan for expiry was removed
([ADR-0031](adr/0031-remove-controller-driven-expiry.md)); a position is live until its row is
gone. `Positions.ttl` is `now + 86,400` (`POSITION_TTL_SECS`) at each write and is reclamation
only: nothing reads it, and DynamoDB may take days to delete an expired row. Unused admission
budget is what compensates for visitors who never arrive.

`Tokens` rows expire on `expires_at`, and because expired rows stay readable until deleted, both
`load_session` and the PKCE consume check `expires_at` themselves. `PreQueue` and `Counters` have
no TTL.

## 12. Known gaps

- **The `Tokens` key is named `request_id`** but holds session ids and PKCE states. Renaming a
  hash key replaces the table, so the name stays; `expr.rs` documents it.
- **`Positions` and `PreQueue` rows carry no `event_id`.** Isolation comes from the deployment
  being single-event. Running another event means changing `event_id` and applying: Terraform
  re-seeds the event item and recreates both tables empty (#248), so no reader has to check an
  attribute. `table_deletion_protection` must be `false` for that apply.

## Appendix A — Ceilings

| Ceiling | Value | Adjustable | Where it binds |
|---|---|---|---|
| Single-partition write | 1,000 WCU/s | No | `queue_counter` below a batch size of about 40 at 40,000 joins/s |
| Single-partition read | 3,000 RCU/s | No | The `Counters` item, absorbed by the in-process cache (§10) |
| Per-table write, on-demand | 40,000 WRU/s | Yes | `Positions` writes at 40,000 joins/s |
| Per-table read, on-demand | 40,000 RRU/s | Yes | `PreQueue` at the first-ask spike, 21 % used (§10) |
| Cold or long-idle table | ~4,000 writes/s, ~12,000 reads/s | By pre-warming | Every un-warmed event |
| Warm throughput minimum | 4,000 write, 12,000 read units | No | The Terraform variable validation |
| Item size | 400 KB | No | Not binding; every item is under 1 KB |
| `BatchGetItem` | 100 items, 16 MB | No | Not binding; batches are 10 shards or ≤ 100 ids |
| API Gateway account throttle | 10,000 requests/s | Yes | The first-ask spike, before DynamoDB (§10) |

## Appendix B — Sources

| Claim | Source |
|---|---|
| Per-table on-demand quotas are adjustable; no account-level throughput quota; writes billed in 1 KB units | [Quotas in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html) |
| New tables serve 4,000 writes/s and 12,000 reads/s; growth to 2× previous peak | [On-demand capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) |
| 3,000 RCU and 1,000 WCU per partition key | [Partition key design](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html) |
| Warm throughput and pre-warming | [Pre-warming DynamoDB tables](https://aws.amazon.com/blogs/database/pre-warming-amazon-dynamodb-tables-with-warm-throughput/) |
| `ADD` is atomic and serialised per item; `UpdateItem` bills the whole item | [UpdateItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) |
| TTL deletes within a few days; expired items remain readable | [Using time to live in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html) |
| Request collapsing is disabled by a Min TTL of 0 or by cookie forwarding | [DDoS resilience with HTTP caching on CloudFront](https://repost.aws/articles/ARTocYphbwQnWtTz8FXrwqew/ddos-resilience-with-http-caching-on-cloudfront) |
| SQS standard queues are at-least-once with best-effort ordering | [Using Lambda with SQS](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html) |
| A batch size above 10 requires a window of at least 1 second | [SQS event source mapping](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) |
