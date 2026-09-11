//! Batch processing for the `assign_position` Lambda: live-join positions and
//! pre-queue registration.
//!
//! Consumes a batch of enqueued join messages. Each batch reads the event's
//! `Counters` item once (a strongly consistent `GetItem`) to decide which of
//! two paths every valid record in the batch takes, or to reject the batch
//! outright when no `Counters` item exists yet:
//!
//! - **Rejected (unconfigured event)** — no `Counters` item exists, so the
//!   event has not been set up. Every valid record is failed and claims
//!   nothing, surfacing the state as a hard failure rather than letting a
//!   pre-seal live join increment `queue_counter` before the seal `SET`s it
//!   to the cohort size (an unconditional `SET` that would discard the
//!   increment and let a later cohort member or post-seal joiner collide on
//!   the same numeric position).
//! - **Live join** — the event is sealed, or its phase is anything but
//!   `PreQueue`. Allocates one contiguous block of queue positions with one
//!   counter increment and writes one `Positions` row per valid record.
//! - **Pre-queue** — the event is not sealed and its phase is `PreQueue`.
//!   Groups the batch's valid records by shard (`hash(request_id) % 10`) and
//!   claims one contiguous block of local indices per shard, then writes one
//!   `PreQueue` row per record.
//!
//! The branch is decided on the seal outputs ([`wr_common::Counters::sealed`]),
//! never on phase alone: an operator can walk the phase back to `PreQueue`
//! after a seal without unsealing the index space, and a record arriving in
//! that state is still a live join.
//!
//! A pre-queue write can race `seal_event`'s `BatchGetItem` — the shard claim
//! landing after the seal read that shard's count, but the row write landing
//! before the batch finishes. Left alone this is a silent dead end: the row
//! resolves to a live join on read, but nothing ever gave it a live position.
//! So after the pre-queue writes land, one more consistent read of `Counters`
//! checks whether the event sealed mid-batch; every row this invocation
//! actually wrote (never a `Duplicate` — its burned index belongs to an
//! earlier invocation with a different local index) that now resolves past
//! its shard's sealed count gets a real live position, claimed from the same
//! `queue_counter` the live path uses. This is safe because the seal sets
//! `queue_counter = N` in the same atomic write that publishes the offsets.
//!
//! The batch logic is generic over the [`Store`] port so it runs without AWS;
//! the SDK-backed implementation lives in `dynamo`.

use std::future::Future;

use serde::Deserialize;
use wr_common::{Assignment, Counters, Phase, SHARDS, shard_for};

pub mod dynamo;

/// A join message body enqueued by the ingest API. Only the two fields the
/// request validator already required are read.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinMessage {
    pub request_id: String,
    pub event_id: String,
}

/// One record from the SQS batch: the message id (for failure reporting) and
/// the raw body to parse.
#[derive(Debug, Clone)]
pub struct BatchRecord {
    pub message_id: String,
    pub body: String,
}

/// A position write to attempt: the request and the queue position it claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionWrite {
    pub request_id: String,
    pub position: u64,
}

/// A pre-queue registration write to attempt: the request and the `(shard,
/// local index)` it claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreQueueWrite {
    pub request_id: String,
    pub shard: usize,
    pub local_index: u64,
}

/// The persistence port the batch logic drives.
pub trait Store {
    /// `ADD queue_counter :n` with `ALL_NEW` for one event, returning the block
    /// end. The claimed block is `[end - n + 1, end]`. `n` is the count of
    /// valid records and is always `>= 1` when called.
    fn claim_block(
        &self,
        event_id: &str,
        n: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes one position row with `attribute_not_exists(request_id)`. A
    /// conditional-check failure (a duplicate request id) is reported as
    /// `Ok(WriteOutcome::Duplicate)`, not an error — the position is simply
    /// abandoned, which is a permitted gap.
    fn put_position(
        &self,
        write: &PositionWrite,
    ) -> impl Future<Output = Result<WriteOutcome, StoreError>> + Send;

    /// Reads the event's `Counters` item with a strongly consistent read.
    /// `None` when no item exists yet (an event that has never been touched).
    fn load_counters(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send;

    /// Claims a contiguous block of `count` local indices within one
    /// pre-queue shard, returning the first local index of the block (the
    /// shard's count after the add, minus `count`). Errors rather than
    /// saturates on underflow: a saturated local index here would be a
    /// duplicate global index, not a harmless gap.
    fn claim_prequeue_block(
        &self,
        event_id: &str,
        shard: usize,
        count: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes one pre-queue row with `attribute_not_exists(r)`. A duplicate
    /// request id reports `Ok(WriteOutcome::Duplicate)`, exactly as
    /// [`Store::put_position`] does.
    fn put_prequeue(
        &self,
        write: &PreQueueWrite,
    ) -> impl Future<Output = Result<WriteOutcome, StoreError>> + Send;
}

/// The result of a single conditional position or pre-queue write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Written,
    /// The `attribute_not_exists` guard rejected a duplicate request id.
    Duplicate,
}

/// A store failure that should return the record to the queue for retry.
#[derive(Debug, thiserror::Error)]
#[error("store error: {0}")]
pub struct StoreError(pub String);

/// The message ids that must be returned to the queue, in the shape the Lambda
/// event source mapping expects for partial-batch failure reporting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    pub failures: Vec<String>,
}

/// Parses `body` and returns the join message if `request_id` is a valid
/// `UUIDv7`; `None` marks the record invalid (its position is never claimed).
#[must_use]
pub fn parse_valid(body: &str) -> Option<JoinMessage> {
    let msg: JoinMessage = serde_json::from_str(body).ok()?;
    if is_uuid_v7(&msg.request_id) {
        Some(msg)
    } else {
        None
    }
}

/// Checks the canonical `8-4-4-4-12` hex form with version nibble `7` and a
/// variant nibble in `8..=b`. Rejects anything else so a malformed or spoofed
/// id claims no position.
#[must_use]
pub fn is_uuid_v7(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => b == b'-',
            14 => b == b'7', // version nibble
            19 => matches!(b, b'8'..=b'9' | b'a'..=b'b' | b'A'..=b'B'), // variant nibble
            _ => b.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Processes one SQS batch.
///
/// Every record whose body is malformed, whose `request_id` is not a
/// `UUIDv7`, or whose `event_id` does not match `event_id` is an immediate
/// batch failure and claims nothing. The remaining valid records all take the
/// same path — live join or pre-queue — decided once from the event's
/// `Counters` item (see the module docs). A `Counters` read failure, or a
/// missing `Counters` item (the event has not been set up yet), fails every
/// valid record and claims nothing.
pub async fn process_batch<S: Store>(
    store: &S,
    event_id: &str,
    records: &[BatchRecord],
) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();
    let mut valid = Vec::new();
    for record in records {
        match parse_valid(&record.body) {
            Some(msg) if msg.event_id == event_id => valid.push((record.message_id.clone(), msg)),
            Some(_) | None => {
                tracing::warn!(message_id = %record.message_id, "invalid join record");
                outcome.failures.push(record.message_id.clone());
            }
        }
    }

    if valid.is_empty() {
        return outcome;
    }

    let counters = match store.load_counters(event_id).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            // No `Counters` item: the event has not been set up yet. Failing
            // every valid record prevents a pre-seal live join from
            // incrementing `queue_counter` before the seal `SET`s it to the
            // cohort size — an unconditional `SET` that would discard the
            // increment and let a later cohort member (or post-seal live
            // joiner) collide on the same numeric position. The message
            // retries until setup, then dead-letters after `maxReceiveCount`:
            // a misconfiguration surfaced as a hard failure rather than
            // silently processed into a collision.
            tracing::warn!(
                event_id = event_id,
                valid = valid.len(),
                "no Counters item; failing batch until the event is set up"
            );
            for (message_id, _) in &valid {
                outcome.failures.push(message_id.clone());
            }
            return outcome;
        }
        Err(err) => {
            tracing::error!(error = %err, "counters read failed; retrying batch");
            for (message_id, _) in &valid {
                outcome.failures.push(message_id.clone());
            }
            return outcome;
        }
    };

    // Branch on the seal outputs, never on phase alone: an operator can walk
    // the phase back to PreQueue after a seal (Active -> Maintenance -> Idle
    // -> PreQueue) without unsealing the index space, and a record arriving
    // in that state is still a live join.
    let live_path = counters.sealed().is_some() || counters.phase != Phase::PreQueue;

    if live_path {
        process_live_batch(store, event_id, valid, &mut outcome).await;
    } else {
        process_prequeue_batch(store, event_id, valid, &mut outcome).await;
    }

    outcome
}

/// Claims one contiguous block of queue positions for the whole valid set and
/// writes one `Positions` row per record.
async fn process_live_batch<S: Store>(
    store: &S,
    event_id: &str,
    valid: Vec<(String, JoinMessage)>,
    outcome: &mut BatchOutcome,
) {
    let n = valid.len() as u64;
    let end = match store.claim_block(event_id, n).await {
        Ok(end) => end,
        Err(err) => {
            tracing::error!(error = %err, "queue_counter claim failed; retrying batch");
            for (message_id, _) in &valid {
                outcome.failures.push(message_id.clone());
            }
            return;
        }
    };
    // Saturating, not bare: the release profile has no overflow checks, and a
    // wrapped start would hand out positions from the top of the u64 range.
    // `end >= n` always holds for a counter that only moves forward, so this
    // is a guard against a counter that was reset, never normal arithmetic.
    let start = end.saturating_sub(n).saturating_add(1);

    for (offset, (message_id, msg)) in valid.into_iter().enumerate() {
        let write = PositionWrite {
            request_id: msg.request_id,
            // Saturating for the same reason `start` itself is: a wrapped
            // position is indistinguishable from a valid low one, never a
            // harmless gap.
            position: start.saturating_add(offset as u64),
        };
        if let Err(err) = store.put_position(&write).await {
            tracing::error!(error = %err, message_id = %message_id, "position write failed");
            outcome.failures.push(message_id);
        }
    }
}

/// Groups the valid set by shard, claims one contiguous local-index block per
/// shard, and writes one `PreQueue` row per record. Then checks whether the
/// event sealed mid-batch and gives every row this invocation actually wrote
/// that now resolves past its shard's count a real live position.
async fn process_prequeue_batch<S: Store>(
    store: &S,
    event_id: &str,
    valid: Vec<(String, JoinMessage)>,
    outcome: &mut BatchOutcome,
) {
    let mut groups: Vec<Vec<(String, JoinMessage)>> = (0..SHARDS).map(|_| Vec::new()).collect();
    for (message_id, msg) in valid {
        let shard = shard_for(msg.request_id.as_bytes());
        groups[shard].push((message_id, msg));
    }

    // What this invocation actually wrote (never a Duplicate — see the module
    // docs), carried into the fix-up below.
    let mut written = Vec::new();

    for (shard, group) in groups.into_iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let count = group.len() as u64;
        let start = match store.claim_prequeue_block(event_id, shard, count).await {
            Ok(start) => start,
            Err(err) => {
                tracing::error!(error = %err, shard, "prequeue shard claim failed; retrying batch");
                for (message_id, _) in &group {
                    outcome.failures.push(message_id.clone());
                }
                continue;
            }
        };

        for (offset, (message_id, msg)) in group.into_iter().enumerate() {
            let write = PreQueueWrite {
                request_id: msg.request_id,
                shard,
                // Saturating: a wrapped local index would be a duplicate
                // global index, not a gap, which is what the shard claim's own
                // `checked_sub` guards against — but `start` and `count` are
                // both small relative to `u64::MAX` here, so this is
                // defensive, not reachable in practice.
                local_index: start.saturating_add(offset as u64),
            };
            match store.put_prequeue(&write).await {
                Ok(WriteOutcome::Written) => written.push(write),
                // Burned index: the authoritative row belongs to an earlier
                // invocation with a different (shard, local index). Never
                // classified below, and never a batch failure.
                Ok(WriteOutcome::Duplicate) => {
                    tracing::debug!(message_id = %message_id, "prequeue write was a duplicate; index burned");
                }
                Err(err) => {
                    // Accepted residual: a write that timed out on the caller
                    // side but actually landed is indistinguishable here from
                    // one that truly failed, so it is classified as "no row"
                    // and reported as a batch failure. Redelivery finds the
                    // event sealed by then and takes the live path, which can
                    // mint a second Positions row for a request id that
                    // already holds a counted PreQueue row, demoting a visitor
                    // who was counted into the cohort. The fix-up below cannot
                    // catch it, because it only classifies writes this
                    // invocation observed as Written. Closing it costs a
                    // PreQueue GetItem per record on the live path, which is
                    // not worth paying on every live join to guard against one
                    // rare timing window; left open.
                    tracing::error!(error = %err, message_id = %message_id, "prequeue write failed");
                    outcome.failures.push(message_id);
                }
            }
        }
    }

    if written.is_empty() {
        return;
    }

    fixup_stragglers(store, event_id, written).await;
}

/// Checks whether the event sealed while the pre-queue writes above were
/// landing, and gives every row this invocation wrote that now resolves past
/// its shard's sealed count a real live position.
///
/// A failure anywhere in this fix-up — the read, the block claim, or a
/// position write — is deliberately never a batch failure. A record whose
/// `PreQueue` row this invocation already wrote must never be redelivered:
/// redelivery would find the event sealed and take the live path directly,
/// minting a second position for a row that (if it turns out to be within its
/// shard's count) is already correctly counted in the pre-queue cohort — the
/// same demotion by a different route. Left alone, an uncorrected straggler
/// row self-heals through the read path instead (a 404 on `/queue_num` that
/// eventually re-joins).
async fn fixup_stragglers<S: Store>(store: &S, event_id: &str, written: Vec<PreQueueWrite>) {
    let counters = match store.load_counters(event_id).await {
        Ok(counters) => counters,
        Err(err) => {
            tracing::error!(error = %err, "prequeue fix-up read failed; leaving stragglers to self-heal");
            return;
        }
    };
    let Some(sealed) = counters.and_then(|c| c.sealed()) else {
        return; // Still not sealed; nothing raced the seal.
    };

    let stragglers: Vec<PreQueueWrite> = written
        .into_iter()
        .filter(|write| {
            matches!(
                sealed.offsets.assign(write.shard, write.local_index),
                Assignment::LiveJoin
            )
        })
        .collect();
    if stragglers.is_empty() {
        return;
    }

    let n = stragglers.len() as u64;
    let end = match store.claim_block(event_id, n).await {
        Ok(end) => end,
        Err(err) => {
            tracing::error!(error = %err, "straggler live-block claim failed; leaving to self-heal");
            return;
        }
    };
    let start = end.saturating_sub(n).saturating_add(1);

    for (offset, write) in stragglers.into_iter().enumerate() {
        let position_write = PositionWrite {
            request_id: write.request_id,
            position: start.saturating_add(offset as u64),
        };
        if let Err(err) = store.put_position(&position_write).await {
            tracing::error!(error = %err, "straggler position write failed; leaving to self-heal");
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::collections::VecDeque;
    use std::sync::Mutex;

    use wr_common::{SealedOffsets, StoredControl};

    use super::*;

    const VALID_ID: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const V4_ID: &str = "018f3a2b-7c9d-4e1f-abcd-0123456789ab";

    /// Finds `n` `UUIDv7`-shaped ids that hash to `shard`, by varying a hex
    /// suffix. Deterministic and fast: 10 shards, so a match lands within a
    /// handful of tries on average. The varying group's leading nibble is
    /// pinned to the valid variant range (`8`) so every generated id still
    /// passes `is_uuid_v7`.
    fn ids_on_shard(shard: usize, n: usize) -> Vec<String> {
        let mut found = Vec::new();
        let mut i: u32 = 0;
        while found.len() < n {
            let id = format!("018f3a2b-7c9d-7e1f-8{i:03x}-0123456789ab");
            if shard_for(id.as_bytes()) == shard {
                found.push(id);
            }
            i += 1;
        }
        found
    }

    struct FakeStore {
        // Live path.
        counter: Mutex<u64>,
        writes: Mutex<Vec<PositionWrite>>,
        seen: Mutex<Vec<String>>,
        claim_fails: bool,
        /// Forces the block end the live claim reports, standing in for a
        /// counter that was reset below the block size.
        claim_end: Option<u64>,
        fail_write_for: Option<String>,

        // Pre-queue path.
        shard_counters: Mutex<[u64; SHARDS]>,
        prequeue_writes: Mutex<Vec<PreQueueWrite>>,
        prequeue_seen: Mutex<Vec<String>>,
        prequeue_claim_fails: bool,
        fail_prequeue_write_for: Option<String>,

        // `load_counters` canned responses, consumed in call order; the last
        // value repeats once the queue is down to one. `None` per response
        // means "no Counters item" (an unconfigured event: the batch is failed
        // rather than taking the live path).
        counters_sequence: Mutex<VecDeque<Option<Counters>>>,
        counters_fail_from_call: Option<u32>,
        counters_calls: Mutex<u32>,
    }

    impl Default for FakeStore {
        fn default() -> Self {
            Self {
                counter: Mutex::new(0),
                writes: Mutex::new(Vec::new()),
                seen: Mutex::new(Vec::new()),
                claim_fails: false,
                claim_end: None,
                fail_write_for: None,
                shard_counters: Mutex::new([0; SHARDS]),
                prequeue_writes: Mutex::new(Vec::new()),
                prequeue_seen: Mutex::new(Vec::new()),
                prequeue_claim_fails: false,
                fail_prequeue_write_for: None,
                counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                    Phase::Idle,
                ))])),
                counters_fail_from_call: None,
                counters_calls: Mutex::new(0),
            }
        }
    }

    impl Store for FakeStore {
        fn claim_block(
            &self,
            _event_id: &str,
            n: u64,
        ) -> impl std::future::Future<Output = Result<u64, StoreError>> + Send {
            let result = if self.claim_fails {
                Err(StoreError("counter down".to_owned()))
            } else if let Some(end) = self.claim_end {
                Ok(end)
            } else {
                let mut c = self.counter.lock().unwrap();
                *c += n;
                Ok(*c)
            };
            std::future::ready(result)
        }

        fn put_position(
            &self,
            write: &PositionWrite,
        ) -> impl std::future::Future<Output = Result<WriteOutcome, StoreError>> + Send {
            let result = if self.fail_write_for.as_deref() == Some(write.request_id.as_str()) {
                Err(StoreError("write down".to_owned()))
            } else {
                let mut seen = self.seen.lock().unwrap();
                if seen.contains(&write.request_id) {
                    Ok(WriteOutcome::Duplicate)
                } else {
                    seen.push(write.request_id.clone());
                    self.writes.lock().unwrap().push(write.clone());
                    Ok(WriteOutcome::Written)
                }
            };
            std::future::ready(result)
        }

        fn load_counters(
            &self,
            _event_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<Counters>, StoreError>> + Send
        {
            let mut calls = self.counters_calls.lock().unwrap();
            *calls += 1;
            let call_n = *calls;
            drop(calls);

            let result = if self.counters_fail_from_call.is_some_and(|k| call_n >= k) {
                Err(StoreError("counters down".to_owned()))
            } else {
                let mut seq = self.counters_sequence.lock().unwrap();
                let next = if seq.len() > 1 {
                    seq.pop_front()
                } else {
                    seq.front().cloned()
                };
                Ok(next.flatten())
            };
            std::future::ready(result)
        }

        fn claim_prequeue_block(
            &self,
            _event_id: &str,
            shard: usize,
            count: u64,
        ) -> impl std::future::Future<Output = Result<u64, StoreError>> + Send {
            let result = if self.prequeue_claim_fails {
                Err(StoreError("shard claim down".to_owned()))
            } else {
                let mut counters = self.shard_counters.lock().unwrap();
                let start = counters[shard];
                counters[shard] += count;
                Ok(start)
            };
            std::future::ready(result)
        }

        fn put_prequeue(
            &self,
            write: &PreQueueWrite,
        ) -> impl std::future::Future<Output = Result<WriteOutcome, StoreError>> + Send {
            let result =
                if self.fail_prequeue_write_for.as_deref() == Some(write.request_id.as_str()) {
                    Err(StoreError("prequeue write down".to_owned()))
                } else {
                    let mut seen = self.prequeue_seen.lock().unwrap();
                    if seen.contains(&write.request_id) {
                        Ok(WriteOutcome::Duplicate)
                    } else {
                        seen.push(write.request_id.clone());
                        self.prequeue_writes.lock().unwrap().push(write.clone());
                        Ok(WriteOutcome::Written)
                    }
                };
            std::future::ready(result)
        }
    }

    fn rec(message_id: &str, request_id: &str) -> BatchRecord {
        BatchRecord {
            message_id: message_id.to_owned(),
            body: format!(r#"{{"request_id":"{request_id}","event_id":"evt-1"}}"#),
        }
    }

    fn counters_with_phase(phase: Phase) -> Counters {
        Counters {
            event_id: "evt-1".to_owned(),
            phase,
            queue_counter: 0,
            serving_counter: 0,
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
        }
    }

    /// A sealed `Counters` built from real per-shard counts via
    /// [`SealedOffsets::seal`], so the offsets are exactly what `seal_event`
    /// would have produced rather than hand-picked to fit a scenario.
    fn sealed_counters(counts: [u64; SHARDS], phase: Phase) -> Counters {
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        Counters {
            event_id: "evt-1".to_owned(),
            phase,
            queue_counter: sealed.participant_count(),
            serving_counter: 0,
            shuffle_seed: Some([7u8; 32]),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
        }
    }

    // --- live path (unchanged behaviour) ------------------------------------

    #[test]
    fn uuid_v7_validation() {
        assert!(is_uuid_v7(VALID_ID));
        assert!(!is_uuid_v7(V4_ID)); // wrong version nibble
        assert!(!is_uuid_v7("not-a-uuid"));
        assert!(!is_uuid_v7("018f3a2b7c9d7e1fabcd0123456789ab")); // no dashes
        assert!(!is_uuid_v7("")); // empty
        assert!(!is_uuid_v7("018f3a2b-7c9d-7e1f-abcd-0123456789ab-extra"));
        // Variant nibble must be 8-b; 0-7, c-f are not RFC 4122 variant 1.
        assert!(!is_uuid_v7("018f3a2b-7c9d-7e1f-0bcd-0123456789ab"));
        assert!(is_uuid_v7("018f3a2b-7c9d-7e1f-bbcd-0123456789ab"));
    }

    #[tokio::test]
    async fn valid_batch_claims_contiguous_block_no_duplicates() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-8003-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        let mut positions: Vec<u64> = writes.iter().map(|w| w.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn invalid_records_are_failures_and_dont_consume_positions() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", VALID_ID),
            BatchRecord {
                message_id: "m2".to_owned(),
                body: "not json".to_owned(),
            },
            rec("m3", V4_ID), // valid json, wrong UUID version
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m2".to_owned(), "m3".to_owned()]);
        // Only the one valid record claimed a position: counter incremented by 1.
        assert_eq!(*store.counter.lock().unwrap(), 1);
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn all_invalid_claims_nothing() {
        let store = FakeStore::default();
        let records = vec![BatchRecord {
            message_id: "m1".to_owned(),
            body: "garbage".to_owned(),
        }];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);
        assert_eq!(*store.counter.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn counter_claim_failure_retries_whole_valid_set() {
        let store = FakeStore {
            claim_fails: true,
            ..FakeStore::default()
        };
        let records = vec![rec("m1", VALID_ID)];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn single_write_failure_is_reported_others_succeed() {
        let store = FakeStore {
            fail_write_for: Some("018f3a2b-7c9d-7e1f-8002-0123456789ab".to_owned()),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-8003-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m2".to_owned()]);
        assert_eq!(store.writes.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn live_joins_after_a_seal_land_above_the_pre_queue_cohort() {
        // The seal starts queue_counter at the cohort size N. A live join
        // arriving afterwards must be numbered outside the pre-queue cohort's
        // [0, N), or two visitors hold the same position.
        const COHORT: u64 = 1_000;
        let store = FakeStore {
            counter: Mutex::new(COHORT),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        let mut positions: Vec<u64> = writes.iter().map(|w| w.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![COHORT + 1, COHORT + 2]);
        for position in positions {
            assert!(
                position >= COHORT,
                "live position {position} collides with the pre-queue cohort [0, {COHORT})"
            );
        }
    }

    #[tokio::test]
    async fn a_block_end_below_the_block_size_does_not_wrap() {
        // A counter reset below the block it just claimed reports an end lower
        // than n. `end - n + 1` would wrap into the top of the u64 range under
        // the release profile's absent overflow checks, handing out positions
        // near u64::MAX. Saturating keeps the block at the bottom instead.
        let store = FakeStore {
            claim_end: Some(1),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-8003-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        for write in writes.iter() {
            assert!(
                write.position < 1_000,
                "position {} wrapped instead of saturating",
                write.position
            );
        }
    }

    #[tokio::test]
    async fn duplicate_request_id_is_not_a_failure() {
        let store = FakeStore::default();
        let dup = "018f3a2b-7c9d-7e1f-8009-0123456789ab";
        // Same id twice in one batch: second write is a Duplicate, not a failure.
        let records = vec![rec("m1", dup), rec("m2", dup)];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }

    // --- routing: seal outputs, not phase -----------------------------------

    #[tokio::test]
    async fn sealed_event_with_prequeue_phase_takes_live_path() {
        // An operator can walk the phase back to PreQueue after a seal without
        // unsealing the index space; a sealed event always takes the live path
        // regardless of what phase says.
        let store = FakeStore::default();
        let counters = sealed_counters([1, 0, 0, 0, 0, 0, 0, 0, 0, 0], Phase::PreQueue);
        *store.counters_sequence.lock().unwrap() = VecDeque::from([Some(counters)]);
        let records = vec![rec("m1", VALID_ID)];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.writes.lock().unwrap().len(), 1);
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn non_prequeue_phases_take_live_path() {
        for phase in [
            Phase::Idle,
            Phase::Active,
            Phase::PostEvent,
            Phase::Maintenance,
        ] {
            let store = FakeStore::default();
            *store.counters_sequence.lock().unwrap() =
                VecDeque::from([Some(counters_with_phase(phase))]);
            let records = vec![rec("m1", VALID_ID)];
            let outcome = process_batch(&store, "evt-1", &records).await;
            assert!(outcome.failures.is_empty(), "{phase:?}");
            assert_eq!(store.writes.lock().unwrap().len(), 1, "{phase:?}");
        }
    }

    #[tokio::test]
    async fn mismatched_event_id_is_a_batch_failure() {
        let store = FakeStore::default();
        let records = vec![rec("m1", VALID_ID)]; // rec() hardcodes event_id "evt-1"
        let outcome = process_batch(&store, "evt-other", &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);
        assert_eq!(*store.counter.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn counters_read_failure_fails_every_valid_record() {
        let store = FakeStore {
            counters_fail_from_call: Some(1),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures.len(), 2);
        assert!(store.writes.lock().unwrap().is_empty());
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn missing_counters_item_fails_every_valid_record_and_claims_nothing() {
        // The contract change: with no `Counters` item the event is
        // unconfigured, so a join is never processed. Failing every valid
        // record (rather than taking the live path) prevents a pre-seal live
        // join from incrementing `queue_counter` before the seal `SET`s it to
        // the cohort size, which would discard the increment and cause a
        // position collision.
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([None])),
            ..FakeStore::default()
        };
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
        ];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned(), "m2".to_owned()]);
        assert_eq!(
            *store.counter.lock().unwrap(),
            0,
            "no live block claimed on an unconfigured event"
        );
        assert!(store.writes.lock().unwrap().is_empty());
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    /// Reproduces the collision from the bug report end-to-end through the
    /// `Store` port. A join that arrives before any `Counters` item exists is
    /// now rejected (not routed to the live path), so it leaves no `Positions`
    /// row to collide with the pre-queue cohort once the event is later set to
    /// `PreQueue` and sealed. Before the fix, step (1) took the live path,
    /// incremented `queue_counter`, and the seal's `SET queue_counter = :n`
    /// overwrote it — handing a cohort member the same numeric position.
    #[tokio::test]
    async fn pre_seal_join_before_counters_item_does_not_collide_with_cohort() {
        let store = FakeStore::default();

        // (1) A join arrives before any `Counters` item exists: rejected, and
        // it neither increments `queue_counter` nor writes a `Positions` row.
        *store.counters_sequence.lock().unwrap() = VecDeque::from([None]);
        let early = process_batch(
            &store,
            "evt-1",
            &[rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab")],
        )
        .await;
        assert_eq!(early.failures, vec!["m1".to_owned()]);
        assert_eq!(
            *store.counter.lock().unwrap(),
            0,
            "the pre-seal joiner must not increment queue_counter"
        );
        assert!(
            store.writes.lock().unwrap().is_empty(),
            "the pre-seal joiner must leave no Positions row to collide"
        );

        // (2) The admin sets phase = PreQueue; two cohort members register on
        // shard 0. The pre-queue path claims local indices and never touches
        // `queue_counter`.
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let ids = ids_on_shard(0, 2);
        let cohort: Vec<BatchRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| rec(&format!("c{i}"), id))
            .collect();
        let outcome = process_batch(&store, "evt-1", &cohort).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 2);
        assert_eq!(
            *store.counter.lock().unwrap(),
            0,
            "pre-queue registration never increments queue_counter"
        );

        // (3) The seal `SET`s queue_counter = N (N = 2). The PRP is a
        // permutation of [0, 2), so the cohort occupies {0, 1}. Because the
        // pre-seal joiner was rejected, no `Positions` row exists at {0, 1},
        // so no numeric position is held by two visitors.
        *store.counter.lock().unwrap() = 2; // the seal's unconditional `SET queue_counter = :n`
        let live_positions: Vec<u64> = store
            .writes
            .lock()
            .unwrap()
            .iter()
            .map(|w| w.position)
            .collect();
        assert!(
            live_positions.is_empty(),
            "no pre-seal live Positions row collides with the cohort [0, 2)"
        );

        // (4) A post-seal live join lands strictly above the cohort
        // (N + 1 = 3), never overlapping {0, 1} — the guard PR #57 intended.
        *store.counters_sequence.lock().unwrap() = VecDeque::from([Some(sealed_counters(
            [2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            Phase::Active,
        ))]);
        let after = process_batch(
            &store,
            "evt-1",
            &[rec("m2", "018f3a2b-7c9d-7e1f-8009-0123456789ab")],
        )
        .await;
        assert!(after.failures.is_empty());
        let live_positions: Vec<u64> = store
            .writes
            .lock()
            .unwrap()
            .iter()
            .map(|w| w.position)
            .collect();
        assert_eq!(
            live_positions,
            vec![3],
            "post-seal live join starts at N + 1"
        );
    }

    // --- pre-queue path ------------------------------------------------------

    #[tokio::test]
    async fn prequeue_path_writes_one_row_per_record_and_no_position_row() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let ids = ids_on_shard(0, 3);
        let records: Vec<BatchRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| rec(&format!("m{i}"), id))
            .collect();
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 3);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn records_sharing_a_shard_get_consecutive_local_indices() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let shard0_ids = ids_on_shard(0, 2);
        let shard1_ids = ids_on_shard(1, 2);
        let records: Vec<BatchRecord> = shard0_ids
            .iter()
            .chain(shard1_ids.iter())
            .enumerate()
            .map(|(i, id)| rec(&format!("m{i}"), id))
            .collect();
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());

        let writes = store.prequeue_writes.lock().unwrap();
        for shard in [0usize, 1usize] {
            let mut locals: Vec<u64> = writes
                .iter()
                .filter(|w| w.shard == shard)
                .map(|w| w.local_index)
                .collect();
            locals.sort_unstable();
            assert_eq!(locals, vec![0, 1], "shard {shard} local indices");
        }
        for write in writes.iter() {
            assert_eq!(write.shard, shard_for(write.request_id.as_bytes()));
        }
    }

    #[tokio::test]
    async fn a_shard_claim_failure_is_a_store_error_and_writes_nothing_for_that_group() {
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                Phase::PreQueue,
            ))])),
            prequeue_claim_fails: true,
            ..FakeStore::default()
        };
        let ids = ids_on_shard(0, 2);
        let records: Vec<BatchRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| rec(&format!("m{i}"), id))
            .collect();
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures.len(), 2);
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    /// The handler-level burned-slot test: unlike the property test over
    /// `SealedOffsets` alone, this drives the actual batch-processing code
    /// through the `Store` port with a real write failure, and proves the
    /// gap it leaves does not collide with a neighbour's index.
    #[tokio::test]
    async fn prequeue_write_failure_burns_a_local_index_without_colliding_others() {
        let ids = ids_on_shard(0, 3);
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                Phase::PreQueue,
            ))])),
            fail_prequeue_write_for: Some(ids[1].clone()),
            ..FakeStore::default()
        };
        let records: Vec<BatchRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| rec(&format!("m{i}"), id))
            .collect();
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned()]);

        let writes = store.prequeue_writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
        let mut locals: Vec<u64> = writes.iter().map(|w| w.local_index).collect();
        locals.sort_unstable();
        assert_eq!(
            locals,
            vec![0, 2],
            "the burned index 1 leaves a gap, not a collision"
        );

        // The shard's claimed count still advanced past the burned index —
        // the fold that decides pre-queue vs. live-join at the seal reads it
        // from there, not from how many rows actually landed.
        assert_eq!(store.shard_counters.lock().unwrap()[0], 3);
    }

    #[tokio::test]
    async fn duplicate_request_id_in_one_prequeue_batch_burns_an_index_and_writes_no_position() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let dup_id = ids_on_shard(0, 1).remove(0);
        let records = vec![rec("m1", &dup_id), rec("m2", &dup_id)];

        // By the time the fix-up read runs, the event has sealed with this
        // shard's count matching the one row that actually landed. The
        // second (duplicate) local index would be a straggler if it were
        // ever classified, but Duplicate outcomes are never classified.
        let mut counts = [0u64; SHARDS];
        counts[0] = 1;
        store
            .counters_sequence
            .lock()
            .unwrap()
            .push_back(Some(sealed_counters(counts, Phase::Active)));

        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 1);
        assert!(
            store.writes.lock().unwrap().is_empty(),
            "no Positions row for either copy"
        );
    }

    #[tokio::test]
    async fn fixup_read_failure_is_not_a_batch_failure() {
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                Phase::PreQueue,
            ))])),
            counters_fail_from_call: Some(2), // the top-of-batch read succeeds; the fix-up fails
            ..FakeStore::default()
        };
        let records = vec![rec("m1", VALID_ID)];
        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 1);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn seal_landing_mid_batch_splits_stragglers_from_counted_rows() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);

        let counted_id = ids_on_shard(0, 1).remove(0);
        let straggler_id = ids_on_shard(1, 1).remove(0);
        let records = vec![rec("m1", &counted_id), rec("m2", &straggler_id)];

        // Sealed at fix-up time: shard 0 (the counted id) has an issued count
        // of 1, so its local index 0 is in range; shard 1 (the straggler)
        // has an issued count of 0, so its local index 0 is already past it.
        let mut counts = [0u64; SHARDS];
        counts[0] = 1;
        store
            .counters_sequence
            .lock()
            .unwrap()
            .push_back(Some(sealed_counters(counts, Phase::Active)));

        let outcome = process_batch(&store, "evt-1", &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 2);

        let live_writes = store.writes.lock().unwrap();
        assert_eq!(
            live_writes.len(),
            1,
            "only the straggler gets a live position"
        );
        assert_eq!(live_writes[0].request_id, straggler_id);
        assert!(
            live_writes[0].position >= 1,
            "straggler position must be >= the cohort size"
        );
    }
}
