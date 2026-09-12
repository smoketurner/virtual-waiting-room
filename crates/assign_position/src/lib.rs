//! Batch processing for the `assign_position` Lambda: live-join positions and
//! pre-queue registration.
//!
//! Consumes a batch of enqueued join messages. Each record is classified
//! first (issue #59): its body must parse, its `event_id` must match, and
//! under [`EntryPolicy::Ticketed`] it must carry a ticket that verifies and a
//! `request_id` that equals the value derived from the ticket's subject. A
//! record that fails any of these is a permanent [`DropReason`] — counted and
//! logged, never retried, never dead-lettered, since no redelivery makes
//! attacker-chosen or malformed input valid.
//!
//! Each batch then reads the event's `Counters` item once (a strongly
//! consistent `GetItem`) to decide which of two paths every accepted record in
//! the batch takes, or to reject the batch outright when no `Counters` item
//! exists yet:
//!
//! - **Rejected (unconfigured event)** — no `Counters` item exists, so the
//!   event has not been set up. Every accepted record is failed and claims
//!   nothing, surfacing the state as a hard failure rather than letting a
//!   pre-seal live join increment `queue_counter` before the seal `SET`s it
//!   to the cohort size (an unconditional `SET` that would discard the
//!   increment and let a later cohort member or post-seal joiner collide on
//!   the same numeric position).
//! - **Live join** — the event is sealed, or its phase is anything but
//!   `PreQueue`. Allocates one contiguous block of queue positions with one
//!   counter increment and writes one `Positions` row per accepted record.
//! - **Pre-queue** — the event is not sealed and its phase is `PreQueue`.
//!   Deduplicates by `request_id` (within the batch, and against any row a
//!   prior invocation already wrote) and claims one contiguous block of local
//!   indices on one randomly drawn shard for whatever remains.
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

use std::collections::HashSet;
use std::future::Future;

use serde::Deserialize;
use wr_common::{Assignment, Counters, EntryPolicy, Phase, Shard, Telemetry, TicketError};

pub mod dynamo;

/// A join message body enqueued by the ingest API.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinMessage {
    pub request_id: String,
    pub event_id: String,
    /// The client-carried entry ticket, present only under
    /// [`EntryPolicy::Ticketed`]. Absent on an open deployment.
    #[serde(default)]
    pub ticket: Option<String>,
}

/// One record from the SQS batch: the message id (for failure reporting), the
/// raw body to parse, and the join-time telemetry `main.rs` lifted from the
/// record's message attributes.
#[derive(Debug, Clone)]
pub struct BatchRecord {
    pub message_id: String,
    pub body: String,
    pub telemetry: Telemetry,
}

/// A position write to attempt: the request, the queue position it claimed,
/// and the telemetry to attach to the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionWrite {
    pub request_id: String,
    pub position: u64,
    pub telemetry: Telemetry,
    /// Whether the write may overwrite an existing row whose `status` is
    /// `expired` (issue #59). A re-join with a derived `request_id` would
    /// otherwise never be able to reclaim the id a controller-expired live
    /// join left behind, permanently stranding a visitor whose reload advice
    /// says "take a new place in line". Excluded for a request id that also
    /// holds a `PreQueue` row, so a fixed-up straggler's expired live
    /// position cannot be resurrected out from under the read path, which
    /// prefers that `PreQueue` row and would keep serving the stale value.
    pub allow_expired_overwrite: bool,
}

/// A pre-queue registration write to attempt: the request, the `(shard, local
/// index)` it claimed, and the telemetry to attach to the row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreQueueWrite {
    pub request_id: String,
    pub shard: Shard,
    pub local_index: u64,
    pub telemetry: Telemetry,
}

/// The persistence port the batch logic drives.
pub trait Store {
    /// `ADD queue_counter :n` with `ALL_NEW` for one event, returning the block
    /// end. The claimed block is `[end - n + 1, end]`. `n` is the count of
    /// accepted records and is always `>= 1` when called.
    fn claim_block(
        &self,
        event_id: &str,
        n: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes one position row with [`PositionWrite::allow_expired_overwrite`]
    /// choosing the condition: `attribute_not_exists(request_id)` alone, or
    /// widened with `OR status = expired`. A conditional-check failure (a
    /// duplicate, or a non-expired row already claiming the id) is reported as
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
        shard: Shard,
        count: u64,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes one pre-queue row with `attribute_not_exists(r)`. A duplicate
    /// request id reports `Ok(WriteOutcome::Duplicate)`, exactly as
    /// [`Store::put_position`] does.
    fn put_prequeue(
        &self,
        write: &PreQueueWrite,
    ) -> impl Future<Output = Result<WriteOutcome, StoreError>> + Send;

    /// Reads which of `request_ids` already have a `PreQueue` row, via one
    /// consistent `BatchGetItem` (issue #59). Used two ways: to skip a
    /// pre-queue claim for an id that already registered (a browser that
    /// denies every storage tier re-sends the join on each reload, and
    /// without this check each reload burned a fresh index), and to keep a
    /// live-join expired-row overwrite from resurrecting a straggler who also
    /// holds a `PreQueue` row (see [`PositionWrite::allow_expired_overwrite`]).
    ///
    /// This is an optimization over the authoritative `attribute_not_exists`
    /// guard, never a substitute for it: a store error, or an id
    /// `BatchGetItem` could not confirm (`UnprocessedKeys`), degrades to
    /// "unknown" — simply absent from the returned set — so the caller falls
    /// through to attempting the claim exactly as it would with no dedupe at
    /// all, never losing a registration to a failed read.
    fn registered_ids(
        &self,
        request_ids: &[String],
    ) -> impl Future<Output = Result<HashSet<String>, StoreError>> + Send;
}

/// The result of a single conditional position or pre-queue write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    Written,
    /// The write's condition rejected it: a duplicate request id, or (for a
    /// live-join write) a row already claiming the id whose status is not
    /// `expired`.
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

/// Why a record was permanently rejected: never retried, never
/// dead-lettered, since no redelivery makes attacker-chosen or malformed
/// input valid. Counted by [`DropCounts`] and logged once per batch under the
/// fixed field names a metric filter keys on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The body did not parse as JSON, or a required field was missing or the
    /// wrong shape (including a malformed ticket under a ticketed policy, and
    /// a malformed `request_id` under an open one).
    BadShape,
    /// The message's `event_id` does not match this deployment's.
    WrongEvent,
    /// A ticketed deployment received a join with no ticket.
    NoTicket,
    /// The ticket's signature does not verify under the configured key.
    BadSignature,
    /// The ticket has expired.
    Expired,
    /// The ticket is not yet valid (`nbf` in the future).
    NotYetValid,
    /// The ticket's `aud` does not name this event.
    WrongAudience,
    /// The ticket's `sub` fails the opaque-subject shape check.
    BadSubject,
    /// The supplied `request_id` does not equal the value derived from the
    /// ticket's verified subject.
    IdMismatch,
}

/// Per-reason drop counts for one batch, logged under fixed field names so a
/// `CloudWatch` metric filter can extract `total` mechanically.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DropCounts {
    bad_shape: u64,
    wrong_event: u64,
    no_ticket: u64,
    bad_signature: u64,
    expired: u64,
    not_yet_valid: u64,
    wrong_audience: u64,
    bad_subject: u64,
    id_mismatch: u64,
}

impl DropCounts {
    fn record(&mut self, reason: DropReason) {
        match reason {
            DropReason::BadShape => self.bad_shape += 1,
            DropReason::WrongEvent => self.wrong_event += 1,
            DropReason::NoTicket => self.no_ticket += 1,
            DropReason::BadSignature => self.bad_signature += 1,
            DropReason::Expired => self.expired += 1,
            DropReason::NotYetValid => self.not_yet_valid += 1,
            DropReason::WrongAudience => self.wrong_audience += 1,
            DropReason::BadSubject => self.bad_subject += 1,
            DropReason::IdMismatch => self.id_mismatch += 1,
        }
    }

    fn total(&self) -> u64 {
        self.bad_shape
            + self.wrong_event
            + self.no_ticket
            + self.bad_signature
            + self.expired
            + self.not_yet_valid
            + self.wrong_audience
            + self.bad_subject
            + self.id_mismatch
    }
}

/// The outcome of classifying one record.
enum RecordVerdict {
    /// Take a position.
    Accept(JoinMessage),
    /// Permanently unacceptable. Counted and logged; never retried, never
    /// dead-lettered.
    Reject(DropReason),
}

/// Parses and validates one record's body against `policy`.
///
/// Order: shape, then envelope `event_id`, then (under a ticketed policy)
/// the ticket itself, then the derived id. An open policy only shape-checks
/// `request_id`, exactly as the pre-issue-#59 `UUIDv7` check did, except the
/// shape it accepts is no longer pinned to version 7 (nothing in this
/// deployment depends on that ordering).
fn classify_record(body: &str, policy: &EntryPolicy, event_id: &str, now: u64) -> RecordVerdict {
    let msg: JoinMessage = match serde_json::from_str(body) {
        Ok(msg) => msg,
        Err(_err) => return RecordVerdict::Reject(DropReason::BadShape),
    };
    if msg.event_id != event_id {
        return RecordVerdict::Reject(DropReason::WrongEvent);
    }

    match policy {
        EntryPolicy::Open => {
            if wr_common::is_uuid_shape(&msg.request_id) {
                RecordVerdict::Accept(msg)
            } else {
                RecordVerdict::Reject(DropReason::BadShape)
            }
        }
        EntryPolicy::Ticketed(key) => {
            let Some(ticket) = msg.ticket.as_deref() else {
                return RecordVerdict::Reject(DropReason::NoTicket);
            };
            let subject = match wr_common::verify_ticket(key, ticket, event_id, now) {
                Ok(subject) => subject,
                Err(TicketError::Malformed) => return RecordVerdict::Reject(DropReason::BadShape),
                Err(TicketError::BadSignature) => {
                    return RecordVerdict::Reject(DropReason::BadSignature);
                }
                Err(TicketError::Expired) => return RecordVerdict::Reject(DropReason::Expired),
                Err(TicketError::NotYetValid) => {
                    return RecordVerdict::Reject(DropReason::NotYetValid);
                }
                Err(TicketError::WrongAudience) => {
                    return RecordVerdict::Reject(DropReason::WrongAudience);
                }
                Err(TicketError::BadSubject) => {
                    return RecordVerdict::Reject(DropReason::BadSubject);
                }
            };
            let derived = wr_common::derive_request_id(event_id, &subject);
            if msg.request_id != derived {
                return RecordVerdict::Reject(DropReason::IdMismatch);
            }
            RecordVerdict::Accept(msg)
        }
    }
}

/// Processes one SQS batch.
///
/// Every record that fails [`classify_record`] is counted, logged once per
/// batch, and dropped without becoming a batch failure — an attacker-chosen or
/// malformed record is not something a retry ever fixes. The remaining
/// accepted records all take the same path — live join or pre-queue — decided
/// once from the event's `Counters` item (see the module docs). A `Counters`
/// read failure, or a missing `Counters` item (the event has not been set up
/// yet), fails every accepted record and claims nothing.
///
/// `shard` is drawn once by the caller for the whole invocation (issue #59):
/// it is server-random rather than derived from `request_id`, so a batch that
/// takes the pre-queue path claims one contiguous block on this one shard
/// rather than grouping by shard and claiming up to ten. It is unused on the
/// live-join path.
pub async fn process_batch<S: Store>(
    store: &S,
    event_id: &str,
    policy: &EntryPolicy,
    shard: Shard,
    now: u64,
    records: &[BatchRecord],
) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();
    let mut drops = DropCounts::default();
    let mut valid = Vec::new();
    for record in records {
        match classify_record(&record.body, policy, event_id, now) {
            RecordVerdict::Accept(msg) => {
                valid.push((record.message_id.clone(), msg, record.telemetry.clone()));
            }
            RecordVerdict::Reject(reason) => drops.record(reason),
        }
    }

    if drops.total() > 0 {
        tracing::warn!(
            event = "join_dropped",
            bad_shape = drops.bad_shape,
            wrong_event = drops.wrong_event,
            no_ticket = drops.no_ticket,
            bad_signature = drops.bad_signature,
            expired = drops.expired,
            not_yet_valid = drops.not_yet_valid,
            wrong_audience = drops.wrong_audience,
            bad_subject = drops.bad_subject,
            id_mismatch = drops.id_mismatch,
            total = drops.total(),
            "dropped invalid join records"
        );
    }

    if valid.is_empty() {
        return outcome;
    }

    let counters = match store.load_counters(event_id).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            // No `Counters` item: the event has not been set up yet. Failing
            // every accepted record prevents a pre-seal live join from
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
            for (message_id, _, _) in &valid {
                outcome.failures.push(message_id.clone());
            }
            return outcome;
        }
        Err(err) => {
            tracing::error!(error = %err, "counters read failed; retrying batch");
            for (message_id, _, _) in &valid {
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
        process_prequeue_batch(store, event_id, shard, valid, &mut outcome).await;
    }

    outcome
}

/// Degrades a [`Store::registered_ids`] failure to "unknown" — an empty set,
/// so every caller falls through to attempting its claim exactly as it would
/// with no dedupe at all. This is the "unknown, claim anyway" degrade path:
/// the authoritative `attribute_not_exists` guard (or, for a live-join
/// overwrite, the widened status check) still catches an actual duplicate.
async fn registered_ids_or_unknown<S: Store>(store: &S, request_ids: &[String]) -> HashSet<String> {
    match store.registered_ids(request_ids).await {
        Ok(ids) => ids,
        Err(err) => {
            tracing::warn!(error = %err, "registered_ids read failed; treating every id as unknown");
            HashSet::new()
        }
    }
}

/// Claims one contiguous block of queue positions for the whole valid set and
/// writes one `Positions` row per record.
///
/// Before writing, checks which request ids also hold a `PreQueue` row
/// (issue #59, R4): those are excluded from the expired-row overwrite, so a
/// fixed-up straggler's controller-expired live position cannot be
/// resurrected out from under `/queue_num`, which prefers the `PreQueue` row
/// and would otherwise keep answering with the stale, already-passed
/// position while `generate_token` polls forever without ever reaching a
/// terminal 410.
async fn process_live_batch<S: Store>(
    store: &S,
    event_id: &str,
    valid: Vec<(String, JoinMessage, Telemetry)>,
    outcome: &mut BatchOutcome,
) {
    let ids: Vec<String> = valid
        .iter()
        .map(|(_, msg, _)| msg.request_id.clone())
        .collect();
    let has_prequeue_row = registered_ids_or_unknown(store, &ids).await;

    let n = valid.len() as u64;
    let end = match store.claim_block(event_id, n).await {
        Ok(end) => end,
        Err(err) => {
            tracing::error!(error = %err, "queue_counter claim failed; retrying batch");
            for (message_id, _, _) in &valid {
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

    for (offset, (message_id, msg, telemetry)) in valid.into_iter().enumerate() {
        let write = PositionWrite {
            allow_expired_overwrite: !has_prequeue_row.contains(&msg.request_id),
            request_id: msg.request_id,
            // Saturating for the same reason `start` itself is: a wrapped
            // position is indistinguishable from a valid low one, never a
            // harmless gap.
            position: start.saturating_add(offset as u64),
            telemetry,
        };
        if let Err(err) = store.put_position(&write).await {
            tracing::error!(error = %err, message_id = %message_id, "position write failed");
            outcome.failures.push(message_id);
        }
    }
}

/// Deduplicates the valid set by `request_id` (within the batch, and against
/// any row a prior invocation already wrote), then claims one contiguous
/// block of local indices on `shard` for whatever remains and writes one
/// `PreQueue` row per record. Then checks whether the event sealed mid-batch
/// and gives every row this invocation actually wrote that now resolves past
/// the shard's count a real live position.
async fn process_prequeue_batch<S: Store>(
    store: &S,
    event_id: &str,
    shard: Shard,
    valid: Vec<(String, JoinMessage, Telemetry)>,
    outcome: &mut BatchOutcome,
) {
    // In-batch dedupe: a browser that denies every storage tier re-sends the
    // join on each poll while storage-denied, and (independently) up to the
    // 1-second batching window can bundle several reloads of the same
    // visitor. Keeping only the first occurrence claims one index for all of
    // them rather than one each.
    let mut seen_in_batch = HashSet::new();
    let mut deduped = Vec::with_capacity(valid.len());
    for (message_id, msg, telemetry) in valid {
        if seen_in_batch.insert(msg.request_id.clone()) {
            deduped.push((message_id, msg, telemetry));
        }
    }

    // Cross-invocation dedupe: an id that already has a PreQueue row from an
    // earlier invocation claims nothing here — not a failure, not a claim, not
    // a write. `attribute_not_exists(r)` on the write below stays the
    // authoritative guard; this only avoids paying for the claim.
    let ids: Vec<String> = deduped
        .iter()
        .map(|(_, msg, _)| msg.request_id.clone())
        .collect();
    let already_registered = registered_ids_or_unknown(store, &ids).await;
    let group: Vec<(String, JoinMessage, Telemetry)> = deduped
        .into_iter()
        .filter(|(_, msg, _)| !already_registered.contains(&msg.request_id))
        .collect();

    if group.is_empty() {
        return;
    }

    let count = group.len() as u64;
    let start = match store.claim_prequeue_block(event_id, shard, count).await {
        Ok(start) => start,
        Err(err) => {
            tracing::error!(error = %err, shard = shard.index(), "prequeue shard claim failed; retrying batch");
            for (message_id, _, _) in &group {
                outcome.failures.push(message_id.clone());
            }
            return;
        }
    };

    // What this invocation actually wrote (never a Duplicate — see the module
    // docs), carried into the fix-up below.
    let mut written = Vec::new();

    for (offset, (message_id, msg, telemetry)) in group.into_iter().enumerate() {
        let write = PreQueueWrite {
            request_id: msg.request_id,
            shard,
            // Saturating: a wrapped local index would be a duplicate global
            // index, not a gap, which is what the shard claim's own
            // `checked_sub` guards against — but `start` and `count` are both
            // small relative to `u64::MAX` here, so this is defensive, not
            // reachable in practice.
            local_index: start.saturating_add(offset as u64),
            telemetry,
        };
        match store.put_prequeue(&write).await {
            Ok(WriteOutcome::Written) => written.push(write),
            // Burned index: the authoritative row belongs to an earlier
            // invocation with a different local index. Never classified
            // below, and never a batch failure.
            Ok(WriteOutcome::Duplicate) => {
                tracing::debug!(message_id = %message_id, "prequeue write was a duplicate; index burned");
            }
            Err(err) => {
                // Accepted residual: a write that timed out on the caller
                // side but actually landed is indistinguishable here from one
                // that truly failed, so it is classified as "no row" and
                // reported as a batch failure. Redelivery finds the event
                // sealed by then and takes the live path, which can mint a
                // second Positions row for a request id that already holds a
                // counted PreQueue row, demoting a visitor who was counted
                // into the cohort. The fix-up below cannot catch it, because
                // it only classifies writes this invocation observed as
                // Written. Closing it costs a PreQueue GetItem per record on
                // the live path, which is not worth paying on every live join
                // to guard against one rare timing window; left open.
                tracing::error!(error = %err, message_id = %message_id, "prequeue write failed");
                outcome.failures.push(message_id);
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
                sealed
                    .offsets
                    .assign(write.shard.index(), write.local_index),
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
            telemetry: write.telemetry,
            // A straggler fix-up is the first live write for this id; there
            // is no row yet to collide with an expired one.
            allow_expired_overwrite: true,
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

    use wr_common::{SHARDS, SealedOffsets, StoredControl};

    use super::*;

    const VALID_ID: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const BAD_SHAPE_ID: &str = "not-a-uuid";

    fn shard(n: usize) -> Shard {
        Shard::new(n).unwrap()
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

        /// Ids `registered_ids` reports as already holding a `PreQueue` row.
        already_registered: Mutex<HashSet<String>>,
        registered_ids_fails: bool,
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
                already_registered: Mutex::new(HashSet::new()),
                registered_ids_fails: false,
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
            shard: Shard,
            count: u64,
        ) -> impl std::future::Future<Output = Result<u64, StoreError>> + Send {
            let result = if self.prequeue_claim_fails {
                Err(StoreError("shard claim down".to_owned()))
            } else {
                let mut counters = self.shard_counters.lock().unwrap();
                let start = counters[shard.index()];
                counters[shard.index()] += count;
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

        fn registered_ids(
            &self,
            request_ids: &[String],
        ) -> impl std::future::Future<Output = Result<HashSet<String>, StoreError>> + Send {
            let result = if self.registered_ids_fails {
                Err(StoreError("registered_ids down".to_owned()))
            } else {
                let already = self.already_registered.lock().unwrap();
                Ok(request_ids
                    .iter()
                    .filter(|id| already.contains(*id))
                    .cloned()
                    .collect())
            };
            std::future::ready(result)
        }
    }

    fn rec(message_id: &str, request_id: &str) -> BatchRecord {
        BatchRecord {
            message_id: message_id.to_owned(),
            body: format!(r#"{{"request_id":"{request_id}","event_id":"evt-1"}}"#),
            telemetry: Telemetry::default(),
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
            starts_at: None,
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
            starts_at: None,
        }
    }

    async fn run_open(store: &FakeStore, records: &[BatchRecord]) -> BatchOutcome {
        process_batch(store, "evt-1", &EntryPolicy::Open, shard(0), 0, records).await
    }

    // --- live path (unchanged behaviour) ------------------------------------

    #[test]
    fn open_policy_uuid_shape_validation() {
        assert!(wr_common::is_uuid_shape(VALID_ID));
        // A v4-shaped id is accepted too: the shape check no longer pins a
        // version nibble (issue #59, M2).
        assert!(wr_common::is_uuid_shape(
            "018f3a2b-7c9d-4e1f-abcd-0123456789ab"
        ));
        assert!(!wr_common::is_uuid_shape(BAD_SHAPE_ID));
        assert!(!wr_common::is_uuid_shape(
            "018f3a2b7c9d7e1fabcd0123456789ab"
        ));
        assert!(!wr_common::is_uuid_shape(""));
    }

    #[tokio::test]
    async fn valid_batch_claims_contiguous_block_no_duplicates() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-8003-0123456789ab"),
        ];
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        let writes = store.writes.lock().unwrap();
        let mut positions: Vec<u64> = writes.iter().map(|w| w.position).collect();
        positions.sort_unstable();
        assert_eq!(positions, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn invalid_records_are_dropped_not_failed() {
        let store = FakeStore::default();
        let records = vec![
            rec("m1", VALID_ID),
            BatchRecord {
                message_id: "m2".to_owned(),
                body: "not json".to_owned(),
                telemetry: Telemetry::default(),
            },
            rec("m3", BAD_SHAPE_ID),
        ];
        let outcome = run_open(&store, &records).await;
        // Rejected records are dropped, not retried or dead-lettered.
        assert!(outcome.failures.is_empty());
        // Only the one valid record claimed a position: counter incremented by 1.
        assert_eq!(*store.counter.lock().unwrap(), 1);
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn all_invalid_claims_nothing_and_fails_nothing() {
        let store = FakeStore::default();
        let records = vec![BatchRecord {
            message_id: "m1".to_owned(),
            body: "garbage".to_owned(),
            telemetry: Telemetry::default(),
        }];
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(*store.counter.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn counter_claim_failure_retries_whole_valid_set() {
        let store = FakeStore {
            claim_fails: true,
            ..FakeStore::default()
        };
        let records = vec![rec("m1", VALID_ID)];
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.writes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_request_id_with_a_prequeue_row_does_not_get_the_expired_overwrite() {
        // The R4 fix: a live-join write for an id that also holds a PreQueue
        // row (the fixed-up-straggler case) must not be allowed to overwrite
        // an expired row, or /queue_num (which prefers the PreQueue row) would
        // keep serving a stale position forever.
        let store = FakeStore::default();
        let id = "018f3a2b-7c9d-7e1f-8001-0123456789ab".to_owned();
        store.already_registered.lock().unwrap().insert(id.clone());
        let records = vec![rec("m1", &id)];
        run_open(&store, &records).await;
        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert!(!writes[0].allow_expired_overwrite);
    }

    #[tokio::test]
    async fn an_id_with_no_prequeue_row_gets_the_expired_overwrite() {
        let store = FakeStore::default();
        let records = vec![rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab")];
        run_open(&store, &records).await;
        let writes = store.writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert!(writes[0].allow_expired_overwrite);
    }

    #[tokio::test]
    async fn a_registered_ids_failure_degrades_to_claim_anyway() {
        let store = FakeStore {
            registered_ids_fails: true,
            ..FakeStore::default()
        };
        let records = vec![rec("m1", VALID_ID)];
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
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
            let outcome = run_open(&store, &records).await;
            assert!(outcome.failures.is_empty(), "{phase:?}");
            assert_eq!(store.writes.lock().unwrap().len(), 1, "{phase:?}");
        }
    }

    #[tokio::test]
    async fn mismatched_event_id_is_dropped_not_failed() {
        let store = FakeStore::default();
        let records = vec![rec("m1", VALID_ID)]; // rec() hardcodes event_id "evt-1"
        let outcome = process_batch(
            &store,
            "evt-other",
            &EntryPolicy::Open,
            shard(0),
            0,
            &records,
        )
        .await;
        assert!(outcome.failures.is_empty());
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
        let outcome = run_open(&store, &records).await;
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
        let outcome = run_open(&store, &records).await;
        assert_eq!(outcome.failures, vec!["m1".to_owned(), "m2".to_owned()]);
        assert_eq!(
            *store.counter.lock().unwrap(),
            0,
            "no live block claimed on an unconfigured event"
        );
        assert!(store.writes.lock().unwrap().is_empty());
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    // --- pre-queue path ------------------------------------------------------

    #[tokio::test]
    async fn prequeue_path_writes_one_row_per_record_and_no_position_row() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let records = vec![
            rec("m0", "018f3a2b-7c9d-7e1f-8000-0123456789ab"),
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
        ];
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 3);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn records_in_one_batch_get_consecutive_local_indices_on_the_drawn_shard() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let records = vec![
            rec("m0", "018f3a2b-7c9d-7e1f-8000-0123456789ab"),
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
            rec("m2", "018f3a2b-7c9d-7e1f-8002-0123456789ab"),
            rec("m3", "018f3a2b-7c9d-7e1f-8003-0123456789ab"),
        ];
        let outcome =
            process_batch(&store, "evt-1", &EntryPolicy::Open, shard(4), 0, &records).await;
        assert!(outcome.failures.is_empty());

        let writes = store.prequeue_writes.lock().unwrap();
        assert_eq!(writes.len(), 4);
        for write in writes.iter() {
            assert_eq!(
                write.shard,
                shard(4),
                "every write lands on the drawn shard"
            );
        }
        let mut locals: Vec<u64> = writes.iter().map(|w| w.local_index).collect();
        locals.sort_unstable();
        assert_eq!(locals, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn a_shard_claim_failure_is_a_store_error_and_writes_nothing() {
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                Phase::PreQueue,
            ))])),
            prequeue_claim_fails: true,
            ..FakeStore::default()
        };
        let records = vec![
            rec("m0", "018f3a2b-7c9d-7e1f-8000-0123456789ab"),
            rec("m1", "018f3a2b-7c9d-7e1f-8001-0123456789ab"),
        ];
        let outcome = run_open(&store, &records).await;
        assert_eq!(outcome.failures.len(), 2);
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
    }

    /// The handler-level burned-slot test: unlike the property test over
    /// `SealedOffsets` alone, this drives the actual batch-processing code
    /// through the `Store` port with a real write failure, and proves the
    /// gap it leaves does not collide with a neighbour's index.
    #[tokio::test]
    async fn prequeue_write_failure_burns_a_local_index_without_colliding_others() {
        let ids = [
            "018f3a2b-7c9d-7e1f-8000-0123456789ab",
            "018f3a2b-7c9d-7e1f-8001-0123456789ab",
            "018f3a2b-7c9d-7e1f-8002-0123456789ab",
        ];
        let store = FakeStore {
            counters_sequence: Mutex::new(VecDeque::from([Some(counters_with_phase(
                Phase::PreQueue,
            ))])),
            fail_prequeue_write_for: Some(ids[1].to_owned()),
            ..FakeStore::default()
        };
        let records: Vec<BatchRecord> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| rec(&format!("m{i}"), id))
            .collect();
        let outcome = run_open(&store, &records).await;
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
    async fn duplicate_request_id_in_one_prequeue_batch_is_deduped_and_burns_no_index() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let dup_id = "018f3a2b-7c9d-7e1f-8000-0123456789ab";
        let records = vec![rec("m1", dup_id), rec("m2", dup_id)];

        let mut counts = [0u64; SHARDS];
        counts[0] = 1;
        store
            .counters_sequence
            .lock()
            .unwrap()
            .push_back(Some(sealed_counters(counts, Phase::Active)));

        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(
            store.prequeue_writes.lock().unwrap().len(),
            1,
            "in-batch dedupe claims one index for both copies"
        );
        assert!(
            store.writes.lock().unwrap().is_empty(),
            "no Positions row for either copy"
        );
        assert_eq!(
            store.shard_counters.lock().unwrap()[0],
            1,
            "the duplicate must not burn a second index"
        );
    }

    #[tokio::test]
    async fn an_id_that_already_has_a_prequeue_row_claims_nothing_on_reload() {
        // The benign case #79 left open: a browser that denies every storage
        // tier re-sends the join on each poll. Without this check every
        // reload burned a fresh index; with it, only the first claims one.
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);
        let id = "018f3a2b-7c9d-7e1f-8000-0123456789ab".to_owned();
        store.already_registered.lock().unwrap().insert(id.clone());

        let records = vec![rec("m1", &id)];
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert!(store.prequeue_writes.lock().unwrap().is_empty());
        assert_eq!(store.shard_counters.lock().unwrap()[0], 0);
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
        let outcome = run_open(&store, &records).await;
        assert!(outcome.failures.is_empty());
        assert_eq!(store.prequeue_writes.lock().unwrap().len(), 1);
        assert!(store.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn seal_landing_mid_batch_splits_stragglers_from_counted_rows() {
        let store = FakeStore::default();
        *store.counters_sequence.lock().unwrap() =
            VecDeque::from([Some(counters_with_phase(Phase::PreQueue))]);

        let counted_id = "018f3a2b-7c9d-7e1f-8000-0123456789ab".to_owned();
        let straggler_id = "018f3a2b-7c9d-7e1f-8001-0123456789ab".to_owned();
        let records = vec![rec("m1", &counted_id), rec("m2", &straggler_id)];

        // Sealed at fix-up time: the drawn shard's issued count is 1, so local
        // index 0 (the first write) is in range and local index 1 (the
        // second) is already past it.
        let mut counts = [0u64; SHARDS];
        counts[0] = 1;
        store
            .counters_sequence
            .lock()
            .unwrap()
            .push_back(Some(sealed_counters(counts, Phase::Active)));

        let outcome = run_open(&store, &records).await;
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

    // --- entry ticket policy (issue #59) ------------------------------------

    mod ticketed {
        use wr_common::{EntryPolicy, TicketKey};

        use super::*;

        /// A real P-256 test key pair, generated once for these tests.
        const X_B64: &str = "q_1C5Qxlm0UznPTg8b4ztGRqGDTPcXTowgzHboJ9WBc";
        const Y_B64: &str = "50IwinLZ2kFdic8gGwFfQ2d6X6UI6s6V3ek9fcjPB-I";
        const PRIVATE_KEY_PKCS8_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgCM8Gu+5Pe7vq5HGC\
             lTlTi057LV3NH5ii+Dukp6okfsyhRANCAASr/ULlDGWbRTOc9ODxvjO0ZGoYNM9xdOjCDMdugn1YF+dCMIpy\
             2dpBXYnPIBsBX0Nnel+lCOrOld3pPX3Izwfi";

        const SUBJECT: &str = "0123456789abcdefghijklmnopqrstuvwxyz-_ABCD";

        fn ticket_key() -> TicketKey {
            let json = format!(r#"{{"kty":"EC","crv":"P-256","x":"{X_B64}","y":"{Y_B64}"}}"#);
            TicketKey::from_jwk_json(&json).unwrap()
        }

        fn policy() -> EntryPolicy {
            EntryPolicy::Ticketed(ticket_key())
        }

        fn sign(event_id: &str, sub: &str, exp: u64) -> String {
            use base64::Engine as _;
            use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
            use serde::Serialize;

            #[derive(Serialize)]
            struct Claims<'a> {
                aud: &'a str,
                sub: &'a str,
                exp: u64,
            }
            let der = base64::engine::general_purpose::STANDARD
                .decode(PRIVATE_KEY_PKCS8_B64)
                .unwrap();
            let key = EncodingKey::from_ec_der(&der);
            encode(
                &Header::new(Algorithm::ES256),
                &Claims {
                    aud: event_id,
                    sub,
                    exp,
                },
                &key,
            )
            .unwrap()
        }

        fn rec_with_ticket(message_id: &str, request_id: &str, ticket: &str) -> BatchRecord {
            BatchRecord {
                message_id: message_id.to_owned(),
                body: format!(
                    r#"{{"request_id":"{request_id}","event_id":"evt-1","ticket":"{ticket}"}}"#
                ),
                telemetry: Telemetry::default(),
            }
        }

        /// The `request_id` a valid, unexpired ticket for `sub` would derive
        /// to, computed the same way `classify_record` does: verify, then
        /// derive from the resulting subject.
        fn expected_id(sub: &str) -> String {
            let ticket = sign("evt-1", sub, 2_000_000_000);
            let subject = wr_common::verify_ticket(&ticket_key(), &ticket, "evt-1", 0).unwrap();
            wr_common::derive_request_id("evt-1", &subject)
        }

        #[tokio::test]
        async fn n_registrations_under_one_identity_yield_one_position() {
            let store = FakeStore::default();
            let policy = policy();
            let subject_id = expected_id(SUBJECT);

            // Five fresh tickets, same subject: each derives the identical
            // request_id, so the fifth "registration" is really four retries.
            let records: Vec<BatchRecord> = (0..5)
                .map(|i| {
                    let ticket = sign("evt-1", SUBJECT, 2_000_000_000);
                    rec_with_ticket(&format!("m{i}"), &subject_id, &ticket)
                })
                .collect();
            let outcome =
                process_batch(&store, "evt-1", &policy, shard(0), 1_000_000_000, &records).await;
            assert!(outcome.failures.is_empty());
            assert_eq!(
                store.writes.lock().unwrap().len(),
                1,
                "five registrations under one identity yield exactly one position"
            );
        }

        #[tokio::test]
        async fn a_tampered_request_id_is_dropped() {
            let store = FakeStore::default();
            let ticket = sign("evt-1", SUBJECT, 2_000_000_000);
            // A client-chosen id that does not match the ticket's derivation.
            let records = vec![rec_with_ticket("m1", VALID_ID, &ticket)];
            let outcome = process_batch(
                &store,
                "evt-1",
                &policy(),
                shard(0),
                1_000_000_000,
                &records,
            )
            .await;
            assert!(outcome.failures.is_empty());
            assert!(store.writes.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn a_missing_ticket_is_dropped_without_client_visible_failure() {
            let store = FakeStore::default();
            let records = vec![rec("m1", VALID_ID)]; // no ticket field
            let outcome = process_batch(
                &store,
                "evt-1",
                &policy(),
                shard(0),
                1_000_000_000,
                &records,
            )
            .await;
            // 200 at join either way: never a batch failure.
            assert!(outcome.failures.is_empty());
            assert!(store.writes.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn an_expired_ticket_is_dropped() {
            let store = FakeStore::default();
            let policy = policy();
            let ticket = sign("evt-1", SUBJECT, 1000);
            let subject = wr_common::verify_ticket(&ticket_key(), &ticket, "evt-1", 500).unwrap();
            let request_id = wr_common::derive_request_id("evt-1", &subject);
            let records = vec![rec_with_ticket("m1", &request_id, &ticket)];
            let outcome =
                process_batch(&store, "evt-1", &policy, shard(0), 2_000_000_000, &records).await;
            assert!(outcome.failures.is_empty());
            assert!(store.writes.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn wrong_event_ticket_is_dropped() {
            let store = FakeStore::default();
            let ticket = sign("other-event", SUBJECT, 2_000_000_000);
            let records = vec![rec_with_ticket("m1", VALID_ID, &ticket)];
            let outcome = process_batch(
                &store,
                "evt-1",
                &policy(),
                shard(0),
                1_000_000_000,
                &records,
            )
            .await;
            assert!(outcome.failures.is_empty());
            assert!(store.writes.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn open_policy_still_behaves_exactly_as_before() {
            let store = FakeStore::default();
            let records = vec![rec("m1", VALID_ID)];
            let outcome =
                process_batch(&store, "evt-1", &EntryPolicy::Open, shard(0), 0, &records).await;
            assert!(outcome.failures.is_empty());
            assert_eq!(store.writes.lock().unwrap().len(), 1);
        }
    }
}
