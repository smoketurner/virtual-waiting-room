//! `DynamoDB` item shapes for the MVP tables, (de)serialized via `serde_dynamo`.
//!
//! Attribute names match the deployed tables.
//!
//! The striped counters are deliberately NOT here. A shard is its own item
//! under its own partition key (see [`crate::expr::prequeue_shard_key`]),
//! because `DynamoDB` caps writes at 1,000 per second per partition key —
//! holding the shards as attributes on this item would put all of them back
//! under one budget and distribute nothing. Readers that need them fetch them
//! explicitly.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use serde::{Deserialize, Serialize};

use crate::ids::{Phase, StoredControl};
use crate::permutation::{Assignment, SHARDS, SealedOffsets, Seed};

/// The admission status of a written [`Position`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    Issued,
    Completed,
    Abandoned,
    Expired,
}

/// A `PreQueue` row: shard `s` and local index `l` for a request, written at
/// registration. The global index is derived on read, never stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreQueueItem {
    /// `request_id` (`UUIDv7`), the partition key.
    pub r: String,
    /// Shard index in `0..SHARDS`.
    pub s: u8,
    /// Local index within the shard.
    pub l: u64,
    /// Registration timestamp, epoch seconds.
    pub t: u64,
}

/// A `Positions` row, written lazily when a visitor is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionItem {
    pub request_id: String,
    /// The queue position this request holds. For a live joiner it is the value
    /// claimed from `queue_counter`; a pre-queue member's position is derived
    /// from the seed on read and never written here.
    pub queue_position: u64,
    /// Server-stamped arrival time in epoch seconds. Authoritative: the
    /// `UUIDv7` request id also carries a timestamp, but that one is
    /// client-supplied and untrusted.
    pub entry_time: u64,
    pub status: PositionStatus,
    /// Post-event storage reclamation only, never the expiry mechanism. A
    /// position is expired by the controller when the admission cursor has
    /// passed it and it was not claimed; there is deliberately no per-row
    /// deadline, because a deadline set when the position is issued expires
    /// people for waiting the length of the queue they are waiting in.
    pub ttl: u64,
}

/// The single `Counters` item for an event: all sequences, the sharded
/// pre-queue counter, phase, and the seal outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counters {
    pub event_id: String,
    pub phase: Phase,
    /// Live-join sequence; starts at `participant_count` after the seal so live
    /// joiners are numbered behind the whole pre-queue cohort.
    pub queue_counter: u64,
    /// Admission cursor advanced by the controller.
    pub serving_counter: u64,
    /// Set only at the seal, absent before: the 256-bit permutation seed.
    pub shuffle_seed: Option<[u8; 32]>,
    /// Set at the seal: cohort size `N`, the sum of the pre-queue shards.
    pub participant_count: Option<u64>,
    /// Set at the seal: prefix offsets `offset[s] = Σ counts[0..s)`.
    pub prequeue_offsets: Option<[u64; SHARDS]>,
    /// Operator broadcast text shown to waiting visitors. Absent until an
    /// operator sets it; cleared by setting it empty.
    pub message: Option<String>,
    /// The operator's target admission rate in visitors per second. Absent
    /// until an operator sets one, which is also how the controller reads "do
    /// not admit anyone yet". Waiting visitors are shown a wait estimate
    /// derived from it before enough of the cursor's own movement has been
    /// observed to measure the real rate.
    pub target_rate: Option<u32>,
    /// The operator's stored admission override (ADR-0019, issue #71):
    /// `Open` / `Paused`. Never `FailOpen` — that is resolved from
    /// `fail_open_until` by [`crate::ids::resolve`], not stored here.
    pub stored_control: StoredControl,
    /// Epoch-seconds fail-open deadline: `now < fail_open_until` means the
    /// waiting room is bypassed. `0` means no fail-open window is in force.
    /// Self-expiring by construction, so a control plane that engages it and
    /// then dies cannot leave the deployment open indefinitely.
    pub fail_open_until: u64,
}

/// The sealed pre-queue index space and permutation seed, once the seal has
/// written them.
///
/// No `Debug`/`PartialEq`: [`Seed`] carries the permutation key and
/// deliberately implements neither, so it cannot end up in a log line or a
/// careless equality check at a call site.
#[derive(Clone, Copy)]
pub struct Sealed {
    pub offsets: SealedOffsets,
    pub seed: Seed,
}

impl Counters {
    /// `Some` once the seal has written the seed, cohort size, and offsets;
    /// `None` before.
    ///
    /// Callers branch on this rather than on [`Phase`]: an operator can walk
    /// the phase back through `Maintenance` to `PreQueue` after a seal
    /// (recovering from an operator error) without unsealing the index
    /// space, so `phase == PreQueue` alone does not mean unsealed.
    #[must_use]
    pub fn sealed(&self) -> Option<Sealed> {
        let seed_bytes = self.shuffle_seed?;
        let offsets = self.prequeue_offsets?;
        let participant_count = self.participant_count?;
        Some(Sealed {
            offsets: SealedOffsets::from_parts(offsets, participant_count),
            seed: Seed(seed_bytes),
        })
    }

    /// Resolves a pre-queue registration to its queue position.
    ///
    /// Reconstructs the global index `i = offset[s] + l` from the sealed offsets
    /// and the row, then derives the position with the permutation. A row whose
    /// local index is at or past its shard's own issued count raced the seal
    /// and belongs to the live-join sequence instead, so the permutation is
    /// never evaluated outside its domain.
    ///
    /// # Errors
    ///
    /// [`ResolveError::NotSealed`] before the seal has written the seed, cohort
    /// size, and offsets; [`ResolveError::BadShard`] if the row's shard is
    /// outside `0..SHARDS`.
    pub fn resolve_prequeue(&self, row: &PreQueueItem) -> Result<ResolvedPosition, ResolveError> {
        let Sealed { offsets, seed } = self.sealed().ok_or(ResolveError::NotSealed)?;

        let shard = usize::from(row.s);
        if shard >= SHARDS {
            return Err(ResolveError::BadShard);
        }

        let participant_count = offsets.participant_count();
        Ok(match offsets.assign(shard, row.l) {
            Assignment::PreQueue { index } => {
                ResolvedPosition::PreQueue(crate::permutation::prp(&seed, index, participant_count))
            }
            // The exact live-join position is claimed by the live-join path,
            // not reconstructed here — every caller falls through to a
            // `Positions` lookup for one, so no value carried on this variant
            // would ever be read.
            Assignment::LiveJoin => ResolvedPosition::LiveJoin,
        })
    }

    /// Assembles the item from its raw attributes, defaulting every field a
    /// fresh event has never written.
    ///
    /// The one parser for the `Counters` item shape — `assign_position`,
    /// `generate_token`, and `read` all call this rather than each keeping its
    /// own copy, so a stored value cannot be interpreted two different ways by
    /// two Lambdas.
    #[must_use]
    pub fn from_item(event_id: &str, item: &HashMap<String, AttributeValue>) -> Self {
        let num = |key: &str| -> Option<u64> {
            item.get(key)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse::<u64>().ok())
        };

        let shuffle_seed = item
            .get("shuffle_seed")
            .and_then(|v| v.as_b().ok())
            .and_then(|b| <[u8; 32]>::try_from(b.as_ref()).ok());

        let prequeue_offsets = item
            .get("prequeue_offsets")
            .and_then(|v| v.as_l().ok())
            .and_then(|list| {
                let parsed: Vec<u64> = list
                    .iter()
                    .filter_map(|e| e.as_n().ok().and_then(|s| s.parse().ok()))
                    .collect();
                <[u64; SHARDS]>::try_from(parsed).ok()
            });

        Self {
            event_id: event_id.to_owned(),
            phase: item
                .get("phase")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(Phase::Idle),
            queue_counter: num("queue_counter").unwrap_or(0),
            serving_counter: num("serving_counter").unwrap_or(0),
            shuffle_seed,
            participant_count: num("participant_count"),
            prequeue_offsets,
            target_rate: num("target_rate").and_then(|n| u32::try_from(n).ok()),
            message: item
                .get("message")
                .and_then(|v| v.as_s().ok())
                .filter(|s| !s.is_empty())
                .cloned(),
            stored_control: item
                .get("admission_control")
                .and_then(|v| v.as_s().ok())
                .and_then(|s| s.parse().ok())
                // Missing, empty, or unrecognized (including a legacy
                // "fail_open" string): normal admission (safe default).
                .unwrap_or_default(),
            fail_open_until: num("fail_open_until").unwrap_or(0),
        }
    }
}

/// Where a pre-queue registration resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedPosition {
    /// A position inside the sealed cohort's `[0, N)`, derived from the seed.
    PreQueue(u64),
    /// The registration raced the seal; it has no pre-queue position and is
    /// instead a live joiner, whose actual position lives in `Positions`.
    LiveJoin,
}

/// Why a pre-queue registration cannot be resolved to a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The event has not been sealed, so no pre-queue position exists yet.
    #[error("event not yet sealed")]
    NotSealed,
    /// The stored shard is outside `0..SHARDS` (a corrupt row).
    #[error("shard index out of range")]
    BadShard,
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn prequeue_item_round_trips_through_attribute_values() {
        let item = PreQueueItem {
            r: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            s: 7,
            l: 42,
            t: 1_788_000_000,
        };
        let av: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            serde_dynamo::to_item(&item).unwrap();
        let back: PreQueueItem = serde_dynamo::from_item(av).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn position_item_round_trips_and_status_is_snake_case() {
        let item = PositionItem {
            request_id: "req-1".to_owned(),
            queue_position: 4_242,
            entry_time: 1_788_000_000,
            status: PositionStatus::Issued,
            ttl: 1_900_000_000,
        };
        let av: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            serde_dynamo::to_item(&item).unwrap();
        assert_eq!(
            av.get("status"),
            Some(&aws_sdk_dynamodb::types::AttributeValue::S(
                "issued".to_owned()
            ))
        );
        let back: PositionItem = serde_dynamo::from_item(av).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn position_and_entry_time_are_separate_numeric_attributes() {
        // The position must not ride in entry_time: a reader looking for a
        // timestamp would parse a queue position, and a reader looking for a
        // position would parse a timestamp.
        use aws_sdk_dynamodb::types::AttributeValue;
        let item = PositionItem {
            request_id: "req-1".to_owned(),
            queue_position: 7,
            entry_time: 1_788_000_000,
            status: PositionStatus::Issued,
            ttl: 1_900_000_000,
        };
        let av: std::collections::HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&item).unwrap();
        assert_eq!(
            av.get("queue_position"),
            Some(&AttributeValue::N("7".to_owned()))
        );
        assert_eq!(
            av.get("entry_time"),
            Some(&AttributeValue::N("1788000000".to_owned()))
        );
    }

    fn unsealed_counters(phase: Phase) -> Counters {
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

    #[test]
    fn sealed_is_none_before_every_seal_output_is_present() {
        assert!(unsealed_counters(Phase::PreQueue).sealed().is_none());

        // Missing just the seed, or just the offsets, or just the count: still
        // not sealed. Every field must be present.
        let mut partial = unsealed_counters(Phase::Active);
        partial.shuffle_seed = Some([1u8; 32]);
        assert!(partial.sealed().is_none());
        partial.participant_count = Some(10);
        assert!(partial.sealed().is_none());
    }

    #[test]
    fn sealed_is_some_once_every_seal_output_is_present_regardless_of_phase() {
        // The operator can walk the phase back to PreQueue after a seal
        // (Active -> Maintenance -> Idle -> PreQueue) without unsealing the
        // index space, so `sealed()` must not gate on phase.
        let mut counters = unsealed_counters(Phase::PreQueue);
        counters.shuffle_seed = Some([9u8; 32]);
        counters.participant_count = Some(3);
        counters.prequeue_offsets = Some([0, 0, 0, 1, 1, 1, 2, 2, 2, 2]);
        let sealed = counters.sealed().unwrap();
        assert_eq!(sealed.offsets.participant_count(), 3);
    }

    #[test]
    fn from_item_defaults_a_fresh_event_to_idle_and_open() {
        // An item with nothing set must not read as a sealed, admitting event.
        let counters = Counters::from_item("evt-1", &HashMap::new());
        assert_eq!(counters.phase, Phase::Idle);
        assert_eq!(counters.queue_counter, 0);
        assert_eq!(counters.serving_counter, 0);
        assert!(counters.shuffle_seed.is_none());
        assert!(counters.participant_count.is_none());
        assert!(counters.prequeue_offsets.is_none());
        assert!(counters.message.is_none());
        // Absent, not zero: no rate set is how the controller reads "admit
        // nobody yet", which a defaulted 0 would be indistinguishable from
        // only by accident.
        assert!(counters.target_rate.is_none());
        assert_eq!(counters.stored_control, StoredControl::Open);
        assert_eq!(counters.fail_open_until, 0);
    }

    #[test]
    fn from_item_reads_the_fail_open_epoch() {
        let mut item = HashMap::new();
        item.insert(
            "fail_open_until".to_owned(),
            AttributeValue::N("1_800_000_000".replace('_', "")),
        );
        assert_eq!(
            Counters::from_item("evt-1", &item).fail_open_until,
            1_800_000_000
        );
    }

    #[test]
    fn from_item_reads_the_operator_target_rate() {
        let mut item = HashMap::new();
        item.insert(
            "target_rate".to_owned(),
            AttributeValue::N("250".to_owned()),
        );
        assert_eq!(Counters::from_item("evt-1", &item).target_rate, Some(250));
    }

    #[test]
    fn from_item_rejects_a_target_rate_too_large_for_u32() {
        // Stored numbers are unbounded, the field is not. A value that does not
        // fit reads as unset rather than wrapping into a small rate that would
        // silently throttle the event.
        let mut item = HashMap::new();
        item.insert(
            "target_rate".to_owned(),
            AttributeValue::N("4294967296".to_owned()),
        );
        assert!(Counters::from_item("evt-1", &item).target_rate.is_none());
    }

    #[test]
    fn from_item_round_trips_the_seal_outputs() {
        let mut item = HashMap::new();
        item.insert("phase".to_owned(), AttributeValue::S("active".to_owned()));
        item.insert(
            "queue_counter".to_owned(),
            AttributeValue::N("15".to_owned()),
        );
        item.insert(
            "shuffle_seed".to_owned(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new([7u8; 32])),
        );
        item.insert(
            "participant_count".to_owned(),
            AttributeValue::N("15".to_owned()),
        );
        item.insert(
            "prequeue_offsets".to_owned(),
            AttributeValue::L(
                [0, 3, 3, 8, 9, 9, 9, 11, 11, 11]
                    .into_iter()
                    .map(|o: u64| AttributeValue::N(o.to_string()))
                    .collect(),
            ),
        );

        let counters = Counters::from_item("evt-1", &item);
        assert_eq!(counters.phase, Phase::Active);
        assert_eq!(counters.queue_counter, 15);
        assert_eq!(counters.shuffle_seed, Some([7u8; 32]));
        assert_eq!(counters.participant_count, Some(15));
        assert_eq!(
            counters.prequeue_offsets,
            Some([0, 3, 3, 8, 9, 9, 9, 11, 11, 11])
        );
        assert!(counters.sealed().is_some());
    }
}
