//! `DynamoDB` item shapes for the MVP tables, (de)serialized via `serde_dynamo`.
//!
//! Attribute names match the deployed tables. The pre-queue counter is striped
//! across [`SHARDS`] shards named `prequeue_counter#0`..`prequeue_counter#9`;
//! `serde` cannot express a `#`-suffixed field, so the `Counters` item keeps
//! the shards in a flat `[u64; SHARDS]` and (de)serializes them with a manual
//! bridge in [`Counters::shard_counts`] / [`Counters::from_item`].

use serde::{Deserialize, Serialize};
use wr_permutation::{SHARDS, SealError, SealedOffsets};

use crate::ids::{AdmissionControl, Phase};

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
    /// Registration timestamp (RFC 3339).
    pub t: String,
}

/// A `Positions` row, written lazily when a visitor is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionItem {
    pub request_id: String,
    pub entry_time: String,
    pub status: PositionStatus,
    /// Position expiry the controller acts on.
    pub expires_at: u64,
    /// Post-event storage reclamation only; never the expiry mechanism.
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
    /// Per-shard registration counts (`prequeue_counter#0..9`).
    pub prequeue_counts: [u64; SHARDS],
    /// Per-shard arrival counts (`arrivals#0..9`), incremented by the authorizer
    /// when it converts an admission token into a session. The controller sums
    /// these each interval to measure the no-show rate (DESIGN §7). Sharded for
    /// the same single-item write-ceiling reason as the pre-queue counter.
    pub arrivals: [u64; SHARDS],
    /// Set only at the seal, absent before: the 256-bit permutation seed.
    pub shuffle_seed: Option<[u8; 32]>,
    /// Set at the seal: cohort size `N = Σ prequeue_counts`.
    pub participant_count: Option<u64>,
    /// Set at the seal: prefix offsets `offset[s] = Σ counts[0..s)`.
    pub prequeue_offsets: Option<[u64; SHARDS]>,
    /// Operator broadcast text shown to waiting visitors. Absent until an
    /// operator sets it; cleared by setting it empty.
    pub message: Option<String>,
    /// The operator's live admission override (ADR-0019): `Open` / `Paused` /
    /// `FailOpen`. Replaces the former `admission_paused` + `fail_open` booleans
    /// so an illegal combination cannot be stored.
    pub admission_control: AdmissionControl,
}

impl Counters {
    /// Builds the sealed offsets from the stored per-shard counts.
    ///
    /// # Errors
    ///
    /// Returns [`SealError::Overflow`] if the summed cohort size exceeds `u64`.
    pub fn seal(&self) -> Result<SealedOffsets, SealError> {
        SealedOffsets::seal(self.prequeue_counts)
    }
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
            t: "2026-08-03T19:12:52.000Z".to_owned(),
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
            entry_time: "2026-08-03T19:12:52.000Z".to_owned(),
            status: PositionStatus::Issued,
            expires_at: 1_800_000_000,
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
    fn counters_seal_matches_direct_sealed_offsets() {
        let counters = Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            prequeue_counts: [3, 0, 5, 1, 0, 0, 2, 0, 0, 4],
            arrivals: [0; SHARDS],
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            admission_control: AdmissionControl::Open,
        };
        let sealed = counters.seal().unwrap();
        assert_eq!(sealed.participant_count(), 15);
        assert_eq!(sealed.offset(3), 8);
    }
}
