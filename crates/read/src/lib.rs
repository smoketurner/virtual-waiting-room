//! Read-path logic for the `read` Lambda: the `/status` payload and the
//! `/queue_num` position resolution. Pure functions over the domain types so
//! they run without AWS; the handler fetches the items and calls these.

use serde::Serialize;
use wr_domain::{Assignment, Counters, Phase, PreQueueItem, SHARDS, SealedOffsets, Seed, prp};

/// The `/status` payload — one document the countdown and queue pages poll.
/// Seal outputs appear only once the event is active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusResponse {
    pub phase: Phase,
    pub serving_position: u64,
    /// Present once sealed: the cohort size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participant_count: Option<u64>,
    /// Present once sealed: the per-shard prefix offsets, so a client can
    /// reconstruct its global index without a round trip.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prequeue_offsets: Option<[u64; SHARDS]>,
    /// Operator broadcast text for the waiting page. Absent when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// A resolved `/queue_num` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueNumResponse {
    pub position: u64,
    /// True when the position came from the live-join sequence rather than the
    /// permutation (a straggler that raced the seal, or a join after opening).
    pub live_join: bool,
}

/// Builds the `/status` payload from the counters item.
#[must_use]
pub fn status(counters: &Counters) -> StatusResponse {
    StatusResponse {
        phase: counters.phase,
        serving_position: counters.serving_counter,
        participant_count: counters.participant_count,
        prequeue_offsets: counters.prequeue_offsets,
        message: counters.message.clone(),
    }
}

/// Why a `/queue_num` request cannot be answered from the queue state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QueueNumError {
    /// The event has not been sealed, so no pre-queue position exists yet.
    #[error("event not yet sealed")]
    NotSealed,
    /// The stored shard is outside `0..SHARDS` (corrupt row).
    #[error("shard index out of range")]
    BadShard,
}

/// Resolves a pre-queue registrant's queue position.
///
/// Reconstructs the global index `i = offset[s] + l` from the sealed offsets
/// and the visitor's `PreQueue` row, then derives the position with the
/// permutation. A row whose reconstructed `i >= N` (a join that raced the seal)
/// resolves to a live-join position instead — the permutation is never
/// evaluated out of domain.
///
/// # Errors
///
/// [`QueueNumError::NotSealed`] before the seal; [`QueueNumError::BadShard`] if
/// the stored shard is out of range.
pub fn queue_num(
    counters: &Counters,
    row: &PreQueueItem,
) -> Result<QueueNumResponse, QueueNumError> {
    let (Some(seed_bytes), Some(offsets)) = (counters.shuffle_seed, counters.prequeue_offsets)
    else {
        return Err(QueueNumError::NotSealed);
    };
    let participant_count = counters.participant_count.ok_or(QueueNumError::NotSealed)?;

    let shard = usize::from(row.s);
    if shard >= SHARDS {
        return Err(QueueNumError::BadShard);
    }

    let sealed = SealedOffsets::from_parts(offsets, participant_count);
    match sealed.assign(shard, row.l) {
        Assignment::PreQueue { index } => {
            let seed = Seed(seed_bytes);
            Ok(QueueNumResponse {
                position: prp(&seed, index, participant_count),
                live_join: false,
            })
        }
        Assignment::LiveJoin => Ok(QueueNumResponse {
            // A straggler is served behind the whole pre-queue cohort; the live
            // sequence starts at participant_count. The exact live position is
            // claimed by the live-join path — here it is reported as the base.
            position: participant_count,
            live_join: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::cast_possible_truncation,
        reason = "test code panics on setup failure; shard/len casts are provably small"
    )]

    use super::*;

    fn sealed_counters(counts: [u64; SHARDS], seed: [u8; 32]) -> Counters {
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::Active,
            queue_counter: sealed.participant_count(),
            serving_counter: 5,
            prequeue_counts: counts,
            shuffle_seed: Some(seed),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
        }
    }

    fn row(s: u8, l: u64) -> PreQueueItem {
        PreQueueItem {
            r: "req-1".to_owned(),
            s,
            l,
            t: "2026-08-03T19:12:52.000Z".to_owned(),
        }
    }

    #[test]
    fn status_hides_seal_outputs_before_seal() {
        let counters = Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            prequeue_counts: [0; SHARDS],
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
        };
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert_eq!(json["phase"], "pre_queue");
        assert!(json.get("participant_count").is_none());
        assert!(json.get("prequeue_offsets").is_none());
    }

    #[test]
    fn status_exposes_seal_outputs_when_active() {
        let counters = sealed_counters([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], [3u8; 32]);
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert_eq!(json["phase"], "active");
        assert_eq!(json["participant_count"], 10);
        assert_eq!(json["serving_position"], 5);
        assert!(json.get("prequeue_offsets").is_some());
    }

    #[test]
    fn status_surfaces_the_broadcast_message_when_set() {
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        counters.message = Some("Doors open at noon".to_owned());
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert_eq!(json["message"], "Doors open at noon");
    }

    #[test]
    fn status_omits_the_message_when_absent() {
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert!(json.get("message").is_none());
    }

    #[test]
    fn queue_num_before_seal_is_not_sealed() {
        let counters = Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            prequeue_counts: [1; SHARDS],
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
        };
        assert_eq!(
            queue_num(&counters, &row(0, 0)),
            Err(QueueNumError::NotSealed)
        );
    }

    #[test]
    fn queue_num_resolves_pre_queue_position_in_domain() {
        let counts = [3, 0, 5, 1, 0, 0, 2, 0, 0, 4];
        let counters = sealed_counters(counts, [42u8; 32]);
        let n = counters.participant_count.unwrap();
        // Every valid (shard, local) resolves to a distinct in-domain position.
        let mut positions = std::collections::BTreeSet::new();
        for (shard, &count) in counts.iter().enumerate() {
            for l in 0..count {
                let resp = queue_num(&counters, &row(shard as u8, l)).unwrap();
                assert!(!resp.live_join);
                assert!(resp.position < n);
                assert!(positions.insert(resp.position), "duplicate position");
            }
        }
        assert_eq!(positions.len() as u64, n);
    }

    #[test]
    fn queue_num_straggler_is_live_join() {
        let counts = [3, 0, 5, 1, 0, 0, 2, 0, 0, 4]; // N = 15, last offset 11
        let counters = sealed_counters(counts, [42u8; 32]);
        // Last shard local index 4 -> i = 15 >= N: a straggler.
        let resp = queue_num(&counters, &row(9, 4)).unwrap();
        assert!(resp.live_join);
        assert_eq!(resp.position, 15);
    }

    #[test]
    fn queue_num_bad_shard_is_rejected() {
        let counters = sealed_counters([1; SHARDS], [1u8; 32]);
        let bad = PreQueueItem {
            r: "req-1".to_owned(),
            s: SHARDS as u8,
            l: 0,
            t: "t".to_owned(),
        };
        assert_eq!(queue_num(&counters, &bad), Err(QueueNumError::BadShard));
    }
}
