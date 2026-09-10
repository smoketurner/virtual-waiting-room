//! Read-path logic for the `read` Lambda: the `/status` payload and the
//! `/queue_num` position resolution. Pure functions over the domain types so
//! they run without AWS; the handler fetches the items and calls these.

use serde::Serialize;
#[cfg(test)]
use wr_common::AdmissionControl;
use wr_common::{
    Counters, Phase, PreQueueItem, ResolveError, ResolvedPosition, SHARDS, ServingState,
    serving_state,
};

/// The `/status` payload — one document the countdown and queue pages poll.
/// Seal outputs appear only once the event is active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusResponse {
    /// The event this room is serving. Published because the join request is
    /// validated against a schema requiring it, so a client that cannot read it
    /// here has no way to construct a valid join.
    pub event_id: String,
    pub phase: Phase,
    /// The visitor-facing serving state (ADR-0019): what an arriving visitor
    /// experiences right now. Derived from the phase and the operator's
    /// admission control. This is the authoritative visitor-facing signal.
    pub serving_state: ServingState,
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
        event_id: counters.event_id.clone(),
        phase: counters.phase,
        serving_state: serving_state(counters.phase, counters.admission_control),
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
    match counters.resolve_prequeue(row) {
        Ok(ResolvedPosition::PreQueue(position)) => Ok(QueueNumResponse {
            position,
            live_join: false,
        }),
        Ok(ResolvedPosition::LiveJoin { base }) => Ok(QueueNumResponse {
            position: base,
            live_join: true,
        }),
        Err(ResolveError::NotSealed) => Err(QueueNumError::NotSealed),
        Err(ResolveError::BadShard) => Err(QueueNumError::BadShard),
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::cast_possible_truncation,
        reason = "test code panics on setup failure; shard/len casts are provably small"
    )]

    use wr_common::SealedOffsets;

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
            arrivals: [0; SHARDS],
            shuffle_seed: Some(seed),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            admission_control: AdmissionControl::Open,
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
            arrivals: [0; SHARDS],
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            admission_control: AdmissionControl::Open,
        };
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert_eq!(json["phase"], "pre_queue");
        assert!(json.get("participant_count").is_none());
        assert!(json.get("prequeue_offsets").is_none());
    }

    #[test]
    fn status_publishes_the_event_id_a_join_needs() {
        // The join request schema requires a non-empty event_id string, so a
        // client that cannot read it from /status cannot construct a request
        // that passes the edge validator.
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let json = serde_json::to_value(status(&counters)).unwrap();
        assert_eq!(json["event_id"], "evt-1");
        assert!(json["event_id"].as_str().is_some_and(|s| !s.is_empty()));
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
    fn status_publishes_serving_state() {
        // Active event -> running; pause it -> paused; fail-open -> fail_open.
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        assert_eq!(
            serde_json::to_value(status(&counters)).unwrap()["serving_state"],
            "running"
        );
        counters.admission_control = AdmissionControl::Paused;
        assert_eq!(
            serde_json::to_value(status(&counters)).unwrap()["serving_state"],
            "paused"
        );
        counters.admission_control = AdmissionControl::FailOpen;
        assert_eq!(
            serde_json::to_value(status(&counters)).unwrap()["serving_state"],
            "fail_open"
        );
    }

    #[test]
    fn status_serving_state_is_closed_before_active() {
        // An idle event with open admission reads Closed to a visitor.
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        counters.phase = Phase::Idle;
        assert_eq!(
            serde_json::to_value(status(&counters)).unwrap()["serving_state"],
            "closed"
        );
    }

    #[test]
    fn queue_num_before_seal_is_not_sealed() {
        let counters = Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            prequeue_counts: [1; SHARDS],
            arrivals: [0; SHARDS],
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            admission_control: AdmissionControl::Open,
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
