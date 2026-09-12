//! Admission check for the `generate_token` Lambda.
//!
//! A visitor whose queue position has been reached exchanges their request id
//! for a session credential, set as a cookie. The `CloudFront` Function gate
//! (issue #71) verifies that cookie itself at the edge on every later request
//! to the protected origin, so an admitted visitor reaches the origin with no
//! compute in the request path, and an un-admitted one is refused before the
//! origin is touched.
//!
//! This module is AWS-free: [`decide`] is a pure function from the queue state
//! to a [`Grant`], and the handler in `main.rs` fetches the items, calls it,
//! and signs the returned grant into a `Set-Cookie`.

use std::future::Future;

use wr_common::{Counters, Phase, PositionStatus, PreQueueItem, ResolveError, ResolvedPosition};

pub mod dynamo;

/// How long a minted session stays valid when the environment does not say.
/// Long enough to finish a purchase, short enough that a leaked cookie is not
/// a standing bypass.
pub const DEFAULT_SESSION_TTL_SECS: u64 = 3600;

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("generate_token store error: {0}")]
pub struct StoreError(pub String);

/// The persistence port. A trait seam so the logic runs without AWS.
pub trait Store {
    /// Reads the event's `Counters` item.
    fn load_counters(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send;

    /// Reads a visitor's `PreQueue` registration, if they have one.
    fn load_prequeue(
        &self,
        request_id: &str,
    ) -> impl Future<Output = Result<Option<PreQueueItem>, StoreError>> + Send;

    /// Reads a live joiner's `Positions` row: the claimed position and its
    /// current status.
    fn load_position(
        &self,
        request_id: &str,
    ) -> impl Future<Output = Result<Option<(u64, PositionStatus)>, StoreError>> + Send;

    /// `ADD arrivals#<shard> :one` — records that this visitor showed up, which
    /// is what the controller measures its no-show rate against.
    fn record_arrival(
        &self,
        event_id: &str,
        shard: usize,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Why a visitor is not being admitted right now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Denied {
    /// No registration exists for this request id.
    #[error("not registered")]
    NotRegistered,
    /// The event is not admitting: not active, or held by the operator.
    #[error("event is not admitting")]
    NotAdmitting,
    /// The event has not been sealed, so no position exists yet.
    #[error("event not yet open")]
    NotSealed,
    /// The visitor's turn has not arrived.
    #[error("still queued at {position}, now serving {serving}")]
    StillQueued { position: u64, serving: u64 },
    /// The position is no longer a live claim: expired by the controller,
    /// already used, or abandoned. Permanent, unlike [`Denied::StillQueued`].
    #[error("position is no longer valid")]
    Spent,
    /// The stored registration is corrupt.
    #[error("corrupt registration")]
    Corrupt,
}

/// An admitted visitor: the position that was reached. The arrival shard is no
/// longer carried here (issue #59) — it is drawn at random by the caller
/// rather than derived from `request_id`, which would make `decide` depend on
/// an RNG and stop being a pure function of the queue state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub position: u64,
}

/// Decides whether the visitor may be admitted.
///
/// Both gates are checked, in this order: the event must be actively admitting
/// (resolving the stored control against the fail-open epoch at `now`, issue
/// #71), and the visitor's position must have been reached. A held event
/// admits nobody even if their position was reached before the hold, which is
/// what makes the operator's pause a real stop rather than a display state.
///
/// # Errors
///
/// [`Denied`] describing which gate refused.
pub fn decide(
    counters: &Counters,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<(u64, PositionStatus)>,
    now: u64,
) -> Result<Grant, Denied> {
    use wr_common::AdmissionControl;
    match wr_common::resolve(counters.stored_control, counters.fail_open_until, now) {
        AdmissionControl::Open => {}
        AdmissionControl::Paused | AdmissionControl::FailOpen => return Err(Denied::NotAdmitting),
    }
    if counters.phase != Phase::Active {
        return Err(Denied::NotAdmitting);
    }

    let position = resolve_position(counters, prequeue, position_row)?;

    if position >= counters.serving_counter {
        return Err(Denied::StillQueued {
            position,
            serving: counters.serving_counter,
        });
    }

    Ok(Grant { position })
}

/// The visitor's position, from whichever path registered them. A live-join
/// row wins over a pre-queue row: it is the position actually claimed from the
/// counter, whereas a pre-queue row that raced the seal only reports the base
/// the live sequence counts from.
fn resolve_position(
    counters: &Counters,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<(u64, PositionStatus)>,
) -> Result<u64, Denied> {
    if let Some((position, status)) = position_row {
        return match status {
            PositionStatus::Issued => Ok(position),
            // Expired by the controller, already used, or given up: none of the
            // three is a live claim on a position, and all three are permanent,
            // so the visitor is told to stop rather than to keep polling.
            PositionStatus::Expired | PositionStatus::Completed | PositionStatus::Abandoned => {
                Err(Denied::Spent)
            }
        };
    }

    let Some(row) = prequeue else {
        return Err(Denied::NotRegistered);
    };

    match counters.resolve_prequeue(row) {
        Ok(ResolvedPosition::PreQueue(position)) => Ok(position),
        // Raced the seal, so a live-join row should exist; without one there is
        // no claimed position to admit against.
        Ok(ResolvedPosition::LiveJoin) => Err(Denied::NotRegistered),
        Err(ResolveError::NotSealed) => Err(Denied::NotSealed),
        Err(ResolveError::BadShard) => Err(Denied::Corrupt),
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use wr_common::{SHARDS, SealedOffsets, StoredControl};

    use super::*;

    fn counters(serving: u64) -> Counters {
        let counts = [2u64; SHARDS];
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        Counters {
            event_id: "evt".to_owned(),
            phase: Phase::Active,
            queue_counter: sealed.participant_count(),
            serving_counter: serving,
            shuffle_seed: Some([9u8; 32]),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            starts_at: None,
        }
    }

    const REQ: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";

    #[test]
    fn a_reached_position_is_admitted() {
        // Live joiner holding position 3, cursor past it.
        let grant = decide(&counters(10), None, Some((3, PositionStatus::Issued)), 0).unwrap();
        assert_eq!(grant.position, 3);
    }

    #[test]
    fn a_position_not_yet_reached_is_refused() {
        let err = decide(&counters(3), None, Some((7, PositionStatus::Issued)), 0).unwrap_err();
        assert_eq!(
            err,
            Denied::StillQueued {
                position: 7,
                serving: 3
            }
        );
    }

    #[test]
    fn the_cursor_is_exclusive() {
        // serving_counter is the count released, so position N is admitted only
        // once the cursor has passed it. Off by one here admits one visitor too
        // many on every interval.
        assert!(decide(&counters(5), None, Some((4, PositionStatus::Issued)), 0).is_ok());
        assert!(decide(&counters(5), None, Some((5, PositionStatus::Issued)), 0).is_err());
    }

    #[test]
    fn a_paused_event_admits_nobody_even_at_a_reached_position() {
        let mut c = counters(10);
        c.stored_control = StoredControl::Paused;
        assert_eq!(
            decide(&c, None, Some((3, PositionStatus::Issued)), 0).unwrap_err(),
            Denied::NotAdmitting
        );
    }

    #[test]
    fn a_fail_open_event_admits_nobody_through_this_path() {
        // The edge already lets a fail-open visitor through without a
        // credential, so nobody should be calling generate_token during the
        // window — but if one does, it must not mint.
        let mut c = counters(10);
        c.fail_open_until = 1000;
        assert_eq!(
            decide(&c, None, Some((3, PositionStatus::Issued)), 500).unwrap_err(),
            Denied::NotAdmitting
        );
        // Once the epoch lapses, the stored Open control governs again.
        assert!(decide(&c, None, Some((3, PositionStatus::Issued)), 1000).is_ok());
    }

    #[test]
    fn a_non_active_event_admits_nobody() {
        for phase in [
            Phase::Idle,
            Phase::PreQueue,
            Phase::PostEvent,
            Phase::Maintenance,
        ] {
            let mut c = counters(10);
            c.phase = phase;
            assert_eq!(
                decide(&c, None, Some((3, PositionStatus::Issued)), 0).unwrap_err(),
                Denied::NotAdmitting
            );
        }
    }

    #[test]
    fn an_expired_or_spent_position_is_refused() {
        for status in [
            PositionStatus::Expired,
            PositionStatus::Completed,
            PositionStatus::Abandoned,
        ] {
            assert_eq!(
                decide(&counters(10), None, Some((3, status)), 0).unwrap_err(),
                Denied::Spent
            );
        }
    }

    #[test]
    fn an_unregistered_visitor_is_refused() {
        assert_eq!(
            decide(&counters(10), None, None, 0).unwrap_err(),
            Denied::NotRegistered
        );
    }

    #[test]
    fn a_pre_queue_registrant_resolves_through_the_permutation() {
        let c = counters(u64::MAX);
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 3,
            l: 1,
            t: 1_788_000_000,
            v: None,
        };
        let grant = decide(&c, Some(&row), None, 0).unwrap();
        // Inside the sealed cohort.
        assert!(grant.position < c.participant_count.unwrap());
    }

    #[test]
    fn an_unsealed_event_has_no_pre_queue_position() {
        let mut c = counters(10);
        c.shuffle_seed = None;
        c.participant_count = None;
        c.prequeue_offsets = None;
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: 1_788_000_000,
            v: None,
        };
        assert_eq!(
            decide(&c, Some(&row), None, 0).unwrap_err(),
            Denied::NotSealed
        );
    }

    #[test]
    fn a_live_join_row_wins_over_a_pre_queue_row() {
        // A straggler has both rows; the claimed live position is authoritative.
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: 1_788_000_000,
            v: None,
        };
        let grant = decide(
            &counters(100),
            Some(&row),
            Some((42, PositionStatus::Issued)),
            0,
        )
        .unwrap();
        assert_eq!(grant.position, 42);
    }
}
