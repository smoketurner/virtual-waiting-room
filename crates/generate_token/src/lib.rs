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

use wr_common::{
    Counters, Phase, PositionStatus, PossessionSecret, PreQueueItem, ResolveError,
    ResolvedPosition, SecretDigest, Shard,
};

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

    /// Reads a visitor's `Positions` row: the claimed position, its current
    /// status, and the possession digest it was written with.
    fn load_position(
        &self,
        request_id: &str,
    ) -> impl Future<Output = Result<Option<PositionRow>, StoreError>> + Send;

    /// Claims this visitor's one admission, reporting whether this call was the
    /// one that claimed it.
    ///
    /// One conditional write on the visitor's `Positions` row, creating it for
    /// a pre-queue member who has none. It is what makes the arrival countable
    /// exactly once: `record_arrival` is an unconditional `ADD`, and
    /// `request_id` travels in a URL, so without a claim a reloaded page counts
    /// a second arrival against one release.
    ///
    /// `digest` is the one the presented secret was verified against. It is
    /// written onto a row this claim creates, so a pre-queue member's later
    /// calls — which read `Positions` first — still have one to verify.
    fn claim_admission(
        &self,
        request_id: &str,
        position: u64,
        digest: &SecretDigest,
        now: u64,
    ) -> impl Future<Output = Result<AdmissionClaim, StoreError>> + Send;

    /// `ADD arrivals#<shard> :one` — records that this visitor showed up, which
    /// is what the controller measures its no-show rate against.
    fn record_arrival(
        &self,
        event_id: &str,
        shard: Shard,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A `Positions` row as the admission check reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionRow {
    pub position: u64,
    pub status: PositionStatus,
    /// Absent only on a row written before issue #62, which cannot be
    /// redeemed.
    pub digest: Option<SecretDigest>,
}

/// Whether an admission claim was this call's to make.
///
/// Not a refusal either way: a repeat still gets a session. It decides only
/// whether the arrival is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClaim {
    /// This call claimed the admission. Count the arrival.
    First,
    /// Someone already claimed it — a reload, a retried request, a second tab.
    /// The arrival is already counted; counting it again would tell the
    /// controller more people showed up than it released.
    Repeat,
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
    /// The event has not been opened, so no position exists yet.
    #[error("event not yet open")]
    NotOpen,
    /// The visitor's turn has not arrived.
    #[error("still queued at {position}, now serving {serving}")]
    StillQueued { position: u64, serving: u64 },
    /// The stored registration is corrupt.
    #[error("corrupt registration")]
    Corrupt,
    /// The caller does not hold the secret this `request_id` joined with
    /// (issue #62): knowing the id is not enough to be admitted as it.
    #[error("not the holder of this place in line")]
    NotHolder,
}

/// An admitted visitor: the position that was reached. The arrival shard is no
/// longer carried here (issue #59) — it is drawn at random by the caller
/// rather than derived from `request_id`, which would make `decide` depend on
/// an RNG and stop being a pure function of the queue state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub position: u64,
    /// The digest the secret was verified against, for the admission claim.
    pub digest: SecretDigest,
}

/// The address part of a `CloudFront-Viewer-Address` header, which is
/// `ip:port` with no brackets around an IPv6 address — so the port is
/// whatever follows the last `:` (issue #61). `None` for a header with no
/// port, which `CloudFront` never sends.
#[must_use]
pub fn viewer_ip(header: &str) -> Option<&str> {
    let (ip, port) = header.trim().rsplit_once(':')?;
    (!ip.is_empty() && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())).then_some(ip)
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
    position_row: Option<&PositionRow>,
    presented: &PossessionSecret,
    now: u64,
) -> Result<Grant, Denied> {
    use wr_common::AdmissionControl;

    // Possession first (issue #62): a caller who only knows the request id
    // learns nothing further here -- not the queue state, not the position.
    // The row that would answer the position is the row whose digest counts.
    let stored = match (position_row, prequeue) {
        (Some(row), _) => row.digest.as_ref(),
        (None, Some(row)) => row.h.as_ref(),
        (None, None) => return Err(Denied::NotRegistered),
    };
    let digest = match stored {
        Some(d) if d.is_digest_of(presented) => d.clone(),
        Some(_) | None => return Err(Denied::NotHolder),
    };

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

    Ok(Grant { position, digest })
}

/// The visitor's position, from whichever path registered them. A live-join
/// row wins over a pre-queue row: it is the position actually claimed from the
/// counter, whereas a pre-queue row that raced the open only reports the base
/// the live sequence counts from.
fn resolve_position(
    counters: &Counters,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<&PositionRow>,
) -> Result<u64, Denied> {
    if let Some(&PositionRow {
        position, status, ..
    }) = position_row
    {
        return match status {
            // An already-admitted row is still a valid claim on the position.
            // Refusing one would strand a visitor whose first response never
            // arrived, or who reloaded the page, for a mistake that was not
            // theirs -- and the cookie they are asking for is one they are
            // entitled to. What the status governs is the arrival count, not
            // admission; the claim in `main.rs` is where it is read.
            PositionStatus::Issued | PositionStatus::Admitted => Ok(position),
        };
    }

    let Some(row) = prequeue else {
        return Err(Denied::NotRegistered);
    };

    match counters.resolve_prequeue(row) {
        Ok(ResolvedPosition::PreQueue(position)) => Ok(position),
        // Raced the open, so a live-join row should exist; without one there is
        // no claimed position to admit against.
        Ok(ResolvedPosition::LiveJoin) => Err(Denied::NotRegistered),
        Err(ResolveError::NotOpen) => Err(Denied::NotOpen),
        Err(ResolveError::BadShard) => Err(Denied::Corrupt),
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use wr_common::{CohortOffsets, SHARDS, StoredControl};

    use super::*;

    fn counters(serving: u64) -> Counters {
        let counts = [2u64; SHARDS];
        let opened = CohortOffsets::from_counts(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = opened.offset(s);
        }
        Counters {
            event_id: "evt".to_owned(),
            phase: Phase::Active,
            queue_counter: opened.participant_count(),
            serving_counter: serving,
            shuffle_seed: Some([9u8; 32]),
            participant_count: Some(opened.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            starts_at: None,
        }
    }

    const REQ: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";

    fn secret() -> PossessionSecret {
        PossessionSecret::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap()
    }

    fn digest() -> SecretDigest {
        secret().digest()
    }

    fn row(position: u64, status: PositionStatus) -> PositionRow {
        PositionRow {
            position,
            status,
            digest: Some(digest()),
        }
    }

    #[test]
    fn a_reached_position_is_admitted() {
        // Live joiner holding position 3, cursor past it.
        let grant = decide(
            &counters(10),
            None,
            Some(&row(3, PositionStatus::Issued)),
            &secret(),
            0,
        )
        .unwrap();
        assert_eq!(grant.position, 3);
    }

    #[test]
    fn a_position_not_yet_reached_is_refused() {
        let err = decide(
            &counters(3),
            None,
            Some(&row(7, PositionStatus::Issued)),
            &secret(),
            0,
        )
        .unwrap_err();
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
        assert!(
            decide(
                &counters(5),
                None,
                Some(&row(4, PositionStatus::Issued)),
                &secret(),
                0
            )
            .is_ok()
        );
        assert!(
            decide(
                &counters(5),
                None,
                Some(&row(5, PositionStatus::Issued)),
                &secret(),
                0
            )
            .is_err()
        );
    }

    #[test]
    fn a_paused_event_admits_nobody_even_at_a_reached_position() {
        let mut c = counters(10);
        c.stored_control = StoredControl::Paused;
        assert_eq!(
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                0
            )
            .unwrap_err(),
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
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                500
            )
            .unwrap_err(),
            Denied::NotAdmitting
        );
        // Once the epoch lapses, the stored Open control governs again.
        assert!(
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                1000
            )
            .is_ok()
        );
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
                decide(
                    &c,
                    None,
                    Some(&row(3, PositionStatus::Issued)),
                    &secret(),
                    0
                )
                .unwrap_err(),
                Denied::NotAdmitting
            );
        }
    }

    #[test]
    fn an_already_admitted_visitor_is_admitted_again() {
        // Their first response may never have reached them, or they reloaded.
        // Refusing would strand a visitor holding a position that is still
        // theirs, for a failure that was not theirs. What being admitted
        // already governs is whether the arrival is counted a second time, and
        // that is the claim's job, not this one's.
        for status in [PositionStatus::Issued, PositionStatus::Admitted] {
            assert_eq!(
                decide(&counters(10), None, Some(&row(3, status)), &secret(), 0).unwrap(),
                Grant {
                    position: 3,
                    digest: digest()
                }
            );
        }
    }

    #[test]
    fn an_unregistered_visitor_is_refused() {
        assert_eq!(
            decide(&counters(10), None, None, &secret(), 0).unwrap_err(),
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
            h: Some(digest()),
        };
        let grant = decide(&c, Some(&row), None, &secret(), 0).unwrap();
        // Inside the cohort.
        assert!(grant.position < c.participant_count.unwrap());
    }

    #[test]
    fn an_unopened_event_has_no_pre_queue_position() {
        let mut c = counters(10);
        c.shuffle_seed = None;
        c.participant_count = None;
        c.prequeue_offsets = None;
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: 1_788_000_000,
            h: Some(digest()),
        };
        assert_eq!(
            decide(&c, Some(&row), None, &secret(), 0).unwrap_err(),
            Denied::NotOpen
        );
    }

    #[test]
    fn a_live_join_row_wins_over_a_pre_queue_row() {
        // A straggler has both rows; the claimed live position is authoritative.
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: 1_788_000_000,
            h: Some(digest()),
        };
        let grant = decide(
            &counters(100),
            Some(&pq),
            Some(&row(42, PositionStatus::Issued)),
            &secret(),
            0,
        )
        .unwrap();
        assert_eq!(grant.position, 42);
    }

    // --- issue #62: knowing the request id is not enough ----------------------

    #[test]
    fn the_wrong_secret_is_refused_before_anything_about_the_queue_is_said() {
        let wrong = PossessionSecret::parse("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        // Even at a reached position, and even while paused (which would
        // otherwise answer NotAdmitting): possession is checked first.
        let mut paused = counters(10);
        paused.stored_control = StoredControl::Paused;
        for c in [counters(10), counters(3), paused] {
            assert_eq!(
                decide(&c, None, Some(&row(3, PositionStatus::Issued)), &wrong, 0).unwrap_err(),
                Denied::NotHolder
            );
        }
    }

    #[test]
    fn a_pre_queue_registrant_needs_their_secret_too() {
        let wrong = PossessionSecret::parse("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 3,
            l: 1,
            t: 1_788_000_000,
            h: Some(digest()),
        };
        assert_eq!(
            decide(&counters(u64::MAX), Some(&pq), None, &wrong, 0).unwrap_err(),
            Denied::NotHolder
        );
    }

    #[test]
    fn a_row_without_a_digest_cannot_be_redeemed() {
        let legacy = PositionRow {
            position: 3,
            status: PositionStatus::Issued,
            digest: None,
        };
        assert_eq!(
            decide(&counters(10), None, Some(&legacy), &secret(), 0).unwrap_err(),
            Denied::NotHolder
        );
    }

    #[test]
    fn the_positions_row_digest_is_the_one_that_counts() {
        // Once admitted, a pre-queue member's Positions row answers first, so
        // it must carry the digest (the claim copies it); a PreQueue digest is
        // not consulted behind it.
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: 1_788_000_000,
            h: Some(digest()),
        };
        let mut admitted = row(3, PositionStatus::Admitted);
        admitted.digest = None;
        assert_eq!(
            decide(&counters(10), Some(&pq), Some(&admitted), &secret(), 0).unwrap_err(),
            Denied::NotHolder
        );
    }

    // --- issue #61: the viewer address the session is tagged with -----------

    #[test]
    fn the_viewer_address_header_loses_its_port() {
        assert_eq!(viewer_ip("198.51.100.7:46532"), Some("198.51.100.7"));
        assert_eq!(
            viewer_ip("2001:0db8:85a3:0000:0000:8a2e:0370:7334:46532"),
            Some("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        );
        assert_eq!(viewer_ip("2001:db8::1:443"), Some("2001:db8::1"));
        for bad in ["", "198.51.100.7", ":443", "198.51.100.7:", "1.2.3.4:x"] {
            assert_eq!(viewer_ip(bad), None, "{bad}");
        }
    }
}
