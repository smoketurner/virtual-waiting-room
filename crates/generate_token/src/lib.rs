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

use jiff::{SignedDuration, Timestamp};
use std::future::Future;

use wr_common::{
    Counters, Phase, PositionStatus, PossessionSecret, PreQueueItem, ResolveError,
    ResolvedPosition, SHARDS, SecretDigest, Session, Shard, Shelf, SigningKey, TurnState,
};

pub mod dynamo;
pub mod http;

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

    /// Takes one held spot from counter `shard` on `shelf`, reporting whether
    /// there was one to take.
    ///
    /// One conditional `ADD` on the arrivals counter, allowed only while its
    /// count is below the cap the controller wrote. It is both the meter and
    /// the count: the controller reads the same number to see who was let in.
    fn take_spot(
        &self,
        event_id: &str,
        shelf: Shelf,
        shard: Shard,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Marks the visitor's `Positions` row admitted, creating it for a
    /// pre-queue member who has none.
    ///
    /// The mark is what lets a later call -- a reload, a second tab, a retried
    /// request -- be recognised as a visitor who has already taken their unit
    /// of budget and must not take another.
    ///
    /// `digest` is the one the presented secret was verified against. It is
    /// written onto a row this creates.
    fn mark_admitted(
        &self,
        request_id: &str,
        position: u64,
        digest: &SecretDigest,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// A `Positions` row as the admission check reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionRow {
    pub position: u64,
    pub status: PositionStatus,
    /// Absent only on a malformed row, which cannot be redeemed.
    pub digest: Option<SecretDigest>,
    /// When the visitor was first admitted, if they have been.
    pub admitted_at: Option<Timestamp>,
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
    /// The visitor's number was called, but their held spot is not there to
    /// take yet: the controller has called them and not yet written where it
    /// is held. Not a refusal of the visitor -- they ask again.
    #[error("at capacity; position {position} is next")]
    AtCapacity { position: u64 },
    /// The visitor's number was called and their turn passed before they took
    /// it. Their spot went to the next place in line; they take a new place.
    #[error("the turn for position {position} has passed")]
    TurnMissed { position: u64 },
    /// The visitor was admitted, and the visit that admission bought is over:
    /// a session lasts its lifetime from the first admission, however often it
    /// is asked for again (issue #237). They take a new place.
    #[error("the admission for this place in line has run out")]
    SessionExpired,
    /// The caller does not hold the secret this `request_id` joined with
    /// (issue #62): knowing the id is not enough to be admitted as it.
    #[error("not the holder of this place in line")]
    NotHolder,
}

/// A visitor whose turn has come: the position that was reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub position: u64,
    /// The digest the secret was verified against, for the admitted mark.
    pub digest: SecretDigest,
    /// When this visitor was first admitted, if they have been. They are
    /// entitled to their session again until it runs out, and have already
    /// taken their spot.
    pub admitted_at: Option<Timestamp>,
    /// Where this visitor's held spot is, if they have not yet taken it.
    pub shelf: Option<Shelf>,
}

/// What a minted session is signed with and scoped to.
pub struct Minting<'a> {
    pub key: &'a SigningKey,
    pub event_id: &'a str,
    pub session: SignedDuration,
}

/// The outcome of [`admit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Admitted: the signed session credential, for a `Set-Cookie`.
    Admitted {
        position: u64,
        expires_at: Timestamp,
        credential: String,
    },
    /// Not admitted now, and why. [`Denied::StillQueued`] carries the
    /// position, which is what a waiting page renders.
    Refused(Denied),
    /// The event has no `Counters` item.
    EventNotFound,
    /// Signing failed. Retryable; never an empty credential (see [`admit`]).
    SignFailed,
}

/// The whole admission: load the rows, [`decide`], take a unit of the
/// admission budget, mark the visitor admitted, and sign the session. Shared by
/// `/_wr/v1/generate_token` and the no-JavaScript waiting page (issue #67), so
/// there is one path by which a session is minted.
///
/// # Errors
///
/// A store failure reading the queue state.
pub async fn admit<S: Store>(
    store: &S,
    minting: &Minting<'_>,
    request_id: &str,
    secret: &PossessionSecret,
    viewer_ip: Option<&str>,
    now: Timestamp,
) -> Result<Admission, StoreError> {
    let Some(counters) = store.load_counters(minting.event_id).await? else {
        return Ok(Admission::EventNotFound);
    };

    // Both rows are read, because which one answers depends on both: a
    // pre-queue row inside its shard's opened count is the visitor's place
    // whatever `Positions` holds (issue #238), and only a live joiner or a
    // straggler is answered by their `Positions` row. This is the same order
    // `read` resolves `/_wr/v1/queue_num` in, so the number a visitor is shown is
    // the number they are admitted at.
    let position_row = store.load_position(request_id).await?;
    let prequeue = store.load_prequeue(request_id).await?;

    let grant = match decide(
        &counters,
        prequeue.as_ref(),
        position_row.as_ref(),
        secret,
        now,
    ) {
        Ok(grant) => grant,
        Err(denied) => return Ok(Admission::Refused(denied)),
    };

    // A visitor is admitted against the budget once. A reload, a second tab or
    // a retried request arrives here again with the row already marked, and
    // gets its session without taking a second unit -- the origin was already
    // charged for this person.
    //
    // What they get again is the same visit, not a new one (issue #237). A
    // place that has reached the front can be handed around -- the id and its
    // secret are two strings -- and if every call minted a full-length session
    // one admission would admit an unlimited number of people for as long as
    // the row lived, each with a session bound to their own network. The
    // session runs from the first admission; once that has passed there is
    // nothing left to hand out.
    let admitted_at = if let Some(at) = grant.admitted_at {
        at
    } else {
        let Some(shelf) = grant.shelf else {
            return Ok(Admission::Refused(Denied::Corrupt));
        };
        if !take_held_spot(store, minting.event_id, shelf).await {
            return Ok(Admission::Refused(Denied::AtCapacity {
                position: grant.position,
            }));
        }
        // The unit is taken, so the visitor is admitted whether or not the
        // mark lands. A mark that fails costs one extra unit if they call
        // again: fewer people admitted than intended, never more.
        if let Err(e) = store
            .mark_admitted(request_id, grant.position, &grant.digest, now)
            .await
        {
            tracing::error!(error = %e, event = "admission_claim_failed", "could not mark the visitor admitted; admitting anyway");
        }
        now
    };

    // A session end past jiff's range is no end at all: refuse it rather than
    // sign one that never expires.
    let Ok(expires_at) = admitted_at.checked_add(minting.session) else {
        return Ok(Admission::Refused(Denied::SessionExpired));
    };
    if expires_at <= now {
        return Ok(Admission::Refused(Denied::SessionExpired));
    }
    let session = Session {
        event_id: minting.event_id.to_owned(),
        request_id: request_id.to_owned(),
        issued_at: now,
        expires_at,
        // Always tagged when the address is known, so turning IP binding on
        // (issue #61) also covers sessions minted before it was switched on.
        ip_tag: viewer_ip.and_then(|ip| minting.key.ip_tag(ip)),
    };
    // A signing failure must not become an empty cookie: an empty string is
    // not a well-formed JWS, so the gate would refuse it and the visitor would
    // bounce between the origin and the waiting page while being told they
    // were admitted. The unit of budget is already taken, which is the safer
    // order: taken-but-not-admitted admits one fewer, the opposite one more.
    let Ok(credential) = session.sign(minting.key) else {
        tracing::error!(
            event = "session_sign_failed",
            "could not sign the session credential; refusing rather than issuing an empty cookie"
        );
        return Ok(Admission::SignFailed);
    };
    Ok(Admission::Admitted {
        position: grant.position,
        expires_at,
        credential,
    })
}

/// Takes the visitor's held spot from their shelf. The call's spots are
/// spread evenly over the shelf's counters and taken at random, so one counter
/// can be empty while another still has this visitor's spot: every counter is
/// tried, from a random one, before answering "at capacity".
async fn take_held_spot<S: Store>(store: &S, event_id: &str, shelf: Shelf) -> bool {
    let start = match Shard::random() {
        Ok(shard) => shard.index(),
        Err(e) => {
            tracing::error!(error = %e, event = "arrival_shard_draw_failed", "failed to draw a random shard; not admitting");
            return false;
        }
    };
    for step in 0..SHARDS {
        let Some(shard) = Shard::new((start + step) % SHARDS) else {
            continue;
        };
        match store.take_spot(event_id, shelf, shard).await {
            Ok(true) => return true,
            Ok(false) => {}
            Err(e) => {
                tracing::error!(error = %e, event = "arrival_record_failed", "failed to take a held spot; not admitting");
                return false;
            }
        }
    }
    false
}

/// The `Set-Cookie` that carries a minted session. `Path=/` because the gate
/// checks it on every protected request; `HttpOnly` because no page script
/// needs it.
#[must_use]
pub fn session_set_cookie(name: &str, credential: &str, ttl: SignedDuration) -> String {
    let max_age = ttl.as_secs();
    format!("{name}={credential}; Path=/; Max-Age={max_age}; Secure; HttpOnly; SameSite=Lax")
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
/// (resolving the stored control against the fail-open deadline at `now`, issue
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
    now: Timestamp,
) -> Result<Grant, Denied> {
    use wr_common::AdmissionControl;

    // Possession first (issue #62): a caller who only knows the request id
    // learns nothing further here -- not the queue state, not the position.
    // The row that would answer the position is the row whose digest counts.
    let stored = match (cohort_position(counters, prequeue), position_row, prequeue) {
        // A counted cohort member: the digest they registered with, never one
        // a later join for the same id wrote onto a `Positions` row.
        (Some(_), _, Some(row)) | (_, None, Some(row)) => row.h.as_ref(),
        (_, Some(row), _) => row.digest.as_ref(),
        (_, None, None) => return Err(Denied::NotRegistered),
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

    let admitted_at = match position_row {
        Some(&PositionRow {
            status: PositionStatus::Admitted,
            admitted_at,
            ..
        }) => {
            // A row marked admitted with no time is not one this code
            // writes. Treating it as admitted at the epoch refuses it as
            // expired, which is the direction that mints nothing.
            Some(admitted_at.unwrap_or(Timestamp::UNIX_EPOCH))
        }
        Some(&PositionRow {
            status: PositionStatus::Issued,
            ..
        })
        | None => None,
    };
    // Someone already let in has their session back whenever they ask; only
    // a visitor taking their turn needs it still to be held.
    let shelf = match (admitted_at, counters.turn_of(position, now)) {
        (Some(_), _) => None,
        (None, TurnState::Held(shelf)) => Some(shelf),
        (None, TurnState::Missed) => return Err(Denied::TurnMissed { position }),
        (None, TurnState::NotCalled) => {
            return Err(Denied::StillQueued {
                position,
                serving: counters.serving_counter,
            });
        }
    };

    Ok(Grant {
        position,
        digest,
        admitted_at,
        shelf,
    })
}

/// The cohort position of a pre-queue row that the open counted, if it is one.
///
/// `None` for no row, an unopened event, a straggler (a local index at or past
/// its shard's opened count) or a corrupt shard -- every case in which the
/// pre-queue row does not, by itself, say where the visitor stands.
fn cohort_position(counters: &Counters, prequeue: Option<&PreQueueItem>) -> Option<u64> {
    match counters.resolve_prequeue(prequeue?) {
        Ok(ResolvedPosition::PreQueue(position)) => Some(position),
        Ok(ResolvedPosition::LiveJoin) | Err(ResolveError::NotOpen | ResolveError::BadShard) => {
            None
        }
    }
}

/// The visitor's position, from whichever path registered them.
///
/// A pre-queue row the open counted wins over a `Positions` row (issue #238).
/// A cohort member has no `Positions` row until they are admitted, so a join
/// for their id that is processed after the open -- an SQS redelivery across
/// T-0, or a client retrying a join whose response was lost -- passes the live
/// path's `attribute_not_exists` guard and mints a position behind the whole
/// cohort. Letting that row answer would demote a visitor who is shown their
/// cohort position by `/_wr/v1/queue_num`, which resolves the pre-queue row first.
/// The row an admission claim writes carries the same cohort position, so for
/// every other cohort member the two agree anyway.
///
/// A `Positions` row answers for a live joiner and for a straggler, whose
/// pre-queue row raced the open and holds no position of its own.
fn resolve_position(
    counters: &Counters,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<&PositionRow>,
) -> Result<u64, Denied> {
    if let Some(position) = cohort_position(counters, prequeue) {
        return Ok(position);
    }

    if let Some(&PositionRow {
        position, status, ..
    }) = position_row
    {
        return match status {
            // An already-admitted row is still a valid claim on the position.
            // Refusing one would strand a visitor whose first response never
            // arrived, or who reloaded the page, for a mistake that was not
            // theirs -- and the cookie they are asking for is one they are
            // entitled to. What the status governs is whether a unit of the
            // admission budget is taken, not admission; `admit` reads it.
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
            origin_concurrency: None,
            stored_control: StoredControl::Open,
            fail_open_until: None,
            starts_at: None,
            // One call, at the tests' time, holding everyone it called.
            calls: vec![wr_common::Call {
                first: 0,
                until: Timestamp::constant(0, 0) + wr_common::TURN_HOLD,
                shelf: shelf(),
            }],
        }
    }

    fn shelf() -> Shelf {
        Shelf::new(0).unwrap()
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
            admitted_at: match status {
                PositionStatus::Issued => None,
                PositionStatus::Admitted => Some(Timestamp::constant(1_700_000_000, 0)),
            },
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
            Timestamp::constant(0, 0),
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
            Timestamp::constant(0, 0),
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
                Timestamp::constant(0, 0)
            )
            .is_ok()
        );
        assert!(
            decide(
                &counters(5),
                None,
                Some(&row(5, PositionStatus::Issued)),
                &secret(),
                Timestamp::constant(0, 0)
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
                Timestamp::constant(0, 0)
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
        c.fail_open_until = Some(Timestamp::constant(1000, 0));
        assert_eq!(
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                Timestamp::constant(500, 0)
            )
            .unwrap_err(),
            Denied::NotAdmitting
        );
        // Once the epoch lapses, the stored Open control governs again, and
        // the controller's next pass calls them.
        c.calls[0].until = Timestamp::constant(1000, 0) + wr_common::TURN_HOLD;
        assert!(
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                Timestamp::constant(1000, 0)
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
                    Timestamp::constant(0, 0)
                )
                .unwrap_err(),
                Denied::NotAdmitting
            );
        }
    }

    #[test]
    fn a_called_visitor_whose_turn_passed_is_refused() {
        let passed = Timestamp::constant(0, 0) + wr_common::TURN_HOLD;
        assert_eq!(
            decide(
                &counters(10),
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                passed
            ),
            Err(Denied::TurnMissed { position: 3 })
        );
        // Called with no call kept that covers them: long past their turn.
        let mut c = counters(10);
        c.calls.clear();
        assert_eq!(
            decide(
                &c,
                None,
                Some(&row(3, PositionStatus::Issued)),
                &secret(),
                Timestamp::constant(0, 0)
            ),
            Err(Denied::TurnMissed { position: 3 })
        );
    }

    #[test]
    fn a_visitor_already_let_in_has_their_session_back_after_their_turn() {
        let passed = Timestamp::constant(0, 0) + wr_common::TURN_HOLD;
        let grant = decide(
            &counters(10),
            None,
            Some(&row(3, PositionStatus::Admitted)),
            &secret(),
            passed,
        )
        .unwrap();
        assert_eq!(grant.shelf, None);
        assert!(grant.admitted_at.is_some());
    }

    #[test]
    fn an_already_admitted_visitor_is_admitted_again() {
        // Their first response may never have reached them, or they reloaded.
        // Refusing would strand a visitor holding a position that is still
        // theirs, for a failure that was not theirs. What being admitted
        // already governs is whether a second unit of budget is taken, which
        // the grant reports and `admit` acts on.
        for (status, admitted_at, held) in [
            (PositionStatus::Issued, None, Some(shelf())),
            (
                PositionStatus::Admitted,
                Some(Timestamp::constant(1_700_000_000, 0)),
                None,
            ),
        ] {
            assert_eq!(
                decide(
                    &counters(10),
                    None,
                    Some(&row(3, status)),
                    &secret(),
                    Timestamp::constant(0, 0)
                )
                .unwrap(),
                Grant {
                    position: 3,
                    digest: digest(),
                    admitted_at,
                    shelf: held,
                }
            );
        }
    }

    #[test]
    fn an_unregistered_visitor_is_refused() {
        assert_eq!(
            decide(
                &counters(10),
                None,
                None,
                &secret(),
                Timestamp::constant(0, 0)
            )
            .unwrap_err(),
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
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        let grant = decide(&c, Some(&row), None, &secret(), Timestamp::constant(0, 0)).unwrap();
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
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        assert_eq!(
            decide(&c, Some(&row), None, &secret(), Timestamp::constant(0, 0)).unwrap_err(),
            Denied::NotOpen
        );
    }

    #[test]
    fn a_live_join_row_answers_for_a_straggler() {
        // A straggler has both rows: its pre-queue index is at or past the
        // shard's opened count (2 here), so it holds no cohort position and
        // the claimed live position is the authoritative one.
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 2,
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        let grant = decide(
            &counters(100),
            Some(&pq),
            Some(&row(42, PositionStatus::Issued)),
            &secret(),
            Timestamp::constant(0, 0),
        )
        .unwrap();
        assert_eq!(grant.position, 42);
    }

    #[test]
    fn a_counted_cohort_member_is_not_demoted_by_a_stray_live_row() {
        // Issue #238: a join for a cohort member's id processed after the open
        // (a redelivery across T-0, a retried join) mints a live position
        // behind the cohort. The pre-queue row the open counted still answers,
        // so the visitor is admitted at the position /_wr/v1/queue_num shows them.
        let c = counters(20);
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 1,
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        let cohort_only =
            decide(&c, Some(&pq), None, &secret(), Timestamp::constant(0, 0)).unwrap();
        let with_stray = decide(
            &c,
            Some(&pq),
            Some(&row(5_000, PositionStatus::Issued)),
            &secret(),
            Timestamp::constant(0, 0),
        )
        .unwrap();
        assert_eq!(with_stray.position, cohort_only.position);
        assert!(with_stray.position < c.participant_count.unwrap());
    }

    #[test]
    fn a_stray_live_row_cannot_replace_a_cohort_members_secret() {
        // The stray row carries whatever digest the later join sent. A caller
        // who knows only the request id must not be able to lock the holder
        // out, or take the place, by joining again under it.
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 1,
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        let other = PossessionSecret::parse("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        let mut stray = row(5_000, PositionStatus::Issued);
        stray.digest = Some(other.digest());
        let c = counters(20);
        assert!(
            decide(
                &c,
                Some(&pq),
                Some(&stray),
                &secret(),
                Timestamp::constant(0, 0)
            )
            .is_ok()
        );
        assert_eq!(
            decide(
                &c,
                Some(&pq),
                Some(&stray),
                &other,
                Timestamp::constant(0, 0)
            )
            .unwrap_err(),
            Denied::NotHolder
        );
    }

    #[test]
    fn the_wrong_secret_is_refused_before_anything_about_the_queue_is_said() {
        let wrong = PossessionSecret::parse("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB").unwrap();
        // Even at a reached position, and even while paused (which would
        // otherwise answer NotAdmitting): possession is checked first.
        let mut paused = counters(10);
        paused.stored_control = StoredControl::Paused;
        for c in [counters(10), counters(3), paused] {
            assert_eq!(
                decide(
                    &c,
                    None,
                    Some(&row(3, PositionStatus::Issued)),
                    &wrong,
                    Timestamp::constant(0, 0)
                )
                .unwrap_err(),
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
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        assert_eq!(
            decide(
                &counters(u64::MAX),
                Some(&pq),
                None,
                &wrong,
                Timestamp::constant(0, 0)
            )
            .unwrap_err(),
            Denied::NotHolder
        );
    }

    #[test]
    fn a_row_without_a_digest_cannot_be_redeemed() {
        let legacy = PositionRow {
            position: 3,
            status: PositionStatus::Issued,
            digest: None,
            admitted_at: None,
        };
        assert_eq!(
            decide(
                &counters(10),
                None,
                Some(&legacy),
                &secret(),
                Timestamp::constant(0, 0)
            )
            .unwrap_err(),
            Denied::NotHolder
        );
    }

    #[test]
    fn the_positions_row_digest_is_the_one_that_counts() {
        // For a straggler the Positions row answers, so it must carry the
        // digest; the PreQueue digest is not consulted behind it.
        let pq = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 2,
            t: Timestamp::constant(1_788_000_000, 0),
            h: Some(digest()),
        };
        let mut admitted = row(3, PositionStatus::Admitted);
        admitted.digest = None;
        assert_eq!(
            decide(
                &counters(10),
                Some(&pq),
                Some(&admitted),
                &secret(),
                Timestamp::constant(0, 0)
            )
            .unwrap_err(),
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
