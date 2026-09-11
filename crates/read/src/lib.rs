//! Read-path logic for the `read` Lambda: the `/status` payload and the
//! `/queue_num` position resolution. Pure functions over the domain types so
//! they run without AWS; the handler fetches the items and calls these.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;
#[cfg(test)]
use wr_common::StoredControl;
use wr_common::{
    Counters, Phase, PreQueueItem, ResolveError, ResolvedPosition, SHARDS, ServingState, resolve,
    serving_state,
};

/// Holds the event's `Counters` item for a beat inside one execution
/// environment.
///
/// `/status` answers the same document to everyone, so the edge collapses it to
/// roughly one origin fetch per second however many people are waiting.
/// `/queue_num` cannot: its answer is per visitor, so its cache key is per
/// visitor and every poll is an origin request. Both read the same single
/// `Counters` item, and a `DynamoDB` partition serves at most 3,000 read units
/// per second — one item's worth of traffic for the whole waiting room. A
/// million waiting visitors would ask for it tens of thousands of times a
/// second.
///
/// Caching it here makes that one read per environment per TTL instead of one
/// per request, which is what keeps origin load flat as the room grows rather
/// than scaling with it.
///
/// Staleness is bounded by the TTL and costs nothing that matters:
/// `serving_position` is the only field that moves once an event is sealed, the
/// controller advances it far more slowly than this, and the edge already
/// serves the same document from cache for a comparable window.
#[derive(Debug)]
pub struct CountersCache {
    ttl: Duration,
    /// `None` while empty. The inner `Option` distinguishes "no event item
    /// exists" from "not looked up yet", so a missing event is cached too and
    /// a flood of requests for an event that does not exist costs one read per
    /// TTL rather than one each.
    slot: Mutex<Option<(Instant, Option<Counters>)>>,
}

impl CountersCache {
    /// A cache holding entries for `ttl`.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            slot: Mutex::new(None),
        }
    }

    /// The cached lookup if it is still fresh at `now`.
    ///
    /// The outer `Option` is the cache hit; the inner one is whether the event
    /// item exists. A poisoned lock reports a miss rather than propagating:
    /// the fallback is a live read, which is always correct.
    #[must_use]
    pub fn get(&self, now: Instant) -> Option<Option<Counters>> {
        let guard = self.slot.lock().ok()?;
        let (stored_at, ref value) = *guard.as_ref()?;
        if now.duration_since(stored_at) < self.ttl {
            Some(value.clone())
        } else {
            None
        }
    }

    /// Records a lookup made at `now`.
    pub fn put(&self, now: Instant, value: Option<Counters>) {
        if let Ok(mut guard) = self.slot.lock() {
            *guard = Some((now, value));
        }
    }
}

/// The adaptive poll policy published on `/status` (#69): the
/// floor, ceiling, and divisor `waiting.js` clamps its poll interval to.
/// Terraform-set, not an admin lever — see the ADR for why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PollPolicy {
    pub floor_ms: u32,
    pub ceiling_ms: u32,
    pub divisor: u32,
}

/// Sanity bounds a published policy must fall within, mirroring
/// `waiting.js`'s own `MIN_MS`/`MAX_MS`/`MAX_DIVISOR` check. `read` enforces
/// the same range as the client rather than relying solely on Terraform's
/// `validation` blocks, so a direct Lambda environment edit or a second
/// Terraform root cannot publish a policy every client would reject anyway.
const MIN_INTERVAL_MS: u32 = 1_000;
const MAX_INTERVAL_MS: u32 = 300_000;
const MAX_DIVISOR: u32 = 1_000;

/// Parses the three poll-policy environment variables into a [`PollPolicy`],
/// or `None` if any is absent, fails to parse as `u32`, falls outside
/// [`MIN_INTERVAL_MS`]/[`MAX_INTERVAL_MS`]/[`MAX_DIVISOR`], or describes an
/// inverted clamp (`ceiling_ms < floor_ms`).
///
/// All-or-nothing: the server never sends a partial policy, so a client
/// cannot tell which field to trust in one, and `/status` omits the whole
/// object rather than publish one this function could already tell is wrong.
#[must_use]
pub fn parse_poll_policy(
    floor_ms: Option<&str>,
    ceiling_ms: Option<&str>,
    divisor: Option<&str>,
) -> Option<PollPolicy> {
    let floor_ms: u32 = floor_ms?.parse().ok()?;
    let ceiling_ms: u32 = ceiling_ms?.parse().ok()?;
    let divisor: u32 = divisor?.parse().ok()?;
    if !(MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&floor_ms)
        || !(MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&ceiling_ms)
        || !(1..=MAX_DIVISOR).contains(&divisor)
        || ceiling_ms < floor_ms
    {
        return None;
    }
    Some(PollPolicy {
        floor_ms,
        ceiling_ms,
        divisor,
    })
}

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
    /// The operator's target admission rate in visitors per second, absent
    /// until one is set.
    ///
    /// Published so a waiting visitor gets a wait estimate on their first poll
    /// rather than after a minute of watching the cursor. It is a target, not a
    /// measurement — the controller corrects releases against a no-show rate,
    /// so the cursor's real speed differs — which is why a client should prefer
    /// the movement it observes once it has enough of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_rate: Option<u32>,
    /// The adaptive poll policy (#69), absent when Terraform has
    /// not set one — the client then keeps its own fixed-interval default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_policy: Option<PollPolicy>,
}

/// A resolved `/queue_num` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueueNumResponse {
    pub position: u64,
    /// True when the position came from the live-join sequence rather than the
    /// permutation (a straggler that raced the seal, or a join after opening).
    pub live_join: bool,
}

/// A resolved pre-queue registration, distinguishing a counted registrant
/// from a straggler whose `PreQueue` row raced the seal.
///
/// The handler needs this distinction, not just a response body: a
/// [`ResolvedQueueNum::Straggler`] has no real position of its own yet and
/// must fall through to the same `Positions` lookup a live joiner uses,
/// answering 404 when no row is there — the 404 is what a client stuck on a
/// stale `PreQueue` row needs in order to recover by re-joining. A
/// [`ResolvedQueueNum::PreQueue`] answers directly and never reaches that
/// lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedQueueNum {
    PreQueue(QueueNumResponse),
    Straggler,
}

/// Builds the `/status` payload from the counters item, the deploy-time poll
/// policy (`None` when Terraform has not set one), and `now` (epoch seconds),
/// which resolves the stored admission control against the fail-open epoch
/// (issue #71).
#[must_use]
pub fn status(counters: &Counters, poll_policy: Option<PollPolicy>, now: u64) -> StatusResponse {
    StatusResponse {
        event_id: counters.event_id.clone(),
        phase: counters.phase,
        serving_state: serving_state(
            counters.phase,
            resolve(counters.stored_control, counters.fail_open_until, now),
        ),
        serving_position: counters.serving_counter,
        participant_count: counters.participant_count,
        prequeue_offsets: counters.prequeue_offsets,
        message: counters.message.clone(),
        target_rate: counters.target_rate,
        poll_policy,
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
/// permutation. A row whose local index is at or past its shard's own issued
/// count (a join that raced the seal) resolves to
/// [`ResolvedQueueNum::Straggler`] instead — the permutation is never
/// evaluated out of domain, and the caller falls through to a `Positions`
/// lookup rather than answering with a position this function does not have.
///
/// # Errors
///
/// [`QueueNumError::NotSealed`] before the seal; [`QueueNumError::BadShard`] if
/// the stored shard is out of range.
pub fn queue_num(
    counters: &Counters,
    row: &PreQueueItem,
) -> Result<ResolvedQueueNum, QueueNumError> {
    match counters.resolve_prequeue(row) {
        Ok(ResolvedPosition::PreQueue(position)) => {
            Ok(ResolvedQueueNum::PreQueue(QueueNumResponse {
                position,
                live_join: false,
            }))
        }
        Ok(ResolvedPosition::LiveJoin) => Ok(ResolvedQueueNum::Straggler),
        Err(ResolveError::NotSealed) => Err(QueueNumError::NotSealed),
        Err(ResolveError::BadShard) => Err(QueueNumError::BadShard),
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::cast_possible_truncation,
        clippy::panic,
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
            shuffle_seed: Some(seed),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
        }
    }

    fn row(s: u8, l: u64) -> PreQueueItem {
        PreQueueItem {
            r: "req-1".to_owned(),
            s,
            l,
            t: 1_788_000_000,
        }
    }

    #[test]
    fn status_hides_seal_outputs_before_seal() {
        let counters = Counters {
            event_id: "evt-1".to_owned(),
            phase: Phase::PreQueue,
            queue_counter: 0,
            serving_counter: 0,
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
        };
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
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
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert_eq!(json["event_id"], "evt-1");
        assert!(json["event_id"].as_str().is_some_and(|s| !s.is_empty()));
    }

    #[test]
    fn status_exposes_seal_outputs_when_active() {
        let counters = sealed_counters([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], [3u8; 32]);
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert_eq!(json["phase"], "active");
        assert_eq!(json["participant_count"], 10);
        assert_eq!(json["serving_position"], 5);
        assert!(json.get("prequeue_offsets").is_some());
    }

    #[test]
    fn status_surfaces_the_broadcast_message_when_set() {
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        counters.message = Some("Doors open at noon".to_owned());
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert_eq!(json["message"], "Doors open at noon");
    }

    #[test]
    fn status_omits_the_message_when_absent() {
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert!(json.get("message").is_none());
    }

    #[test]
    fn status_publishes_the_target_rate_when_set() {
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        counters.target_rate = Some(250);
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert_eq!(json["target_rate"], 250);
    }

    #[test]
    fn status_omits_the_target_rate_when_unset() {
        // A waiting page must be able to tell "no rate set" from "rate is
        // zero": the first means the operator has not started admitting and no
        // estimate can be made, the second would read as an infinite wait.
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert!(json.get("target_rate").is_none());
    }

    #[test]
    fn status_omits_poll_policy_when_unset() {
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let json = serde_json::to_value(status(&counters, None, 0)).unwrap();
        assert!(json.get("poll_policy").is_none());
    }

    #[test]
    fn status_publishes_poll_policy_when_set() {
        let counters = sealed_counters([1; SHARDS], [7u8; 32]);
        let policy = PollPolicy {
            floor_ms: 5000,
            ceiling_ms: 30000,
            divisor: 10,
        };
        let json = serde_json::to_value(status(&counters, Some(policy), 0)).unwrap();
        assert_eq!(json["poll_policy"]["floor_ms"], 5000);
        assert_eq!(json["poll_policy"]["ceiling_ms"], 30000);
        assert_eq!(json["poll_policy"]["divisor"], 10);
    }

    #[test]
    fn parse_poll_policy_accepts_a_valid_policy() {
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("30000"), Some("10")),
            Some(PollPolicy {
                floor_ms: 5000,
                ceiling_ms: 30000,
                divisor: 10,
            })
        );
    }

    #[test]
    fn parse_poll_policy_rejects_when_any_field_is_missing() {
        // All-or-nothing: the server never sends a partial policy, so a
        // client would have no sound default for whichever piece is missing.
        assert_eq!(parse_poll_policy(None, Some("30000"), Some("10")), None);
        assert_eq!(parse_poll_policy(Some("5000"), None, Some("10")), None);
        assert_eq!(parse_poll_policy(Some("5000"), Some("30000"), None), None);
        assert_eq!(parse_poll_policy(None, None, None), None);
    }

    #[test]
    fn parse_poll_policy_rejects_unparseable_numbers() {
        assert_eq!(
            parse_poll_policy(Some("not-a-number"), Some("30000"), Some("10")),
            None
        );
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("30000"), Some("-1")),
            None
        );
    }

    #[test]
    fn parse_poll_policy_rejects_a_zero_divisor() {
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("30000"), Some("0")),
            None
        );
    }

    #[test]
    fn parse_poll_policy_rejects_an_inverted_clamp() {
        // ceiling < floor describes no valid interval at all; guessing at a
        // repair is worse than treating the whole document as wrong.
        assert_eq!(
            parse_poll_policy(Some("30000"), Some("5000"), Some("10")),
            None
        );
    }

    #[test]
    fn parse_poll_policy_accepts_an_equal_floor_and_ceiling() {
        // The fixed-interval case: floor == ceiling is a valid, degenerate
        // clamp, not an inversion.
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("5000"), Some("1")),
            Some(PollPolicy {
                floor_ms: 5000,
                ceiling_ms: 5000,
                divisor: 1,
            })
        );
    }

    #[test]
    fn parse_poll_policy_rejects_values_outside_the_sanity_bounds() {
        // Mirrors waiting.js's own MIN_MS/MAX_MS/MAX_DIVISOR check: a
        // policy that is in-range numerically but outside the range every
        // client already enforces must not reach /status either.
        assert_eq!(
            parse_poll_policy(Some("999"), Some("30000"), Some("10")),
            None,
            "floor below MIN_INTERVAL_MS"
        );
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("300001"), Some("10")),
            None,
            "ceiling above MAX_INTERVAL_MS"
        );
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("30000"), Some("1001")),
            None,
            "divisor above MAX_DIVISOR"
        );
        assert_eq!(
            parse_poll_policy(Some("5000"), Some("30000"), Some("0")),
            None,
            "divisor below 1"
        );
        assert_eq!(
            parse_poll_policy(Some("1000"), Some("300000"), Some("1000")),
            Some(PollPolicy {
                floor_ms: 1000,
                ceiling_ms: 300_000,
                divisor: 1000,
            }),
            "the bounds themselves are inclusive"
        );
    }

    #[test]
    fn poll_policy_wire_field_names_are_frozen() {
        // waiting.js's adoptPolicy reads these three literal keys
        // (p.floor_ms, p.ceiling_ms, p.divisor) off the /status document.
        // Renaming any of them here ships silently and degrades every
        // visitor to the fixed interval, with nothing else to catch it —
        // this pins the contract the way the repo's other frozen wire
        // encodings already do (.kiro/steering/testing.md).
        let policy = PollPolicy {
            floor_ms: 1,
            ceiling_ms: 2,
            divisor: 3,
        };
        assert_eq!(
            serde_json::to_value(policy).unwrap(),
            serde_json::json!({ "floor_ms": 1, "ceiling_ms": 2, "divisor": 3 })
        );
    }

    // --- Property tests -------------------------------------------------

    use proptest::prelude::{any, prop_assert_eq, proptest};

    proptest! {
        #[test]
        fn parse_poll_policy_matches_its_stated_contract(
            floor in any::<u32>(),
            ceiling in any::<u32>(),
            divisor in any::<u32>(),
        ) {
            let result = parse_poll_policy(
                Some(&floor.to_string()),
                Some(&ceiling.to_string()),
                Some(&divisor.to_string()),
            );
            let expected_some = (MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&floor)
                && (MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&ceiling)
                && (1..=MAX_DIVISOR).contains(&divisor)
                && ceiling >= floor;
            prop_assert_eq!(result.is_some(), expected_some);
            if let Some(policy) = result {
                prop_assert_eq!(policy.floor_ms, floor);
                prop_assert_eq!(policy.ceiling_ms, ceiling);
                prop_assert_eq!(policy.divisor, divisor);
            }
        }
    }

    #[test]
    fn status_publishes_serving_state() {
        // Active event -> running; pause it -> paused; fail-open -> fail_open.
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        assert_eq!(
            serde_json::to_value(status(&counters, None, 0)).unwrap()["serving_state"],
            "running"
        );
        counters.stored_control = StoredControl::Paused;
        assert_eq!(
            serde_json::to_value(status(&counters, None, 0)).unwrap()["serving_state"],
            "paused"
        );
        counters.fail_open_until = 100;
        assert_eq!(
            serde_json::to_value(status(&counters, None, 50)).unwrap()["serving_state"],
            "fail_open"
        );
    }

    #[test]
    fn status_serving_state_is_closed_before_active() {
        // An idle event with open admission reads Closed to a visitor.
        let mut counters = sealed_counters([1; SHARDS], [7u8; 32]);
        counters.phase = Phase::Idle;
        assert_eq!(
            serde_json::to_value(status(&counters, None, 0)).unwrap()["serving_state"],
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
            shuffle_seed: None,
            participant_count: None,
            prequeue_offsets: None,
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
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
                match queue_num(&counters, &row(shard as u8, l)).unwrap() {
                    ResolvedQueueNum::PreQueue(resp) => {
                        assert!(!resp.live_join);
                        assert!(resp.position < n);
                        assert!(positions.insert(resp.position), "duplicate position");
                    }
                    ResolvedQueueNum::Straggler => panic!("issued index resolved as a straggler"),
                }
            }
        }
        assert_eq!(positions.len() as u64, n);
    }

    #[test]
    fn queue_num_straggler_is_reported_distinctly() {
        let counts = [3, 0, 5, 1, 0, 0, 2, 0, 0, 4]; // last shard's own count is 4
        let counters = sealed_counters(counts, [42u8; 32]);
        // Last shard local index 4 is past its own count: a straggler.
        assert_eq!(
            queue_num(&counters, &row(9, 4)).unwrap(),
            ResolvedQueueNum::Straggler
        );
    }

    #[test]
    fn queue_num_straggler_on_an_interior_shard_is_reported_too() {
        // The case the old global i >= N rule missed: an over-count on a
        // shard that is NOT the last one. Shard 2's own count is 5 (offsets
        // [0,3,3,8,...]), so local 5 is past it even though offset[2] + 5 =
        // 8 still lands inside [0, N).
        let counts = [3, 0, 5, 1, 0, 0, 2, 0, 0, 4];
        let counters = sealed_counters(counts, [42u8; 32]);
        assert_eq!(
            queue_num(&counters, &row(2, 5)).unwrap(),
            ResolvedQueueNum::Straggler
        );
    }

    #[test]
    fn queue_num_bad_shard_is_rejected() {
        let counters = sealed_counters([1; SHARDS], [1u8; 32]);
        let bad = PreQueueItem {
            r: "req-1".to_owned(),
            s: SHARDS as u8,
            l: 0,
            t: 1_788_000_000,
        };
        assert_eq!(queue_num(&counters, &bad), Err(QueueNumError::BadShard));
    }

    // --- counters cache -----------------------------------------------------

    const TTL: Duration = Duration::from_secs(1);

    #[test]
    fn an_empty_cache_is_a_miss() {
        let cache = CountersCache::new(TTL);
        assert!(cache.get(Instant::now()).is_none());
    }

    #[test]
    fn a_fresh_entry_is_served_without_a_read() {
        let cache = CountersCache::new(TTL);
        let now = Instant::now();
        let counters = sealed_counters([1, 0, 0, 0, 0, 0, 0, 0, 0, 0], [3u8; 32]);
        cache.put(now, Some(counters.clone()));
        assert_eq!(cache.get(now + TTL / 2), Some(Some(counters)));
    }

    #[test]
    fn an_entry_expires_exactly_at_the_ttl() {
        let cache = CountersCache::new(TTL);
        let now = Instant::now();
        cache.put(
            now,
            Some(sealed_counters([1, 0, 0, 0, 0, 0, 0, 0, 0, 0], [3u8; 32])),
        );
        // At the boundary the entry is already stale: the handler must re-read
        // rather than serve a value older than the window it promised.
        assert!(cache.get(now + TTL).is_none());
        assert!(cache.get(now + TTL + Duration::from_millis(1)).is_none());
    }

    #[test]
    fn a_missing_event_is_cached_too() {
        // Otherwise a flood aimed at an event that does not exist costs one
        // DynamoDB read per request, which is the case the cache exists for.
        let cache = CountersCache::new(TTL);
        let now = Instant::now();
        cache.put(now, None);
        assert_eq!(cache.get(now + TTL / 2), Some(None));
    }

    #[test]
    fn a_later_put_replaces_an_earlier_one() {
        let cache = CountersCache::new(TTL);
        let now = Instant::now();
        let first = sealed_counters([1, 0, 0, 0, 0, 0, 0, 0, 0, 0], [3u8; 32]);
        let mut second = first.clone();
        second.serving_counter = 99;
        cache.put(now, Some(first));
        cache.put(now + TTL / 2, Some(second.clone()));
        // Freshness is measured from the newer write, so the entry outlives
        // the first put's expiry.
        assert_eq!(cache.get(now + TTL), Some(Some(second)));
    }
}
