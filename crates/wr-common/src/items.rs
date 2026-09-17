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

use crate::expr::STARTS_AT_ATTR;
use crate::ids::{Phase, StoredControl};
use crate::permutation::{Assignment, CohortOffsets, SHARDS, Seed};

/// What has happened to a `Positions` row.
///
/// Two states, and the transition between them is a conditional write: it is
/// what makes admission happen once per visitor (issue #62). Neither is a
/// refusal — a visitor whose row says `Admitted` is admitted again, with a
/// freshly signed session, because the first response may simply not have
/// reached them. What the transition decides is whether their arrival is
/// counted, and the controller's no-show correction is only meaningful if it is
/// counted once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    /// A position claimed from `queue_counter`, not yet exchanged for a
    /// session.
    Issued,
    /// The position has been exchanged for a session and the arrival counted.
    Admitted,
}

/// A `PreQueue` row: shard `s` and local index `l` for a request, written at
/// registration. The global index is derived on read, never stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreQueueItem {
    /// `request_id`, the partition key.
    pub r: String,
    /// Shard index in `0..SHARDS`.
    pub s: u8,
    /// Local index within the shard.
    pub l: u64,
    /// Registration timestamp, epoch seconds.
    pub t: u64,
}

/// A `Positions` row.
///
/// A live joiner gets one at registration, carrying the position claimed from
/// `queue_counter`. A pre-queue member has none — their position is derived
/// from the seed on read — until they are admitted, when the admission claim
/// creates one to record that their arrival has been counted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionItem {
    pub request_id: String,
    /// The queue position this request holds: claimed from `queue_counter` for
    /// a live joiner, or the seed-derived one stamped at admission for a
    /// pre-queue member.
    pub queue_position: u64,
    /// Server-stamped arrival time in epoch seconds. Authoritative: a
    /// client-supplied request id may also carry a timestamp (a `UUIDv7`
    /// shape), but that one is untrusted and, since issue #59, not even
    /// guaranteed to be present.
    pub entry_time: u64,
    pub status: PositionStatus,
    /// Post-event storage reclamation only, never an admission deadline
    /// (ADR-0031). A deadline set when the position is issued would expire
    /// people for waiting the length of the queue they are waiting in, so
    /// nothing reads this to decide whether a visitor may be admitted; the
    /// controller's no-show correction is what compensates for absentees.
    pub ttl: u64,
}

/// The single `Counters` item for an event: all sequences, the sharded
/// pre-queue counter, phase, and the open outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counters {
    pub event_id: String,
    pub phase: Phase,
    /// Live-join sequence; starts at `participant_count` after the open so live
    /// joiners are numbered behind the whole pre-queue cohort.
    pub queue_counter: u64,
    /// Admission cursor advanced by the controller.
    pub serving_counter: u64,
    /// Set only at the open, absent before: the 256-bit permutation seed.
    pub shuffle_seed: Option<[u8; 32]>,
    /// Set at the open: cohort size `N`, the sum of the pre-queue shards.
    pub participant_count: Option<u64>,
    /// Set at the open: prefix offsets `offset[s] = Σ counts[0..s)`.
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
    /// The scheduled event start, epoch seconds; absent when no start time is
    /// set. Written by the admin, which arms the `EventBridge` schedule in the
    /// same action: the schedule is what fires the open, this is what the
    /// waiting page counts down to.
    ///
    /// Absent rather than `0`, unlike `fail_open_until`: there, a zero and a
    /// past deadline both mean "not in force" and callers treat them
    /// identically. Here they do not — a visitor arriving before an unscheduled
    /// event must be told the event is not open, while one arriving after a
    /// scheduled start has passed must be told it is opening, so collapsing the
    /// two would erase a distinction the read path needs.
    pub starts_at: Option<u64>,
}

/// The fixed pre-queue index space and permutation seed, once the event has
/// opened and written them.
///
/// No `Debug`/`PartialEq`: [`Seed`] carries the permutation key and
/// deliberately implements neither, so it cannot end up in a log line or a
/// careless equality check at a call site.
#[derive(Clone, Copy)]
pub struct OpenOutputs {
    pub offsets: CohortOffsets,
    pub seed: Seed,
}

impl Counters {
    /// `Some` once the open has written the seed, cohort size, and offsets;
    /// `None` before.
    ///
    /// Callers branch on this rather than on [`Phase`]: an operator can walk
    /// the phase back through `Maintenance` to `PreQueue` after an open
    /// (recovering from an operator error) without undoing the index
    /// space, so `phase == PreQueue` alone does not mean unopened.
    #[must_use]
    pub fn open_outputs(&self) -> Option<OpenOutputs> {
        let seed_bytes = self.shuffle_seed?;
        let offsets = self.prequeue_offsets?;
        let participant_count = self.participant_count?;
        Some(OpenOutputs {
            offsets: CohortOffsets::from_parts(offsets, participant_count),
            seed: Seed(seed_bytes),
        })
    }

    /// Resolves a pre-queue registration to its queue position.
    ///
    /// Reconstructs the global index `i = offset[s] + l` from the fixed offsets
    /// and the row, then derives the position with the permutation. A row whose
    /// local index is at or past its shard's own issued count raced the open
    /// and belongs to the live-join sequence instead, so the permutation is
    /// never evaluated outside its domain.
    ///
    /// # Errors
    ///
    /// [`ResolveError::NotOpen`] before the open has written the seed, cohort
    /// size, and offsets; [`ResolveError::BadShard`] if the row's shard is
    /// outside `0..SHARDS`.
    pub fn resolve_prequeue(&self, row: &PreQueueItem) -> Result<ResolvedPosition, ResolveError> {
        let OpenOutputs { offsets, seed } = self.open_outputs().ok_or(ResolveError::NotOpen)?;

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
                // A present element that is not an `N` parsing as `u64` is
                // corruption, not an absent offset — fail the whole parse so
                // the item reads back as `None`. `filter_map` would instead
                // drop the bad entry and let an over-length list collapse to a
                // `SHARDS`-length array whose tail is shifted in from beyond
                // the prefix, a wrong-but-plausible-looking result rather than
                // the `None` callers fall back to. This matches the
                // corruption-discipline of `shard_count_of` / `shard_index_of`
                // above: a present-but-unreadable value surfaces as `None`.
                let parsed: Result<Vec<u64>, ()> = list
                    .iter()
                    .map(|e| e.as_n().ok().and_then(|s| s.parse().ok()).ok_or(()))
                    .collect();
                parsed.ok().and_then(|p| <[u64; SHARDS]>::try_from(p).ok())
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
            stored_control: stored_control_of(item),
            fail_open_until: num("fail_open_until").unwrap_or(0),
            starts_at: num(STARTS_AT_ATTR),
        }
    }
}

/// Where a pre-queue registration resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedPosition {
    /// A position inside the cohort's `[0, N)`, derived from the seed.
    PreQueue(u64),
    /// The registration raced the open; it has no pre-queue position and is
    /// instead a live joiner, whose actual position lives in `Positions`.
    LiveJoin,
}

/// Why a pre-queue registration cannot be resolved to a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The event has not opened yet, so no pre-queue position exists.
    #[error("event not yet open")]
    NotOpen,
    /// The stored shard is outside `0..SHARDS` (a corrupt row).
    #[error("shard index out of range")]
    BadShard,
}

/// Reads the operator's admission control off a `Counters` item.
///
/// Absent or empty means an event nobody has paused, which is normal
/// admission. A value that is *present* and unreadable is a different thing:
/// the only writer is the operator's pause, so a stored string this cannot
/// parse is a pause that did not land cleanly, and reading it as
/// [`StoredControl::Open`] resumes admission during the incident someone was
/// trying to stop. Holding is both the safe direction and the visible one — a
/// queue that stops moving gets noticed; an un-pause does not.
///
/// A legacy `"fail_open"` string from before issue #71 lands in the same place
/// and holds rather than reopening, which is correct: the epoch is the sole
/// authority for fail-open and a stale string carries no window. An operator
/// with a live window still gets it, because [`crate::resolve`] checks the
/// epoch before the stored value.
///
/// Shared rather than duplicated per reader: the controller is the component
/// that acts on this, and two copies of the rule are two chances for the
/// component that releases people to disagree with the one that displays the
/// state.
#[must_use]
pub fn stored_control_of<S: std::hash::BuildHasher>(
    item: &HashMap<String, AttributeValue, S>,
) -> StoredControl {
    item.get("admission_control")
        .and_then(|v| v.as_s().ok())
        .filter(|s| !s.is_empty())
        .map_or(StoredControl::Open, |stored| {
            stored.parse().unwrap_or(StoredControl::Paused)
        })
}

/// Reads a shard item's own index, or `None` if it is missing or out of range.
///
/// Readers fetch shards in a batch and get them back in arbitrary order, so
/// each item says which shard it is rather than having its key taken apart.
#[must_use]
pub fn shard_index_of<S: std::hash::BuildHasher>(
    item: &HashMap<String, AttributeValue, S>,
) -> Option<usize> {
    let shard = item
        .get(crate::expr::SHARD_INDEX_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<usize>().ok())?;
    (shard < SHARDS).then_some(shard)
}

/// Reads a shard item's count, or `None` if the count attribute is absent,
/// not a `Number`, or not a parseable `u64`.
///
/// `None` distinguishes "this item has no readable count" from "this shard has
/// no item": a shard that nothing has written yields no item at all from a
/// `BatchGetItem` response, so it never reaches a fold that could call this. A
/// present item whose count cannot be read is therefore corruption, not an
/// empty shard — callers that fold batch-get items must turn `None` into an
/// error rather than silently zero the shard, reserving zero for a shard that
/// never reached them because it had no item.
#[must_use]
pub fn shard_count_of<S: std::hash::BuildHasher>(
    item: &HashMap<String, AttributeValue, S>,
) -> Option<u64> {
    item.get(crate::expr::SHARD_COUNT_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn a_shard_item_reports_its_own_index() {
        // Readers fetch shards in a batch and get them back in arbitrary order,
        // so each item says which shard it is rather than having its key taken
        // apart to find out.
        let item = HashMap::from([
            (
                crate::expr::SHARD_INDEX_ATTR.to_owned(),
                AttributeValue::N("7".to_owned()),
            ),
            (
                crate::expr::SHARD_COUNT_ATTR.to_owned(),
                AttributeValue::N("42".to_owned()),
            ),
        ]);
        assert_eq!(shard_index_of(&item), Some(7));
        assert_eq!(shard_count_of(&item), Some(42));

        // Out of range or absent is None, never a wrong shard.
        let bad = HashMap::from([(
            crate::expr::SHARD_INDEX_ATTR.to_owned(),
            AttributeValue::N(SHARDS.to_string()),
        )]);
        assert_eq!(shard_index_of(&bad), None);
        assert_eq!(shard_index_of(&HashMap::new()), None);
        // An unreadable or absent count is None; callers reserve zero for an
        // absent *item*, since a present item reaching a fold is never empty —
        // a missing count on one is corruption, not a still-unwritten shard.
        assert_eq!(shard_count_of(&HashMap::new()), None);
        // A non-numeric count is corruption, not zero.
        let corrupt = HashMap::from([(
            crate::expr::SHARD_COUNT_ATTR.to_owned(),
            AttributeValue::S("not a number".to_owned()),
        )]);
        assert_eq!(shard_count_of(&corrupt), None);
    }

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

    fn unopened_counters(phase: Phase) -> Counters {
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

    #[test]
    fn open_outputs_is_none_until_every_output_is_present() {
        assert!(unopened_counters(Phase::PreQueue).open_outputs().is_none());

        // Missing just the seed, or just the offsets, or just the count: still
        // not opened. Every field must be present.
        let mut partial = unopened_counters(Phase::Active);
        partial.shuffle_seed = Some([1u8; 32]);
        assert!(partial.open_outputs().is_none());
        partial.participant_count = Some(10);
        assert!(partial.open_outputs().is_none());
    }

    #[test]
    fn open_outputs_is_some_once_every_output_is_present_regardless_of_phase() {
        // The operator can walk the phase back to PreQueue after an open
        // (Active -> Maintenance -> Idle -> PreQueue) without undoing the
        // index space, so `opened()` must not gate on phase.
        let mut counters = unopened_counters(Phase::PreQueue);
        counters.shuffle_seed = Some([9u8; 32]);
        counters.participant_count = Some(3);
        counters.prequeue_offsets = Some([0, 0, 0, 1, 1, 1, 2, 2, 2, 2]);
        let opened = counters.open_outputs().unwrap();
        assert_eq!(opened.offsets.participant_count(), 3);
    }

    fn opened_counters(counts: [u64; SHARDS]) -> Counters {
        let opened = CohortOffsets::from_counts(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = opened.offset(s);
        }
        let mut counters = unopened_counters(Phase::Active);
        counters.shuffle_seed = Some([3u8; 32]);
        counters.participant_count = Some(opened.participant_count());
        counters.prequeue_offsets = Some(offsets);
        counters.queue_counter = opened.participant_count();
        counters
    }

    fn prequeue_row(s: u8, l: u64) -> PreQueueItem {
        PreQueueItem {
            r: format!("r-{s}-{l}"),
            s,
            l,
            t: 0,
        }
    }

    fn position_of(counters: &Counters, row: &PreQueueItem) -> u64 {
        match counters.resolve_prequeue(row).unwrap() {
            ResolvedPosition::PreQueue(p) => Some(p),
            ResolvedPosition::LiveJoin => None,
        }
        .unwrap()
    }

    #[test]
    fn a_cohort_resolves_to_distinct_positions_inside_its_own_range() {
        // A duplicate position is a visible fairness failure, so the property
        // is asserted over a whole assembled cohort rather than left to the
        // permutation's own bijectivity tests: this covers shard assembly too.
        let counters = opened_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0]);
        let n = 20;
        let mut positions = std::collections::HashSet::new();
        for s in 0..2u8 {
            for l in 0..10u64 {
                let p = position_of(&counters, &prequeue_row(s, l));
                assert!(p < n, "position {p} outside the cohort's [0, {n})");
                assert!(positions.insert(p), "duplicate position {p}");
            }
        }
        assert_eq!(positions.len(), usize::try_from(n).unwrap());
    }

    #[test]
    fn a_straggler_past_its_shards_issued_count_is_a_live_join() {
        // A row past its shard's issued count was never in the cohort the open
        // fixed, so it has no pre-queue position and falls through to a
        // `Positions` lookup.
        let counters = opened_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            counters.resolve_prequeue(&prequeue_row(0, 10)).unwrap(),
            ResolvedPosition::LiveJoin
        );
    }

    #[test]
    fn an_unreadable_admission_control_holds_rather_than_resumes() {
        // The only writer of this attribute is the operator's pause, so a
        // value that will not parse is a pause that did not land cleanly.
        // Reading it as Open would resume admission during the incident
        // someone was trying to stop -- silently, because nothing else
        // changes. Holding is both the safe direction and the visible one.
        for stored in ["fail_open", "PAUSED", "paused ", "\u{1}", "0"] {
            let item = HashMap::from([(
                "admission_control".to_owned(),
                aws_sdk_dynamodb::types::AttributeValue::S(stored.to_owned()),
            )]);
            assert_eq!(
                Counters::from_item("evt-1", &item).stored_control,
                StoredControl::Paused,
                "{stored:?} resumed admission"
            );
        }
    }

    #[test]
    fn an_absent_admission_control_is_still_normal_admission() {
        // Absent is not corrupt: it is an event nobody has paused. Holding
        // here would stall every event that never touched the control.
        assert_eq!(
            Counters::from_item("evt-1", &HashMap::new()).stored_control,
            StoredControl::Open
        );
        let empty = HashMap::from([(
            "admission_control".to_owned(),
            aws_sdk_dynamodb::types::AttributeValue::S(String::new()),
        )]);
        assert_eq!(
            Counters::from_item("evt-1", &empty).stored_control,
            StoredControl::Open
        );
    }

    #[test]
    fn a_live_fail_open_window_outranks_an_unreadable_stored_control() {
        // resolve() checks the epoch first, so an operator who engaged
        // fail-open still gets it even if the stored string is garbage. The
        // hold must not strand a deliberate break-glass.
        let item = HashMap::from([(
            "admission_control".to_owned(),
            aws_sdk_dynamodb::types::AttributeValue::S("nonsense".to_owned()),
        )]);
        let counters = Counters::from_item("evt-1", &item);
        assert_eq!(counters.stored_control, StoredControl::Paused);
        assert_eq!(
            crate::resolve(counters.stored_control, 2_000, 1_000),
            crate::AdmissionControl::FailOpen
        );
    }

    #[test]
    fn from_item_defaults_a_fresh_event_to_idle_and_open() {
        // An item with nothing set must not read as an opened, admitting event.
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
        // Absent, not zero: the waiting page tells "no start time set" from "a
        // start time that has passed", and a defaulted 0 would read as the
        // latter for every event that was never scheduled.
        assert!(counters.starts_at.is_none());
    }

    #[test]
    fn from_item_reads_the_scheduled_start() {
        let mut item = HashMap::new();
        item.insert(
            STARTS_AT_ATTR.to_owned(),
            AttributeValue::N("1800000000".to_owned()),
        );
        assert_eq!(
            Counters::from_item("evt-1", &item).starts_at,
            Some(1_800_000_000)
        );
    }

    #[test]
    fn from_item_treats_an_unparsable_start_as_unscheduled() {
        // A clear removes the attribute, but a hand-edited or half-migrated row
        // must not resolve to an epoch nobody wrote.
        for bad in [
            AttributeValue::N("not-a-number".to_owned()),
            AttributeValue::S("1800000000".to_owned()),
            AttributeValue::N("-1".to_owned()),
        ] {
            let mut item = HashMap::new();
            item.insert(STARTS_AT_ATTR.to_owned(), bad);
            assert!(Counters::from_item("evt-1", &item).starts_at.is_none());
        }
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
    fn from_item_round_trips_the_open_outputs() {
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
        assert!(counters.open_outputs().is_some());
    }

    #[test]
    fn prequeue_offsets_shifts_on_unreadable_entries_are_surfaced_as_none() {
        // A list longer than `SHARDS` with unparseable `Null`s: under the old
        // `filter_map` parser the Nulls were dropped, exactly `SHARDS` numbers
        // survived, and `try_from` succeeded — but the tail (800, 900) was
        // drawn from list indices 10/11, beyond the `SHARDS`-length prefix,
        // filling the gap the dropped Nulls left. That shifted-but-plausible
        // array was served as `Some` instead of surfaced as a corrupt item, so
        // `open_outputs()` admitted visitors against a wrong index space. The
        // parser now fails the whole list on the first unreadable entry, so a
        // corrupt `prequeue_offsets` reads back as `None` — matching the
        // corruption-discipline of `shard_count_of` / `shard_index_of`, which
        // treat a present-but-unreadable value as `None` rather than salvage.
        let stored: Vec<AttributeValue> = vec![
            AttributeValue::N("0".to_owned()),
            AttributeValue::N("100".to_owned()),
            AttributeValue::N("200".to_owned()),
            AttributeValue::N("300".to_owned()),
            AttributeValue::Null(true),
            AttributeValue::N("400".to_owned()),
            AttributeValue::N("500".to_owned()),
            AttributeValue::N("600".to_owned()),
            AttributeValue::Null(true),
            AttributeValue::N("700".to_owned()),
            AttributeValue::N("800".to_owned()),
            AttributeValue::N("900".to_owned()),
        ];
        let mut item = HashMap::new();
        item.insert("prequeue_offsets".to_owned(), AttributeValue::L(stored));
        let counters = Counters::from_item("evt-1", &item);
        assert!(
            counters.prequeue_offsets.is_none(),
            "corrupt prequeue_offsets must surface as None, not a shifted array"
        );
        assert!(
            counters.open_outputs().is_none(),
            "open_outputs must be None when prequeue_offsets is corrupt"
        );
    }
}
