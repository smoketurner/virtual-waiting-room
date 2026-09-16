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

use crate::demotion::{DemotionRef, DemotionSet};
use crate::expr::{DEMOTED_COUNT_ATTR, DEMOTION_CHUNKS_ATTR, DEMOTION_NONCE_ATTR, STARTS_AT_ATTR};
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

/// Join-time signals `assign_position` lifts out of `CloudFront`'s viewer
/// headers, carried as SQS message attributes rather than in the request body
/// (issue #59). Reported, not attested: the regional API Gateway endpoint has
/// no resource policy restricting it to `CloudFront`, so a caller that bypasses
/// the edge can set these values itself. Nothing reads them yet — they exist so
/// a farm is analysable after the fact and so a future mitigation has
/// something to act on.
///
/// Single-letter field names for the same reason the striped counters use
/// them: this rides inside `v` on every `PreQueue` and `Positions` row, and
/// `DynamoDB` bills attribute names on every write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Telemetry {
    /// `CloudFront-Viewer-Address`, verbatim (port included).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<String>,
    /// `CloudFront-Viewer-ASN`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<String>,
    /// `CloudFront-Viewer-Country`, ISO-3166-1 alpha-2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
    /// `CloudFront-Viewer-JA4-Fingerprint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub j: Option<String>,
    /// `User-Agent`, truncated to 256 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub u: Option<String>,
    /// The API Gateway request id, for correlating a row back to access logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
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
    /// Join-time telemetry, absent on a row written before issue #59 or on an
    /// open (untelemetered) deployment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub v: Option<Telemetry>,
}

/// A `Positions` row, written lazily when a visitor is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionItem {
    pub request_id: String,
    /// The queue position this request holds. For a live joiner it is the value
    /// claimed from `queue_counter`; a pre-queue member's position is derived
    /// from the seed on read and never written here.
    pub queue_position: u64,
    /// Server-stamped arrival time in epoch seconds. Authoritative: a
    /// client-supplied request id may also carry a timestamp (a `UUIDv7`
    /// shape), but that one is untrusted and, since issue #59, not even
    /// guaranteed to be present.
    pub entry_time: u64,
    pub status: PositionStatus,
    /// Post-event storage reclamation only, never the expiry mechanism. A
    /// position is expired by the controller when the admission cursor has
    /// passed it and it was not claimed; there is deliberately no per-row
    /// deadline, because a deadline set when the position is issued expires
    /// people for waiting the length of the queue they are waiting in.
    pub ttl: u64,
    /// Join-time telemetry, absent on a row written before issue #59 or on an
    /// open (untelemetered) deployment.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub v: Option<Telemetry>,
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
    /// Set at the seal when demotion is enforced (issue #145): `D`, how many
    /// cohort rows the stored demotion set matches. They resolve into the
    /// tail `[N, 2N)`, a second copy of the index space behind the whole
    /// cohort, and `queue_counter` starts at `2N` to keep live joiners behind
    /// it. `0` when nothing was demoted.
    pub demoted_count: u64,
    /// Where the demotion set the seal wrote lives, when `demoted_count > 0`:
    /// the nonce its chunk items are keyed under and how many there are. A
    /// resolver loads it once per execution environment and matches every
    /// pre-queue row against it.
    pub demotion: Option<DemotionRef>,
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
    /// same action: the schedule is what fires the seal, this is what the
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
    /// The tail's size `D` (issue #145); `0` when nothing was demoted.
    pub demoted: u64,
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
            demoted: self.demoted_count,
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
    /// A row the seal demoted (issue #145) — one the stored [`DemotionSet`]
    /// matches — resolves to `N + PRP(seed, i, N)` instead: the same slot in a
    /// second copy of the index space behind the whole cohort, so every
    /// position stays unique (primary slots are `< N`, tail slots are in
    /// `[N, 2N)`) and the live-join sequence, started at `2N`, stays behind
    /// both. Nothing is written per row; the tail is sparse, and the
    /// controller walks it at its known density.
    ///
    /// `demotion` is the set the event item names. It may be `None` when the
    /// seal demoted nobody, or for a row that raced the seal: such a row
    /// resolves to [`ResolvedPosition::LiveJoin`] without consulting the set —
    /// the permutation is never evaluated for it, so there is no primary slot
    /// to answer from and no demoted row to un-demote. A cohort row on a sealed
    /// event with `demoted_count > 0` and no set to match against cannot be
    /// resolved, because answering from the primary slot alone would silently
    /// un-demote every demoted row.
    ///
    /// # Errors
    ///
    /// [`ResolveError::NotSealed`] before the seal has written the seed, cohort
    /// size, and offsets; [`ResolveError::BadShard`] if the row's shard is
    /// outside `0..SHARDS`; [`ResolveError::DemotionUnavailable`] when a cohort
    /// row's event demoted rows and the caller could not supply the set — a
    /// straggler never yields this error.
    pub fn resolve_prequeue(
        &self,
        row: &PreQueueItem,
        demotion: Option<&DemotionSet>,
    ) -> Result<ResolvedPosition, ResolveError> {
        let Sealed {
            offsets,
            seed,
            demoted,
        } = self.sealed().ok_or(ResolveError::NotSealed)?;

        let shard = usize::from(row.s);
        if shard >= SHARDS {
            return Err(ResolveError::BadShard);
        }
        let participant_count = offsets.participant_count();
        Ok(match offsets.assign(shard, row.l) {
            Assignment::PreQueue { index } => {
                let demotion = match (demoted, demotion) {
                    (0, _) => None,
                    (_, Some(set)) => Some(set),
                    (_, None) => return Err(ResolveError::DemotionUnavailable),
                };
                let primary = crate::permutation::prp(&seed, index, participant_count);
                let position = match demotion {
                    Some(set) if set.matches(row.v.as_ref()) => {
                        // `2N` was checked to fit at the seal; a saturated
                        // add here cannot happen for any cohort that sealed.
                        participant_count.saturating_add(primary)
                    }
                    Some(_) | None => primary,
                };
                ResolvedPosition::PreQueue(position)
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
            demoted_count: num(DEMOTED_COUNT_ATTR).unwrap_or(0),
            demotion: item
                .get(DEMOTION_NONCE_ATTR)
                .and_then(|v| v.as_s().ok())
                .filter(|nonce| !nonce.is_empty())
                .and_then(|nonce| {
                    let chunks = num(DEMOTION_CHUNKS_ATTR)
                        .and_then(|n| u32::try_from(n).ok())
                        .filter(|n| *n > 0)?;
                    Some(DemotionRef {
                        nonce: nonce.clone(),
                        chunks,
                    })
                }),
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
            starts_at: num(STARTS_AT_ATTR),
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
    /// The seal demoted rows but the caller had no demotion set to match
    /// against, so no position can be answered without risking un-demoting.
    #[error("demotion set unavailable")]
    DemotionUnavailable,
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
            v: None,
        };
        let av: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            serde_dynamo::to_item(&item).unwrap();
        let back: PreQueueItem = serde_dynamo::from_item(av).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn prequeue_item_round_trips_with_telemetry() {
        let item = PreQueueItem {
            r: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            s: 7,
            l: 42,
            t: 1_788_000_000,
            v: Some(Telemetry {
                a: Some("203.0.113.1:443".to_owned()),
                n: Some("64500".to_owned()),
                c: Some("US".to_owned()),
                j: Some("t13d1516h2_8daaf6152771_02713d6af862".to_owned()),
                u: Some("Mozilla/5.0".to_owned()),
                q: Some("req-abc-123".to_owned()),
            }),
        };
        let av: std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            serde_dynamo::to_item(&item).unwrap();
        let back: PreQueueItem = serde_dynamo::from_item(av).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn a_row_written_before_telemetry_existed_still_deserializes() {
        // A PreQueue row with no `v` attribute at all (written before issue
        // #59, or by an untelemetered join) must still deserialize, with `v`
        // defaulting to `None` rather than failing to parse.
        let av = std::collections::HashMap::from([
            (
                "r".to_owned(),
                aws_sdk_dynamodb::types::AttributeValue::S("req-1".to_owned()),
            ),
            (
                "s".to_owned(),
                aws_sdk_dynamodb::types::AttributeValue::N("3".to_owned()),
            ),
            (
                "l".to_owned(),
                aws_sdk_dynamodb::types::AttributeValue::N("1".to_owned()),
            ),
            (
                "t".to_owned(),
                aws_sdk_dynamodb::types::AttributeValue::N("1788000000".to_owned()),
            ),
        ]);
        let item: PreQueueItem = serde_dynamo::from_item(av).unwrap();
        assert!(item.v.is_none());
    }

    #[test]
    fn position_item_round_trips_and_status_is_snake_case() {
        let item = PositionItem {
            request_id: "req-1".to_owned(),
            queue_position: 4_242,
            entry_time: 1_788_000_000,
            status: PositionStatus::Issued,
            ttl: 1_900_000_000,
            v: None,
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
            v: None,
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
            demoted_count: 0,
            demotion: None,
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            starts_at: None,
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

    fn sealed_counters(counts: [u64; SHARDS], demoted: u64) -> Counters {
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        let mut counters = unsealed_counters(Phase::Active);
        counters.shuffle_seed = Some([3u8; 32]);
        counters.participant_count = Some(sealed.participant_count());
        counters.prequeue_offsets = Some(offsets);
        counters.demoted_count = demoted;
        counters.queue_counter = if demoted > 0 {
            sealed.participant_count().saturating_mul(2)
        } else {
            sealed.participant_count()
        };
        counters
    }

    fn prequeue_row(s: u8, l: u64, asn: Option<&str>) -> PreQueueItem {
        PreQueueItem {
            r: format!("r-{s}-{l}"),
            s,
            l,
            t: 0,
            v: asn.map(|asn| Telemetry {
                a: None,
                n: Some(asn.to_owned()),
                c: None,
                j: None,
                u: None,
                q: None,
            }),
        }
    }

    fn farm_set() -> DemotionSet {
        DemotionSet::from_groups(&[crate::demotion::DemotedGroup {
            signal: crate::demotion::Signal::Asn,
            value: "64500".to_owned(),
            count: 5,
            max: 2,
        }])
    }

    fn position_of(counters: &Counters, row: &PreQueueItem, set: Option<&DemotionSet>) -> u64 {
        match counters.resolve_prequeue(row, set).unwrap() {
            ResolvedPosition::PreQueue(p) => Some(p),
            ResolvedPosition::LiveJoin => None,
        }
        .unwrap()
    }

    #[test]
    fn a_row_the_set_matches_resolves_into_the_tail_behind_the_whole_cohort() {
        // Cohort of 20 across two shards; every fourth row reports the farm's
        // ASN. Matched rows must land in [N, 2N) at their own slot, unmatched
        // rows in [0, N), and no two rows may share a position — a duplicate
        // position is a visible fairness failure.
        let counters = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 5);
        let set = farm_set();
        let n = 20;
        let mut positions = std::collections::HashSet::new();
        for s in 0..2u8 {
            for l in 0..10u64 {
                let farmed = l % 4 == 0;
                let asn = if farmed { Some("64500") } else { Some("7922") };
                let row = prequeue_row(s, l, asn);
                let p = position_of(&counters, &row, Some(&set));
                let primary = position_of(&counters, &prequeue_row(s, l, None), Some(&set));
                if farmed {
                    assert!((n..2 * n).contains(&p), "demoted row landed at {p}");
                    // The same slot, one copy of the index space later.
                    assert_eq!(p, n + primary);
                } else {
                    assert!(p < n, "primary row landed at {p}");
                }
                assert!(positions.insert(p), "duplicate position {p}");
            }
        }
    }

    #[test]
    fn an_untelemetered_row_is_never_demoted() {
        let counters = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 5);
        let p = position_of(&counters, &prequeue_row(0, 3, None), Some(&farm_set()));
        assert!(p < 20);
    }

    #[test]
    fn a_demoting_event_refuses_to_resolve_without_its_set() {
        // Answering from the primary slot alone would silently un-demote
        // every demoted row, so no answer is the only safe answer.
        let counters = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 5);
        assert_eq!(
            counters.resolve_prequeue(&prequeue_row(0, 3, Some("64500")), None),
            Err(ResolveError::DemotionUnavailable)
        );
        // An event that demoted nobody needs no set, and ignores one.
        let plain = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 0);
        let without = position_of(&plain, &prequeue_row(0, 3, Some("64500")), None);
        let with = position_of(
            &plain,
            &prequeue_row(0, 3, Some("64500")),
            Some(&farm_set()),
        );
        assert_eq!(without, with);
        assert!(without < 20);
    }

    #[test]
    fn a_straggler_is_a_live_join_even_when_the_set_matches_it() {
        // The straggler rule runs first: a row past its shard's issued count
        // was never in the cohort the seal classified.
        let counters = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 5);
        assert_eq!(
            counters
                .resolve_prequeue(&prequeue_row(0, 10, Some("64500")), Some(&farm_set()))
                .unwrap(),
            ResolvedPosition::LiveJoin
        );
    }

    #[test]
    fn a_straggler_is_a_live_join_even_when_the_set_is_unavailable() {
        // The straggler rule runs before the demotion-availability gate: a row
        // past its shard's issued count was never in the cohort the seal
        // classified, so it has no primary slot to un-demote from and falls
        // through to a `Positions` lookup even when the set could not be loaded
        // (cold-start / transient-failure window, where `demoted_count > 0`
        // and the set is `None`). The symmetric cohort row still refuses.
        let counters = sealed_counters([10, 10, 0, 0, 0, 0, 0, 0, 0, 0], 5);
        assert_eq!(
            counters.resolve_prequeue(&prequeue_row(0, 10, Some("64500")), None),
            Ok(ResolvedPosition::LiveJoin)
        );
        assert_eq!(
            counters.resolve_prequeue(&prequeue_row(0, 3, Some("64500")), None),
            Err(ResolveError::DemotionUnavailable)
        );
    }

    #[test]
    fn from_item_reads_the_demotion_ref_only_when_whole() {
        let mut item = HashMap::new();
        item.insert(
            DEMOTION_NONCE_ATTR.to_owned(),
            AttributeValue::S("0badcafe".to_owned()),
        );
        // Nonce without a chunk count is not a usable reference.
        assert!(Counters::from_item("evt-1", &item).demotion.is_none());
        item.insert(
            DEMOTION_CHUNKS_ATTR.to_owned(),
            AttributeValue::N("3".to_owned()),
        );
        assert_eq!(
            Counters::from_item("evt-1", &item).demotion,
            Some(DemotionRef {
                nonce: "0badcafe".to_owned(),
                chunks: 3
            })
        );
    }

    #[test]
    fn from_item_reads_the_demoted_count_and_defaults_it_to_zero() {
        assert_eq!(
            Counters::from_item("evt-1", &HashMap::new()).demoted_count,
            0
        );
        let mut item = HashMap::new();
        item.insert(
            DEMOTED_COUNT_ATTR.to_owned(),
            AttributeValue::N("42".to_owned()),
        );
        assert_eq!(Counters::from_item("evt-1", &item).demoted_count, 42);
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
