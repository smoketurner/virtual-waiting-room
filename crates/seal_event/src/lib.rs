//! Seal logic for the `seal_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence — so a retry or a double-fire seals exactly
//! once.
//!
//! The seal is also where `queue_counter` starts at the cohort size, so live
//! joiners are numbered behind the whole pre-queue cohort instead of colliding
//! with `[0, N)`. That clause is in the same atomic update as the rest of the
//! seal write: a separate write could be lost between the seal and the first
//! live join.
//!
//! # Demotion (issue #145)
//!
//! When the operator has set demotion rules, the seal also reads the cohort's
//! join-time telemetry and demotes whole groups that exceed a threshold to a
//! compact tail (`wr_common::demotion`). The order of operations is what keeps
//! the seal's guarantees intact:
//!
//! 1. Read the shard counts, so the cohort is fixed.
//! 2. Scan the pre-queue and classify it. Nothing is written yet, so a
//!    double-fire at this point costs a duplicate scan and nothing else.
//! 3. The one conditional seal write, now also carrying `D` and starting the
//!    live-join sequence at `N + D`. This is the election: exactly one run
//!    wins it.
//! 4. The winner alone writes the report and a tail index `d` on every demoted
//!    row. It holds the phase at `pre_queue` while it does, so the waiting
//!    page — which never asks for a position during the pre-queue — cannot
//!    observe a demoted row at its primary slot before its tail index lands.
//!    A row that never receives its `d` (the winner died part way) keeps its
//!    primary slot, which nobody else holds, so partial application can
//!    misplace a registration but never duplicate a position.
//! 5. The winner flips the phase to `active` and records how many tail
//!    indices it wrote.
//!
//! No rules means no scan at all: the seal is then exactly the single write it
//! was before this existed.

use std::future::Future;
use std::sync::{Arc, Mutex};

use wr_common::{
    Classification, Cohort, DemotionMode, DemotionReport, DemotionRules, Phase, RuleParseError,
    SHARDS, SealError, SealedOffsets, Telemetry,
};

pub mod dynamo;

/// The values written by a seal: the seed, cohort size, prefix offsets, the
/// tail size, and the phase the seal leaves the event in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealValues {
    pub seed: [u8; 32],
    pub participant_count: u64,
    pub offsets: [u64; SHARDS],
    /// `D`: the tail's size, `0` unless demotion is enforced and caught
    /// something. `queue_counter` starts at `participant_count + demoted_count`.
    pub demoted_count: u64,
    /// `Active`, or `PreQueue` when tail indices are still to be written and
    /// the winner will flip it once they are.
    pub phase: Phase,
}

impl SealValues {
    /// Where the live-join sequence starts: behind the cohort and its tail.
    ///
    /// # Errors
    ///
    /// [`SealError::Overflow`] if `N + D` exceeds `u64`.
    pub fn queue_counter_start(&self) -> Result<u64, SealError> {
        self.participant_count
            .checked_add(self.demoted_count)
            .ok_or(SealError::Overflow)
    }
}

/// The demotion configuration the seal runs with: the operator's rules and
/// whether to act on them. `rules_text` is kept so a report can quote what
/// was configured even when it did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemotionConfig {
    pub rules: Result<DemotionRules, RuleParseError>,
    pub rules_text: String,
    pub mode: DemotionMode,
}

impl DemotionConfig {
    /// No rules: the seal never scans.
    #[must_use]
    pub fn off() -> Self {
        Self {
            rules: Ok(DemotionRules::default()),
            rules_text: String::new(),
            mode: DemotionMode::Observe,
        }
    }

    /// Parses the two environment values. A rules string that does not parse
    /// is carried as the error rather than failing here, so the seal still
    /// runs — an event that never opens is worse than one that opened without
    /// a control the operator can see, in the report, did not apply.
    #[must_use]
    pub fn parse(rules_text: &str, mode: DemotionMode) -> Self {
        Self {
            rules: DemotionRules::parse(rules_text),
            rules_text: rules_text.to_owned(),
            mode,
        }
    }
}

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("seal store error: {0}")]
pub struct StoreError(pub String);

/// The result of attempting a seal.
#[derive(Debug, PartialEq, Eq)]
pub enum SealResult {
    /// This call performed the seal and wrote the values.
    Sealed(Box<SealValues>),
    /// The event was already sealed (the guard rejected the write); nothing
    /// changed. A double-fire or retry lands here.
    AlreadySealed,
}

/// One pre-queue row as the seal's scan sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRow {
    pub request_id: String,
    pub shard: u8,
    pub local_index: u64,
    pub telemetry: Option<Telemetry>,
}

/// The persistence port the seal drives.
pub trait Store {
    /// Reads the 10 pre-queue shard counts for the event.
    fn read_shard_counts(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send;

    /// Streams every pre-queue row through `visit`, in any order, possibly
    /// from several segments at once. Returns the number of rows scanned.
    fn scan_prequeue(
        &self,
        visit: Arc<dyn Fn(ScannedRow) + Send + Sync>,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Writes the seal values, starts `queue_counter` at `N + D`, and sets the
    /// phase, guarded by `attribute_not_exists(shuffle_seed)`. Returns `false`
    /// if the guard rejected the write (already sealed).
    fn write_seal(
        &self,
        event_id: &str,
        values: &SealValues,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Writes the demotion report item.
    fn write_report(
        &self,
        event_id: &str,
        report: &DemotionReport,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Writes tail index `k` on the row at index `k` of `request_ids`, each
    /// guarded by `attribute_not_exists(d)`. Returns how many rows took one.
    fn write_tail_indices(
        &self,
        request_ids: &[String],
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Flips a held `pre_queue` phase to `active` and records `applied`,
    /// guarded on the phase still being `pre_queue`.
    fn finish_demotion(
        &self,
        event_id: &str,
        applied: u64,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Folds the shard counts into the offsets and cohort size and pairs them with
/// a freshly generated seed. The tail is empty and the phase `Active`; a
/// demoting seal overrides both.
///
/// # Errors
///
/// Returns [`SealError::Overflow`] if the summed cohort size exceeds `u64`.
pub fn seal_values(counts: [u64; SHARDS], seed: [u8; 32]) -> Result<SealValues, SealError> {
    let sealed: SealedOffsets = SealedOffsets::seal(counts)?;
    let mut offsets = [0u64; SHARDS];
    for (shard, slot) in offsets.iter_mut().enumerate() {
        *slot = sealed.offset(shard);
    }
    Ok(SealValues {
        seed,
        participant_count: sealed.participant_count(),
        offsets,
        demoted_count: 0,
        phase: Phase::Active,
    })
}

/// Reads the shard counts, computes the seal values with the supplied seed,
/// classifies the cohort if rules are set, and writes the seal under the
/// once-only guard. The seed is passed in so the logic is deterministic under
/// test; production generates it from a CSPRNG. `now` stamps the report.
///
/// # Errors
///
/// Returns [`StoreError`] if reading the shard counts, scanning, folding, or
/// writing the seal fails. A failure *after* the seal write (report, tail
/// indices, phase flip) is also returned, but the seal itself has landed by
/// then and a retry finds the event already sealed.
pub async fn seal_event<S: Store>(
    store: &S,
    event_id: &str,
    seed: [u8; 32],
    config: &DemotionConfig,
    now: u64,
) -> Result<SealResult, StoreError> {
    let counts = store.read_shard_counts(event_id).await?;
    let mut values =
        seal_values(counts, seed).map_err(|e| StoreError(format!("fold shard counts: {e}")))?;

    let rules = match &config.rules {
        Ok(rules) => rules,
        Err(error) => {
            // Seal without demotion, and say so where the operator looks.
            tracing::error!(event_id, %error, rules = %config.rules_text, "demotion rules did not parse; sealing without demotion");
            if !store.write_seal(event_id, &values).await? {
                tracing::info!(event_id, "event already sealed; no-op");
                return Ok(SealResult::AlreadySealed);
            }
            let report = DemotionReport::from_error(&config.rules_text, error, now);
            if let Err(e) = store.write_report(event_id, &report).await {
                tracing::error!(event_id, error = %e, "could not write the demotion report");
            }
            return Ok(SealResult::Sealed(Box::new(values)));
        }
    };

    if rules.is_empty() {
        if !store.write_seal(event_id, &values).await? {
            tracing::info!(event_id, "event already sealed; no-op");
            return Ok(SealResult::AlreadySealed);
        }
        tracing::info!(
            event_id,
            participant_count = values.participant_count,
            "event sealed"
        );
        return Ok(SealResult::Sealed(Box::new(values)));
    }

    let classification = classify_cohort(store, &values, rules.clone()).await?;
    let demoted = classification.demoted_count();
    let enforcing = config.mode == DemotionMode::Enforce && demoted > 0;
    if enforcing {
        values.demoted_count = demoted;
        values.phase = Phase::PreQueue;
        // Fail before the election, not after: an overflow here would leave
        // the phase held with nothing to flip it.
        values
            .queue_counter_start()
            .map_err(|e| StoreError(format!("live-join start: {e}")))?;
    }

    if !store.write_seal(event_id, &values).await? {
        tracing::info!(event_id, "event already sealed; no-op");
        return Ok(SealResult::AlreadySealed);
    }
    tracing::info!(
        event_id,
        participant_count = values.participant_count,
        cohort = classification.cohort,
        demoted_groups = classification.groups.len(),
        demoted,
        mode = config.mode.as_wire_str(),
        "event sealed"
    );

    let report = DemotionReport::from_classification(config.mode, rules, &classification, now);
    if let Err(e) = store.write_report(event_id, &report).await {
        // The report is for the operator; the seal has landed and the tail
        // still has to be applied, so this is logged rather than fatal.
        tracing::error!(event_id, error = %e, "could not write the demotion report");
    }

    if enforcing {
        let applied = match store.write_tail_indices(&classification.demoted).await {
            Ok(applied) => applied,
            Err(e) => {
                tracing::error!(event_id, error = %e, "tail indices failed part way; unwritten rows keep their primary slots");
                0
            }
        };
        if applied != demoted {
            tracing::warn!(
                event_id,
                applied,
                demoted,
                "not every demoted row took a tail index"
            );
        }
        store.finish_demotion(event_id, applied).await?;
        tracing::info!(event_id, applied, "demotion applied; event active");
    }

    Ok(SealResult::Sealed(Box::new(values)))
}

/// Scans the pre-queue and classifies every row the sealed offsets place in
/// the cohort. A straggler (local index at or past its shard's issued count)
/// is a live joiner with no pre-queue position and is left out.
async fn classify_cohort<S: Store>(
    store: &S,
    values: &SealValues,
    rules: DemotionRules,
) -> Result<Classification, StoreError> {
    let offsets = SealedOffsets::from_parts(values.offsets, values.participant_count);
    let cohort = Arc::new(Mutex::new(Cohort::new(rules)));
    let sink = Arc::clone(&cohort);
    let visit: Arc<dyn Fn(ScannedRow) + Send + Sync> =
        Arc::new(move |row: ScannedRow| {
            match offsets.assign(usize::from(row.shard), row.local_index) {
                wr_common::Assignment::PreQueue { .. } => {
                    if let Ok(mut cohort) = sink.lock() {
                        cohort.observe(&row.request_id, row.telemetry.as_ref());
                    }
                }
                wr_common::Assignment::LiveJoin => {}
            }
        });
    let scanned = store.scan_prequeue(visit).await?;
    let cohort = Arc::try_unwrap(cohort)
        .map_err(|_arc| StoreError("scan still holds the cohort".to_owned()))?
        .into_inner()
        .map_err(|_poison| StoreError("cohort lock poisoned".to_owned()))?;
    tracing::info!(scanned, cohort = cohort.len(), "pre-queue scanned");
    Ok(cohort.classify())
}

/// The phase a sealed event is in once any demotion has been applied.
#[must_use]
pub fn sealed_phase() -> Phase {
    Phase::Active
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure"
    )]

    use std::sync::Mutex;

    use super::*;

    struct FakeStore {
        counts: [u64; SHARDS],
        already_sealed: bool,
        rows: Vec<ScannedRow>,
        written: Mutex<Option<SealValues>>,
        report: Mutex<Option<DemotionReport>>,
        tails: Mutex<Vec<String>>,
        finished: Mutex<Option<u64>>,
        /// How many tail writes to accept before failing the rest.
        tail_budget: Option<usize>,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_sealed: bool) -> Self {
            Self {
                counts,
                already_sealed,
                rows: Vec::new(),
                written: Mutex::new(None),
                report: Mutex::new(None),
                tails: Mutex::new(Vec::new()),
                finished: Mutex::new(None),
                tail_budget: None,
            }
        }

        fn with_rows(mut self, rows: Vec<ScannedRow>) -> Self {
            self.rows = rows;
            self
        }
    }

    impl Store for FakeStore {
        fn read_shard_counts(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<[u64; SHARDS], StoreError>> + Send {
            std::future::ready(Ok(self.counts))
        }

        fn scan_prequeue(
            &self,
            visit: Arc<dyn Fn(ScannedRow) + Send + Sync>,
        ) -> impl Future<Output = Result<u64, StoreError>> + Send {
            for row in &self.rows {
                visit(row.clone());
            }
            std::future::ready(Ok(self.rows.len() as u64))
        }

        fn write_seal(
            &self,
            _event_id: &str,
            values: &SealValues,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            let wrote = if self.already_sealed {
                false
            } else {
                *self.written.lock().unwrap() = Some(values.clone());
                true
            };
            std::future::ready(Ok(wrote))
        }

        fn write_report(
            &self,
            _event_id: &str,
            report: &DemotionReport,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.report.lock().unwrap() = Some(report.clone());
            std::future::ready(Ok(()))
        }

        fn write_tail_indices(
            &self,
            request_ids: &[String],
        ) -> impl Future<Output = Result<u64, StoreError>> + Send {
            let take = self.tail_budget.unwrap_or(request_ids.len());
            let mut tails = self.tails.lock().unwrap();
            for id in request_ids.iter().take(take) {
                tails.push(id.clone());
            }
            let result = if take < request_ids.len() {
                Err(StoreError("tail write failed".to_owned()))
            } else {
                Ok(request_ids.len() as u64)
            };
            std::future::ready(result)
        }

        fn finish_demotion(
            &self,
            _event_id: &str,
            applied: u64,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            *self.finished.lock().unwrap() = Some(applied);
            std::future::ready(Ok(()))
        }
    }

    fn row(request_id: &str, shard: u8, local_index: u64, address: &str) -> ScannedRow {
        ScannedRow {
            request_id: request_id.to_owned(),
            shard,
            local_index,
            telemetry: Some(Telemetry {
                a: Some(format!("{address}:4433")),
                n: Some("64500".to_owned()),
                c: None,
                j: None,
                u: None,
                q: None,
            }),
        }
    }

    /// Ten rows on shard 0: seven from one address, three from another.
    fn farmed_rows() -> Vec<ScannedRow> {
        let mut rows = Vec::new();
        for l in 0..7 {
            rows.push(row(&format!("farm-{l}"), 0, l, "198.51.100.1"));
        }
        for l in 7..10 {
            rows.push(row(&format!("home-{l}"), 0, l, "203.0.113.5"));
        }
        rows
    }

    fn config(rules: &str, mode: DemotionMode) -> DemotionConfig {
        DemotionConfig::parse(rules, mode)
    }

    #[test]
    fn seal_values_are_prefix_sums_and_total() {
        let values = seal_values([3, 0, 5, 1, 0, 0, 2, 0, 0, 4], [7u8; 32]).unwrap();
        assert_eq!(values.participant_count, 15);
        assert_eq!(values.offsets, [0, 3, 3, 8, 9, 9, 9, 11, 11, 11]);
        assert_eq!(values.seed, [7u8; 32]);
        assert_eq!(values.demoted_count, 0);
        assert_eq!(values.phase, Phase::Active);
        assert_eq!(values.queue_counter_start().unwrap(), 15);
    }

    #[tokio::test]
    async fn first_seal_writes_values() {
        let store = FakeStore::new([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], false);
        let result = seal_event(&store, "evt-1", [9u8; 32], &DemotionConfig::off(), 1)
            .await
            .unwrap();
        match result {
            SealResult::Sealed(values) => assert_eq!(values.participant_count, 10),
            SealResult::AlreadySealed => panic!("expected a first seal"),
        }
        assert!(store.written.lock().unwrap().is_some());
        // No rules: no report, no tail, no phase hold.
        assert!(store.report.lock().unwrap().is_none());
        assert!(store.finished.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn second_seal_is_noop() {
        let store = FakeStore::new([1; SHARDS], true);
        let result = seal_event(&store, "evt-1", [9u8; 32], &DemotionConfig::off(), 1)
            .await
            .unwrap();
        assert_eq!(result, SealResult::AlreadySealed);
        assert!(store.written.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_cohort_seals_to_zero() {
        let store = FakeStore::new([0; SHARDS], false);
        let result = seal_event(&store, "evt-1", [1u8; 32], &DemotionConfig::off(), 1)
            .await
            .unwrap();
        match result {
            SealResult::Sealed(values) => {
                assert_eq!(values.participant_count, 0);
                assert_eq!(values.offsets, [0; SHARDS]);
            }
            SealResult::AlreadySealed => panic!("expected a first seal"),
        }
    }

    #[tokio::test]
    async fn observe_mode_reports_what_it_would_demote_and_demotes_nobody() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:5", DemotionMode::Observe),
            77,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        // The seal itself is untouched by observation.
        assert_eq!(values.demoted_count, 0);
        assert_eq!(values.phase, Phase::Active);
        assert_eq!(values.queue_counter_start().unwrap(), 10);
        assert!(store.tails.lock().unwrap().is_empty());
        assert!(store.finished.lock().unwrap().is_none());
        // But the operator can see exactly what enforcing would have done.
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "observe");
        assert_eq!(report.rules, "address:5");
        assert_eq!(report.cohort, 10);
        assert_eq!(report.demoted, 7);
        assert_eq!(report.groups_total, 1);
        assert_eq!(report.groups[0].value, "198.51.100.1");
        assert_eq!(report.groups[0].count, 7);
        assert_eq!(report.groups[0].max, 5);
        assert_eq!(report.sealed_at, 77);
        assert!(report.error.is_none());
    }

    #[tokio::test]
    async fn enforce_mode_holds_the_phase_writes_the_tail_then_opens() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.demoted_count, 7);
        // The seal write itself holds the phase; live joins start past the tail.
        assert_eq!(values.phase, Phase::PreQueue);
        assert_eq!(values.queue_counter_start().unwrap(), 17);
        assert_eq!(
            store.written.lock().unwrap().clone().unwrap().phase,
            Phase::PreQueue
        );
        // Every farmed row, and only those, took a tail index, in sorted order
        // so `d` is the row's index in this list.
        let tails = store.tails.lock().unwrap().clone();
        assert_eq!(
            tails,
            (0..7).map(|l| format!("farm-{l}")).collect::<Vec<_>>()
        );
        assert_eq!(*store.finished.lock().unwrap(), Some(7));
        assert_eq!(
            store.report.lock().unwrap().clone().unwrap().mode,
            "enforce"
        );
    }

    #[tokio::test]
    async fn enforce_mode_with_nothing_over_threshold_is_a_plain_seal() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:7", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.demoted_count, 0);
        assert_eq!(values.phase, Phase::Active);
        assert!(store.tails.lock().unwrap().is_empty());
        assert!(store.finished.lock().unwrap().is_none());
        // Still reported, so the operator sees the rule ran and caught nothing.
        assert_eq!(store.report.lock().unwrap().clone().unwrap().demoted, 0);
    }

    #[tokio::test]
    async fn stragglers_are_not_classified() {
        // Shard 0 issued 5 indices; rows 5..9 raced the seal. All ten share an
        // address, but only the five in the cohort count against the rule —
        // and a rule of 5 therefore catches nothing.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.cohort, 5);
        assert_eq!(report.demoted, 0);
        assert!(store.tails.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_lost_election_writes_nothing_after_classifying() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], true).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        assert_eq!(result, SealResult::AlreadySealed);
        assert!(store.report.lock().unwrap().is_none());
        assert!(store.tails.lock().unwrap().is_empty());
        assert!(store.finished.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn a_tail_write_failure_still_opens_the_event_with_what_landed() {
        // The winner died after three of seven tail writes. The other four
        // keep their primary slots — never a duplicate — and the phase must
        // still flip, or the event never opens.
        let mut store =
            FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        store.tail_budget = Some(3);
        seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        assert_eq!(store.tails.lock().unwrap().len(), 3);
        assert_eq!(*store.finished.lock().unwrap(), Some(0));
    }

    #[tokio::test]
    async fn unparsable_rules_seal_without_demotion_and_say_so() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            &config("address:lots", DemotionMode::Enforce),
            5,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.demoted_count, 0);
        assert_eq!(values.phase, Phase::Active);
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.rules, "address:lots");
        assert!(report.error.is_some());
        assert_eq!(report.sealed_at, 5);
        assert!(store.tails.lock().unwrap().is_empty());
    }
}
