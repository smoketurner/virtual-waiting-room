//! Open logic for the `open_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence — so a retry or a double-fire opens exactly
//! once.
//!
//! The open is also where `queue_counter` starts behind the cohort, so live
//! joiners are numbered behind every pre-queue position instead of colliding
//! with one. That clause is in the same atomic update as the rest of the open
//! write: a separate write could be lost between the open and the first live
//! join.
//!
//! # Demotion (issue #145)
//!
//! When the operator has set demotion rules, the open also reads the cohort's
//! join-time telemetry and demotes whole groups that exceed a threshold
//! (`wr_common::demotion`). Nothing is written per row; the order of
//! operations is what keeps the open's guarantees intact:
//!
//! 1. Read the shard counts, so the cohort is fixed.
//! 2. Scan the pre-queue and classify it. Nothing is written yet, so a
//!    double-fire at this point costs a duplicate scan and nothing else.
//! 3. Write the demoted group set — the [`wr_common::DemotionSet`] every
//!    resolver will match rows against — as chunk items under a per-run
//!    nonce. Written *before* the election so the winning open never names a
//!    set that does not exist yet.
//! 4. The one conditional open write, now also carrying `D`, the nonce, and
//!    the chunk count, and starting the live-join sequence at `2N`. This is
//!    the election: exactly one run wins it, and only its nonce is ever read.
//!    A loser deletes its own chunks; if that fails they are orphans nothing
//!    names.
//! 5. The winner writes the report.
//!
//! No rules means no scan at all: the open is then exactly the single write it
//! was before this existed.

use std::future::Future;
use std::sync::{Arc, Mutex};

use wr_common::{
    Classification, Cohort, CohortError, CohortOffsets, DemotionMode, DemotionRef, DemotionReport,
    DemotionRules, DemotionSet, MAX_CHUNK_BYTES, RuleParseError, SHARDS, Telemetry,
};

pub mod dynamo;

/// The values written when the event opens: the seed, cohort size, prefix
/// offsets, and what was demoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenValues {
    pub seed: [u8; 32],
    pub participant_count: u64,
    pub offsets: [u64; SHARDS],
    /// `D`: cohort rows the demotion set matches, `0` unless demotion is
    /// enforced and caught something. Positive means the tail `[N, 2N)` is in
    /// use and `queue_counter` starts at `2N`.
    pub demoted_count: u64,
    /// The set's location, present exactly when `demoted_count > 0`.
    pub demotion: Option<DemotionRef>,
}

impl OpenValues {
    /// Where the live-join sequence starts: `N` with no tail, `2N` with one.
    ///
    /// # Errors
    ///
    /// [`CohortError::Overflow`] if `2N` exceeds `u64`.
    pub fn queue_counter_start(&self) -> Result<u64, CohortError> {
        if self.demoted_count == 0 {
            return Ok(self.participant_count);
        }
        self.participant_count
            .checked_mul(2)
            .ok_or(CohortError::Overflow)
    }
}

/// The demotion configuration the open runs with: the operator's rules and
/// whether to act on them. `rules_text` is kept so a report can quote what
/// was configured even when it did not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemotionConfig {
    pub rules: Result<DemotionRules, RuleParseError>,
    pub rules_text: String,
    pub mode: DemotionMode,
}

impl DemotionConfig {
    /// No rules: the open never scans.
    #[must_use]
    pub fn off() -> Self {
        Self {
            rules: Ok(DemotionRules::default()),
            rules_text: String::new(),
            mode: DemotionMode::Observe,
        }
    }

    /// Parses the two environment values. A rules string that does not parse
    /// is carried as the error rather than failing here, so the open still
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
#[error("open store error: {0}")]
pub struct StoreError(pub String);

/// The result of attempting an open.
#[derive(Debug, PartialEq, Eq)]
pub enum OpenResult {
    /// This call performed the open and wrote the values.
    Opened(Box<OpenValues>),
    /// The event was already open (the guard rejected the write); nothing
    /// changed. A double-fire or retry lands here.
    AlreadyOpen,
}

/// One pre-queue row as the open's scan sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRow {
    pub shard: u8,
    pub local_index: u64,
    pub telemetry: Option<Telemetry>,
}

/// The persistence port the open drives.
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

    /// Writes the demotion set's chunks under `nonce`, chunk `k` holding
    /// `chunks[k]`.
    fn write_demotion_chunks(
        &self,
        event_id: &str,
        nonce: &str,
        chunks: &[Vec<String>],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Deletes the `count` chunks written under `nonce`: a lost election's
    /// leftovers. Best effort; the winning event item never names them.
    fn delete_demotion_chunks(
        &self,
        event_id: &str,
        nonce: &str,
        count: u32,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Writes the open values, starts `queue_counter`, and flips the phase to
    /// active, guarded by `attribute_not_exists(shuffle_seed)`. Returns
    /// `false` if the guard rejected the write (already open).
    fn write_open(
        &self,
        event_id: &str,
        values: &OpenValues,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Writes the demotion report item.
    fn write_report(
        &self,
        event_id: &str,
        report: &DemotionReport,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Folds the shard counts into the offsets and cohort size and pairs them with
/// a freshly generated seed. Nothing demoted; a demoting open fills that in.
///
/// # Errors
///
/// Returns [`CohortError::Overflow`] if the summed cohort size exceeds `u64`.
pub fn open_values(counts: [u64; SHARDS], seed: [u8; 32]) -> Result<OpenValues, CohortError> {
    let opened: CohortOffsets = CohortOffsets::from_counts(counts)?;
    let mut offsets = [0u64; SHARDS];
    for (shard, slot) in offsets.iter_mut().enumerate() {
        *slot = opened.offset(shard);
    }
    Ok(OpenValues {
        seed,
        participant_count: opened.participant_count(),
        offsets,
        demoted_count: 0,
        demotion: None,
    })
}

/// Lowercase hex of the per-run nonce. Never contains `#`, so it is safe
/// inside a `Counters` key.
#[must_use]
pub fn nonce_hex(nonce: [u8; 8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(16);
    for byte in nonce {
        // Writing two hex digits into a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Reads the shard counts, computes the open values with the supplied seed,
/// classifies the cohort if rules are set, and writes the open under the
/// once-only guard. The seed and nonce are passed in so the logic is
/// deterministic under test; production generates both from a CSPRNG. `now`
/// stamps the report.
///
/// # Errors
///
/// Returns [`StoreError`] if reading the shard counts, scanning, folding,
/// writing the set, or writing the open fails. A report write failure after
/// the open is logged, not returned: the open has landed by then.
pub async fn open_event<S: Store>(
    store: &S,
    event_id: &str,
    seed: [u8; 32],
    nonce: [u8; 8],
    config: &DemotionConfig,
    now: u64,
) -> Result<OpenResult, StoreError> {
    let counts = store.read_shard_counts(event_id).await?;
    let mut values =
        open_values(counts, seed).map_err(|e| StoreError(format!("fold shard counts: {e}")))?;

    let rules = match &config.rules {
        Ok(rules) => rules,
        Err(error) => {
            // Open without demotion, and say so where the operator looks.
            tracing::error!(event_id, %error, rules = %config.rules_text, "demotion rules did not parse; opening without demotion");
            if !store.write_open(event_id, &values).await? {
                tracing::info!(event_id, "event already open; no-op");
                return Ok(OpenResult::AlreadyOpen);
            }
            let report = DemotionReport::from_error(&config.rules_text, config.mode, error, now);
            if let Err(e) = store.write_report(event_id, &report).await {
                tracing::error!(event_id, error = %e, "could not write the demotion report");
            }
            return Ok(OpenResult::Opened(Box::new(values)));
        }
    };

    if rules.is_empty() {
        if !store.write_open(event_id, &values).await? {
            tracing::info!(event_id, "event already open; no-op");
            return Ok(OpenResult::AlreadyOpen);
        }
        tracing::info!(
            event_id,
            participant_count = values.participant_count,
            "event opened"
        );
        return Ok(OpenResult::Opened(Box::new(values)));
    }

    let classification = classify_cohort(store, &values, rules.clone()).await?;
    let enforcing = config.mode == DemotionMode::Enforce && classification.demoted > 0;
    if enforcing {
        let set = DemotionSet::from_groups(&classification.groups);
        let chunks = set.to_chunks(MAX_CHUNK_BYTES);
        let chunk_count = u32::try_from(chunks.len())
            .map_err(|_err| StoreError("demotion set has too many chunks".to_owned()))?;
        values.demoted_count = classification.demoted;
        values.demotion = Some(DemotionRef {
            nonce: nonce_hex(nonce),
            chunks: chunk_count,
        });
        // Fail before anything is written: an overflow here would open an
        // event whose live joins collide with its tail.
        values
            .queue_counter_start()
            .map_err(|e| StoreError(format!("live-join start: {e}")))?;
        store
            .write_demotion_chunks(event_id, &nonce_hex(nonce), &chunks)
            .await?;
    }

    if !store.write_open(event_id, &values).await? {
        tracing::info!(event_id, "event already open; no-op");
        if let Some(demotion) = &values.demotion
            && let Err(e) = store
                .delete_demotion_chunks(event_id, &demotion.nonce, demotion.chunks)
                .await
        {
            // Orphans nothing names; worth a line, not a failure.
            tracing::warn!(event_id, error = %e, "could not delete a lost election's demotion chunks");
        }
        return Ok(OpenResult::AlreadyOpen);
    }
    tracing::info!(
        event_id,
        participant_count = values.participant_count,
        cohort = classification.cohort,
        demoted_groups = classification.groups.len(),
        demoted = classification.demoted,
        enforced = enforcing,
        mode = config.mode.as_wire_str(),
        "event opened"
    );

    let report = DemotionReport::from_classification(config.mode, rules, &classification, now);
    if let Err(e) = store.write_report(event_id, &report).await {
        // The report is for the operator; the open has landed, so this is
        // logged rather than fatal.
        tracing::error!(event_id, error = %e, "could not write the demotion report");
    }

    Ok(OpenResult::Opened(Box::new(values)))
}

/// Scans the pre-queue and classifies every row the fixed offsets place in
/// the cohort. A straggler (local index at or past its shard's issued count)
/// is a live joiner with no pre-queue position and is left out.
async fn classify_cohort<S: Store>(
    store: &S,
    values: &OpenValues,
    rules: DemotionRules,
) -> Result<Classification, StoreError> {
    let offsets = CohortOffsets::from_parts(values.offsets, values.participant_count);
    let cohort = Arc::new(Mutex::new(Cohort::new(rules)));
    let sink = Arc::clone(&cohort);
    let visit: Arc<dyn Fn(ScannedRow) + Send + Sync> =
        Arc::new(move |row: ScannedRow| {
            match offsets.assign(usize::from(row.shard), row.local_index) {
                wr_common::Assignment::PreQueue { .. } => {
                    if let Ok(mut cohort) = sink.lock() {
                        cohort.observe(row.telemetry.as_ref());
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

/// The phase the open leaves the event in.
#[must_use]
pub fn opened_phase() -> wr_common::Phase {
    wr_common::Phase::Active
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure"
    )]

    use std::sync::Mutex;

    use wr_common::Phase;

    use super::*;

    struct FakeStore {
        counts: [u64; SHARDS],
        already_open: bool,
        rows: Vec<ScannedRow>,
        written: Mutex<Option<OpenValues>>,
        report: Mutex<Option<DemotionReport>>,
        /// `(nonce, chunks)` written, in order.
        chunks: Mutex<Vec<(String, Vec<Vec<String>>)>>,
        /// `(nonce, count)` deleted, in order.
        deleted: Mutex<Vec<(String, u32)>>,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_open: bool) -> Self {
            Self {
                counts,
                already_open,
                rows: Vec::new(),
                written: Mutex::new(None),
                report: Mutex::new(None),
                chunks: Mutex::new(Vec::new()),
                deleted: Mutex::new(Vec::new()),
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

        fn write_demotion_chunks(
            &self,
            _event_id: &str,
            nonce: &str,
            chunks: &[Vec<String>],
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.chunks
                .lock()
                .unwrap()
                .push((nonce.to_owned(), chunks.to_vec()));
            std::future::ready(Ok(()))
        }

        fn delete_demotion_chunks(
            &self,
            _event_id: &str,
            nonce: &str,
            count: u32,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.deleted.lock().unwrap().push((nonce.to_owned(), count));
            std::future::ready(Ok(()))
        }

        fn write_open(
            &self,
            _event_id: &str,
            values: &OpenValues,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            let wrote = if self.already_open {
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
    }

    const NONCE: [u8; 8] = [0x0b, 0xad, 0xca, 0xfe, 0x00, 0x11, 0x22, 0x33];

    fn row(shard: u8, local_index: u64, address: &str) -> ScannedRow {
        ScannedRow {
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
            rows.push(row(0, l, "198.51.100.1"));
        }
        for l in 7..10 {
            rows.push(row(0, l, "203.0.113.5"));
        }
        rows
    }

    fn config(rules: &str, mode: DemotionMode) -> DemotionConfig {
        DemotionConfig::parse(rules, mode)
    }

    #[test]
    fn open_values_are_prefix_sums_and_total() {
        let values = open_values([3, 0, 5, 1, 0, 0, 2, 0, 0, 4], [7u8; 32]).unwrap();
        assert_eq!(values.participant_count, 15);
        assert_eq!(values.offsets, [0, 3, 3, 8, 9, 9, 9, 11, 11, 11]);
        assert_eq!(values.seed, [7u8; 32]);
        assert_eq!(values.demoted_count, 0);
        assert!(values.demotion.is_none());
        assert_eq!(values.queue_counter_start().unwrap(), 15);
    }

    #[test]
    fn the_nonce_is_lowercase_hex_with_no_separator() {
        let hex = nonce_hex(NONCE);
        assert_eq!(hex, "0badcafe00112233");
        assert!(!hex.contains('#'));
    }

    #[tokio::test]
    async fn first_open_writes_values() {
        let store = FakeStore::new([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], false);
        let result = open_event(&store, "evt-1", [9u8; 32], NONCE, &DemotionConfig::off(), 1)
            .await
            .unwrap();
        match result {
            OpenResult::Opened(values) => assert_eq!(values.participant_count, 10),
            OpenResult::AlreadyOpen => panic!("expected a first open"),
        }
        let written = store.written.lock().unwrap().clone().unwrap();
        assert_eq!(written.demoted_count, 0);
        // No rules: no report, no chunks.
        assert!(store.report.lock().unwrap().is_none());
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn second_open_is_noop() {
        let store = FakeStore::new([1; SHARDS], true);
        let result = open_event(&store, "evt-1", [9u8; 32], NONCE, &DemotionConfig::off(), 1)
            .await
            .unwrap();
        assert_eq!(result, OpenResult::AlreadyOpen);
        assert!(store.written.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_cohort_opens_to_zero() {
        let store = FakeStore::new([0; SHARDS], false);
        let result = open_event(&store, "evt-1", [1u8; 32], NONCE, &DemotionConfig::off(), 1)
            .await
            .unwrap();
        match result {
            OpenResult::Opened(values) => {
                assert_eq!(values.participant_count, 0);
                assert_eq!(values.offsets, [0; SHARDS]);
            }
            OpenResult::AlreadyOpen => panic!("expected a first open"),
        }
    }

    #[tokio::test]
    async fn observe_mode_reports_what_it_would_demote_and_demotes_nobody() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Observe),
            77,
        )
        .await
        .unwrap();
        let OpenResult::Opened(values) = result else {
            panic!("expected a first open");
        };
        // The open itself is untouched by observation.
        assert_eq!(values.demoted_count, 0);
        assert!(values.demotion.is_none());
        assert_eq!(values.queue_counter_start().unwrap(), 10);
        assert!(store.chunks.lock().unwrap().is_empty());
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
        assert_eq!(report.opened_at, 77);
        assert!(report.error.is_none());
    }

    #[tokio::test]
    async fn enforce_mode_writes_the_set_before_the_open_and_names_it_in_the_open() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let OpenResult::Opened(values) = result else {
            panic!("expected a first open");
        };
        assert_eq!(values.demoted_count, 7);
        // Live joins start behind the tail, which is a second copy of [0, N).
        assert_eq!(values.queue_counter_start().unwrap(), 20);
        let demotion = values.demotion.clone().unwrap();
        assert_eq!(demotion.nonce, "0badcafe00112233");
        assert_eq!(demotion.chunks, 1);
        // Exactly the demoted group, under the nonce the open names, and
        // nothing written per row anywhere.
        let chunks = store.chunks.lock().unwrap().clone();
        assert_eq!(
            chunks,
            vec![(
                "0badcafe00112233".to_owned(),
                vec![vec!["address:198.51.100.1".to_owned()]]
            )]
        );
        let set = DemotionSet::from_entries(chunks[0].1.iter().flatten().cloned()).unwrap();
        assert!(set.matches(farmed_rows()[0].telemetry.as_ref()));
        assert!(!set.matches(farmed_rows()[9].telemetry.as_ref()));
        assert!(store.deleted.lock().unwrap().is_empty());
        assert_eq!(
            store.report.lock().unwrap().clone().unwrap().mode,
            "enforce"
        );
        assert_eq!(
            store.written.lock().unwrap().clone().unwrap().demotion,
            Some(demotion)
        );
    }

    #[tokio::test]
    async fn enforce_mode_with_nothing_over_threshold_is_a_plain_open() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:7", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let OpenResult::Opened(values) = result else {
            panic!("expected a first open");
        };
        assert_eq!(values.demoted_count, 0);
        assert!(values.demotion.is_none());
        assert_eq!(values.queue_counter_start().unwrap(), 10);
        assert!(store.chunks.lock().unwrap().is_empty());
        // Still reported, so the operator sees the rule ran and caught nothing.
        assert_eq!(store.report.lock().unwrap().clone().unwrap().demoted, 0);
    }

    #[tokio::test]
    async fn stragglers_are_not_classified() {
        // Shard 0 issued 5 indices; rows 5..9 raced the open. All ten share an
        // address, but only the five in the cohort count against the rule —
        // and a rule of 5 therefore catches nothing.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.cohort, 5);
        assert_eq!(report.demoted, 0);
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_lost_election_deletes_its_own_chunks_and_writes_nothing_else() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], true).with_rows(farmed_rows());
        let result = open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        assert_eq!(result, OpenResult::AlreadyOpen);
        // The chunks were written before the election (they must exist before
        // an open could name them) and cleaned up after losing it.
        assert_eq!(store.chunks.lock().unwrap().len(), 1);
        assert_eq!(
            store.deleted.lock().unwrap().clone(),
            vec![("0badcafe00112233".to_owned(), 1)]
        );
        assert!(store.report.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn unparsable_rules_open_without_demotion_and_say_so() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:lots", DemotionMode::Enforce),
            5,
        )
        .await
        .unwrap();
        let OpenResult::Opened(values) = result else {
            panic!("expected a first open");
        };
        assert_eq!(values.demoted_count, 0);
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "enforce");
        assert_eq!(report.rules, "address:lots");
        assert!(report.error.is_some());
        assert_eq!(report.opened_at, 5);
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unparsable_rules_preserve_the_configured_mode_in_the_report() {
        // A misconfigured `enforce` event that fails to parse must record
        // `enforce` in the audit item, not `observe` — observe, per ADR-0029,
        // means a classification ran, and the error path runs none.
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:lots", DemotionMode::Enforce),
            5,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "enforce");
    }

    #[tokio::test]
    async fn unparsable_rules_in_observe_mode_record_observe_in_the_report() {
        // The configured mode is forwarded verbatim, so observe stays observe.
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        open_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:lots", DemotionMode::Observe),
            5,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "observe");
    }

    #[test]
    fn opening_always_activates_the_event() {
        // There is no held phase any more: with nothing to write per row after
        // the election, the open write itself is the moment the event opens.
        assert_eq!(opened_phase(), Phase::Active);
    }
}
