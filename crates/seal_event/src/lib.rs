//! Seal logic for the `seal_event` Lambda: at the scheduled start it reads the
//! 10 pre-queue shard counts, folds them into prefix offsets and the cohort
//! size, generates the permutation seed, and writes all four plus the active
//! phase and the live-join counter's starting value in one conditional update
//! guarded by the seed's absence — so a retry or a double-fire seals exactly
//! once.
//!
//! The seal is also where `queue_counter` starts behind the cohort, so live
//! joiners are numbered behind every pre-queue position instead of colliding
//! with one. That clause is in the same atomic update as the rest of the seal
//! write: a separate write could be lost between the seal and the first live
//! join.
//!
//! # Demotion (issue #145)
//!
//! When the operator has set demotion rules, the seal also reads the cohort's
//! join-time telemetry and demotes whole groups that exceed a threshold
//! (`wr_common::demotion`). Nothing is written per row; the order of
//! operations is what keeps the seal's guarantees intact:
//!
//! 1. Read the shard counts, so the cohort is fixed.
//! 2. Scan the pre-queue and classify it. Nothing is written yet, so a
//!    double-fire at this point costs a duplicate scan and nothing else. The
//!    scan is the one store boundary that cannot be scoped to the event — the
//!    pre-queue table is shared and its rows carry no event id. `N`
//!    (`participant_count`) counts indices *issued*, not rows written, so a
//!    burned slot (claimed-but-unwritten) makes `cohort < N` even on a clean
//!    event, and `cohort > N` is no longer a reliable contamination signal.
//!    Two signals are used instead: a *repeated `(shard, local index)` slot*
//!    means another event's row duplicated a current one (a single event
//!    issues each slot at most once), which is a definite contamination signal
//!    and skips enforcement; and `cohort < N` means burned slots make the read
//!    indistinguishable from one that folded in foreign rows, so contamination
//!    cannot be ruled out and the report says so rather than recording a clean
//!    demotion.
//! 3. Write the demoted group set — the [`wr_common::DemotionSet`] every
//!    resolver will match rows against — as chunk items under a per-run
//!    nonce. Written *before* the election so the winning seal never names a
//!    set that does not exist yet.
//! 4. The one conditional seal write, now also carrying `D`, the nonce, and
//!    the chunk count, and starting the live-join sequence at `2N`. This is
//!    the election: exactly one run wins it, and only its nonce is ever read.
//!    A loser deletes its own chunks; if that fails they are orphans nothing
//!    names.
//! 5. The winner writes the report.
//!
//! No rules means no scan at all: the seal is then exactly the single write it
//! was before this existed.

use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use wr_common::{
    Classification, Cohort, DemotionMode, DemotionRef, DemotionReport, DemotionRules, DemotionSet,
    MAX_CHUNK_BYTES, RuleParseError, SHARDS, SealError, SealedOffsets, Telemetry,
};

pub mod dynamo;

/// The values written by a seal: the seed, cohort size, prefix offsets, and
/// what was demoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealValues {
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

impl SealValues {
    /// Where the live-join sequence starts: `N` with no tail, `2N` with one.
    ///
    /// # Errors
    ///
    /// [`SealError::Overflow`] if `2N` exceeds `u64`.
    pub fn queue_counter_start(&self) -> Result<u64, SealError> {
        if self.demoted_count == 0 {
            return Ok(self.participant_count);
        }
        self.participant_count
            .checked_mul(2)
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

    /// Writes the seal values, starts `queue_counter`, and flips the phase to
    /// active, guarded by `attribute_not_exists(shuffle_seed)`. Returns
    /// `false` if the guard rejected the write (already sealed).
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
}

/// Folds the shard counts into the offsets and cohort size and pairs them with
/// a freshly generated seed. Nothing demoted; a demoting seal fills that in.
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

/// Reads the shard counts, computes the seal values with the supplied seed,
/// classifies the cohort if rules are set, and writes the seal under the
/// once-only guard. The seed and nonce are passed in so the logic is
/// deterministic under test; production generates both from a CSPRNG. `now`
/// stamps the report.
///
/// # Errors
///
/// Returns [`StoreError`] if reading the shard counts, scanning, folding,
/// writing the set, or writing the seal fails. A report write failure after
/// the seal is logged, not returned: the seal has landed by then.
pub async fn seal_event<S: Store>(
    store: &S,
    event_id: &str,
    seed: [u8; 32],
    nonce: [u8; 8],
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
            let report = DemotionReport::from_error(&config.rules_text, config.mode, error, now);
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

    let (classification, distinct) = classify_cohort(store, &values, rules.clone()).await?;

    // `N` (`participant_count`) counts indices *issued* — claimed by a shard
    // counter — not rows written: a registration whose row write failed after
    // the counter incremented (see `assign_position`'s `Duplicate` and `Err`
    // branches) leaves a *burned* slot with no row, so a clean event can read
    // `cohort < N` and `cohort > N` is no longer a reliable contamination
    // signal. Two signals are used instead, set up by [`Contamination::assess`]:
    //   - A repeated `(shard, local_index)` slot (a single event issues each
    //     at most once) is a *definite* contamination signal — demotion is
    //     withheld.
    //   - `cohort < N` means burned slots make the read indistinguishable from
    //     one that folded in foreign rows. Contamination cannot be *ruled out*,
    //     so the report flags it; enforcement is *not* withheld, because
    //     burned indices are an expected, throughput-dependent residual and
    //     withholding would disable demotion at any real scale. The complete
    //     fix — an event id on `PreQueue` rows — is a design change the ADR
    //     would revisit.
    let contamination =
        Contamination::assess(classification.cohort, distinct, values.participant_count);
    contamination.log(
        event_id,
        classification.cohort,
        distinct,
        values.participant_count,
    );

    let enforcing = !contamination.contaminated
        && config.mode == DemotionMode::Enforce
        && classification.demoted > 0;
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
        // Fail before anything is written: an overflow here would seal an
        // event whose live joins collide with its tail.
        values
            .queue_counter_start()
            .map_err(|e| StoreError(format!("live-join start: {e}")))?;
        store
            .write_demotion_chunks(event_id, &nonce_hex(nonce), &chunks)
            .await?;
    }

    if !store.write_seal(event_id, &values).await? {
        tracing::info!(event_id, "event already sealed; no-op");
        if let Some(demotion) = &values.demotion
            && let Err(e) = store
                .delete_demotion_chunks(event_id, &demotion.nonce, demotion.chunks)
                .await
        {
            // Orphans nothing names; worth a line, not a failure.
            tracing::warn!(event_id, error = %e, "could not delete a lost election's demotion chunks");
        }
        return Ok(SealResult::AlreadySealed);
    }
    tracing::info!(
        event_id,
        participant_count = values.participant_count,
        cohort = classification.cohort,
        distinct,
        demoted_groups = classification.groups.len(),
        demoted = classification.demoted,
        enforced = enforcing,
        contaminated = contamination.contaminated,
        burned_regime = contamination.burned_regime,
        mode = config.mode.as_wire_str(),
        "event sealed"
    );

    let mut report = DemotionReport::from_classification(config.mode, rules, &classification, now);
    report.error = contamination.caveat(classification.cohort, distinct, values.participant_count);
    if let Err(e) = store.write_report(event_id, &report).await {
        // The report is for the operator; the seal has landed, so this is
        // logged rather than fatal.
        tracing::error!(event_id, error = %e, "could not write the demotion report");
    }

    Ok(SealResult::Sealed(Box::new(values)))
}

/// How the seal treats a scanned cohort relative to the event's own
/// registrations: definitely contaminated, unverifiable, or clean.
///
/// `N` (`participant_count`) counts indices *issued*, not rows written, so a
/// burned slot makes `cohort < N` even on a clean event and `cohort > N` is
/// not a reliable contamination signal. This replaces it with two checks:
///   - `cohort > distinct`: a single event issues each `(shard, local_index)`
///     at most once, so a slot seen more than once is another event's row
///     duplicating a current one. Definite contamination — demotion withheld.
///   - `cohort < N`: burned slots make a clean read indistinguishable from one
///     that folded in foreign rows. Unverifiable — reported, not withheld.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Contamination {
    contaminated: bool,
    burned_regime: bool,
}

impl Contamination {
    /// Assesses a scanned cohort of `cohort` rows in `distinct` slots against
    /// `participant_count` (`N`) claimed indices.
    #[must_use]
    fn assess(cohort: u64, distinct: u64, participant_count: u64) -> Self {
        Self {
            contaminated: cohort > distinct,
            burned_regime: cohort < participant_count,
        }
    }

    /// Emits the run's contamination log: `error` when contamination is
    /// definite, `warn` when it cannot be ruled out, nothing when clean.
    fn log(self, event_id: &str, cohort: u64, distinct: u64, participant_count: u64) {
        if self.contaminated {
            tracing::error!(
                event_id,
                participant_count,
                cohort,
                distinct,
                "pre-queue scan read repeated pre-queue slots; sealing without demotion"
            );
        } else if self.burned_regime {
            tracing::warn!(
                event_id,
                participant_count,
                cohort,
                "pre-queue scan read fewer rows than were registered; contamination cannot be \
                 ruled out"
            );
        }
    }

    /// The operator-facing `DemotionReport::error` caveat for the run, or
    /// `None` for a clean full turnout.
    #[must_use]
    fn caveat(self, cohort: u64, distinct: u64, participant_count: u64) -> Option<String> {
        if self.contaminated {
            Some(format!(
                "the scan covered {cohort} cohort rows in {distinct} distinct pre-queue slots \
                 against {participant_count} registrations for this event; a single event issues \
                 each (shard, local index) at most once, so this included another event's rows \
                 and nothing was demoted"
            ))
        } else if self.burned_regime {
            Some(format!(
                "the scan covered {cohort} cohort rows against {participant_count} \
                 registrations for this event; burned pre-queue indices make this read \
                 indistinguishable from one that folded in another event's rows, so \
                 contamination cannot be ruled out"
            ))
        } else {
            None
        }
    }
}

/// Scans the pre-queue and classifies every row the sealed offsets place in
/// the cohort. A straggler (local index at or past its shard's issued count)
/// is a live joiner with no pre-queue position and is left out.
///
/// Returns the classification plus the count of *distinct* `(shard,
/// local_index)` slots observed inside the cohort. A single event issues each
/// slot at most once (`assign_position` claims a fresh contiguous block per
/// batch and never reuses an index), so `cohort > distinct` means another
/// event's row duplicated a current one — the cross-event contamination signal
/// [`seal_event`] uses alongside `participant_count`. It cannot detect a
/// foreign row that fills a *burned* slot (no current row to duplicate); the
/// burned-index regime `cohort < N` surfaces that residual to the operator.
async fn classify_cohort<S: Store>(
    store: &S,
    values: &SealValues,
    rules: DemotionRules,
) -> Result<(Classification, u64), StoreError> {
    let offsets = SealedOffsets::from_parts(values.offsets, values.participant_count);
    let cohort = Arc::new(Mutex::new(Cohort::new(rules)));
    let slots = Arc::new(Mutex::new(HashSet::<(u8, u64)>::new()));
    let sink = Arc::clone(&cohort);
    let slot_sink = Arc::clone(&slots);
    let visit: Arc<dyn Fn(ScannedRow) + Send + Sync> =
        Arc::new(move |row: ScannedRow| {
            match offsets.assign(usize::from(row.shard), row.local_index) {
                wr_common::Assignment::PreQueue { .. } => {
                    if let Ok(mut cohort) = sink.lock() {
                        cohort.observe(row.telemetry.as_ref());
                    }
                    if let Ok(mut seen) = slot_sink.lock() {
                        seen.insert((row.shard, row.local_index));
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
    let distinct = u64::try_from(
        Arc::try_unwrap(slots)
            .map_err(|_arc| StoreError("scan still holds the slot set".to_owned()))?
            .into_inner()
            .map_err(|_poison| StoreError("slot set lock poisoned".to_owned()))?
            .len(),
    )
    .unwrap_or(u64::MAX);
    tracing::info!(
        scanned,
        cohort = cohort.len(),
        distinct,
        "pre-queue scanned"
    );
    Ok((cohort.classify(), distinct))
}

/// The phase the seal leaves the event in.
#[must_use]
pub fn sealed_phase() -> wr_common::Phase {
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
        already_sealed: bool,
        rows: Vec<ScannedRow>,
        written: Mutex<Option<SealValues>>,
        report: Mutex<Option<DemotionReport>>,
        /// `(nonce, chunks)` written, in order.
        chunks: Mutex<Vec<(String, Vec<Vec<String>>)>>,
        /// `(nonce, count)` deleted, in order.
        deleted: Mutex<Vec<(String, u32)>>,
    }

    impl FakeStore {
        fn new(counts: [u64; SHARDS], already_sealed: bool) -> Self {
            Self {
                counts,
                already_sealed,
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
    fn seal_values_are_prefix_sums_and_total() {
        let values = seal_values([3, 0, 5, 1, 0, 0, 2, 0, 0, 4], [7u8; 32]).unwrap();
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
    async fn first_seal_writes_values() {
        let store = FakeStore::new([2, 2, 2, 2, 2, 0, 0, 0, 0, 0], false);
        let result = seal_event(&store, "evt-1", [9u8; 32], NONCE, &DemotionConfig::off(), 1)
            .await
            .unwrap();
        match result {
            SealResult::Sealed(values) => assert_eq!(values.participant_count, 10),
            SealResult::AlreadySealed => panic!("expected a first seal"),
        }
        let written = store.written.lock().unwrap().clone().unwrap();
        assert_eq!(written.demoted_count, 0);
        // No rules: no report, no chunks.
        assert!(store.report.lock().unwrap().is_none());
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn second_seal_is_noop() {
        let store = FakeStore::new([1; SHARDS], true);
        let result = seal_event(&store, "evt-1", [9u8; 32], NONCE, &DemotionConfig::off(), 1)
            .await
            .unwrap();
        assert_eq!(result, SealResult::AlreadySealed);
        assert!(store.written.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_cohort_seals_to_zero() {
        let store = FakeStore::new([0; SHARDS], false);
        let result = seal_event(&store, "evt-1", [1u8; 32], NONCE, &DemotionConfig::off(), 1)
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
            NONCE,
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
        assert_eq!(report.sealed_at, 77);
        assert!(report.error.is_none());
    }

    #[tokio::test]
    async fn enforce_mode_writes_the_set_before_the_seal_and_names_it_in_the_seal() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.demoted_count, 7);
        // Live joins start behind the tail, which is a second copy of [0, N).
        assert_eq!(values.queue_counter_start().unwrap(), 20);
        let demotion = values.demotion.clone().unwrap();
        assert_eq!(demotion.nonce, "0badcafe00112233");
        assert_eq!(demotion.chunks, 1);
        // Exactly the demoted group, under the nonce the seal names, and
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
    async fn enforce_mode_with_nothing_over_threshold_is_a_plain_seal() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:7", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
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
        // Shard 0 issued 5 indices; rows 5..9 raced the seal. All ten share an
        // address, but only the five in the cohort count against the rule —
        // and a rule of 5 therefore catches nothing.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        seal_event(
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

    /// Eight rows on shard 0 sharing one address, for an event that issued
    /// five indices: five are this event's and three were left in the shared
    /// pre-queue table by a previous one. The scan cannot tell them apart, so
    /// the cohort comes back at 8 against a participant count of 5.
    fn another_events_rows_mixed_in() -> Vec<ScannedRow> {
        (0..8).map(|l| row(0, l % 5, "198.51.100.1")).collect()
    }

    #[tokio::test]
    async fn a_scan_wider_than_the_event_seals_on_time_without_demoting() {
        // The groups are built from another event's rows, so they are not this
        // event's to demote — but the seal's own values come from the shard
        // counts, so the event still opens, with the tail unused.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false)
            .with_rows(another_events_rows_mixed_in());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:4", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.participant_count, 5);
        assert_eq!(values.demoted_count, 0);
        assert!(values.demotion.is_none());
        // No tail, so live joins start at N rather than 2N.
        assert_eq!(values.queue_counter_start().unwrap(), 5);
        assert!(store.chunks.lock().unwrap().is_empty());
        assert!(store.written.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn a_scan_wider_than_the_event_says_why_in_the_report() {
        // The operator's only record of a control that did not apply: both
        // counts, so the mismatch is visible, and the reason beside them.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false)
            .with_rows(another_events_rows_mixed_in());
        seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:4", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.cohort, 8);
        assert_eq!(report.mode, "enforce");
        let error = report.error.unwrap();
        assert!(error.contains('8'), "names the cohort: {error}");
        assert!(error.contains('5'), "names the registrations: {error}");
        assert!(error.contains("another event"), "names the cause: {error}");
    }

    #[tokio::test]
    async fn a_scan_wider_than_the_event_is_reported_under_observe_too() {
        // Observe demotes nobody either way; what the guard adds here is that
        // the report does not present another event's rows as a classification
        // this event's thresholds can be tuned against.
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false)
            .with_rows(another_events_rows_mixed_in());
        seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:4", DemotionMode::Observe),
            1,
        )
        .await
        .unwrap();
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "observe");
        assert!(report.error.is_some());
    }

    #[tokio::test]
    async fn a_clean_scan_reports_no_error() {
        // The boundary — a cohort exactly filling the event — is a full
        // turnout, not contamination, and is pinned to enforce by
        // `enforce_mode_writes_the_set_before_the_seal_and_names_it_in_the_seal`.
        // This is the other half: nothing is flagged on the clean path.
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        assert!(
            store
                .report
                .lock()
                .unwrap()
                .clone()
                .unwrap()
                .error
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_lost_election_deletes_its_own_chunks_and_writes_nothing_else() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], true).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:5", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        assert_eq!(result, SealResult::AlreadySealed);
        // The chunks were written before the election (they must exist before
        // a seal could name them) and cleaned up after losing it.
        assert_eq!(store.chunks.lock().unwrap().len(), 1);
        assert_eq!(
            store.deleted.lock().unwrap().clone(),
            vec![("0badcafe00112233".to_owned(), 1)]
        );
        assert!(store.report.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn unparsable_rules_seal_without_demotion_and_say_so() {
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:lots", DemotionMode::Enforce),
            5,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        assert_eq!(values.demoted_count, 0);
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "enforce");
        assert_eq!(report.rules, "address:lots");
        assert!(report.error.is_some());
        assert_eq!(report.sealed_at, 5);
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unparsable_rules_preserve_the_configured_mode_in_the_report() {
        // A misconfigured `enforce` event that fails to parse must record
        // `enforce` in the audit item, not `observe` — observe, per ADR-0029,
        // means a classification ran, and the error path runs none.
        let store = FakeStore::new([10, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(farmed_rows());
        seal_event(
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
        seal_event(
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
    fn the_seal_always_opens_the_event() {
        // There is no held phase any more: with nothing to write per row after
        // the election, the seal write itself is the moment the event opens.
        assert_eq!(sealed_phase(), Phase::Active);
    }

    // --- cross-event contamination with burned indices (issue from PR #159) ---
    //
    // `participant_count` (N) counts indices *issued* — claimed by a shard
    // counter — not rows written. A burned slot (claimed-but-unwritten, from
    // `assign_position`'s `Duplicate` and `Err` branches) makes a clean event
    // read `cohort < N`, and a foreign row that fills a burned slot does not
    // push `cohort` above `N`, so the old `cohort > N` guard missed it. These
    // tests pin the two signals that replace it.

    /// `REPRO_A`: foreign rows duplicating current-event slots while burned
    /// indices keep `cohort == N`. The old guard (`cohort > N`) does not fire
    /// (`5 > 5` is false); the duplicate-slot guard (`cohort > distinct`) does:
    /// distinct pre-queue slots is 3 against 5 cohort rows, so contamination is
    /// flagged and demotion is withheld. This is the test the bug report says
    /// the suite never combined: contamination plus burned indices.
    #[tokio::test]
    async fn burned_indices_do_not_mask_contamination_from_repeated_slots() {
        // counts[0] = 5: five indices 0..4 CLAIMED on shard 0.
        // l=2,3,4 are BURNED (claimed, no row written).
        // CURRENT event only has surviving rows for l=0,1 (address A).
        // Three FOREIGN rows from a previous event remain at l=0,1,2 (also
        // address A); l=0,1 collide with current (duplicate slots), l=2 fills a
        // burned slot.
        let mut rows: Vec<ScannedRow> = Vec::new();
        for l in 0..3u64 {
            rows.push(row(0, l, "198.51.100.1")); // foreign
        }
        for l in 0..2u64 {
            rows.push(row(0, l, "198.51.100.1")); // current
        }
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(rows);
        let result = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:2", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap();
        let SealResult::Sealed(values) = result else {
            panic!("expected a first seal");
        };
        let report = store.report.lock().unwrap().clone().unwrap();
        // Contamination is surfaced, not hidden: the report carries an error.
        assert!(report.error.is_some(), "expected a contamination error");
        assert!(
            report.error.as_deref().unwrap().contains("another event"),
            "expected the error to name another event: {:?}",
            report.error
        );
        // Demotion is withheld: no chunks written, no tail in the seal.
        assert!(values.demotion.is_none());
        assert_eq!(values.demoted_count, 0);
        assert!(store.chunks.lock().unwrap().is_empty());
        // The contaminated group set is still classified for the report (the
        // operator sees what would have been demoted), but not enforced.
        assert_eq!(report.demoted, 5);
        // The event still seals and opens on time.
        assert!(store.written.lock().unwrap().is_some());
    }

    /// `REPRO_B`: foreign rows filling *only* burned slots collide with no
    /// current row, so `cohort == distinct` and the duplicate-slot guard does
    /// not fire either — the scan genuinely cannot tell these foreign rows
    /// from current survivors. The run is therefore *flagged* as
    /// "contamination cannot be ruled out" rather than recorded as clean, and
    /// demotion is not withheld (the burned-index regime is the expected,
    /// throughput-dependent normal case; withholding would disable demotion
    /// at any real scale).
    #[tokio::test]
    async fn burned_slot_fill_surfaces_as_unverifiable_in_the_report() {
        // counts[0] = 5: indices 0..4 claimed. l=2,3,4 BURNED (no current row).
        // CURRENT wrote l=0,1. Two FOREIGN rows remain at l=3,4 — each fills a
        // burned slot, colliding with no current row.
        let rows = vec![
            row(0, 0, "198.51.100.1"), // current
            row(0, 1, "198.51.100.1"), // current
            row(0, 3, "198.51.100.1"), // foreign, fills a burned slot
            row(0, 4, "198.51.100.1"), // foreign, fills a burned slot
        ];
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(rows);
        let SealResult::Sealed(values) = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:2", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap() else {
            panic!("expected seal");
        };
        let report = store.report.lock().unwrap().clone().unwrap();
        // cohort (4) < N (5): the run is flagged, not recorded as clean.
        assert!(report.error.is_some());
        assert!(
            report
                .error
                .as_deref()
                .unwrap()
                .contains("cannot be ruled out"),
            "expected the burned-index regime to be flagged: {:?}",
            report.error
        );
        assert!(
            !report
                .error
                .as_deref()
                .unwrap()
                .contains("nothing was demoted"),
            "the burned-index regime is not a no-demotion outcome: {:?}",
            report.error
        );
        // Demotion is not withheld on the burned-index regime, so the tail is
        // in use and the (mixed) address group is demoted.
        assert!(values.demotion.is_some());
        assert_eq!(store.chunks.lock().unwrap().len(), 1);
        assert_eq!(values.demoted_count, 4);
        assert_eq!(values.queue_counter_start().unwrap(), 10);
    }

    /// A clean event with burned indices and no foreign rows reads
    /// `cohort < N` too — it is indistinguishable from `REPRO_B`, so the report
    /// honestly flags it "cannot be ruled out". Demotion still proceeds: a
    /// burned index is the expected throughput-dependent residual, and the
    /// operator tuned the threshold against real (clean) data.
    #[tokio::test]
    async fn a_clean_event_with_burned_indices_is_flagged_uncertain_but_still_demotes() {
        // counts[0] = 5: indices 0..4 claimed; l=3,4 burned (no row). Three
        // real registrations, all one address, over the threshold of 2.
        let rows = vec![
            row(0, 0, "198.51.100.1"),
            row(0, 1, "198.51.100.1"),
            row(0, 2, "198.51.100.1"),
        ];
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(rows);
        let SealResult::Sealed(values) = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:2", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap() else {
            panic!("expected seal");
        };
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.cohort, 3);
        assert!(report.error.is_some());
        assert!(
            report
                .error
                .as_deref()
                .unwrap()
                .contains("cannot be ruled out")
        );
        // Demotion still enforced on the clean (if burned) cohort.
        assert!(values.demotion.is_some());
        assert_eq!(values.demoted_count, 3);
        assert_eq!(store.chunks.lock().unwrap().len(), 1);
    }

    /// The burned-index flag is mode-independent: under observe the
    /// classification is reported with the same caveat, and nobody is demoted.
    #[tokio::test]
    async fn the_burned_regime_is_flagged_under_observe_too() {
        let rows = vec![
            row(0, 0, "198.51.100.1"),
            row(0, 1, "198.51.100.1"),
            row(0, 2, "198.51.100.1"),
        ];
        let store = FakeStore::new([5, 0, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(rows);
        let SealResult::Sealed(values) = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:2", DemotionMode::Observe),
            1,
        )
        .await
        .unwrap() else {
            panic!("expected seal");
        };
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.mode, "observe");
        assert!(report.error.is_some());
        assert!(
            report
                .error
                .as_deref()
                .unwrap()
                .contains("cannot be ruled out")
        );
        // Observe never enforces.
        assert!(values.demotion.is_none());
        assert_eq!(values.demoted_count, 0);
        assert!(store.chunks.lock().unwrap().is_empty());
    }

    /// Distinct-slot counting is over the `(shard, local_index)` tuple, not
    /// `local_index` alone: a clean full turnout whose shards each issued a
    /// `0, 1` block reads `cohort == distinct == N` and is *not* flagged. A
    /// distinct impl that collapsed on `local_index` would see 3 slots here
    /// against 5 cohort rows and false-positive contamination.
    #[tokio::test]
    async fn distinct_slots_are_counted_per_shard_so_a_clean_multi_shard_turnout_is_clean() {
        // counts = [3, 2, 0, ...]: shard 0 issued l=0,1,2; shard 1 issued
        // l=0,1. Five rows, all one address (over a threshold of 4 so the
        // whole cohort is demoted), all distinct (shard, l) tuples.
        let rows = vec![
            row(0, 0, "198.51.100.1"),
            row(0, 1, "198.51.100.1"),
            row(0, 2, "198.51.100.1"),
            row(1, 0, "198.51.100.1"),
            row(1, 1, "198.51.100.1"),
        ];
        let store = FakeStore::new([3, 2, 0, 0, 0, 0, 0, 0, 0, 0], false).with_rows(rows);
        let SealResult::Sealed(values) = seal_event(
            &store,
            "evt-1",
            [1u8; 32],
            NONCE,
            &config("address:4", DemotionMode::Enforce),
            1,
        )
        .await
        .unwrap() else {
            panic!("expected seal");
        };
        let report = store.report.lock().unwrap().clone().unwrap();
        assert_eq!(report.cohort, 5);
        // cohort == N and cohort == distinct: no flag, no residual caveat.
        assert!(report.error.is_none());
        // A real demotion over a clean cohort still runs.
        assert!(values.demotion.is_some());
        assert_eq!(values.demoted_count, 5);
        assert_eq!(store.chunks.lock().unwrap().len(), 1);
    }
}
