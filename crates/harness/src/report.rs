//! Turns a run into a list of claims, each checked against what happened.
//!
//! Every check reads evidence the run collected itself: the positions each
//! visitor was told, the admissions the origin counted, what the edge fetched,
//! what DynamoDB was asked, and an independent recomputation of the pre-queue
//! order from the values `/status` publishes and the rows in `PreQueue`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use jiff::{SignedDuration, Timestamp};
use serde::Serialize;

use std::time::Duration;

use crate::metrics::Metrics;
use crate::queue::QueueTotals;
use crate::scenario::Scenario;
use crate::visitor::{Outcome, Record};

#[derive(Debug, Serialize)]
pub struct Check {
    pub claim: String,
    pub passed: bool,
    pub evidence: String,
}

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub checks: Vec<Check>,
    /// Whether the harness itself kept up. When it did not, a failed check
    /// may be the harness's, not the design's, so the run is inconclusive.
    pub harness: Vec<Check>,
    pub facts: BTreeMap<String, String>,
    /// Per-second series, for `report.json` only.
    pub series: BTreeMap<String, Vec<u64>>,
}

impl Report {
    pub fn check(&mut self, claim: impl Into<String>, passed: bool, evidence: impl Into<String>) {
        self.checks.push(Check {
            claim: claim.into(),
            passed,
            evidence: evidence.into(),
        });
    }

    pub fn harness_check(
        &mut self,
        claim: impl Into<String>,
        passed: bool,
        evidence: impl Into<String>,
    ) {
        self.harness.push(Check {
            claim: claim.into(),
            passed,
            evidence: evidence.into(),
        });
    }

    #[must_use]
    pub fn conclusive(&self) -> bool {
        self.harness.iter().all(|c| c.passed)
    }

    pub fn fact(&mut self, name: impl Into<String>, value: impl ToString) {
        self.facts.insert(name.into(), value.to_string());
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.passed)
    }

    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "\nFacts");
        for (k, v) in &self.facts {
            let _ = writeln!(out, "  {k:<44} {v}");
        }
        let _ = writeln!(out, "\nChecks");
        for c in &self.checks {
            let mark = if c.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(out, "  [{mark}] {}\n         {}", c.claim, c.evidence);
        }
        let _ = writeln!(out, "\nHarness");
        for c in &self.harness {
            let mark = if c.passed { "OK  " } else { "BAD " };
            let _ = writeln!(out, "  [{mark}] {}\n         {}", c.claim, c.evidence);
        }
        let passed = self.checks.iter().filter(|c| c.passed).count();
        let _ = writeln!(out, "\n{passed}/{} checks passed", self.checks.len());
        if !self.conclusive() {
            let _ = writeln!(
                out,
                "INCONCLUSIVE: the harness did not keep up, so a failure above may be its own. \
                 Lower --concurrency and --visitors, or use --scale."
            );
        }
        out
    }
}

/// A pre-queue row, as an auditor reads it out of the table.
#[derive(Debug, Clone)]
pub struct Registration {
    pub request_id: String,
    pub shard: usize,
    pub local: u64,
}

/// What `/status` published once the event opened.
#[derive(Debug, Clone)]
pub struct Published {
    pub seed: [u8; 32],
    pub participant_count: u64,
    pub offsets: [u64; wr_common::SHARDS],
}

/// Recomputes every registration's position from the published values, the
/// way a third party would: `PRP(seed, offset[s] + l, N)`, with a row at or
/// past its shard's own count a straggler that holds no cohort position.
#[must_use]
pub fn audit(published: &Published, rows: &[Registration]) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    for row in rows {
        let Some(&offset) = published.offsets.get(row.shard) else {
            continue;
        };
        let end = published
            .offsets
            .get(row.shard + 1)
            .copied()
            .unwrap_or(published.participant_count);
        let count = end.saturating_sub(offset);
        if row.local >= count {
            continue;
        }
        let index = offset + row.local;
        out.insert(
            row.request_id.clone(),
            wr_common::prp(
                &wr_common::Seed(published.seed),
                index,
                published.participant_count,
            ),
        );
    }
    out
}

/// Spearman's rank correlation of two equally long samples.
#[must_use]
pub fn spearman(xs: &[f64], ys: &[f64]) -> f64 {
    fn ranks(v: &[f64]) -> Vec<f64> {
        let mut idx: Vec<usize> = (0..v.len()).collect();
        idx.sort_by(|&a, &b| v[a].total_cmp(&v[b]));
        let mut r = vec![0.0; v.len()];
        let mut i = 0;
        while i < idx.len() {
            let mut j = i;
            while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
                j += 1;
            }
            let rank = (i + j) as f64 / 2.0;
            for &k in &idx[i..=j] {
                r[k] = rank;
            }
            i = j + 1;
        }
        r
    }
    let (rx, ry) = (ranks(xs), ranks(ys));
    let n = xs.len() as f64;
    let mx = rx.iter().sum::<f64>() / n;
    let my = ry.iter().sum::<f64>() / n;
    let mut cov = 0.0;
    let mut vx = 0.0;
    let mut vy = 0.0;
    for (a, b) in rx.iter().zip(&ry) {
        cov += (a - mx) * (b - my);
        vx += (a - mx) * (a - mx);
        vy += (b - my) * (b - my);
    }
    if vx == 0.0 || vy == 0.0 {
        return 0.0;
    }
    cov / (vx * vy).sqrt()
}

/// Each session's start and end in epoch seconds, from the end
/// `generate_token` gave it and the session length.
fn spans(ends: &[Timestamp], session: SignedDuration) -> Vec<(i64, i64)> {
    let length = session.as_secs();
    ends.iter()
        .map(|end| (end.as_second().saturating_sub(length), end.as_second()))
        .collect()
}

/// The most sessions live at any instant, each running `session` before the
/// end `generate_token` gave it.
#[must_use]
pub fn peak_live(ends: &[Timestamp], session: SignedDuration) -> u64 {
    let mut edges: Vec<(i64, bool)> = Vec::with_capacity(ends.len() * 2);
    for (start, end) in spans(ends, session) {
        edges.push((start, true));
        edges.push((end, false));
    }
    // An ending at the same second as a start frees its slot first: a session
    // is live over [start, end).
    edges.sort_unstable_by_key(|&(at, starts)| (at, starts));
    let mut live = 0u64;
    let mut peak = 0u64;
    for (_, starts) in edges {
        if starts {
            live += 1;
            peak = peak.max(live);
        } else {
            live = live.saturating_sub(1);
        }
    }
    peak
}

/// Sessions live in each second from the first session's start to the last
/// one's end, and when the series starts.
#[must_use]
pub fn live_by_second(
    ends: &[Timestamp],
    session: SignedDuration,
) -> (Option<Timestamp>, Vec<u64>) {
    let spans = spans(ends, session);
    let (Some(first), Some(last)) = (
        spans.iter().map(|&(start, _)| start).min(),
        spans.iter().map(|&(_, end)| end).max(),
    ) else {
        return (None, Vec::new());
    };
    let mut live = vec![0u64; usize::try_from(last - first).unwrap_or(0)];
    for (start, end) in spans {
        for second in start..end {
            if let Some(n) = usize::try_from(second - first)
                .ok()
                .and_then(|i| live.get_mut(i))
            {
                *n += 1;
            }
        }
    }
    (Timestamp::from_second(first).ok(), live)
}

/// The share of `limit` slots held, second by second, from the first session's
/// start to the last's: how full the origin was kept while anyone waited.
#[must_use]
pub fn occupancy(ends: &[Timestamp], session: SignedDuration, limit: u64) -> Option<f64> {
    let spans = spans(ends, session);
    let first = spans.iter().map(|&(start, _)| start).min()?;
    let last = spans.iter().map(|&(start, _)| start).max()?;
    if last <= first || limit == 0 {
        return None;
    }
    let mut held = 0i64;
    for (start, end) in spans {
        held += (end.min(last) - start.max(first)).max(0);
    }
    Some(held as f64 / ((last - first) as f64 * limit as f64))
}

/// The most admissions in any window of `width` seconds, and where it starts.
#[must_use]
pub fn busiest_window(series: &[u64], width: usize) -> (u64, usize) {
    let mut best = (0, 0);
    if width == 0 || series.is_empty() {
        return best;
    }
    let mut sum: u64 = series.iter().take(width).sum();
    best = (sum, 0);
    for start in 1..series.len().saturating_sub(width).saturating_add(1) {
        sum = sum + series.get(start + width - 1).copied().unwrap_or(0) - series[start - 1];
        if sum > best.0 {
            best = (sum, start);
        }
    }
    best
}

#[must_use]
pub fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted.get(rank).copied()
}

/// Positions told to more than one visitor.
#[must_use]
pub fn duplicate_positions(records: &[Record]) -> Vec<u64> {
    let mut seen = HashSet::new();
    let mut dupes = Vec::new();
    for r in records {
        if let Some(p) = r.position
            && !seen.insert(p)
        {
            dupes.push(p);
        }
    }
    dupes
}

#[must_use]
pub fn outcome_counts(records: &[Record]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for r in records {
        let key = match r.outcome {
            Outcome::Admitted => "admitted".to_owned(),
            Outcome::NoShow => "no-show".to_owned(),
            Outcome::StillWaiting => "still waiting at the end".to_owned(),
            Outcome::GaveUp(why) => format!("gave up: {why}"),
            Outcome::Left => "left when the event ended".to_owned(),
        };
        *out.entry(key).or_default() += 1;
    }
    out
}

/// What a run collected, for the checks every tool makes.
pub struct Evidence<'a> {
    pub scenario: &'a Scenario,
    pub records: &'a [Record],
    pub metrics: &'a Metrics,
    pub queue: QueueTotals,
    /// What `/status` published after the open, if it did.
    pub published: Option<&'a Published>,
    pub registrations: &'a [Registration],
    pub opened: bool,
    /// When the open fired, ms from the start of the run.
    pub open_at_ms: u64,
    pub open_took: Duration,
    pub passes: &'a [Result<controller::PassOutcome, String>],
    pub elapsed: Duration,
    /// Function invocations that returned an error.
    pub failed_invocations: u64,
    /// Error-level log events, by name.
    pub logged_errors: &'a BTreeMap<String, u64>,
}

/// The facts and checks every tool reports.
#[must_use]
pub fn build(e: &Evidence<'_>) -> Report {
    let mut r = Report::default();
    let scenario = e.scenario;
    let seconds = e.elapsed.as_secs();

    r.fact("visitors", scenario.visitors);
    r.fact("origin concurrency", scenario.concurrency);
    r.fact("session length (s)", scenario.session.as_secs());
    r.fact(
        "arrival",
        format!(
            "{:?}, countdown {} s",
            scenario.arrival,
            scenario.countdown.as_secs()
        ),
    );
    r.fact("no-show share", scenario.no_show);
    r.fact("duplicate delivery rate", scenario.duplicate_rate);
    r.fact("API Gateway limits scaled by", scenario.scale);
    r.fact("run length (s)", seconds);
    for (outcome, n) in outcome_counts(e.records) {
        r.fact(format!("outcome: {outcome}"), n);
    }

    // --- positions ----------------------------------------------------------
    let dupes = duplicate_positions(e.records);
    r.check(
        "No two visitors are told the same position",
        dupes.is_empty(),
        format!(
            "{} positions told, {} duplicated{}",
            e.records.iter().filter(|x| x.position.is_some()).count(),
            dupes.len(),
            dupes
                .first()
                .map_or(String::new(), |p| format!(" (first: {p})"))
        ),
    );

    let joined_no_position = e
        .records
        .iter()
        .filter(|x| {
            x.joined_ms.is_some()
                && x.position.is_none()
                && !matches!(x.outcome, Outcome::StillWaiting | Outcome::Left)
        })
        .count();
    let gave_up = e
        .records
        .iter()
        .filter(|x| matches!(x.outcome, Outcome::GaveUp(_)))
        .count();
    r.check(
        "Every visitor who joined is given a position, and no visitor gives up",
        joined_no_position == 0 && gave_up == 0,
        format!("{joined_no_position} joined without a position, {gave_up} gave up"),
    );
    let still = e
        .records
        .iter()
        .filter(|x| x.outcome == Outcome::StillWaiting)
        .count();
    r.check(
        "Every visitor who stays is admitted before the run ends",
        still == 0,
        format!("{still} still waiting at the deadline"),
    );

    // --- the open and the cohort --------------------------------------------
    r.check(
        "The scheduled open runs and writes the cohort",
        e.opened && e.published.is_some(),
        format!(
            "open invoked: {}, took {:.1} s (including the queue drain wait), published: {}",
            e.opened,
            e.open_took.as_secs_f64(),
            e.published.is_some()
        ),
    );
    let behind = e
        .records
        .iter()
        .filter(|x| x.live_join && x.joined_ms.is_some_and(|j| j < e.open_at_ms))
        .count();
    let before = e
        .records
        .iter()
        .filter(|x| x.joined_ms.is_some_and(|j| j < e.open_at_ms))
        .count();
    r.check(
        "Every join accepted before the open is in the cohort",
        behind == 0,
        format!("{before} joins accepted before the open, {behind} numbered behind the cohort"),
    );
    if let Some(published) = e.published {
        let recomputed = audit(published, e.registrations);
        let told: HashMap<&str, u64> = e
            .records
            .iter()
            .filter(|x| !x.live_join)
            .filter_map(|x| x.position.map(|p| (x.request_id.as_str(), p)))
            .collect();
        let mismatched = told
            .iter()
            .filter(|(id, p)| recomputed.get(**id).is_some_and(|q| q != *p))
            .count();
        let distinct: HashSet<u64> = recomputed.values().copied().collect();
        let in_range = recomputed
            .values()
            .all(|&p| p < published.participant_count);
        r.fact("cohort size (published)", published.participant_count);
        r.fact("pre-queue rows", e.registrations.len());
        r.check(
            "Every cohort position recomputes from the published seed and offsets, and they are distinct",
            mismatched == 0 && distinct.len() == recomputed.len() && in_range,
            format!(
                "{} rows recomputed, {} distinct, all < N: {in_range}; {} visitors cross-checked, {mismatched} disagree",
                recomputed.len(),
                distinct.len(),
                told.len()
            ),
        );
        let cohort: Vec<&Record> = e
            .records
            .iter()
            .filter(|x| !x.live_join && x.position.is_some() && x.joined_ms.is_some())
            .collect();
        if cohort.len() >= 100 {
            let xs: Vec<f64> = cohort
                .iter()
                .map(|x| x.joined_ms.unwrap_or(0) as f64)
                .collect();
            let ys: Vec<f64> = cohort
                .iter()
                .map(|x| x.position.unwrap_or(0) as f64)
                .collect();
            let rho = spearman(&xs, &ys);
            let bound = 4.0 / (cohort.len() as f64).sqrt();
            r.check(
                "A cohort position is uncorrelated with when the visitor registered",
                rho.abs() < bound,
                format!(
                    "Spearman rho {rho:+.4} over {} visitors (bound ±{bound:.4})",
                    cohort.len()
                ),
            );
        }
    }

    // --- admission ----------------------------------------------------------
    let admissions = e.metrics.origin_series("protected", seconds);
    let total_admitted: u64 = admissions.iter().sum();
    r.fact("admissions reaching the origin", total_admitted);
    let (busiest, start) = busiest_window(&admissions, 1);
    r.fact(
        "busiest second of admissions",
        format!("{busiest} (from t={start} s)"),
    );
    let sessions: Vec<Timestamp> = e.records.iter().filter_map(|x| x.session_ends).collect();
    let peak = peak_live(&sessions, scenario.session);
    let (first_start, live) = live_by_second(&sessions, scenario.session);
    r.fact(
        "first session start",
        first_start.map_or_else(|| "none".to_owned(), |t| t.to_string()),
    );
    r.series.insert("live sessions per second".to_owned(), live);
    r.series
        .insert("admissions per second".to_owned(), admissions.clone());
    let limit = u64::from(scenario.concurrency);
    r.check(
        "No more sessions are ever live than the origin takes",
        peak <= limit,
        format!("most live at once: {peak} against {limit}"),
    );
    // While visitors are still waiting, the origin should be close to full.
    // It runs a little under at each turnover: a slot is seen free up to a
    // pass after its session ends, and its replacement has to poll in.
    if let Some(occupancy) = occupancy(&sessions, scenario.session, limit) {
        r.check(
            "The origin stays near its limit while visitors are waiting",
            occupancy >= 0.85,
            format!(
                "{:.0}% of the slots held, from the first admission to the last",
                occupancy * 100.0
            ),
        );
    }

    // --- the edge -----------------------------------------------------------
    let status_origin = e.metrics.origin_series("/_wr/v1/status", seconds);
    let max_status = status_origin.iter().copied().max().unwrap_or(0);
    r.check(
        "/status reaches its origin about once a second however many are waiting",
        max_status <= 2,
        format!(
            "busiest second: {max_status} origin fetches; {} in total over {seconds} s",
            status_origin.iter().sum::<u64>()
        ),
    );
    r.check(
        "The gate never passes a request because it failed",
        e.metrics.gate_failed_open_count() == 0,
        format!("{} fail-open passes", e.metrics.gate_failed_open_count()),
    );
    r.fact(
        "peak visitor requests per second",
        e.metrics.peak_viewer_rate(),
    );
    for ((route, status), n) in e.metrics.viewer_totals() {
        r.fact(format!("requests {route} {status}"), n);
    }
    for (route, n) in e.metrics.throttled_totals() {
        r.fact(format!("API Gateway 429s on {route}"), n);
    }
    let mut known: Vec<u64> = e
        .records
        .iter()
        .filter_map(|x| Some(x.position_known_ms?.saturating_sub(x.arrived_ms)))
        .collect();
    known.sort_unstable();
    let mut admitted: Vec<u64> = e
        .records
        .iter()
        .filter_map(|x| Some(x.admitted_ms?.saturating_sub(x.arrived_ms)))
        .collect();
    admitted.sort_unstable();
    for (name, series) in [
        ("arrival to position", &known),
        ("arrival to admission", &admitted),
    ] {
        for p in [0.5, 0.9, 0.99] {
            if let Some(v) = percentile(series, p) {
                r.fact(
                    format!("{name} p{:.0} (s)", p * 100.0),
                    format!("{:.1}", v as f64 / 1000.0),
                );
            }
        }
    }

    // --- the backend --------------------------------------------------------
    r.check(
        "No accepted join is dead-lettered",
        e.queue.dead_lettered == 0,
        format!(
            "{} sent, {} delivered twice, {} dead-lettered, {} left",
            e.queue.sent, e.queue.duplicated, e.queue.dead_lettered, e.queue.left
        ),
    );
    let failed_passes = e.passes.iter().filter(|p| p.is_err()).count();
    r.fact(
        "controller passes",
        format!("{} ({} failed)", e.passes.len(), failed_passes),
    );
    r.check(
        "No function invocation fails and no function logs an error",
        e.failed_invocations == 0 && e.logged_errors.is_empty() && failed_passes == 0,
        format!(
            "{} failed invocations, {failed_passes} failed controller passes, logged errors: {}",
            e.failed_invocations,
            if e.logged_errors.is_empty() {
                "none".to_owned()
            } else {
                e.logged_errors
                    .iter()
                    .map(|(k, v)| format!("{k} x{v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ),
    );
    r
}

/// The open's values off a `/status` document, once it publishes them.
#[must_use]
pub fn parse_published(status: &serde_json::Value) -> Option<Published> {
    let seed_hex = status.get("shuffle_seed")?.as_str()?;
    let bytes = hex_decode(seed_hex)?;
    let seed: [u8; 32] = bytes.try_into().ok()?;
    let participant_count = status.get("participant_count")?.as_u64()?;
    let offsets_v = status.get("prequeue_offsets")?.as_array()?;
    let mut offsets = [0u64; wr_common::SHARDS];
    for (slot, v) in offsets.iter_mut().zip(offsets_v) {
        *slot = v.as_u64()?;
    }
    Some(Published {
        seed,
        participant_count,
        offsets,
    })
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_orders_correlate_perfectly_and_reversed_ones_negatively() {
        let x = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((spearman(&x, &x) - 1.0).abs() < 1e-12);
        let y = [5.0, 4.0, 3.0, 2.0, 1.0];
        assert!((spearman(&x, &y) + 1.0).abs() < 1e-12);
    }

    #[test]
    fn sessions_ending_as_others_start_do_not_overlap() {
        // Two back-to-back generations of two sessions each.
        assert_eq!(
            peak_live(
                &[
                    Timestamp::constant(110, 0),
                    Timestamp::constant(110, 0),
                    Timestamp::constant(120, 0),
                    Timestamp::constant(120, 0)
                ],
                SignedDuration::from_secs(10)
            ),
            2
        );
        assert_eq!(
            peak_live(
                &[
                    Timestamp::constant(110, 0),
                    Timestamp::constant(110, 0),
                    Timestamp::constant(119, 0),
                    Timestamp::constant(120, 0)
                ],
                SignedDuration::from_secs(10)
            ),
            3
        );
        assert_eq!(peak_live(&[], SignedDuration::from_secs(10)), 0);
    }

    #[test]
    fn a_full_origin_is_fully_occupied() {
        // Two slots, refilled exactly as each session ends.
        let full = [
            Timestamp::constant(110, 0),
            Timestamp::constant(110, 0),
            Timestamp::constant(120, 0),
            Timestamp::constant(120, 0),
            Timestamp::constant(130, 0),
            Timestamp::constant(130, 0),
        ];
        assert_eq!(
            occupancy(&full, SignedDuration::from_secs(10), 2),
            Some(1.0)
        );
        let half = [
            Timestamp::constant(110, 0),
            Timestamp::constant(120, 0),
            Timestamp::constant(130, 0),
        ];
        assert_eq!(
            occupancy(&half, SignedDuration::from_secs(10), 2),
            Some(0.5)
        );
    }

    #[test]
    fn the_busiest_window_is_found_anywhere_in_the_series() {
        let s = [1, 0, 5, 5, 0, 1];
        assert_eq!(busiest_window(&s, 2), (10, 2));
        assert_eq!(busiest_window(&s, 1), (5, 2));
        assert_eq!(busiest_window(&s, 10), (12, 0));
    }

    #[test]
    fn the_audit_skips_stragglers_and_keeps_positions_distinct() {
        let published = Published {
            seed: [7u8; 32],
            participant_count: 6,
            offsets: [0, 3, 3, 6, 6, 6, 6, 6, 6, 6],
        };
        let mut rows = Vec::new();
        for l in 0..4 {
            rows.push(Registration {
                request_id: format!("a{l}"),
                shard: 0,
                local: l,
            });
        }
        for l in 0..3 {
            rows.push(Registration {
                request_id: format!("c{l}"),
                shard: 2,
                local: l,
            });
        }
        let positions = audit(&published, &rows);
        // a3 is past shard 0's count of 3.
        assert_eq!(positions.len(), 6);
        assert!(!positions.contains_key("a3"));
        let distinct: HashSet<u64> = positions.values().copied().collect();
        assert_eq!(distinct.len(), 6);
        assert!(distinct.iter().all(|&p| p < 6));
    }
}
