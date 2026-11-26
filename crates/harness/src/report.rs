//! Turns a run into a list of claims, each checked against what happened.
//!
//! Every check reads evidence the run collected itself: the positions each
//! visitor was told, the admissions the origin counted, what the edge fetched,
//! what DynamoDB was asked, and an independent recomputation of the pre-queue
//! order from the values `/status` publishes and the rows in `PreQueue`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;

use serde::Serialize;

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

/// The most sessions live at any instant, each running the `session` seconds
/// before the end `generate_token` gave it.
#[must_use]
pub fn peak_live(ends: &[u64], session: u64) -> u64 {
    let mut edges: Vec<(u64, bool)> = Vec::with_capacity(ends.len() * 2);
    for &end in ends {
        edges.push((end.saturating_sub(session), true));
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
/// one's end, and the epoch second the series starts at.
#[must_use]
pub fn live_by_second(ends: &[u64], session: u64) -> (u64, Vec<u64>) {
    let (Some(&first_end), Some(&last)) = (ends.iter().min(), ends.iter().max()) else {
        return (0, Vec::new());
    };
    let first = first_end.saturating_sub(session);
    let mut live = vec![0u64; usize::try_from(last - first).unwrap_or(0)];
    for &end in ends {
        let start = end.saturating_sub(session);
        for second in start..end {
            if let Some(n) = usize::try_from(second - first)
                .ok()
                .and_then(|i| live.get_mut(i))
            {
                *n += 1;
            }
        }
    }
    (first, live)
}

/// The share of `limit` slots held, second by second, from the first session's
/// start to the last's: how full the origin was kept while anyone waited.
#[must_use]
pub fn occupancy(ends: &[u64], session: u64, limit: u64) -> Option<f64> {
    let first = ends.iter().min()?.saturating_sub(session);
    let last = ends.iter().max()?.saturating_sub(session);
    if last <= first || limit == 0 {
        return None;
    }
    let mut held = 0u64;
    for &end in ends {
        let start = end.saturating_sub(session);
        held += end.min(last).saturating_sub(start.max(first));
    }
    Some(held as f64 / ((last - first) * limit) as f64)
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
        };
        *out.entry(key).or_default() += 1;
    }
    out
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
        assert_eq!(peak_live(&[110, 110, 120, 120], 10), 2);
        assert_eq!(peak_live(&[110, 110, 119, 120], 10), 3);
        assert_eq!(peak_live(&[], 10), 0);
    }

    #[test]
    fn a_full_origin_is_fully_occupied() {
        // Two slots, refilled exactly as each session ends.
        let ends = [110, 110, 120, 120, 130, 130];
        assert_eq!(occupancy(&ends, 10, 2), Some(1.0));
        let half = [110, 120, 130];
        assert_eq!(occupancy(&half, 10, 2), Some(0.5));
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
