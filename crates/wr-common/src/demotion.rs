//! Seal-time demotion (issue #145, requirement F6.3): classifying the
//! pre-queue cohort by its join-time telemetry and moving whole groups of
//! registrations to the tail of the queue at the seal.
//!
//! Registration is compute-free and `request_id` is client-supplied, so
//! nothing bounds how many positions one party takes; randomization converts
//! that volume into expected share of the front of the queue linearly. What a
//! farm cannot easily vary is what every registration reports on the way in:
//! the viewer address, its ASN, the TLS stack's JA4 fingerprint, the user
//! agent. Thousands of registrations collapsing onto one value of any of those
//! is the signature this module acts on.
//!
//! Acting at the seal, not at join, is the point: nothing is written or
//! revealed while there is still time to retool and re-register, and a group
//! is judged on its whole pre-queue footprint rather than a moving window.
//!
//! The mitigation is a **demotion to a compact tail**, never a block. A rule
//! that catches a farm also catches an office NAT or a campus network, and a
//! demoted office still gets in — after everyone the rules did not touch. The
//! tail is compact rather than sparse (each demoted row takes a fresh tail
//! index `d` in `[0, D)`) so the controller does not have to sweep `N`
//! positions of mostly-gaps to reach the live joiners behind it.
//!
//! Every rule here is a **count threshold** chosen by the operator, applied to
//! one signal: a group whose registrations exceed the threshold is demoted
//! whole. Nothing is enabled by default, and `Observe` mode runs the whole
//! classification and writes the report without demoting anyone, which is how
//! a threshold is measured against a real event before it is trusted.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::items::Telemetry;

/// A join-time signal a demotion rule groups registrations by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    /// The viewer's IP address, `CloudFront-Viewer-Address` with the source
    /// port removed — the port varies per connection, the address is the
    /// group.
    Address,
    /// The viewer's autonomous system number, `CloudFront-Viewer-ASN`.
    Asn,
    /// The TLS client fingerprint, `CloudFront-Viewer-JA4-Fingerprint`. Note
    /// that every honest user of one browser release shares one value: this
    /// separates tooling from browsers, not one visitor from another.
    Ja4,
    /// The `User-Agent` string as stored (truncated to 256 bytes at join).
    UserAgent,
}

impl Signal {
    /// The name a rule uses on the wire and the report records.
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            Signal::Address => "address",
            Signal::Asn => "asn",
            Signal::Ja4 => "ja4",
            Signal::UserAgent => "ua",
        }
    }

    /// The registration's value for this signal, or `None` when the row did
    /// not report it — an untelemetered row belongs to no group and is never
    /// demoted.
    #[must_use]
    pub fn extract(self, telemetry: &Telemetry) -> Option<&str> {
        match self {
            Signal::Address => telemetry.a.as_deref().map(strip_port),
            Signal::Asn => telemetry.n.as_deref(),
            Signal::Ja4 => telemetry.j.as_deref(),
            Signal::UserAgent => telemetry.u.as_deref(),
        }
    }
}

impl std::str::FromStr for Signal {
    type Err = RuleParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "address" => Ok(Signal::Address),
            "asn" => Ok(Signal::Asn),
            "ja4" => Ok(Signal::Ja4),
            "ua" => Ok(Signal::UserAgent),
            other => Err(RuleParseError::UnknownSignal(other.to_owned())),
        }
    }
}

/// `CloudFront-Viewer-Address` is `address:port` for both IP versions, with no
/// brackets around an IPv6 address, so the port is everything after the last
/// colon. An address with no colon at all is returned whole.
fn strip_port(address: &str) -> &str {
    match address.rsplit_once(':') {
        Some((ip, _port)) => ip,
        None => address,
    }
}

/// One demotion rule: every group of registrations sharing a value of
/// `signal` whose size exceeds `max` is demoted whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemotionRule {
    pub signal: Signal,
    /// The largest group size left alone. A group of `max + 1` or more is
    /// demoted.
    pub max: u64,
}

/// Why a rules string could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleParseError {
    #[error("unknown signal {0:?}; expected address, asn, ja4 or ua")]
    UnknownSignal(String),
    #[error("rule {0:?} is not of the form signal:max")]
    Malformed(String),
    #[error("rule {0:?} has a threshold that is not a whole number")]
    BadThreshold(String),
    #[error(
        "rule {0:?} has a threshold of 0, which would demote every registration reporting that signal"
    )]
    ZeroThreshold(String),
    #[error("signal {0} appears in more than one rule")]
    Duplicate(&'static str),
}

/// The operator's demotion rules, parsed from the `signal:max` list Terraform
/// sets (`address:25,asn:5000`). Empty means the feature is off and the seal
/// never scans the cohort.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DemotionRules(Vec<DemotionRule>);

impl DemotionRules {
    /// Parses a comma-separated list of `signal:max` rules. Whitespace around
    /// entries is ignored and an empty (or all-whitespace) string is no rules.
    ///
    /// # Errors
    ///
    /// Any entry that is not `signal:max` with a known signal and a threshold
    /// of at least 1, or a signal that appears twice.
    pub fn parse(text: &str) -> Result<Self, RuleParseError> {
        let mut rules: Vec<DemotionRule> = Vec::new();
        for entry in text.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((signal, max)) = entry.split_once(':') else {
                return Err(RuleParseError::Malformed(entry.to_owned()));
            };
            let signal: Signal = signal.trim().parse()?;
            let max: u64 = max
                .trim()
                .parse()
                .map_err(|_err| RuleParseError::BadThreshold(entry.to_owned()))?;
            if max == 0 {
                return Err(RuleParseError::ZeroThreshold(entry.to_owned()));
            }
            if rules.iter().any(|r| r.signal == signal) {
                return Err(RuleParseError::Duplicate(signal.as_wire_str()));
            }
            rules.push(DemotionRule { signal, max });
        }
        Ok(Self(rules))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[DemotionRule] {
        &self.0
    }

    /// The canonical `signal:max` form, for the report.
    #[must_use]
    pub fn to_wire_string(&self) -> String {
        let mut parts = Vec::with_capacity(self.0.len());
        for rule in &self.0 {
            parts.push(format!("{}:{}", rule.signal.as_wire_str(), rule.max));
        }
        parts.join(",")
    }
}

/// Whether a classification is acted on. `Observe` runs the whole
/// classification and writes the report but demotes nobody — the
/// count-then-block discipline (requirement O5) applied to a fairness control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DemotionMode {
    #[default]
    Observe,
    Enforce,
}

impl DemotionMode {
    #[must_use]
    pub fn as_wire_str(self) -> &'static str {
        match self {
            DemotionMode::Observe => "observe",
            DemotionMode::Enforce => "enforce",
        }
    }
}

/// An unrecognised demotion mode string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown demotion mode {0:?}; expected observe or enforce")]
pub struct UnknownMode(pub String);

impl std::str::FromStr for DemotionMode {
    type Err = UnknownMode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "observe" => Ok(DemotionMode::Observe),
            "enforce" => Ok(DemotionMode::Enforce),
            other => Err(UnknownMode(other.to_owned())),
        }
    }
}

/// The most rules a cohort row can belong to: one group per signal.
const MAX_RULES: usize = 4;

/// One scanned cohort row: its id and, per rule, the interned id of the group
/// it fell into (`None` when the row did not report that signal).
struct CohortRow {
    request_id: String,
    groups: [Option<u32>; MAX_RULES],
}

/// Accumulates the sealed cohort during the seal's scan, then classifies it.
///
/// Group values are interned once and rows carry only interned ids, so a
/// million-row cohort costs roughly its request ids plus a few words per row
/// in memory rather than a copy of every telemetry string.
pub struct Cohort {
    rules: DemotionRules,
    rows: Vec<CohortRow>,
    /// `(rule index, value)` to interned group id.
    interner: HashMap<(usize, String), u32>,
    /// Interned group id to `(rule index, value)`.
    keys: Vec<(usize, String)>,
    /// Registrations per interned group id.
    counts: Vec<u64>,
}

impl Cohort {
    #[must_use]
    pub fn new(rules: DemotionRules) -> Self {
        Self {
            rules,
            rows: Vec::new(),
            interner: HashMap::new(),
            keys: Vec::new(),
            counts: Vec::new(),
        }
    }

    /// Records one cohort row. Callers pass only rows the sealed offsets place
    /// inside the cohort; a straggler that raced the seal is a live joiner and
    /// has no pre-queue position to demote.
    pub fn observe(&mut self, request_id: &str, telemetry: Option<&Telemetry>) {
        let mut groups = [None; MAX_RULES];
        if let Some(telemetry) = telemetry {
            let rules: Vec<DemotionRule> = self
                .rules
                .as_slice()
                .iter()
                .copied()
                .take(MAX_RULES)
                .collect();
            for (rule_index, rule) in rules.into_iter().enumerate() {
                let Some(value) = rule.signal.extract(telemetry) else {
                    continue;
                };
                let id = self.intern(rule_index, value);
                if let Some(count) = self.counts.get_mut(id as usize) {
                    *count = count.saturating_add(1);
                }
                groups[rule_index] = Some(id);
            }
        }
        self.rows.push(CohortRow {
            request_id: request_id.to_owned(),
            groups,
        });
    }

    fn intern(&mut self, rule_index: usize, value: &str) -> u32 {
        if let Some(&id) = self.interner.get(&(rule_index, value.to_owned())) {
            return id;
        }
        let id = u32::try_from(self.keys.len()).unwrap_or(u32::MAX);
        self.interner.insert((rule_index, value.to_owned()), id);
        self.keys.push((rule_index, value.to_owned()));
        self.counts.push(0);
        id
    }

    /// Rows observed so far.
    #[must_use]
    pub fn len(&self) -> u64 {
        u64::try_from(self.rows.len()).unwrap_or(u64::MAX)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Applies the rules: every group over its rule's threshold is demoted,
    /// and every row in any demoted group is listed for the tail.
    #[must_use]
    pub fn classify(self) -> Classification {
        let mut demoted_group = vec![false; self.keys.len()];
        let mut groups = Vec::new();
        for (id, (rule_index, value)) in self.keys.iter().enumerate() {
            let Some(rule) = self.rules.as_slice().get(*rule_index) else {
                continue;
            };
            let count = self.counts.get(id).copied().unwrap_or(0);
            if count > rule.max {
                demoted_group[id] = true;
                groups.push(DemotedGroup {
                    signal: rule.signal,
                    value: value.clone(),
                    count,
                    max: rule.max,
                });
            }
        }
        // Largest first, then by value so the order is total and the report
        // is stable across runs.
        groups.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.signal.as_wire_str().cmp(b.signal.as_wire_str()))
                .then_with(|| a.value.cmp(&b.value))
        });

        let cohort = self.len();
        let mut demoted = Vec::new();
        for row in self.rows {
            let hit = row
                .groups
                .iter()
                .flatten()
                .any(|&id| demoted_group.get(id as usize).copied().unwrap_or(false));
            if hit {
                demoted.push(row.request_id);
            }
        }
        // Sorted so the tail index a row receives does not depend on the
        // scan's segment interleaving; the permutation over the tail
        // randomizes the order regardless.
        demoted.sort_unstable();

        Classification {
            cohort,
            groups,
            demoted,
        }
    }
}

/// One group the rules demoted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemotedGroup {
    pub signal: Signal,
    pub value: String,
    /// Registrations in the group.
    pub count: u64,
    /// The rule's threshold it exceeded.
    pub max: u64,
}

/// The outcome of classifying a cohort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    /// Cohort rows observed.
    pub cohort: u64,
    /// Demoted groups, largest first.
    pub groups: Vec<DemotedGroup>,
    /// Request ids of every row in a demoted group, sorted. A row's index in
    /// this list is the tail index `d` it is written with under `Enforce`.
    pub demoted: Vec<String>,
}

impl Classification {
    /// `D`, the tail's size.
    #[must_use]
    pub fn demoted_count(&self) -> u64 {
        u64::try_from(self.demoted.len()).unwrap_or(u64::MAX)
    }
}

/// The most groups the report item lists. The report exists so the operator
/// can see what was demoted and why; a farm across ten thousand addresses is
/// legible from its largest groups and its total, and an unbounded list would
/// grow the item with the attack.
pub const MAX_REPORT_GROUPS: usize = 200;

/// One row of the report's group table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportGroup {
    pub signal: String,
    pub value: String,
    pub count: u64,
    pub max: u64,
}

/// What the seal decided and why, written to its own `Counters`-table item
/// (`EVT#{event_id}#DM`, [`crate::expr::Key::DemotionReport`]) rather than the
/// event item: the event item is read by every poll and must stay small, and
/// only the operator's dashboard reads this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemotionReport {
    /// `observe` or `enforce`.
    pub mode: String,
    /// The rules in effect, canonical `signal:max` form.
    pub rules: String,
    /// Cohort rows the seal classified.
    pub cohort: u64,
    /// Rows in a demoted group: demoted under `enforce`, *would have been*
    /// under `observe`.
    pub demoted: u64,
    /// Demoted groups in total, of which at most [`MAX_REPORT_GROUPS`] are
    /// listed in `groups`.
    pub groups_total: u64,
    /// The largest demoted groups.
    pub groups: Vec<ReportGroup>,
    /// When the seal ran, epoch seconds.
    pub sealed_at: u64,
    /// Set when the rules could not be parsed at all: the seal then ran with no
    /// demotion rather than not at all, and this says so.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub error: Option<String>,
}

impl DemotionReport {
    /// Builds the report for a classification.
    #[must_use]
    pub fn from_classification(
        mode: DemotionMode,
        rules: &DemotionRules,
        classification: &Classification,
        sealed_at: u64,
    ) -> Self {
        let mut groups = Vec::with_capacity(classification.groups.len().min(MAX_REPORT_GROUPS));
        for group in classification.groups.iter().take(MAX_REPORT_GROUPS) {
            groups.push(ReportGroup {
                signal: group.signal.as_wire_str().to_owned(),
                value: group.value.clone(),
                count: group.count,
                max: group.max,
            });
        }
        Self {
            mode: mode.as_wire_str().to_owned(),
            rules: rules.to_wire_string(),
            cohort: classification.cohort,
            demoted: classification.demoted_count(),
            groups_total: u64::try_from(classification.groups.len()).unwrap_or(u64::MAX),
            groups,
            sealed_at,
            error: None,
        }
    }

    /// The report for a seal whose rules string could not be parsed.
    #[must_use]
    pub fn from_error(rules_text: &str, error: &RuleParseError, sealed_at: u64) -> Self {
        Self {
            mode: DemotionMode::Observe.as_wire_str().to_owned(),
            rules: rules_text.to_owned(),
            cohort: 0,
            demoted: 0,
            groups_total: 0,
            groups: Vec::new(),
            sealed_at,
            error: Some(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    fn telemetry(address: &str, asn: &str, ja4: &str, ua: &str) -> Telemetry {
        Telemetry {
            a: Some(address.to_owned()),
            n: Some(asn.to_owned()),
            c: Some("US".to_owned()),
            j: Some(ja4.to_owned()),
            u: Some(ua.to_owned()),
            q: None,
        }
    }

    #[test]
    fn rules_parse_the_documented_form_and_round_trip() {
        let rules = DemotionRules::parse(" address:25, asn:5000 ,ja4:10000,ua:1 ").unwrap();
        assert_eq!(
            rules.as_slice(),
            &[
                DemotionRule {
                    signal: Signal::Address,
                    max: 25
                },
                DemotionRule {
                    signal: Signal::Asn,
                    max: 5000
                },
                DemotionRule {
                    signal: Signal::Ja4,
                    max: 10_000
                },
                DemotionRule {
                    signal: Signal::UserAgent,
                    max: 1
                },
            ]
        );
        assert_eq!(rules.to_wire_string(), "address:25,asn:5000,ja4:10000,ua:1");
    }

    #[test]
    fn an_empty_rules_string_is_no_rules() {
        assert!(DemotionRules::parse("").unwrap().is_empty());
        assert!(DemotionRules::parse("  , ,").unwrap().is_empty());
    }

    #[test]
    fn malformed_rules_are_rejected_not_ignored() {
        // Silently dropping a bad rule would leave the operator believing a
        // control is armed that is not.
        assert_eq!(
            DemotionRules::parse("address"),
            Err(RuleParseError::Malformed("address".to_owned()))
        );
        assert_eq!(
            DemotionRules::parse("country:5"),
            Err(RuleParseError::UnknownSignal("country".to_owned()))
        );
        assert_eq!(
            DemotionRules::parse("address:many"),
            Err(RuleParseError::BadThreshold("address:many".to_owned()))
        );
        assert_eq!(
            DemotionRules::parse("address:0"),
            Err(RuleParseError::ZeroThreshold("address:0".to_owned()))
        );
        assert_eq!(
            DemotionRules::parse("address:5,address:6"),
            Err(RuleParseError::Duplicate("address"))
        );
    }

    #[test]
    fn the_address_signal_drops_the_port_for_both_ip_versions() {
        let v4 = telemetry("203.0.113.9:51234", "1", "j", "u");
        let v6 = telemetry("2001:db8::1:51234", "1", "j", "u");
        assert_eq!(Signal::Address.extract(&v4), Some("203.0.113.9"));
        assert_eq!(Signal::Address.extract(&v6), Some("2001:db8::1"));
        let bare = telemetry("203.0.113.9", "1", "j", "u");
        assert_eq!(Signal::Address.extract(&bare), Some("203.0.113.9"));
    }

    #[test]
    fn a_group_over_its_threshold_is_demoted_whole_and_others_are_not() {
        let rules = DemotionRules::parse("address:2").unwrap();
        let mut cohort = Cohort::new(rules);
        // Three from one address (over 2), two from another (not over), one
        // untelemetered row.
        for (i, port) in [1, 2, 3].iter().enumerate() {
            cohort.observe(
                &format!("farm-{i}"),
                Some(&telemetry(
                    &format!("198.51.100.1:{port}"),
                    "64500",
                    "j",
                    "u",
                )),
            );
        }
        for i in 0..2 {
            cohort.observe(
                &format!("office-{i}"),
                Some(&telemetry("203.0.113.7:9", "64501", "j", "u")),
            );
        }
        cohort.observe("blank", None);

        let classification = cohort.classify();
        assert_eq!(classification.cohort, 6);
        assert_eq!(
            classification.groups,
            vec![DemotedGroup {
                signal: Signal::Address,
                value: "198.51.100.1".to_owned(),
                count: 3,
                max: 2,
            }]
        );
        assert_eq!(classification.demoted, vec!["farm-0", "farm-1", "farm-2"]);
        assert_eq!(classification.demoted_count(), 3);
    }

    #[test]
    fn a_row_in_two_demoted_groups_is_listed_once() {
        let rules = DemotionRules::parse("address:1,asn:1").unwrap();
        let mut cohort = Cohort::new(rules);
        cohort.observe("a", Some(&telemetry("1.1.1.1:1", "64500", "j", "u")));
        cohort.observe("b", Some(&telemetry("1.1.1.1:2", "64500", "j", "u")));
        let classification = cohort.classify();
        assert_eq!(classification.groups.len(), 2);
        assert_eq!(classification.demoted, vec!["a", "b"]);
    }

    #[test]
    fn groups_are_reported_largest_first_and_capped() {
        let rules = DemotionRules::parse("ua:1").unwrap();
        let mut cohort = Cohort::new(rules);
        for g in 0..(MAX_REPORT_GROUPS + 5) {
            // Group g has g + 2 members, so later groups are larger.
            for m in 0..(g + 2) {
                cohort.observe(
                    &format!("r-{g}-{m}"),
                    Some(&telemetry("1.1.1.1:1", "1", "j", &format!("agent-{g}"))),
                );
            }
        }
        let classification = cohort.classify();
        assert_eq!(classification.groups.len(), MAX_REPORT_GROUPS + 5);
        assert!(
            classification
                .groups
                .windows(2)
                .all(|w| w[0].count >= w[1].count)
        );
        let report = DemotionReport::from_classification(
            DemotionMode::Enforce,
            &DemotionRules::parse("ua:1").unwrap(),
            &classification,
            1,
        );
        assert_eq!(report.groups.len(), MAX_REPORT_GROUPS);
        assert_eq!(report.groups_total, (MAX_REPORT_GROUPS + 5) as u64);
        assert_eq!(
            report.groups[0].value,
            format!("agent-{}", MAX_REPORT_GROUPS + 4)
        );
        assert_eq!(report.mode, "enforce");
        assert_eq!(report.rules, "ua:1");
        assert_eq!(report.demoted, classification.demoted_count());
    }

    #[test]
    fn no_rules_means_nothing_is_demoted_whatever_the_cohort_looks_like() {
        let mut cohort = Cohort::new(DemotionRules::default());
        for i in 0..100 {
            cohort.observe(
                &format!("r{i}"),
                Some(&telemetry("1.1.1.1:1", "1", "j", "u")),
            );
        }
        let classification = cohort.classify();
        assert!(classification.groups.is_empty());
        assert!(classification.demoted.is_empty());
        assert_eq!(classification.cohort, 100);
    }

    #[test]
    fn report_round_trips_through_attribute_values() {
        let report = DemotionReport {
            mode: "observe".to_owned(),
            rules: "address:25".to_owned(),
            cohort: 10,
            demoted: 3,
            groups_total: 1,
            groups: vec![ReportGroup {
                signal: "address".to_owned(),
                value: "198.51.100.1".to_owned(),
                count: 3,
                max: 25,
            }],
            sealed_at: 1_788_000_000,
            error: None,
        };
        let av: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            serde_dynamo::to_item(&report).unwrap();
        assert!(!av.contains_key("error"));
        let back: DemotionReport = serde_dynamo::from_item(av).unwrap();
        assert_eq!(report, back);
    }

    #[test]
    fn mode_parses_its_two_values_and_defaults_to_observe() {
        assert_eq!("observe".parse::<DemotionMode>(), Ok(DemotionMode::Observe));
        assert_eq!("enforce".parse::<DemotionMode>(), Ok(DemotionMode::Enforce));
        assert!("block".parse::<DemotionMode>().is_err());
        assert_eq!(DemotionMode::default(), DemotionMode::Observe);
    }
}
