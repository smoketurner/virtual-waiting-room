//! Pre-event readiness (issue #70): the diagnostics panel at the top of the
//! dashboard, and the checklist in `docs/RUNBOOK.md` generated from the same
//! list.
//!
//! Every check is a read of the running deployment compared against what the
//! requirement it protects needs. The panel is diagnostics only: nothing here
//! writes, and every remedy is a link out. A check whose read fails or times
//! out renders as "could not evaluate: <reason>" rather than failing the
//! dashboard, because the operator still needs the controls below it.
//!
//! The judgement is pure — a measured value in, a [`Status`] and the text
//! the operator reads out — so it is tested without AWS. The reads go through
//! [`ReadinessProbe`], whose SDK-backed implementation lives in `probe`, and
//! through the existing [`EdgeConfigStore`] for the gate's ruleset.

use std::fmt::Write as _;
use std::future::Future;
use std::time::Duration;

use crate::{EdgeConfigStore, GateConfig};

/// How long one check may take before it is reported as not evaluable. The
/// checks run concurrently, so this bounds the whole panel as well; it is
/// short because the dashboard waits for it.
pub const PER_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// `DynamoDB`'s default per-table throughput quota, in units per second. At or
/// below it no increase has been granted for this account.
pub const DEFAULT_TABLE_THROUGHPUT_LIMIT: i64 = 40_000;

/// API Gateway's default account-level steady-state rate, requests/second.
pub const DEFAULT_API_GATEWAY_RATE: f64 = 10_000.0;

/// The registration rate C3 sizes the live path for. A `PreQueue` table
/// warmed below it ramps on demand under exactly the burst it exists for —
/// the 4,000-unit AWS minimum is a floor, not a sized value.
pub const REGISTRATION_TARGET_PER_SECOND: i64 = 10_000;

/// One row of the panel, in display order. Each names the requirement it
/// protects through [`CheckId::spec`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckId {
    WarmThroughput,
    DynamoDbLimits,
    ApiGatewayThrottle,
    AssignPositionConcurrency,
    OpenSchedule,
    ControllerSchedule,
    GateRuleset,
    StatusCaching,
    GateAssociation,
}

/// What a check is, which requirement it protects, and how to fix it. The
/// single source for both the panel and the runbook checklist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckSpec {
    pub name: &'static str,
    /// The requirement ID in `docs/REQUIREMENTS.md`.
    pub requirement: &'static str,
    /// What a pass means, in one sentence.
    pub means: &'static str,
    /// How to fix a failing row.
    pub fix: &'static str,
    /// The AWS console page or document the fix happens on.
    pub fix_url: &'static str,
}

impl CheckId {
    /// Every check, in the order the panel and the runbook list them.
    pub const ALL: [CheckId; 9] = [
        CheckId::WarmThroughput,
        CheckId::DynamoDbLimits,
        CheckId::ApiGatewayThrottle,
        CheckId::AssignPositionConcurrency,
        CheckId::OpenSchedule,
        CheckId::ControllerSchedule,
        CheckId::GateRuleset,
        CheckId::StatusCaching,
        CheckId::GateAssociation,
    ];

    #[must_use]
    pub const fn spec(self) -> CheckSpec {
        match self {
            CheckId::WarmThroughput => CheckSpec {
                name: "Tables pre-warmed",
                requirement: "O1",
                means: "All four tables report warm throughput at or above the configured `warm_throughput_write_units` / `warm_throughput_read_units`, and `PreQueue` at or above the 10,000/s registration rate. The 4,000-unit AWS minimum is a floor, not a sized value; 0 configured means no pre-warm.",
                fix: "Set `warm_throughput_write_units` (and `_read_units`) in `terraform.tfvars` at or above the event's target write rate and apply, the day before rather than the hour before: warming is asynchronous.",
                fix_url: "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/warm-throughput.html",
            },
            CheckId::DynamoDbLimits => CheckSpec {
                name: "DynamoDB throughput quotas",
                requirement: "O2",
                means: "`DescribeLimits` reports a per-table write limit above the 40,000-unit default and at or above the configured warm throughput.",
                fix: "File a Service Quotas increase for DynamoDB table-level write throughput weeks ahead; an increase still pending at T-0 means the load test measured throttling.",
                fix_url: "https://console.aws.amazon.com/servicequotas/home/services/dynamodb/quotas",
            },
            CheckId::ApiGatewayThrottle => CheckSpec {
                name: "API Gateway account throttle",
                requirement: "O2",
                means: "The account's API Gateway steady-state rate is above the 10,000 requests/s default, which is what C3's 40,000 joins/s needs.",
                fix: "File a Service Quotas increase for API Gateway throttle rate in this region, weeks ahead.",
                fix_url: "https://console.aws.amazon.com/servicequotas/home/services/apigateway/quotas",
            },
            CheckId::AssignPositionConcurrency => CheckSpec {
                name: "assign_position reserved concurrency",
                requirement: "N9",
                means: "The join consumer has reserved concurrency above zero, so one event cannot starve another and the queue drains at a known rate.",
                fix: "Set `assign_position_reserved_concurrency` in the core module (the default is sized for 10,000/s) and apply.",
                fix_url: "https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html",
            },
            CheckId::OpenSchedule => CheckSpec {
                name: "Open schedule armed",
                requirement: "F0.3",
                means: "The one-time open schedule is enabled at a time still in the future, so the event opens on its own at T-0.",
                fix: "Set the start time on the dashboard (Start time), in the timezone you mean. The schedule itself is in EventBridge Scheduler.",
                fix_url: "https://console.aws.amazon.com/scheduler/home#schedules",
            },
            CheckId::ControllerSchedule => CheckSpec {
                name: "Controller schedule running",
                requirement: "F3.2",
                means: "The controller's `rate(1 minute)` schedule exists and is enabled; without it the queue forms and nobody is admitted.",
                fix: "Re-apply Terraform, which creates the schedule enabled; check nobody disabled it in EventBridge Scheduler.",
                fix_url: "https://console.aws.amazon.com/scheduler/home#schedules",
            },
            CheckId::GateRuleset => CheckSpec {
                name: "Gate ruleset",
                requirement: "F0.6",
                means: "The gate's KeyValueStore holds at least one protection rule and no fail-open window is active. An empty ruleset passes every request through, which looks exactly like a working deployment.",
                fix: "Set rules under Protection rules on the dashboard, then load a protected URL in a private window and confirm you are sent to the waiting page.",
                fix_url: "#protection-rules",
            },
            CheckId::StatusCaching => CheckSpec {
                name: "/status cache behaviour",
                requirement: "C4",
                means: "The polled `/v1/status` behaviour has a Min TTL above zero and keeps cookies out of its cache key, so CloudFront collapses polls and origin load is independent of waiter count.",
                fix: "Re-apply the edge module (`polled_min_ttl_seconds` must be at least 1); do not edit the cache policy in the console.",
                fix_url: "https://console.aws.amazon.com/cloudfront/v4/home#/policies/cache",
            },
            CheckId::GateAssociation => CheckSpec {
                name: "Gate on the protected behaviour only",
                requirement: "N7",
                means: "The gate CloudFront Function is associated at viewer-request with the default (protected) behaviour and with no other: elsewhere it bills every poll and refuses joins.",
                fix: "Re-apply the edge module; remove any function association added to another behaviour in the console.",
                fix_url: "https://console.aws.amazon.com/cloudfront/v4/home#/distributions",
            },
        }
    }
}

/// A check's verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pass,
    /// Not wrong, but worth an operator's attention before T-0: a default
    /// quota, a dormant ruleset, a warm-up still in progress.
    Warn,
    Fail,
    /// The read failed or timed out, or the thing to read is not configured.
    NotEvaluable,
}

impl Status {
    /// The label the panel shows.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Status::Pass => "pass",
            Status::Warn => "warn",
            Status::Fail => "fail",
            Status::NotEvaluable => "not evaluable",
        }
    }

    /// The stylesheet's badge class for this status.
    #[must_use]
    pub const fn css_class(self) -> &'static str {
        match self {
            Status::Pass => "status-success",
            Status::Warn => "status-warning",
            Status::Fail => "status-error",
            Status::NotEvaluable => "status-inactive",
        }
    }

    /// Whether the row should offer its fix link.
    #[must_use]
    pub const fn needs_fix(self) -> bool {
        match self {
            Status::Warn | Status::Fail => true,
            Status::Pass | Status::NotEvaluable => false,
        }
    }

    /// The worse of two verdicts on one row. Not-evaluable ranks above fail:
    /// a row that could not see everything must not read as settled.
    #[must_use]
    pub const fn worst(self, other: Status) -> Status {
        const fn rank(s: Status) -> u8 {
            match s {
                Status::Pass => 0,
                Status::Warn => 1,
                Status::Fail => 2,
                Status::NotEvaluable => 3,
            }
        }
        if rank(other) > rank(self) {
            other
        } else {
            self
        }
    }
}

/// One evaluated row: the check, its verdict, and the measured value (or the
/// reason it could not be measured).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub check: CheckId,
    pub status: Status,
    pub detail: String,
}

impl Row {
    fn new(check: CheckId, (status, detail): (Status, String)) -> Self {
        Self {
            check,
            status,
            detail,
        }
    }

    fn not_evaluable(check: CheckId, reason: &str) -> Self {
        Self {
            check,
            status: Status::NotEvaluable,
            detail: format!("could not evaluate: {reason}"),
        }
    }
}

/// A failed read. The reason is shown to the operator verbatim.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ProbeError(pub String);

// --- Measured values -----------------------------------------------------------

/// The four tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Counters,
    PreQueue,
    Positions,
    Tokens,
}

impl Table {
    pub const ALL: [Table; 4] = [
        Table::Counters,
        Table::PreQueue,
        Table::Positions,
        Table::Tokens,
    ];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Table::Counters => "Counters",
            Table::PreQueue => "PreQueue",
            Table::Positions => "Positions",
            Table::Tokens => "Tokens",
        }
    }
}

/// What Terraform configured the tables to be warmed to. `0` is "no
/// pre-warm", as in the Terraform variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WarmTargets {
    pub write_units: i64,
    pub read_units: i64,
}

/// A table's warm throughput as `DescribeTable` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WarmThroughput {
    pub write_units: Option<i64>,
    pub read_units: Option<i64>,
    /// The table reports its warm throughput as still being applied.
    pub warming: bool,
}

/// `DescribeLimits`' four values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DynamoDbLimits {
    pub table_max_write: Option<i64>,
    pub table_max_read: Option<i64>,
    pub account_max_write: Option<i64>,
    pub account_max_read: Option<i64>,
}

/// API Gateway's account throttle settings.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ApiGatewayThrottle {
    pub rate_limit: Option<f64>,
    pub burst_limit: Option<i32>,
}

/// The two schedules this deployment runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleKind {
    Open,
    Controller,
}

/// A schedule as `GetSchedule` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleFacts {
    pub enabled: bool,
    pub expression: String,
    pub timezone: String,
}

/// What a cache behaviour does with cookies in its cache key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CookieKey {
    None,
    /// Some cookies are in the key; the value is `CloudFront`'s own name for
    /// the behaviour (`whitelist`, `allExcept`, `all`).
    Included(String),
}

/// The polled `/status` behaviour's caching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachingFacts {
    pub min_ttl: i64,
    pub cookies: CookieKey,
}

/// What the distribution says about caching and the gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeFacts {
    /// The polled path the facts were read for, e.g. `/v1/status`.
    pub status_path: String,
    /// `None` when no behaviour matches that path: polls fall through to the
    /// default behaviour, uncached.
    pub status_caching: Option<CachingFacts>,
    /// The gate function is associated with the default behaviour at
    /// viewer-request.
    pub gate_on_default: bool,
    /// Path patterns of every other behaviour that carries the gate.
    pub gate_elsewhere: Vec<String>,
}

/// The port the readiness checks read through. A trait seam so the panel runs
/// without AWS; the SDK-backed implementation lives in `probe`.
pub trait ReadinessProbe {
    fn warm_throughput(
        &self,
        table: Table,
    ) -> impl Future<Output = Result<WarmThroughput, ProbeError>> + Send;

    fn dynamodb_limits(&self) -> impl Future<Output = Result<DynamoDbLimits, ProbeError>> + Send;

    fn api_gateway_throttle(
        &self,
    ) -> impl Future<Output = Result<ApiGatewayThrottle, ProbeError>> + Send;

    /// `assign_position`'s reserved concurrency; `None` when unreserved.
    fn reserved_concurrency(&self) -> impl Future<Output = Result<Option<i32>, ProbeError>> + Send;

    /// `None` when the schedule does not exist.
    fn schedule(
        &self,
        kind: ScheduleKind,
    ) -> impl Future<Output = Result<Option<ScheduleFacts>, ProbeError>> + Send;

    /// `None` when the edge module has not published where the distribution
    /// is (between core's and edge's halves of a first apply).
    fn edge_facts(&self) -> impl Future<Output = Result<Option<EdgeFacts>, ProbeError>> + Send;
}

// --- Evaluation ----------------------------------------------------------------

/// Formats a whole number with thousands separators: `12000` -> `12,000`.
#[must_use]
pub fn thousands(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn units(v: Option<i64>) -> String {
    v.map_or_else(|| "not reported".to_owned(), thousands)
}

/// Judges the four tables' warm throughput against the configured targets.
#[must_use]
pub fn evaluate_warm_throughput(
    targets: WarmTargets,
    applied: &[(Table, WarmThroughput)],
) -> (Status, String) {
    let mut status = Status::Pass;
    let mut measured = Vec::with_capacity(applied.len());
    let mut notes = Vec::new();

    for (table, warm) in applied {
        let write = warm.write_units.unwrap_or(0);
        let read = warm.read_units.unwrap_or(0);
        measured.push(format!(
            "{} write {} / read {}",
            table.label(),
            thousands(write),
            thousands(read)
        ));

        if warm.warming {
            status = status.worst(Status::Warn);
            notes.push(format!("{} is still warming", table.label()));
        }
        if targets.write_units > 0 && write < targets.write_units {
            status = status.worst(Status::Fail);
            notes.push(format!(
                "{} write {} is below the configured {}",
                table.label(),
                thousands(write),
                thousands(targets.write_units)
            ));
        }
        if targets.read_units > 0 && read < targets.read_units {
            status = status.worst(Status::Fail);
            notes.push(format!(
                "{} read {} is below the configured {}",
                table.label(),
                thousands(read),
                thousands(targets.read_units)
            ));
        }
        if *table == Table::PreQueue && write < REGISTRATION_TARGET_PER_SECOND {
            status = status.worst(Status::Warn);
            notes.push(format!(
                "PreQueue write {} is under the {}/s registration rate",
                thousands(write),
                thousands(REGISTRATION_TARGET_PER_SECOND)
            ));
        }
    }

    if targets.write_units == 0 && targets.read_units == 0 {
        status = status.worst(Status::Warn);
        notes.push("no pre-warm is configured (warm_throughput_write_units = 0)".to_owned());
    } else {
        notes.push(format!(
            "configured write {} / read {}",
            thousands(targets.write_units),
            thousands(targets.read_units)
        ));
    }

    let mut detail = format!("{} units/s", measured.join("; "));
    for note in notes {
        let _ = write!(detail, ". {note}");
    }
    (status, detail)
}

/// Judges `DescribeLimits` against the default quota and the configured warm
/// throughput, which cannot exceed the table limit.
#[must_use]
pub fn evaluate_dynamodb_limits(limits: DynamoDbLimits, targets: WarmTargets) -> (Status, String) {
    let detail = format!(
        "table write {} / read {} units/s; account write {} / read {}",
        units(limits.table_max_write),
        units(limits.table_max_read),
        units(limits.account_max_write),
        units(limits.account_max_read),
    );
    let Some(table_write) = limits.table_max_write else {
        return (
            Status::NotEvaluable,
            format!("could not evaluate: DescribeLimits reported no table write limit. {detail}"),
        );
    };
    if targets.write_units > 0 && table_write < targets.write_units {
        return (
            Status::Fail,
            format!(
                "{detail}. The table limit is below the configured warm throughput {}",
                thousands(targets.write_units)
            ),
        );
    }
    if table_write <= DEFAULT_TABLE_THROUGHPUT_LIMIT {
        return (
            Status::Warn,
            format!("{detail}. At the AWS default: no increase is in force"),
        );
    }
    (Status::Pass, detail)
}

/// Judges the account's API Gateway throttle against the default and C3.
#[must_use]
pub fn evaluate_api_gateway_throttle(throttle: ApiGatewayThrottle) -> (Status, String) {
    let Some(rate) = throttle.rate_limit else {
        return (
            Status::NotEvaluable,
            "could not evaluate: GetAccount reported no throttle rate".to_owned(),
        );
    };
    let burst = throttle
        .burst_limit
        .map_or_else(|| "not reported".to_owned(), |b| thousands(i64::from(b)));
    let detail = format!("rate {} requests/s, burst {burst}", format_rate(rate));
    if rate < DEFAULT_API_GATEWAY_RATE {
        (
            Status::Fail,
            format!("{detail}. Below the 10,000/s C3 needs even at default quotas"),
        )
    } else if rate > DEFAULT_API_GATEWAY_RATE {
        (Status::Pass, detail)
    } else {
        (
            Status::Warn,
            format!("{detail}. At the AWS default: no increase is in force"),
        )
    }
}

/// A rate as a whole number with separators. Rates are whole in practice;
/// anything else is shown as the service reported it.
fn format_rate(rate: f64) -> String {
    // `Display` prints a whole f64 without a fraction, so a whole rate parses
    // back as an integer and anything else keeps its fraction.
    let shown = rate.to_string();
    shown.parse::<i64>().map_or(shown, thousands)
}

/// Judges `assign_position`'s reserved concurrency.
#[must_use]
pub fn evaluate_reserved_concurrency(reserved: Option<i32>) -> (Status, String) {
    match reserved {
        None => (
            Status::Fail,
            "unreserved: shares the account's unreserved pool".to_owned(),
        ),
        Some(n) if n <= 0 => (
            Status::Fail,
            format!(
                "reserved {n}: the function is throttled entirely and nobody is assigned a position"
            ),
        ),
        Some(n) => (
            Status::Pass,
            format!("reserved {}", thousands(i64::from(n))),
        ),
    }
}

/// The epoch second a one-time `at(YYYY-MM-DDTHH:MM:SS)` expression fires at,
/// read in `timezone`. `None` for any other expression.
#[must_use]
pub fn at_epoch(expression: &str, timezone: &str) -> Option<i64> {
    let inner = expression.strip_prefix("at(")?.strip_suffix(')')?;
    let civil: jiff::civil::DateTime = inner.parse().ok()?;
    let tz = jiff::tz::TimeZone::get(timezone).ok()?;
    let zoned = tz.to_ambiguous_zoned(civil).compatible().ok()?;
    Some(zoned.timestamp().as_second())
}

/// Judges the one-time open schedule at `now` (epoch seconds).
#[must_use]
pub fn evaluate_open_schedule(schedule: Option<&ScheduleFacts>, now: u64) -> (Status, String) {
    let Some(s) = schedule else {
        return (
            Status::Fail,
            "the open schedule does not exist: nothing will open the event".to_owned(),
        );
    };
    let when = format!("{} {}", s.expression, s.timezone);
    if !s.enabled {
        return (
            Status::Warn,
            format!("disabled ({when}): the event will not open on its own"),
        );
    }
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    match at_epoch(&s.expression, &s.timezone) {
        Some(at) if at > now => (Status::Pass, format!("enabled, {when}")),
        Some(_) => (
            Status::Warn,
            format!("enabled, {when}: in the past, so it has fired or will not fire"),
        ),
        None => (
            Status::Warn,
            format!("enabled, {when}: not a one-time at() expression"),
        ),
    }
}

/// Judges the controller's schedule.
#[must_use]
pub fn evaluate_controller_schedule(schedule: Option<&ScheduleFacts>) -> (Status, String) {
    match schedule {
        None => (
            Status::Fail,
            "the controller schedule does not exist: nobody will be admitted".to_owned(),
        ),
        Some(s) if !s.enabled => (
            Status::Fail,
            format!("disabled ({}): nobody will be admitted", s.expression),
        ),
        Some(s) => (Status::Pass, format!("enabled, {}", s.expression)),
    }
}

/// Judges the gate's ruleset and fail-open window at `now` (epoch seconds).
#[must_use]
pub fn evaluate_gate_ruleset(cfg: &GateConfig, now: u64) -> (Status, String) {
    let (mut status, mut detail) = match cfg.rules.len() {
        0 => (
            Status::Warn,
            "dormant: no rules, so every request passes through (standby, #60)".to_owned(),
        ),
        1 => (Status::Pass, "1 rule configured".to_owned()),
        n => (Status::Pass, format!("{n} rules configured")),
    };
    if cfg.fail_open_until > now {
        status = status.worst(Status::Warn);
        let until = i64::try_from(cfg.fail_open_until)
            .ok()
            .and_then(|s| jiff::Timestamp::from_second(s).ok())
            .map_or_else(|| cfg.fail_open_until.to_string(), |t| t.to_string());
        let _ = write!(
            detail,
            ". Fail-open is active until {until} ({}s left): the gate is bypassed",
            cfg.fail_open_until.saturating_sub(now)
        );
    }
    (status, detail)
}

/// Judges the polled `/status` behaviour's caching.
#[must_use]
pub fn evaluate_status_caching(facts: &EdgeFacts) -> (Status, String) {
    let Some(caching) = &facts.status_caching else {
        return (
            Status::Fail,
            format!(
                "no behaviour for {}: polls fall through to the uncached default behaviour",
                facts.status_path
            ),
        );
    };
    let cookies = match &caching.cookies {
        CookieKey::None => "no cookies in the cache key".to_owned(),
        CookieKey::Included(how) => format!("cookies in the cache key ({how})"),
    };
    let detail = format!(
        "{}: min TTL {} s, {cookies}",
        facts.status_path, caching.min_ttl
    );
    let collapses = caching.min_ttl > 0 && caching.cookies == CookieKey::None;
    if collapses {
        (Status::Pass, detail)
    } else {
        (
            Status::Fail,
            format!("{detail}. Every poll reaches the origin"),
        )
    }
}

/// Judges where the gate function is associated.
#[must_use]
pub fn evaluate_gate_association(facts: &EdgeFacts) -> (Status, String) {
    let mut problems = Vec::new();
    if !facts.gate_on_default {
        problems.push("not on the default behaviour: the protected origin is unguarded".to_owned());
    }
    if !facts.gate_elsewhere.is_empty() {
        problems.push(format!(
            "also on {}: billed per request there, and a visitor with no session is refused",
            facts.gate_elsewhere.join(", ")
        ));
    }
    if problems.is_empty() {
        (
            Status::Pass,
            "viewer-request on the default behaviour only".to_owned(),
        )
    } else {
        (Status::Fail, problems.join("; "))
    }
}

// --- Running --------------------------------------------------------------------

/// Bounds one read by `limit`, folding a timeout into a [`ProbeError`].
async fn bounded<T>(
    limit: Duration,
    read: impl Future<Output = Result<T, ProbeError>>,
) -> Result<T, ProbeError> {
    tokio::time::timeout(limit, read)
        .await
        .unwrap_or_else(|_| Err(ProbeError(format!("timed out after {}s", limit.as_secs()))))
}

/// Runs every check concurrently, each bounded by `limit`, and returns one row
/// per [`CheckId::ALL`] entry in order. Never fails: a read that errors or
/// times out becomes a not-evaluable row.
pub async fn run_checks<P, E>(
    probe: &P,
    edge: &E,
    targets: WarmTargets,
    now: u64,
    limit: Duration,
) -> Vec<Row>
where
    P: ReadinessProbe + Sync,
    E: EdgeConfigStore + Sync,
{
    let warm = bounded(limit, async {
        let (c, p, s, t) = tokio::join!(
            probe.warm_throughput(Table::Counters),
            probe.warm_throughput(Table::PreQueue),
            probe.warm_throughput(Table::Positions),
            probe.warm_throughput(Table::Tokens),
        );
        let mut applied = Vec::with_capacity(Table::ALL.len());
        for (table, read) in [
            (Table::Counters, c),
            (Table::PreQueue, p),
            (Table::Positions, s),
            (Table::Tokens, t),
        ] {
            let warm = read.map_err(|e| ProbeError(format!("{}: {e}", table.label())))?;
            applied.push((table, warm));
        }
        Ok(applied)
    });
    let gate = bounded(limit, async {
        edge.read_config()
            .await
            .map(|(cfg, _etag)| cfg)
            .map_err(|e| ProbeError(e.to_string()))
    });

    let (warm, limits, api, concurrency, open, controller, gate, edge_facts) = tokio::join!(
        warm,
        bounded(limit, probe.dynamodb_limits()),
        bounded(limit, probe.api_gateway_throttle()),
        bounded(limit, probe.reserved_concurrency()),
        bounded(limit, probe.schedule(ScheduleKind::Open)),
        bounded(limit, probe.schedule(ScheduleKind::Controller)),
        gate,
        bounded(limit, probe.edge_facts()),
    );

    let row = |check: CheckId, judged: Result<(Status, String), ProbeError>| match judged {
        Ok(verdict) => Row::new(check, verdict),
        Err(e) => Row::not_evaluable(check, &e.0),
    };

    let mut rows = vec![
        row(
            CheckId::WarmThroughput,
            warm.map(|applied| evaluate_warm_throughput(targets, &applied)),
        ),
        row(
            CheckId::DynamoDbLimits,
            limits.map(|l| evaluate_dynamodb_limits(l, targets)),
        ),
        row(
            CheckId::ApiGatewayThrottle,
            api.map(evaluate_api_gateway_throttle),
        ),
        row(
            CheckId::AssignPositionConcurrency,
            concurrency.map(evaluate_reserved_concurrency),
        ),
        row(
            CheckId::OpenSchedule,
            open.map(|s| evaluate_open_schedule(s.as_ref(), now)),
        ),
        row(
            CheckId::ControllerSchedule,
            controller.map(|s| evaluate_controller_schedule(s.as_ref())),
        ),
        row(
            CheckId::GateRuleset,
            gate.map(|cfg| evaluate_gate_ruleset(&cfg, now)),
        ),
    ];
    match edge_facts {
        Ok(Some(facts)) => {
            rows.push(Row::new(
                CheckId::StatusCaching,
                evaluate_status_caching(&facts),
            ));
            rows.push(Row::new(
                CheckId::GateAssociation,
                evaluate_gate_association(&facts),
            ));
        }
        Ok(None) => {
            let reason = "not configured: the edge module has not published the distribution's readiness parameter (apply the edge module)";
            rows.push(Row::not_evaluable(CheckId::StatusCaching, reason));
            rows.push(Row::not_evaluable(CheckId::GateAssociation, reason));
        }
        Err(e) => {
            rows.push(Row::not_evaluable(CheckId::StatusCaching, &e.0));
            rows.push(Row::not_evaluable(CheckId::GateAssociation, &e.0));
        }
    }
    rows
}

// --- Runbook --------------------------------------------------------------------

/// The marker lines around the generated checklist in `docs/RUNBOOK.md`.
pub const RUNBOOK_BEGIN: &str =
    "<!-- readiness-checks:begin (generated from crates/admin/src/readiness.rs; do not edit) -->";
pub const RUNBOOK_END: &str = "<!-- readiness-checks:end -->";

/// The runbook's automated checklist, rendered from [`CheckId::ALL`]: one
/// Markdown item per check, between the two marker lines.
#[must_use]
pub fn runbook_checklist() -> String {
    let mut out = String::new();
    out.push_str(RUNBOOK_BEGIN);
    out.push('\n');
    for check in CheckId::ALL {
        let spec = check.spec();
        let link = if spec.fix_url.starts_with('#') {
            "the dashboard".to_owned()
        } else {
            format!("[fix]({})", spec.fix_url)
        };
        let _ = writeln!(
            out,
            "- [ ] **{}** ({}). {} Fix: {} ({link})",
            spec.name, spec.requirement, spec.means, spec.fix
        );
    }
    out.push_str(RUNBOOK_END);
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;
    use crate::{ETag, EdgeStoreError};

    fn warm(write: i64, read: i64) -> WarmThroughput {
        WarmThroughput {
            write_units: Some(write),
            read_units: Some(read),
            warming: false,
        }
    }

    fn all_tables(w: WarmThroughput) -> Vec<(Table, WarmThroughput)> {
        Table::ALL.iter().map(|t| (*t, w)).collect()
    }

    const CONFIGURED: WarmTargets = WarmTargets {
        write_units: 12_000,
        read_units: 12_000,
    };

    // --- Formatting ---

    #[test]
    fn thousands_groups_digits() {
        for (n, s) in [
            (0, "0"),
            (999, "999"),
            (1_000, "1,000"),
            (12_000, "12,000"),
            (1_234_567, "1,234,567"),
            (-40_000, "-40,000"),
        ] {
            assert_eq!(thousands(n), s);
        }
    }

    // --- Warm throughput (O1) ---

    #[test]
    fn warm_throughput_at_or_above_target_passes_and_shows_the_value() {
        let (status, detail) =
            evaluate_warm_throughput(CONFIGURED, &all_tables(warm(12_000, 12_000)));
        assert_eq!(status, Status::Pass, "{detail}");
        assert!(
            detail.contains("PreQueue write 12,000 / read 12,000"),
            "{detail}"
        );
        assert!(detail.contains("configured write 12,000"), "{detail}");
    }

    #[test]
    fn a_table_below_its_configured_write_fails_and_names_the_table() {
        let mut applied = all_tables(warm(12_000, 12_000));
        applied[2].1 = warm(4_000, 12_000);
        let (status, detail) = evaluate_warm_throughput(CONFIGURED, &applied);
        assert_eq!(status, Status::Fail);
        assert!(
            detail.contains("Positions write 4,000 is below the configured 12,000"),
            "{detail}"
        );
    }

    #[test]
    fn a_table_below_its_configured_read_fails() {
        let mut applied = all_tables(warm(12_000, 12_000));
        applied[0].1 = warm(12_000, 6_000);
        let (status, detail) = evaluate_warm_throughput(CONFIGURED, &applied);
        assert_eq!(status, Status::Fail);
        assert!(detail.contains("Counters read 6,000"), "{detail}");
    }

    #[test]
    fn an_unreported_value_counts_as_zero_against_a_target() {
        let mut applied = all_tables(warm(12_000, 12_000));
        applied[3].1 = WarmThroughput {
            write_units: None,
            read_units: Some(12_000),
            warming: false,
        };
        assert_eq!(
            evaluate_warm_throughput(CONFIGURED, &applied).0,
            Status::Fail
        );
    }

    #[test]
    fn nothing_configured_warns_and_still_shows_what_is_applied() {
        let (status, detail) =
            evaluate_warm_throughput(WarmTargets::default(), &all_tables(warm(10_000, 12_000)));
        assert_eq!(status, Status::Warn);
        assert!(detail.contains("no pre-warm is configured"), "{detail}");
        assert!(
            detail.contains("Counters write 10,000 / read 12,000"),
            "{detail}"
        );
    }

    #[test]
    fn warming_in_progress_warns() {
        let mut applied = all_tables(warm(12_000, 12_000));
        applied[1].1.warming = true;
        let (status, detail) = evaluate_warm_throughput(CONFIGURED, &applied);
        assert_eq!(status, Status::Warn);
        assert!(detail.contains("PreQueue is still warming"), "{detail}");
    }

    #[test]
    fn prequeue_warmed_to_the_aws_minimum_is_not_sized_for_registrations() {
        // 4,000 is what the Terraform validation accepts; it is the AWS floor,
        // not a value sized for ~10,000 registrations a second.
        let targets = WarmTargets {
            write_units: 4_000,
            read_units: 0,
        };
        let (status, detail) = evaluate_warm_throughput(targets, &all_tables(warm(4_000, 12_000)));
        assert_eq!(status, Status::Warn);
        assert!(
            detail.contains("PreQueue write 4,000 is under the 10,000/s"),
            "{detail}"
        );
    }

    #[test]
    fn a_failure_is_not_softened_by_a_warning_on_another_table() {
        let mut applied = all_tables(warm(12_000, 12_000));
        applied[1].1.warming = true;
        applied[0].1 = warm(1, 12_000);
        assert_eq!(
            evaluate_warm_throughput(CONFIGURED, &applied).0,
            Status::Fail
        );
    }

    // --- DynamoDB limits (O2) ---

    fn limits(table_write: Option<i64>) -> DynamoDbLimits {
        DynamoDbLimits {
            table_max_write: table_write,
            table_max_read: Some(40_000),
            account_max_write: Some(80_000),
            account_max_read: Some(80_000),
        }
    }

    #[test]
    fn dynamodb_limits_at_default_warn() {
        let (status, detail) = evaluate_dynamodb_limits(limits(Some(40_000)), CONFIGURED);
        assert_eq!(status, Status::Warn);
        assert!(
            detail.contains("table write 40,000 / read 40,000"),
            "{detail}"
        );
        assert!(detail.contains("account write 80,000"), "{detail}");
    }

    #[test]
    fn dynamodb_limits_raised_pass() {
        assert_eq!(
            evaluate_dynamodb_limits(limits(Some(100_000)), CONFIGURED).0,
            Status::Pass
        );
    }

    #[test]
    fn a_table_limit_below_the_configured_warm_throughput_fails() {
        let targets = WarmTargets {
            write_units: 50_000,
            read_units: 0,
        };
        assert_eq!(
            evaluate_dynamodb_limits(limits(Some(40_000)), targets).0,
            Status::Fail
        );
    }

    #[test]
    fn an_unreported_table_limit_is_not_evaluable() {
        let (status, detail) = evaluate_dynamodb_limits(limits(None), CONFIGURED);
        assert_eq!(status, Status::NotEvaluable);
        assert!(detail.starts_with("could not evaluate"), "{detail}");
    }

    // --- API Gateway (O2) ---

    fn throttle(rate: f64) -> ApiGatewayThrottle {
        ApiGatewayThrottle {
            rate_limit: Some(rate),
            burst_limit: Some(5_000),
        }
    }

    #[test]
    fn api_gateway_rate_is_judged_against_the_default() {
        let (status, detail) = evaluate_api_gateway_throttle(throttle(10_000.0));
        assert_eq!(status, Status::Warn);
        assert!(
            detail.contains("rate 10,000 requests/s, burst 5,000"),
            "{detail}"
        );
        assert_eq!(
            evaluate_api_gateway_throttle(throttle(40_000.0)).0,
            Status::Pass
        );
        assert_eq!(
            evaluate_api_gateway_throttle(throttle(5_000.0)).0,
            Status::Fail
        );
    }

    #[test]
    fn a_fractional_rate_is_shown_as_reported() {
        let (_, detail) = evaluate_api_gateway_throttle(throttle(10_000.5));
        assert!(detail.contains("rate 10000.5 requests/s"), "{detail}");
    }

    #[test]
    fn an_unreported_rate_is_not_evaluable() {
        let t = ApiGatewayThrottle {
            rate_limit: None,
            burst_limit: None,
        };
        assert_eq!(evaluate_api_gateway_throttle(t).0, Status::NotEvaluable);
    }

    // --- Reserved concurrency (N9) ---

    #[test]
    fn reserved_concurrency_must_be_set_and_positive() {
        assert_eq!(evaluate_reserved_concurrency(None).0, Status::Fail);
        assert_eq!(evaluate_reserved_concurrency(Some(0)).0, Status::Fail);
        let (status, detail) = evaluate_reserved_concurrency(Some(1_200));
        assert_eq!(status, Status::Pass);
        assert_eq!(detail, "reserved 1,200");
    }

    // --- Schedules (F0.3, F3.2) ---

    fn schedule(enabled: bool, expression: &str) -> ScheduleFacts {
        ScheduleFacts {
            enabled,
            expression: expression.to_owned(),
            timezone: "America/New_York".to_owned(),
        }
    }

    // 2030-06-15T10:00 in New York is 14:00 UTC.
    const OPENS_AT: i64 = 1_907_762_400;

    #[test]
    fn at_epoch_reads_the_expression_in_its_zone() {
        assert_eq!(
            at_epoch("at(2030-06-15T10:00:00)", "America/New_York"),
            Some(OPENS_AT)
        );
        assert_eq!(at_epoch("rate(1 minute)", "UTC"), None);
        assert_eq!(at_epoch("at(2030-06-15T10:00:00)", "Not/AZone"), None);
    }

    #[test]
    fn an_armed_open_in_the_future_passes_and_shows_when() {
        let s = schedule(true, "at(2030-06-15T10:00:00)");
        let now = u64::try_from(OPENS_AT - 1).unwrap();
        let (status, detail) = evaluate_open_schedule(Some(&s), now);
        assert_eq!(status, Status::Pass);
        assert_eq!(detail, "enabled, at(2030-06-15T10:00:00) America/New_York");
    }

    #[test]
    fn an_armed_open_in_the_past_warns() {
        let s = schedule(true, "at(2030-06-15T10:00:00)");
        let now = u64::try_from(OPENS_AT).unwrap();
        assert_eq!(evaluate_open_schedule(Some(&s), now).0, Status::Warn);
    }

    #[test]
    fn a_disabled_open_warns_and_a_missing_one_fails() {
        let s = schedule(false, "at(2099-12-31T23:59:59)");
        let (status, detail) = evaluate_open_schedule(Some(&s), 0);
        assert_eq!(status, Status::Warn);
        assert!(detail.contains("will not open on its own"), "{detail}");
        assert_eq!(evaluate_open_schedule(None, 0).0, Status::Fail);
    }

    #[test]
    fn an_open_that_is_not_a_one_time_expression_warns() {
        let s = schedule(true, "rate(1 day)");
        assert_eq!(evaluate_open_schedule(Some(&s), 0).0, Status::Warn);
    }

    #[test]
    fn the_controller_must_exist_and_be_enabled() {
        let on = schedule(true, "rate(1 minute)");
        assert_eq!(
            evaluate_controller_schedule(Some(&on)),
            (Status::Pass, "enabled, rate(1 minute)".to_owned())
        );
        let off = schedule(false, "rate(1 minute)");
        assert_eq!(evaluate_controller_schedule(Some(&off)).0, Status::Fail);
        assert_eq!(evaluate_controller_schedule(None).0, Status::Fail);
    }

    // --- Gate ruleset (F0.6) ---

    fn gate(rules: usize, fail_open_until: u64) -> GateConfig {
        GateConfig {
            v: 1,
            enforce_from: 0,
            fail_open_until,
            rules: (0..rules)
                .map(|i| wr_common::ProtectionRule::PathPrefix(format!("/p{i}")))
                .collect(),
            bind_ip: false,
        }
    }

    #[test]
    fn a_dormant_ruleset_warns_and_says_so() {
        let (status, detail) = evaluate_gate_ruleset(&gate(0, 0), 100);
        assert_eq!(status, Status::Warn);
        assert!(detail.starts_with("dormant"), "{detail}");
    }

    #[test]
    fn a_configured_ruleset_passes_with_its_count() {
        assert_eq!(
            evaluate_gate_ruleset(&gate(1, 0), 100),
            (Status::Pass, "1 rule configured".to_owned())
        );
        assert_eq!(
            evaluate_gate_ruleset(&gate(3, 0), 100),
            (Status::Pass, "3 rules configured".to_owned())
        );
    }

    #[test]
    fn an_active_fail_open_window_warns_and_a_lapsed_one_does_not() {
        let (status, detail) = evaluate_gate_ruleset(&gate(2, 1_000), 400);
        assert_eq!(status, Status::Warn);
        assert!(
            detail.contains("Fail-open is active until 1970-01-01T00:16:40Z (600s left)"),
            "{detail}"
        );
        assert_eq!(
            evaluate_gate_ruleset(&gate(2, 1_000), 1_000).0,
            Status::Pass
        );
    }

    // --- CloudFront (C4, N7) ---

    fn edge_facts() -> EdgeFacts {
        EdgeFacts {
            status_path: "/v1/status".to_owned(),
            status_caching: Some(CachingFacts {
                min_ttl: 1,
                cookies: CookieKey::None,
            }),
            gate_on_default: true,
            gate_elsewhere: Vec::new(),
        }
    }

    #[test]
    fn a_collapsing_status_behaviour_passes_with_its_ttl() {
        assert_eq!(
            evaluate_status_caching(&edge_facts()),
            (
                Status::Pass,
                "/v1/status: min TTL 1 s, no cookies in the cache key".to_owned()
            )
        );
    }

    #[test]
    fn a_zero_min_ttl_fails() {
        let mut f = edge_facts();
        f.status_caching = Some(CachingFacts {
            min_ttl: 0,
            cookies: CookieKey::None,
        });
        assert_eq!(evaluate_status_caching(&f).0, Status::Fail);
    }

    #[test]
    fn cookies_in_the_status_cache_key_fail() {
        let mut f = edge_facts();
        f.status_caching = Some(CachingFacts {
            min_ttl: 1,
            cookies: CookieKey::Included("all".to_owned()),
        });
        let (status, detail) = evaluate_status_caching(&f);
        assert_eq!(status, Status::Fail);
        assert!(
            detail.contains("cookies in the cache key (all)"),
            "{detail}"
        );
    }

    #[test]
    fn a_missing_status_behaviour_fails() {
        let mut f = edge_facts();
        f.status_caching = None;
        assert_eq!(evaluate_status_caching(&f).0, Status::Fail);
    }

    #[test]
    fn the_gate_must_be_on_the_default_behaviour_and_nowhere_else() {
        assert_eq!(evaluate_gate_association(&edge_facts()).0, Status::Pass);

        let mut off = edge_facts();
        off.gate_on_default = false;
        let (status, detail) = evaluate_gate_association(&off);
        assert_eq!(status, Status::Fail);
        assert!(detail.contains("unguarded"), "{detail}");

        let mut wide = edge_facts();
        wide.gate_elsewhere = vec!["/v1/status".to_owned(), "/v1/join".to_owned()];
        let (status, detail) = evaluate_gate_association(&wide);
        assert_eq!(status, Status::Fail);
        assert!(detail.contains("also on /v1/status, /v1/join"), "{detail}");
    }

    // --- Status ---

    #[test]
    fn worst_ranks_every_pair() {
        use Status::{Fail, NotEvaluable, Pass, Warn};
        let order = [Pass, Warn, Fail, NotEvaluable];
        for (i, a) in order.iter().enumerate() {
            for (j, b) in order.iter().enumerate() {
                assert_eq!(a.worst(*b), order[i.max(j)], "{a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn only_warn_and_fail_offer_a_fix() {
        assert!(!Status::Pass.needs_fix());
        assert!(Status::Warn.needs_fix());
        assert!(Status::Fail.needs_fix());
        assert!(!Status::NotEvaluable.needs_fix());
    }

    // --- The runner, with every port faked ---

    /// A probe answering each read from a canned result, or never answering
    /// the ones listed in `hang`.
    struct FakeProbe {
        warm: Result<WarmThroughput, ProbeError>,
        limits: Result<DynamoDbLimits, ProbeError>,
        api: Result<ApiGatewayThrottle, ProbeError>,
        concurrency: Result<Option<i32>, ProbeError>,
        open: Result<Option<ScheduleFacts>, ProbeError>,
        controller: Result<Option<ScheduleFacts>, ProbeError>,
        edge: Result<Option<EdgeFacts>, ProbeError>,
        hang_limits: bool,
    }

    impl FakeProbe {
        fn healthy() -> Self {
            Self {
                warm: Ok(warm(12_000, 12_000)),
                limits: Ok(limits(Some(100_000))),
                api: Ok(throttle(40_000.0)),
                concurrency: Ok(Some(1_200)),
                open: Ok(Some(schedule(true, "at(2030-06-15T10:00:00)"))),
                controller: Ok(Some(schedule(true, "rate(1 minute)"))),
                edge: Ok(Some(edge_facts())),
                hang_limits: false,
            }
        }
    }

    impl ReadinessProbe for FakeProbe {
        fn warm_throughput(
            &self,
            _table: Table,
        ) -> impl Future<Output = Result<WarmThroughput, ProbeError>> + Send {
            std::future::ready(self.warm.clone())
        }

        async fn dynamodb_limits(&self) -> Result<DynamoDbLimits, ProbeError> {
            if self.hang_limits {
                std::future::pending::<()>().await;
            }
            self.limits.clone()
        }

        fn api_gateway_throttle(
            &self,
        ) -> impl Future<Output = Result<ApiGatewayThrottle, ProbeError>> + Send {
            std::future::ready(self.api.clone())
        }

        fn reserved_concurrency(
            &self,
        ) -> impl Future<Output = Result<Option<i32>, ProbeError>> + Send {
            std::future::ready(self.concurrency.clone())
        }

        fn schedule(
            &self,
            kind: ScheduleKind,
        ) -> impl Future<Output = Result<Option<ScheduleFacts>, ProbeError>> + Send {
            std::future::ready(match kind {
                ScheduleKind::Open => self.open.clone(),
                ScheduleKind::Controller => self.controller.clone(),
            })
        }

        fn edge_facts(&self) -> impl Future<Output = Result<Option<EdgeFacts>, ProbeError>> + Send {
            std::future::ready(self.edge.clone())
        }
    }

    struct FakeGate(Mutex<Result<GateConfig, String>>);

    impl EdgeConfigStore for FakeGate {
        fn read_config(
            &self,
        ) -> impl Future<Output = Result<(GateConfig, ETag), EdgeStoreError>> + Send {
            let r = self.0.lock().unwrap().clone();
            std::future::ready(r.map(|c| (c, ETag("1".to_owned()))).map_err(EdgeStoreError))
        }

        fn write_config(
            &self,
            _etag: &ETag,
            _cfg: &GateConfig,
        ) -> impl Future<Output = Result<(), EdgeStoreError>> + Send {
            // Diagnostics only: the panel must never write.
            std::future::ready(Err(EdgeStoreError("the readiness panel wrote".to_owned())))
        }
    }

    fn gate_with(rules: usize) -> FakeGate {
        FakeGate(Mutex::new(Ok(gate(rules, 0))))
    }

    async fn run(probe: &FakeProbe, edge: &FakeGate) -> Vec<Row> {
        run_checks(probe, edge, CONFIGURED, 0, Duration::from_millis(50)).await
    }

    #[tokio::test]
    async fn a_healthy_deployment_passes_every_row_in_catalogue_order() {
        let rows = run(&FakeProbe::healthy(), &gate_with(2)).await;
        let ids: Vec<_> = rows.iter().map(|r| r.check).collect();
        assert_eq!(ids, CheckId::ALL);
        for r in &rows {
            assert_eq!(r.status, Status::Pass, "{:?}: {}", r.check, r.detail);
        }
    }

    #[tokio::test]
    async fn a_failed_read_is_not_evaluable_and_the_rest_still_render() {
        let mut probe = FakeProbe::healthy();
        probe.api = Err(ProbeError("AccessDeniedException".to_owned()));
        let rows = run(&probe, &gate_with(2)).await;
        let api = rows
            .iter()
            .find(|r| r.check == CheckId::ApiGatewayThrottle)
            .unwrap();
        assert_eq!(api.status, Status::NotEvaluable);
        assert_eq!(api.detail, "could not evaluate: AccessDeniedException");
        assert_eq!(
            rows.iter()
                .filter(|r| r.status == Status::NotEvaluable)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_read_that_never_answers_times_out_as_not_evaluable() {
        let mut probe = FakeProbe::healthy();
        probe.hang_limits = true;
        let rows = run(&probe, &gate_with(2)).await;
        let row = rows
            .iter()
            .find(|r| r.check == CheckId::DynamoDbLimits)
            .unwrap();
        assert_eq!(row.status, Status::NotEvaluable);
        assert!(
            row.detail.starts_with("could not evaluate: timed out"),
            "{}",
            row.detail
        );
    }

    #[tokio::test]
    async fn one_table_failing_to_describe_names_the_table() {
        let mut probe = FakeProbe::healthy();
        probe.warm = Err(ProbeError("throttled".to_owned()));
        let rows = run(&probe, &gate_with(2)).await;
        assert_eq!(rows[0].status, Status::NotEvaluable);
        assert_eq!(rows[0].detail, "could not evaluate: Counters: throttled");
    }

    #[tokio::test]
    async fn an_unreadable_ruleset_is_not_evaluable() {
        let edge = FakeGate(Mutex::new(Err("get_key: denied".to_owned())));
        let rows = run(&FakeProbe::healthy(), &edge).await;
        let row = rows
            .iter()
            .find(|r| r.check == CheckId::GateRuleset)
            .unwrap();
        assert_eq!(row.status, Status::NotEvaluable);
        assert!(row.detail.contains("get_key: denied"), "{}", row.detail);
    }

    #[tokio::test]
    async fn an_unpublished_distribution_leaves_both_cloudfront_rows_not_configured() {
        let mut probe = FakeProbe::healthy();
        probe.edge = Ok(None);
        let rows = run(&probe, &gate_with(2)).await;
        for check in [CheckId::StatusCaching, CheckId::GateAssociation] {
            let row = rows.iter().find(|r| r.check == check).unwrap();
            assert_eq!(row.status, Status::NotEvaluable);
            assert!(row.detail.contains("not configured"), "{}", row.detail);
        }
    }

    #[tokio::test]
    async fn a_distribution_read_error_leaves_both_cloudfront_rows_not_evaluable() {
        let mut probe = FakeProbe::healthy();
        probe.edge = Err(ProbeError("NoSuchDistribution".to_owned()));
        let rows = run(&probe, &gate_with(2)).await;
        assert_eq!(rows[7].status, Status::NotEvaluable);
        assert_eq!(rows[8].status, Status::NotEvaluable);
    }

    #[tokio::test]
    async fn missing_pieces_fail_their_rows() {
        let mut probe = FakeProbe::healthy();
        probe.concurrency = Ok(None);
        probe.controller = Ok(None);
        let rows = run(&probe, &gate_with(0)).await;
        let status = |c: CheckId| rows.iter().find(|r| r.check == c).unwrap().status;
        assert_eq!(status(CheckId::AssignPositionConcurrency), Status::Fail);
        assert_eq!(status(CheckId::ControllerSchedule), Status::Fail);
        assert_eq!(status(CheckId::GateRuleset), Status::Warn);
    }

    // --- The catalogue ---

    #[test]
    fn every_check_names_a_requirement_and_a_fix() {
        for check in CheckId::ALL {
            let spec = check.spec();
            assert!(!spec.name.is_empty());
            assert!(
                spec.fix_url.starts_with("https://") || spec.fix_url.starts_with('#'),
                "{check:?}: {}",
                spec.fix_url
            );
            let id = spec.requirement;
            assert!(
                id.starts_with('O')
                    || id.starts_with('F')
                    || id.starts_with('C')
                    || id.starts_with('N'),
                "{check:?}: {id}"
            );
        }
    }

    #[test]
    fn every_requirement_named_exists_in_the_requirements_doc() {
        let doc = include_str!("../../../docs/REQUIREMENTS.md");
        for check in CheckId::ALL {
            let id = check.spec().requirement;
            assert!(
                doc.contains(&format!("| {id} |")),
                "{check:?} names {id}, which docs/REQUIREMENTS.md does not define"
            );
        }
    }

    #[test]
    fn the_runbook_checklist_has_one_item_per_check_between_the_markers() {
        let md = runbook_checklist();
        assert!(md.starts_with(RUNBOOK_BEGIN));
        assert!(md.trim_end().ends_with(RUNBOOK_END));
        assert_eq!(md.matches("\n- [ ] ").count(), CheckId::ALL.len());
    }
}
