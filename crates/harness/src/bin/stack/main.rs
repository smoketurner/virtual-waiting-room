//! Runs the waiting room locally, end to end, and checks the design's claims
//! against what happened.
//!
//! The shipped Lambda binaries run as local processes behind a Lambda Runtime
//! API host. DynamoDB and SSM are the AWS Emulator (or `ExtendDB` for
//! DynamoDB), which enforces DynamoDB's 1,000 writes/s per partition key. The
//! edge — CloudFront's caching and request collapsing, API Gateway's throttles
//! and validation, the SQS join queue and its event source mapping, the open
//! schedule and the controller's cadence — is modelled in this process, and
//! the shipped CloudFront Function decides every protected request. Visitors
//! follow the waiting page's protocol.
//!
//! ```text
//! cargo run -p harness --bin stack -- --visitors 20000 --concurrency 2000 --session 300 --countdown 60
//! cargo run -p harness --bin stack -- --visitors 100000 --arrival herd --herd-window 10
//! ```
//!
//! Prerequisites: Docker with the AWS Emulator running (`make stack-emulator`),
//! and Node.js for the gate.

mod aws;
mod lambda;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use tokio::io::AsyncBufReadExt as _;
use tokio::time::Instant;

use harness::report::{Registration, Report};
use harness::rng::Rng;
use harness::scenario::{Arrival, Scenario};
use harness::visitor::World;
use harness::{edge, metrics, queue, report, scenario};
use jiff::SignedDuration;

#[derive(Debug, Clone)]
struct Args {
    scenario: Scenario,
    emulator: String,
    emulator_ca: PathBuf,
    dynamodb: Option<String>,
    read_envs: usize,
    token_envs: usize,
    assign_envs: usize,
    pollers: usize,
    max_environments: usize,
    page_visitors: usize,
    max_seconds: u64,
    skip_build: bool,
    out: PathBuf,
}

fn parse_args() -> Result<Args> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let mut a = Args {
        scenario: Scenario {
            visitors: 2000,
            after_open: 0,
            after_open_from: SignedDuration::ZERO,
            concurrency: 500,
            session: SignedDuration::from_mins(5),
            countdown: SignedDuration::from_secs(30),
            arrival: Arrival::Late,
            herd_window: SignedDuration::from_secs(10),
            no_show: 0.0,
            duplicate_rate: 0.0,
            scale: 1.0,
            seed: now,
        },
        emulator: "https://127.0.0.1:9297".to_owned(),
        emulator_ca: PathBuf::new(),
        dynamodb: None,
        read_envs: 4,
        token_envs: 8,
        assign_envs: 8,
        pollers: 8,
        max_environments: 128,
        page_visitors: 20,
        max_seconds: 3600,
        skip_build: false,
        out: PathBuf::from(format!("target/stack-runs/{now}")),
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().context(format!("{flag} needs a value"));
        match flag.as_str() {
            "--visitors" => a.scenario.visitors = value()?.parse()?,
            "--concurrency" => a.scenario.concurrency = value()?.parse()?,
            "--session" => a.scenario.session = SignedDuration::from_secs(value()?.parse()?),
            "--countdown" => a.scenario.countdown = SignedDuration::from_secs(value()?.parse()?),
            "--arrival" => {
                a.scenario.arrival = match value()?.as_str() {
                    "late" => Arrival::Late,
                    "uniform" => Arrival::Uniform,
                    "herd" => Arrival::Herd,
                    other => anyhow::bail!("--arrival must be late, uniform or herd: {other}"),
                }
            }
            "--herd-window" => {
                a.scenario.herd_window = SignedDuration::from_secs(value()?.parse()?);
            }
            "--no-show" => a.scenario.no_show = value()?.parse()?,
            "--duplicate-rate" => a.scenario.duplicate_rate = value()?.parse()?,
            "--seed" => a.scenario.seed = value()?.parse()?,
            "--emulator" => a.emulator = value()?,
            "--emulator-ca" => a.emulator_ca = PathBuf::from(value()?),
            "--dynamodb" => a.dynamodb = Some(value()?),
            "--read-environments" => a.read_envs = value()?.parse()?,
            "--token-environments" => a.token_envs = value()?.parse()?,
            "--assign-environments" => a.assign_envs = value()?.parse()?,
            "--pollers" => a.pollers = value()?.parse()?,
            "--max-environments" => a.max_environments = value()?.parse()?,
            "--scale" => a.scenario.scale = value()?.parse()?,
            "--page-visitors" => a.page_visitors = value()?.parse()?,
            "--max-seconds" => a.max_seconds = value()?.parse()?,
            "--skip-build" => a.skip_build = true,
            "--out" => a.out = PathBuf::from(value()?),
            other => anyhow::bail!("unknown flag {other}"),
        }
    }
    anyhow::ensure!(
        !a.emulator_ca.as_os_str().is_empty(),
        "--emulator-ca is required: the ca.pem the AWS Emulator wrote"
    );
    anyhow::ensure!(
        (0.0..1.0).contains(&a.scenario.no_show),
        "--no-show must be in [0, 1)"
    );
    Ok(a)
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    match run().await {
        Ok(Verdict::Passed) => ExitCode::SUCCESS,
        Ok(Verdict::Failed) => ExitCode::from(1),
        Ok(Verdict::Inconclusive) => ExitCode::from(3),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

/// How long a caller behind API Gateway waits for an environment and a
/// response.
const API_GATEWAY_TIMEOUT: Duration = Duration::from_secs(29);

const FUNCTIONS: [&str; 4] = ["read", "generate_token", "assign_position", "open_event"];
const PROTECTED_PATH: &str = "/shop/checkout";
const SIGNING_KEY_PARAMETER: &str = "/stack/signing-key";

async fn run() -> Result<Verdict> {
    let mut args = parse_args()?;
    std::fs::create_dir_all(&args.out)?;
    let logs = args.out.join("logs");
    std::fs::create_dir_all(&logs)?;

    let binaries = if args.skip_build {
        FUNCTIONS
            .iter()
            .map(|n| ((*n).to_owned(), PathBuf::from("target/stack-bin").join(n)))
            .collect()
    } else {
        say("building the Lambda binaries for this machine");
        lambda::build(&FUNCTIONS, Path::new("target/stack-bin"))?
    };

    let started_std = std::time::Instant::now();
    let started = Instant::now();
    let run_start = harness::clock::now();
    // Tables are per run, so a seed can be reused against a long-lived emulator.
    let run_id = format!(
        "r{}-{}",
        args.scenario.seed % 1_000_000,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() % 100_000_000
    );
    let event_id = "local".to_owned();
    let tables = aws::Tables::new(&format!("stack-{run_id}"));

    // The endpoint every SDK call goes through, and the join queue it serves.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy = listener.local_addr()?;
    let queue = Arc::new(queue::JoinQueue::new(
        format!("http://{proxy}/{}/join", aws::ACCOUNT_ID),
        args.scenario.duplicate_rate,
        args.scenario.seed ^ 0x5151,
    ));
    let ca = std::fs::read(&args.emulator_ca).context("read --emulator-ca")?;
    let upstreams = aws::Upstreams {
        emulator: args.emulator.clone(),
        dynamodb: args
            .dynamodb
            .clone()
            .unwrap_or_else(|| args.emulator.clone()),
    };
    let aws_counts = aws::start_proxy(listener, upstreams, &ca, Arc::clone(&queue), started_std)?;
    let config = aws::sdk_config(proxy).await;
    let ddb = aws_sdk_dynamodb::Client::new(&config);
    let ssm = aws_sdk_ssm::Client::new(&config);

    say("creating tables and seeding the event");
    aws::create_tables(&ddb, &tables).await?;
    let open_at = args.scenario.open_at(harness::clock::now());
    // Arrivals are timed from the start of the run, so the countdown is the
    // exact time from it to the open, setup and rounding included.
    args.scenario.countdown = open_at.duration_since(run_start);
    aws::seed_event(&ddb, &tables, &event_id, args.scenario.concurrency, open_at).await?;
    let mut key_bytes = [0u8; 32];
    Rng::new(args.scenario.seed ^ 0x006b_6579).fill(&mut key_bytes);
    let signing_key = base64::engine::general_purpose::STANDARD.encode(key_bytes);
    aws::put_signing_key(&ssm, SIGNING_KEY_PARAMETER, &signing_key).await?;

    say("starting the gate");
    let gate_port = free_port().await?;
    let gate_config =
        serde_json::json!({"v": 1, "s": 0, "f": 0, "r": [["p", "/shop"]]}).to_string();
    let mut gate = tokio::process::Command::new("node")
        .arg("crates/harness/js/gate-sidecar.js")
        .arg(gate_port.to_string())
        .arg(&event_id)
        .arg(&gate_config)
        .arg(&signing_key)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("start node")?;
    if let Some(stdout) = gate.stdout.take() {
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let ready = tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await;
        anyhow::ensure!(
            matches!(ready, Ok(Ok(Some(ref l))) if l == "ready"),
            "the gate did not start"
        );
    }

    say("starting the functions");
    let common = |extra: &[(&str, String)]| {
        let mut env = vec![
            ("AWS_ENDPOINT_URL".to_owned(), format!("http://{proxy}")),
            (
                "AWS_ACCESS_KEY_ID".to_owned(),
                aws::ACCESS_KEY_ID.to_owned(),
            ),
            (
                "AWS_SECRET_ACCESS_KEY".to_owned(),
                aws::SECRET_ACCESS_KEY.to_owned(),
            ),
            ("AWS_REGION".to_owned(), aws::REGION.to_owned()),
            ("AWS_EC2_METADATA_DISABLED".to_owned(), "true".to_owned()),
            ("AWS_ACCOUNT_ID".to_owned(), aws::ACCOUNT_ID.to_owned()),
            ("COUNTERS_TABLE".to_owned(), tables.counters.clone()),
            ("PREQUEUE_TABLE".to_owned(), tables.prequeue.clone()),
            ("POSITIONS_TABLE".to_owned(), tables.positions.clone()),
            ("EVENT_ID".to_owned(), event_id.clone()),
        ];
        for (k, v) in extra {
            env.push(((*k).to_owned(), v.clone()));
        }
        env
    };
    let bin = |name: &str| binaries.get(name).cloned().unwrap_or_default();
    // The poll policy the dev root ships (`poll_floor_ms`, `poll_ceiling_ms`,
    // `poll_divisor`) and the session settings `generate_token.tf` passes.
    let read_bin = bin("read");
    let read = lambda::start(lambda::Spec {
        name: "read",
        binary: &read_bin,
        environments: args.read_envs,
        max_environments: args.max_environments,
        env: common(&[
            ("POLL_FLOOR_MS", "5000".to_owned()),
            ("POLL_CEILING_MS", "30000".to_owned()),
            ("POLL_DIVISOR", "10".to_owned()),
            (
                "SESSION_TTL_SECS",
                args.scenario.session.as_secs().to_string(),
            ),
        ]),
        log_dir: &logs,
        timeout: API_GATEWAY_TIMEOUT,
    })
    .await?;
    let token_bin = bin("generate_token");
    let generate_token = lambda::start(lambda::Spec {
        name: "generate_token",
        binary: &token_bin,
        environments: args.token_envs,
        max_environments: args.max_environments,
        env: common(&[
            ("SESSION_COOKIE_NAME", "vwr_session".to_owned()),
            (
                "SESSION_TTL_SECS",
                args.scenario.session.as_secs().to_string(),
            ),
            ("SIGNING_KEY_PARAMETER", SIGNING_KEY_PARAMETER.to_owned()),
            ("SIGNING_KEY_FINGERPRINT", "local".to_owned()),
        ]),
        log_dir: &logs,
        timeout: API_GATEWAY_TIMEOUT,
    })
    .await?;
    let assign_bin = bin("assign_position");
    let assign = lambda::start(lambda::Spec {
        name: "assign_position",
        binary: &assign_bin,
        environments: args.assign_envs,
        max_environments: args.max_environments,
        env: common(&[("JOIN_QUEUE_URL", queue.url().to_owned())]),
        log_dir: &logs,
        timeout: Duration::from_secs(30),
    })
    .await?;
    let open_bin = bin("open_event");
    let open = lambda::start(lambda::Spec {
        name: "open_event",
        binary: &open_bin,
        environments: 1,
        max_environments: 1,
        env: common(&[("JOIN_QUEUE_URL", queue.url().to_owned())]),
        log_dir: &logs,
        timeout: Duration::from_mins(15),
    })
    .await?;

    let metrics = Arc::new(metrics::Metrics::new(started));
    let edge = Arc::new(edge::Edge::new(
        args.scenario.throttles(),
        Arc::new(lambda::Shipped::new(
            Arc::clone(&read),
            Arc::clone(&generate_token),
            format!("http://127.0.0.1:{gate_port}/"),
        )),
        Arc::clone(&queue),
        &event_id,
        Arc::clone(&metrics),
    ));

    // The event source mapping.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut pollers = Vec::new();
    for _ in 0..args.pollers {
        let queue = Arc::clone(&queue);
        let assign = Arc::clone(&assign);
        let stop = stop_rx.clone();
        pollers.push(tokio::spawn(async move {
            queue.poll(assign.as_ref(), &stop).await
        }));
    }

    // The open schedule and the controller's ten-second cadence.
    let open_instant = started + args.scenario.countdown.unsigned_abs();
    let opener = {
        let open = Arc::clone(&open);
        let event_id = event_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(open_instant).await;
            let t = Instant::now();
            let reply = open
                .invoke(&serde_json::json!({ "event_id": event_id }))
                .await;
            (reply.map_err(|e| format!("{e:#}")), t.elapsed())
        })
    };
    let controller_store = controller::dynamo::DynamoStore::new(
        ddb.clone(),
        tables.counters.clone(),
        event_id.clone(),
        args.scenario.session,
    );
    let controller_task = {
        let event_id = event_id.clone();
        let stop = stop_rx.clone();
        tokio::spawn(
            async move { scenario::run_controller(&controller_store, &event_id, stop).await },
        )
    };

    // The visitors.
    let budget = args
        .scenario
        .budget(SignedDuration::from_secs(i64::try_from(args.max_seconds)?));
    let deadline = started + budget.unsigned_abs();
    let world = Arc::new(World {
        edge: Arc::clone(&edge),
        started,
        deadline,
        protected_path: PROTECTED_PATH.to_owned(),
    });
    say(&format!(
        "{} visitors, {} at once for {} s each, opening in {} s, deadline in {} s",
        args.scenario.visitors,
        args.scenario.concurrency,
        args.scenario.session.as_secs(),
        args.scenario.countdown.as_secs(),
        budget.as_secs()
    ));
    let handles = args.scenario.spawn_visitors(&world);

    // A sample of visitors run the shipped waiting.js itself, over HTTP, so
    // the page is checked against the stack and not only the model of it.
    let pages = if args.page_visitors > 0 {
        let addr = edge::serve(Arc::clone(&edge)).await?;
        let mut cmd = tokio::process::Command::new("node");
        cmd.arg("crates/harness/js/page-driver.js")
            .arg(format!("http://{addr}"))
            .arg(PROTECTED_PATH)
            .arg(budget.as_millis().to_string());
        let mut page_rng = Rng::new(args.scenario.seed ^ 0x7061_6765);
        for _ in 0..args.page_visitors {
            let at = args.scenario.arrival_after(page_rng.unit());
            cmd.arg(at.as_millis().to_string());
        }
        Some(
            cmd.stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .context("start the page driver")?,
        )
    } else {
        None
    };

    let progress = {
        let queue = Arc::clone(&queue);
        let edge = Arc::clone(&edge);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let admitted: u64 = edge
                    .metrics
                    .origin_series("protected", 100_000)
                    .iter()
                    .sum();
                say(&format!(
                    "t={:>5}s  admitted {admitted:>8}  queue {:>6}",
                    started.elapsed().as_secs(),
                    queue.visible()
                ));
            }
        })
    };

    let records = scenario::collect(handles).await;
    let page_results: Vec<String> = match pages {
        Some(child) => {
            let out = child.wait_with_output().await?;
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .map(|v| {
                    v.get("outcome")
                        .and_then(|o| o.as_str())
                        .unwrap_or("?")
                        .to_owned()
                })
                .collect()
        }
        None => Vec::new(),
    };
    progress.abort();
    let elapsed = started.elapsed();
    let _ = stop_tx.send(true);
    for p in pollers {
        let _ = p.await;
    }
    let passes = controller_task.await.unwrap_or_default();
    let (opened, open_took) = opener
        .await
        .unwrap_or((Err("the open task failed".to_owned()), Duration::ZERO));

    say("auditing");
    let status = edge
        .request(edge::ViewerRequest {
            method: "GET",
            path: "/_wr/v1/status".to_owned(),
            viewer_ip: "10.0.0.1".to_owned(),
            ..edge::ViewerRequest::default()
        })
        .await
        .json()
        .unwrap_or_default();
    let published = report::parse_published(&status);
    let registrations = scan_prequeue(&ddb, &tables.prequeue).await?;

    let functions = [&read, &generate_token, &assign, &open];
    let logged_errors = scan_logs(&logs);
    let mut report = report::build(&report::Evidence {
        scenario: &args.scenario,
        records: &records,
        metrics: &metrics,
        queue: queue.totals(),
        published: published.as_ref(),
        registrations: &registrations,
        opened: opened.is_ok(),
        draw_closes: open_at + wr_common::DRAW_GRACE,
        open_took,
        passes: &passes,
        elapsed,
        failed_invocations: failed_invocations(&functions),
        logged_errors: &logged_errors,
    });
    stack_checks(
        &mut report,
        &functions,
        &metrics,
        &aws_counts,
        &page_results,
    );

    for f in [&read, &generate_token, &assign, &open] {
        f.shutdown().await;
    }
    let _ = gate.kill().await;

    let text = report.render();
    println!("{text}");
    std::fs::write(args.out.join("report.txt"), &text)?;
    std::fs::write(
        args.out.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    say(&format!("report and logs in {}", args.out.display()));
    if report.conclusive() {
        Ok(Verdict::from(report.passed()))
    } else {
        Ok(Verdict::Inconclusive)
    }
}

enum Verdict {
    Passed,
    Failed,
    Inconclusive,
}

impl From<bool> for Verdict {
    fn from(passed: bool) -> Self {
        if passed { Self::Passed } else { Self::Failed }
    }
}

fn say(line: &str) {
    println!("[stack] {line}");
}

async fn free_port() -> Result<u16> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    Ok(l.local_addr()?.port())
}

async fn scan_prequeue(ddb: &aws_sdk_dynamodb::Client, table: &str) -> Result<Vec<Registration>> {
    let mut out = Vec::new();
    let mut start = None;
    loop {
        let page = ddb
            .scan()
            .table_name(table)
            .set_exclusive_start_key(start)
            .send()
            .await
            .context("scan PreQueue")?;
        for item in page.items() {
            let s = |k: &str| {
                item.get(k)
                    .and_then(|v| v.as_n().ok())
                    .and_then(|n| n.parse::<u64>().ok())
            };
            let (Some(AttributeValue::S(r)), Some(shard), Some(local)) =
                (item.get("r"), s("s"), s("l"))
            else {
                continue;
            };
            out.push(Registration {
                request_id: r.clone(),
                shard: usize::try_from(shard).unwrap_or(usize::MAX),
                local,
            });
        }
        start = page.last_evaluated_key().cloned();
        if start.is_none() {
            return Ok(out);
        }
    }
}

/// Invocations that returned an error, not counting the ones that found no
/// environment in time, which the harness checks on its own.
fn failed_invocations(functions: &[&Arc<lambda::Function>]) -> u64 {
    let mut failed = 0u64;
    for f in functions {
        let timeouts = f.failures.lock().map_or(0, |failures| {
            failures
                .get(&lambda::Failure::Timeout)
                .copied()
                .unwrap_or(0)
        });
        failed += f
            .errors
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_sub(timeouts);
    }
    failed
}

/// The checks only the shipped binaries can answer: the page itself, each
/// function's invocations, the harness's own plumbing, and what `DynamoDB`
/// was asked.
fn stack_checks(
    r: &mut Report,
    functions: &[&Arc<lambda::Function>],
    metrics: &metrics::Metrics,
    aws: &std::sync::Mutex<aws::AwsCounts>,
    pages: &[String],
) {
    if !pages.is_empty() {
        let admitted = pages.iter().filter(|o| *o == "admitted").count();
        let mut others: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
        for o in pages.iter().filter(|o| *o != "admitted") {
            *others.entry(o.as_str()).or_default() += 1;
        }
        r.check(
            "Visitors running the shipped waiting.js are admitted and pass the gate",
            admitted == pages.len(),
            format!(
                "{admitted} of {} pages admitted{}",
                pages.len(),
                others
                    .iter()
                    .map(|(k, v)| format!("; {v} {k}"))
                    .collect::<String>()
            ),
        );
    }
    let mut timeouts = 0u64;
    for f in functions {
        if let Ok(failures) = f.failures.lock() {
            timeouts += failures
                .get(&lambda::Failure::Timeout)
                .copied()
                .unwrap_or(0);
        }
        let inv = f.invocations.load(std::sync::atomic::Ordering::Relaxed);
        let err = f.errors.load(std::sync::atomic::Ordering::Relaxed);
        r.fact(
            format!("{} invocations", f.name),
            format!(
                "{inv} ({err} failed{}) over {} environments",
                f.failures
                    .lock()
                    .map(|r| r
                        .iter()
                        .map(|(k, v)| format!(": {v} {k:?}"))
                        .collect::<String>())
                    .unwrap_or_default(),
                f.environments()
            ),
        );
    }
    r.harness_check(
        "Every invocation found an environment within API Gateway's 29 s",
        timeouts == 0,
        format!("{timeouts} invocations timed out"),
    );
    let sidecar = metrics
        .viewer_totals()
        .get(&("protected", 599))
        .copied()
        .unwrap_or(0);
    r.harness_check(
        "The gate sidecar answered every protected request",
        sidecar == 0,
        format!("{sidecar} unanswered"),
    );
    if let Ok(aws) = aws.lock() {
        let mut per_op: std::collections::BTreeMap<&str, (u64, u64)> =
            std::collections::BTreeMap::new();
        for ((_, op), n) in &aws.calls {
            let entry = per_op.entry(op.as_str()).or_default();
            entry.0 += n;
            entry.1 = entry.1.max(*n);
        }
        for (op, (total, peak)) in per_op {
            r.fact(format!("{op} calls"), format!("{total} (peak {peak}/s)"));
        }
        let throttled: u64 = aws.throttled.values().sum();
        r.fact("DynamoDB throttled calls", throttled);
        r.check(
            "DynamoDB answers every call it does not throttle",
            aws.errors.is_empty(),
            if aws.errors.is_empty() {
                "no errors".to_owned()
            } else {
                aws.errors
                    .iter()
                    .map(|(k, v)| format!("{k} x{v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        );
    }
}

/// Error-level lines in the functions' logs, by event name.
fn scan_logs(dir: &Path) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let Ok(text) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        for line in text.lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if v.get("level").and_then(|l| l.as_str()) != Some("ERROR") {
                continue;
            }
            let fields = v.get("fields");
            let name = fields
                .and_then(|f| f.get("event").or_else(|| f.get("message")))
                .and_then(|m| m.as_str())
                .unwrap_or("unnamed")
                .to_owned();
            *out.entry(name).or_default() += 1;
        }
    }
    out
}
