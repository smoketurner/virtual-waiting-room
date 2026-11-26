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
//! cargo run -p harness --bin stack -- --visitors 20000 --concurrency 2000 --session 60 --countdown 60
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

use harness::report::{Published, Registration, Report};
use harness::rng::Rng;
use harness::visitor::{Outcome, Record, World};
use harness::{edge, metrics, queue, report, visitor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arrival {
    /// Arrive during the countdown, bunched towards the start (the design's
    /// cost model: time before the open is the countdown times `u^5`).
    Late,
    /// Arrive uniformly during the countdown.
    Uniform,
    /// Nobody early: everyone arrives within `herd_window` seconds after the
    /// open.
    Herd,
}

#[derive(Debug, Clone)]
struct Args {
    visitors: usize,
    concurrency: u32,
    session: u64,
    countdown: u64,
    arrival: Arrival,
    herd_window: u64,
    no_show: f64,
    duplicate_rate: f64,
    seed: u64,
    emulator: String,
    emulator_ca: PathBuf,
    dynamodb: Option<String>,
    read_envs: usize,
    token_envs: usize,
    assign_envs: usize,
    pollers: usize,
    max_environments: usize,
    scale: f64,
    page_visitors: usize,
    max_seconds: u64,
    skip_build: bool,
    out: PathBuf,
}

fn parse_args() -> Result<Args> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let mut a = Args {
        visitors: 2000,
        concurrency: 500,
        session: 60,
        countdown: 30,
        arrival: Arrival::Late,
        herd_window: 10,
        no_show: 0.0,
        duplicate_rate: 0.0,
        seed: now,
        emulator: "https://127.0.0.1:9297".to_owned(),
        emulator_ca: PathBuf::new(),
        dynamodb: None,
        read_envs: 4,
        token_envs: 8,
        assign_envs: 8,
        pollers: 8,
        max_environments: 128,
        scale: 1.0,
        page_visitors: 20,
        max_seconds: 3600,
        skip_build: false,
        out: PathBuf::from(format!("target/stack-runs/{now}")),
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().context(format!("{flag} needs a value"));
        match flag.as_str() {
            "--visitors" => a.visitors = value()?.parse()?,
            "--concurrency" => a.concurrency = value()?.parse()?,
            "--session" => a.session = value()?.parse()?,
            "--countdown" => a.countdown = value()?.parse()?,
            "--arrival" => {
                a.arrival = match value()?.as_str() {
                    "late" => Arrival::Late,
                    "uniform" => Arrival::Uniform,
                    "herd" => Arrival::Herd,
                    other => anyhow::bail!("--arrival must be late, uniform or herd: {other}"),
                }
            }
            "--herd-window" => a.herd_window = value()?.parse()?,
            "--no-show" => a.no_show = value()?.parse()?,
            "--duplicate-rate" => a.duplicate_rate = value()?.parse()?,
            "--seed" => a.seed = value()?.parse()?,
            "--emulator" => a.emulator = value()?,
            "--emulator-ca" => a.emulator_ca = PathBuf::from(value()?),
            "--dynamodb" => a.dynamodb = Some(value()?),
            "--read-environments" => a.read_envs = value()?.parse()?,
            "--token-environments" => a.token_envs = value()?.parse()?,
            "--assign-environments" => a.assign_envs = value()?.parse()?,
            "--pollers" => a.pollers = value()?.parse()?,
            "--max-environments" => a.max_environments = value()?.parse()?,
            "--scale" => a.scale = value()?.parse()?,
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
        (0.0..1.0).contains(&a.no_show),
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
    let args = parse_args()?;
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
    // Tables are per run, so a seed can be reused against a long-lived emulator.
    let run_id = format!(
        "r{}-{}",
        args.seed % 1_000_000,
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() % 100_000_000
    );
    let event_id = "local".to_owned();
    let tables = aws::Tables::new(&format!("stack-{run_id}"));

    // The endpoint every SDK call goes through, and the join queue it serves.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy = listener.local_addr()?;
    let queue = Arc::new(queue::JoinQueue::new(
        format!("http://{proxy}/{}/join", aws::ACCOUNT_ID),
        args.duplicate_rate,
        args.seed ^ 0x5151,
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
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let open_at = now + args.countdown;
    aws::seed_event(&ddb, &tables, &event_id, args.concurrency, open_at).await?;
    let mut key_bytes = [0u8; 32];
    Rng::new(args.seed ^ 0x006b_6579).fill(&mut key_bytes);
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
            ("SESSION_TTL_SECS", args.session.to_string()),
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
            ("SESSION_TTL_SECS", args.session.to_string()),
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
        env: common(&[]),
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
        // AWS's default account limit and the stage throttles `core/api.tf`
        // defaults to, shrunk by --scale for a scaled-down model of a larger
        // event.
        edge::Throttles {
            account_rate: 10_000.0 * args.scale,
            account_burst: 5_000.0 * args.scale,
            join_rate: 7_000.0 * args.scale,
            join_burst: 3_500.0 * args.scale,
            method_rate: 10_000.0 * args.scale,
            method_burst: 5_000.0 * args.scale,
        },
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
    let open_instant = started + Duration::from_secs(args.countdown);
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
        args.session,
    );
    let controller_task = {
        let event_id = event_id.clone();
        let mut stop = stop_rx.clone();
        tokio::spawn(async move {
            let mut passes = Vec::new();
            loop {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                let outcome = controller::run_pass(&controller_store, &event_id, now).await;
                passes.push(outcome.map_err(|e| e.to_string()));
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(controller::INTERVAL_SECS)) => {}
                    _ = stop.changed() => return passes,
                }
            }
        })
    };

    // The visitors.
    // A full origin turns over once a session length, and a slot is seen free
    // up to a pass after its session ends.
    let turnover = args.session + controller::INTERVAL_SECS;
    let expected_drain =
        (args.visitors as u64).div_ceil(u64::from(args.concurrency.max(1))) * turnover;
    let last_arrival = match args.arrival {
        Arrival::Herd => args.countdown + args.herd_window,
        Arrival::Late | Arrival::Uniform => args.countdown,
    };
    let budget = (last_arrival + expected_drain * 3 / 2 + 300).min(args.max_seconds);
    let deadline = started + Duration::from_secs(budget);
    let world = Arc::new(World {
        edge: Arc::clone(&edge),
        started,
        deadline,
        protected_path: PROTECTED_PATH.to_owned(),
    });
    say(&format!(
        "{} visitors, {} at once for {} s each, opening in {} s, deadline in {budget} s",
        args.visitors, args.concurrency, args.session, args.countdown
    ));
    let mut rng = Rng::new(args.seed);
    let mut handles = Vec::with_capacity(args.visitors);
    for _ in 0..args.visitors {
        let mut v = rng.fork();
        let u = v.unit();
        let at = match args.arrival {
            Arrival::Late => args.countdown as f64 * (1.0 - u.powi(5)),
            Arrival::Uniform => args.countdown as f64 * u,
            Arrival::Herd => args.countdown as f64 + args.herd_window as f64 * u,
        };
        let no_show = v.unit() < args.no_show;
        let arrive = started + Duration::from_secs_f64(at);
        handles.push(tokio::spawn(visitor::visit(
            Arc::clone(&world),
            v,
            arrive,
            no_show,
        )));
    }

    // A sample of visitors run the shipped waiting.js itself, over HTTP, so
    // the page is checked against the stack and not only the model of it.
    let pages = if args.page_visitors > 0 {
        let addr = edge::serve(Arc::clone(&edge)).await?;
        let mut cmd = tokio::process::Command::new("node");
        cmd.arg("crates/harness/js/page-driver.js")
            .arg(format!("http://{addr}"))
            .arg(PROTECTED_PATH)
            .arg((budget * 1000).to_string());
        let mut page_rng = Rng::new(args.seed ^ 0x7061_6765);
        for _ in 0..args.page_visitors {
            let u = page_rng.unit();
            let at = match args.arrival {
                Arrival::Late => args.countdown as f64 * (1.0 - u.powi(5)),
                Arrival::Uniform => args.countdown as f64 * u,
                Arrival::Herd => args.countdown as f64 + args.herd_window as f64 * u,
            };
            cmd.arg(((at * 1000.0) as u64).to_string());
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

    let mut records: Vec<Record> = Vec::with_capacity(args.visitors);
    for h in handles {
        if let Ok(r) = h.await {
            records.push(r);
        }
    }
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
    let published = parse_published(&status);
    let registrations = scan_prequeue(&ddb, &tables.prequeue).await?;

    let report = build_report(&Evidence {
        args: &args,
        records: &records,
        metrics: &metrics,
        aws: &aws_counts,
        queue: queue.totals(),
        published: published.as_ref(),
        registrations: &registrations,
        opened: opened.is_ok(),
        open_at_ms: args.countdown.saturating_mul(1000),
        open_took,
        passes: &passes,
        elapsed,
        functions: &[&read, &generate_token, &assign, &open],
        logs: &logs,
        pages: &page_results,
    });

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

fn parse_published(status: &serde_json::Value) -> Option<Published> {
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

struct Evidence<'a> {
    args: &'a Args,
    records: &'a [Record],
    metrics: &'a metrics::Metrics,
    aws: &'a std::sync::Mutex<aws::AwsCounts>,
    queue: queue::QueueTotals,
    published: Option<&'a Published>,
    registrations: &'a [Registration],
    opened: bool,
    /// When the open fired, ms from the start of the run.
    open_at_ms: u64,
    open_took: Duration,
    passes: &'a [Result<controller::PassOutcome, String>],
    elapsed: Duration,
    functions: &'a [&'a Arc<lambda::Function>],
    logs: &'a Path,
    /// How each page running the shipped `waiting.js` ended.
    pages: &'a [String],
}

fn build_report(e: &Evidence<'_>) -> Report {
    let mut r = Report::default();
    let args = e.args;
    let seconds = e.elapsed.as_secs();

    r.fact("visitors", args.visitors);
    r.fact("origin concurrency", args.concurrency);
    r.fact("session length (s)", args.session);
    r.fact(
        "arrival",
        format!("{:?}, countdown {} s", args.arrival, args.countdown),
    );
    r.fact("no-show share", args.no_show);
    r.fact("duplicate delivery rate", args.duplicate_rate);
    r.fact("API Gateway limits scaled by", args.scale);
    r.fact("run length (s)", seconds);
    for (outcome, n) in report::outcome_counts(e.records) {
        r.fact(format!("outcome: {outcome}"), n);
    }

    // --- positions ----------------------------------------------------------
    let dupes = report::duplicate_positions(e.records);
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
            x.joined_ms.is_some() && x.position.is_none() && x.outcome != Outcome::StillWaiting
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

    if !e.pages.is_empty() {
        let admitted = e.pages.iter().filter(|o| *o == "admitted").count();
        let mut others: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
        for o in e.pages.iter().filter(|o| *o != "admitted") {
            *others.entry(o.as_str()).or_default() += 1;
        }
        r.check(
            "Visitors running the shipped waiting.js are admitted and pass the gate",
            admitted == e.pages.len(),
            format!(
                "{admitted} of {} pages admitted{}",
                e.pages.len(),
                others
                    .iter()
                    .map(|(k, v)| format!("; {v} {k}"))
                    .collect::<String>()
            ),
        );
    }

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
        let recomputed = report::audit(published, e.registrations);
        let told: std::collections::HashMap<&str, u64> = e
            .records
            .iter()
            .filter(|x| !x.live_join)
            .filter_map(|x| x.position.map(|p| (x.request_id.as_str(), p)))
            .collect();
        let mismatched = told
            .iter()
            .filter(|(id, p)| recomputed.get(**id).is_some_and(|q| q != *p))
            .count();
        let distinct: std::collections::HashSet<u64> = recomputed.values().copied().collect();
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
            let rho = report::spearman(&xs, &ys);
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
    let (busiest, start) = report::busiest_window(&admissions, 1);
    r.fact(
        "busiest second of admissions",
        format!("{busiest} (from t={start} s)"),
    );
    let sessions: Vec<u64> = e.records.iter().filter_map(|x| x.session_ends).collect();
    let peak = report::peak_live(&sessions, args.session);
    let (first_start, live) = report::live_by_second(&sessions, args.session);
    r.fact("first session start (epoch s)", first_start);
    r.series.insert("live sessions per second".to_owned(), live);
    r.series
        .insert("admissions per second".to_owned(), admissions.clone());
    let limit = u64::from(args.concurrency);
    r.check(
        "No more sessions are ever live than the origin takes",
        peak <= limit,
        format!("most live at once: {peak} against {limit}"),
    );
    // While visitors are still waiting, the origin should be close to full.
    // It runs a little under at each turnover: a slot is seen free up to a
    // pass after its session ends, and its replacement has to poll in.
    if let Some(occupancy) = report::occupancy(&sessions, args.session, limit) {
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
            if let Some(v) = report::percentile(series, p) {
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
    let mut function_errors = 0u64;
    let mut timeouts = 0u64;
    for f in e.functions {
        if let Ok(failures) = f.failures.lock() {
            timeouts += failures
                .get(&lambda::Failure::Timeout)
                .copied()
                .unwrap_or(0);
        }
        let inv = f.invocations.load(std::sync::atomic::Ordering::Relaxed);
        let err = f.errors.load(std::sync::atomic::Ordering::Relaxed);
        function_errors += err;
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
    let function_errors = function_errors - timeouts;
    r.harness_check(
        "Every invocation found an environment within API Gateway's 29 s",
        timeouts == 0,
        format!("{timeouts} invocations timed out"),
    );
    let sidecar = e
        .metrics
        .viewer_totals()
        .get(&("protected", 599))
        .copied()
        .unwrap_or(0);
    r.harness_check(
        "The gate sidecar answered every protected request",
        sidecar == 0,
        format!("{sidecar} unanswered"),
    );
    let log_errors = scan_logs(e.logs);
    r.check(
        "No function invocation fails and no function logs an error",
        function_errors == 0 && log_errors.is_empty() && failed_passes == 0,
        format!(
            "{function_errors} failed invocations, {failed_passes} failed controller passes, logged errors: {}",
            if log_errors.is_empty() {
                "none".to_owned()
            } else {
                log_errors.iter().map(|(k, v)| format!("{k} x{v}")).collect::<Vec<_>>().join(", ")
            }
        ),
    );
    if let Ok(aws) = e.aws.lock() {
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
    r
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
