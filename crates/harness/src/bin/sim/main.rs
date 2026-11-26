//! Runs the waiting room's own logic in one process on simulated time, and
//! checks the design's claims against what happened.
//!
//! The functions are their libraries: `read` and `generate_token` answer the
//! edge through their API Gateway handlers, `assign_position` takes the join
//! queue's batches through its SQS handler, and the open and the controller
//! run their library calls on their schedule. They share one
//! [`harness::memory::Memory`] in place of `DynamoDB`. The edge, the queue, the
//! visitors and the report are the ones `stack` uses.
//!
//! Time is tokio's paused clock on one thread: when every task is waiting, the
//! clock jumps to the next timer. A run of hours takes as long as its work.
//! What it cannot show is how the shipped binaries, their `DynamoDB`
//! expressions and the JavaScript gate behave; `stack` runs those.
//!
//! With `--tickets`, the origin sells that many tickets, 1 to 4 to each
//! visitor it admits, and the operator ends the event when they are gone: the
//! visitors still waiting see it and leave.
//!
//! ```text
//! cargo run --release -p harness --bin sim -- --visitors 500000 --concurrency 1000 --session 900 --countdown 1800 --tickets 20000
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use harness::backend::{Api, Backend, Consumer, Reply};
use harness::memory::Memory;
use harness::report::Registration;
use harness::scenario::{Arrival, Scenario};
use harness::visitor::World;
use harness::{clock, edge, metrics, queue, report, scenario};
use jiff::SignedDuration;
use lambda_http::Body;
use tokio::time::Instant;
use wr_common::{Counters, Phase, SigningKey, StoredControl};

const EVENT_ID: &str = "local";
const PROTECTED_PATH: &str = "/shop/checkout";
const SESSION_COOKIE: &str = "vwr_session";

struct Args {
    scenario: Scenario,
    /// Tickets the origin sells before the event is ended.
    tickets: Option<u64>,
    pollers: usize,
    max_seconds: i64,
    out: PathBuf,
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        scenario: Scenario {
            visitors: 2000,
            concurrency: 500,
            session: SignedDuration::from_mins(15),
            countdown: SignedDuration::from_mins(30),
            arrival: Arrival::Late,
            herd_window: SignedDuration::from_secs(10),
            no_show: 0.0,
            duplicate_rate: 0.0,
            scale: 1.0,
            seed: 1,
        },
        tickets: None,
        pollers: 8,
        max_seconds: 7 * 24 * 3600,
        out: PathBuf::from("target/sim-runs/latest"),
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
            "--scale" => a.scenario.scale = value()?.parse()?,
            "--seed" => a.scenario.seed = value()?.parse()?,
            "--tickets" => a.tickets = Some(value()?.parse()?),
            "--pollers" => a.pollers = value()?.parse()?,
            "--max-seconds" => a.max_seconds = value()?.parse()?,
            "--out" => a.out = PathBuf::from(value()?),
            other => anyhow::bail!("unknown flag {other}"),
        }
    }
    anyhow::ensure!(
        (0.0..1.0).contains(&a.scenario.no_show),
        "--no-show must be in [0, 1)"
    );
    Ok(a)
}

fn main() -> ExitCode {
    let errors = ErrorCount::default();
    {
        use tracing_subscriber::layer::SubscriberExt as _;
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(errors.clone()),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build();
    let outcome = match runtime {
        Ok(rt) => rt.block_on(run(&errors)),
        Err(e) => Err(e.into()),
    };
    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

async fn run(errors: &ErrorCount) -> Result<bool> {
    let args = parse_args()?;
    std::fs::create_dir_all(&args.out)?;
    let scenario = &args.scenario;
    let started = Instant::now();
    let run_start = clock::now();
    let open_at = run_start + scenario.countdown;

    let memory = Memory::new(scenario.session);
    memory.seed(Counters {
        event_id: EVENT_ID.to_owned(),
        phase: Phase::PreQueue,
        queue_counter: 0,
        serving_counter: 0,
        shuffle_seed: None,
        participant_count: None,
        prequeue_offsets: None,
        message: None,
        origin_concurrency: Some(scenario.concurrency),
        stored_control: StoredControl::Open,
        fail_open_until: None,
        starts_at: Some(open_at),
    });

    let mut key_bytes = [0u8; 32];
    harness::rng::Rng::new(scenario.seed ^ 0x006b_6579).fill(&mut key_bytes);
    let key = SigningKey::new(&key_bytes);
    let venue = args.tickets.map(|tickets| Venue {
        left: tickets,
        sold: 0,
        buyers: 0,
        sold_out_at: None,
        rng: harness::rng::Rng::new(scenario.seed ^ 0x7469_636b),
    });
    let functions = Arc::new(Functions::new(&memory, key, scenario.session, venue));

    let queue = Arc::new(queue::JoinQueue::new(
        "sim://join".to_owned(),
        scenario.duplicate_rate,
        scenario.seed ^ 0x5151,
    ));
    let metrics = Arc::new(metrics::Metrics::new(started));
    let backend: Arc<dyn Backend> = functions.clone();
    let edge = Arc::new(edge::Edge::new(
        scenario.throttles(),
        backend,
        Arc::clone(&queue),
        EVENT_ID,
        Arc::clone(&metrics),
    ));

    // The event source mapping.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut pollers = Vec::new();
    for _ in 0..args.pollers {
        let queue = Arc::clone(&queue);
        let functions = Arc::clone(&functions);
        let stop = stop_rx.clone();
        pollers.push(tokio::spawn(async move {
            queue.poll(functions.as_ref(), &stop).await;
        }));
    }

    // The open schedule.
    let opener = {
        let memory = memory.clone();
        let queue = Arc::clone(&queue);
        let at = started + scenario.countdown.unsigned_abs();
        let seed = {
            let mut seed = [0u8; 32];
            harness::rng::Rng::new(scenario.seed ^ 0x6f70_656e).fill(&mut seed);
            seed
        };
        tokio::spawn(async move {
            tokio::time::sleep_until(at).await;
            let t = Instant::now();
            let progress = Progress {
                queue: &queue,
                memory: &memory,
            };
            // Lambda's 900 s less the open's own 10 s, a read a second.
            let drain = open_event::wait_for_drain(&progress, 890, || {
                tokio::time::sleep(Duration::from_secs(1))
            })
            .await;
            tracing::info!(drain = ?drain, "join queue checked before the open");
            let opened = open_event::open_event(&memory, EVENT_ID, seed).await;
            (opened.map_err(|e| e.to_string()), t.elapsed())
        })
    };

    let controller_task = {
        let memory = memory.clone();
        let stop = stop_rx.clone();
        tokio::spawn(async move { scenario::run_controller(&memory, EVENT_ID, stop).await })
    };

    let budget = scenario.budget(SignedDuration::from_secs(args.max_seconds));
    let world = Arc::new(World {
        edge: Arc::clone(&edge),
        started,
        deadline: started + budget.unsigned_abs(),
        protected_path: PROTECTED_PATH.to_owned(),
    });
    say(&format!(
        "{} visitors, {} at once for {} s each, opening in {} s, deadline in {} s",
        scenario.visitors,
        scenario.concurrency,
        scenario.session.as_secs(),
        scenario.countdown.as_secs(),
        budget.as_secs()
    ));
    let wall = std::time::Instant::now();
    let handles = scenario.spawn_visitors(&world);
    let progress = {
        let edge = Arc::clone(&edge);
        let queue = Arc::clone(&queue);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_mins(10)).await;
                let admitted: u64 = edge
                    .metrics
                    .origin_series("protected", started.elapsed().as_secs())
                    .iter()
                    .sum();
                say(&format!(
                    "t={:>7}s  admitted {admitted:>8}  queue {:>6}  ({:.0} s of wall clock)",
                    started.elapsed().as_secs(),
                    queue.visible(),
                    wall.elapsed().as_secs_f64()
                ));
            }
        })
    };
    let records = scenario::collect(handles).await;
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
    let registrations: Vec<Registration> = memory
        .prequeue_rows()
        .into_iter()
        .map(|row| Registration {
            request_id: row.r,
            shard: usize::from(row.s),
            local: row.l,
        })
        .collect();
    let logged_errors = errors.snapshot();
    let mut report = report::build(&report::Evidence {
        scenario,
        records: &records,
        metrics: &metrics,
        queue: queue.totals(),
        published: published.as_ref(),
        registrations: &registrations,
        opened: opened.is_ok(),
        open_at_ms: u64::try_from(scenario.countdown.as_millis()).unwrap_or(u64::MAX),
        open_took,
        passes: &passes,
        elapsed,
        failed_invocations: functions.failed.load(Ordering::Relaxed),
        logged_errors: &logged_errors,
    });
    if let Some(venue) = functions.venue.as_ref().and_then(|v| v.lock().ok()) {
        report.fact("tickets sold", venue.sold);
        report.fact("buyers", venue.buyers);
        report.fact(
            "sold out after (s)",
            venue.sold_out_at.map_or_else(
                || "not sold out".to_owned(),
                |at| at.duration_since(run_start).as_secs().to_string(),
            ),
        );
    }
    report.fact(
        "wall clock (s)",
        format!("{:.1}", wall.elapsed().as_secs_f64()),
    );

    let text = report.render();
    println!("{text}");
    std::fs::write(args.out.join("report.txt"), &text)?;
    std::fs::write(
        args.out.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    say(&format!("report in {}", args.out.display()));
    Ok(report.passed())
}

fn say(line: &str) {
    println!("[sim] {line}");
}

/// The tickets the origin has to sell.
struct Venue {
    left: u64,
    sold: u64,
    buyers: u64,
    sold_out_at: Option<jiff::Timestamp>,
    rng: harness::rng::Rng,
}

impl Venue {
    /// A visitor the gate let in buys 1 to 4 of what is left. Returns whether
    /// that sold the last one.
    fn sell(&mut self) -> bool {
        if self.left == 0 {
            return false;
        }
        let draw = (self.rng.unit() * 4.0).floor();
        let wanted = if draw >= 3.0 {
            4
        } else if draw >= 2.0 {
            3
        } else if draw >= 1.0 {
            2
        } else {
            1
        };
        let bought = wanted.min(self.left);
        self.left -= bought;
        self.sold += bought;
        self.buyers += 1;
        if self.left == 0 {
            self.sold_out_at = Some(clock::now());
            return true;
        }
        false
    }
}

/// The functions behind the edge, as their libraries, and the origin's sale.
struct Functions {
    read: read::http::Ctx<Memory>,
    generate_token: generate_token::http::AppState<Memory>,
    memory: Memory,
    venue: Option<Mutex<Venue>>,
    /// Invocations that returned an error.
    failed: AtomicU64,
}

impl Functions {
    fn new(
        memory: &Memory,
        key: SigningKey,
        session: SignedDuration,
        venue: Option<Venue>,
    ) -> Self {
        Self {
            read: read::http::Ctx {
                store: memory.clone(),
                event_id: EVENT_ID.to_owned(),
                counters_cache: read::CountersCache::new(Duration::from_secs(1)),
                // The policy the dev root ships.
                poll_policy: read::parse_poll_policy(Some("5000"), Some("30000"), Some("10")),
                session,
                monotonic: clock::monotonic,
            },
            generate_token: generate_token::http::AppState {
                store: memory.clone(),
                key,
                event_id: EVENT_ID.to_owned(),
                session_cookie_name: SESSION_COOKIE.to_owned(),
                session,
            },
            memory: memory.clone(),
            venue: venue.map(Mutex::new),
            failed: AtomicU64::new(0),
        }
    }

    fn failed<E: std::fmt::Display>(&self, e: E) -> anyhow::Error {
        self.failed.fetch_add(1, Ordering::Relaxed);
        anyhow::anyhow!("{e}")
    }
}

/// A Lambda proxy response as API Gateway hands it on.
fn proxy_response(response: &lambda_http::Response<Body>) -> serde_json::Value {
    let body = String::from_utf8_lossy(response.body().as_ref()).into_owned();
    let cookies: Vec<&str> = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    serde_json::json!({
        "statusCode": response.status().as_u16(),
        "body": body,
        "multiValueHeaders": { "set-cookie": cookies },
    })
}

impl Backend for Functions {
    fn invoke(&self, api: Api, event: serde_json::Value) -> Reply<'_> {
        Box::pin(async move {
            let req =
                lambda_http::request::from_str(&event.to_string()).map_err(|e| self.failed(e))?;
            let response = match api {
                Api::Read => read::http::route(&self.read, req).await,
                Api::GenerateToken => generate_token::http::handle(&self.generate_token, req).await,
            }
            .map_err(|e| self.failed(e))?;
            Ok(proxy_response(&response))
        })
    }

    /// The gate's decision for a protected path every rule covers: a session
    /// cookie that verifies for this event passes, anything else is sent to
    /// the waiting page (a navigation) or refused (a script).
    fn gate(&self, viewer: serde_json::Value) -> Reply<'_> {
        Box::pin(async move {
            let cookie = viewer
                .get("cookies")
                .and_then(|c| c.get(SESSION_COOKIE))
                .and_then(serde_json::Value::as_str);
            let valid = cookie
                .and_then(|c| {
                    wr_common::Session::verify(c, &self.generate_token.key, clock::now()).ok()
                })
                .is_some_and(|s| s.event_id == EVENT_ID);
            if valid {
                // The origin sells; when the last ticket goes, the operator
                // says so and ends the event.
                let sold_out = self
                    .venue
                    .as_ref()
                    .and_then(|v| v.lock().ok())
                    .is_some_and(|mut v| v.sell());
                if sold_out {
                    self.memory.update_counters(EVENT_ID, |c| {
                        c.message = Some("Sold out".to_owned());
                        c.phase = Phase::PostEvent;
                    });
                }
                return Ok(serde_json::json!({ "pass": true }));
            }
            let navigation = viewer
                .get("headers")
                .and_then(|h| h.get("accept"))
                .and_then(serde_json::Value::as_str)
                == Some("text/html");
            Ok(serde_json::json!({
                "pass": false,
                "status": if navigation { 302 } else { 403 },
            }))
        })
    }
}

impl Consumer for Functions {
    fn deliver(&self, event: serde_json::Value) -> Reply<'_> {
        Box::pin(async move {
            let event: aws_lambda_events::sqs::SqsEvent =
                serde_json::from_value(event).map_err(|e| self.failed(e))?;
            let response = assign_position::handler::handle(&self.memory, EVENT_ID, event).await;
            Ok(serde_json::to_value(response)?)
        })
    }
}

/// The join queue and the shard counts it drains into, as the open sees them.
struct Progress<'a> {
    queue: &'a queue::JoinQueue,
    memory: &'a Memory,
}

impl open_event::Backlog for Progress<'_> {
    async fn waiting(&self) -> Result<u64, open_event::StoreError> {
        Ok(self.queue.visible())
    }

    async fn counted(&self) -> Result<u64, open_event::StoreError> {
        let counts = open_event::Store::read_shard_counts(self.memory, EVENT_ID).await?;
        Ok(counts.iter().sum())
    }
}

/// Error-level events the functions log, by name, the way `stack` reads them
/// from each function's log.
#[derive(Clone, Default)]
struct ErrorCount(Arc<Mutex<BTreeMap<String, u64>>>);

impl ErrorCount {
    fn snapshot(&self) -> BTreeMap<String, u64> {
        self.0.lock().map(|m| m.clone()).unwrap_or_default()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ErrorCount {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::ERROR {
            return;
        }
        let mut name = Name::default();
        event.record(&mut name);
        if let Ok(mut counts) = self.0.lock() {
            *counts
                .entry(
                    name.event
                        .or(name.message)
                        .unwrap_or_else(|| "unnamed".to_owned()),
                )
                .or_default() += 1;
        }
    }
}

/// An event's `event` field, or its message.
#[derive(Default)]
struct Name {
    event: Option<String>,
    message: Option<String>,
}

impl tracing::field::Visit for Name {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "event" => self.event = Some(value.to_owned()),
            "message" => self.message = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    #[test]
    fn the_venue_sells_one_to_four_and_never_more_than_it_has() {
        let mut venue = Venue {
            left: 1_000,
            sold: 0,
            buyers: 0,
            sold_out_at: None,
            rng: harness::rng::Rng::new(3),
        };
        let mut sold_out = false;
        while !sold_out {
            let before = venue.left;
            sold_out = venue.sell();
            assert!((1..=4).contains(&(before - venue.left)));
        }
        assert_eq!((venue.left, venue.sold), (0, 1_000));
        // Somewhere between all fours and all ones.
        assert!((250..=1_000).contains(&venue.buyers));
        assert!(!venue.sell(), "nothing is left to sell");
    }

    #[test]
    fn a_response_keeps_its_status_body_and_cookies() {
        let response = lambda_http::Response::builder()
            .status(425)
            .header("set-cookie", "a=1")
            .header("set-cookie", "b=2")
            .body(Body::from("{}"))
            .unwrap();
        let proxy = proxy_response(&response);
        assert_eq!(proxy["statusCode"], 425);
        assert_eq!(proxy["body"], "{}");
        assert_eq!(
            proxy["multiValueHeaders"]["set-cookie"],
            serde_json::json!(["a=1", "b=2"])
        );
    }
}
