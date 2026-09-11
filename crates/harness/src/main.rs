//! Generates waiting-room load locally and measures what reaches the origin.
//!
//! The question this answers is a counting one: how many origin requests does a
//! waiting visitor cause per minute, and does that number stay flat as the room
//! fills or grow with it. That needs no AWS account — only the cache-key rules
//! the distribution is configured with, which `edge` models, and a client that
//! polls the way the real one does, which `visitor` models.
//!
//! What it cannot tell you: anything about quotas, latency, or the behaviour of
//! a real CDN across many edge locations. Those decide whether real
//! infrastructure copes with the load; this decides how much load there is.
//!
//! ```text
//! harness --visitors 500 --seconds 120 --origin http://127.0.0.1:9000/lambda-url/bootstrap
//! harness --visitors 500 --seconds 120 --polling every-tick   # before holding positions
//! harness --visitors 3000 --seconds 120 --countdown 60 --polling backoff --target-rate 50  # #69
//! ```
//!
//! With no `--origin` it serves its own stub, so the request pattern can be
//! measured without a stack running. Point it at `cargo lambda watch` to put the
//! real Lambda, its `Store`, and its execution-environment cache in the path.

mod edge;
mod visitor;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::edge::Edge;
use crate::visitor::{Arrival, Polling, VisitorTally};

/// CloudFront's default minimum lifetime for a cached error response.
const ERROR_TTL: Duration = Duration::from_secs(10);

struct Args {
    visitors: usize,
    seconds: u64,
    countdown: u64,
    polling: Polling,
    arrival: Arrival,
    origin: Option<String>,
    /// Visitors admitted per second. Drives the built-in stub's moving
    /// `serving_position` and, under `--polling backoff`, the client's own
    /// interval calculation — both read the same value, the way a real
    /// event's controller and client both work from `target_rate`.
    target_rate: u64,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        visitors: 100,
        seconds: 60,
        countdown: 0,
        polling: Polling::HoldPosition,
        arrival: Arrival::Uniform,
        origin: None,
        target_rate: 500,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().context(format!("{flag} needs a value"));
        match flag.as_str() {
            "--visitors" => args.visitors = value()?.parse().context("--visitors")?,
            "--seconds" => args.seconds = value()?.parse().context("--seconds")?,
            "--countdown" => args.countdown = value()?.parse().context("--countdown")?,
            "--target-rate" => args.target_rate = value()?.parse().context("--target-rate")?,
            "--arrival" => {
                args.arrival = match value()?.as_str() {
                    "uniform" => Arrival::Uniform,
                    "late" => Arrival::Late,
                    other => anyhow::bail!("--arrival must be uniform or late: {other}"),
                }
            }
            "--origin" => args.origin = Some(value()?),
            "--polling" => {
                args.polling = match value()?.as_str() {
                    "hold-position" => Polling::HoldPosition,
                    "every-tick" => Polling::EveryTick,
                    "derive-position" => Polling::DerivePosition,
                    "backoff" => Polling::Backoff,
                    other => {
                        anyhow::bail!(
                            "--polling must be hold-position, every-tick, derive-position or backoff: {other}"
                        )
                    }
                }
            }
            other => anyhow::bail!("unknown flag: {other}"),
        }
    }
    Ok(args)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let edge = Arc::new(Edge::new(ERROR_TTL, args.seconds));

    let origin_url = args.origin.clone();
    let client = reqwest::Client::builder()
        .build()
        .context("building the origin client")?;
    let origin_hits = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let target_rate = args.target_rate;
    let countdown_secs = args.countdown;
    let stub_started = std::time::Instant::now();

    let hits = Arc::clone(&origin_hits);
    let origin = Arc::new(move |path: String| {
        let client = client.clone();
        let base = origin_url.clone();
        let hits = Arc::clone(&hits);
        async move {
            hits.fetch_add(1, Ordering::Relaxed);
            match base {
                // No stack running: answer the shape the real endpoints answer,
                // so the request pattern is measurable on its own.
                None => {
                    if path.starts_with("/v1/status") {
                        let elapsed_secs = stub_started.elapsed().as_secs();
                        if elapsed_secs < countdown_secs {
                            // Countdown still running: no cursor yet, matching
                            // the real server's `closed` serving_state before
                            // the seal. Without this row --polling backoff's
                            // ceiling-during-closed branch never fires and the
                            // measured saving is 2.6x smaller than the
                            // client's actual countdown-phase behaviour.
                            (
                                200,
                                r#"{"event_id":"harness","phase":"pre_queue","serving_state":"closed","serving_position":0}"#
                                    .to_owned(),
                            )
                        } else {
                            // A moving cursor, offset by the countdown so the
                            // drain starts at T-0 rather than at process
                            // start — so --polling backoff has a
                            // shrinking "ahead" to react to: every visitor's
                            // /status shares this one path-keyed cache entry,
                            // so they all see the same cursor advance, the
                            // way a real controller does.
                            let serving = target_rate * (elapsed_secs - countdown_secs);
                            (
                                200,
                                format!(
                                    r#"{{"event_id":"harness","phase":"active","serving_state":"running","serving_position":{serving},"target_rate":{target_rate}}}"#
                                ),
                            )
                        }
                    } else if let Some(position) = position_from_query(&path) {
                        (
                            200,
                            format!(r#"{{"position":{position},"live_join":true}}"#),
                        )
                    } else {
                        (200, r#"{"position":42,"live_join":true}"#.to_owned())
                    }
                }
                Some(base) => match client.get(format!("{base}{path}")).send().await {
                    Ok(res) => {
                        let status = res.status().as_u16();
                        let body = res.text().await.unwrap_or_default();
                        (status, body)
                    }
                    Err(e) => (502, e.to_string()),
                },
            }
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.seconds);
    let tally = Arc::new(VisitorTally::default());

    println!(
        "{} visitors, {}s, polling={}, origin={}",
        args.visitors,
        args.seconds,
        match args.polling {
            Polling::HoldPosition => "hold-position",
            Polling::EveryTick => "every-tick",
            Polling::DerivePosition => "derive-position",
            Polling::Backoff => "backoff",
        },
        args.origin.as_deref().unwrap_or("(built-in stub)")
    );

    let settings = visitor::RunSettings {
        polling: args.polling,
        spread_ms: visitor::first_ask_spread_ms(args.visitors as u64),
        countdown_ms: args.countdown * 1000,
        arrival: args.arrival,
        deadline,
        target_rate: args.target_rate,
    };
    println!(
        "  first-ask spread: {:.0}s",
        settings.spread_ms as f64 / 1000.0
    );

    let mut tasks = Vec::with_capacity(args.visitors);
    for n in 0..args.visitors {
        tasks.push(tokio::spawn(visitor::run(
            Arc::clone(&edge),
            format!("0199f0c0-0000-7000-8000-{n:012x}"),
            n as u64 + 1,
            settings,
            Arc::clone(&origin),
            Arc::clone(&tally),
        )));
    }
    for task in tasks {
        task.await.context("a visitor task panicked")?;
    }

    report(&edge, args.seconds, args.countdown, &tally).await;
    Ok(())
}

/// Reconstructs a simulated visitor's own position from the hex suffix of
/// the synthetic request id `main` assigns it (`format!("...{n:012x}")`),
/// so the built-in stub answers `/v1/queue_num` with a real, distinct
/// position per visitor instead of the constant every other polling mode is
/// indifferent to. `--polling backoff` needs a real "ahead" to react to.
fn position_from_query(path: &str) -> Option<u64> {
    let id = path.split("request_id=").nth(1)?;
    let hex = id.rsplit('-').next()?;
    u64::from_str_radix(hex, 16).ok()
}

/// Prints what the run measured, normalized by visitor-minutes.
///
/// The denominator is the time visitors were actually present and polling, not
/// `visitors × run length`. Under `--countdown`/`--arrival` a visitor sleeps
/// until their arrival offset and issues no requests before it, so charging
/// them for the whole run divides real requests by imaginary waiting and
/// understates every per-visitor rate — by ~1.5x for a uniform arrival over
/// half the run, more under `Arrival::Late`.
async fn report(edge: &Edge, seconds: u64, countdown_s: u64, tally: &VisitorTally) {
    let per = tally.active_ms.load(Ordering::Relaxed) as f64 / 60_000.0;
    if per == 0.0 {
        println!("\n  no visitor was ever present: every arrival offset landed past the deadline.");
        println!("  --countdown must be shorter than --seconds for anyone to poll.");
        return;
    }
    println!("\n  visitor-minutes (time present and polling): {per:.1}");

    println!("\n  endpoint        client    origin      hits  collapsed   origin/visitor/min");
    println!("  ---------------------------------------------------------------------------");
    let mut total_origin = 0u64;
    let mut total_client = 0u64;
    for (endpoint, client, origin, hits, collapsed) in edge.snapshot().await {
        total_origin += origin;
        total_client += client;
        println!(
            "  {endpoint:<14} {client:>7} {origin:>9} {hits:>9} {collapsed:>10}   {:>8.2}",
            origin as f64 / per
        );
    }
    // What visitors sent, not what reached the origin: /status collapses
    // heavily regardless of poll rate, so this is the number that actually
    // moves between polling modes.
    println!(
        "\n  client requests per visitor per minute, all endpoints: {:.2}",
        total_client as f64 / per
    );
    println!("  total client requests, all endpoints: {total_client}");
    println!(
        "\n  origin requests per visitor per minute, all endpoints: {:.2}",
        total_origin as f64 / per
    );
    println!(
        "  origin requests per second, all endpoints: {:.1}",
        total_origin as f64 / seconds as f64
    );
    let (peak, at) = edge.peak_origin_per_second();
    println!("  busiest second: {peak} origin requests, at t={at}s");
    histogram(&edge.origin_timeline(), countdown_s);
}

/// Origin requests per second, as a chart.
///
/// The totals say what an event costs; the shape says whether it fits through a
/// throttle. A cohort that does one thing in unison produces a spike that a
/// total hides completely.
fn histogram(timeline: &[u64], countdown_s: u64) {
    let peak = timeline.iter().copied().max().unwrap_or(0);
    if peak == 0 {
        return;
    }
    // Keep the chart a readable height however long the run was.
    let buckets = 40usize.min(timeline.len());
    let per_bucket = timeline.len().div_ceil(buckets);

    println!("\n  origin requests per second over the run (▏= peak {peak}/s)");
    for (n, chunk) in timeline.chunks(per_bucket).enumerate() {
        let seconds = (n * per_bucket) as u64;
        // The busiest second in the bucket, not the mean: a throttle is tripped
        // by the worst instant, and averaging it away is how a spike gets
        // reported as comfortable.
        let worst = chunk.iter().copied().max().unwrap_or(0);
        let width = (worst * 50 / peak) as usize;
        let seal = if seconds == countdown_s.saturating_sub(countdown_s % per_bucket as u64)
            && countdown_s > 0
        {
            " <- seal"
        } else {
            ""
        };
        println!(
            "  t={seconds:>4}s {:>9} {}{}",
            worst,
            "#".repeat(width),
            seal
        );
    }
}
