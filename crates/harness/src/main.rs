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
use crate::visitor::{Polling, VisitorTally};

/// CloudFront's default minimum lifetime for a cached error response.
const ERROR_TTL: Duration = Duration::from_secs(10);

struct Args {
    visitors: usize,
    seconds: u64,
    polling: Polling,
    origin: Option<String>,
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        visitors: 100,
        seconds: 60,
        polling: Polling::HoldPosition,
        origin: None,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().context(format!("{flag} needs a value"));
        match flag.as_str() {
            "--visitors" => args.visitors = value()?.parse().context("--visitors")?,
            "--seconds" => args.seconds = value()?.parse().context("--seconds")?,
            "--origin" => args.origin = Some(value()?),
            "--polling" => {
                args.polling = match value()?.as_str() {
                    "hold-position" => Polling::HoldPosition,
                    "every-tick" => Polling::EveryTick,
                    other => {
                        anyhow::bail!("--polling must be hold-position or every-tick: {other}")
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
    let edge = Arc::new(Edge::new(ERROR_TTL));

    let origin_url = args.origin.clone();
    let client = reqwest::Client::builder()
        .build()
        .context("building the origin client")?;
    let origin_hits = Arc::new(std::sync::atomic::AtomicU64::new(0));

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
                        (
                            200,
                            r#"{"event_id":"harness","phase":"active","serving_state":"running","serving_position":1}"#
                                .to_owned(),
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
        },
        args.origin.as_deref().unwrap_or("(built-in stub)")
    );

    let settings = visitor::RunSettings {
        polling: args.polling,
        spread_ms: visitor::first_ask_spread_ms(args.visitors as u64),
        deadline,
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

    report(&edge, args.visitors, args.seconds).await;
    Ok(())
}

async fn report(edge: &Edge, visitors: usize, seconds: u64) {
    let minutes = seconds as f64 / 60.0;
    let per = visitors as f64 * minutes;

    println!("\n  endpoint        client    origin      hits  collapsed   origin/visitor/min");
    println!("  ---------------------------------------------------------------------------");
    let mut total_origin = 0u64;
    for (endpoint, client, origin, hits, collapsed) in edge.snapshot().await {
        total_origin += origin;
        println!(
            "  {endpoint:<14} {client:>7} {origin:>9} {hits:>9} {collapsed:>10}   {:>8.2}",
            origin as f64 / per
        );
    }
    println!(
        "\n  origin requests per visitor per minute, all endpoints: {:.2}",
        total_origin as f64 / per
    );
    println!(
        "  origin requests per second, all endpoints: {:.1}",
        total_origin as f64 / seconds as f64
    );
    println!(
        "\n  Run this again with a different --visitors. An endpoint whose origin\n  \
         column barely moves is collapsing and stays affordable as the room fills;\n  \
         one that scales with the visitor count is a per-visitor cost at every poll."
    );
}
