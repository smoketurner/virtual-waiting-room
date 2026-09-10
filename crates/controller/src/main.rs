//! Lambda entry point for the outflow controller.
//!
//! The `EventBridge` Scheduler `rate()` minimum is one minute, but the design's
//! cadence is 10 seconds (DESIGN §7). The schedule fires `rate(1 minute)` and
//! each invoke runs [`controller::PASSES_PER_INVOKE`] passes
//! [`controller::INTERVAL_SECS`] seconds apart, so one invoke covers a full
//! minute at the 10-second cadence. The function timeout must exceed
//! `PASSES_PER_INVOKE * INTERVAL_SECS` plus the per-pass work.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use controller::dynamo::DynamoStore;
use controller::{INTERVAL_SECS, PASSES_PER_INVOKE, Store, run_pass};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let counters_table = std::env::var("COUNTERS_TABLE")?;
    let positions_table = std::env::var("POSITIONS_TABLE")?;
    let event_id = std::env::var("EVENT_ID")?;
    let store = DynamoStore::new(client, counters_table, positions_table, event_id);

    // Force one real read in the Init phase so the aws-lc-rs jitter-entropy seed
    // and the TLS handshake land on boosted Init CPU rather than the first
    // invoke (tech.md cold-start mitigation). A failure here is not fatal: the
    // scheduled invoke will retry.
    if let Err(e) = store.read_state(store.event_id()).await {
        tracing::warn!(error = %e, "init warm-up read failed; continuing");
    }

    lambda_runtime::run(service_fn(|event: LambdaEvent<Value>| {
        handle(&store, event)
    }))
    .await
}

async fn handle(store: &DynamoStore, _event: LambdaEvent<Value>) -> Result<(), Error> {
    let event_id = store.event_id().to_owned();
    for pass in 0..PASSES_PER_INVOKE {
        run_pass(store, &event_id, now_secs()).await?;
        // Sleep between passes to hit the 10-second cadence; skip the final
        // sleep so the invoke returns promptly after its last pass.
        if pass + 1 < PASSES_PER_INVOKE {
            tokio::time::sleep(Duration::from_secs(INTERVAL_SECS)).await;
        }
    }
    Ok(())
}

/// Current epoch-seconds. A clock before the epoch is impossible on Lambda;
/// should it ever read backwards, treat now as 0 (expiring nothing) rather than
/// panicking.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
