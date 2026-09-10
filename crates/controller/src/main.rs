//! Lambda entry point for the outflow controller.
//!
//! The `EventBridge` Scheduler `rate()` minimum is one minute, but the design's
//! cadence is 10 seconds. The schedule fires `rate(1 minute)` and each invoke
//! starts one durable execution that runs [`controller::PASSES_PER_INVOKE`]
//! passes [`controller::INTERVAL_SECS`] seconds apart, so one execution covers a
//! full minute at the 10-second cadence.
//!
//! Each pass is a durable step and each gap a durable wait. A wait suspends the
//! execution instead of holding the invocation open, so the function is not
//! billed for the 50 seconds it spends between passes; Lambda re-invokes it to
//! resume and the SDK replays completed steps from their checkpoints rather than
//! re-running them. The function timeout therefore bounds a single pass, not a
//! whole minute of cadence.
//!
//! A pass advances `serving_counter`, so re-running one that was interrupted
//! after its `UpdateItem` landed would release a second time.
//! [`StepSemantics::AtMostOncePerRetry`] makes an interrupted pass fail rather
//! than replay, leaving the retry decision to the strategy.

use std::sync::Arc;
use std::time::Duration;

use aws_durable_execution_sdk as durable;
use aws_durable_execution_sdk::{RetryDecision, StepSemantics};
use controller::dynamo::DynamoStore;
use controller::{INTERVAL_SECS, PASSES_PER_INVOKE, PassOutcome, Store, run_pass};
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
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
    let store = Arc::new(DynamoStore::new(
        client,
        counters_table,
        positions_table,
        event_id,
    ));

    // Force one real read in the Init phase so the aws-lc-rs jitter-entropy seed
    // and the TLS handshake land on boosted Init CPU rather than the first
    // invoke. A failure here is not fatal: the scheduled invoke will retry.
    if let Err(e) = store.read_state(store.event_id()).await {
        tracing::warn!(error = %e, "init warm-up read failed; continuing");
    }

    durable::run(move |_event: Value, ctx: durable::DurableContext| {
        let store = Arc::clone(&store);
        handle(store, ctx)
    })
    .await
}

/// Runs a minute of cadence as one durable execution: [`PASSES_PER_INVOKE`]
/// checkpointed passes separated by [`INTERVAL_SECS`] durable waits.
///
/// Returns each pass's outcome so the execution history records what the minute
/// did.
async fn handle(
    store: Arc<DynamoStore>,
    ctx: durable::DurableContext,
) -> Result<Vec<PassOutcome>, durable::BoxError> {
    let event_id = store.event_id().to_owned();
    let mut outcomes = Vec::with_capacity(PASSES_PER_INVOKE as usize);

    for pass in 0..PASSES_PER_INVOKE {
        let pass_store = Arc::clone(&store);
        let pass_event_id = event_id.clone();
        // Operation names are minted in call order and must be identical on
        // every replay, so they are derived from the pass index alone.
        let outcome = ctx
            .step(move |_| async move {
                run_pass(pass_store.as_ref(), &pass_event_id)
                    .await
                    .map_err(durable::BoxError::from)
            })
            .name(format!("pass-{pass}"))
            .semantics(StepSemantics::AtMostOncePerRetry)
            // The SDK default retries a failed step up to six times with
            // exponential backoff, suspending the execution for each delay.
            // That is wrong here: the controller is a closed loop, so a failed
            // pass is better skipped than retried late, and the backoff would
            // push the remaining passes off the 10-second cadence. Stopping
            // ends the execution and the next scheduled one starts a clean
            // minute, which is what a failed pass did before.
            .retry_strategy(|_err, _attempt| RetryDecision::Stop)
            .await?;
        outcomes.push(outcome);

        // Skip the final wait so the execution completes on its last pass
        // rather than suspending for an interval it does not use.
        if pass + 1 < PASSES_PER_INVOKE {
            ctx.wait(Duration::from_secs(INTERVAL_SECS))
                .name(format!("cadence-{pass}"))
                .await?;
        }
    }

    Ok(outcomes)
}
