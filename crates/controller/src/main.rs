//! Lambda entry point for the outflow controller.
//!
//! The `EventBridge` Scheduler `rate()` minimum is one minute, but the design's
//! cadence is 10 seconds. The schedule fires `rate(1 minute)` and each invoke
//! starts one durable execution that runs [`controller::PASSES_PER_INVOKE`]
//! passes [`controller::INTERVAL`] apart, so one execution covers a
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

use aws_durable_execution_sdk as durable;
use aws_durable_execution_sdk::{OperationErrorKind, RetryDecision, StepSemantics};
use controller::dynamo::DynamoStore;
use controller::{INTERVAL, PASSES_PER_INVOKE, PassOutcome, Store, run_pass};
use serde_json::Value;
use wr_common::Phase;

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
    let event_id = std::env::var("EVENT_ID")?;
    // Required, and the same value `generate_token` mints with: a session the
    // controller thinks has ended while the gate still honours it is a visitor
    // over the origin's limit.
    let session = jiff::SignedDuration::from_secs(std::env::var("SESSION_TTL_SECS")?.parse()?);
    let store = Arc::new(DynamoStore::new(client, counters_table, event_id, session));

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
/// checkpointed passes separated by [`INTERVAL`] durable waits.
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
                // Sampled fresh on every pass rather than once per invocation:
                // it is what lets a fail-open window that lapses mid-minute be
                // observed on the very next pass.
                run_pass(pass_store.as_ref(), &pass_event_id, jiff::Timestamp::now())
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
            .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            // A pass that failed is skipped, and the cadence carries on (issue
            // #236). Ending the execution here would leave the rest of the
            // minute with no passes at all, so one transient `DynamoDB` error
            // stalled admission for up to fifty seconds. The next pass is owed
            // the seconds this one did not grant, so nothing is lost. Any
            // other failure is the execution's own and still ends it.
            Err(err) if matches!(err.kind(), OperationErrorKind::Step(_)) => {
                tracing::error!(
                    error = %err,
                    event = "controller_pass_failed",
                    pass,
                    "controller pass failed; skipping it"
                );
                wait_for_next(&ctx, pass).await?;
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        let idle = idles(&outcome);
        outcomes.push(outcome);

        // An event that has not started, or is over, only becomes active by
        // way of `pre_queue` and an open, so the rest of this minute's passes
        // would almost always read the same thing and wait in between -- which
        // is most of what the controller costs between events (N1). The price
        // is that an operator who registers and opens within one minute waits
        // for the next execution before admission starts.
        if idle {
            break;
        }

        wait_for_next(&ctx, pass).await?;
    }

    Ok(outcomes)
}

/// Whether a pass found the event in a phase the rest of the minute cannot
/// leave for `Active`: only the open makes an event active, and the open needs
/// `pre_queue`. `pre_queue` and `maintenance` keep the cadence, so admission
/// starts within one interval of the open or of a recovery.
fn idles(outcome: &PassOutcome) -> bool {
    match outcome {
        PassOutcome::NotActive(phase) => match phase {
            Phase::Idle | Phase::PostEvent => true,
            Phase::PreQueue | Phase::Active | Phase::Maintenance => false,
        },
        PassOutcome::Held(_) | PassOutcome::Ran { .. } => false,
    }
}

/// The durable wait between passes. Skipped after the last one, so the
/// execution completes on its final pass rather than suspending for an
/// interval it does not use.
async fn wait_for_next(ctx: &durable::DurableContext, pass: u32) -> Result<(), durable::BoxError> {
    if pass.saturating_add(1) < PASSES_PER_INVOKE {
        ctx.wait(INTERVAL.unsigned_abs())
            .name(format!("cadence-{pass}"))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use controller::PassOutcome;
    use wr_common::{AdmissionControl, Phase};

    use super::idles;

    #[test]
    fn only_a_phase_that_cannot_open_this_minute_ends_the_cadence_early() {
        assert!(idles(&PassOutcome::NotActive(Phase::Idle)));
        assert!(idles(&PassOutcome::NotActive(Phase::PostEvent)));
        // The open can land at any second of a countdown, and a recovery from
        // maintenance should resume within one interval.
        assert!(!idles(&PassOutcome::NotActive(Phase::PreQueue)));
        assert!(!idles(&PassOutcome::NotActive(Phase::Maintenance)));
        assert!(!idles(&PassOutcome::Held(AdmissionControl::Paused)));
        assert!(!idles(&PassOutcome::Ran { released: 0 }));
    }
}
