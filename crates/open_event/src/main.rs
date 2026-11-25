//! Lambda entry point: the scheduler fires this once at the event start time.
//! It generates the permutation seed and performs the one-time open.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use open_event::dynamo::DynamoStore;
use open_event::sqs::JoinBacklog;
use open_event::{OpenResult, open_event, wait_for_drain};
use serde::{Deserialize, Serialize};

/// The pause between reads of the join queue while it drains.
const DRAIN_PAUSE: std::time::Duration = std::time::Duration::from_secs(1);

/// The scheduler payload names the event to open.
#[derive(Debug, Deserialize)]
struct OpenRequest {
    event_id: String,
}

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
    let store = DynamoStore::new(client, counters_table);
    let backlog = JoinBacklog::new(
        aws_sdk_sqs::Client::new(&config),
        std::env::var("JOIN_QUEUE_URL")?,
    );
    let rng = SystemRandom::new();

    lambda_runtime::run(service_fn(|event: LambdaEvent<OpenRequest>| {
        handle(&store, &backlog, &rng, event)
    }))
    .await
}

/// What the open did. The scheduler ignores this; the admin Lambda's
/// "Open now" reads it, because "already open" and "opened" are the same
/// success to the caller and a different thing to say to the operator.
#[derive(Debug, Serialize)]
struct OpenResponse {
    opened: bool,
}

async fn handle(
    store: &DynamoStore,
    backlog: &JoinBacklog,
    rng: &SystemRandom,
    event: LambdaEvent<OpenRequest>,
) -> Result<OpenResponse, Error> {
    // Let the consumers catch up on joins accepted before the start, so they
    // are in the cohort rather than behind it (issue #239). Bounded, and never
    // a reason not to open.
    let drain = wait_for_drain(backlog, || tokio::time::sleep(DRAIN_PAUSE)).await;
    tracing::info!(drain = ?drain, "join queue checked before the open");

    let mut seed = [0u8; 32];
    rng.fill(&mut seed)
        .map_err(|_| Error::from("failed to generate open seed"))?;
    let opened = match open_event(store, &event.payload.event_id, seed).await? {
        OpenResult::Opened(_) => true,
        OpenResult::AlreadyOpen => false,
    };
    Ok(OpenResponse { opened })
}
