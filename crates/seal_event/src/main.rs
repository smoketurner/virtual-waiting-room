//! Lambda entry point: the scheduler fires this once at the event start time.
//! It generates the permutation seed and performs the one-time seal.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use seal_event::dynamo::DynamoStore;
use seal_event::seal_event;
use serde::Deserialize;

/// The scheduler payload names the event to seal.
#[derive(Debug, Deserialize)]
struct SealRequest {
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
    let rng = SystemRandom::new();

    lambda_runtime::run(service_fn(|event: LambdaEvent<SealRequest>| {
        handle(&store, &rng, event)
    }))
    .await
}

async fn handle(
    store: &DynamoStore,
    rng: &SystemRandom,
    event: LambdaEvent<SealRequest>,
) -> Result<(), Error> {
    let mut seed = [0u8; 32];
    rng.fill(&mut seed)
        .map_err(|_| Error::from("failed to generate seal seed"))?;
    seal_event(store, &event.payload.event_id, seed).await?;
    Ok(())
}
