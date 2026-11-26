//! Lambda entry point: an SQS event source mapping delivers a batch of join
//! messages; each is assigned a queue position and the ids to retry are
//! returned as partial-batch failures.

use assign_position::dynamo::DynamoStore;
use assign_position::handler::handle;
use aws_lambda_events::sqs::SqsEvent;
use lambda_runtime::{Error, LambdaEvent, service_fn};

/// Resolved once at cold start and shared across invocations.
struct AppState {
    store: DynamoStore,
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

    let state = init().await?;

    lambda_runtime::run(service_fn(|event: LambdaEvent<SqsEvent>| async {
        Ok::<_, Error>(handle(&state.store, &state.event_id, event.payload).await)
    }))
    .await
}

async fn init() -> Result<AppState, Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let counters_table = std::env::var("COUNTERS_TABLE")?;
    let prequeue_table = std::env::var("PREQUEUE_TABLE")?;
    let positions_table = std::env::var("POSITIONS_TABLE")?;
    let event_id = std::env::var("EVENT_ID")?;
    let store = DynamoStore::new(client, counters_table, prequeue_table, positions_table);

    Ok(AppState { store, event_id })
}
