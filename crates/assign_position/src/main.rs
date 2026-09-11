//! Lambda entry point: an SQS event source mapping delivers a batch of join
//! messages; each is assigned a queue position and the ids to retry are
//! returned as partial-batch failures.

use assign_position::dynamo::DynamoStore;
use assign_position::{BatchRecord, process_batch};
use aws_lambda_events::sqs::{BatchItemFailure, SqsBatchResponse, SqsEvent, SqsMessage};
use lambda_runtime::{Error, LambdaEvent, service_fn};

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
    let prequeue_table = std::env::var("PREQUEUE_TABLE")?;
    let positions_table = std::env::var("POSITIONS_TABLE")?;
    let event_id = std::env::var("EVENT_ID")?;
    let store = DynamoStore::new(client, counters_table, prequeue_table, positions_table);

    lambda_runtime::run(service_fn(|event: LambdaEvent<SqsEvent>| {
        handle(&store, &event_id, event)
    }))
    .await
}

// The SqsBatchResponse / BatchItemFailure event structs are #[non_exhaustive],
// so they cannot be built with struct-literal syntax from outside their crate;
// Default + field assignment is the only construction path.
async fn handle(
    store: &DynamoStore,
    event_id: &str,
    event: LambdaEvent<SqsEvent>,
) -> Result<SqsBatchResponse, Error> {
    let records: Vec<BatchRecord> = event
        .payload
        .records
        .into_iter()
        .filter_map(record_from_sqs)
        .collect();

    let outcome = process_batch(store, event_id, &records).await;

    let batch_item_failures = outcome
        .failures
        .into_iter()
        .map(|id| {
            let mut failure = BatchItemFailure::default();
            failure.item_identifier = id;
            failure
        })
        .collect();

    let mut response = SqsBatchResponse::default();
    response.batch_item_failures = batch_item_failures;
    Ok(response)
}

/// Maps an SQS message to a batch record, dropping any message with no id or no
/// body — such a message cannot be processed or reported, so it is skipped.
fn record_from_sqs(msg: SqsMessage) -> Option<BatchRecord> {
    Some(BatchRecord {
        message_id: msg.message_id?,
        body: msg.body?,
    })
}
