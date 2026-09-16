//! Lambda entry point: an SQS event source mapping delivers a batch of join
//! messages; each is assigned a queue position and the ids to retry are
//! returned as partial-batch failures.

use assign_position::dynamo::DynamoStore;
use assign_position::{BatchRecord, process_batch};
use aws_lambda_events::sqs::{BatchItemFailure, SqsBatchResponse, SqsEvent, SqsMessage};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use wr_common::Shard;

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

    lambda_runtime::run(service_fn(|event: LambdaEvent<SqsEvent>| {
        handle(&state, event)
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

// The SqsBatchResponse / BatchItemFailure event structs are #[non_exhaustive],
// so they cannot be built with struct-literal syntax from outside their crate;
// Default + field assignment is the only construction path.
async fn handle(state: &AppState, event: LambdaEvent<SqsEvent>) -> Result<SqsBatchResponse, Error> {
    let records: Vec<BatchRecord> = event
        .payload
        .records
        .into_iter()
        .filter_map(record_from_sqs)
        .collect();

    // Drawn once per invocation (issue #59): server-random rather than
    // derived from request_id, so a batch that takes the pre-queue path
    // claims one contiguous block on this one shard. A draw failure fails the
    // whole batch for retry rather than falling back to a fixed shard, which
    // would reintroduce the attacker-steerable behaviour this replaces.
    let shard = match Shard::random() {
        Ok(shard) => shard,
        Err(err) => {
            tracing::error!(error = %err, "failed to draw a random shard; retrying batch");
            let mut response = SqsBatchResponse::default();
            response.batch_item_failures = records
                .into_iter()
                .map(|record| {
                    let mut failure = BatchItemFailure::default();
                    failure.item_identifier = record.message_id;
                    failure
                })
                .collect();
            return Ok(response);
        }
    };

    let outcome = process_batch(&state.store, &state.event_id, shard, &records).await;

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

/// Maps an SQS message to a batch record, dropping any message with no id or
/// no body — such a message cannot be processed or reported, so it is skipped.
fn record_from_sqs(msg: SqsMessage) -> Option<BatchRecord> {
    Some(BatchRecord {
        message_id: msg.message_id?,
        body: msg.body?,
    })
}
