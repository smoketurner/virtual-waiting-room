//! Lambda entry point: the scheduler fires this once at the event start time.
//! It generates the permutation seed and performs the one-time open.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use open_event::dynamo::DynamoStore;
use open_event::sqs::JoinBacklog;
use open_event::{Backlog, OpenResult, Store, StoreError, open_when_ready};
use serde::Deserialize;

use jiff::{SignedDuration, Timestamp};

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

async fn handle(
    store: &DynamoStore,
    backlog: &JoinBacklog,
    rng: &SystemRandom,
    event: LambdaEvent<OpenRequest>,
) -> Result<(), Error> {
    let deadline = i64::try_from(event.context.deadline)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
        .ok_or("the invocation carried no usable deadline")?;
    let progress = JoinProgress {
        queue: backlog,
        store,
        event_id: &event.payload.event_id,
    };
    let mut seed = [0u8; 32];
    rng.fill(&mut seed)
        .map_err(|_| Error::from("failed to generate open seed"))?;
    match open_when_ready(
        store,
        &progress,
        &event.payload.event_id,
        seed,
        deadline,
        Timestamp::now,
        |d: SignedDuration| tokio::time::sleep(d.unsigned_abs()),
    )
    .await?
    {
        OpenResult::Opened(_) | OpenResult::AlreadyOpen => Ok(()),
    }
}

/// The join queue, and the shard counts of the joins drained from it: those
/// registered in the draw and those sent back to wait for the open.
struct JoinProgress<'a> {
    queue: &'a JoinBacklog,
    store: &'a DynamoStore,
    event_id: &'a str,
}

impl Backlog for JoinProgress<'_> {
    async fn waiting(&self) -> Result<u64, StoreError> {
        self.queue.waiting().await
    }

    async fn counted(&self) -> Result<u64, StoreError> {
        let registered = self.store.read_shard_counts(self.event_id).await?;
        let deferred = self.store.read_deferred_counts(self.event_id).await?;
        Ok(registered
            .iter()
            .chain(&deferred)
            .fold(0u64, |sum, n| sum.saturating_add(*n)))
    }
}
