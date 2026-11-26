//! Lambda entry point: the scheduler fires this once at the event start time.
//! It generates the permutation seed and performs the one-time open.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use open_event::dynamo::DynamoStore;
use open_event::sqs::JoinBacklog;
use open_event::{Backlog, OpenResult, Store, StoreError, open_event, wait_for_drain};
use serde::Deserialize;

use jiff::{SignedDuration, Timestamp};

/// The pause between reads of the join queue while it drains.
const DRAIN_PAUSE: SignedDuration = SignedDuration::from_secs(1);

/// What the open keeps of its deadline for the open itself: one `BatchGetItem`
/// and one conditional `UpdateItem`.
const OPEN_RESERVE: SignedDuration = SignedDuration::from_secs(10);

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
    // Let the consumers catch up on joins accepted before the start, so they
    // are in the cohort rather than behind it (issue #239). As long as the
    // invocation allows, and never a reason not to open.
    let deadline = i64::try_from(event.context.deadline)
        .ok()
        .and_then(|ms| Timestamp::from_millisecond(ms).ok())
        .ok_or("the invocation carried no usable deadline")?;
    let polls = drain_polls(deadline, Timestamp::now());
    let progress = JoinProgress {
        queue: backlog,
        store,
        event_id: &event.payload.event_id,
    };
    let drain = wait_for_drain(&progress, polls, || {
        tokio::time::sleep(DRAIN_PAUSE.unsigned_abs())
    })
    .await;
    tracing::info!(drain = ?drain, "join queue checked before the open");

    let mut seed = [0u8; 32];
    rng.fill(&mut seed)
        .map_err(|_| Error::from("failed to generate open seed"))?;
    match open_event(store, &event.payload.event_id, seed).await? {
        OpenResult::Opened(_) | OpenResult::AlreadyOpen => Ok(()),
    }
}

/// The join queue and the shard counts it is drained into.
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
        let counts = self.store.read_shard_counts(self.event_id).await?;
        Ok(counts.iter().fold(0u64, |sum, n| sum.saturating_add(*n)))
    }
}

/// How many reads of the join queue fit before `deadline`, keeping
/// [`OPEN_RESERVE`] for the open itself.
fn drain_polls(deadline: Timestamp, now: Timestamp) -> u32 {
    let left = deadline
        .duration_since(now)
        .saturating_sub(OPEN_RESERVE)
        .max(SignedDuration::ZERO);
    u32::try_from(left.as_secs() / DRAIN_PAUSE.as_secs()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_drain_runs_until_the_open_must_go_ahead() {
        let now = Timestamp::constant(1_000, 0);
        assert_eq!(drain_polls(now + SignedDuration::from_mins(15), now), 890);
        assert_eq!(drain_polls(now + SignedDuration::from_secs(10), now), 0);
        assert_eq!(drain_polls(now, now + SignedDuration::from_secs(5)), 0);
    }
}
