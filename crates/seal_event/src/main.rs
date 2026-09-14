//! Lambda entry point: the scheduler fires this once at the event start time.
//! It generates the permutation seed and performs the one-time seal, applying
//! the operator's demotion rules (issue #145) when any are set.

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use seal_event::dynamo::DynamoStore;
use seal_event::{DemotionConfig, seal_event};
use serde::Deserialize;
use wr_common::DemotionMode;

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
    let prequeue_table = std::env::var("PREQUEUE_TABLE")?;
    let store = DynamoStore::new(client, counters_table, prequeue_table);
    let rng = SystemRandom::new();
    let demotion = load_demotion_config();

    lambda_runtime::run(service_fn(|event: LambdaEvent<SealRequest>| {
        handle(&store, &rng, &demotion, event)
    }))
    .await
}

/// Reads the demotion rules and mode Terraform set. Neither is fatal when
/// malformed: the seal is the one thing that must happen at T−0, so a bad
/// value is logged, recorded in the report, and sealed past without demotion.
fn load_demotion_config() -> DemotionConfig {
    let rules_text = std::env::var("DEMOTION_RULES").unwrap_or_default();
    let mode = match std::env::var("DEMOTION_MODE") {
        Ok(text) => match text.parse::<DemotionMode>() {
            Ok(mode) => mode,
            Err(e) => {
                tracing::error!(error = %e, "DEMOTION_MODE rejected; observing only");
                DemotionMode::Observe
            }
        },
        Err(_unset) => DemotionMode::Observe,
    };
    let config = DemotionConfig::parse(&rules_text, mode);
    if let Err(e) = &config.rules {
        tracing::error!(error = %e, rules = %rules_text, "DEMOTION_RULES rejected; the seal will run without demotion");
    }
    config
}

async fn handle(
    store: &DynamoStore,
    rng: &SystemRandom,
    demotion: &DemotionConfig,
    event: LambdaEvent<SealRequest>,
) -> Result<(), Error> {
    let mut seed = [0u8; 32];
    rng.fill(&mut seed)
        .map_err(|_| Error::from("failed to generate seal seed"))?;
    seal_event(store, &event.payload.event_id, seed, demotion, now_secs()).await?;
    Ok(())
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
