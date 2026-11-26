//! Lambda entry point for `POST /_wr/v1/enter` and `GET /_wr/v1/wait`: the queue for
//! a visitor without JavaScript (issue #67). See the library docs.

use std::env;

use generate_token::dynamo::DynamoStore;
use lambda_http::{Error, Request, run, service_fn};
use nojs::http::{AppState, handle};
use nojs::sqs::SqsJoinQueue;
use wr_common::SigningKey;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let state = init().await?;
    run(service_fn(|req: Request| handle(&state, req))).await
}

async fn init() -> Result<AppState<DynamoStore, SqsJoinQueue>, Error> {
    let config = aws_config::load_from_env().await;
    let ssm = aws_sdk_ssm::Client::new(&config);
    let secret = ssm
        .get_parameter()
        .name(env::var("SIGNING_KEY_PARAMETER")?)
        .with_decryption(true)
        .send()
        .await?;
    let key_material = secret
        .parameter()
        .and_then(aws_sdk_ssm::types::Parameter::value)
        .ok_or("signing key parameter is empty")?;

    Ok(AppState {
        store: DynamoStore::new(
            aws_sdk_dynamodb::Client::new(&config),
            env::var("COUNTERS_TABLE")?,
            env::var("PREQUEUE_TABLE")?,
            env::var("POSITIONS_TABLE")?,
        ),
        queue: SqsJoinQueue::new(
            aws_sdk_sqs::Client::new(&config),
            env::var("JOIN_QUEUE_URL")?,
        ),
        key: SigningKey::new(key_material.as_bytes()),
        event_id: env::var("EVENT_ID")?,
        session_cookie_name: env::var("SESSION_COOKIE_NAME")
            .unwrap_or_else(|_| "vwr_session".to_owned()),
        // Required, for the reason generate_token gives.
        session_ttl_secs: {
            let raw = env::var("SESSION_TTL_SECS")?;
            raw.parse().map_err(|_| {
                Error::from(format!(
                    "SESSION_TTL_SECS is set to {raw:?}, which is not a whole number of seconds"
                ))
            })?
        },
    })
}
