//! Lambda entry point for the public read endpoints, served over API Gateway:
//! `GET /_wr/v1/status` and `GET /_wr/v1/queue_num?event_id&request_id`.

use lambda_http::{Error, Request, service_fn};
use read::dynamo::DynamoStore;
use read::http::{Ctx, route};
use read::{CountersCache, PollPolicy, parse_poll_policy};

/// How long one execution environment holds the `Counters` item. Matched to the
/// edge's own TTL on the polled behaviours, so a reader is never staler than
/// what `CloudFront` is already serving for the same document.
const COUNTERS_TTL: std::time::Duration = std::time::Duration::from_secs(1);

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let config = aws_config::load_from_env().await;
    let ctx = Ctx {
        store: DynamoStore::new(
            aws_sdk_dynamodb::Client::new(&config),
            std::env::var("COUNTERS_TABLE")?,
            std::env::var("PREQUEUE_TABLE")?,
            std::env::var("POSITIONS_TABLE")?,
        ),
        event_id: std::env::var("EVENT_ID")?,
        counters_cache: CountersCache::new(COUNTERS_TTL),
        poll_policy: load_poll_policy(),
        session: jiff::SignedDuration::from_secs(std::env::var("SESSION_TTL_SECS")?.parse()?),
        monotonic: std::time::Instant::now,
    };

    lambda_http::run(service_fn(|req: Request| route(&ctx, req))).await
}

/// Reads the three poll-policy env vars and parses them, warning if any was
/// present but the whole policy was rejected — otherwise a typo'd Terraform
/// value is indistinguishable in `CloudWatch` from a deliberate omission, and
/// both silently fall back to the client's fixed interval.
fn load_poll_policy() -> Option<PollPolicy> {
    let floor_ms = std::env::var("POLL_FLOOR_MS").ok();
    let ceiling_ms = std::env::var("POLL_CEILING_MS").ok();
    let divisor = std::env::var("POLL_DIVISOR").ok();
    let any_set = floor_ms.is_some() || ceiling_ms.is_some() || divisor.is_some();

    let policy = parse_poll_policy(
        floor_ms.as_deref(),
        ceiling_ms.as_deref(),
        divisor.as_deref(),
    );
    if policy.is_none() && any_set {
        tracing::warn!(
            poll_floor_ms = %floor_ms.as_deref().unwrap_or("<unset>"),
            poll_ceiling_ms = %ceiling_ms.as_deref().unwrap_or("<unset>"),
            poll_divisor = %divisor.as_deref().unwrap_or("<unset>"),
            "poll policy env vars present but rejected; falling back to the client's fixed interval"
        );
    }
    policy
}
