//! Lambda entry point for the public read endpoints, served over API Gateway:
//! `GET /v1/status` and `GET /v1/queue_num?event_id&request_id`.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use lambda_http::{Body, Error, Request, RequestExt, Response, service_fn};
use read::{
    CountersCache, PollPolicy, QueueNumError, ResolvedQueueNum, parse_poll_policy, queue_num,
    status,
};
use wr_common::expr::event_key;
use wr_common::{Counters, PreQueueItem};

/// How long one execution environment holds the `Counters` item. Matched to the
/// edge's own TTL on the polled behaviours, so a reader is never staler than
/// what `CloudFront` is already serving for the same document.
const COUNTERS_TTL: std::time::Duration = std::time::Duration::from_secs(1);

struct Ctx {
    client: Client,
    counters_table: String,
    prequeue_table: String,
    positions_table: String,
    event_id: String,
    counters_cache: CountersCache,
    /// The adaptive poll policy (#69), parsed once at cold start
    /// from Terraform-set env vars. `None` when any is absent or invalid.
    poll_policy: Option<PollPolicy>,
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
    let ctx = Ctx {
        client: Client::new(&config),
        counters_table: std::env::var("COUNTERS_TABLE")?,
        prequeue_table: std::env::var("PREQUEUE_TABLE")?,
        positions_table: std::env::var("POSITIONS_TABLE")?,
        event_id: std::env::var("EVENT_ID")?,
        counters_cache: CountersCache::new(COUNTERS_TTL),
        poll_policy: load_poll_policy(),
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

async fn route(ctx: &Ctx, req: Request) -> Result<Response<Body>, Error> {
    let path = req.uri().path();
    if path.ends_with("/status") {
        handle_status(ctx).await
    } else if path.ends_with("/queue_num") {
        handle_queue_num(ctx, &req).await
    } else {
        json(404, &serde_json::json!({ "error": "not found" }))
    }
}

async fn handle_status(ctx: &Ctx) -> Result<Response<Body>, Error> {
    let Some(counters) = load_counters(ctx).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };
    json(200, &status(&counters, ctx.poll_policy))
}

async fn handle_queue_num(ctx: &Ctx, req: &Request) -> Result<Response<Body>, Error> {
    let params = req.query_string_parameters();
    let Some(request_id) = params.first("request_id") else {
        return json(400, &serde_json::json!({ "error": "request_id required" }));
    };

    let Some(counters) = load_counters(ctx).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };

    let Some(row) = load_prequeue(ctx, request_id).await? else {
        // No pre-queue row: this may be a live joiner, whose position lives in
        // the Positions table (written by assign_position), not the pre-queue.
        return respond_from_position(ctx, request_id).await;
    };

    match queue_num(&counters, &row) {
        Ok(ResolvedQueueNum::PreQueue(resp)) => json(200, &resp),
        // The row raced the seal: it holds no real position, so fall through
        // to the same Positions lookup a live joiner uses. A row there means
        // this visitor re-joined and already holds a live position; no row
        // means "not registered yet" — a 404 the straggler's client treats as
        // a miss and eventually recovers from by re-joining.
        Ok(ResolvedQueueNum::Straggler) => respond_from_position(ctx, request_id).await,
        Err(QueueNumError::NotSealed) => {
            json(409, &serde_json::json!({ "error": "event not yet open" }))
        }
        Err(QueueNumError::BadShard) => {
            json(500, &serde_json::json!({ "error": "corrupt registration" }))
        }
    }
}

/// Answers from the `Positions` table alone: 200 with the live-join position
/// if a row exists, 404 otherwise. Shared by the two callers with no
/// `PreQueue` row to resolve — a genuine live joiner, and a pre-queue
/// straggler falling back to the same lookup to recover.
async fn respond_from_position(ctx: &Ctx, request_id: &str) -> Result<Response<Body>, Error> {
    let (status, body) = position_response(load_position(ctx, request_id).await?);
    json(status, &body)
}

/// The pure decision `respond_from_position` answers with: 200 and the
/// live-join position if `Positions` holds a row, 404 otherwise. Split out so
/// the straggler's recovery branch is testable without a `DynamoDB` client.
fn position_response(position: Option<u64>) -> (u16, serde_json::Value) {
    match position {
        Some(position) => (
            200,
            serde_json::json!({ "position": position, "live_join": true }),
        ),
        None => (404, serde_json::json!({ "error": "not registered" })),
    }
}

async fn load_counters(ctx: &Ctx) -> Result<Option<Counters>, Error> {
    // Both endpoints read this one item, and /queue_num is answered per visitor
    // so the edge cannot collapse it. Without this the whole waiting room's
    // polling lands on a single DynamoDB partition key.
    if let Some(hit) = ctx.counters_cache.get(std::time::Instant::now()) {
        return Ok(hit);
    }

    let out = ctx
        .client
        .get_item()
        .table_name(&ctx.counters_table)
        .set_key(Some(event_key(&ctx.event_id)))
        .send()
        .await?;
    let counters = out
        .item()
        .map(|item| Counters::from_item(&ctx.event_id, item));

    // Stamped after the read, not before: the value is only as fresh as the
    // moment it arrived, and a slow read must not be credited a full TTL it
    // already spent in flight.
    ctx.counters_cache
        .put(std::time::Instant::now(), counters.clone());
    Ok(counters)
}

async fn load_prequeue(ctx: &Ctx, request_id: &str) -> Result<Option<PreQueueItem>, Error> {
    let out = ctx
        .client
        .get_item()
        .table_name(&ctx.prequeue_table)
        .key("r", AttributeValue::S(request_id.to_owned()))
        .send()
        .await?;
    match out.item() {
        Some(item) => Ok(Some(serde_dynamo::from_item(item.clone())?)),
        None => Ok(None),
    }
}

/// Fetches a live joiner's position from the `queue_position` attribute of their
/// `Positions` row, written by `assign_position`. Returns `None` when the request
/// id has no row.
async fn load_position(ctx: &Ctx, request_id: &str) -> Result<Option<u64>, Error> {
    let out = ctx
        .client
        .get_item()
        .table_name(&ctx.positions_table)
        .key("request_id", AttributeValue::S(request_id.to_owned()))
        .send()
        .await?;
    Ok(out.item().and_then(position_from_item))
}

/// Reads `queue_position` off a `Positions` item. `None` when the attribute is
/// absent or is not a number, so a row written in some other shape reads as "no
/// position" rather than as position zero.
fn position_from_item(item: &std::collections::HashMap<String, AttributeValue>) -> Option<u64> {
    item.get("queue_position")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

fn json<T: serde::Serialize>(status: u16, body: &T) -> Result<Response<Body>, Error> {
    let payload = serde_json::to_string(body)?;
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        // Stated rather than left to the browser's judgement. With no directive
        // at all a browser is free to apply heuristic freshness and answer a
        // poll from its own cache, which reads as a queue that has stopped
        // moving. max-age=0 keeps every poll honest; s-maxage preserves the edge
        // collapsing that makes origin load independent of how many people wait.
        .header("cache-control", "max-age=0, s-maxage=1")
        .body(Body::from(payload))?)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use wr_common::{PositionItem, PositionStatus};

    use super::*;

    #[test]
    fn reads_the_position_assign_position_wrote() {
        // Round-trips a real PositionItem, so the reader and the writer cannot
        // drift onto different attribute names or types.
        let written = PositionItem {
            request_id: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            queue_position: 4_242,
            entry_time: 1_788_000_000,
            status: PositionStatus::Issued,
            ttl: 1_788_086_700,
        };
        let item: std::collections::HashMap<String, AttributeValue> =
            serde_dynamo::to_item(&written).unwrap();
        assert_eq!(position_from_item(&item), Some(4_242));
    }

    #[test]
    fn a_row_without_a_position_reads_as_none_not_zero() {
        // Position zero is a real position at the head of the queue; a missing
        // attribute must never be reported as one.
        let mut item = std::collections::HashMap::new();
        item.insert(
            "request_id".to_owned(),
            AttributeValue::S("req-1".to_owned()),
        );
        assert_eq!(position_from_item(&item), None);
        // A timestamp-shaped string in the old attribute is not a position.
        item.insert(
            "entry_time".to_owned(),
            AttributeValue::S("2026-08-03T19:12:52.000Z".to_owned()),
        );
        assert_eq!(position_from_item(&item), None);
    }

    #[test]
    fn a_straggler_with_no_positions_row_answers_404_not_registered() {
        // A PreQueue row that raced the seal falls through to this lookup; no
        // Positions row means the re-join hasn't landed yet, not an error.
        assert_eq!(
            position_response(None),
            (404, serde_json::json!({ "error": "not registered" }))
        );
    }

    #[test]
    fn a_straggler_with_a_positions_row_answers_200_as_a_live_join() {
        // The re-join succeeded (attribute_not_exists(request_id) passed
        // because the straggler held no Positions row), so it now resolves
        // exactly like any other live joiner.
        assert_eq!(
            position_response(Some(4_242)),
            (
                200,
                serde_json::json!({ "position": 4_242, "live_join": true })
            )
        );
    }
}
