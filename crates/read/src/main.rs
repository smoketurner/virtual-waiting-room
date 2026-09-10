//! Lambda entry point for the public read endpoints, served over API Gateway:
//! `GET /v1/status` and `GET /v1/queue_num?event_id&request_id`.

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use lambda_http::{Body, Error, Request, RequestExt, Response, service_fn};
use read::{QueueNumError, queue_num, status};
use wr_common::{AdmissionControl, Counters, Phase, PreQueueItem, SHARDS};

struct Ctx {
    client: Client,
    counters_table: String,
    prequeue_table: String,
    positions_table: String,
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
    let ctx = Ctx {
        client: Client::new(&config),
        counters_table: std::env::var("COUNTERS_TABLE")?,
        prequeue_table: std::env::var("PREQUEUE_TABLE")?,
        positions_table: std::env::var("POSITIONS_TABLE")?,
        event_id: std::env::var("EVENT_ID")?,
    };

    lambda_http::run(service_fn(|req: Request| route(&ctx, req))).await
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
    json(200, &status(&counters))
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
        return match load_position(ctx, request_id).await? {
            Some(position) => json(
                200,
                &serde_json::json!({ "position": position, "live_join": true }),
            ),
            None => json(404, &serde_json::json!({ "error": "not registered" })),
        };
    };

    match queue_num(&counters, &row) {
        Ok(resp) => json(200, &resp),
        Err(QueueNumError::NotSealed) => {
            json(409, &serde_json::json!({ "error": "event not yet open" }))
        }
        Err(QueueNumError::BadShard) => {
            json(500, &serde_json::json!({ "error": "corrupt registration" }))
        }
    }
}

async fn load_counters(ctx: &Ctx) -> Result<Option<Counters>, Error> {
    let out = ctx
        .client
        .get_item()
        .table_name(&ctx.counters_table)
        .key("event_id", AttributeValue::S(ctx.event_id.clone()))
        .send()
        .await?;
    let Some(item) = out.item() else {
        return Ok(None);
    };
    Ok(Some(counters_from_item(&ctx.event_id, item)))
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

/// Reads the `Counters` item and its optional seal outputs. The striped
/// counters are separate items and no read path here needs them.
fn counters_from_item(
    event_id: &str,
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Counters {
    let num = |key: &str| -> Option<u64> {
        item.get(key)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse::<u64>().ok())
    };

    let shuffle_seed = item
        .get("shuffle_seed")
        .and_then(|v| v.as_b().ok())
        .and_then(|b| <[u8; 32]>::try_from(b.as_ref()).ok());

    let prequeue_offsets = item
        .get("prequeue_offsets")
        .and_then(|v| v.as_l().ok())
        .and_then(|list| {
            let parsed: Vec<u64> = list
                .iter()
                .filter_map(|e| e.as_n().ok().and_then(|s| s.parse().ok()))
                .collect();
            <[u64; SHARDS]>::try_from(parsed).ok()
        });

    let phase = item
        .get("phase")
        .and_then(|v| v.as_s().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(Phase::Idle);

    Counters {
        event_id: event_id.to_owned(),
        phase,
        queue_counter: num("queue_counter").unwrap_or(0),
        serving_counter: num("serving_counter").unwrap_or(0),
        shuffle_seed,
        participant_count: num("participant_count"),
        prequeue_offsets,
        message: item
            .get("message")
            .and_then(|v| v.as_s().ok())
            .filter(|s| !s.is_empty())
            .cloned(),
        admission_control: item
            .get("admission_control")
            .and_then(|v| v.as_s().ok())
            .and_then(|s| s.parse().ok())
            // "open", missing, or unrecognized: normal admission (safe default).
            .unwrap_or(AdmissionControl::Open),
    }
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
}
