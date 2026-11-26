//! The API Gateway handler: a proxy request in, a proxy response out. The
//! runtime in `main.rs` calls it with the live store and clock, and the local
//! simulation with its own.

use lambda_http::{Body, Error, Request, RequestExt, Response};
use wr_common::Counters;
use wr_common::http::received_at;

use crate::{CountersCache, PollPolicy, QueueNumError, ResolvedQueueNum, Store, queue_num, status};

/// What one execution environment holds across invocations.
pub struct Ctx<S: Store> {
    pub store: S,
    pub event_id: String,
    pub counters_cache: CountersCache,
    /// The adaptive poll policy (#69), parsed once at cold start
    /// from Terraform-set env vars. `None` when any is absent or invalid.
    pub poll_policy: Option<PollPolicy>,
    pub session: jiff::SignedDuration,
    /// The monotonic clock the `Counters` copy ages on: `Instant::now` under
    /// the runtime.
    pub monotonic: fn() -> std::time::Instant,
}

/// Answers one read request, at the time API Gateway received it.
///
/// # Errors
///
/// The store failed, or a response could not be built.
pub async fn route<S: Store>(ctx: &Ctx<S>, req: Request) -> Result<Response<Body>, Error> {
    let path = req.uri().path();
    if path.ends_with("/status") {
        handle_status(ctx, &req).await
    } else if path.ends_with("/queue_num") {
        handle_queue_num(ctx, &req).await
    } else {
        json(404, &serde_json::json!({ "error": "not found" }))
    }
}

async fn handle_status<S: Store>(ctx: &Ctx<S>, req: &Request) -> Result<Response<Body>, Error> {
    let now = received_at(req).ok_or("request carried no API Gateway request time")?;
    let Some(counters) = load_counters(ctx).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };
    json(200, &status(&counters, ctx.poll_policy, ctx.session, now))
}

async fn handle_queue_num<S: Store>(ctx: &Ctx<S>, req: &Request) -> Result<Response<Body>, Error> {
    let params = req.query_string_parameters();
    let Some(request_id) = params.first("request_id") else {
        return json(400, &serde_json::json!({ "error": "request_id required" }));
    };

    let Some(counters) = load_counters(ctx).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };

    let Some(row) = ctx.store.load_prequeue(request_id).await? else {
        // No pre-queue row: this may be a live joiner, whose position lives in
        // the Positions table (written by assign_position), not the pre-queue.
        return respond_from_position(ctx, request_id).await;
    };

    match queue_num(&counters, &row) {
        Ok(ResolvedQueueNum::PreQueue(resp)) => json(200, &resp),
        // The row raced the open: it holds no real position, so fall through
        // to the same Positions lookup a live joiner uses. A row there means
        // this visitor re-joined and already holds a live position; no row
        // means "not registered yet" — a 404 the straggler's client treats as
        // a miss and eventually recovers from by re-joining.
        Ok(ResolvedQueueNum::Straggler) => respond_from_position(ctx, request_id).await,
        Err(QueueNumError::NotOpen) => {
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
async fn respond_from_position<S: Store>(
    ctx: &Ctx<S>,
    request_id: &str,
) -> Result<Response<Body>, Error> {
    let (status, body) = position_response(ctx.store.load_position(request_id).await?);
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

async fn load_counters<S: Store>(ctx: &Ctx<S>) -> Result<Option<Counters>, Error> {
    // Both endpoints read this one item, and /queue_num is answered per visitor
    // so the edge cannot collapse it. Without this the whole waiting room's
    // polling lands on a single DynamoDB partition key.
    if let Some(hit) = ctx.counters_cache.get((ctx.monotonic)()) {
        return Ok(hit);
    }
    let counters = ctx.store.load_counters(&ctx.event_id).await?;
    // Stamped after the read, not before: the value is only as fresh as the
    // moment it arrived, and a slow read must not be credited a full TTL it
    // already spent in flight.
    ctx.counters_cache.put((ctx.monotonic)(), counters.clone());
    Ok(counters)
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
    use wr_common::PreQueueItem;

    use super::*;
    use crate::StoreError;

    /// A store with no event in it.
    struct NoEvent;

    impl Store for NoEvent {
        fn load_counters(
            &self,
            _event_id: &str,
        ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send {
            std::future::ready(Ok(None))
        }

        fn load_prequeue(
            &self,
            _request_id: &str,
        ) -> impl Future<Output = Result<Option<PreQueueItem>, StoreError>> + Send {
            std::future::ready(Ok(None))
        }

        fn load_position(
            &self,
            _request_id: &str,
        ) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send {
            std::future::ready(Ok(None))
        }
    }

    fn ctx() -> Ctx<NoEvent> {
        Ctx {
            store: NoEvent,
            event_id: "evt".to_owned(),
            counters_cache: CountersCache::new(std::time::Duration::from_secs(1)),
            poll_policy: None,
            session: jiff::SignedDuration::from_mins(15),
            monotonic: std::time::Instant::now,
        }
    }

    fn status_request() -> Request {
        let mut req = Request::new(Body::Empty);
        *req.uri_mut() = "/_wr/v1/status".parse().unwrap_or_default();
        req
    }

    #[tokio::test]
    async fn a_status_request_without_api_gateways_time_is_refused() {
        assert!(route(&ctx(), status_request()).await.is_err());
    }

    #[tokio::test]
    async fn a_status_request_api_gateway_stamped_is_answered() {
        let req = wr_common::http::received(status_request(), jiff::Timestamp::now());
        let response = route(&ctx(), req).await;
        assert!(response.is_ok_and(|r| r.status() == 404));
    }

    #[test]
    fn a_straggler_with_no_positions_row_answers_404_not_registered() {
        // A PreQueue row that raced the open falls through to this lookup; no
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
