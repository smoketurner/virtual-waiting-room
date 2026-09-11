//! Lambda entry point for `POST /v1/generate_token`.
//!
//! A waiting visitor calls this once their position has been reached. It
//! checks the queue state, records the arrival the controller measures
//! no-shows against, and returns a signed session cookie: the `CloudFront`
//! Function gate (issue #71) verifies it itself at the edge on every later
//! request to the protected origin.
//!
//! The signing key is read from SSM once at cold start, in the boosted Init
//! phase, so the TLS handshake does not land on a visitor's request.

use std::env;

use generate_token::dynamo::DynamoStore;
use generate_token::{DEFAULT_SESSION_TTL_SECS, Denied, Store, decide};
use lambda_http::{Body, Error, Request, RequestExt, Response, run, service_fn};
use tracing::{error, info};
use wr_common::{Session, SigningKey};

/// Resolved once at cold start and shared across invocations.
struct AppState {
    store: DynamoStore,
    key: SigningKey,
    event_id: String,
    session_cookie_name: String,
    session_ttl_secs: u64,
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
    run(service_fn(|req: Request| handle(&state, req))).await
}

async fn init() -> Result<AppState, Error> {
    let config = aws_config::load_from_env().await;
    let dynamo = aws_sdk_dynamodb::Client::new(&config);
    let ssm = aws_sdk_ssm::Client::new(&config);

    let key_parameter = env::var("SIGNING_KEY_PARAMETER")?;

    let secret = ssm
        .get_parameter()
        .name(&key_parameter)
        .with_decryption(true)
        .send()
        .await?;
    let key_material = secret
        .parameter()
        .and_then(aws_sdk_ssm::types::Parameter::value)
        .ok_or("signing key parameter is empty")?;
    let key = SigningKey::new(key_material.as_bytes());

    Ok(AppState {
        store: DynamoStore::new(
            dynamo,
            env::var("COUNTERS_TABLE")?,
            env::var("PREQUEUE_TABLE")?,
            env::var("POSITIONS_TABLE")?,
        ),
        key,
        event_id: env::var("EVENT_ID")?,
        session_cookie_name: env::var("SESSION_COOKIE_NAME")
            .unwrap_or_else(|_| "vwr_session".to_owned()),
        session_ttl_secs: env::var("SESSION_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SESSION_TTL_SECS),
    })
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn handle(state: &AppState, req: Request) -> Result<Response<Body>, Error> {
    let Some(request_id) = request_id(&req) else {
        return json(400, &serde_json::json!({ "error": "request_id required" }));
    };

    let Some(counters) = state.store.load_counters(&state.event_id).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };

    // A live-join row is the authoritative position when one exists, so it is
    // read first and the pre-queue lookup is skipped when it answers.
    let position_row = state.store.load_position(&request_id).await?;
    let prequeue = match position_row {
        Some(_) => None,
        None => state.store.load_prequeue(&request_id).await?,
    };

    let now = now_secs();
    let grant = match decide(&counters, &request_id, prequeue.as_ref(), position_row, now) {
        Ok(grant) => grant,
        Err(denied) => return refusal(&denied),
    };

    // Recorded before the cookie is handed out: a visitor counted but not
    // admitted only understates the no-show rate, whereas one admitted but not
    // counted makes the controller over-release for every later interval.
    if let Err(e) = state
        .store
        .record_arrival(&state.event_id, grant.arrival_shard)
        .await
    {
        // Non-fatal for this visitor: the controller tolerates a missed
        // arrival better than the visitor tolerates being refused at their
        // turn. Logged at error with a stable event name because the damage is
        // cumulative and silent — every uncounted arrival inflates the measured
        // no-show rate, and the controller answers that by releasing more
        // people than the origin agreed to serve. Attach a metric filter to
        // `arrival_record_failed` to alarm on it.
        error!(error = %e, event = "arrival_record_failed", "failed to record arrival; admitting anyway");
    }

    let expires_at = now.saturating_add(state.session_ttl_secs);
    let session = Session {
        event_id: state.event_id.clone(),
        request_id: request_id.clone(),
        issued_at: now,
        expires_at,
    };
    let set_cookie = format!(
        "{}={}; Path=/; Max-Age={}; Secure; HttpOnly; SameSite=Lax",
        state.session_cookie_name,
        session.sign(&state.key),
        state.session_ttl_secs
    );

    info!(
        position = grant.position,
        shard = grant.arrival_shard,
        "admitted"
    );

    let body = serde_json::to_string(&serde_json::json!({
        "admitted": true,
        "position": grant.position,
        "expires_at": expires_at,
    }))?;
    Ok(Response::builder()
        .status(200)
        .header("content-type", "application/json")
        // The waiting page polls this; a cached admission would hand one
        // visitor's cookie to another.
        .header("cache-control", "no-store")
        .header("set-cookie", set_cookie)
        .body(Body::from(body))?)
}

/// The request id from the query string or the JSON body, so the endpoint works
/// for both a form post and a fetch.
fn request_id(req: &Request) -> Option<String> {
    if let Some(id) = req.query_string_parameters().first("request_id") {
        return Some(id.to_owned());
    }
    let body = std::str::from_utf8(req.body()).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    parsed
        .get("request_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// Maps a refusal to a status the waiting page can act on: 425 means "keep
/// polling", everything else means "stop and show why".
fn refusal(denied: &Denied) -> Result<Response<Body>, Error> {
    let status = match denied {
        Denied::StillQueued { .. } => 425,
        Denied::NotAdmitting | Denied::NotSealed => 409,
        Denied::NotRegistered => 404,
        Denied::Spent => 410,
        Denied::Corrupt => 500,
    };
    let mut payload = serde_json::json!({
        "admitted": false,
        "error": denied.to_string(),
    });
    if let Denied::StillQueued { position, serving } = denied {
        payload["position"] = (*position).into();
        payload["serving_position"] = (*serving).into();
    }
    json(status, &payload)
}

fn json<T: serde::Serialize>(status: u16, body: &T) -> Result<Response<Body>, Error> {
    let payload = serde_json::to_string(body)?;
    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Body::from(payload))?)
}
