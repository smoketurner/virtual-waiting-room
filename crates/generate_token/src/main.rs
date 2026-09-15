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
use std::sync::Arc;

use generate_token::dynamo::DynamoStore;
use generate_token::{DEFAULT_SESSION_TTL_SECS, Denied, Store, decide};
use lambda_http::{Body, Error, Request, RequestExt, Response, run, service_fn};
use tracing::{error, info};
use wr_common::{Counters, DemotionCache, DemotionSet, Session, SigningKey};

/// Resolved once at cold start and shared across invocations. Parameterized
/// over [`Store`] so the handler's branching — what it loads, and in what
/// order — is exercisable against a mock without the AWS SDK.
struct AppState<S: Store> {
    store: S,
    key: SigningKey,
    event_id: String,
    session_cookie_name: String,
    session_ttl_secs: u64,
    /// The sealed event's demotion set (issue #145), loaded once per
    /// execution environment under the nonce the event item names.
    demotion_cache: DemotionCache,
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

async fn init() -> Result<AppState<DynamoStore>, Error> {
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
        demotion_cache: DemotionCache::new(),
    })
}

/// The demotion set the event item names, from the per-environment cache or
/// one read of its chunk items; `None` when the seal demoted nobody, and also
/// when the set cannot be read — `decide` then refuses with a retryable
/// status rather than admitting from the primary slot. Logged, because a set
/// that stays unreadable refuses every demoting event's admissions.
async fn load_demotion<S: Store>(
    state: &AppState<S>,
    counters: &Counters,
) -> Result<Option<Arc<DemotionSet>>, Error> {
    let Some(demotion) = &counters.demotion else {
        return Ok(None);
    };
    if let Some(set) = state.demotion_cache.get(&demotion.nonce) {
        return Ok(Some(set));
    }
    let Some(entries) = state
        .store
        .load_demotion_entries(&state.event_id, &demotion.nonce, demotion.chunks)
        .await?
    else {
        error!(nonce = %demotion.nonce, "demotion set chunk missing");
        return Ok(None);
    };
    match DemotionSet::from_entries(entries) {
        Ok(set) => {
            let set = Arc::new(set);
            state.demotion_cache.put(&demotion.nonce, Arc::clone(&set));
            Ok(Some(set))
        }
        Err(e) => {
            error!(nonce = %demotion.nonce, error = %e, "demotion set unreadable");
            Ok(None)
        }
    }
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn handle<S: Store>(state: &AppState<S>, req: Request) -> Result<Response<Body>, Error> {
    let Some(request_id) = request_id(&req) else {
        return json(400, &serde_json::json!({ "error": "request_id required" }));
    };

    let Some(counters) = state.store.load_counters(&state.event_id).await? else {
        return json(404, &serde_json::json!({ "error": "event not found" }));
    };

    // A live-join row is the authoritative position when one exists, so it is
    // read first and the pre-queue lookup is skipped when it answers. The
    // demotion set (issue #145) is consulted only on the pre-queue path — a
    // live `Positions` row resolves directly in `decide` without ever reading
    // the set — so its load is gated to that path too. Loading it
    // unconditionally made a live joiner pay a DynamoDB read whose result
    // `decide` provably ignores, and worse, propagated a transient send error
    // from that read as `Err` out of `handle`, failing a request whose own
    // admission logic would never have touched the set. This mirrors how
    // `crates/read/src/main.rs` keeps `load_demotion` on the pre-queue branch.
    let position_row = state.store.load_position(&request_id).await?;
    let prequeue = if position_row.is_none() {
        state.store.load_prequeue(&request_id).await?
    } else {
        None
    };
    // `decide` reaches the demotion set only through the pre-queue resolver,
    // so a visitor with no pre-queue row never needs it either; skip the load
    // there too rather than failing an unregistered request on an irrelevant
    // read.
    let demotion = if prequeue.is_some() {
        load_demotion(state, &counters).await?
    } else {
        None
    };

    let now = now_secs();
    let grant = match decide(
        &counters,
        prequeue.as_ref(),
        position_row,
        now,
        demotion.as_deref(),
    ) {
        Ok(grant) => grant,
        Err(denied) => return refusal(&denied),
    };

    // Recorded before the cookie is handed out: a visitor counted but not
    // admitted only understates the no-show rate, whereas one admitted but not
    // counted makes the controller over-release for every later interval.
    //
    // The shard is drawn at random per admission (issue #59) rather than
    // hashed from `request_id`, so it is drawn here rather than by `decide`,
    // which stays a pure function of the queue state. A draw failure is
    // logged and swallowed for the same reason a record failure is: the
    // controller tolerates a missed arrival better than the visitor tolerates
    // being refused at their turn.
    match wr_common::Shard::random() {
        Ok(shard) => {
            if let Err(e) = state.store.record_arrival(&state.event_id, shard).await {
                // Non-fatal for this visitor: the controller tolerates a
                // missed arrival better than the visitor tolerates being
                // refused at their turn. Logged at error with a stable event
                // name because the damage is cumulative and silent — every
                // uncounted arrival inflates the measured no-show rate, and
                // the controller answers that by releasing more people than
                // the origin agreed to serve. Attach a metric filter to
                // `arrival_record_failed` to alarm on it.
                error!(error = %e, event = "arrival_record_failed", "failed to record arrival; admitting anyway");
            }
        }
        Err(e) => {
            error!(error = %e, event = "arrival_shard_draw_failed", "failed to draw a random shard; admitting without recording arrival");
        }
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

    info!(position = grant.position, "admitted");

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
        Denied::Unavailable => 503,
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]
    #![expect(
        clippy::unused_async_trait_impl,
        reason = "mock store methods are synchronous stubs with no I/O to await"
    )]

    use std::collections::HashMap;

    use generate_token::StoreError;
    use wr_common::{
        DemotionRef, Phase, PositionStatus, PreQueueItem, SHARDS, SealedOffsets, Shard,
        StoredControl,
    };

    use super::*;

    /// An in-memory `Store` over a sealed, demoting event, whose demotion-chunk
    /// read always fails with a transient send error. It proves the handler
    /// gates `load_demotion` to the pre-queue path: a live joiner never reaches
    /// that read, so a live-join admission succeeds even when the read errors.
    struct MockStore {
        position: Option<(u64, PositionStatus)>,
        counters: Counters,
    }

    impl Store for MockStore {
        async fn load_counters(&self, _event_id: &str) -> Result<Option<Counters>, StoreError> {
            Ok(Some(self.counters.clone()))
        }

        async fn load_prequeue(
            &self,
            _request_id: &str,
        ) -> Result<Option<PreQueueItem>, StoreError> {
            Ok(None)
        }

        async fn load_position(
            &self,
            _request_id: &str,
        ) -> Result<Option<(u64, PositionStatus)>, StoreError> {
            Ok(self.position)
        }

        async fn load_demotion_entries(
            &self,
            _event_id: &str,
            _nonce: &str,
            _chunks: u32,
        ) -> Result<Option<Vec<String>>, StoreError> {
            Err(StoreError("transient send error".to_owned()))
        }

        async fn record_arrival(&self, _event_id: &str, _shard: Shard) -> Result<(), StoreError> {
            Ok(())
        }
    }

    /// A sealed, active event that demoted one cohort row: `counters.demotion`
    /// is `Some`, so `load_demotion` would actually issue a chunk read rather
    /// than short-circuit on the `None` arm.
    fn mock_counters() -> Counters {
        let counts = [2u64; SHARDS];
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        Counters {
            event_id: "evt".to_owned(),
            phase: Phase::Active,
            queue_counter: sealed.participant_count(),
            // Past position 3, so a live joiner holding it is admitted.
            serving_counter: 10,
            shuffle_seed: Some([9u8; 32]),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            demoted_count: 1,
            demotion: Some(DemotionRef {
                nonce: "deadbeef".to_owned(),
                chunks: 1,
            }),
            message: None,
            target_rate: None,
            stored_control: StoredControl::Open,
            fail_open_until: 0,
            starts_at: None,
        }
    }

    fn request_with_id(id: &str) -> Request {
        let query: HashMap<String, String> =
            HashMap::from([("request_id".to_owned(), id.to_owned())]);
        Request::default().with_query_string_parameters(query)
    }

    /// A live joiner holding an issued `Positions` row at position 3, against a
    /// sealed, demoting event whose demotion-chunk read always errors.
    fn live_join_state() -> AppState<MockStore> {
        AppState {
            store: MockStore {
                position: Some((3, PositionStatus::Issued)),
                counters: mock_counters(),
            },
            key: SigningKey::new(b"test-key-for-singing-and-verify-for-real-use"),
            event_id: "evt".to_owned(),
            session_cookie_name: "vwr_session".to_owned(),
            session_ttl_secs: 3600,
            demotion_cache: DemotionCache::new(),
        }
    }

    /// A live joiner is admitted even when the demotion-chunk read errors,
    /// because `decide` never consults the demotion set for a live `Positions`
    /// row. The buggy code ran `load_demotion().await?` unconditionally, so a
    /// transient `StoreError` from that read propagated as `Err` out of
    /// `handle`, failing the request with a generic 5xx instead of the 200 the
    /// visitor's own admission logic earns.
    #[tokio::test]
    async fn live_join_admits_when_demotion_read_errors() {
        let state = live_join_state();
        let req = request_with_id("test-req-id");

        let resp = handle(&state, req).await.unwrap();
        assert_eq!(resp.status(), 200);
    }
}
