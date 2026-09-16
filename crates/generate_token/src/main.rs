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
use generate_token::{AdmissionClaim, DEFAULT_SESSION_TTL_SECS, Denied, Store, decide};
use lambda_http::{Body, Error, Request, RequestExt, Response, run, service_fn};
use tracing::{error, info};
use wr_common::{Session, SigningKey};

/// Resolved once at cold start and shared across invocations. Parameterized
/// over [`Store`] so the handler's branching — what it loads, and in what
/// order — is exercisable against a mock without the AWS SDK.
struct AppState<S: Store> {
    store: S,
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
        // Absent is a real choice — the default is a sensible hour. A value
        // that is *present* and unparseable is a typo in the deployment, and
        // silently substituting the default means the operator's chosen
        // session lifetime is not the one in force. That difference is
        // invisible until a visitor is logged out mid-checkout, which is
        // exactly the failure `session_ttl_seconds` exists to prevent.
        session_ttl_secs: match env::var("SESSION_TTL_SECS") {
            Err(_unset) => DEFAULT_SESSION_TTL_SECS,
            Ok(raw) => raw.parse().map_err(|_| {
                Error::from(format!(
                    "SESSION_TTL_SECS is set to {raw:?}, which is not a whole number of seconds"
                ))
            })?,
        },
    })
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
    // read first and the pre-queue lookup is skipped when it answers.
    let position_row = state.store.load_position(&request_id).await?;
    let prequeue = if position_row.is_none() {
        state.store.load_prequeue(&request_id).await?
    } else {
        None
    };

    let now = now_secs();
    let grant = match decide(&counters, prequeue.as_ref(), position_row, now) {
        Ok(grant) => grant,
        Err(denied) => return refusal(&denied),
    };

    // Claim the visitor's one admission, so their arrival is counted once
    // however many times they call (issue #62). `request_id` travels in a URL
    // and the waiting page polls, so a reload, a second tab or a retried
    // request all arrive here again; `record_arrival` is an unconditional
    // `ADD`, and a second one tells the controller more people showed up than
    // it released, understating the no-show rate and under-releasing for the
    // rest of the event.
    //
    // A failed claim leaves it unknown whether the arrival has been counted, so
    // it is counted: over-counting understates the no-show rate and releases
    // fewer people, while missing it releases more than the origin agreed to
    // serve. Neither refuses the visitor -- the claim governs the count, not
    // admission.
    let claim = match state
        .store
        .claim_admission(&request_id, grant.position, now)
        .await
    {
        Ok(claim) => claim,
        Err(e) => {
            error!(error = %e, event = "admission_claim_failed", "could not claim the admission; counting the arrival and admitting anyway");
            AdmissionClaim::First
        }
    };

    match claim {
        // The shard is drawn at random per admission (issue #59) rather than
        // hashed from `request_id`, so it is drawn here rather than by
        // `decide`, which stays a pure function of the queue state. A draw
        // failure is logged and swallowed for the same reason a record failure
        // is: the controller tolerates a missed arrival better than the visitor
        // tolerates being refused at their turn.
        AdmissionClaim::First => match wr_common::Shard::random() {
            Ok(shard) => {
                if let Err(e) = state.store.record_arrival(&state.event_id, shard).await {
                    // Non-fatal for this visitor: the controller tolerates a
                    // missed arrival better than the visitor tolerates being
                    // refused at their turn. Logged at error with a stable event
                    // name because the damage is cumulative and silent — every
                    // uncounted arrival inflates the measured no-show rate, and
                    // the controller answers that by releasing more people than
                    // the origin agreed to serve. The metric filter and alarm on
                    // `arrival_record_failed` live in modules/core/logging.tf.
                    error!(error = %e, event = "arrival_record_failed", "failed to record arrival; admitting anyway");
                }
            }
            Err(e) => {
                error!(error = %e, event = "arrival_shard_draw_failed", "failed to draw a random shard; admitting without recording arrival");
            }
        },
        // Already counted. The visitor still gets their session below.
        AdmissionClaim::Repeat => {}
    }

    let expires_at = now.saturating_add(state.session_ttl_secs);
    let session = Session {
        event_id: state.event_id.clone(),
        request_id: request_id.clone(),
        issued_at: now,
        expires_at,
    };
    // A signing failure must not become an empty cookie. An empty string is
    // not a well-formed JWS, so the gate would refuse it and the visitor would
    // bounce between the origin and the waiting page — while the response that
    // sent them there said `admitted: true`. Refusing is the honest answer, and
    // it is retryable: the position is still theirs, and the next poll tries
    // again.
    //
    // The arrival was already counted above, which is the right order for the
    // reason given there: a visitor counted but not admitted understates the
    // no-show rate, which under-releases. The opposite mistake over-releases.
    let Ok(credential) = session.sign(&state.key) else {
        error!(
            event = "session_sign_failed",
            "could not sign the session credential; refusing rather than issuing an empty cookie"
        );
        return json(
            500,
            &serde_json::json!({ "admitted": false, "error": "try again" }),
        );
    };
    let set_cookie = format!(
        "{}={}; Path=/; Max-Age={}; Secure; HttpOnly; SameSite=Lax",
        state.session_cookie_name, credential, state.session_ttl_secs
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
        Denied::NotAdmitting | Denied::NotOpen => 409,
        Denied::NotRegistered => 404,
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use generate_token::{AdmissionClaim, StoreError};
    use wr_common::{Counters, Phase, PositionStatus, PreQueueItem, SHARDS, Shard};

    use super::*;

    /// An in-memory store that counts what the handler did, so a test can tell
    /// "admitted and counted" from "admitted without counting".
    struct FakeStore {
        counters: Counters,
        position: Option<(u64, PositionStatus)>,
        prequeue: Option<PreQueueItem>,
        /// How many times the admission has already been claimed. The first
        /// claim wins, mirroring the conditional write.
        claims: Mutex<u32>,
        /// How many arrivals were recorded — the number that must not exceed
        /// one release.
        arrivals: Mutex<u32>,
        /// When set, every claim fails as a store error rather than answering.
        claim_fails: bool,
    }

    impl FakeStore {
        fn admitting() -> Self {
            Self {
                counters: Counters {
                    event_id: "evt".to_owned(),
                    phase: Phase::Active,
                    queue_counter: 100,
                    serving_counter: 50,
                    shuffle_seed: Some([7u8; 32]),
                    participant_count: Some(100),
                    prequeue_offsets: Some([0u64; SHARDS]),
                    target_rate: Some(10),
                    message: None,
                    stored_control: wr_common::StoredControl::Open,
                    fail_open_until: 0,
                    starts_at: None,
                },
                position: Some((3, PositionStatus::Issued)),
                prequeue: None,
                claims: Mutex::new(0),
                arrivals: Mutex::new(0),
                claim_fails: false,
            }
        }

        fn arrivals(&self) -> u32 {
            *self.arrivals.lock().unwrap()
        }
    }

    impl Store for FakeStore {
        fn load_counters(
            &self,
            _event_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<Counters>, StoreError>> + Send
        {
            std::future::ready(Ok(Some(self.counters.clone())))
        }

        fn load_prequeue(
            &self,
            _request_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<PreQueueItem>, StoreError>> + Send
        {
            std::future::ready(Ok(self.prequeue.clone()))
        }

        fn load_position(
            &self,
            _request_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<(u64, PositionStatus)>, StoreError>> + Send
        {
            std::future::ready(Ok(self.position))
        }

        fn claim_admission(
            &self,
            _request_id: &str,
            _position: u64,
            _now: u64,
        ) -> impl std::future::Future<Output = Result<AdmissionClaim, StoreError>> + Send {
            let result = if self.claim_fails {
                Err(StoreError("claim unavailable".to_owned()))
            } else {
                let mut claims = self.claims.lock().unwrap();
                *claims = claims.saturating_add(1);
                Ok(if *claims == 1 {
                    AdmissionClaim::First
                } else {
                    AdmissionClaim::Repeat
                })
            };
            std::future::ready(result)
        }

        fn record_arrival(
            &self,
            _event_id: &str,
            _shard: Shard,
        ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
            let mut arrivals = self.arrivals.lock().unwrap();
            *arrivals = arrivals.saturating_add(1);
            std::future::ready(Ok(()))
        }
    }

    fn state(store: FakeStore) -> AppState<FakeStore> {
        AppState {
            store,
            key: SigningKey::new(b"a-test-signing-key"),
            event_id: "evt".to_owned(),
            session_cookie_name: "vwr_session".to_owned(),
            session_ttl_secs: 3600,
        }
    }

    /// The request id travels in the body here; the query-string form needs the
    /// Lambda request context the runtime attaches.
    fn request() -> Request {
        Request::new(Body::from(r#"{"request_id":"r1"}"#))
    }

    #[tokio::test]
    async fn a_reloaded_page_is_admitted_again_but_counted_once() {
        // request_id travels in a URL and the waiting page polls, so the second
        // call is the normal case, not the exceptional one. Both calls must
        // hand out a working session; only the first may reach the counter the
        // controller measures no-shows against.
        let state = state(FakeStore::admitting());

        for _ in 0..3u8 {
            let response = handle(&state, request()).await.unwrap();
            assert_eq!(response.status(), 200);
            assert!(response.headers().contains_key("set-cookie"));
        }

        assert_eq!(
            state.store.arrivals(),
            1,
            "three admissions of one visitor counted more than one arrival"
        );
    }

    #[tokio::test]
    async fn an_unavailable_claim_counts_the_arrival_rather_than_losing_it() {
        // The claim failed, so whether this arrival is already counted is
        // unknown. Counting it understates the no-show rate and releases fewer
        // people; missing it releases more than the origin agreed to serve.
        let store = FakeStore {
            claim_fails: true,
            ..FakeStore::admitting()
        };
        let state = state(store);

        let response = handle(&state, request()).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(state.store.arrivals(), 1);
    }

    #[tokio::test]
    async fn a_visitor_whose_turn_has_not_come_claims_nothing() {
        // The claim is made only after `decide` admits, so a visitor still in
        // the queue leaves no row behind and no arrival counted -- otherwise
        // polling would count an arrival for everyone waiting.
        let mut store = FakeStore::admitting();
        store.position = Some((70, PositionStatus::Issued));
        let state = state(store);

        let response = handle(&state, request()).await.unwrap();

        assert_eq!(response.status(), 425);
        assert_eq!(*state.store.claims.lock().unwrap(), 0);
        assert_eq!(state.store.arrivals(), 0);
    }
}
