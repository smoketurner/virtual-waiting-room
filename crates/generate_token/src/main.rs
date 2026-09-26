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
use generate_token::{
    Admission, DEFAULT_SESSION_TTL_SECS, Denied, Minting, Store, admit, session_set_cookie,
    viewer_ip,
};
use lambda_http::{Body, Error, Request, RequestExt, Response, run, service_fn};
use tracing::info;
use wr_common::{PossessionSecret, SigningKey};

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
    let Some(secret) = possession_secret(&req) else {
        return json(400, &serde_json::json!({ "error": "secret required" }));
    };

    let viewer = req
        .headers()
        .get("cloudfront-viewer-address")
        .and_then(|v| v.to_str().ok())
        .and_then(viewer_ip);
    let now = now_secs();
    let admission = admit(
        &state.store,
        &Minting {
            key: &state.key,
            event_id: &state.event_id,
            session_ttl_secs: state.session_ttl_secs,
        },
        &request_id,
        &secret,
        viewer,
        now,
    )
    .await?;
    let (position, expires_at, credential) = match admission {
        Admission::Admitted {
            position,
            expires_at,
            credential,
        } => (position, expires_at, credential),
        Admission::EventNotFound => {
            return json(404, &serde_json::json!({ "error": "event not found" }));
        }
        Admission::Refused(denied) => return refusal(&denied),
        Admission::SignFailed => {
            return json(
                500,
                &serde_json::json!({ "admitted": false, "error": "try again" }),
            );
        }
    };
    let set_cookie = session_set_cookie(
        &state.session_cookie_name,
        &credential,
        state.session_ttl_secs,
    );

    info!(position, "admitted");

    let body = serde_json::to_string(&serde_json::json!({
        "admitted": true,
        "position": position,
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

/// The possession secret (issue #62), from the JSON body only: a query string
/// is written to access logs and browser history, which is exactly where the
/// `request_id` it protects already leaks.
fn possession_secret(req: &Request) -> Option<PossessionSecret> {
    let body = std::str::from_utf8(req.body()).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    let raw = parsed.get("secret").and_then(serde_json::Value::as_str)?;
    PossessionSecret::parse(raw).ok()
}

/// Maps a refusal to a status the waiting page can act on: 425 means "keep
/// polling", everything else means "stop and show why".
fn refusal(denied: &Denied) -> Result<Response<Body>, Error> {
    let status = match denied {
        Denied::StillQueued { .. } => 425,
        Denied::NotAdmitting | Denied::NotOpen => 409,
        Denied::NotRegistered => 404,
        Denied::NotHolder => 403,
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
    use wr_common::{Counters, Phase, PositionStatus, PreQueueItem, SHARDS, Session, Shard};

    use super::*;

    /// An in-memory store that counts what the handler did, so a test can tell
    /// "admitted and counted" from "admitted without counting".
    struct FakeStore {
        counters: Counters,
        position: Option<generate_token::PositionRow>,
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
                position: Some(row(3)),
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
        ) -> impl std::future::Future<
            Output = Result<Option<generate_token::PositionRow>, StoreError>,
        > + Send {
            std::future::ready(Ok(self.position.clone()))
        }

        fn claim_admission(
            &self,
            _request_id: &str,
            _position: u64,
            _digest: &wr_common::SecretDigest,
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
        Request::new(Body::from(format!(
            r#"{{"request_id":"r1","secret":"{SECRET}"}}"#
        )))
    }

    const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn row(position: u64) -> generate_token::PositionRow {
        generate_token::PositionRow {
            position,
            status: PositionStatus::Issued,
            digest: Some(PossessionSecret::parse(SECRET).unwrap().digest()),
        }
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
        store.position = Some(row(70));
        let state = state(store);

        let response = handle(&state, request()).await.unwrap();

        assert_eq!(response.status(), 425);
        assert_eq!(*state.store.claims.lock().unwrap(), 0);
        assert_eq!(state.store.arrivals(), 0);
    }

    #[tokio::test]
    async fn the_request_id_alone_mints_nothing() {
        // Issue #62: a request id read from a log or a shared URL, presented
        // with no secret or with the wrong one, gets no cookie, no claim, and
        // no position in the answer.
        let state = state(FakeStore::admitting());
        for body in [
            r#"{"request_id":"r1"}"#.to_owned(),
            r#"{"request_id":"r1","secret":"short"}"#.to_owned(),
            format!(r#"{{"request_id":"r1","secret":"{}"}}"#, "B".repeat(43)),
        ] {
            let response = handle(&state, Request::new(Body::from(body.clone())))
                .await
                .unwrap();
            assert!(
                matches!(response.status().as_u16(), 400 | 403),
                "{body}: {}",
                response.status()
            );
            assert!(!response.headers().contains_key("set-cookie"), "{body}");
            let text = std::str::from_utf8(response.body()).unwrap();
            assert!(!text.contains("position"), "{body}: {text}");
        }
        assert_eq!(*state.store.claims.lock().unwrap(), 0);
        assert_eq!(state.store.arrivals(), 0);
    }

    #[tokio::test]
    async fn a_secret_in_the_query_string_is_not_accepted() {
        // Accepting it there would put the credential back in access logs.
        let request =
            Request::new(Body::from(r#"{"request_id":"r1"}"#)).with_query_string_parameters(
                std::collections::HashMap::from([("secret".to_owned(), SECRET.to_owned())]),
            );
        let response = handle(&state(FakeStore::admitting()), request)
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn the_session_is_tagged_with_the_viewer_network() {
        let state = state(FakeStore::admitting());
        let mut req = request();
        req.headers_mut().insert(
            "cloudfront-viewer-address",
            "198.51.100.7:46532".parse().unwrap(),
        );
        let response = handle(&state, req).await.unwrap();
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        let credential = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        let session = Session::verify(credential, &state.key, 1).unwrap();
        assert_eq!(session.ip_tag, state.key.ip_tag("198.51.100.7"));

        // No header (a request that bypassed CloudFront): minted untagged.
        let response = handle(&state, request()).await.unwrap();
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        let credential = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        assert_eq!(
            Session::verify(credential, &state.key, 1).unwrap().ip_tag,
            None
        );
    }
}
