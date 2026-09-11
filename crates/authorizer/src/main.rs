//! Lambda entry point for the origin authorizer.
//!
//! Invocation model: a regular Rust Lambda at the `CloudFront` VPC origin (NOT
//! `Lambda@Edge`, which VPC origins forbid), invoked with the
//! `API-Gateway`/ALB HTTP request shape via `lambda_http`. It reads the signing
//! key from SSM once at cold start, then decides every request locally.
//!
//! The hot-path writes when a token converts to a session are reserving the
//! single-use admission token (denying any replay) and recording the arrival;
//! a failure to record is logged and swallowed so a transient `DynamoDB` blip
//! never blocks an already-admitted visitor, while a failure to reserve denies
//! by redirecting to the waiting room.

use std::env;

use authorizer::dynamo::{DynamoStore, Store};
use authorizer::{
    Config, Decision, ProtectionRule, Reachability, Request, SessionMode, UnreachablePolicy, decide,
};
use lambda_http::{Body, Error, Request as HttpRequest, RequestExt, Response, run, service_fn};
use tracing::{error, info, warn};
use wr_common::SigningKey;

/// Resolved once at cold start and shared across invokes.
struct AppState {
    store: DynamoStore,
    key: SigningKey,
    cfg: Config,
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
    run(service_fn(|req: HttpRequest| handle(&state, req))).await
}

/// Reads configuration and the signing key. Runs in the boosted Init phase, so
/// the SSM read (a TLS handshake) also warms the client so the jitter-entropy
/// seed and handshake land on boosted Init CPU rather than the first invoke.
async fn init() -> Result<AppState, Error> {
    let config = aws_config::load_from_env().await;
    let dynamo = aws_sdk_dynamodb::Client::new(&config);
    let ssm = aws_sdk_ssm::Client::new(&config);

    let counters_table = env::var("COUNTERS_TABLE")?;
    let tokens_table = env::var("TOKENS_TABLE")?;
    let event_id = env::var("EVENT_ID")?;
    let key_parameter = env::var("SIGNING_KEY_PARAMETER")?;
    let waiting_room_url = env::var("WAITING_ROOM_URL")?;

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

    let session_cookie_name =
        env::var("SESSION_COOKIE_NAME").unwrap_or_else(|_| "vwr_session".to_owned());
    let bypass_cookie_name =
        env::var("BYPASS_COOKIE_NAME").unwrap_or_else(|_| "vwr_bypass".to_owned());
    let session_mode = session_mode_from_env();
    let unreachable_policy = if env::var("FAIL_CLOSED").is_ok_and(|v| v == "true") {
        UnreachablePolicy::FailClosed
    } else {
        UnreachablePolicy::FailOpen
    };
    let bypass_ttl_secs = env_u64("BYPASS_TTL_SECS", 300);
    let rules = protection_rules_from_env();

    Ok(AppState {
        store: DynamoStore::new(dynamo, counters_table, tokens_table),
        key,
        cfg: Config {
            event_id,
            session_cookie_name,
            bypass_cookie_name,
            session_mode,
            unreachable_policy,
            bypass_ttl_secs,
            waiting_room_url,
            rules,
        },
    })
}

/// The session lifetime mode from `SESSION_MODE` (`fixed` or `sliding`) and its
/// window variables. Defaults to a fixed one-hour session.
fn session_mode_from_env() -> SessionMode {
    match env::var("SESSION_MODE").as_deref() {
        Ok("sliding") => SessionMode::Sliding {
            idle_secs: env_u64("SESSION_IDLE_SECS", 1800),
            cap_secs: env_u64("SESSION_CAP_SECS", 28800),
        },
        // Fixed lifetime is the default: any other value or an unset variable.
        _ => SessionMode::Fixed {
            ttl_secs: env_u64("SESSION_TTL_SECS", 3600),
        },
    }
}

/// Protection rules from `PROTECTED_PATH_PREFIXES` (comma-separated). Only the
/// path-prefix rule is wired from env for now; the other matchers exist in the
/// library for callers that configure them programmatically.
fn protection_rules_from_env() -> Vec<ProtectionRule> {
    match env::var("PROTECTED_PATH_PREFIXES") {
        Ok(prefixes) => prefixes
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| ProtectionRule::PathPrefix(p.to_owned()))
            .collect(),
        // No configured prefixes means the whole origin is protected.
        Err(_) => vec![ProtectionRule::PathPrefix("/".to_owned())],
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn handle(state: &AppState, http: HttpRequest) -> Result<Response<Body>, Error> {
    let req = parse_request(&http);
    // Reachability is observed by the caller. This authorizer sits at the
    // origin and does not itself call the waiting-room backend, so on this path
    // the room is reachable by construction; the fail-open branch is exercised
    // by an upstream health signal when one is wired.
    let decision = decide(
        &req,
        &state.cfg,
        &state.key,
        now_secs(),
        Reachability::Reachable,
    );

    carry_out(&state.store, &state.cfg, &req.path, decision).await
}

/// Performs the side effects of a [`Decision`] and builds the response.
///
/// Generic over [`Store`] so the reservation and arrival writes are exercised
/// in tests with a fake store; the live handler calls it with [`DynamoStore`].
///
/// Token admission reserves the single-use admission token *before* minting a
/// session. A replay (reservation returns `false`) or a store failure both
/// deny by redirecting to the waiting room — fail closed on the credential
/// rather than mint a second session from one admission. A failure to record
/// the subsequent arrival is non-fatal: the controller tolerates a missed
/// arrival count better than we tolerate blocking an already-admitted visitor.
async fn carry_out<S: Store>(
    store: &S,
    cfg: &Config,
    path: &str,
    decision: Decision,
) -> Result<Response<Body>, Error> {
    match decision {
        Decision::Forward => Ok(forward()),
        Decision::SetSessionAndForward {
            set_cookie,
            arrival_shard,
            request_id,
            expires_at,
            stripped_path,
        } => {
            match store.reserve_token(&request_id, expires_at).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!(request_id, "admission token already consumed; redirecting");
                    return Ok(redirect(&cfg.waiting_room_url));
                }
                Err(e) => {
                    warn!(error = %e, "reserve_token failed; denying replay-ambiguous token");
                    return Ok(redirect(&cfg.waiting_room_url));
                }
            }
            if let Err(e) = store.record_arrival(&cfg.event_id, arrival_shard).await {
                // Non-fatal: admit the visitor; the controller tolerates a
                // missed arrival count better than we tolerate blocking them.
                warn!(error = %e, "failed to record arrival; admitting anyway");
            }
            info!(shard = arrival_shard, "admitted via token");
            Ok(set_session(&set_cookie, &stripped_path))
        }
        Decision::FailOpenBypass { set_cookie } => {
            warn!("waiting room unreachable; failing open with bypass cookie");
            Ok(set_session(&set_cookie, path))
        }
        Decision::Redirect { location } => Ok(redirect(&location)),
    }
}

/// Parses the `lambda_http` request into the AWS-free [`Request`] the decision
/// tree reads. Header names are lowercased; the admission token is taken from
/// the `token` or `wr_token` query parameter.
fn parse_request(http: &HttpRequest) -> Request {
    let path = http.uri().path().to_owned();
    let headers = http
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_ascii_lowercase(), v.to_owned()))
        })
        .collect::<Vec<_>>();

    let cookies = headers
        .iter()
        .find(|(k, _)| k == "cookie")
        .map(|(_, v)| parse_cookies(v))
        .unwrap_or_default();

    let query = http.query_string_parameters();
    let url_token = query
        .first("token")
        .or_else(|| query.first("wr_token"))
        .map(str::to_owned);
    let request_id = query.first("request_id").map(str::to_owned);

    Request {
        path,
        headers,
        cookies,
        url_token,
        request_id,
    }
}

/// Parses a `Cookie` header into `(name, value)` pairs.
fn parse_cookies(header: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in header.split(';') {
        if let Some((name, value)) = pair.split_once('=') {
            out.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    out
}

/// A 200 that tells the origin to serve the request. At a `CloudFront` VPC origin
/// the authorizer's success response is the pass-through; the edge forwards on a
/// `2xx` and honors `Set-Cookie`.
fn forward() -> Response<Body> {
    build(200, None, None)
}

fn set_session(set_cookie: &str, _forward_path: &str) -> Response<Body> {
    build(200, Some(set_cookie), None)
}

fn redirect(location: &str) -> Response<Body> {
    build(302, None, Some(location))
}

fn build(status: u16, set_cookie: Option<&str>, location: Option<&str>) -> Response<Body> {
    let mut builder = Response::builder().status(status);
    if let Some(cookie) = set_cookie {
        builder = builder.header("set-cookie", cookie);
    }
    if let Some(loc) = location {
        builder = builder.header("location", loc);
    }
    builder.body(Body::Empty).unwrap_or_else(|e| {
        error!(error = %e, "failed to build response");
        let mut fallback = Response::new(Body::Empty);
        *fallback.status_mut() = lambda_http::http::StatusCode::INTERNAL_SERVER_ERROR;
        fallback
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        reason = "test code panics on setup and assertion failure"
    )]

    use std::future::Future;
    use std::sync::Mutex;

    use super::carry_out;
    use authorizer::dynamo::{Store, StoreError};
    use authorizer::{Config, Decision, ProtectionRule, SessionMode, UnreachablePolicy};
    use lambda_http::{Body, Response};

    /// What [`FakeStore::reserve_token`] returns, to drive each replay branch.
    #[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
    enum ReserveResult {
        /// `Ok(true)` — first use, admission proceeds.
        #[default]
        FirstUse,
        /// `Ok(false)` — token already consumed (a replay).
        Consumed,
        /// `Err` — the `Tokens` write failed.
        Fail,
    }

    /// A fake [`Store`] that records every call and returns a configurable
    /// reservation outcome. Arrivals succeed unless `arrival_err` is set.
    #[derive(Default)]
    struct FakeStore {
        reserve: Mutex<ReserveResult>,
        reservations: Mutex<Vec<(String, u64)>>,
        arrivals: Mutex<Vec<(String, usize)>>,
        arrival_err: Mutex<bool>,
    }

    impl FakeStore {
        fn new() -> Self {
            Self::default()
        }
    }

    impl Store for FakeStore {
        fn record_arrival(
            &self,
            event_id: &str,
            shard: usize,
        ) -> impl Future<Output = Result<(), StoreError>> + Send {
            self.arrivals
                .lock()
                .unwrap()
                .push((event_id.to_owned(), shard));
            let err = *self.arrival_err.lock().unwrap();
            std::future::ready(if err {
                Err(StoreError::Backend("injected arrival failure".to_owned()))
            } else {
                Ok(())
            })
        }

        fn reserve_token(
            &self,
            request_id: &str,
            expires_at: u64,
        ) -> impl Future<Output = Result<bool, StoreError>> + Send {
            self.reservations
                .lock()
                .unwrap()
                .push((request_id.to_owned(), expires_at));
            match *self.reserve.lock().unwrap() {
                ReserveResult::FirstUse => std::future::ready(Ok(true)),
                ReserveResult::Consumed => std::future::ready(Ok(false)),
                ReserveResult::Fail => std::future::ready(Err(StoreError::Backend(
                    "injected reserve failure".to_owned(),
                ))),
            }
        }
    }

    fn cfg() -> Config {
        Config {
            event_id: "smoke".to_owned(),
            session_cookie_name: "vwr_session".to_owned(),
            bypass_cookie_name: "vwr_bypass".to_owned(),
            session_mode: SessionMode::Fixed { ttl_secs: 3600 },
            unreachable_policy: UnreachablePolicy::FailOpen,
            bypass_ttl_secs: 300,
            waiting_room_url: "https://wait.example/".to_owned(),
            rules: vec![ProtectionRule::PathPrefix("/".to_owned())],
        }
    }

    const REQ: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const TOKEN_EXPIRES: u64 = 10_000;

    fn token_decision() -> Decision {
        Decision::SetSessionAndForward {
            set_cookie: "vwr_session=abc; Max-Age=3600; HttpOnly".to_owned(),
            arrival_shard: 3,
            request_id: REQ.to_owned(),
            expires_at: TOKEN_EXPIRES,
            stripped_path: "/tickets".to_owned(),
        }
    }

    async fn run(store: &FakeStore, decision: Decision) -> Response<Body> {
        let cfg = cfg();
        carry_out(store, &cfg, "/tickets", decision).await.unwrap()
    }

    #[tokio::test]
    async fn first_token_use_is_admitted_and_recorded() {
        let store = FakeStore::new();
        let resp = run(&store, token_decision()).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers().get("set-cookie").unwrap().to_str().unwrap(),
            "vwr_session=abc; Max-Age=3600; HttpOnly"
        );
        // The reservation claimed the token's exact request id and expiry.
        assert_eq!(
            store.reservations.lock().unwrap().as_slice(),
            &[(REQ.to_owned(), TOKEN_EXPIRES)]
        );
        // The arrival was recorded for the event and shard after the reservation.
        assert_eq!(
            store.arrivals.lock().unwrap().as_slice(),
            &[("smoke".to_owned(), 3)]
        );
    }

    #[tokio::test]
    async fn token_replay_is_redirected_and_not_recorded() {
        let store = FakeStore {
            reserve: Mutex::new(ReserveResult::Consumed),
            ..FakeStore::new()
        };
        let resp = run(&store, token_decision()).await;
        assert_eq!(resp.status(), 302);
        assert_eq!(
            resp.headers().get("location").unwrap().to_str().unwrap(),
            "https://wait.example/"
        );
        // A replay mints no session.
        assert!(resp.headers().get("set-cookie").is_none());
        // reserve_token was consulted exactly once; no arrival was recorded.
        assert_eq!(store.reservations.lock().unwrap().len(), 1);
        assert!(store.arrivals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reserve_token_error_denies_and_skips_arrival() {
        let store = FakeStore {
            reserve: Mutex::new(ReserveResult::Fail),
            ..FakeStore::new()
        };
        let resp = run(&store, token_decision()).await;
        assert_eq!(resp.status(), 302);
        assert_eq!(
            resp.headers().get("location").unwrap().to_str().unwrap(),
            "https://wait.example/"
        );
        assert!(resp.headers().get("set-cookie").is_none());
        assert_eq!(store.reservations.lock().unwrap().len(), 1);
        assert!(store.arrivals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn arrival_failure_still_admits() {
        let store = FakeStore {
            arrival_err: Mutex::new(true),
            ..FakeStore::new()
        };
        let resp = run(&store, token_decision()).await;
        // Reservation succeeded; the arrival failure is non-fatal.
        assert_eq!(resp.status(), 200);
        assert!(resp.headers().get("set-cookie").is_some());
        assert_eq!(store.reservations.lock().unwrap().len(), 1);
        assert_eq!(store.arrivals.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn forward_decision_makes_no_store_calls() {
        let store = FakeStore::new();
        let resp = run(&store, Decision::Forward).await;
        assert_eq!(resp.status(), 200);
        assert!(resp.headers().get("set-cookie").is_none());
        assert!(store.reservations.lock().unwrap().is_empty());
        assert!(store.arrivals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fail_open_bypass_sets_bypass_cookie() {
        let store = FakeStore::new();
        let decision = Decision::FailOpenBypass {
            set_cookie: "vwr_bypass=1; Max-Age=300".to_owned(),
        };
        let resp = run(&store, decision).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers().get("set-cookie").unwrap().to_str().unwrap(),
            "vwr_bypass=1; Max-Age=300"
        );
        // The bypass path makes no reservation or arrival writes.
        assert!(store.reservations.lock().unwrap().is_empty());
        assert!(store.arrivals.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn redirect_decision_redirects() {
        let store = FakeStore::new();
        let resp = run(
            &store,
            Decision::Redirect {
                location: "https://wait.example/".to_owned(),
            },
        )
        .await;
        assert_eq!(resp.status(), 302);
        assert_eq!(
            resp.headers().get("location").unwrap().to_str().unwrap(),
            "https://wait.example/"
        );
    }
}
