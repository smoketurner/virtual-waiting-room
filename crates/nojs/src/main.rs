//! Lambda entry point for `POST /v1/enter` and `GET /v1/wait`: the queue for
//! a visitor without JavaScript (issue #67). See the library docs.

use std::env;

use askama::Template as _;
use generate_token::dynamo::DynamoStore;
use generate_token::{
    DEFAULT_SESSION_TTL_SECS, Minting, Store, admit, session_set_cookie, viewer_ip,
};
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, Request, Response, run, service_fn};
use nojs::page::{WaitHtml, encode_component};
use nojs::sqs::SqsJoinQueue;
use nojs::{
    ENTER_PATH, Identity, JoinQueue, WAIT_PATH, WaitOutcome, WaitPage, clear_identity_cookie,
    enter, identity_from_cookies, next_from_referer, next_param, outcome, safe_next,
};
use tracing::error;
use wr_common::SigningKey;

struct AppState<S: Store, Q: JoinQueue> {
    store: S,
    queue: Q,
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
        // Present and unparseable is a deployment typo, not a default, for the
        // reason generate_token gives.
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

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers().get(name).and_then(|v| v.to_str().ok())
}

async fn handle<S: Store, Q: JoinQueue>(
    state: &AppState<S, Q>,
    req: Request,
) -> Result<Response<Body>, Error> {
    // Matched by suffix so the API Gateway stage prefix does not matter.
    let path = req.uri().path().to_owned();
    if req.method() == Method::POST && path.ends_with(ENTER_PATH) {
        return handle_enter(state, &req).await;
    }
    if req.method() == Method::GET && path.ends_with(WAIT_PATH) {
        return handle_wait(state, &req, now_ms() / 1000).await;
    }
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("cache-control", "no-store")
        .body(Body::from("not found"))?)
}

/// Joins, once. A visitor who already holds an identity is sent to the wait
/// page rather than joined again, which would take them a second place.
async fn handle_enter<S: Store, Q: JoinQueue>(
    state: &AppState<S, Q>,
    req: &Request,
) -> Result<Response<Body>, Error> {
    // The wait page's own form carries next in its URL; the waiting page's
    // <noscript> form cannot, so it comes from the Referer.
    let next = match req.uri().query().and_then(next_param) {
        Some(raw) => safe_next(Some(raw)),
        None => next_from_referer(header(req, "referer")),
    };
    let location = format!("{WAIT_PATH}?next={}", encode_component(&next));

    if header(req, "cookie")
        .and_then(identity_from_cookies)
        .is_some()
    {
        return redirect(&location, None);
    }
    let identity = match Identity::mint(now_ms()) {
        Ok(identity) => identity,
        Err(e) => {
            error!(error = %e, event = "nojs_join_failed", "could not mint an identity");
            return page(
                StatusCode::SERVICE_UNAVAILABLE,
                &WaitPage::Unavailable,
                &next,
                None,
            );
        }
    };
    if let Err(e) = enter(&state.queue, &state.event_id, &identity).await {
        // No cookie: the visitor holds no place, and the form is still theirs
        // to press again.
        error!(error = %e, event = "nojs_join_failed", "could not enqueue a no-JavaScript join");
        return page(
            StatusCode::SERVICE_UNAVAILABLE,
            &WaitPage::NotInLine,
            &next,
            None,
        );
    }
    redirect(&location, Some(&identity.set_cookie()))
}

async fn handle_wait<S: Store, Q: JoinQueue>(
    state: &AppState<S, Q>,
    req: &Request,
    now: u64,
) -> Result<Response<Body>, Error> {
    let next = safe_next(req.uri().query().and_then(next_param));
    let Some(identity) = header(req, "cookie").and_then(identity_from_cookies) else {
        return page(StatusCode::OK, &WaitPage::NotInLine, &next, None);
    };
    let minting = Minting {
        key: &state.key,
        event_id: &state.event_id,
        session_ttl_secs: state.session_ttl_secs,
    };
    let viewer = header(req, "cloudfront-viewer-address").and_then(viewer_ip);
    let admission = match admit(
        &state.store,
        &minting,
        &identity.request_id,
        &identity.secret,
        viewer,
        now,
    )
    .await
    {
        Ok(admission) => admission,
        Err(e) => {
            error!(error = %e, "no-JavaScript wait page could not read the queue");
            return page(
                StatusCode::SERVICE_UNAVAILABLE,
                &WaitPage::Unavailable,
                &next,
                None,
            );
        }
    };
    match outcome(admission) {
        WaitOutcome::Admit { credential } => redirect(
            &next,
            Some(&session_set_cookie(
                &state.session_cookie_name,
                &credential,
                state.session_ttl_secs,
            )),
        ),
        WaitOutcome::Show(WaitPage::Lost) => page(
            StatusCode::OK,
            &WaitPage::Lost,
            &next,
            Some(&clear_identity_cookie()),
        ),
        WaitOutcome::Show(shown) => page(StatusCode::OK, &shown, &next, None),
    }
}

fn redirect(location: &str, set_cookie: Option<&str>) -> Result<Response<Body>, Error> {
    let mut builder = Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header("location", location)
        .header("cache-control", "no-store");
    if let Some(cookie) = set_cookie {
        builder = builder.header("set-cookie", cookie);
    }
    Ok(builder.body(Body::Empty)?)
}

fn page(
    status: StatusCode,
    shown: &WaitPage,
    next: &str,
    set_cookie: Option<&str>,
) -> Result<Response<Body>, Error> {
    let html = WaitHtml::of(shown, next).render()?;
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-store");
    if let Some(cookie) = set_cookie {
        builder = builder.header("set-cookie", cookie);
    }
    Ok(builder.body(Body::from(html))?)
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use generate_token::{AdmissionClaim, PositionRow, StoreError};
    use nojs::{IDENTITY_COOKIE, QueueError};
    use wr_common::{
        Counters, Phase, PositionStatus, PossessionSecret, PreQueueItem, SHARDS, Session, Shard,
    };

    use super::*;

    const ID: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct FakeStore {
        serving: u64,
        position: Option<PositionRow>,
    }

    impl Store for FakeStore {
        fn load_counters(
            &self,
            _event_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<Counters>, StoreError>> + Send
        {
            std::future::ready(Ok(Some(Counters {
                event_id: "evt".to_owned(),
                phase: Phase::Active,
                queue_counter: 1_000,
                serving_counter: self.serving,
                shuffle_seed: Some([7u8; 32]),
                participant_count: Some(0),
                prequeue_offsets: Some([0u64; SHARDS]),
                target_rate: Some(10),
                message: None,
                stored_control: wr_common::StoredControl::Open,
                fail_open_until: 0,
                starts_at: None,
            })))
        }
        fn load_prequeue(
            &self,
            _request_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<PreQueueItem>, StoreError>> + Send
        {
            std::future::ready(Ok(None))
        }
        fn load_position(
            &self,
            _request_id: &str,
        ) -> impl std::future::Future<Output = Result<Option<PositionRow>, StoreError>> + Send
        {
            std::future::ready(Ok(self.position.clone()))
        }
        fn claim_admission(
            &self,
            _request_id: &str,
            _position: u64,
            _digest: &wr_common::SecretDigest,
            _now: u64,
        ) -> impl std::future::Future<Output = Result<AdmissionClaim, StoreError>> + Send {
            std::future::ready(Ok(AdmissionClaim::First))
        }
        fn record_arrival(
            &self,
            _event_id: &str,
            _shard: Shard,
        ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
            std::future::ready(Ok(()))
        }
    }

    struct FakeQueue {
        sent: Mutex<Vec<String>>,
        fails: bool,
    }

    impl JoinQueue for FakeQueue {
        fn send(
            &self,
            body: String,
        ) -> impl std::future::Future<Output = Result<(), QueueError>> + Send {
            let result = if self.fails {
                Err(QueueError("down".to_owned()))
            } else {
                self.sent.lock().unwrap().push(body);
                Ok(())
            };
            std::future::ready(result)
        }
    }

    fn state(position: Option<u64>, fails: bool) -> AppState<FakeStore, FakeQueue> {
        AppState {
            store: FakeStore {
                serving: 50,
                position: position.map(|p| PositionRow {
                    position: p,
                    status: PositionStatus::Issued,
                    digest: Some(PossessionSecret::parse(SECRET).unwrap().digest()),
                }),
            },
            queue: FakeQueue {
                sent: Mutex::new(Vec::new()),
                fails,
            },
            key: SigningKey::new(b"a-test-signing-key"),
            event_id: "evt".to_owned(),
            session_cookie_name: "vwr_session".to_owned(),
            session_ttl_secs: 3600,
        }
    }

    fn request(method: &str, uri: &str, headers: &[(&str, &str)]) -> Request {
        let mut builder = lambda_http::http::Request::builder()
            .method(method)
            .uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        builder.body(Body::Empty).unwrap()
    }

    fn text(response: &Response<Body>) -> &str {
        if let Body::Text(html) = response.body() {
            html
        } else {
            ""
        }
    }

    fn identity_cookie(secret: &str) -> String {
        format!("{IDENTITY_COOKIE}={ID}.{secret}")
    }

    #[tokio::test]
    async fn entering_joins_once_and_keeps_the_identity_server_side() {
        let state = state(None, false);
        let response = handle(
            &state,
            request(
                "POST",
                "https://x/v1/enter",
                &[(
                    "referer",
                    "https://x/_wr/waiting.html?r=none&next=%2Fcheckout",
                )],
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 303);
        assert_eq!(response.headers()["location"], "/v1/wait?next=%2Fcheckout");
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        assert!(cookie.starts_with(&format!("{IDENTITY_COOKIE}=")));
        assert!(cookie.contains("HttpOnly"));

        let sent = state.queue.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let msg: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        let held =
            Identity::parse(cookie.split(';').next().unwrap().split_once('=').unwrap().1).unwrap();
        assert_eq!(msg["request_id"], held.request_id);
        assert_eq!(msg["h"], held.secret.digest().as_str());
    }

    #[tokio::test]
    async fn entering_again_does_not_take_a_second_place() {
        let state = state(None, false);
        let response = handle(
            &state,
            request("POST", "/v1/enter", &[("cookie", &identity_cookie(SECRET))]),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 303);
        assert!(state.queue.sent.lock().unwrap().is_empty());
        assert!(response.headers().get("set-cookie").is_none());
    }

    #[tokio::test]
    async fn a_join_the_queue_refused_holds_no_place() {
        let state = state(None, true);
        let response = handle(&state, request("POST", "/v1/enter", &[]))
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert!(response.headers().get("set-cookie").is_none());
    }

    #[tokio::test]
    async fn waiting_without_an_identity_offers_the_form() {
        let response = handle(&state(None, false), request("GET", "/v1/wait", &[]))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let html = text(&response);
        assert!(html.contains(r#"action="/v1/enter?next=%2F""#));
    }

    #[tokio::test]
    async fn a_waiting_visitor_sees_their_place() {
        let response = handle(
            &state(Some(700), false),
            request(
                "GET",
                "/v1/wait?next=%2Fcheckout",
                &[("cookie", &identity_cookie(SECRET))],
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 200);
        let html = text(&response);
        assert!(html.contains("700"));
        assert!(html.contains("http-equiv=\"refresh\""));
    }

    #[tokio::test]
    async fn a_reached_visitor_is_admitted_and_sent_on() {
        let state = state(Some(3), false);
        let response = handle(
            &state,
            request(
                "GET",
                "/v1/wait?next=%2Fcheckout",
                &[
                    ("cookie", &identity_cookie(SECRET)),
                    ("cloudfront-viewer-address", "198.51.100.7:4000"),
                ],
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 303);
        assert_eq!(response.headers()["location"], "/checkout");
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        let credential = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        let session = Session::verify(credential, &state.key, 1).unwrap();
        assert_eq!(session.request_id, ID);
        assert_eq!(session.ip_tag, state.key.ip_tag("198.51.100.7"));
    }

    #[tokio::test]
    async fn a_cookie_with_the_wrong_secret_is_forgotten() {
        let response = handle(
            &state(Some(3), false),
            request(
                "GET",
                "/v1/wait",
                &[("cookie", &identity_cookie(&"B".repeat(43)))],
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 200);
        assert!(
            response.headers()["set-cookie"]
                .to_str()
                .unwrap()
                .contains("Max-Age=0")
        );
    }
}
