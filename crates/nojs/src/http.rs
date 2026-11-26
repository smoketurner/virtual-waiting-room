//! The API Gateway handler: a proxy request in, a proxy response out. The
//! runtime in `main.rs` calls it with the live store and queue, and the local
//! simulation with its own.

use askama::Template as _;
use generate_token::{Minting, Store, admit, session_set_cookie, viewer_ip};
use jiff::Timestamp;
use lambda_http::http::{Method, StatusCode};
use lambda_http::{Body, Error, Request, Response};
use tracing::{error, warn};
use wr_common::SigningKey;
use wr_common::http::received_at;

use crate::page::{WaitHtml, encode_component};
use crate::{
    ENTER_PATH, Identity, JoinQueue, Unanswered, WAIT_PATH, WaitOutcome, WaitPage,
    clear_identity_cookie, enter, identity_from_cookies, next_from_referer, next_param, outcome,
    safe_next,
};

/// Resolved once at cold start and shared across invocations.
pub struct AppState<S: Store, Q: JoinQueue> {
    pub store: S,
    pub queue: Q,
    pub key: SigningKey,
    pub event_id: String,
    pub session_cookie_name: String,
    pub session: jiff::SignedDuration,
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers().get(name).and_then(|v| v.to_str().ok())
}

/// Answers one no-JavaScript request, at the time API Gateway received it.
///
/// # Errors
///
/// The request carried no API Gateway time, or a response could not be
/// built.
pub async fn handle<S: Store, Q: JoinQueue>(
    state: &AppState<S, Q>,
    req: Request,
) -> Result<Response<Body>, Error> {
    let now = received_at(&req).ok_or("request carried no API Gateway request time")?;
    // Matched by suffix so the API Gateway stage prefix does not matter.
    let path = req.uri().path().to_owned();
    if req.method() == Method::POST && path.ends_with(ENTER_PATH) {
        return handle_enter(state, &req, now).await;
    }
    if req.method() == Method::GET && path.ends_with(WAIT_PATH) {
        return handle_wait(state, &req, now).await;
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
    now: Timestamp,
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
    let identity = match Identity::mint(now) {
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
    now: Timestamp,
) -> Result<Response<Body>, Error> {
    let next = safe_next(req.uri().query().and_then(next_param));
    let Some(identity) = header(req, "cookie").and_then(identity_from_cookies) else {
        return page(StatusCode::OK, &WaitPage::NotInLine, &next, None);
    };
    let minting = Minting {
        key: &state.key,
        event_id: &state.event_id,
        session: state.session,
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
                state.session,
            )),
        ),
        WaitOutcome::Show(WaitPage::Lost) => page(
            StatusCode::OK,
            &WaitPage::Lost,
            &next,
            Some(&clear_identity_cookie()),
        ),
        WaitOutcome::Show(WaitPage::Joining) => {
            handle_unanswered(state, &identity, &next, now).await
        }
        WaitOutcome::Show(shown) => page(StatusCode::OK, &shown, &next, None),
    }
}

/// The wait page for a visitor no registration exists for (issue #251).
///
/// Usually the join is still in flight. One the queue or the ingest lost
/// would otherwise leave this page refreshing forever, since `/_wr/v1/enter` will
/// not join a visitor who already holds an identity: so an overdue join is
/// sent again from here, a bounded number of times, and after that the
/// visitor is told and gets the form back.
async fn handle_unanswered<S: Store, Q: JoinQueue>(
    state: &AppState<S, Q>,
    identity: &Identity,
    next: &str,
    now: Timestamp,
) -> Result<Response<Body>, Error> {
    match identity.unanswered(now) {
        Unanswered::InFlight => page(StatusCode::OK, &WaitPage::Joining, next, None),
        Unanswered::Resend(restamped) => {
            if let Err(e) = enter(&state.queue, &state.event_id, &restamped).await {
                // The cookie keeps its old stamp, so the next load tries again.
                error!(error = %e, event = "nojs_join_failed", "could not resend a no-JavaScript join");
                return page(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &WaitPage::Unavailable,
                    next,
                    None,
                );
            }
            warn!(
                event = "nojs_join_resent",
                rejoins = restamped.rejoins,
                "resent a no-JavaScript join that never registered"
            );
            page(
                StatusCode::OK,
                &WaitPage::Joining,
                next,
                Some(&restamped.set_cookie()),
            )
        }
        Unanswered::GaveUp => {
            error!(
                event = "nojs_join_lost",
                "a no-JavaScript join never registered after every resend"
            );
            page(
                StatusCode::OK,
                &WaitPage::JoinLost,
                next,
                Some(&clear_identity_cookie()),
            )
        }
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

    use crate::{IDENTITY_COOKIE, QueueError};
    use generate_token::{PositionRow, StoreError};
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
                origin_concurrency: Some(10),
                message: None,
                stored_control: wr_common::StoredControl::Open,
                fail_open_until: None,
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
        fn take_slot(
            &self,
            _event_id: &str,
            _shard: Shard,
        ) -> impl std::future::Future<Output = Result<bool, StoreError>> + Send {
            std::future::ready(Ok(true))
        }
        fn mark_admitted(
            &self,
            _request_id: &str,
            _position: u64,
            _digest: &wr_common::SecretDigest,
            _now: Timestamp,
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
                    admitted_at: None,
                }),
            },
            queue: FakeQueue {
                sent: Mutex::new(Vec::new()),
                fails,
            },
            key: SigningKey::new(b"a-test-signing-key"),
            event_id: "evt".to_owned(),
            session_cookie_name: "vwr_session".to_owned(),
            session: jiff::SignedDuration::from_hours(1),
        }
    }

    fn request(method: &str, uri: &str, headers: &[(&str, &str)]) -> Request {
        let mut builder = lambda_http::http::Request::builder()
            .method(method)
            .uri(uri);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        wr_common::http::received(builder.body(Body::Empty).unwrap(), Timestamp::now())
    }

    fn text(response: &Response<Body>) -> &str {
        if let Body::Text(html) = response.body() {
            html
        } else {
            ""
        }
    }

    /// The second every wait in these tests is served at.
    const NOW: Timestamp = Timestamp::constant(1_700_000_000, 0);

    /// A cookie whose join was sent `ago` seconds before [`NOW`].
    fn stamped_cookie(secret: &str, ago: i64, rejoins: u8) -> String {
        let joined = NOW.as_second() - ago;
        format!("{IDENTITY_COOKIE}={ID}.{secret}.{joined}.{rejoins}")
    }

    fn identity_cookie(secret: &str) -> String {
        stamped_cookie(secret, 0, 0)
    }

    async fn wait_with(state: &AppState<FakeStore, FakeQueue>, cookie: &str) -> Response<Body> {
        handle_wait(
            state,
            &request(
                "GET",
                "/_wr/v1/wait?next=%2Fcheckout",
                &[("cookie", cookie)],
            ),
            NOW,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_join_still_in_flight_is_waited_on_not_resent() {
        let state = state(None, false);
        let response = wait_with(&state, &stamped_cookie(SECRET, 30, 0)).await;
        assert_eq!(response.status(), 200);
        assert!(text(&response).contains("http-equiv=\"refresh\" content=\"5\""));
        assert!(response.headers().get("set-cookie").is_none());
        assert!(state.queue.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_lost_join_is_resent_as_the_same_join_and_the_cookie_restamped() {
        let state = state(None, false);
        let response = wait_with(&state, &stamped_cookie(SECRET, 61, 0)).await;
        assert_eq!(response.status(), 200);
        assert!(text(&response).contains("http-equiv=\"refresh\""));

        let sent = state.queue.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let msg: serde_json::Value = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(msg["request_id"], ID);
        assert_eq!(
            msg["h"],
            PossessionSecret::parse(SECRET).unwrap().digest().as_str()
        );
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        assert!(
            cookie.starts_with(&format!(
                "{IDENTITY_COOKIE}={ID}.{SECRET}.{}.1;",
                NOW.as_second()
            )),
            "{cookie}"
        );
    }

    #[tokio::test]
    async fn a_join_lost_after_every_resend_gets_the_form_back() {
        let state = state(None, false);
        let response = wait_with(&state, &stamped_cookie(SECRET, 61, crate::MAX_REJOINS)).await;
        assert_eq!(response.status(), 200);
        let html = text(&response);
        assert!(html.contains(r#"action="/_wr/v1/enter?next=%2Fcheckout""#));
        assert!(!html.contains("http-equiv"));
        assert!(
            response.headers()["set-cookie"]
                .to_str()
                .unwrap()
                .contains("Max-Age=0"),
            "the identity is forgotten so /_wr/v1/enter joins afresh"
        );
        assert!(state.queue.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_resend_the_queue_refused_is_tried_again_on_the_next_load() {
        let state = state(None, true);
        let response = wait_with(&state, &stamped_cookie(SECRET, 61, 0)).await;
        assert_eq!(response.status(), 503);
        assert!(text(&response).contains("http-equiv=\"refresh\""));
        // No restamp: the attempt did not happen, so it is not counted.
        assert!(response.headers().get("set-cookie").is_none());
    }

    #[tokio::test]
    async fn a_registered_visitor_is_never_resent_however_old_the_stamp() {
        let state = state(Some(700), false);
        let response = wait_with(&state, &stamped_cookie(SECRET, 80_000, crate::MAX_REJOINS)).await;
        assert!(text(&response).contains("700"));
        assert!(state.queue.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn entering_joins_once_and_keeps_the_identity_server_side() {
        let state = state(None, false);
        let response = handle(
            &state,
            request(
                "POST",
                "https://x/_wr/v1/enter",
                &[(
                    "referer",
                    "https://x/_wr/waiting.html?r=none&next=%2Fcheckout",
                )],
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 303);
        assert_eq!(
            response.headers()["location"],
            "/_wr/v1/wait?next=%2Fcheckout"
        );
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
            request(
                "POST",
                "/_wr/v1/enter",
                &[("cookie", &identity_cookie(SECRET))],
            ),
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
        let response = handle(&state, request("POST", "/_wr/v1/enter", &[]))
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert!(response.headers().get("set-cookie").is_none());
    }

    #[tokio::test]
    async fn waiting_without_an_identity_offers_the_form() {
        let response = handle(&state(None, false), request("GET", "/_wr/v1/wait", &[]))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let html = text(&response);
        assert!(html.contains(r#"action="/_wr/v1/enter?next=%2F""#));
    }

    #[tokio::test]
    async fn a_waiting_visitor_sees_their_place() {
        let response = handle(
            &state(Some(700), false),
            request(
                "GET",
                "/_wr/v1/wait?next=%2Fcheckout",
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
                "/_wr/v1/wait?next=%2Fcheckout",
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
        let session = Session::verify(credential, &state.key, Timestamp::constant(1, 0)).unwrap();
        assert_eq!(session.request_id, ID);
        assert_eq!(session.ip_tag, state.key.ip_tag("198.51.100.7"));
    }

    #[tokio::test]
    async fn a_cookie_with_the_wrong_secret_is_forgotten() {
        let response = handle(
            &state(Some(3), false),
            request(
                "GET",
                "/_wr/v1/wait",
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
