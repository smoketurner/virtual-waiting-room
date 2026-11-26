//! The API Gateway handler: a proxy request in, a proxy response out. The
//! runtime in `main.rs` calls it with the live store and clock, and the local
//! simulation with its own.

use lambda_http::{Body, Error, Request, RequestExt, Response};
use tracing::info;
use wr_common::http::{epoch_seconds, received_at};
use wr_common::{PossessionSecret, SigningKey};

use crate::{Admission, Denied, Minting, Store, admit, session_set_cookie, viewer_ip};

/// Resolved once at cold start and shared across invocations. Parameterized
/// over [`Store`] so the handler's branching — what it loads, and in what
/// order — is exercisable against a mock without the AWS SDK.
pub struct AppState<S: Store> {
    pub store: S,
    pub key: SigningKey,
    pub event_id: String,
    pub session_cookie_name: String,
    pub session_ttl_secs: u64,
}

/// Answers one `POST /_wr/v1/generate_token`, at the time API Gateway
/// received it.
///
/// # Errors
///
/// The store failed, or a response could not be built.
pub async fn handle<S: Store>(state: &AppState<S>, req: Request) -> Result<Response<Body>, Error> {
    let now = received_at(&req)
        .and_then(epoch_seconds)
        .ok_or("request carried no API Gateway request time")?;
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
        Denied::StillQueued { .. } | Denied::AtCapacity { .. } => 425,
        Denied::NotAdmitting | Denied::NotOpen => 409,
        Denied::NotRegistered => 404,
        Denied::NotHolder => 403,
        // Gone: the place was used and its visit is over. The page takes a
        // new one rather than asking again.
        Denied::SessionExpired => 410,
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

    use crate::StoreError;
    use wr_common::{Counters, Phase, PositionStatus, PreQueueItem, SHARDS, Session, Shard};

    use super::*;

    /// The request as API Gateway delivers it: stamped with when it arrived.
    fn received(req: Request) -> Request {
        wr_common::http::received(req, jiff::Timestamp::now())
    }

    #[tokio::test]
    async fn a_request_without_api_gateways_time_is_refused() {
        let state = state(FakeStore::admitting());
        assert!(handle(&state, request()).await.is_err());
        assert_eq!(state.store.arrivals(), 0);
    }

    /// An in-memory store that counts what the handler did, so a test can tell
    /// "admitted and counted" from "admitted without counting".
    struct FakeStore {
        counters: Counters,
        position: Mutex<Option<crate::PositionRow>>,
        prequeue: Option<PreQueueItem>,
        /// Units of admission budget left to take.
        budget: Mutex<u32>,
        /// How many units were taken — the number the controller reads.
        arrivals: Mutex<u32>,
        /// When set, marking the row admitted fails as a store error.
        mark_fails: bool,
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
                    origin_concurrency: Some(10),
                    message: None,
                    stored_control: wr_common::StoredControl::Open,
                    fail_open_until: 0,
                    starts_at: None,
                },
                position: Mutex::new(Some(row(3))),
                prequeue: None,
                budget: Mutex::new(100),
                arrivals: Mutex::new(0),
                mark_fails: false,
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
        ) -> impl std::future::Future<Output = Result<Option<crate::PositionRow>, StoreError>> + Send
        {
            std::future::ready(Ok(self.position.lock().unwrap().clone()))
        }

        fn take_slot(
            &self,
            _event_id: &str,
            _shard: Shard,
        ) -> impl std::future::Future<Output = Result<bool, StoreError>> + Send {
            let mut budget = self.budget.lock().unwrap();
            let took = *budget > 0;
            if took {
                *budget -= 1;
                let mut arrivals = self.arrivals.lock().unwrap();
                *arrivals = arrivals.saturating_add(1);
            }
            std::future::ready(Ok(took))
        }

        fn mark_admitted(
            &self,
            _request_id: &str,
            _position: u64,
            _digest: &wr_common::SecretDigest,
            now: u64,
        ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
            let result = if self.mark_fails {
                Err(StoreError("mark unavailable".to_owned()))
            } else {
                // Mirrors the conditional write: the row is admitted from here.
                if let Some(row) = self.position.lock().unwrap().as_mut() {
                    row.status = PositionStatus::Admitted;
                    row.admitted_at = Some(now);
                }
                Ok(())
            };
            std::future::ready(result)
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

    fn row(position: u64) -> crate::PositionRow {
        crate::PositionRow {
            position,
            status: PositionStatus::Issued,
            digest: Some(PossessionSecret::parse(SECRET).unwrap().digest()),
            admitted_at: None,
        }
    }

    #[tokio::test]
    async fn a_reloaded_page_is_admitted_again_but_counted_once() {
        // request_id travels in a URL and the waiting page polls, so the second
        // call is the normal case, not the exceptional one. Every call must
        // hand out a working session; only the first may take a unit of the
        // admission budget.
        let state = state(FakeStore::admitting());

        for _ in 0..3u8 {
            let response = handle(&state, received(request())).await.unwrap();
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
    async fn a_spent_budget_admits_nobody_and_says_to_keep_polling() {
        // ADR-0039: the visitor's turn has come, but the origin has had its
        // share for this interval. No cookie, no mark, and a 425 so the page
        // asks again rather than giving up.
        let store = FakeStore {
            budget: Mutex::new(0),
            ..FakeStore::admitting()
        };
        let state = state(store);

        let response = handle(&state, received(request())).await.unwrap();

        assert_eq!(response.status(), 425);
        assert!(!response.headers().contains_key("set-cookie"));
        assert_eq!(state.store.arrivals(), 0);
        assert_eq!(
            state
                .store
                .position
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .status,
            PositionStatus::Issued
        );
    }

    #[tokio::test]
    async fn the_budget_bounds_how_many_distinct_visitors_are_admitted() {
        // Two units, three visitors whose turn has come: the third waits.
        let state = state(FakeStore {
            budget: Mutex::new(2),
            ..FakeStore::admitting()
        });
        let mut statuses = Vec::new();
        for _ in 0..3u8 {
            // A different visitor each time: the row is unmarked again.
            *state.store.position.lock().unwrap() = Some(row(3));
            statuses.push(
                handle(&state, received(request()))
                    .await
                    .unwrap()
                    .status()
                    .as_u16(),
            );
        }
        assert_eq!(statuses, [200, 200, 425]);
    }

    #[tokio::test]
    async fn an_admitted_visitor_gets_their_session_again_on_a_spent_budget() {
        // They were charged for when they were first admitted; a reload must
        // not strand them because the interval's budget has since run out.
        let state = state(FakeStore {
            budget: Mutex::new(1),
            ..FakeStore::admitting()
        });
        assert_eq!(
            handle(&state, received(request())).await.unwrap().status(),
            200
        );
        assert_eq!(*state.store.budget.lock().unwrap(), 0);
        let again = handle(&state, received(request())).await.unwrap();
        assert_eq!(again.status(), 200);
        assert!(again.headers().contains_key("set-cookie"));
    }

    #[tokio::test]
    async fn a_session_asked_for_again_expires_when_the_first_one_did() {
        // Issue #237: a place that reached the front can be handed around. If
        // every call minted a full-length session, one admission would admit
        // anyone for as long as the row lived.
        let state = state(FakeStore::admitting());
        let first = handle(&state, received(request())).await.unwrap();
        let admitted_at = state
            .store
            .position
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .admitted_at
            .unwrap();
        // Half the lifetime has gone by.
        state
            .store
            .position
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .admitted_at = Some(admitted_at - 1800);
        let second = handle(&state, received(request())).await.unwrap();

        let expiry = |response: &Response<Body>| {
            let body: serde_json::Value = serde_json::from_slice(response.body().as_ref()).unwrap();
            body["expires_at"].as_u64().unwrap()
        };
        assert_eq!(expiry(&first), admitted_at + 3600);
        assert_eq!(expiry(&second), admitted_at - 1800 + 3600);
    }

    #[tokio::test]
    async fn a_place_whose_session_has_run_out_mints_nothing() {
        let state = state(FakeStore::admitting());
        {
            let mut position = state.store.position.lock().unwrap();
            let row = position.as_mut().unwrap();
            row.status = PositionStatus::Admitted;
            row.admitted_at = Some(1_000);
        }
        let response = handle(&state, received(request())).await.unwrap();
        assert_eq!(response.status(), 410);
        assert!(!response.headers().contains_key("set-cookie"));
        assert_eq!(state.store.arrivals(), 0);
    }

    #[tokio::test]
    async fn a_failed_mark_still_admits_the_visitor_who_took_a_unit() {
        let store = FakeStore {
            mark_fails: true,
            ..FakeStore::admitting()
        };
        let state = state(store);

        let response = handle(&state, received(request())).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(state.store.arrivals(), 1);
    }

    #[tokio::test]
    async fn a_visitor_whose_turn_has_not_come_takes_nothing() {
        // The budget is touched only after `decide` admits, so a visitor still
        // in the queue takes no unit and leaves no mark -- otherwise polling
        // would spend the budget on everyone waiting.
        let store = FakeStore::admitting();
        *store.position.lock().unwrap() = Some(row(70));
        let state = state(store);

        let response = handle(&state, received(request())).await.unwrap();

        assert_eq!(response.status(), 425);
        assert_eq!(state.store.arrivals(), 0);
        assert_eq!(
            state
                .store
                .position
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .status,
            PositionStatus::Issued
        );
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
            let response = handle(&state, received(Request::new(Body::from(body.clone()))))
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
        assert_eq!(state.store.arrivals(), 0);
    }

    #[tokio::test]
    async fn a_secret_in_the_query_string_is_not_accepted() {
        // Accepting it there would put the credential back in access logs.
        let request =
            Request::new(Body::from(r#"{"request_id":"r1"}"#)).with_query_string_parameters(
                std::collections::HashMap::from([("secret".to_owned(), SECRET.to_owned())]),
            );
        let response = handle(&state(FakeStore::admitting()), received(request))
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
        let response = handle(&state, received(req)).await.unwrap();
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        let credential = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        let session = Session::verify(credential, &state.key, 1).unwrap();
        assert_eq!(session.ip_tag, state.key.ip_tag("198.51.100.7"));

        // No header (a request that bypassed CloudFront): minted untagged.
        let response = handle(&state, received(request())).await.unwrap();
        let cookie = response.headers()["set-cookie"].to_str().unwrap();
        let credential = cookie.split(';').next().unwrap().split_once('=').unwrap().1;
        assert_eq!(
            Session::verify(credential, &state.key, 1).unwrap().ip_tag,
            None
        );
    }
}
