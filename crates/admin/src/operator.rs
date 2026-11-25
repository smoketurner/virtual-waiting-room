//! Who may act on the console, and the one place a state-changing request is
//! checked before it does (issue #243).
//!
//! [`guard_writes`] is mounted on the whole router and decides by method, not
//! by route, so a route added later is covered without doing anything: every
//! request that is not a safe method must carry a live session still on the
//! allowlist and that session's CSRF token as a `csrf` form field. A handler
//! that changes state takes [`Operator`], which only the guard can supply, so
//! it cannot run on a request the guard did not pass.
//!
//! The token is a form field rather than a header so that every operator
//! action keeps working with JavaScript disabled.

use std::future::Future;
use std::sync::Arc;

use axum::Form;
use axum::body::{Body, Bytes};
use axum::extract::{FromRequest as _, FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse as _, Redirect, Response};

use crate::arrival::ArrivalTime;
use crate::sessions::{AdminSession, SessionStore};

/// The opaque session cookie. `__Host-` makes the browser refuse it unless it
/// is `Secure`, `Path=/` and has no `Domain`, so a sibling subdomain cannot
/// plant or overwrite an operator's session (issue #243).
pub const SESSION_COOKIE: &str = "__Host-vwr_admin_session";
/// The short-lived cookie binding a login to the browser that started it:
/// holds the OIDC state, checked against the callback's `state` param.
pub const STATE_COOKIE: &str = "__Host-vwr_admin_login_state";

/// The session cookie's lifetime, matching the session row's (8 h).
const SESSION_MAX_AGE_SECS: u32 = 8 * 60 * 60;
/// The login-state cookie's lifetime, matching the pending login's (10 min).
const STATE_MAX_AGE_SECS: u32 = 600;

/// The largest form the guard will buffer to read the token from. The rules
/// form is the biggest the console posts and is bounded far below this by the
/// `KeyValueStore` value ceiling.
const MAX_FORM_BYTES: usize = 64 * 1024;

/// `Set-Cookie` for a new session. `SameSite=Strict`: no request another site
/// starts carries it, a top-level navigation included.
#[must_use]
pub fn session_cookie(session_id: &str) -> String {
    format!(
        "{SESSION_COOKIE}={session_id}; Path=/; HttpOnly; Secure; SameSite=Strict; \
         Max-Age={SESSION_MAX_AGE_SECS}"
    )
}

/// `Set-Cookie` removing the session cookie.
#[must_use]
pub fn cleared_session_cookie() -> String {
    format!("{SESSION_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=0")
}

/// `Set-Cookie` for the login-state cookie. `SameSite=Lax`, not `Strict`: the
/// callback is a top-level navigation the identity provider starts, and a
/// `Strict` cookie would not be sent on it, so no login could complete.
#[must_use]
pub fn state_cookie(state: &str) -> String {
    format!(
        "{STATE_COOKIE}={state}; Path=/; HttpOnly; Secure; SameSite=Lax; \
         Max-Age={STATE_MAX_AGE_SECS}"
    )
}

/// `Set-Cookie` removing the login-state cookie once it has been consumed.
#[must_use]
pub fn cleared_state_cookie() -> String {
    format!("{STATE_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0")
}

/// What the callback answers once the session cookie is set: a page that
/// navigates to the dashboard itself, not a redirect.
///
/// The callback is reached by a navigation the identity provider started, and
/// a redirect from it is still that cross-site navigation, so the browser
/// would withhold the `SameSite=Strict` cookie this very response sets. The
/// dashboard would then see no session and start another login, forever. A
/// navigation this page starts is same-site and carries the cookie. A meta
/// refresh rather than a script, so it works with JavaScript disabled and
/// needs nothing from the CSP.
pub const SIGNED_IN_PAGE: &str = "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n\
    <meta charset=\"utf-8\">\n\
    <meta http-equiv=\"refresh\" content=\"0;url=/admin\">\n\
    <title>Signed in</title>\n</head>\n<body>\n\
    <p>Signed in. <a href=\"/admin\">Continue to the console</a>.</p>\n\
    </body>\n</html>\n";

/// Reads a named cookie's value from the request cookie header, if present.
#[must_use]
pub fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie::Cookie::split_parse(raw)
        .filter_map(Result::ok)
        .find(|c| c.name() == name)
        .map(|c| c.value().to_string())
}

/// The emails permitted to hold an admin session. Empty = deny all (fail
/// closed for a control plane).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Allowlist(Vec<String>);

impl Allowlist {
    /// Parses the comma-separated `OIDC_ALLOWED_EMAILS` value; entries are
    /// trimmed and lowercased, empty ones dropped.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        Self(
            raw.split(',')
                .map(|entry| entry.trim().to_ascii_lowercase())
                .filter(|entry| !entry.is_empty())
                .collect(),
        )
    }

    /// Whether `email` is on the list, compared case-insensitively.
    #[must_use]
    pub fn permits(&self, email: &str) -> bool {
        let email = email.to_ascii_lowercase();
        self.0.contains(&email)
    }
}

/// Why a completed OIDC login was not given a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoginRefusal {
    /// The ID token carries no `email` claim.
    NoEmail,
    /// `email_verified` is false or absent. An address the provider has not
    /// verified is one anybody can type into a sign-up form, so matching it
    /// against the allowlist would prove nothing (issue #243).
    EmailNotVerified,
    /// The verified address is not on the allowlist.
    NotAllowed,
}

impl LoginRefusal {
    /// The reason as it appears in the `admin_login_refused` log line.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::NoEmail => "no_email_claim",
            Self::EmailNotVerified => "email_not_verified",
            Self::NotAllowed => "email_not_in_allowlist",
        }
    }
}

/// Decides whether the identity an ID token asserts may hold a session, and
/// returns the email to record on it.
///
/// # Errors
/// A [`LoginRefusal`] naming the first check that failed.
pub fn admit_login(
    allowlist: &Allowlist,
    email: Option<&str>,
    email_verified: Option<bool>,
) -> Result<String, LoginRefusal> {
    let email = email
        .filter(|address| !address.is_empty())
        .ok_or(LoginRefusal::NoEmail)?;
    if email_verified != Some(true) {
        return Err(LoginRefusal::EmailNotVerified);
    }
    if !allowlist.permits(email) {
        return Err(LoginRefusal::NotAllowed);
    }
    Ok(email.to_owned())
}

/// Where sessions are read from: the port the guard is tested through.
pub trait SessionLookup {
    /// The live session with this id, or `None` if there is none, it has
    /// expired, or it could not be read.
    fn load(
        &self,
        session_id: &str,
        now: ArrivalTime,
    ) -> impl Future<Output = Option<AdminSession>> + Send;
}

impl SessionLookup for SessionStore {
    async fn load(&self, session_id: &str, now: ArrivalTime) -> Option<AdminSession> {
        match self.load_session(session_id, now).await {
            Ok(session) => session,
            Err(e) => {
                // Unreadable is unauthenticated: the console fails closed.
                tracing::error!(error = %e, "could not read the admin session");
                None
            }
        }
    }
}

/// The session store and the allowlist every request is checked against.
pub struct Gate<S> {
    sessions: S,
    allowlist: Allowlist,
}

impl<S: SessionLookup + Sync> Gate<S> {
    pub fn new(sessions: S, allowlist: Allowlist) -> Self {
        Self {
            sessions,
            allowlist,
        }
    }

    /// The session store, for the login flow that writes to it.
    pub fn sessions(&self) -> &S {
        &self.sessions
    }

    /// The allowlist, for the login flow's own check.
    pub fn allowlist(&self) -> &Allowlist {
        &self.allowlist
    }

    /// The request's session, or `None` if it is unauthenticated.
    ///
    /// The allowlist is consulted on every request, not only at login: a
    /// session lasts eight hours, and removing an operator from the list has
    /// to end their access when the list changes rather than when the session
    /// happens to expire (issue #243).
    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
        now: ArrivalTime,
    ) -> Option<AdminSession> {
        let id = cookie_value(headers, SESSION_COOKIE)?;
        let session = self.sessions.load(&id, now).await?;
        if !self.allowlist.permits(&session.email) {
            tracing::warn!(
                event = "admin_session_refused",
                subject = %session.subject,
                "session belongs to an operator no longer on the allowlist"
            );
            return None;
        }
        Some(session)
    }
}

/// The operator a state-changing request was made by.
///
/// Only [`guard_writes`] puts one on a request, after the session and the CSRF
/// token have both been checked, so taking it as an argument is what makes a
/// handler unreachable without those checks.
#[derive(Debug, Clone)]
pub struct Operator(pub AdminSession);

impl<S: Send + Sync> FromRequestParts<S> for Operator {
    type Rejection = StatusCode;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(parts.extensions.get::<Self>().cloned().ok_or_else(|| {
            tracing::error!(
                "guard_writes did not pass this request; an operator action has \
                 no verified operator to run as"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        }))
    }
}

/// Methods that never change state here, and so need no token.
#[must_use]
pub fn is_safe(method: &Method) -> bool {
    matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// Whether the submitted token is the session's, compared in constant time.
#[must_use]
pub fn tokens_match(expected: &str, presented: &str) -> bool {
    !expected.is_empty()
        && aws_lc_rs::constant_time::verify_slices_are_equal(
            expected.as_bytes(),
            presented.as_bytes(),
        )
        .is_ok()
}

#[derive(serde::Deserialize)]
struct CsrfField {
    csrf: String,
}

/// The `csrf` field of a urlencoded form body, if it is one and has one.
///
/// Parsed by the same extractor the handlers use, on a copy of the body, so
/// the guard and the handler cannot disagree about what the form says.
async fn csrf_field(content_type: Option<&HeaderValue>, body: &Bytes) -> Option<String> {
    let mut probe = Request::new(Body::from(body.clone()));
    *probe.method_mut() = Method::POST;
    probe
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type?.clone());
    Form::<CsrfField>::from_request(probe, &())
        .await
        .ok()
        .map(|Form(field)| field.csrf)
}

/// Lets a state-changing request through only with a live, still-allowed
/// session and that session's CSRF token (issue #243).
///
/// No session answers as the handlers always have, with a redirect to the
/// login. A missing or wrong token is a 403. The body is buffered to read the
/// token and handed on whole, so the handler's own `Form` extractor still
/// sees it.
pub async fn guard_writes<S: SessionLookup + Send + Sync + 'static>(
    State(gate): State<Arc<Gate<S>>>,
    request: Request,
    next: Next,
) -> Response {
    if is_safe(request.method()) {
        return next.run(request).await;
    }
    let Some(now) = request.extensions().get::<ArrivalTime>().copied() else {
        tracing::error!("arrival_layer is not mounted outside guard_writes");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Some(session) = gate.authenticate(request.headers(), now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    let (parts, body) = request.into_parts();
    let Ok(body) = axum::body::to_bytes(body, MAX_FORM_BYTES).await else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let presented = csrf_field(parts.headers.get(header::CONTENT_TYPE), &body).await;
    if !presented.is_some_and(|token| tokens_match(&session.csrf, &token)) {
        tracing::warn!(
            event = "admin_csrf_refused",
            path = parts.uri.path(),
            subject = %session.subject,
            "refused a state-changing admin request without the session's CSRF token"
        );
        return (
            StatusCode::FORBIDDEN,
            "Missing or stale form token. Reload the console and retry.",
        )
            .into_response();
    }
    let mut request = Request::from_parts(parts, Body::from(body));
    request.extensions_mut().insert(Operator(session));
    next.run(request).await
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use axum::Router;
    use axum::routing::{get, post};
    use tower::ServiceExt as _;

    use super::*;
    use crate::arrival::arrival_layer;

    const SESSION_ID: &str = "sid";
    const TOKEN: &str = "the-session-token";

    /// One session, `sid`, belonging to `op@example.com`.
    struct OneSession;

    impl SessionLookup for OneSession {
        fn load(
            &self,
            session_id: &str,
            _now: ArrivalTime,
        ) -> impl Future<Output = Option<AdminSession>> + Send {
            std::future::ready((session_id == SESSION_ID).then(|| AdminSession {
                subject: "sub".to_owned(),
                email: "Op@Example.com".to_owned(),
                csrf: TOKEN.to_owned(),
            }))
        }
    }

    #[derive(serde::Deserialize)]
    struct RateForm {
        rate: String,
    }

    /// A router shaped like the console's: actions that take [`Operator`], one
    /// that also reads its own form, and a safe read.
    fn app(allowed: &str) -> Router {
        let gate = Arc::new(Gate::new(OneSession, Allowlist::parse(allowed)));
        Router::new()
            .route("/admin", get(|| async { "dashboard" }))
            .route(
                "/admin/pause",
                post(|Operator(session): Operator| async move {
                    format!("paused by {}", session.email)
                }),
            )
            .route(
                "/admin/rate",
                post(|_: Operator, Form(form): Form<RateForm>| async move {
                    format!("rate {}", form.rate)
                }),
            )
            .layer(axum::middleware::from_fn_with_state(gate, guard_writes))
            .layer(axum::middleware::from_fn(arrival_layer))
    }

    fn post_form(path: &str, cookie: Option<&str>, body: &'static str) -> Request {
        let mut request = Request::new(Body::from(body));
        *request.method_mut() = Method::POST;
        *request.uri_mut() = path.parse().unwrap();
        request.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        if let Some(id) = cookie {
            request.headers_mut().insert(
                header::COOKIE,
                format!("{SESSION_COOKIE}={id}").parse().unwrap(),
            );
        }
        request
    }

    async fn text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn a_post_with_the_sessions_token_reaches_the_action() {
        let response = app("op@example.com")
            .oneshot(post_form(
                "/admin/pause",
                Some(SESSION_ID),
                "csrf=the-session-token",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(text(response).await, "paused by Op@Example.com");
    }

    #[tokio::test]
    async fn the_handler_still_reads_its_own_form_after_the_guard_read_it() {
        let response = app("op@example.com")
            .oneshot(post_form(
                "/admin/rate",
                Some(SESSION_ID),
                "rate=25&csrf=the-session-token",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(text(response).await, "rate 25");
    }

    #[tokio::test]
    async fn a_post_without_the_token_is_forbidden() {
        for body in [
            "",
            "rate=25",
            "csrf=",
            "csrf=the-session-toke",
            "csrf=other",
        ] {
            let response = app("op@example.com")
                .oneshot(post_form("/admin/rate", Some(SESSION_ID), body))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "body {body:?}");
        }
    }

    #[tokio::test]
    async fn a_token_outside_a_form_body_does_not_count() {
        // A header or a query string is something a cross-origin script can
        // sometimes set; the field of a same-origin form is not.
        let mut request = post_form("/admin/pause?csrf=the-session-token", Some(SESSION_ID), "");
        request
            .headers_mut()
            .insert("x-csrf-token", HeaderValue::from_static(TOKEN));
        let response = app("op@example.com").oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let mut json = post_form(
            "/admin/pause",
            Some(SESSION_ID),
            r#"{"csrf":"the-session-token"}"#,
        );
        json.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let response = app("op@example.com").oneshot(json).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn every_state_changing_method_and_path_is_guarded() {
        // The guard decides by method, so a route nobody has written yet is
        // covered: without a token the request never reaches routing.
        for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
            for path in ["/admin/pause", "/admin/rate", "/admin/not_written_yet"] {
                let mut request = post_form(path, Some(SESSION_ID), "rate=1");
                *request.method_mut() = method.clone();
                let response = app("op@example.com").oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {path}");
            }
        }
    }

    #[tokio::test]
    async fn a_post_without_a_session_goes_to_the_login() {
        for cookie in [None, Some("unknown")] {
            let response = app("op@example.com")
                .oneshot(post_form("/admin/pause", cookie, "csrf=the-session-token"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SEE_OTHER);
            assert_eq!(response.headers()[header::LOCATION], "/admin/login");
        }
    }

    #[tokio::test]
    async fn a_session_whose_operator_left_the_allowlist_is_unauthenticated() {
        let response = app("someone-else@example.com")
            .oneshot(post_form(
                "/admin/pause",
                Some(SESSION_ID),
                "csrf=the-session-token",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);

        let gate = Gate::new(OneSession, Allowlist::parse(""));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            format!("{SESSION_COOKIE}={SESSION_ID}").parse().unwrap(),
        );
        let now = ArrivalTime::for_test_millis(1_700_000_000_000);
        assert_eq!(gate.authenticate(&headers, now).await, None);
        let gate = Gate::new(OneSession, Allowlist::parse("op@example.com"));
        assert!(gate.authenticate(&headers, now).await.is_some());
    }

    #[tokio::test]
    async fn a_safe_method_needs_no_token() {
        let mut request = Request::new(Body::empty());
        *request.uri_mut() = "/admin".parse().unwrap();
        let response = app("op@example.com").oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn an_action_mounted_without_the_guard_cannot_run() {
        // The other half of the guarantee: a handler that takes `Operator` on
        // a router the guard is not on answers 500 instead of acting.
        let unguarded = Router::new()
            .route("/admin/pause", post(|_: Operator| async { "paused" }))
            .layer(axum::middleware::from_fn(arrival_layer));
        let response = unguarded
            .oneshot(post_form(
                "/admin/pause",
                Some(SESSION_ID),
                "csrf=the-session-token",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn an_oversized_form_is_refused_before_it_is_parsed() {
        let body: &'static str = Box::leak("a".repeat(MAX_FORM_BYTES + 1).into_boxed_str());
        let response = app("op@example.com")
            .oneshot(post_form("/admin/rate", Some(SESSION_ID), body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn tokens_match_only_when_equal_and_non_empty() {
        assert!(tokens_match("abc", "abc"));
        assert!(!tokens_match("abc", "abd"));
        assert!(!tokens_match("abc", "ab"));
        assert!(!tokens_match("", ""));
    }

    #[test]
    fn cookies_are_host_prefixed_and_scoped_as_the_prefix_requires() {
        // `__Host-` is honoured only with Secure, Path=/ and no Domain.
        for cookie in [
            session_cookie("id"),
            cleared_session_cookie(),
            state_cookie("st"),
            cleared_state_cookie(),
        ] {
            assert!(cookie.starts_with("__Host-"), "{cookie}");
            assert!(cookie.contains("; Path=/;"), "{cookie}");
            assert!(cookie.contains("; Secure;"), "{cookie}");
            assert!(cookie.contains("; HttpOnly;"), "{cookie}");
            assert!(!cookie.to_ascii_lowercase().contains("domain"), "{cookie}");
        }
    }

    #[test]
    fn the_session_cookie_is_strict_and_the_state_cookie_is_lax() {
        // Lax on the state cookie is load-bearing: the provider's redirect to
        // the callback is a cross-site navigation, and Strict would drop it.
        assert!(session_cookie("id").contains("SameSite=Strict"));
        assert!(cleared_session_cookie().contains("SameSite=Strict"));
        assert!(state_cookie("st").contains("SameSite=Lax"));
        assert!(cleared_state_cookie().contains("SameSite=Lax"));
    }

    #[test]
    fn the_signed_in_page_navigates_itself_rather_than_redirecting() {
        assert!(SIGNED_IN_PAGE.contains(r#"<meta http-equiv="refresh" content="0;url=/admin">"#));
        assert!(SIGNED_IN_PAGE.contains(r#"<a href="/admin">"#));
        assert!(!SIGNED_IN_PAGE.contains("<script"));
    }

    #[test]
    fn the_allowlist_is_case_insensitive_and_empty_denies_all() {
        let list = Allowlist::parse(" Op@Example.com , ,second@example.com");
        assert!(list.permits("op@example.com"));
        assert!(list.permits("SECOND@example.com"));
        assert!(!list.permits("third@example.com"));
        assert!(!Allowlist::parse("").permits(""));
        assert!(!Allowlist::parse("").permits("op@example.com"));
    }

    #[test]
    fn a_login_needs_a_verified_allowlisted_email() {
        let list = Allowlist::parse("op@example.com");
        assert_eq!(
            admit_login(&list, Some("Op@example.com"), Some(true)),
            Ok("Op@example.com".to_owned())
        );
        assert_eq!(
            admit_login(&list, Some("op@example.com"), Some(false)),
            Err(LoginRefusal::EmailNotVerified)
        );
        assert_eq!(
            admit_login(&list, Some("op@example.com"), None),
            Err(LoginRefusal::EmailNotVerified)
        );
        assert_eq!(
            admit_login(&list, None, Some(true)),
            Err(LoginRefusal::NoEmail)
        );
        assert_eq!(
            admit_login(&list, Some(""), Some(true)),
            Err(LoginRefusal::NoEmail)
        );
        assert_eq!(
            admit_login(&list, Some("other@example.com"), Some(true)),
            Err(LoginRefusal::NotAllowed)
        );
    }
}
