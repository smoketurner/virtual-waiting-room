//! Origin-authorizer decision logic.
//!
//! Every authorizer decision is local: the request carries its own credentials
//! (a session cookie, or an admission token on the URL), and the authorizer
//! validates them against the per-deployment signing key without a backend call
//! on the hot path. The one write it makes is recording an arrival when it
//! converts a token into a session.
//!
//! This module is AWS-free. [`decide`] is a pure function from the parsed
//! request and the current state to a [`Decision`]; the handler in `main.rs`
//! parses the event, calls it, and carries out the resulting effect.

pub mod dynamo;
pub mod token;

use wr_common::{Session, SigningKey, VerifyError};

pub use token::{TokenError, generate_token};

/// How a session's lifetime is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMode {
    /// The session expires a fixed duration after issue, regardless of activity.
    Fixed { ttl_secs: u64 },
    /// The session expires after a period of inactivity, re-issued with a later
    /// expiry on each request, up to a hard cap from first issue.
    Sliding { idle_secs: u64, cap_secs: u64 },
}

/// Whether the authorizer admits or blocks when the waiting room is
/// unreachable. Fail-open is the default; a client may fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnreachablePolicy {
    /// Admit with a time-limited bypass cookie while the client retries.
    #[default]
    FailOpen,
    /// Block: send the visitor to the waiting room even though it is down.
    FailClosed,
}

/// One local protection rule: a request matches when the named request
/// attribute contains the configured substring. A path that no rule matches is
/// unprotected and forwarded without a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtectionRule {
    /// The request path starts with this prefix.
    PathPrefix(String),
    /// A request header equals this `(name, value)` pair (case-insensitive name).
    Header { name: String, value: String },
    /// A cookie of this name is present.
    Cookie(String),
    /// The user agent contains this substring.
    UserAgent(String),
}

impl ProtectionRule {
    /// Whether this rule matches the request.
    #[must_use]
    pub fn matches(&self, req: &Request) -> bool {
        match self {
            Self::PathPrefix(prefix) => req.path.starts_with(prefix.as_str()),
            Self::Header { name, value } => req
                .header(name)
                .is_some_and(|actual| actual.eq_ignore_ascii_case(value)),
            Self::Cookie(name) => req.cookie(name).is_some(),
            Self::UserAgent(needle) => req
                .header("user-agent")
                .is_some_and(|ua| ua.contains(needle.as_str())),
        }
    }
}

/// The parsed request the authorizer decides over. Header names are compared
/// case-insensitively; the handler lowercases them at construction.
#[derive(Debug, Clone, Default)]
pub struct Request {
    /// The request path (no query string).
    pub path: String,
    /// Lowercased header name to value.
    pub headers: Vec<(String, String)>,
    /// Parsed cookies as `(name, value)`.
    pub cookies: Vec<(String, String)>,
    /// The admission token from the URL query, if present.
    pub url_token: Option<String>,
    /// The client request id (`UUIDv7`), used to shard the arrival counter.
    pub request_id: Option<String>,
}

impl Request {
    /// The value of a header by case-insensitive name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The value of a cookie by name.
    #[must_use]
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Whether any rule protects this request.
    #[must_use]
    pub fn is_protected(&self, rules: &[ProtectionRule]) -> bool {
        rules.iter().any(|r| r.matches(self))
    }
}

/// The authorizer's configuration, resolved once at cold start.
pub struct Config {
    pub event_id: String,
    pub session_cookie_name: String,
    pub bypass_cookie_name: String,
    pub session_mode: SessionMode,
    pub unreachable_policy: UnreachablePolicy,
    /// Seconds a fail-open bypass cookie is honored.
    pub bypass_ttl_secs: u64,
    /// Where to send an un-admitted visitor.
    pub waiting_room_url: String,
    pub rules: Vec<ProtectionRule>,
}

/// What the authorizer decided to do with the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The request already carries a valid session, or is unprotected. Forward
    /// as-is.
    Forward,
    /// A valid admission token was converted to a session. Set this signed
    /// session cookie, record an arrival for this shard, strip the token from
    /// the URL, and forward.
    SetSessionAndForward {
        set_cookie: String,
        arrival_shard: usize,
        /// The path with the admission token removed, for the forwarded request.
        stripped_path: String,
    },
    /// The waiting room is unreachable and the policy is fail-open. Forward with
    /// this time-limited bypass cookie.
    FailOpenBypass { set_cookie: String },
    /// No valid credential and the path is protected. Redirect to the waiting
    /// room.
    Redirect { location: String },
}

/// Whether the waiting-room backend is reachable, as observed by the handler
/// before calling [`decide`]. `Unreachable` triggers the fail-open branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachability {
    Reachable,
    Unreachable,
}

/// The core decision tree. Pure: no I/O, no clock — `now` is
/// passed in, and the effect (writing the arrival, setting cookies) is carried
/// out by the caller from the returned [`Decision`].
///
/// Order matters: a valid session short-circuits before any token work, and the
/// unreachable/fail-open branch is only reached when there is no valid
/// credential, so a healthy visitor is never gratuitously bypassed.
#[must_use]
pub fn decide(
    req: &Request,
    cfg: &Config,
    key: &SigningKey,
    now: u64,
    reachability: Reachability,
) -> Decision {
    // 1. A valid, unexpired, correctly-scoped session cookie forwards straight
    //    through with no further work.
    if let Some(cookie) = req.cookie(&cfg.session_cookie_name)
        && let Ok(session) = Session::verify(cookie, key, now)
        && session.event_id == cfg.event_id
    {
        return Decision::Forward;
    }

    // 2. A valid admission token becomes a session: mint the cookie, mark the
    //    arrival, strip the token from the URL.
    if let Some(token) = &req.url_token
        && let Ok(admitted) = wr_common::AdmissionToken::verify(token, key, now)
        && admitted.event_id == cfg.event_id
    {
        let session = mint_session(&admitted.request_id, cfg, now);
        let set_cookie = session_cookie(&session.sign(key), cfg, now);
        let arrival_shard = wr_common::shard_for(admitted.request_id.as_bytes());
        let stripped_path = strip_token(&req.path);
        return Decision::SetSessionAndForward {
            set_cookie,
            arrival_shard,
            stripped_path,
        };
    }

    // 3. A request no rule protects is forwarded without a credential.
    if !req.is_protected(&cfg.rules) {
        return Decision::Forward;
    }

    // 4. The path is protected and the visitor has no valid credential. If the
    //    waiting room is down, fail open (or closed, per policy); otherwise send
    //    the visitor to wait.
    match (reachability, cfg.unreachable_policy) {
        (Reachability::Unreachable, UnreachablePolicy::FailOpen) => Decision::FailOpenBypass {
            set_cookie: bypass_cookie(cfg, now),
        },
        (Reachability::Unreachable, UnreachablePolicy::FailClosed)
        | (Reachability::Reachable, _) => Decision::Redirect {
            location: cfg.waiting_room_url.clone(),
        },
    }
}

/// Builds the session for a freshly-admitted visitor under the configured mode.
fn mint_session(request_id: &str, cfg: &Config, now: u64) -> Session {
    let expires_at = match cfg.session_mode {
        SessionMode::Fixed { ttl_secs } => now.saturating_add(ttl_secs),
        // First issue of a sliding session: expire after the idle window, but
        // never past the hard cap from now.
        SessionMode::Sliding {
            idle_secs,
            cap_secs,
        } => {
            let cap = now.saturating_add(cap_secs);
            now.saturating_add(idle_secs).min(cap)
        }
    };
    Session {
        event_id: cfg.event_id.clone(),
        request_id: request_id.to_owned(),
        issued_at: now,
        expires_at,
    }
}

/// Re-issues a sliding session on activity: extends the idle window but never
/// past the hard cap measured from the original `issued_at`. A fixed session is
/// returned unchanged (its expiry does not move). Returns `None` when nothing
/// needs updating, so the caller can skip re-setting the cookie.
#[must_use]
pub fn slide(session: &Session, mode: SessionMode, now: u64) -> Option<Session> {
    match mode {
        SessionMode::Fixed { .. } => None,
        SessionMode::Sliding {
            idle_secs,
            cap_secs,
        } => {
            let cap = session.issued_at.saturating_add(cap_secs);
            let extended = now.saturating_add(idle_secs).min(cap);
            if extended > session.expires_at {
                Some(Session {
                    expires_at: extended,
                    ..session.clone()
                })
            } else {
                None
            }
        }
    }
}

/// A `Set-Cookie` header value for the session, scoped per event, `HttpOnly`,
/// `Secure`, `SameSite=Lax`, with a `Max-Age` matching the session expiry.
fn session_cookie(value: &str, cfg: &Config, now: u64) -> String {
    let max_age = match cfg.session_mode {
        SessionMode::Fixed { ttl_secs } => ttl_secs,
        SessionMode::Sliding { idle_secs, .. } => idle_secs,
    };
    let _ = now;
    format!(
        "{}={value}; Max-Age={max_age}; Path=/; HttpOnly; Secure; SameSite=Lax",
        cfg.session_cookie_name
    )
}

/// A `Set-Cookie` header value for the time-limited fail-open bypass.
fn bypass_cookie(cfg: &Config, now: u64) -> String {
    let _ = now;
    format!(
        "{}=1; Max-Age={}; Path=/; HttpOnly; Secure; SameSite=Lax",
        cfg.bypass_cookie_name, cfg.bypass_ttl_secs
    )
}

/// Removes the admission token from the path's query string so the forwarded
/// URL no longer carries the single-use credential. The `Request`
/// path here already excludes the query in the API-Gateway shape, so this is a
/// defensive strip for shapes that include it.
fn strip_token(path: &str) -> String {
    let Some((base, query)) = path.split_once('?') else {
        return path.to_owned();
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|p| !p.starts_with("token=") && !p.starts_with("wr_token="))
        .collect();
    if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

/// Classifies a session-verification failure for logging, without leaking which
/// field failed to the client.
#[must_use]
pub fn describe(err: VerifyError) -> &'static str {
    match err {
        VerifyError::Malformed => "malformed",
        VerifyError::BadSignature => "bad_signature",
        VerifyError::Expired => "expired",
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "test code panics on setup failure and on unexpected decision variants"
    )]

    use super::*;
    use wr_common::AdmissionToken;

    fn key() -> SigningKey {
        SigningKey::new(b"a-32-byte-test-signing-key-value")
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
            rules: vec![ProtectionRule::PathPrefix("/tickets".to_owned())],
        }
    }

    fn req_to(path: &str) -> Request {
        Request {
            path: path.to_owned(),
            request_id: Some("018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned()),
            ..Request::default()
        }
    }

    #[test]
    fn valid_session_forwards() {
        let session = Session {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.cookies
            .push(("vwr_session".to_owned(), session.sign(&key())));
        assert_eq!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Forward
        );
    }

    #[test]
    fn expired_session_is_not_forwarded() {
        let session = Session {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.cookies
            .push(("vwr_session".to_owned(), session.sign(&key())));
        // Past expiry, on a protected path, reachable: redirected to wait.
        assert!(matches!(
            decide(&req, &cfg(), &key(), 6000, Reachability::Reachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn session_for_another_event_is_rejected() {
        let session = Session {
            event_id: "other-event".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.cookies
            .push(("vwr_session".to_owned(), session.sign(&key())));
        assert!(matches!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn valid_token_sets_session_and_records_arrival() {
        let token = AdmissionToken {
            event_id: "smoke".to_owned(),
            request_id: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.url_token = Some(token.sign(&key()));
        match decide(&req, &cfg(), &key(), 2000, Reachability::Reachable) {
            Decision::SetSessionAndForward {
                set_cookie,
                arrival_shard,
                ..
            } => {
                assert!(set_cookie.starts_with("vwr_session="));
                assert!(set_cookie.contains("HttpOnly"));
                assert!(arrival_shard < wr_common::SHARDS);
                // The minted cookie must verify as a session for this event.
                let value = set_cookie.split(['=', ';']).nth(1).unwrap();
                let s = Session::verify(value, &key(), 2000).unwrap();
                assert_eq!(s.event_id, "smoke");
            }
            other => panic!("expected SetSessionAndForward, got {other:?}"),
        }
    }

    #[test]
    fn expired_token_does_not_admit() {
        let token = AdmissionToken {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            expires_at: 1000,
        };
        let mut req = req_to("/tickets");
        req.url_token = Some(token.sign(&key()));
        assert!(matches!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn a_session_string_is_not_accepted_as_a_token() {
        // A session credential placed in the URL token slot must not admit: the
        // kind tags differ, so AdmissionToken::verify rejects it.
        let session = Session {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.url_token = Some(session.sign(&key()));
        assert!(matches!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn unprotected_path_forwards_without_credential() {
        let req = req_to("/public/home");
        assert_eq!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Forward
        );
    }

    #[test]
    fn protected_path_no_credential_redirects_when_reachable() {
        let req = req_to("/tickets/buy");
        assert!(matches!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Reachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn fail_open_bypasses_when_unreachable() {
        let req = req_to("/tickets/buy");
        match decide(&req, &cfg(), &key(), 2000, Reachability::Unreachable) {
            Decision::FailOpenBypass { set_cookie } => {
                assert!(set_cookie.starts_with("vwr_bypass="));
                assert!(set_cookie.contains("Max-Age=300"));
            }
            other => panic!("expected FailOpenBypass, got {other:?}"),
        }
    }

    #[test]
    fn fail_closed_redirects_when_unreachable() {
        let mut cfg = cfg();
        cfg.unreachable_policy = UnreachablePolicy::FailClosed;
        let req = req_to("/tickets/buy");
        assert!(matches!(
            decide(&req, &cfg, &key(), 2000, Reachability::Unreachable),
            Decision::Redirect { .. }
        ));
    }

    #[test]
    fn unreachable_but_valid_session_still_forwards() {
        // A healthy visitor is never gratuitously bypassed: a valid session
        // short-circuits before the reachability branch.
        let session = Session {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 5000,
        };
        let mut req = req_to("/tickets");
        req.cookies
            .push(("vwr_session".to_owned(), session.sign(&key())));
        assert_eq!(
            decide(&req, &cfg(), &key(), 2000, Reachability::Unreachable),
            Decision::Forward
        );
    }

    #[test]
    fn protection_rules_match_each_attribute() {
        let path = ProtectionRule::PathPrefix("/buy".to_owned());
        let header = ProtectionRule::Header {
            name: "x-protect".to_owned(),
            value: "yes".to_owned(),
        };
        let cookie = ProtectionRule::Cookie("gate".to_owned());
        let ua = ProtectionRule::UserAgent("BadBot".to_owned());

        let mut req = Request {
            path: "/buy/thing".to_owned(),
            ..Request::default()
        };
        req.headers.push(("x-protect".to_owned(), "yes".to_owned()));
        req.headers
            .push(("user-agent".to_owned(), "Mozilla BadBot/1".to_owned()));
        req.cookies.push(("gate".to_owned(), "1".to_owned()));

        assert!(path.matches(&req));
        assert!(header.matches(&req));
        assert!(cookie.matches(&req));
        assert!(ua.matches(&req));

        let empty = Request {
            path: "/other".to_owned(),
            ..Request::default()
        };
        assert!(!path.matches(&empty));
        assert!(!header.matches(&empty));
        assert!(!cookie.matches(&empty));
        assert!(!ua.matches(&empty));
    }

    #[test]
    fn sliding_session_extends_up_to_the_cap() {
        let mode = SessionMode::Sliding {
            idle_secs: 100,
            cap_secs: 500,
        };
        let session = Session {
            event_id: "smoke".to_owned(),
            request_id: "r1".to_owned(),
            issued_at: 1000,
            expires_at: 1100,
        };
        // Activity at 1050 extends expiry to 1150.
        let slid = slide(&session, mode, 1050).unwrap();
        assert_eq!(slid.expires_at, 1150);
        // Near the cap, expiry is clamped to issued_at + cap = 1500.
        let slid = slide(&session, mode, 1450).unwrap();
        assert_eq!(slid.expires_at, 1500);
        // A fixed session never slides.
        assert!(slide(&session, SessionMode::Fixed { ttl_secs: 100 }, 1050).is_none());
    }

    #[test]
    fn strip_token_removes_only_the_token_param() {
        assert_eq!(strip_token("/x?token=abc"), "/x");
        assert_eq!(strip_token("/x?a=1&token=abc&b=2"), "/x?a=1&b=2");
        assert_eq!(strip_token("/x?a=1"), "/x?a=1");
        assert_eq!(strip_token("/x"), "/x");
    }
}
