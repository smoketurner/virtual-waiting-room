//! The way into the queue for a visitor without JavaScript (issue #67).
//!
//! The waiting page is a JavaScript state machine: it mints an identity,
//! joins, polls and redeems in the browser. A visitor whose browser runs no
//! script gets its `<noscript>` notice and a plain form that posts here
//! instead, and from then on this does in the server what `waiting.js` does in
//! the browser:
//!
//! - `POST /v1/enter` mints the same identity the script would (a `UUIDv7`
//!   request id and a 32-byte possession secret, ADR-0035), keeps it in an
//!   `HttpOnly` cookie, and puts the same `{request_id, event_id, h}` message
//!   on the same join queue, so `assign_position` cannot tell the two apart.
//! - `GET /v1/wait` is a server-rendered page that refreshes itself. Each load
//!   runs [`generate_token::admit`], the one path a session is minted by, and
//!   either shows the visitor their place or sets the session cookie and sends
//!   them on.
//!
//! Compute on this path is deliberate and bounded: the function has its own
//! small reserved concurrency, so a flood of form posts cannot take capacity
//! from the burst path or from `generate_token`.
//!
//! This module is AWS-free: the join queue is a port ([`JoinQueue`]) and the
//! admission store is `generate_token`'s.

use std::future::Future;

pub mod page;
pub mod sqs;

use generate_token::{Admission, Denied};
use wr_common::{PossessionSecret, SecretDigest};

/// The identity cookie. `HttpOnly`, so no script on the origin's pages can
/// read the secret, and scoped to `/v1/`, which a browser without JavaScript
/// requests only for [`ENTER_PATH`] and [`WAIT_PATH`].
pub const IDENTITY_COOKIE: &str = "vwr_nojs";
pub const IDENTITY_COOKIE_PATH: &str = "/v1/";
/// How long the identity is kept, matching the script's storage.
pub const IDENTITY_MAX_AGE_SECS: u64 = 86_400;

pub const ENTER_PATH: &str = "/v1/enter";
pub const WAIT_PATH: &str = "/v1/wait";

/// A request id and the secret that proves it, as one value. The same shape
/// `waiting.js` stores (`<request_id>.<secret>`), so a visitor who later turns
/// JavaScript on is not asked to hold two different things.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub request_id: String,
    pub secret: PossessionSecret,
}

/// The operating system RNG failed. There is nothing to fall back to: an
/// identity without randomness is one anyone can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the system random number generator is unavailable")]
pub struct RngUnavailable;

impl Identity {
    /// Mints a fresh identity: a `UUIDv7` whose timestamp is `now_ms` and 32
    /// random bytes of secret, both from the aws-lc-rs RNG.
    ///
    /// # Errors
    ///
    /// [`RngUnavailable`] if the RNG cannot be read.
    pub fn mint(now_ms: u64) -> Result<Self, RngUnavailable> {
        use aws_lc_rs::rand::{SecureRandom, SystemRandom};
        use base64::Engine as _;

        let rng = SystemRandom::new();
        let mut id_bits = [0u8; 10];
        let mut secret = [0u8; 32];
        rng.fill(&mut id_bits).map_err(|_| RngUnavailable)?;
        rng.fill(&mut secret).map_err(|_| RngUnavailable)?;
        let request_id = uuid::Builder::from_unix_timestamp_millis(now_ms, &id_bits)
            .into_uuid()
            .to_string();
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        let secret = PossessionSecret::parse(&encoded).map_err(|_| RngUnavailable)?;
        Ok(Self { request_id, secret })
    }

    /// Parses the cookie value. Anything else -- a truncated value, a legacy
    /// bare id -- is no identity, so the visitor is offered the form again.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let (request_id, secret) = value.split_once('.')?;
        if !wr_common::is_uuid_shape(request_id) {
            return None;
        }
        Some(Self {
            request_id: request_id.to_owned(),
            secret: PossessionSecret::parse(secret).ok()?,
        })
    }

    /// The cookie value, `<request_id>.<secret>`.
    #[must_use]
    pub fn cookie_value(&self) -> String {
        format!("{}.{}", self.request_id, self.secret.expose())
    }

    /// The `Set-Cookie` header that stores this identity.
    #[must_use]
    pub fn set_cookie(&self) -> String {
        format!(
            "{IDENTITY_COOKIE}={}; Path={IDENTITY_COOKIE_PATH}; Max-Age={IDENTITY_MAX_AGE_SECS}; Secure; HttpOnly; SameSite=Lax",
            self.cookie_value()
        )
    }

    /// The join message, exactly as `waiting.js` posts it: the secret itself
    /// never goes on the queue, only its digest.
    #[must_use]
    pub fn join_message(&self, event_id: &str) -> String {
        let digest: SecretDigest = self.secret.digest();
        serde_json::json!({
            "request_id": self.request_id,
            "event_id": event_id,
            "h": digest.as_str(),
        })
        .to_string()
    }
}

/// The `Set-Cookie` that forgets an identity which no longer proves anything.
#[must_use]
pub fn clear_identity_cookie() -> String {
    format!(
        "{IDENTITY_COOKIE}=; Path={IDENTITY_COOKIE_PATH}; Max-Age=0; Secure; HttpOnly; SameSite=Lax"
    )
}

/// Finds the identity in a `Cookie` header.
#[must_use]
pub fn identity_from_cookies(header: &str) -> Option<Identity> {
    header.split(';').find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        (name == IDENTITY_COOKIE)
            .then(|| Identity::parse(value))
            .flatten()
    })
}

/// The join queue: `SendMessage` with the given body.
pub trait JoinQueue {
    fn send(&self, body: String) -> impl Future<Output = Result<(), QueueError>> + Send;
}

/// The join could not be enqueued.
#[derive(Debug, thiserror::Error)]
#[error("join queue: {0}")]
pub struct QueueError(pub String);

/// Puts the visitor's join on the queue.
///
/// # Errors
///
/// [`QueueError`] if the message was not accepted; the visitor holds no place.
pub async fn enter<Q: JoinQueue>(
    queue: &Q,
    event_id: &str,
    identity: &Identity,
) -> Result<(), QueueError> {
    queue.send(identity.join_message(event_id)).await
}

/// A same-site path to send an admitted visitor to, from a raw `next` value.
/// The rule `waiting.js` applies, plus a backslash in second position, which
/// browsers read as `//` and so would leave the site. Anything else is `/`.
#[must_use]
pub fn safe_next(raw: Option<&str>) -> String {
    let Some(decoded) = raw.and_then(percent_decode) else {
        return "/".to_owned();
    };
    let mut chars = decoded.chars();
    let first = chars.next();
    let second = chars.next();
    let controls = decoded.chars().any(char::is_control);
    if first != Some('/') || matches!(second, Some('/' | '\\')) || controls {
        return "/".to_owned();
    }
    decoded
}

/// The `next` parameter from a query string, still percent-encoded.
#[must_use]
pub fn next_param(query: &str) -> Option<&str> {
    query
        .trim_start_matches('?')
        .split('&')
        .find_map(|pair| pair.strip_prefix("next="))
}

/// The `next` the waiting page carried, read from the form post's `Referer`
/// (a plain form cannot copy the page's query string into itself). Only the
/// path it names is used, and [`safe_next`] keeps that on this site whatever
/// origin the header claims.
#[must_use]
pub fn next_from_referer(referer: Option<&str>) -> String {
    let query = referer.and_then(|r| r.split_once('?')).map(|(_, q)| q);
    safe_next(query.and_then(next_param))
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// What `GET /v1/wait` answers with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// Admitted: set the session cookie and send the visitor on.
    Admit { credential: String },
    /// Show a page, which refreshes itself.
    Show(WaitPage),
}

/// The page a waiting visitor sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitPage {
    /// No identity yet: the form.
    NotInLine,
    /// Joined, but the join has not been processed yet.
    Joining,
    /// In line, with a known place.
    Queued { position: u64, ahead: u64 },
    /// In line; the event is not admitting yet (not open, or held).
    Holding,
    /// The identity is not the one this place was taken with. Forgotten, so
    /// the form comes back.
    Lost,
    /// Something is wrong on this side; retrying may help.
    Unavailable,
}

impl WaitPage {
    /// Seconds until the page reloads itself. Short near the front and while a
    /// join is in flight, long while nothing can change.
    #[must_use]
    pub const fn refresh_secs(&self) -> Option<u64> {
        match self {
            Self::NotInLine | Self::Lost => None,
            Self::Joining => Some(5),
            Self::Queued { ahead, .. } => Some(if *ahead < 100 { 5 } else { 20 }),
            Self::Holding => Some(30),
            Self::Unavailable => Some(15),
        }
    }
}

/// Maps an admission attempt to what the visitor sees.
#[must_use]
pub fn outcome(admission: Admission) -> WaitOutcome {
    match admission {
        Admission::Admitted { credential, .. } => WaitOutcome::Admit { credential },
        Admission::Refused(denied) => WaitOutcome::Show(match denied {
            Denied::StillQueued { position, serving } => WaitPage::Queued {
                position,
                ahead: position.saturating_sub(serving),
            },
            Denied::NotRegistered => WaitPage::Joining,
            Denied::NotAdmitting | Denied::NotOpen => WaitPage::Holding,
            Denied::NotHolder => WaitPage::Lost,
            Denied::Corrupt => WaitPage::Unavailable,
        }),
        Admission::EventNotFound | Admission::SignFailed => {
            WaitOutcome::Show(WaitPage::Unavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use std::sync::Mutex;

    use super::*;

    const ID: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";
    const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn identity() -> Identity {
        Identity::parse(&format!("{ID}.{SECRET}")).unwrap()
    }

    #[test]
    fn a_minted_identity_round_trips_through_its_cookie() {
        let minted = Identity::mint(1_700_000_000_000).unwrap();
        assert!(wr_common::is_uuid_shape(&minted.request_id));
        assert_eq!(
            Identity::parse(&minted.cookie_value()),
            Some(minted.clone())
        );
        assert_ne!(Identity::mint(1_700_000_000_000).unwrap(), minted);
    }

    #[test]
    fn the_cookie_keeps_the_secret_from_scripts_and_the_origin() {
        let cookie = identity().set_cookie();
        for part in ["HttpOnly", "Secure", "SameSite=Lax", "Path=/v1/;"] {
            assert!(cookie.contains(part), "{part} missing from {cookie}");
        }
    }

    #[test]
    fn only_a_well_formed_identity_cookie_is_an_identity() {
        let header = format!("other=1; {IDENTITY_COOKIE}={ID}.{SECRET}; x=y");
        assert_eq!(identity_from_cookies(&header), Some(identity()));
        for bad in [
            format!("{IDENTITY_COOKIE}={ID}"),
            format!("{IDENTITY_COOKIE}={ID}.short"),
            format!("{IDENTITY_COOKIE}=not-a-uuid.{SECRET}"),
            format!("x{IDENTITY_COOKIE}={ID}.{SECRET}"),
            String::new(),
        ] {
            assert_eq!(identity_from_cookies(&bad), None, "{bad}");
        }
    }

    #[test]
    fn the_join_is_the_one_the_script_sends() {
        let msg: serde_json::Value = serde_json::from_str(&identity().join_message("evt")).unwrap();
        assert_eq!(msg["request_id"], ID);
        assert_eq!(msg["event_id"], "evt");
        assert_eq!(msg["h"], identity().secret.digest().as_str());
        assert!(
            msg.get("secret").is_none(),
            "the secret never goes on the queue"
        );
        assert_eq!(
            msg.as_object().unwrap().len(),
            3,
            "the API schema allows no more"
        );
    }

    struct Recorder(Mutex<Vec<String>>);
    impl JoinQueue for Recorder {
        fn send(&self, body: String) -> impl Future<Output = Result<(), QueueError>> + Send {
            self.0.lock().unwrap().push(body);
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn entering_sends_one_join() {
        let queue = Recorder(Mutex::new(Vec::new()));
        enter(&queue, "evt", &identity()).await.unwrap();
        assert_eq!(queue.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn next_stays_on_this_site() {
        assert_eq!(safe_next(Some("%2Fcheckout%3Fid%3D1")), "/checkout?id=1");
        for off_site in [
            "%2F%2Fevil.example",
            "%2F%5Cevil.example",
            "https%3A%2F%2Fevil.example",
            "evil",
            "%2Fa%0D%0ASet-Cookie:x",
            "%zz",
            "",
        ] {
            assert_eq!(safe_next(Some(off_site)), "/", "{off_site}");
        }
        assert_eq!(safe_next(None), "/");
    }

    #[test]
    fn next_is_read_from_the_waiting_page_referer() {
        assert_eq!(
            next_from_referer(Some(
                "https://shop.example/_wr/waiting.html?r=none&next=%2Fcheckout"
            )),
            "/checkout"
        );
        assert_eq!(
            next_from_referer(Some("https://evil.example/?next=%2F%2Fevil.example")),
            "/"
        );
        assert_eq!(
            next_from_referer(Some("https://shop.example/_wr/waiting.html")),
            "/"
        );
        assert_eq!(next_from_referer(None), "/");
    }

    #[test]
    fn each_admission_outcome_has_a_page() {
        assert_eq!(
            outcome(Admission::Refused(Denied::StillQueued {
                position: 700,
                serving: 500
            })),
            WaitOutcome::Show(WaitPage::Queued {
                position: 700,
                ahead: 200
            })
        );
        assert_eq!(
            outcome(Admission::Admitted {
                position: 1,
                expires_at: 2,
                credential: "jws".to_owned()
            }),
            WaitOutcome::Admit {
                credential: "jws".to_owned()
            }
        );
        for (denied, page) in [
            (Denied::NotRegistered, WaitPage::Joining),
            (Denied::NotAdmitting, WaitPage::Holding),
            (Denied::NotOpen, WaitPage::Holding),
            (Denied::NotHolder, WaitPage::Lost),
            (Denied::Corrupt, WaitPage::Unavailable),
        ] {
            assert_eq!(outcome(Admission::Refused(denied)), WaitOutcome::Show(page));
        }
        assert_eq!(
            outcome(Admission::SignFailed),
            WaitOutcome::Show(WaitPage::Unavailable)
        );
    }

    #[test]
    fn a_page_that_can_change_refreshes_and_one_that_cannot_does_not() {
        assert_eq!(WaitPage::NotInLine.refresh_secs(), None);
        assert_eq!(WaitPage::Lost.refresh_secs(), None);
        assert_eq!(
            WaitPage::Queued {
                position: 5,
                ahead: 5
            }
            .refresh_secs(),
            Some(5)
        );
        assert!(WaitPage::Holding.refresh_secs().is_some());
    }
}
