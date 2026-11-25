//! The way into the queue for a visitor without JavaScript (issue #67).
//!
//! The waiting page is a JavaScript state machine: it mints an identity,
//! joins, polls and redeems in the browser. A visitor whose browser runs no
//! script gets its `<noscript>` notice and a plain form that posts here
//! instead, and from then on this does in the server what `waiting.js` does in
//! the browser:
//!
//! - `POST /_wr/v1/enter` mints the same identity the script would (a `UUIDv7`
//!   request id and a 32-byte possession secret, ADR-0035), keeps it in an
//!   `HttpOnly` cookie, and puts the same `{request_id, event_id, h}` message
//!   on the same join queue, so `assign_position` cannot tell the two apart.
//!   The cookie also records when the join was sent, so a join the queue lost
//!   is sent again rather than waited on forever (issue #251).
//! - `GET /_wr/v1/wait` is a server-rendered page that refreshes itself. Each load
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
/// read the secret, and scoped to `/_wr/v1/`, which a browser without JavaScript
/// requests only for [`ENTER_PATH`] and [`WAIT_PATH`].
pub const IDENTITY_COOKIE: &str = "vwr_nojs";
pub const IDENTITY_COOKIE_PATH: &str = "/_wr/v1/";
/// How long the identity is kept, matching the script's storage.
pub const IDENTITY_MAX_AGE_SECS: u64 = 86_400;

/// How long a join may go unanswered before it is taken for lost and sent
/// again (issue #251). The ingest's worst-case delivery latency is about 20
/// seconds (the event source mapping's batch window on a quiet queue), so a
/// join still in flight is not resent; `waiting.js` clears the same window
/// with its `MAX_MISSES`.
pub const REJOIN_AFTER_SECS: u64 = 60;
/// How many times a lost join is sent again before the visitor is told and
/// given the form back. Bounded so a join nothing will ever accept does not
/// cost a queue write a minute for the cookie's whole day.
pub const MAX_REJOINS: u8 = 3;

pub const ENTER_PATH: &str = "/_wr/v1/enter";
pub const WAIT_PATH: &str = "/_wr/v1/wait";

/// A request id, the secret that proves it, and when its join was last sent,
/// as one cookie value: `<request_id>.<secret>.<joined_at>.<rejoins>`.
///
/// The last two are what `waiting.js` keeps in memory as its miss counter: a
/// page that reloads has nowhere else to remember that a join has gone
/// unanswered (issue #251). They are not secret and not signed. A visitor who
/// rewrites them can only resend their own join, which a bare post to
/// [`ENTER_PATH`] already costs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub request_id: String,
    pub secret: PossessionSecret,
    /// Epoch seconds at which the join was last put on the queue.
    pub joined_at: u64,
    /// How many times it has been sent again since the first.
    pub rejoins: u8,
}

/// What to do about a join no registration has appeared for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unanswered {
    /// Recent enough to still be in flight: keep waiting.
    InFlight,
    /// Overdue: send the same join again, and keep this restamped identity
    /// once the queue has taken it.
    Resend(Identity),
    /// Sent [`MAX_REJOINS`] more times and still nothing: tell the visitor.
    GaveUp,
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
        Ok(Self {
            request_id,
            secret,
            joined_at: now_ms / 1000,
            rejoins: 0,
        })
    }

    /// Parses the cookie value. Anything else -- a truncated value, a bare id,
    /// one without its join stamp -- is no identity, so the visitor is offered
    /// the form again.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split('.');
        let (request_id, secret, joined_at, rejoins) =
            (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() || !wr_common::is_uuid_shape(request_id) {
            return None;
        }
        Some(Self {
            request_id: request_id.to_owned(),
            secret: PossessionSecret::parse(secret).ok()?,
            joined_at: parse_decimal(joined_at)?,
            rejoins: u8::try_from(parse_decimal(rejoins)?).ok()?,
        })
    }

    /// The cookie value, `<request_id>.<secret>.<joined_at>.<rejoins>`.
    #[must_use]
    pub fn cookie_value(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.request_id,
            self.secret.expose(),
            self.joined_at,
            self.rejoins
        )
    }

    /// What to do at `now` (epoch seconds) when no registration exists for
    /// this identity (issue #251).
    ///
    /// A join the queue or the ingest lost leaves nothing behind, so the only
    /// evidence is time. The resend carries the same request id and digest:
    /// `assign_position` writes a place only where none exists for the id, so
    /// a first join that was merely slow is not given a second one. A stamp
    /// from the future counts as just sent.
    #[must_use]
    pub fn unanswered(&self, now: u64) -> Unanswered {
        if now.saturating_sub(self.joined_at) <= REJOIN_AFTER_SECS {
            return Unanswered::InFlight;
        }
        match self.rejoins.checked_add(1) {
            Some(rejoins) if rejoins <= MAX_REJOINS => Unanswered::Resend(Self {
                request_id: self.request_id.clone(),
                secret: self.secret.clone(),
                joined_at: now,
                rejoins,
            }),
            Some(_) | None => Unanswered::GaveUp,
        }
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

/// A plain decimal number: digits only, so `+5` and a 40-digit string a
/// visitor typed into their cookie are both refused rather than interpreted.
fn parse_decimal(text: &str) -> Option<u64> {
    if text.is_empty() || text.len() > 20 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
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

/// What `GET /_wr/v1/wait` answers with.
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
    /// The join was sent [`MAX_REJOINS`] more times and never registered
    /// (issue #251). Forgotten, so the form comes back.
    JoinLost,
    /// Something is wrong on this side; retrying may help.
    Unavailable,
}

impl WaitPage {
    /// Seconds until the page reloads itself. Short near the front and while a
    /// join is in flight, long while nothing can change.
    #[must_use]
    pub const fn refresh_secs(&self) -> Option<u64> {
        match self {
            Self::NotInLine | Self::Lost | Self::JoinLost => None,
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
            // Their turn has come and the origin is full for the moment: the
            // front of the line, refreshing at the short interval.
            Denied::AtCapacity { position } => WaitPage::Queued { position, ahead: 0 },
            Denied::NotRegistered => WaitPage::Joining,
            Denied::NotAdmitting | Denied::NotOpen => WaitPage::Holding,
            // A used-up place is, for this visitor, the same as one that is
            // not theirs: nothing here can be redeemed, and they start again.
            Denied::NotHolder | Denied::SessionExpired => WaitPage::Lost,
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

    /// The second the test identity's join was sent at.
    const JOINED: u64 = 1_700_000_000;

    fn identity() -> Identity {
        Identity::parse(&format!("{ID}.{SECRET}.{JOINED}.0")).unwrap()
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
        for part in ["HttpOnly", "Secure", "SameSite=Lax", "Path=/_wr/v1/;"] {
            assert!(cookie.contains(part), "{part} missing from {cookie}");
        }
    }

    #[test]
    fn only_a_well_formed_identity_cookie_is_an_identity() {
        let header = format!("other=1; {IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}.0; x=y");
        assert_eq!(identity_from_cookies(&header), Some(identity()));
        for bad in [
            format!("{IDENTITY_COOKIE}={ID}"),
            format!("{IDENTITY_COOKIE}={ID}.short.{JOINED}.0"),
            format!("{IDENTITY_COOKIE}=not-a-uuid.{SECRET}.{JOINED}.0"),
            format!("x{IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}.0"),
            // Without the join stamp there is no telling a lost join from a
            // slow one (issue #251), so it is no identity.
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}.0.0"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.+5.0"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}.256"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.{JOINED}.-1"),
            format!("{IDENTITY_COOKIE}={ID}.{SECRET}.{}.0", "9".repeat(21)),
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

    /// The restamped identity of a resend, if that is what was decided.
    fn resent(decided: Unanswered) -> Option<Identity> {
        match decided {
            Unanswered::Resend(identity) => Some(identity),
            Unanswered::InFlight | Unanswered::GaveUp => None,
        }
    }

    #[test]
    fn a_minted_identity_is_stamped_with_when_it_joined() {
        let minted = Identity::mint(1_700_000_000_999).unwrap();
        assert_eq!((minted.joined_at, minted.rejoins), (JOINED, 0));
    }

    #[test]
    fn an_unanswered_join_is_left_alone_while_it_can_still_be_in_flight() {
        // The ingest may take 20 s on a quiet queue; a resend inside that is
        // a duplicate message for a place that is simply late.
        for elapsed in [0, 20, REJOIN_AFTER_SECS] {
            assert_eq!(
                identity().unanswered(JOINED + elapsed),
                Unanswered::InFlight,
                "{elapsed}s"
            );
        }
        // A stamp ahead of this clock is treated as just sent.
        assert_eq!(identity().unanswered(JOINED - 500), Unanswered::InFlight);
    }

    #[test]
    fn an_overdue_join_is_resent_as_the_same_join_and_restamped() {
        let now = JOINED + REJOIN_AFTER_SECS + 1;
        let resent = resent(identity().unanswered(now)).unwrap();
        assert_eq!(resent.joined_at, now);
        assert_eq!(resent.rejoins, 1);
        // Same id, same digest: assign_position cannot give it a second place.
        assert_eq!(resent.join_message("evt"), identity().join_message("evt"));
        // Restamped, so the next reload waits a full window before the next.
        assert_eq!(resent.unanswered(now + 5), Unanswered::InFlight);
        assert_eq!(Identity::parse(&resent.cookie_value()), Some(resent));
    }

    #[test]
    fn a_join_is_resent_a_bounded_number_of_times_then_given_up() {
        let mut held = identity();
        let mut now = JOINED;
        for expected in 1..=MAX_REJOINS {
            now += REJOIN_AFTER_SECS + 1;
            let resent = resent(held.unanswered(now)).unwrap();
            assert_eq!(resent.rejoins, expected);
            held = resent;
        }
        assert_eq!(held.unanswered(now + 5), Unanswered::InFlight);
        now += REJOIN_AFTER_SECS + 1;
        assert_eq!(held.unanswered(now), Unanswered::GaveUp);

        // A count a visitor wrote into the cookie cannot overflow into more.
        let maxed = Identity {
            rejoins: u8::MAX,
            ..identity()
        };
        assert_eq!(maxed.unanswered(now), Unanswered::GaveUp);
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
        assert_eq!(WaitPage::JoinLost.refresh_secs(), None);
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
