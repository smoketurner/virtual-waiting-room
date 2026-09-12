//! Client-signed entry tickets (issue #59).
//!
//! `request_id` used to be a client-supplied `UUIDv7`, so `attribute_not_exists`
//! only caught a repeat of the same id — a fresh id defeated it, and volume
//! converted linearly into share of the queue. An entry ticket closes that: the
//! customer's own system signs an ES256 JWS naming an opaque subject, this
//! module verifies it, and `request_id` is derived deterministically from the
//! subject, so re-registering under the same identity always claims the same
//! id and the existing conditional-write guard enforces one position per
//! identity with no new table and no new write.
//!
//! A deployment with no configured public key runs [`EntryPolicy::Open`]:
//! today's bare raffle, unchanged. Verification happens once, in
//! `assign_position` (the SQS consumer) — never in VTL, so the join path keeps
//! zero compute either way.

use aws_lc_rs::digest;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;

/// The ES256 public key a configured deployment verifies tickets against.
pub struct TicketKey(DecodingKey);

/// Error building a [`TicketKey`] from the configured JWK. A parse failure
/// here is a hard `assign_position` init failure and must never silently
/// downgrade to [`EntryPolicy::Open`] — a customer expecting the entry check
/// to be enforced must not find out it was not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// Not a JSON object, or missing a required member.
    #[error("public key is not a well-formed JWK")]
    NotJson,
    /// `kty` is not `"EC"`, or `crv` is not `"P-256"`.
    #[error("key type or curve is not EC P-256")]
    UnsupportedKeyType,
    /// `x` or `y` does not decode to exactly 32 bytes.
    #[error("a coordinate does not decode to 32 bytes")]
    BadCoordinates,
}

/// The subset of a JSON JWK this deployment accepts: `{"kty":"EC","crv":"P-256","x":"...","y":"..."}`.
#[derive(Deserialize)]
struct Jwk {
    kty: String,
    crv: String,
    x: String,
    y: String,
}

impl TicketKey {
    /// Parses a JSON JWK and checks it is a structurally valid P-256 public
    /// key: right key type, right curve, and both coordinates exactly 32
    /// bytes once base64url-decoded.
    ///
    /// This cannot detect every unusable key — an off-curve point of the
    /// correct size passes here and fails only when the first ticket is
    /// verified against it, surfacing as an ordinary bad signature. The
    /// backstop for that residual is the operator-facing drop-count alarm
    /// (`join_dropped`), not this check.
    ///
    /// # Errors
    ///
    /// [`KeyError`] describing what about the JWK was wrong.
    pub fn from_jwk_json(json: &str) -> Result<Self, KeyError> {
        let jwk: Jwk = serde_json::from_str(json).map_err(|_err| KeyError::NotJson)?;
        if jwk.kty != "EC" || jwk.crv != "P-256" {
            return Err(KeyError::UnsupportedKeyType);
        }
        let x = URL_SAFE_NO_PAD
            .decode(&jwk.x)
            .map_err(|_err| KeyError::BadCoordinates)?;
        let y = URL_SAFE_NO_PAD
            .decode(&jwk.y)
            .map_err(|_err| KeyError::BadCoordinates)?;
        if x.len() != 32 || y.len() != 32 {
            return Err(KeyError::BadCoordinates);
        }
        // `from_ec_components` re-decodes x and y itself; the decode above is
        // purely the length check, not reused here.
        let key = DecodingKey::from_ec_components(&jwk.x, &jwk.y)
            .map_err(|_err| KeyError::BadCoordinates)?;
        Ok(Self(key))
    }
}

/// Whether a deployment gates entry with a signed ticket.
pub enum EntryPolicy {
    /// No public key configured: today's bare raffle. `request_id` is
    /// accepted at face value, shape-checked only.
    Open,
    /// A public key is configured: every join must carry a ticket that
    /// verifies under it, and `request_id` must equal the value derived from
    /// the ticket's subject.
    Ticketed(TicketKey),
}

impl EntryPolicy {
    /// Empty or whitespace-only configuration means [`EntryPolicy::Open`].
    /// Anything else must parse as a valid ES256 JWK or this returns
    /// [`KeyError`] — the caller must treat that as a hard init failure.
    ///
    /// # Errors
    ///
    /// [`KeyError`] if `configured` is non-empty and not a valid P-256 JWK.
    pub fn parse(configured: &str) -> Result<Self, KeyError> {
        if configured.trim().is_empty() {
            return Ok(Self::Open);
        }
        Ok(Self::Ticketed(TicketKey::from_jwk_json(configured)?))
    }
}

/// The opaque per-(identity, event) subject a verified ticket attests. Never
/// the raw identifier: the integration contract requires the customer to
/// derive it (e.g. `base64url(HMAC-SHA256(pepper, identity || event_id))`), so
/// the waiting room never sees, stores, or derives anything reversible from
/// the identifier itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketSubject(String);

impl TicketSubject {
    /// The subject as a plain string, for hashing into a `request_id`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Minimum length of an opaque `sub` claim: the unpadded base64url encoding of
/// a 16-byte value. Long enough to fail closed on a raw membership id, an
/// email, or a UUID — the mistakes a customer would actually make — without
/// also rejecting sound alternatives such as a hex-encoded SHA-256 (64 chars)
/// or a shorter base64url subject. This checks shape, not entropy: 22 `a`s
/// pass. Opaqueness is a customer obligation the wire format cannot enforce.
const MIN_SUBJECT_LEN: usize = 22;

/// Maximum length of a `sub` claim, generous enough for any sound encoding
/// while still bounding how much a caller can stuff into the claim.
const MAX_SUBJECT_LEN: usize = 256;

/// Clock-skew and SQS batching-window leeway applied to `exp`/`nbf`. Any
/// `maximum_batching_window_in_seconds` setting lets Lambda hold a message up
/// to 20 seconds even on a low-traffic queue (`infra/modules/core/main.tf`),
/// which this leeway also covers.
const LEEWAY_SECS: u64 = 60;

/// Why a presented ticket does not verify.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TicketError {
    /// Not a well-formed compact JWS, or the claims do not deserialize.
    #[error("ticket is malformed")]
    Malformed,
    /// The signature does not verify under the configured key.
    #[error("ticket signature does not verify")]
    BadSignature,
    /// `now` is at or past `exp` (plus leeway).
    #[error("ticket has expired")]
    Expired,
    /// `now` is before `nbf` (minus leeway).
    #[error("ticket is not yet valid")]
    NotYetValid,
    /// `aud` does not contain this deployment's `event_id`.
    #[error("ticket audience does not match this event")]
    WrongAudience,
    /// `sub` fails the opaque-subject shape check.
    #[error("ticket subject is not an opaque identifier")]
    BadSubject,
}

/// `aud` is a registered claim that may be a single string or an array of
/// strings (RFC 7519 §4.1.3); both shapes are accepted.
#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn contains(&self, event_id: &str) -> bool {
        match self {
            Self::One(aud) => aud == event_id,
            Self::Many(auds) => auds.iter().any(|aud| aud == event_id),
        }
    }
}

#[derive(Deserialize)]
struct TicketClaims {
    aud: Audience,
    sub: String,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
}

/// Verifies a compact ES256 entry ticket and returns its subject.
///
/// Pins [`Algorithm::ES256`] in the validator rather than reading `alg` from
/// the token to select a verifier, so a token cannot choose its own
/// verification algorithm. Expiry and not-before are checked here against an
/// injected `now` (with [`LEEWAY_SECS`] on both sides), the same shape
/// `crypto.rs` already uses, so this stays clock-free and testable.
///
/// # Errors
///
/// [`TicketError`] describing why the ticket was rejected.
pub fn verify_ticket(
    key: &TicketKey,
    ticket: &str,
    event_id: &str,
    now: u64,
) -> Result<TicketSubject, TicketError> {
    let mut validation = Validation::new(Algorithm::ES256);
    validation.validate_exp = false;
    validation.validate_aud = false;
    validation.validate_nbf = false;
    validation.required_spec_claims.clear();

    let data = jsonwebtoken::decode::<TicketClaims>(ticket, &key.0, &validation).map_err(
        |err| match err.kind() {
            jsonwebtoken::errors::ErrorKind::InvalidSignature => TicketError::BadSignature,
            _ => TicketError::Malformed,
        },
    )?;
    let claims = data.claims;

    if !claims.aud.contains(event_id) {
        return Err(TicketError::WrongAudience);
    }
    if now >= claims.exp.saturating_add(LEEWAY_SECS) {
        return Err(TicketError::Expired);
    }
    if let Some(nbf) = claims.nbf
        && now.saturating_add(LEEWAY_SECS) < nbf
    {
        return Err(TicketError::NotYetValid);
    }
    if !is_opaque_subject(&claims.sub) {
        return Err(TicketError::BadSubject);
    }
    Ok(TicketSubject(claims.sub))
}

/// Shape-checks a `sub` claim: `[A-Za-z0-9_-]` only, between
/// [`MIN_SUBJECT_LEN`] and [`MAX_SUBJECT_LEN`] characters.
fn is_opaque_subject(sub: &str) -> bool {
    (MIN_SUBJECT_LEN..=MAX_SUBJECT_LEN).contains(&sub.len())
        && sub
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Derives `request_id` from a verified ticket's subject:
/// `uuid_shape(SHA-256("vwr/rid/v1" || 0x00 || event_id || 0x00 || sub)[0..16])`.
///
/// Deterministic and replay-safe: presenting the same ticket twice re-derives
/// the same id, so the existing `attribute_not_exists` guard on that id is
/// what enforces one position per identity — no new table, no new write.
/// Folding `event_id` in means two deployments cannot be cross-linked by
/// request id even if a customer reuses a subject.
#[must_use]
pub fn derive_request_id(event_id: &str, subject: &TicketSubject) -> String {
    let mut message =
        Vec::with_capacity(b"vwr/rid/v1".len() + 1 + event_id.len() + 1 + subject.0.len());
    message.extend_from_slice(b"vwr/rid/v1");
    message.push(0);
    message.extend_from_slice(event_id.as_bytes());
    message.push(0);
    message.extend_from_slice(subject.0.as_bytes());

    let hash = digest::digest(&digest::SHA256, &message);
    let hash_bytes = hash.as_ref();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash_bytes[0..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format_uuid(&bytes)
}

/// Formats 16 bytes as the canonical `8-4-4-4-12` lowercase hex form.
fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// Checks the canonical `8-4-4-4-12` hex shape. No version or variant nibble
/// check: under [`EntryPolicy::Ticketed`] the id must equal
/// [`derive_request_id`]'s output exactly, and under [`EntryPolicy::Open`] the
/// nibbles carry no security value — this only rejects a malformed or
/// truncated id before it reaches a claim. Replaces the old `is_uuid_v7`
/// check, which rejected an id this deployment never depended on being
/// time-ordered (no consumer sorts by it, and no GSI exists).
#[must_use]
pub fn is_uuid_shape(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use jsonwebtoken::{EncodingKey, Header, encode};
    use proptest::prelude::*;
    use serde::Serialize;

    use super::*;

    /// A fixed P-256 key pair for tests, as raw components. Generated once and
    /// pinned here rather than regenerated per run, so a failure is
    /// reproducible.
    const PRIVATE_KEY_PKCS8_B64: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgCM8Gu+5Pe7vq5HGC\
         lTlTi057LV3NH5ii+Dukp6okfsyhRANCAASr/ULlDGWbRTOc9ODxvjO0ZGoYNM9xdOjCDMdugn1YF+dCMIpy\
         2dpBXYnPIBsBX0Nnel+lCOrOld3pPX3Izwfi";
    const X_B64: &str = "q_1C5Qxlm0UznPTg8b4ztGRqGDTPcXTowgzHboJ9WBc";
    const Y_B64: &str = "50IwinLZ2kFdic8gGwFfQ2d6X6UI6s6V3ek9fcjPB-I";

    fn jwk_json() -> String {
        format!(r#"{{"kty":"EC","crv":"P-256","x":"{X_B64}","y":"{Y_B64}"}}"#)
    }

    fn key() -> TicketKey {
        TicketKey::from_jwk_json(&jwk_json()).unwrap()
    }

    fn encoding_key() -> EncodingKey {
        use base64::Engine as _;
        let der = base64::engine::general_purpose::STANDARD
            .decode(PRIVATE_KEY_PKCS8_B64)
            .unwrap();
        EncodingKey::from_ec_der(&der)
    }

    #[derive(Serialize)]
    struct RawClaims<'a> {
        aud: &'a str,
        sub: &'a str,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<u64>,
    }

    const SUBJECT: &str = "0123456789abcdefghijklmnopqrstuvwxyz-_ABCD";

    fn sign(event_id: &str, sub: &str, exp: u64, nbf: Option<u64>) -> String {
        let claims = RawClaims {
            aud: event_id,
            sub,
            exp,
            nbf,
        };
        encode(&Header::new(Algorithm::ES256), &claims, &encoding_key()).unwrap()
    }

    #[test]
    fn a_valid_ticket_verifies_and_returns_its_subject() {
        let ticket = sign("evt-1", SUBJECT, 2_000_000_000, None);
        let subject = verify_ticket(&key(), &ticket, "evt-1", 1_000_000_000).unwrap();
        assert_eq!(subject.as_str(), SUBJECT);
    }

    #[test]
    fn wrong_event_is_rejected() {
        let ticket = sign("evt-1", SUBJECT, 2_000_000_000, None);
        assert_eq!(
            verify_ticket(&key(), &ticket, "evt-2", 1_000_000_000),
            Err(TicketError::WrongAudience)
        );
    }

    #[test]
    fn an_array_audience_containing_the_event_is_accepted() {
        let claims = serde_json::json!({
            "aud": ["other-event", "evt-1"],
            "sub": SUBJECT,
            "exp": 2_000_000_000u64,
        });
        let ticket = encode(&Header::new(Algorithm::ES256), &claims, &encoding_key()).unwrap();
        assert!(verify_ticket(&key(), &ticket, "evt-1", 1_000_000_000).is_ok());
    }

    #[test]
    fn expired_ticket_is_rejected() {
        let ticket = sign("evt-1", SUBJECT, 1000, None);
        assert_eq!(
            verify_ticket(&key(), &ticket, "evt-1", 1000 + LEEWAY_SECS),
            Err(TicketError::Expired)
        );
        // Inside the leeway window, still valid.
        assert!(verify_ticket(&key(), &ticket, "evt-1", 1000 + LEEWAY_SECS - 1).is_ok());
    }

    #[test]
    fn not_yet_valid_ticket_is_rejected() {
        let ticket = sign("evt-1", SUBJECT, 2_000_000_000, Some(1_000_000));
        assert_eq!(
            verify_ticket(&key(), &ticket, "evt-1", 1_000_000 - LEEWAY_SECS - 1),
            Err(TicketError::NotYetValid)
        );
        assert!(verify_ticket(&key(), &ticket, "evt-1", 1_000_000 - LEEWAY_SECS).is_ok());
    }

    #[test]
    fn a_non_opaque_subject_is_rejected() {
        for bad_sub in ["member-00412", "user@example.com", "short-id"] {
            let ticket = sign("evt-1", bad_sub, 2_000_000_000, None);
            assert_eq!(
                verify_ticket(&key(), &ticket, "evt-1", 1_000_000_000),
                Err(TicketError::BadSubject),
                "subject {bad_sub:?} should have been rejected"
            );
        }
    }

    #[test]
    fn subject_length_boundaries() {
        let just_short = "a".repeat(MIN_SUBJECT_LEN - 1);
        let exactly_min = "a".repeat(MIN_SUBJECT_LEN);
        let exactly_max = "a".repeat(MAX_SUBJECT_LEN);
        let just_long = "a".repeat(MAX_SUBJECT_LEN + 1);

        assert!(!is_opaque_subject(&just_short));
        assert!(is_opaque_subject(&exactly_min));
        assert!(is_opaque_subject(&exactly_max));
        assert!(!is_opaque_subject(&just_long));

        // A hex-encoded SHA-256 (64 chars) and a 16-byte base64url subject (22
        // chars) are both sound customer choices the check must not reject.
        assert!(is_opaque_subject(&"a".repeat(64)));
        assert!(is_opaque_subject(&"a".repeat(22)));
    }

    #[test]
    fn a_tampered_signature_is_rejected() {
        let ticket = sign("evt-1", SUBJECT, 2_000_000_000, None);
        let mut parts: Vec<&str> = ticket.split('.').collect();
        let tampered_sig = if parts[2].starts_with('A') {
            format!("B{}", &parts[2][1..])
        } else {
            format!("A{}", &parts[2][1..])
        };
        parts[2] = &tampered_sig;
        let forged = parts.join(".");
        assert_eq!(
            verify_ticket(&key(), &forged, "evt-1", 1_000_000_000),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn malformed_tickets_are_rejected_not_panicking() {
        for bad in ["", "not-a-jwt", "a.b.c", "a.b"] {
            assert_eq!(
                verify_ticket(&key(), bad, "evt-1", 0),
                Err(TicketError::Malformed)
            );
        }
    }

    #[test]
    fn an_hs256_token_signed_with_the_public_key_bytes_does_not_verify() {
        // Confusing the public key material for an HMAC secret must not work:
        // the validator pins ES256, so a token whose header claims HS256 is
        // rejected before the (wrong) algorithm family is ever tried.
        let claims = RawClaims {
            aud: "evt-1",
            sub: SUBJECT,
            exp: 2_000_000_000,
            nbf: None,
        };
        let hmac_key = EncodingKey::from_secret(X_B64.as_bytes());
        let ticket = encode(&Header::new(Algorithm::HS256), &claims, &hmac_key).unwrap();
        assert!(verify_ticket(&key(), &ticket, "evt-1", 1_000_000_000).is_err());
    }

    #[test]
    fn a_31_byte_coordinate_is_rejected_at_key_construction() {
        // 31 raw bytes base64url-encodes to 42 chars, one short of the 32-byte
        // (43-char) form.
        let short_x = URL_SAFE_NO_PAD.encode([0u8; 31]);
        let json = format!(r#"{{"kty":"EC","crv":"P-256","x":"{short_x}","y":"{Y_B64}"}}"#);
        assert_eq!(
            TicketKey::from_jwk_json(&json).err(),
            Some(KeyError::BadCoordinates)
        );
    }

    #[test]
    fn a_non_ec_key_type_is_rejected() {
        let json = format!(r#"{{"kty":"RSA","crv":"P-256","x":"{X_B64}","y":"{Y_B64}"}}"#);
        assert_eq!(
            TicketKey::from_jwk_json(&json).err(),
            Some(KeyError::UnsupportedKeyType)
        );
    }

    #[test]
    fn a_non_p256_curve_is_rejected() {
        let json = format!(r#"{{"kty":"EC","crv":"P-384","x":"{X_B64}","y":"{Y_B64}"}}"#);
        assert_eq!(
            TicketKey::from_jwk_json(&json).err(),
            Some(KeyError::UnsupportedKeyType)
        );
    }

    #[test]
    fn malformed_json_is_rejected() {
        assert_eq!(
            TicketKey::from_jwk_json("not json").err(),
            Some(KeyError::NotJson)
        );
        assert_eq!(TicketKey::from_jwk_json("").err(), Some(KeyError::NotJson));
    }

    #[test]
    fn empty_configuration_is_the_open_policy() {
        assert!(matches!(EntryPolicy::parse("").unwrap(), EntryPolicy::Open));
        assert!(matches!(
            EntryPolicy::parse("   ").unwrap(),
            EntryPolicy::Open
        ));
    }

    #[test]
    fn a_configured_key_is_the_ticketed_policy() {
        assert!(matches!(
            EntryPolicy::parse(&jwk_json()).unwrap(),
            EntryPolicy::Ticketed(_)
        ));
    }

    #[test]
    fn a_bad_configured_key_is_a_hard_error_not_open() {
        assert!(EntryPolicy::parse("not a jwk").is_err());
    }

    #[test]
    fn derivation_is_deterministic_and_scoped_to_event_and_subject() {
        let subject = TicketSubject(SUBJECT.to_owned());
        let a = derive_request_id("evt-1", &subject);
        let b = derive_request_id("evt-1", &subject);
        assert_eq!(a, b);
        assert!(is_uuid_shape(&a));

        // A different event, or a different subject, changes the id.
        assert_ne!(a, derive_request_id("evt-2", &subject));
        let other = TicketSubject(format!("{SUBJECT}x"));
        assert_ne!(a, derive_request_id("evt-1", &other));
    }

    #[test]
    fn uuid_shape_validation() {
        assert!(is_uuid_shape("018f3a2b-7c9d-7e1f-abcd-0123456789ab"));
        // A v4-shaped id (no version-7 nibble) still passes: the shape check
        // no longer pins a version, only the hyphen/hex layout.
        assert!(is_uuid_shape("018f3a2b-7c9d-4e1f-abcd-0123456789ab"));
        assert!(!is_uuid_shape("not-a-uuid"));
        assert!(!is_uuid_shape("018f3a2b7c9d7e1fabcd0123456789ab"));
        assert!(!is_uuid_shape(""));
        assert!(!is_uuid_shape("018f3a2b-7c9d-7e1f-abcd-0123456789ab-extra"));
    }

    proptest! {
        /// Verification is a boundary: the ticket comes off the join body and
        /// is attacker-chosen. Any input must be rejected, never panic.
        #[test]
        fn verify_never_panics_on_arbitrary_input(s in ".{0,300}") {
            let k = key();
            prop_assert!(verify_ticket(&k, &s, "evt-1", 0).is_err());
        }

        /// The opaque-subject check never panics on arbitrary input either.
        #[test]
        fn is_opaque_subject_never_panics(s in ".{0,400}") {
            let _ = is_opaque_subject(&s);
        }
    }
}
