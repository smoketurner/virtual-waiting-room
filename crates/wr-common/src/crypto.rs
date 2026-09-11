//! Signed admission tokens and session cookies.
//!
//! Both are JSON Web Signatures in compact serialization — a JWT — signed
//! `HS256` under a per-deployment key. An origin, a proxy or an operator can
//! validate one with any JWT library; only the `CloudFront` Function hand-rolls
//! verification, because its runtime has no library to call.
//!
//! `HS256` is not a preference. The `CloudFront` Functions `crypto` module
//! exposes `createHash` and `createHmac` over `md5`, `sha1` and `sha256` and
//! nothing else, so `RS256`, `ES256`, `HS384` and `HS512` cannot be verified at
//! the edge, and JWE cannot be decrypted there at all.
//!
//! # Claims
//!
//! `aud` the event id, `sub` the request id, `exp` the hard expiry, and for a
//! session `iat` as well. All three are registered claims, so the payload reads
//! the same to any JWT tool.
//!
//! # Domain separation
//!
//! A token must never validate as a session. That is enforced at the signature
//! rather than by a claim: each kind signs under its own key, derived from the
//! deployment secret by [`SigningKey`]. Presenting one kind as the other fails
//! `BadSignature`, so there is no check a caller can forget to make. A `typ`
//! claim would have to be read *after* verifying, and skipping it would admit
//! the wrong credential.

use aws_lc_rs::hmac;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// The registered claims carried by both credentials. `iat` is absent on an
/// admission token, which has no issue time to record.
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    aud: String,
    sub: String,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    iat: Option<u64>,
}

/// A credential kind. Each signs under its own derived key, so the two are
/// separated by the signature rather than by a claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Token,
    Session,
}

impl Kind {
    /// The derivation label for this kind's key. Distinct constant strings, so
    /// the two derived keys cannot coincide. Versioned because changing a
    /// label changes every credential it signs.
    const fn label(self) -> &'static [u8] {
        match self {
            Self::Token => b"vwr/jws/token/v1",
            Self::Session => b"vwr/jws/session/v1",
        }
    }
}

/// The per-deployment signing key, read from SSM at Lambda init. A newtype so
/// a raw byte slice is never mistaken for the key at a call site.
///
/// The deployment secret is never used to sign directly. Each credential kind
/// signs under `HMAC-SHA256(secret, label)`, so a credential of one kind cannot
/// validate as the other. The edge derives the session key the same way from
/// the same secret; it holds the secret, not a reduced key, because Terraform
/// has no HMAC function to derive one with at apply time.
pub struct SigningKey {
    token: [u8; 32],
    session: [u8; 32],
}

impl SigningKey {
    /// Derives both per-kind keys from the deployment secret.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        Self {
            token: derive(secret, Kind::Token),
            session: derive(secret, Kind::Session),
        }
    }

    const fn for_kind(&self, kind: Kind) -> &[u8; 32] {
        match kind {
            Kind::Token => &self.token,
            Kind::Session => &self.session,
        }
    }
}

/// `HMAC-SHA256(secret, label)`. One pass is enough for a full-entropy secret
/// and a fixed label; the labels are distinct constants, so the two outputs
/// cannot collide.
fn derive(secret: &[u8], kind: Kind) -> [u8; 32] {
    let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), kind.label());
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

/// Verification pins `HS256` and does its own expiry check against an injected
/// `now`, so the result does not depend on the host clock and stays testable.
/// `aud` carries the event id and is compared by the caller, which knows which
/// event it is serving.
fn validation() -> Validation {
    let mut v = Validation::new(Algorithm::HS256);
    v.validate_exp = false;
    v.validate_aud = false;
    v
}

/// A single-use admission token: proof a visitor reached the front of the
/// queue, carried on the URL and validated once by the authorizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionToken {
    pub event_id: String,
    pub request_id: String,
    /// Epoch-seconds hard expiry.
    pub expires_at: u64,
}

/// A per-event session credential set after the token validates. Signed over
/// different inputs (and a different kind tag) from the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub event_id: String,
    pub request_id: String,
    /// Epoch-seconds issue time.
    pub issued_at: u64,
    /// Epoch-seconds hard expiry (the cap; a sliding window re-issues).
    pub expires_at: u64,
}

/// Why a credential string did not validate. Deliberately coarse: a caller
/// learns only that the credential is not valid, never which field failed, so
/// nothing about the signed contents leaks through the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// The string is not `payload "." mac`, or either half is not base64url.
    #[error("malformed credential")]
    Malformed,
    /// The MAC did not match — wrong key, tampered payload, or wrong kind.
    #[error("bad signature")]
    BadSignature,
    /// The signature is valid but the credential has expired.
    #[error("expired")]
    Expired,
}

impl AdmissionToken {
    /// Signs the token as a compact JWS (`HS256`).
    #[must_use]
    pub fn sign(&self, key: &SigningKey) -> String {
        sign_claims(
            key,
            Kind::Token,
            &Claims {
                aud: self.event_id.clone(),
                sub: self.request_id.clone(),
                exp: self.expires_at,
                iat: None,
            },
        )
    }

    /// Verifies a signed token against `key` and checks it has not expired at
    /// `now` (epoch seconds).
    ///
    /// # Errors
    ///
    /// [`VerifyError::Malformed`] if the string is not well-formed;
    /// [`VerifyError::BadSignature`] if the MAC does not match under this kind;
    /// [`VerifyError::Expired`] if `now >= expires_at`.
    pub fn verify(token: &str, key: &SigningKey, now: u64) -> Result<Self, VerifyError> {
        let claims = verify_claims(key, Kind::Token, token)?;
        if now >= claims.exp {
            return Err(VerifyError::Expired);
        }
        Ok(Self {
            event_id: claims.aud,
            request_id: claims.sub,
            expires_at: claims.exp,
        })
    }
}

impl Session {
    /// Signs the session as a compact JWS (`HS256`).
    #[must_use]
    pub fn sign(&self, key: &SigningKey) -> String {
        sign_claims(
            key,
            Kind::Session,
            &Claims {
                aud: self.event_id.clone(),
                sub: self.request_id.clone(),
                exp: self.expires_at,
                iat: Some(self.issued_at),
            },
        )
    }

    /// Verifies a signed session against `key` and checks it has not expired at
    /// `now` (epoch seconds).
    ///
    /// # Errors
    ///
    /// [`VerifyError::Malformed`] if the string is not well-formed;
    /// [`VerifyError::BadSignature`] if the MAC does not match under this kind;
    /// [`VerifyError::Expired`] if `now >= expires_at`.
    pub fn verify(session: &str, key: &SigningKey, now: u64) -> Result<Self, VerifyError> {
        let claims = verify_claims(key, Kind::Session, session)?;
        if now >= claims.exp {
            return Err(VerifyError::Expired);
        }
        Ok(Self {
            event_id: claims.aud,
            request_id: claims.sub,
            issued_at: claims.iat.ok_or(VerifyError::Malformed)?,
            expires_at: claims.exp,
        })
    }
}

/// Signs `kind || payload` and returns `base64url(payload).base64url(mac)`.
fn sign_claims(key: &SigningKey, kind: Kind, claims: &Claims) -> String {
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        claims,
        &EncodingKey::from_secret(key.for_kind(kind)),
    )
    .unwrap_or_default()
}

/// Verifies a compact JWS and returns its claims. Errors are collapsed to
/// [`VerifyError`] so a caller learns only that the credential is invalid: a
/// wrong key, a tampered payload and a credential of the other kind are
/// indistinguishable.
fn verify_claims(key: &SigningKey, kind: Kind, credential: &str) -> Result<Claims, VerifyError> {
    use jsonwebtoken::errors::ErrorKind;

    jsonwebtoken::decode::<Claims>(
        credential,
        &DecodingKey::from_secret(key.for_kind(kind)),
        &validation(),
    )
    .map(|data| data.claims)
    .map_err(|e| match e.kind() {
        ErrorKind::InvalidSignature => VerifyError::BadSignature,
        _ => VerifyError::Malformed,
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;
    use proptest::prelude::*;

    fn key() -> SigningKey {
        SigningKey::new(b"a-32-byte-test-signing-key-value")
    }

    fn token() -> AdmissionToken {
        AdmissionToken {
            event_id: "smoke".to_owned(),
            request_id: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            expires_at: 2_000_000_000,
        }
    }

    fn session() -> Session {
        Session {
            event_id: "smoke".to_owned(),
            request_id: "018f3a2b-7c9d-7e1f-abcd-0123456789ab".to_owned(),
            issued_at: 1_000_000_000,
            expires_at: 2_000_000_000,
        }
    }

    #[test]
    fn token_round_trips() {
        let signed = token().sign(&key());
        let back = AdmissionToken::verify(&signed, &key(), 1_500_000_000).unwrap();
        assert_eq!(back, token());
    }

    #[test]
    fn session_round_trips() {
        let signed = session().sign(&key());
        let back = Session::verify(&signed, &key(), 1_500_000_000).unwrap();
        assert_eq!(back, session());
    }

    #[test]
    fn expired_token_is_rejected() {
        let signed = token().sign(&key());
        assert_eq!(
            AdmissionToken::verify(&signed, &key(), 2_000_000_000),
            Err(VerifyError::Expired)
        );
        // One second before expiry is still valid.
        assert!(AdmissionToken::verify(&signed, &key(), 1_999_999_999).is_ok());
    }

    #[test]
    fn expired_session_is_rejected() {
        let signed = session().sign(&key());
        assert_eq!(
            Session::verify(&signed, &key(), 2_000_000_001),
            Err(VerifyError::Expired)
        );
    }

    #[test]
    fn wrong_key_fails_signature() {
        let signed = token().sign(&key());
        let other = SigningKey::new(b"a-different-32-byte-signing-keyy");
        assert_eq!(
            AdmissionToken::verify(&signed, &other, 1_500_000_000),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn a_token_does_not_verify_as_a_session() {
        // Same fields, but the kind tag differs, so the MACs differ: a captured
        // admission token cannot be replayed as a session.
        let signed = token().sign(&key());
        assert_eq!(
            Session::verify(&signed, &key(), 1_500_000_000),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn a_session_does_not_verify_as_a_token() {
        let signed = session().sign(&key());
        assert_eq!(
            AdmissionToken::verify(&signed, &key(), 1_500_000_000),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn tampered_payload_fails() {
        let signed = token().sign(&key());
        let parts: Vec<&str> = signed.split('.').collect();
        // Flip a character in the claims segment; the signature covers
        // "header.payload", so it no longer matches.
        let mut chars: Vec<char> = parts[1].chars().collect();
        chars[0] = if chars[0] == 'e' { 'f' } else { 'e' };
        let tampered: String = chars.into_iter().collect();
        let forged = format!("{}.{tampered}.{}", parts[0], parts[2]);
        assert_eq!(
            AdmissionToken::verify(&forged, &key(), 1_500_000_000),
            Err(VerifyError::BadSignature)
        );
    }

    #[test]
    fn a_credential_is_a_readable_jwt() {
        use base64::Engine as _;

        // The point of the format: any JWT tool can read the claims. Decode
        // the payload segment and check the registered names are there.
        let signed = session().sign(&key());
        let parts: Vec<&str> = signed.split('.').collect();
        assert_eq!(parts.len(), 3, "compact serialization is three segments");

        let header = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[0])
                .unwrap(),
        )
        .unwrap();
        assert!(header.contains(r#""alg":"HS256""#), "header was {header}");

        let payload = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts[1])
                .unwrap(),
        )
        .unwrap();
        for claim in ["aud", "sub", "exp", "iat"] {
            assert!(
                payload.contains(&format!("\"{claim}\"")),
                "{claim} missing from {payload}"
            );
        }
    }

    #[test]
    fn malformed_strings_are_rejected() {
        let k = key();
        assert_eq!(
            AdmissionToken::verify("no-dot-here", &k, 0),
            Err(VerifyError::Malformed)
        );
        assert_eq!(
            AdmissionToken::verify("bad*.chars", &k, 0),
            Err(VerifyError::Malformed)
        );
        assert_eq!(
            AdmissionToken::verify("", &k, 0),
            Err(VerifyError::Malformed)
        );
    }

    proptest! {
        #[test]
        fn any_token_round_trips(
            event_id in "[a-z0-9-]{1,40}",
            request_id in "[a-z0-9-]{1,40}",
            expires_at in 1u64..u64::MAX,
        ) {
            let k = key();
            let t = AdmissionToken { event_id, request_id, expires_at };
            let signed = t.sign(&k);
            let back = AdmissionToken::verify(&signed, &k, expires_at - 1).unwrap();
            prop_assert_eq!(back, t);
        }

        /// Verification is a boundary: the string comes off a cookie or a URL
        /// and is attacker-chosen. Any input must be rejected, never panic.
        #[test]
        fn verify_never_panics_on_arbitrary_input(s in ".{0,120}") {
            let k = key();
            prop_assert!(Session::verify(&s, &k, 0).is_err());
            prop_assert!(AdmissionToken::verify(&s, &k, 0).is_err());
        }
    }
}
