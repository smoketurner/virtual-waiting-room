//! Signed admission tokens and session cookies for the authorizer (ADR-0011).
//!
//! Both credentials are minted from one per-deployment key with
//! `HMAC-SHA256`, but over domain-separated messages so neither validates as
//! the other: the message begins with a one-byte kind tag
//! (`0x01` token, `0x02` session) that differs before any field, so a token's
//! bytes can never reproduce a session's MAC or vice versa. The key is held in
//! Secrets Manager and read at Lambda init; its compromise mints admission for
//! every event in the deployment.
//!
//! # Wire encoding
//!
//! A credential serializes to `base64url(payload) "." base64url(mac)` with no
//! padding. The MAC covers the kind tag followed by the canonical field
//! encoding below — not the base64url text — so a change in text encoding
//! cannot alter what was signed. All integers are big-endian.
//!
//! - **Admission token** (kind `0x01`): `event_id` and `request_id` as
//!   length-prefixed UTF-8 (`u16` length then bytes), then `expires_at` as an
//!   8-byte epoch-seconds `u64`.
//! - **Session cookie** (kind `0x02`): `event_id`, `request_id`,
//!   `issued_at` (`u64`), `expires_at` (`u64`). The hard cap is `expires_at`;
//!   a sliding window re-issues with a later `expires_at` on activity.

use aws_lc_rs::hmac;

/// A credential kind tag, the first signed byte so the two credentials are
/// domain-separated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Token,
    Session,
}

impl Kind {
    /// The one-byte tag that leads the signed message.
    const fn tag(self) -> u8 {
        match self {
            Self::Token => 0x01,
            Self::Session => 0x02,
        }
    }
}

/// The per-deployment signing key (Secrets Manager). A newtype so a raw byte
/// slice is never mistaken for the key at a call site.
pub struct SigningKey(hmac::Key);

impl SigningKey {
    /// Builds a signing key from the raw secret bytes.
    #[must_use]
    pub fn new(secret: &[u8]) -> Self {
        Self(hmac::Key::new(hmac::HMAC_SHA256, secret))
    }
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
    /// Serializes and signs the token into `base64url(payload).base64url(mac)`.
    #[must_use]
    pub fn sign(&self, key: &SigningKey) -> String {
        let payload = encode_token(self);
        sign_payload(key, Kind::Token, &payload)
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
        let payload = verify_payload(key, Kind::Token, token)?;
        let token = decode_token(&payload).ok_or(VerifyError::Malformed)?;
        if now >= token.expires_at {
            return Err(VerifyError::Expired);
        }
        Ok(token)
    }
}

impl Session {
    /// Serializes and signs the session into `base64url(payload).base64url(mac)`.
    #[must_use]
    pub fn sign(&self, key: &SigningKey) -> String {
        let payload = encode_session(self);
        sign_payload(key, Kind::Session, &payload)
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
        let payload = verify_payload(key, Kind::Session, session)?;
        let session = decode_session(&payload).ok_or(VerifyError::Malformed)?;
        if now >= session.expires_at {
            return Err(VerifyError::Expired);
        }
        Ok(session)
    }
}

/// Signs `kind || payload` and returns `base64url(payload).base64url(mac)`.
fn sign_payload(key: &SigningKey, kind: Kind, payload: &[u8]) -> String {
    let mut message = Vec::with_capacity(payload.len() + 1);
    message.push(kind.tag());
    message.extend_from_slice(payload);
    let mac = hmac::sign(&key.0, &message);
    format!(
        "{}.{}",
        base64url_encode(payload),
        base64url_encode(mac.as_ref())
    )
}

/// Splits `payload.mac`, verifies the MAC over `kind || payload` in constant
/// time, and returns the raw payload bytes on success.
fn verify_payload(key: &SigningKey, kind: Kind, credential: &str) -> Result<Vec<u8>, VerifyError> {
    let (payload_b64, mac_b64) = credential.split_once('.').ok_or(VerifyError::Malformed)?;
    let payload = base64url_decode(payload_b64).ok_or(VerifyError::Malformed)?;
    let mac = base64url_decode(mac_b64).ok_or(VerifyError::Malformed)?;

    let mut message = Vec::with_capacity(payload.len() + 1);
    message.push(kind.tag());
    message.extend_from_slice(&payload);
    // hmac::verify is constant-time over the tag length, so a wrong kind, wrong
    // key, or tampered payload are indistinguishable and non-timing-leaking.
    hmac::verify(&key.0, &message, &mac).map_err(|_| VerifyError::BadSignature)?;
    Ok(payload)
}

/// `event_id ‖ request_id ‖ expires_at`.
fn encode_token(t: &AdmissionToken) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, &t.event_id);
    put_str(&mut out, &t.request_id);
    out.extend_from_slice(&t.expires_at.to_be_bytes());
    out
}

fn decode_token(payload: &[u8]) -> Option<AdmissionToken> {
    let mut cursor = Cursor::new(payload);
    let event_id = cursor.take_str()?;
    let request_id = cursor.take_str()?;
    let expires_at = cursor.take_u64()?;
    if !cursor.at_end() {
        return None;
    }
    Some(AdmissionToken {
        event_id,
        request_id,
        expires_at,
    })
}

/// `event_id ‖ request_id ‖ issued_at ‖ expires_at`.
fn encode_session(s: &Session) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, &s.event_id);
    put_str(&mut out, &s.request_id);
    out.extend_from_slice(&s.issued_at.to_be_bytes());
    out.extend_from_slice(&s.expires_at.to_be_bytes());
    out
}

fn decode_session(payload: &[u8]) -> Option<Session> {
    let mut cursor = Cursor::new(payload);
    let event_id = cursor.take_str()?;
    let request_id = cursor.take_str()?;
    let issued_at = cursor.take_u64()?;
    let expires_at = cursor.take_u64()?;
    if !cursor.at_end() {
        return None;
    }
    Some(Session {
        event_id,
        request_id,
        issued_at,
        expires_at,
    })
}

/// Appends a `u16`-length-prefixed UTF-8 string. A value longer than
/// `u16::MAX` is truncated at the prefix, which fails to round-trip and so is
/// rejected on decode — event and request ids are far shorter.
fn put_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&bytes[..usize::from(len)]);
}

/// A forward-only reader over a signed payload.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn take_u64(&mut self) -> Option<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().ok()?;
        Some(u64::from_be_bytes(bytes))
    }

    fn take_str(&mut self) -> Option<String> {
        let len_bytes: [u8; 2] = self.take(2)?.try_into().ok()?;
        let len = usize::from(u16::from_be_bytes(len_bytes));
        let bytes = self.take(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    fn at_end(&self) -> bool {
        self.pos == self.buf.len()
    }
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// base64url without padding.
fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        out.push(ALPHABET[b0 >> 2] as char);
        out.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[b2 & 0x3f] as char);
        }
    }
    out
}

/// base64url (no padding) decode. Returns `None` on any invalid character or a
/// length that cannot correspond to a byte string.
fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            // The byte being emitted is the low 8 bits of the window; the
            // groups above `bits` belong to the next byte. Take the low byte
            // directly so no truncating cast is needed.
            out.push((acc >> bits).to_le_bytes()[0]);
        }
    }
    // A valid encoding leaves only zero leftover bits; any set leftover bit is
    // a corrupt tail.
    if acc & ((1 << bits) - 1) != 0 {
        return None;
    }
    Some(out)
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
        // admission token cannot be replayed as a session (ADR-0011).
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
        let (payload, mac) = signed.split_once('.').unwrap();
        // Flip a payload character; the MAC no longer matches.
        let mut chars: Vec<char> = payload.chars().collect();
        chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
        let tampered: String = chars.into_iter().collect();
        let forged = format!("{tampered}.{mac}");
        assert_eq!(
            AdmissionToken::verify(&forged, &key(), 1_500_000_000),
            Err(VerifyError::BadSignature)
        );
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

    #[test]
    fn base64url_round_trips_arbitrary_bytes() {
        for len in 0..40usize {
            let bytes: Vec<u8> = (0..len)
                .map(|i| ((i * 7 + 3) % 256).to_le_bytes()[0])
                .collect();
            let encoded = base64url_encode(&bytes);
            assert_eq!(base64url_decode(&encoded).unwrap(), bytes);
        }
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

        #[test]
        fn base64url_decode_never_panics(s in ".{0,64}") {
            let _ = base64url_decode(&s);
        }
    }
}
