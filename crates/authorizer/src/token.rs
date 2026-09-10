//! `/generate_token`: exchange a served queue position for a single-use
//! admission token (DESIGN §8, F3.3).
//!
//! A visitor is eligible when the event is serving their position
//! (`serving_counter >= position`). The token is short-lived and single-use;
//! the handler records its issuance in the `Tokens` table so a second request
//! for the same visitor is rejected.

use wr_crypto::{AdmissionToken, SigningKey};

/// Why a token could not be issued. Coarse on purpose: a caller learns only
/// that it is not yet eligible or already has one, never internal detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// The visitor's position has not been reached yet.
    #[error("position not yet served")]
    NotServed,
}

/// Mints a single-use admission token for a served visitor.
///
/// `serving_counter` is the event's current admission cursor and `position` is
/// the visitor's resolved queue position. `ttl_secs` is the token's short
/// lifetime; `now` is epoch seconds. Single-use enforcement is the caller's
/// (a conditional write to `Tokens`); this function only gates on eligibility
/// and mints.
///
/// # Errors
///
/// [`TokenError::NotServed`] if `serving_counter < position`.
pub fn generate_token(
    event_id: &str,
    request_id: &str,
    serving_counter: u64,
    position: u64,
    ttl_secs: u64,
    now: u64,
    key: &SigningKey,
) -> Result<String, TokenError> {
    if serving_counter < position {
        return Err(TokenError::NotServed);
    }
    let token = AdmissionToken {
        event_id: event_id.to_owned(),
        request_id: request_id.to_owned(),
        expires_at: now.saturating_add(ttl_secs),
    };
    Ok(token.sign(key))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;
    use wr_crypto::VerifyError;

    fn key() -> SigningKey {
        SigningKey::new(b"a-32-byte-test-signing-key-value")
    }

    #[test]
    fn served_position_mints_a_verifiable_token() {
        let signed = generate_token("smoke", "r1", 100, 50, 60, 1000, &key()).unwrap();
        let token = AdmissionToken::verify(&signed, &key(), 1000).unwrap();
        assert_eq!(token.event_id, "smoke");
        assert_eq!(token.request_id, "r1");
        assert_eq!(token.expires_at, 1060);
    }

    #[test]
    fn position_at_the_cursor_is_served() {
        // serving_counter == position is eligible (the cursor has reached it).
        assert!(generate_token("smoke", "r1", 50, 50, 60, 1000, &key()).is_ok());
    }

    #[test]
    fn unserved_position_is_rejected() {
        assert_eq!(
            generate_token("smoke", "r1", 49, 50, 60, 1000, &key()),
            Err(TokenError::NotServed)
        );
    }

    #[test]
    fn minted_token_expires_after_ttl() {
        let signed = generate_token("smoke", "r1", 100, 50, 60, 1000, &key()).unwrap();
        assert_eq!(
            AdmissionToken::verify(&signed, &key(), 1060),
            Err(VerifyError::Expired)
        );
    }
}
