//! DynamoDB-backed login-transaction and session store for the admin OIDC flow
//! (ADR-0016). Both record kinds live in the existing `Tokens` table (PK
//! `request_id`) and set the table's TTL attribute
//! ([`wr_common::expr::TOKENS_TTL_ATTR`], epoch seconds) so they self-expire —
//! no cleanup job. Reclamation is lazy, so a reader that must not accept a
//! stale row also compares the attribute to the current time.
//!
//! * Login transaction: PK `pkce#<csrf_state>`, holds the PKCE verifier + nonce
//!   for the ~10 min between `/admin/login` and `/admin/callback`. On Lambda the
//!   two requests can hit different execution environments, so this must not be
//!   in-process state.
//! * Session: PK `session#<id>`, holds the authenticated subject + email and the
//!   session's CSRF token (issue #243) for ~8 h. The cookie carries only the
//!   opaque `<id>`.

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{Key, TOKENS_TTL_ATTR};

use crate::arrival::ArrivalTime;

/// TTL for a pending login transaction (PKCE verifier + nonce).
const PKCE_TTL_SECS: u64 = 600;
/// TTL for an authenticated session.
const SESSION_TTL_SECS: u64 = 8 * 60 * 60;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("dynamodb: {0}")]
    Backend(String),
    /// The operating system RNG could not produce a session id or its CSRF
    /// token. Refusing the login is the only safe answer: a session id is a
    /// bearer credential, and one drawn from anything predictable can be
    /// guessed by whoever knows the fallback.
    #[error("no randomness available for a session id")]
    Rng,
}

/// A pending OIDC login transaction, keyed by the CSRF state token.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingLogin {
    pub pkce_verifier: String,
    pub nonce: String,
}

/// An authenticated admin session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminSession {
    pub subject: String,
    pub email: String,
    /// The token every state-changing form must echo (issue #243). Drawn once
    /// when the session is created and never sent anywhere but the operator's
    /// own dashboard, so another page -- a same-site one included -- cannot
    /// build a form the console accepts.
    pub csrf: String,
}

/// Session/PKCE store over the `Tokens` table.
pub struct SessionStore {
    client: Client,
    tokens_table: String,
}

/// Decodes a consumed pending-login row from its `DynamoDB` attributes.
///
/// Returns `None` when the row is absent, already past its
/// [`TOKENS_TTL_ATTR`] deadline (`DynamoDB` TTL deletion is not instant, so the
/// expiry is enforced on read), or missing the PKCE verifier / nonce. `now` is
/// the epoch second the request arrived at.
///
/// This is the AWS-free pure half of `take_pending`: it takes exactly the
/// shape `DeleteItem(AllOld)` returns so the expiry/parsing logic can be
/// unit-tested without a live `Client`.
fn pending_login_from(
    attributes: Option<&HashMap<String, AttributeValue>>,
    now: u64,
) -> Option<PendingLogin> {
    let item = attributes?;
    let expired = item
        .get(TOKENS_TTL_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .is_some_and(|exp| now >= exp);
    if expired {
        return None;
    }
    let pkce_verifier = item
        .get("pkce_verifier")
        .and_then(|v| v.as_s().ok())
        .cloned();
    let nonce = item.get("nonce").and_then(|v| v.as_s().ok()).cloned();
    match (pkce_verifier, nonce) {
        (Some(pkce_verifier), Some(nonce)) => Some(PendingLogin {
            pkce_verifier,
            nonce,
        }),
        _ => None,
    }
}

/// Decodes a session row, or `None` when it is absent, past its
/// [`TOKENS_TTL_ATTR`] deadline, or missing a field. A row without a CSRF token
/// is no session: its forms could not be told from a forged one.
///
/// The AWS-free half of `load_session`, as [`pending_login_from`] is of
/// `take_pending`.
fn session_from(
    attributes: Option<&HashMap<String, AttributeValue>>,
    now: u64,
) -> Option<AdminSession> {
    let item = attributes?;
    let expired = item
        .get(TOKENS_TTL_ATTR)
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .is_some_and(|exp| now >= exp);
    if expired {
        return None;
    }
    let text = |name: &str| item.get(name).and_then(|v| v.as_s().ok()).cloned();
    Some(AdminSession {
        subject: text("subject")?,
        email: text("email")?,
        csrf: text("csrf").filter(|token| !token.is_empty())?,
    })
}

impl SessionStore {
    #[must_use]
    pub fn new(client: Client, tokens_table: String) -> Self {
        Self {
            client,
            tokens_table,
        }
    }

    /// Persists a pending login keyed by the CSRF state, with a short TTL
    /// measured from `now`.
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the write fails.
    pub async fn put_pending(
        &self,
        state: &str,
        pending: &PendingLogin,
        now: ArrivalTime,
    ) -> Result<(), SessionError> {
        let expires = now.epoch_seconds().saturating_add(PKCE_TTL_SECS);
        self.client
            .put_item()
            .table_name(&self.tokens_table)
            .set_item(Some(Key::PkceTransaction { state }.build()))
            .item(
                "pkce_verifier",
                AttributeValue::S(pending.pkce_verifier.clone()),
            )
            .item("nonce", AttributeValue::S(pending.nonce.clone()))
            .item(TOKENS_TTL_ATTR, AttributeValue::N(expires.to_string()))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("put_pending: {e}")))?;
        Ok(())
    }

    /// Consumes (reads and deletes) a pending login by CSRF state. Returns
    /// `None` if absent or past its TTL — `DynamoDB` TTL deletion is not
    /// instant, so the expiry is also checked on read (the same compensation
    /// `load_session` uses). Callers treat `None` as an invalid callback.
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the delete fails.
    pub async fn take_pending(
        &self,
        state: &str,
        now: ArrivalTime,
    ) -> Result<Option<PendingLogin>, SessionError> {
        let out = self
            .client
            .delete_item()
            .table_name(&self.tokens_table)
            .set_key(Some(Key::PkceTransaction { state }.build()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("take_pending: {e}")))?;

        Ok(pending_login_from(out.attributes(), now.epoch_seconds()))
    }

    /// Creates a session expiring `SESSION_TTL_SECS` after `now`, with a fresh
    /// CSRF token (issue #243), and returns its opaque id (for the cookie).
    ///
    /// # Errors
    /// Returns [`SessionError::Rng`] if no session id or CSRF token can be
    /// drawn, or [`SessionError::Backend`] if the write fails.
    pub async fn create_session(
        &self,
        subject: &str,
        email: &str,
        now: ArrivalTime,
    ) -> Result<String, SessionError> {
        let id = session_id(now)?;
        let csrf = csrf_token()?;
        let expires = now.epoch_seconds().saturating_add(SESSION_TTL_SECS);
        self.client
            .put_item()
            .table_name(&self.tokens_table)
            .set_item(Some(Key::OidcSession { session_id: &id }.build()))
            .item("subject", AttributeValue::S(subject.to_owned()))
            .item("email", AttributeValue::S(email.to_owned()))
            .item("csrf", AttributeValue::S(csrf))
            .item(TOKENS_TTL_ATTR, AttributeValue::N(expires.to_string()))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("create_session: {e}")))?;
        Ok(id)
    }

    /// Loads a session by id. Returns `None` if absent or past its TTL
    /// (`DynamoDB` TTL deletion is not instant, so the expiry is also checked on
    /// read).
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the read fails.
    pub async fn load_session(
        &self,
        id: &str,
        now: ArrivalTime,
    ) -> Result<Option<AdminSession>, SessionError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.tokens_table)
            .set_key(Some(Key::OidcSession { session_id: id }.build()))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("load_session: {e}")))?;

        Ok(session_from(out.item(), now.epoch_seconds()))
    }

    /// Deletes a session (logout).
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the delete fails.
    pub async fn delete_session(&self, id: &str) -> Result<(), SessionError> {
        self.client
            .delete_item()
            .table_name(&self.tokens_table)
            .set_key(Some(Key::OidcSession { session_id: id }.build()))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("delete_session: {e}")))?;
        Ok(())
    }
}

/// A `UUIDv7` session id: the arrival instant in the leading 48 bits and 74
/// random bits below it — the same shape a visitor's `request_id` carries.
///
/// The timestamp is the request's own, not a fresh reading, so the id records
/// when the login it belongs to arrived.
///
/// `uuid` lays out the fields; the randomness is drawn here from the aws-lc-rs
/// RNG this deployment already uses everywhere else. `Uuid::new_v7` would draw
/// its own from `getrandom` and panic if that failed, where the RNG failing is
/// something this returns and the login reports.
///
/// # Errors
///
/// [`SessionError::Rng`] if the operating system RNG is unavailable. The
/// random bits are the unguessable part of the credential and the timestamp is
/// public knowledge, so there is nothing to fall back to: an id without
/// randomness is one an attacker can produce for themselves.
fn session_id(now: ArrivalTime) -> Result<String, SessionError> {
    use aws_lc_rs::rand::{SecureRandom, SystemRandom};

    let mut random_bytes = [0u8; 10];
    SystemRandom::new()
        .fill(&mut random_bytes)
        .map_err(|_| SessionError::Rng)?;
    Ok(
        uuid::Builder::from_unix_timestamp_millis(now.epoch_millis(), &random_bytes)
            .into_uuid()
            .to_string(),
    )
}

/// A session's CSRF token (issue #243): 256 random bits, base64url. Drawn from
/// the same RNG as the session id, and refused the same way when it fails.
fn csrf_token() -> Result<String, SessionError> {
    use aws_lc_rs::rand::{SecureRandom, SystemRandom};
    use base64::Engine as _;

    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| SessionError::Rng)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use super::*;

    // Fixed "current time" (2023-11-14 22:13:20 UTC) so the expiry tests are
    // independent of the wall clock and of DynamoDB's lazy TTL sweep.
    const NOW: u64 = 1_700_000_000;

    /// Builds a PKCE row shaped like the one `put_pending` writes.
    ///
    /// The attribute names are spelled out rather than taken from
    /// `TOKENS_TTL_ATTR` on purpose: these rows stand in for what is already
    /// stored in the table, so renaming the constant without migrating the data
    /// makes these tests fail instead of quietly passing while the reader looks
    /// at a name nothing writes.
    fn row(
        verifier: &str,
        nonce: &str,
        expires_at: Option<u64>,
    ) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::from([
            (
                "pkce_verifier".to_string(),
                AttributeValue::S(verifier.to_string()),
            ),
            ("nonce".to_string(), AttributeValue::S(nonce.to_string())),
        ]);
        if let Some(exp) = expires_at {
            item.insert("expires_at".to_string(), AttributeValue::N(exp.to_string()));
        }
        item
    }

    // ---- Regression: an expired row must NOT be returned as a valid login ----

    #[test]
    fn expired_row_is_rejected_even_when_verifier_and_nonce_are_present() {
        // A row past `expires_at` that DynamoDB has not yet purged is exactly
        // the bug: `DeleteItem(AllOld)` still returns it. The read-time check
        // must treat it as expired instead of handing the stale verifier and
        // nonce to the callback flow.
        let item = row("verifier", "nonce", Some(NOW - 1));
        assert_eq!(
            pending_login_from(Some(&item), NOW),
            None,
            "an expired PKCE transaction must not be returned as valid"
        );
    }

    #[test]
    fn row_at_the_expiry_boundary_is_rejected() {
        // `now >= exp` ⇒ expired: at the exact `expires_at` second the
        // transaction is gone, matching `load_session`.
        let item = row("verifier", "nonce", Some(NOW));
        assert_eq!(pending_login_from(Some(&item), NOW), None);
    }

    // ---- Happy / adjacent paths around the TTL window ----

    #[test]
    fn fresh_row_is_returned_with_verifier_and_nonce() {
        let item = row("verifier-123", "nonce-456", Some(NOW + PKCE_TTL_SECS));
        assert_eq!(
            pending_login_from(Some(&item), NOW),
            Some(PendingLogin {
                pkce_verifier: "verifier-123".to_string(),
                nonce: "nonce-456".to_string(),
            })
        );
    }

    #[test]
    fn row_just_before_expiry_is_still_valid() {
        let item = row("v", "n", Some(NOW + 1));
        assert!(pending_login_from(Some(&item), NOW).is_some());
    }

    #[test]
    fn the_full_pkce_ttl_window_stays_valid() {
        // `put_pending` writes `expires_at = now + PKCE_TTL_SECS`. The whole
        // window must remain usable; only at/after `expires_at` does it expire.
        let written_at = NOW;
        let item = row("v", "n", Some(written_at + PKCE_TTL_SECS));
        let last_valid_second = written_at + PKCE_TTL_SECS - 1;
        assert!(pending_login_from(Some(&item), last_valid_second).is_some());
        assert_eq!(
            pending_login_from(Some(&item), written_at + PKCE_TTL_SECS),
            None
        );
        assert!(pending_login_from(Some(&item), written_at + PKCE_TTL_SECS + 999).is_none());
    }

    // ---- Absent / sparse handles ----

    #[test]
    fn absent_attributes_yield_none() {
        // `DeleteItem` returns no attributes when the row never existed (or was
        // already consumed by a prior callback) — an invalid state.
        assert_eq!(pending_login_from(None, NOW), None);
    }

    #[test]
    fn row_missing_expires_at_is_treated_as_unexpired() {
        // Matches `load_session`: a missing TTL attribute does not reject the
        // row — only a present, numeric, past `expires_at` expires it.
        let item = row("v", "n", None);
        assert_eq!(
            pending_login_from(Some(&item), NOW),
            Some(PendingLogin {
                pkce_verifier: "v".to_string(),
                nonce: "n".to_string(),
            })
        );
    }

    #[test]
    fn row_with_non_numeric_expires_at_is_treated_as_unexpired() {
        // A malformed `expires_at` can't be parsed, so the expiry predicate is
        // not satisfied — defensive, mirroring `load_session`'s `.ok()` chain.
        let mut item = row("v", "n", None);
        item.insert(
            "expires_at".to_string(),
            AttributeValue::N("not-a-number".to_string()),
        );
        assert_eq!(
            pending_login_from(Some(&item), NOW),
            Some(PendingLogin {
                pkce_verifier: "v".to_string(),
                nonce: "n".to_string(),
            })
        );
    }

    // ---- Malformed payloads ----

    #[test]
    fn row_missing_pkce_verifier_yields_none() {
        let mut item = HashMap::new();
        item.insert("nonce".to_string(), AttributeValue::S("n".to_string()));
        item.insert(
            "expires_at".to_string(),
            AttributeValue::N((NOW + 60).to_string()),
        );
        assert_eq!(pending_login_from(Some(&item), NOW), None);
    }

    #[test]
    fn row_missing_nonce_yields_none() {
        let mut item = HashMap::new();
        item.insert(
            "pkce_verifier".to_string(),
            AttributeValue::S("v".to_string()),
        );
        item.insert(
            "expires_at".to_string(),
            AttributeValue::N((NOW + 60).to_string()),
        );
        assert_eq!(pending_login_from(Some(&item), NOW), None);
    }

    #[test]
    fn row_with_wrong_value_types_yields_none() {
        // Verifier/nonce stored as numbers can't satisfy `.as_s()`.
        let item = HashMap::from([
            (
                "pkce_verifier".to_string(),
                AttributeValue::N("123".to_string()),
            ),
            ("nonce".to_string(), AttributeValue::N("456".to_string())),
            (
                "expires_at".to_string(),
                AttributeValue::N((NOW + 60).to_string()),
            ),
        ]);
        assert_eq!(pending_login_from(Some(&item), NOW), None);
    }

    #[test]
    fn empty_row_yields_none() {
        let item: HashMap<String, AttributeValue> = HashMap::new();
        assert_eq!(pending_login_from(Some(&item), NOW), None);
    }

    #[test]
    fn extra_attributes_are_ignored() {
        let mut item = row("v", "n", Some(NOW + 60));
        item.insert(
            "future_field".to_string(),
            AttributeValue::S("z".to_string()),
        );
        assert_eq!(
            pending_login_from(Some(&item), NOW),
            Some(PendingLogin {
                pkce_verifier: "v".to_string(),
                nonce: "n".to_string(),
            })
        );
    }

    // ---- The instant the expiry is measured against ----

    #[test]
    fn expiry_is_measured_against_the_arrival_stamp() {
        // The store reads no clock of its own: the second a row is compared
        // against is the one the request arrived at, so a login cannot expire
        // between the session check and the action it authorises.
        let arrival = ArrivalTime::for_test_millis(1_700_000_000_000);
        assert_eq!(arrival.epoch_seconds(), NOW);

        let expired = row("v", "n", Some(NOW));
        assert_eq!(
            pending_login_from(Some(&expired), arrival.epoch_seconds()),
            None
        );

        let fresh = row("v", "n", Some(NOW + 1));
        assert!(pending_login_from(Some(&fresh), arrival.epoch_seconds()).is_some());
    }

    // ---- Session ids ----

    #[test]
    fn a_session_id_is_a_uuidv7() {
        let id = session_id(ArrivalTime::for_test_millis(1_700_000_000_000)).unwrap();
        assert_eq!(id.len(), 36);
        assert_eq!(
            id.split('-').map(str::len).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12]
        );
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        // The two nibbles the shape fixes: version 7, RFC 9562 variant. A
        // visitor's request_id is validated against exactly these.
        assert_eq!(&id[14..15], "7");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
    }

    #[test]
    fn a_session_id_carries_the_arrival_instant() {
        // The leading 48 bits are the request's own millisecond, not a fresh
        // reading, so the id records when the login that minted it arrived.
        let at = ArrivalTime::for_test_millis(1_700_000_000_000);
        let id = session_id(at).unwrap();
        let leading: String = id.chars().filter(|c| *c != '-').take(12).collect();
        assert_eq!(
            u64::from_str_radix(&leading, 16).unwrap(),
            at.epoch_millis()
        );
    }

    #[test]
    fn session_ids_from_one_instant_still_differ() {
        // Two logins inside the same millisecond share every timestamp bit, so
        // the 74 random bits are the whole of what makes the credential
        // unguessable.
        let at = ArrivalTime::for_test_millis(1_700_000_000_000);
        assert_ne!(session_id(at).unwrap(), session_id(at).unwrap());
    }

    #[test]
    fn session_ids_sort_by_the_instant_they_were_minted() {
        let earlier = session_id(ArrivalTime::for_test_millis(1_700_000_000_000)).unwrap();
        let later = session_id(ArrivalTime::for_test_millis(1_700_000_000_001)).unwrap();
        assert!(
            earlier < later,
            "a UUIDv7 orders lexicographically by time: {earlier} then {later}"
        );
    }

    // ---- Session rows and their CSRF token (issue #243) ----

    /// A session row shaped like the one `create_session` writes.
    fn session_row(csrf: Option<&str>, expires_at: u64) -> HashMap<String, AttributeValue> {
        let mut item = HashMap::from([
            ("subject".to_string(), AttributeValue::S("sub".to_string())),
            (
                "email".to_string(),
                AttributeValue::S("op@example.com".to_string()),
            ),
            (
                "expires_at".to_string(),
                AttributeValue::N(expires_at.to_string()),
            ),
        ]);
        if let Some(csrf) = csrf {
            item.insert("csrf".to_string(), AttributeValue::S(csrf.to_string()));
        }
        item
    }

    #[test]
    fn a_session_row_carries_its_csrf_token() {
        let item = session_row(Some("tok"), NOW + 60);
        assert_eq!(
            session_from(Some(&item), NOW),
            Some(AdminSession {
                subject: "sub".to_string(),
                email: "op@example.com".to_string(),
                csrf: "tok".to_string(),
            })
        );
    }

    #[test]
    fn a_session_row_without_a_csrf_token_is_no_session() {
        // Otherwise an empty stored token would match an empty submitted one.
        assert_eq!(session_from(Some(&session_row(None, NOW + 60)), NOW), None);
        assert_eq!(
            session_from(Some(&session_row(Some(""), NOW + 60)), NOW),
            None
        );
    }

    #[test]
    fn an_expired_session_row_is_rejected() {
        assert_eq!(
            session_from(Some(&session_row(Some("tok"), NOW)), NOW),
            None
        );
        assert_eq!(session_from(None, NOW), None);
    }

    #[test]
    fn csrf_tokens_are_256_bits_and_differ() {
        let (a, b) = (csrf_token().unwrap(), csrf_token().unwrap());
        assert_eq!(a.len(), 43);
        assert_ne!(a, b);
    }
}
