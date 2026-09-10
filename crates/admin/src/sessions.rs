//! DynamoDB-backed login-transaction and session store for the admin OIDC flow
//! (ADR-0016). Both record kinds live in the existing `Tokens` table (PK
//! `request_id`) with a `DynamoDB` TTL attribute (`expires_at`, epoch seconds) so
//! they self-expire — no cleanup job.
//!
//! * Login transaction: PK `pkce#<csrf_state>`, holds the PKCE verifier + nonce
//!   for the ~10 min between `/admin/login` and `/admin/callback`. On Lambda the
//!   two requests can hit different execution environments, so this must not be
//!   in-process state.
//! * Session: PK `session#<id>`, holds the authenticated subject + email for
//!   ~8 h. The cookie carries only the opaque `<id>`.

use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use wr_common::expr::{oidc_session_key, pkce_transaction_key};

/// TTL for a pending login transaction (PKCE verifier + nonce).
const PKCE_TTL_SECS: u64 = 600;
/// TTL for an authenticated session.
const SESSION_TTL_SECS: u64 = 8 * 60 * 60;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("dynamodb: {0}")]
    Backend(String),
    #[error("system clock before epoch")]
    Clock,
}

/// A pending OIDC login transaction, keyed by the CSRF state token.
pub struct PendingLogin {
    pub pkce_verifier: String,
    pub nonce: String,
}

/// An authenticated admin session.
pub struct AdminSession {
    pub subject: String,
    pub email: String,
}

/// Session/PKCE store over the `Tokens` table.
pub struct SessionStore {
    client: Client,
    tokens_table: String,
}

fn now_secs() -> Result<u64, SessionError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| SessionError::Clock)
}

impl SessionStore {
    #[must_use]
    pub fn new(client: Client, tokens_table: String) -> Self {
        Self {
            client,
            tokens_table,
        }
    }

    /// Persists a pending login keyed by the CSRF state, with a short TTL.
    ///
    /// # Errors
    /// Returns [`SessionError`] if the clock is invalid or the write fails.
    pub async fn put_pending(
        &self,
        state: &str,
        pending: &PendingLogin,
    ) -> Result<(), SessionError> {
        let expires = now_secs()? + PKCE_TTL_SECS;
        self.client
            .put_item()
            .table_name(&self.tokens_table)
            .set_item(Some(pkce_transaction_key(state)))
            .item(
                "pkce_verifier",
                AttributeValue::S(pending.pkce_verifier.clone()),
            )
            .item("nonce", AttributeValue::S(pending.nonce.clone()))
            .item("expires_at", AttributeValue::N(expires.to_string()))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("put_pending: {e}")))?;
        Ok(())
    }

    /// Consumes (reads and deletes) a pending login by CSRF state. Returns
    /// `None` if absent or expired — callers treat that as an invalid callback.
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the delete fails.
    pub async fn take_pending(&self, state: &str) -> Result<Option<PendingLogin>, SessionError> {
        let out = self
            .client
            .delete_item()
            .table_name(&self.tokens_table)
            .set_key(Some(pkce_transaction_key(state)))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("take_pending: {e}")))?;

        let Some(item) = out.attributes() else {
            return Ok(None);
        };
        let pkce_verifier = item
            .get("pkce_verifier")
            .and_then(|v| v.as_s().ok())
            .cloned();
        let nonce = item.get("nonce").and_then(|v| v.as_s().ok()).cloned();
        match (pkce_verifier, nonce) {
            (Some(pkce_verifier), Some(nonce)) => Ok(Some(PendingLogin {
                pkce_verifier,
                nonce,
            })),
            _ => Ok(None),
        }
    }

    /// Creates a session and returns its opaque id (for the cookie).
    ///
    /// # Errors
    /// Returns [`SessionError`] if the clock is invalid or the write fails.
    pub async fn create_session(&self, session: &AdminSession) -> Result<String, SessionError> {
        let id = uuid_v4();
        let expires = now_secs()? + SESSION_TTL_SECS;
        self.client
            .put_item()
            .table_name(&self.tokens_table)
            .set_item(Some(oidc_session_key(&id)))
            .item("subject", AttributeValue::S(session.subject.clone()))
            .item("email", AttributeValue::S(session.email.clone()))
            .item("expires_at", AttributeValue::N(expires.to_string()))
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
    pub async fn load_session(&self, id: &str) -> Result<Option<AdminSession>, SessionError> {
        let out = self
            .client
            .get_item()
            .table_name(&self.tokens_table)
            .set_key(Some(oidc_session_key(id)))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("load_session: {e}")))?;

        let Some(item) = out.item() else {
            return Ok(None);
        };
        let expired = item
            .get("expires_at")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .is_some_and(|exp| now_secs().is_ok_and(|now| now >= exp));
        if expired {
            return Ok(None);
        }
        let subject = item.get("subject").and_then(|v| v.as_s().ok()).cloned();
        let email = item.get("email").and_then(|v| v.as_s().ok()).cloned();
        match (subject, email) {
            (Some(subject), Some(email)) => Ok(Some(AdminSession { subject, email })),
            _ => Ok(None),
        }
    }

    /// Deletes a session (logout).
    ///
    /// # Errors
    /// Returns [`SessionError::Backend`] if the delete fails.
    pub async fn delete_session(&self, id: &str) -> Result<(), SessionError> {
        self.client
            .delete_item()
            .table_name(&self.tokens_table)
            .set_key(Some(oidc_session_key(id)))
            .send()
            .await
            .map_err(|e| SessionError::Backend(format!("delete_session: {e}")))?;
        Ok(())
    }
}

/// A random UUIDv4-shaped opaque id from the aws-lc-rs RNG (already in the tree
/// via rustls). Avoids adding the `uuid` crate for one identifier.
fn uuid_v4() -> String {
    use aws_lc_rs::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0u8; 16];
    // `SystemRandom.fill` only errors if the OS RNG is unavailable, which on
    // Lambda does not happen; fall back to a time-seeded value rather than panic.
    if SystemRandom::new().fill(&mut bytes).is_err() {
        let seed = now_secs().unwrap_or(0).to_be_bytes();
        bytes[..8].copy_from_slice(&seed);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut h = String::with_capacity(32);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(h, "{b:02x}");
    }
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}
