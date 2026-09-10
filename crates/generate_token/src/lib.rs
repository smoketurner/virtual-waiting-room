//! Admission check and `CloudFront` signed-cookie minting for the
//! `generate_token` Lambda.
//!
//! A visitor whose queue position has been reached exchanges their request id
//! for a set of signed cookies. `CloudFront` verifies those cookies itself at
//! the edge on every later request to the protected origin, so an admitted
//! visitor reaches the origin with no compute in the request path, and an
//! un-admitted one is refused before the origin is touched.
//!
//! This module is AWS-free apart from the signing key: [`decide`] is a pure
//! function from the queue state to a [`Grant`], and the handler in `main.rs`
//! fetches the items, calls it, and writes the effects.
//!
//! # Cookie encoding
//!
//! The encoding is fixed by `CloudFront` and is a compatibility contract with
//! the edge, not an implementation detail:
//!
//! - A **custom** policy is used, so the cookie set is `CloudFront-Policy`,
//!   `CloudFront-Signature`, and `CloudFront-Key-Pair-Id`. `CloudFront-Expires`
//!   belongs to canned policies only and is never sent.
//! - The policy JSON carries no whitespace and one `Statement` entry.
//! - Both the policy and the signature are base64 with `+/=` replaced by `-~_`.
//! - The signature is RSA PKCS#1 v1.5 over SHA-256, which requires the
//!   `CloudFront-Hash-Algorithm=SHA256` cookie; omitting it makes `CloudFront`
//!   assume SHA-1 and reject every signature.
//! - Cookies are set with `Path=/` and no `Domain`, so they default to the
//!   distribution host. A narrower `Path` means the browser never sends them
//!   back on the protected request and every visitor sees a 403.

use std::future::Future;

use aws_lc_rs::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::Serialize;
use wr_common::{
    AdmissionControl, Counters, Phase, PositionStatus, PreQueueItem, ResolveError, ResolvedPosition,
};

pub mod dynamo;

/// How long a minted cookie set stays valid when the environment does not say.
/// Long enough to finish a purchase, short enough that a leaked cookie set is
/// not a standing bypass.
pub const DEFAULT_COOKIE_TTL_SECS: u64 = 3600;

/// The policy `Resource` the cookies are minted for. A wildcard, because the
/// key group is bound to one distribution's behaviour: only that distribution
/// honours these cookies, so the meaningful limits are the expiry and the key,
/// not the URL pattern. Using the distribution's own domain here would force
/// the edge module to feed its host back into the core module that mints the
/// cookie, which is a dependency cycle between the two.
pub const RESOURCE_WILDCARD: &str = "https://*";

/// A store failure worth retrying.
#[derive(Debug, thiserror::Error)]
#[error("generate_token store error: {0}")]
pub struct StoreError(pub String);

/// The persistence port. A trait seam so the logic runs without AWS.
pub trait Store {
    /// Reads the event's `Counters` item.
    fn load_counters(
        &self,
        event_id: &str,
    ) -> impl Future<Output = Result<Option<Counters>, StoreError>> + Send;

    /// Reads a visitor's `PreQueue` registration, if they have one.
    fn load_prequeue(
        &self,
        request_id: &str,
    ) -> impl Future<Output = Result<Option<PreQueueItem>, StoreError>> + Send;

    /// Reads a live joiner's `Positions` row: the claimed position and its
    /// current status.
    fn load_position(
        &self,
        request_id: &str,
    ) -> impl Future<Output = Result<Option<(u64, PositionStatus)>, StoreError>> + Send;

    /// `ADD arrivals#<shard> :one` — records that this visitor showed up, which
    /// is what the controller measures its no-show rate against.
    fn record_arrival(
        &self,
        event_id: &str,
        shard: usize,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}

/// Why a visitor is not being admitted right now.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Denied {
    /// No registration exists for this request id.
    #[error("not registered")]
    NotRegistered,
    /// The event is not admitting: not active, or held by the operator.
    #[error("event is not admitting")]
    NotAdmitting,
    /// The event has not been sealed, so no position exists yet.
    #[error("event not yet open")]
    NotSealed,
    /// The visitor's turn has not arrived.
    #[error("still queued at {position}, now serving {serving}")]
    StillQueued { position: u64, serving: u64 },
    /// The position is no longer a live claim: expired by the controller,
    /// already used, or abandoned. Permanent, unlike [`Denied::StillQueued`].
    #[error("position is no longer valid")]
    Spent,
    /// The stored registration is corrupt.
    #[error("corrupt registration")]
    Corrupt,
}

/// An admitted visitor: the position that was reached and the arrival shard to
/// count them under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub position: u64,
    pub arrival_shard: usize,
}

/// Decides whether the visitor may be admitted.
///
/// Both gates are checked, in this order: the event must be actively admitting,
/// and the visitor's position must have been reached. A held event admits
/// nobody even if their position was reached before the hold, which is what
/// makes the operator's pause a real stop rather than a display state.
///
/// # Errors
///
/// [`Denied`] describing which gate refused.
pub fn decide(
    counters: &Counters,
    request_id: &str,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<(u64, PositionStatus)>,
) -> Result<Grant, Denied> {
    match counters.admission_control {
        AdmissionControl::Open => {}
        AdmissionControl::Paused | AdmissionControl::FailOpen => return Err(Denied::NotAdmitting),
    }
    if counters.phase != Phase::Active {
        return Err(Denied::NotAdmitting);
    }

    let position = resolve_position(counters, prequeue, position_row)?;

    if position >= counters.serving_counter {
        return Err(Denied::StillQueued {
            position,
            serving: counters.serving_counter,
        });
    }

    Ok(Grant {
        position,
        arrival_shard: wr_common::shard_for(request_id.as_bytes()),
    })
}

/// The visitor's position, from whichever path registered them. A live-join
/// row wins over a pre-queue row: it is the position actually claimed from the
/// counter, whereas a pre-queue row that raced the seal only reports the base
/// the live sequence counts from.
fn resolve_position(
    counters: &Counters,
    prequeue: Option<&PreQueueItem>,
    position_row: Option<(u64, PositionStatus)>,
) -> Result<u64, Denied> {
    if let Some((position, status)) = position_row {
        return match status {
            PositionStatus::Issued => Ok(position),
            // Expired by the controller, already used, or given up: none of the
            // three is a live claim on a position, and all three are permanent,
            // so the visitor is told to stop rather than to keep polling.
            PositionStatus::Expired | PositionStatus::Completed | PositionStatus::Abandoned => {
                Err(Denied::Spent)
            }
        };
    }

    let Some(row) = prequeue else {
        return Err(Denied::NotRegistered);
    };

    match counters.resolve_prequeue(row) {
        Ok(ResolvedPosition::PreQueue(position)) => Ok(position),
        // Raced the seal, so a live-join row should exist; without one there is
        // no claimed position to admit against.
        Ok(ResolvedPosition::LiveJoin { .. }) => Err(Denied::NotRegistered),
        Err(ResolveError::NotSealed) => Err(Denied::NotSealed),
        Err(ResolveError::BadShard) => Err(Denied::Corrupt),
    }
}

/// The `CloudFront` custom policy statement, serialized with no whitespace.
#[derive(Serialize)]
struct Policy {
    #[serde(rename = "Statement")]
    statement: [Statement; 1],
}

#[derive(Serialize)]
struct Statement {
    #[serde(rename = "Resource")]
    resource: String,
    #[serde(rename = "Condition")]
    condition: Condition,
}

#[derive(Serialize)]
struct Condition {
    #[serde(rename = "DateLessThan")]
    date_less_than: EpochTime,
}

#[derive(Serialize)]
struct EpochTime {
    #[serde(rename = "AWS:EpochTime")]
    epoch: u64,
}

/// Builds the policy JSON granting access to `resource` until `expires_at`.
///
/// # Panics
///
/// Never in practice: the value is a fixed struct of a string and an integer,
/// which `serde_json` cannot fail to serialize.
#[must_use]
pub fn policy_json(resource: &str, expires_at: u64) -> String {
    let policy = Policy {
        statement: [Statement {
            resource: resource.to_owned(),
            condition: Condition {
                date_less_than: EpochTime { epoch: expires_at },
            },
        }],
    };
    serde_json::to_string(&policy).unwrap_or_else(|_| {
        // Unreachable for this shape; fall back to a hand-built equivalent
        // rather than panicking on the admission path.
        format!(
            r#"{{"Statement":[{{"Resource":"{resource}","Condition":{{"DateLessThan":{{"AWS:EpochTime":{expires_at}}}}}}}]}}"#
        )
    })
}

/// `CloudFront`'s base64 variant: standard base64 with the three characters
/// that are unsafe in a cookie value replaced.
#[must_use]
pub fn cf_base64(bytes: &[u8]) -> String {
    let mut out = B64.encode(bytes);
    // CloudFront's documented substitution, applied in place.
    out = out.replace('+', "-").replace('=', "_").replace('/', "~");
    out
}

/// A minted cookie set, ready to become `Set-Cookie` headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCookies {
    pub policy: String,
    pub signature: String,
    pub key_pair_id: String,
    pub expires_at: u64,
}

impl SignedCookies {
    /// The `Set-Cookie` header values, in the order `CloudFront` documents.
    ///
    /// `Path=/` with no `Domain` is required, not stylistic: a narrower path
    /// means the browser never sends the cookies back on the protected request.
    #[must_use]
    pub fn set_cookie_headers(&self, ttl_secs: u64) -> Vec<String> {
        let attrs = format!("Path=/; Max-Age={ttl_secs}; Secure; HttpOnly; SameSite=Lax");
        vec![
            format!("CloudFront-Policy={}; {attrs}", self.policy),
            format!("CloudFront-Signature={}; {attrs}", self.signature),
            format!("CloudFront-Key-Pair-Id={}; {attrs}", self.key_pair_id),
            // Selects SHA-256; without it CloudFront assumes SHA-1 and every
            // signature fails.
            format!("CloudFront-Hash-Algorithm=SHA256; {attrs}"),
        ]
    }
}

/// A signing failure. The key is loaded once at cold start, so this is a
/// configuration fault rather than a per-request one.
#[derive(Debug, thiserror::Error)]
pub enum SignError {
    #[error("signing key is not a valid PKCS#8 RSA key")]
    BadKey,
    #[error("signature generation failed")]
    Sign,
}

/// The RSA key that signs policies, loaded once at cold start.
pub struct Signer {
    key: RsaKeyPair,
    key_pair_id: String,
}

impl Signer {
    /// Loads a PKCS#8 DER private key.
    ///
    /// # Errors
    ///
    /// [`SignError::BadKey`] if the bytes are not a PKCS#8 RSA key.
    pub fn from_pkcs8_der(der: &[u8], key_pair_id: String) -> Result<Self, SignError> {
        let key = RsaKeyPair::from_pkcs8(der).map_err(|_| SignError::BadKey)?;
        Ok(Self { key, key_pair_id })
    }

    /// Loads a PKCS#8 PEM private key by stripping the armour and decoding the
    /// base64 body.
    ///
    /// # Errors
    ///
    /// [`SignError::BadKey`] if the armour is missing or the body is not a
    /// PKCS#8 RSA key.
    pub fn from_pkcs8_pem(pem: &str, key_pair_id: String) -> Result<Self, SignError> {
        let der = pem_body(pem).ok_or(SignError::BadKey)?;
        Self::from_pkcs8_der(&der, key_pair_id)
    }

    /// Signs a policy for `resource`, valid until `expires_at`.
    ///
    /// # Errors
    ///
    /// [`SignError::Sign`] if the RSA operation fails.
    pub fn sign(&self, resource: &str, expires_at: u64) -> Result<SignedCookies, SignError> {
        let policy = policy_json(resource, expires_at);
        // The buffer must be exactly the modulus length or the sign call fails.
        let mut signature = vec![0u8; self.key.public_modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &aws_lc_rs::rand::SystemRandom::new(),
                policy.as_bytes(),
                &mut signature,
            )
            .map_err(|_| SignError::Sign)?;

        Ok(SignedCookies {
            policy: cf_base64(policy.as_bytes()),
            signature: cf_base64(&signature),
            key_pair_id: self.key_pair_id.clone(),
            expires_at,
        })
    }
}

/// Decodes the base64 body of a PEM document, ignoring the armour lines.
fn pem_body(pem: &str) -> Option<Vec<u8>> {
    let mut body = String::new();
    let mut inside = false;
    for line in pem.lines() {
        let line = line.trim();
        if line.starts_with("-----BEGIN") {
            inside = true;
        } else if line.starts_with("-----END") {
            break;
        } else if inside {
            body.push_str(line);
        }
    }
    if body.is_empty() {
        return None;
    }
    B64.decode(body).ok()
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

    use aws_lc_rs::encoding::AsDer;
    use wr_common::{SHARDS, SealedOffsets};

    use super::*;

    fn counters(serving: u64) -> Counters {
        let counts = [2u64; SHARDS];
        let sealed = SealedOffsets::seal(counts).unwrap();
        let mut offsets = [0u64; SHARDS];
        for (s, slot) in offsets.iter_mut().enumerate() {
            *slot = sealed.offset(s);
        }
        Counters {
            event_id: "evt".to_owned(),
            phase: Phase::Active,
            queue_counter: sealed.participant_count(),
            serving_counter: serving,
            prequeue_counts: counts,
            arrivals: [0; SHARDS],
            shuffle_seed: Some([9u8; 32]),
            participant_count: Some(sealed.participant_count()),
            prequeue_offsets: Some(offsets),
            message: None,
            admission_control: AdmissionControl::Open,
        }
    }

    const REQ: &str = "018f3a2b-7c9d-7e1f-abcd-0123456789ab";

    #[test]
    fn a_reached_position_is_admitted() {
        // Live joiner holding position 3, cursor past it.
        let grant = decide(&counters(10), REQ, None, Some((3, PositionStatus::Issued))).unwrap();
        assert_eq!(grant.position, 3);
        assert!(grant.arrival_shard < SHARDS);
    }

    #[test]
    fn a_position_not_yet_reached_is_refused() {
        let err = decide(&counters(3), REQ, None, Some((7, PositionStatus::Issued))).unwrap_err();
        assert_eq!(
            err,
            Denied::StillQueued {
                position: 7,
                serving: 3
            }
        );
    }

    #[test]
    fn the_cursor_is_exclusive() {
        // serving_counter is the count released, so position N is admitted only
        // once the cursor has passed it. Off by one here admits one visitor too
        // many on every interval.
        assert!(decide(&counters(5), REQ, None, Some((4, PositionStatus::Issued))).is_ok());
        assert!(decide(&counters(5), REQ, None, Some((5, PositionStatus::Issued))).is_err());
    }

    #[test]
    fn a_paused_event_admits_nobody_even_at_a_reached_position() {
        let mut c = counters(10);
        c.admission_control = AdmissionControl::Paused;
        assert_eq!(
            decide(&c, REQ, None, Some((3, PositionStatus::Issued))).unwrap_err(),
            Denied::NotAdmitting
        );
    }

    #[test]
    fn a_non_active_event_admits_nobody() {
        for phase in [
            Phase::Idle,
            Phase::PreQueue,
            Phase::PostEvent,
            Phase::Maintenance,
        ] {
            let mut c = counters(10);
            c.phase = phase;
            assert_eq!(
                decide(&c, REQ, None, Some((3, PositionStatus::Issued))).unwrap_err(),
                Denied::NotAdmitting
            );
        }
    }

    #[test]
    fn an_expired_or_spent_position_is_refused() {
        for status in [
            PositionStatus::Expired,
            PositionStatus::Completed,
            PositionStatus::Abandoned,
        ] {
            assert_eq!(
                decide(&counters(10), REQ, None, Some((3, status))).unwrap_err(),
                Denied::Spent
            );
        }
    }

    #[test]
    fn an_unregistered_visitor_is_refused() {
        assert_eq!(
            decide(&counters(10), REQ, None, None).unwrap_err(),
            Denied::NotRegistered
        );
    }

    #[test]
    fn a_pre_queue_registrant_resolves_through_the_permutation() {
        let c = counters(u64::MAX);
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 3,
            l: 1,
            t: "t".to_owned(),
        };
        let grant = decide(&c, REQ, Some(&row), None).unwrap();
        // Inside the sealed cohort.
        assert!(grant.position < c.participant_count.unwrap());
    }

    #[test]
    fn an_unsealed_event_has_no_pre_queue_position() {
        let mut c = counters(10);
        c.shuffle_seed = None;
        c.participant_count = None;
        c.prequeue_offsets = None;
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: "t".to_owned(),
        };
        assert_eq!(
            decide(&c, REQ, Some(&row), None).unwrap_err(),
            Denied::NotSealed
        );
    }

    #[test]
    fn a_live_join_row_wins_over_a_pre_queue_row() {
        // A straggler has both rows; the claimed live position is authoritative.
        let row = PreQueueItem {
            r: REQ.to_owned(),
            s: 0,
            l: 0,
            t: "t".to_owned(),
        };
        let grant = decide(
            &counters(100),
            REQ,
            Some(&row),
            Some((42, PositionStatus::Issued)),
        )
        .unwrap();
        assert_eq!(grant.position, 42);
    }

    // --- cookie encoding ------------------------------------------------------

    #[test]
    fn policy_json_has_no_whitespace_and_the_documented_shape() {
        let json = policy_json("https://*", 1_800_000_000);
        assert_eq!(
            json,
            r#"{"Statement":[{"Resource":"https://*","Condition":{"DateLessThan":{"AWS:EpochTime":1800000000}}}]}"#
        );
        assert!(!json.contains(' '), "CloudFront rejects a spaced policy");
    }

    #[test]
    fn cf_base64_applies_the_documented_substitutions() {
        // Bytes chosen so standard base64 emits all three replaced characters.
        let encoded = B64.encode([0xfb, 0xff, 0xbf, 0x00]);
        assert!(encoded.contains('+') && encoded.contains('/') && encoded.contains('='));
        let cf = cf_base64(&[0xfb, 0xff, 0xbf, 0x00]);
        assert!(!cf.contains('+') && !cf.contains('/') && !cf.contains('='));
        assert_eq!(
            cf,
            encoded
                .replace('+', "-")
                .replace('=', "_")
                .replace('/', "~")
        );
    }

    fn test_signer() -> Signer {
        let key = RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
        let der = key.as_der().unwrap();
        Signer::from_pkcs8_der(der.as_ref(), "K123".to_owned()).unwrap()
    }

    #[test]
    fn signing_produces_cookie_safe_values() {
        let signer = test_signer();
        let cookies = signer.sign(RESOURCE_WILDCARD, 1_800_000_000).unwrap();
        for value in [&cookies.policy, &cookies.signature] {
            assert!(!value.contains('+'));
            assert!(!value.contains('/'));
            assert!(!value.contains('='));
            assert!(!value.contains(' '));
        }
        assert_eq!(cookies.key_pair_id, "K123");
    }

    #[test]
    fn the_cookie_set_is_the_custom_policy_set() {
        let signer = test_signer();
        let headers = signer
            .sign(RESOURCE_WILDCARD, 1_800_000_000)
            .unwrap()
            .set_cookie_headers(3600);
        let joined = headers.join("\n");
        // A custom policy sends Policy, not Expires. Sending CloudFront-Expires
        // instead makes CloudFront read it as a canned policy and reject it.
        assert!(joined.contains("CloudFront-Policy="));
        assert!(joined.contains("CloudFront-Signature="));
        assert!(joined.contains("CloudFront-Key-Pair-Id=K123"));
        assert!(!joined.contains("CloudFront-Expires"));
        // SHA-256 must be declared or CloudFront verifies against SHA-1.
        assert!(joined.contains("CloudFront-Hash-Algorithm=SHA256"));
    }

    #[test]
    fn cookies_are_path_root_with_no_domain_attribute() {
        // A narrower Path means the browser never sends them back on the
        // protected request, which shows up as a 403 for every admitted visitor.
        let signer = test_signer();
        let headers = signer
            .sign(RESOURCE_WILDCARD, 1_800_000_000)
            .unwrap()
            .set_cookie_headers(3600);
        for header in &headers {
            assert!(header.contains("Path=/;"), "not path-root: {header}");
            assert!(!header.contains("Domain="), "domain-scoped: {header}");
            assert!(header.contains("Secure"));
        }
    }

    #[test]
    fn a_pem_key_round_trips_into_a_signer() {
        let key = RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap();
        let der = key.as_der().unwrap();
        let pem = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
            B64.encode(der.as_ref())
        );
        let signer = Signer::from_pkcs8_pem(&pem, "K1".to_owned()).unwrap();
        assert!(signer.sign(RESOURCE_WILDCARD, 1).is_ok());
    }

    #[test]
    fn a_malformed_key_is_rejected_not_panicked_on() {
        assert!(matches!(
            Signer::from_pkcs8_pem("not a pem", "K1".to_owned()),
            Err(SignError::BadKey)
        ));
        assert!(matches!(
            Signer::from_pkcs8_der(b"garbage", "K1".to_owned()),
            Err(SignError::BadKey)
        ));
        assert!(matches!(
            Signer::from_pkcs8_pem(
                "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----",
                "K1".to_owned()
            ),
            Err(SignError::BadKey)
        ));
    }
}
