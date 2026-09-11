//! Cross-language conformance vectors for the edge gate (issue #71).
//!
//! Positives are minted by the real `wr_common::crypto::Session::sign` — the
//! thing that has to prove itself is "a Rust-minted credential verifies in
//! JavaScript", so a positive vector must come from the real implementation,
//! not a reimplementation of it.
//!
//! Negatives need byte-level tampering `Session`/`SigningKey`'s public API
//! cannot produce (a trailing byte, a wrong-kind tag, a flipped bit), so this
//! file hand-encodes the wire format independently, using only the
//! `aws_lc_rs` and `base64` crates directly rather than `wr_common`'s private
//! `encode_session`/`sign_payload` helpers. Reimplementing the format here —
//! the same thing `infra/modules/edge/functions/gate.js.tftpl` does — is what
//! makes agreement between the two a real cross-check rather than a
//! comparison of one implementation against itself.
//!
//! The committed file is generated, never hand-typed: run
//! `cargo test -p wr-common -- --ignored regenerate_vectors` after a wire
//! format change, then commit the diff. The ordinary test in this file
//! fails if the generator and the committed file disagree, so a format
//! change cannot silently drift.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test/fixture code panics on setup failure or an unexpected vector"
)]

use std::fmt::Write as _;
use std::path::PathBuf;

use aws_lc_rs::hmac;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use serde::{Deserialize, Serialize};
use wr_common::{Session, SigningKey};

const KIND_SESSION: u8 = 0x02;
const KIND_TOKEN: u8 = 0x01;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PositiveVector {
    name: String,
    key_hex: String,
    event_id: String,
    request_id: String,
    issued_at: u64,
    expires_at: u64,
    /// The instant both verifiers check the credential at.
    now: u64,
    credential: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NegativeVector {
    name: String,
    key_hex: String,
    credential: String,
    now: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RuleRequest {
    path: String,
    headers: Vec<(String, String)>,
    cookies: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RuleVector {
    name: String,
    /// The compact wire tuple, e.g. `["p", "/checkout"]`.
    rule: serde_json::Value,
    request: RuleRequest,
    matches: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Vectors {
    positives: Vec<PositiveVector>,
    negatives: Vec<NegativeVector>,
    rules: Vec<RuleVector>,
}

fn key_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex in a vector fixture"))
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// `u16`-length-prefixed UTF-8, mirroring `wr_common::crypto::put_str`.
fn put_str(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).expect("test string fits in u16");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Hand-encodes a session payload: `event_id ‖ request_id ‖ issued_at ‖
/// expires_at`. Independent of `wr_common::crypto`'s private `encode_session`.
fn session_payload(event_id: &str, request_id: &str, issued_at: u64, expires_at: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, event_id);
    put_str(&mut out, request_id);
    out.extend_from_slice(&issued_at.to_be_bytes());
    out.extend_from_slice(&expires_at.to_be_bytes());
    out
}

/// Hand-signs `kind || payload` and returns `base64url(payload).base64url(mac)`,
/// independent of `wr_common::crypto`'s private `sign_payload`.
fn sign_local(key: &[u8], kind: u8, payload: &[u8]) -> String {
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let mut message = Vec::with_capacity(payload.len() + 1);
    message.push(kind);
    message.extend_from_slice(payload);
    let mac = hmac::sign(&hmac_key, &message);
    format!("{}.{}", B64.encode(payload), B64.encode(mac.as_ref()))
}

fn positives() -> Vec<PositiveVector> {
    let key_hex = to_hex(b"a-32-byte-test-signing-key-value");
    let mk = |name: &str,
              event_id: &str,
              request_id: &str,
              issued_at: u64,
              expires_at: u64,
              now: u64| {
        let key = SigningKey::new(&key_bytes(&key_hex));
        let session = Session {
            event_id: event_id.to_owned(),
            request_id: request_id.to_owned(),
            issued_at,
            expires_at,
        };
        PositiveVector {
            name: name.to_owned(),
            key_hex: key_hex.clone(),
            event_id: event_id.to_owned(),
            request_id: request_id.to_owned(),
            issued_at,
            expires_at,
            now,
            credential: session.sign(&key),
        }
    };

    let long_id: String = "r".repeat(300);

    vec![
        mk(
            "typical",
            "smoke",
            "018f3a2b-7c9d-7e1f-abcd-0123456789ab",
            1_700_000_000,
            1_700_003_600,
            1_700_000_001,
        ),
        mk("empty_event_id", "", "r1", 0, 1_000, 500),
        mk(
            "multibyte_event_id",
            "événement-日本語-🎫",
            "r1",
            0,
            1_000,
            500,
        ),
        // Above 2^32: catches a 32-bit truncation bug in a naive JS reader.
        mk(
            "expires_at_above_u32",
            "smoke",
            "r1",
            0,
            4_294_967_296 + 12_345,
            4_294_967_296,
        ),
        mk("near_max_length_ids", &long_id, &long_id, 0, 1_000, 500),
        // now is the last valid instant before expiry (now < expires_at).
        mk("expires_at_boundary", "smoke", "r1", 0, 1_000, 999),
    ]
}

fn negatives() -> Vec<NegativeVector> {
    let key_hex = to_hex(b"a-32-byte-test-signing-key-value");
    let key = key_bytes(&key_hex);
    let other_key_hex = to_hex(b"a-different-32-byte-signing-keyy");
    let other_key = key_bytes(&other_key_hex);

    let payload = session_payload("smoke", "r1", 1000, 2000);
    let valid = sign_local(&key, KIND_SESSION, &payload);
    let (valid_payload_b64, valid_mac_b64) = valid.split_once('.').expect("has a dot");

    // Flip one payload bit: decode, flip, re-encode without a new signature.
    let mut tampered_payload = B64.decode(valid_payload_b64).unwrap();
    tampered_payload[0] ^= 0x01;
    let flipped_payload = format!("{}.{}", B64.encode(&tampered_payload), valid_mac_b64);

    // Flip one MAC bit similarly.
    let mut tampered_mac = B64.decode(valid_mac_b64).unwrap();
    tampered_mac[0] ^= 0x01;
    let flipped_mac = format!("{}.{}", valid_payload_b64, B64.encode(&tampered_mac));

    // A trailing byte appended to the payload, re-signed so the MAC is valid
    // for the tampered (longer) payload — the only way to isolate the
    // trailing-bytes bug from a MAC failure.
    let mut with_trailing = payload.clone();
    with_trailing.push(0xAB);
    let trailing_byte = sign_local(&key, KIND_SESSION, &with_trailing);

    // A token-kind credential offered where a session is expected.
    let token_payload = {
        let mut out = Vec::new();
        put_str(&mut out, "smoke");
        put_str(&mut out, "r1");
        out.extend_from_slice(&2000u64.to_be_bytes());
        out
    };
    let wrong_kind = sign_local(&key, KIND_TOKEN, &token_payload);

    vec![
        NegativeVector {
            name: "wrong_key".into(),
            key_hex: key_hex.clone(),
            credential: sign_local(&other_key, KIND_SESSION, &payload),
            now: 1500,
        },
        NegativeVector {
            name: "flipped_payload_bit".into(),
            key_hex: key_hex.clone(),
            credential: flipped_payload,
            now: 1500,
        },
        NegativeVector {
            name: "flipped_mac_bit".into(),
            key_hex: key_hex.clone(),
            credential: flipped_mac,
            now: 1500,
        },
        NegativeVector {
            name: "token_kind_as_session".into(),
            key_hex: key_hex.clone(),
            credential: wrong_kind,
            now: 1500,
        },
        NegativeVector {
            name: "trailing_byte".into(),
            key_hex: key_hex.clone(),
            credential: trailing_byte,
            now: 1500,
        },
        NegativeVector {
            name: "no_dot".into(),
            key_hex: key_hex.clone(),
            credential: "not-a-credential".into(),
            now: 0,
        },
        NegativeVector {
            name: "base64_with_padding".into(),
            key_hex: key_hex.clone(),
            credential: format!("{valid_payload_b64}==.{valid_mac_b64}"),
            now: 1500,
        },
        NegativeVector {
            name: "base64_with_plus_slash".into(),
            key_hex,
            credential: "AA+/.AA+/".into(),
            now: 0,
        },
    ]
}

fn rules() -> Vec<RuleVector> {
    let req = |path: &str, headers: &[(&str, &str)], cookies: &[(&str, &str)]| RuleRequest {
        path: path.to_owned(),
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        cookies: cookies
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    };

    vec![
        RuleVector {
            name: "path_prefix_matches".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("/checkout/step2", &[], &[]),
            matches: true,
        },
        RuleVector {
            name: "path_prefix_no_match".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("/about", &[], &[]),
            matches: false,
        },
        RuleVector {
            name: "cookie_present".into(),
            rule: serde_json::json!(["c", "loyalty_member"]),
            request: req("/", &[], &[("loyalty_member", "1")]),
            matches: true,
        },
        RuleVector {
            name: "cookie_absent".into(),
            rule: serde_json::json!(["c", "loyalty_member"]),
            request: req("/", &[], &[]),
            matches: false,
        },
        RuleVector {
            name: "user_agent_substring".into(),
            rule: serde_json::json!(["u", "HeadlessChrome"]),
            request: req(
                "/",
                &[("user-agent", "Mozilla/5.0 HeadlessChrome/120")],
                &[],
            ),
            matches: true,
        },
        RuleVector {
            name: "user_agent_absent".into(),
            rule: serde_json::json!(["u", "HeadlessChrome"]),
            request: req("/", &[], &[]),
            matches: false,
        },
        RuleVector {
            name: "header_exact_match_case_insensitive_name".into(),
            rule: serde_json::json!(["h", "X-Internal-Monitor", "true"]),
            request: req("/", &[("x-internal-monitor", "true")], &[]),
            matches: true,
        },
        RuleVector {
            name: "header_value_case_insensitive".into(),
            rule: serde_json::json!(["h", "x-internal-monitor", "TRUE"]),
            request: req("/", &[("x-internal-monitor", "true")], &[]),
            matches: true,
        },
        RuleVector {
            name: "header_value_mismatch".into(),
            rule: serde_json::json!(["h", "x-internal-monitor", "true"]),
            request: req("/", &[("x-internal-monitor", "false")], &[]),
            matches: false,
        },
        RuleVector {
            name: "empty_ruleset_has_no_bearing_here_but_prefix_of_root_matches_everything".into(),
            rule: serde_json::json!(["p", "/"]),
            request: req("/anything/at/all", &[], &[]),
            matches: true,
        },
    ]
}

fn build_vectors() -> Vectors {
    Vectors {
        positives: positives(),
        negatives: negatives(),
        rules: rules(),
    }
}

fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/session.json")
}

/// Regenerates `tests/vectors/session.json`. Run explicitly after a wire
/// format change: `cargo test -p wr-common -- --ignored regenerate_vectors`.
#[test]
#[ignore = "writes tests/vectors/session.json; run explicitly after a wire format change"]
fn regenerate_vectors() {
    let json = serde_json::to_string_pretty(&build_vectors()).expect("vectors serialize");
    std::fs::write(vectors_path(), json + "\n").expect("write tests/vectors/session.json");
}

/// The committed file must match what the generator produces right now, so a
/// wire-format change that was not regenerated fails CI instead of the two
/// silently drifting apart.
#[test]
fn committed_vectors_match_the_generator() {
    let committed_text = std::fs::read_to_string(vectors_path()).expect(
        "tests/vectors/session.json must exist; run \
         `cargo test -p wr-common -- --ignored regenerate_vectors` to create it",
    );
    let committed: Vectors = serde_json::from_str(&committed_text).expect("valid vectors JSON");
    assert_eq!(committed, build_vectors());
}

/// Self-check: every positive vector verifies against `Session::verify` at
/// its `now`, and every negative is rejected. Proves the generator's own
/// hand-rolled negatives are actually invalid under the real implementation
/// (not just under the JS reimplementation), before the JS side ever runs.
#[test]
fn rust_accepts_every_positive_and_rejects_every_negative() {
    for v in positives() {
        let key = SigningKey::new(&key_bytes(&v.key_hex));
        let session = Session::verify(&v.credential, &key, v.now)
            .unwrap_or_else(|e| panic!("positive vector {:?} failed to verify: {e:?}", v.name));
        assert_eq!(session.event_id, v.event_id, "vector {:?}", v.name);
        assert_eq!(session.request_id, v.request_id, "vector {:?}", v.name);
        assert_eq!(session.expires_at, v.expires_at, "vector {:?}", v.name);
    }
    for v in negatives() {
        let key = SigningKey::new(&key_bytes(&v.key_hex));
        assert!(
            Session::verify(&v.credential, &key, v.now).is_err(),
            "negative vector {:?} unexpectedly verified",
            v.name
        );
    }
}

impl wr_common::RequestView for RuleRequest {
    fn path(&self) -> &str {
        &self.path
    }

    fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    fn cookie(&self, name: &str) -> Option<&str> {
        self.cookies
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Self-check: every rule vector's expected outcome matches
/// `wr_common::ProtectionRule::matches` under the real Rust matcher.
#[test]
fn rust_rule_vectors_match_the_real_matcher() {
    use wr_common::ProtectionRule;

    for v in rules() {
        let rule: ProtectionRule = serde_json::from_value(v.rule.clone())
            .unwrap_or_else(|e| panic!("vector {:?} has an unparsable rule: {e}", v.name));
        assert_eq!(
            rule.matches(&v.request),
            v.matches,
            "rule vector {:?}",
            v.name
        );
    }
}
