//! Cross-language conformance vectors for the edge gate (issue #71).
//!
//! Positives are minted by the real `wr_common::crypto::Session::sign` — the
//! thing that has to prove itself is "a Rust-minted credential verifies in
//! JavaScript", so a positive vector must come from the real implementation,
//! not a reimplementation of it.
//!
//! Negatives are minted the same way — a real `Session::sign` JWT — and then
//! tampered *after* minting, so each "tamper" vector is a credential that is
//! valid in *shape* (a three-segment JWS) but fails verification for the
//! reason it is named for (a wrong key, a flipped payload/signature char, a
//! trailing fourth segment). That is what lets the JS conformance suite prove
//! the two implementations reject the *same* tampered JWT: the credential
//! reaches the signature/shape check it is named for, instead of being
//! dismissed up-front as a non-JWS blob. The same rule governs the charset
//! negatives (`base64_with_plus_slash`, `base64_with_padding`): they carry
//! three segments so the arity check passes and the base64url charset check
//! is what refuses them. `no_dot` is the one deliberate blob, testing the
//! arity check itself.
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

use serde::{Deserialize, Serialize};
use wr_common::{Session, SigningKey};

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

/// Flips the first base64url char of `segment` to a different one, keeping the
/// segment valid base64url so it isn't dismissed as a charset/shape error: the
/// named tamper (a signature mismatch) is what rejects the credential, not the
/// encoding. `A` and `B` are both in the base64url alphabet and the two branches
/// are mutually exclusive, so the result is always a different valid char.
fn flip_first_b64url_char(segment: &str) -> String {
    let mut chars: Vec<char> = segment.chars().collect();
    chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
    chars.into_iter().collect()
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
            credential: session.sign(&key).unwrap(),
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
    let other_key_hex = to_hex(b"a-different-32-byte-signing-keyy");

    let key = SigningKey::new(&key_bytes(&key_hex));
    let other_key = SigningKey::new(&key_bytes(&other_key_hex));

    // Mint the baseline as a real three-segment JWS, the way a credential is
    // actually issued, then tamper *after* minting so each negative is valid in
    // shape and fails verification for the reason it is named for rather than
    // being dismissed up-front as a non-JWS blob.
    let session = Session {
        event_id: "smoke".to_owned(),
        request_id: "r1".to_owned(),
        issued_at: 1000,
        expires_at: 2000,
    };
    let real = session.sign(&key).expect("real session signs");

    // `header.payload.signature`.
    let parts: Vec<&str> = real.split('.').collect();
    assert_eq!(parts.len(), 3, "compact JWS has exactly three segments");
    let (header, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    // A real JWT signed under a different deployment key. Goes through the
    // real derived-key path (`SigningKey::new` -> `for_kind(Session)`), not a
    // raw-HMAC shortcut over the bare secret, so the rejection is for the
    // named reason (wrong key) under the same signing path production uses.
    let wrong_key = session.sign(&other_key).expect("real session signs");

    // Flip one base64url char in the payload segment: the signature covers
    // `header.payload`, so it no longer matches the (unchanged) signature.
    let flipped_payload_bit = format!("{header}.{}.{sig_b64}", flip_first_b64url_char(payload_b64));

    // Flip one base64url char in the signature segment: the MAC recomputed over
    // the (unchanged) `header.payload` no longer matches it.
    let flipped_mac_bit = format!("{header}.{payload_b64}.{}", flip_first_b64url_char(sig_b64));

    // A well-formed fourth segment: still three valid segments plus one, so
    // the `parts.length !== 3` (JS) / not-a-compact-JWS (Rust) check is what
    // rejects it, not a signature or charset failure.
    let four_segments = format!("{real}.AAAA");

    // Charset negatives. Both need the *right number of segments*, or the
    // arity check rejects them first and the charset check they are named for
    // never runs — the same defect as the tamper vectors above. Standard
    // base64's `+` and `/`, and its `=` padding, are all outside base64url, so
    // `gate.js.tftpl`'s `BASE64URL_RE` (and the `URL_SAFE_NO_PAD` decode Rust
    // does) is what refuses them, on a credential that is otherwise a
    // three-segment JWS.
    let base64_with_plus_slash = format!("{header}.+/{}.{sig_b64}", &payload_b64[2..]);
    let base64_with_padding = format!("{header}.{payload_b64}==.{sig_b64}");

    vec![
        NegativeVector {
            name: "wrong_key".into(),
            key_hex: key_hex.clone(),
            credential: wrong_key,
            now: 1500,
        },
        NegativeVector {
            name: "flipped_payload_bit".into(),
            key_hex: key_hex.clone(),
            credential: flipped_payload_bit,
            now: 1500,
        },
        NegativeVector {
            name: "flipped_mac_bit".into(),
            key_hex: key_hex.clone(),
            credential: flipped_mac_bit,
            now: 1500,
        },
        NegativeVector {
            name: "four_segments".into(),
            key_hex: key_hex.clone(),
            credential: four_segments,
            now: 1500,
        },
        NegativeVector {
            name: "no_dot".into(),
            key_hex: key_hex.clone(),
            credential: "not-a-credential".into(),
            now: 0,
        },
        NegativeVector {
            name: "base64_with_plus_slash".into(),
            key_hex: key_hex.clone(),
            credential: base64_with_plus_slash,
            now: 1500,
        },
        NegativeVector {
            name: "base64_with_padding".into(),
            key_hex,
            credential: base64_with_padding,
            now: 1500,
        },
    ]
}

fn req(path: &str, headers: &[(&str, &str)], cookies: &[(&str, &str)]) -> RuleRequest {
    RuleRequest {
        path: path.to_owned(),
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        cookies: cookies
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    }
}

fn rules() -> Vec<RuleVector> {
    let mut all = vec![
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
        // Reproduced against the shipped gate during implementation review
        // (SEC-H2 / S3): each of these evaded the rule before path
        // normalization landed. Kept as vectors so a future change cannot
        // silently reopen any of them in either language.
        RuleVector {
            name: "path_prefix_matches_case_folded".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("/CHECKOUT", &[], &[]),
            matches: true,
        },
        RuleVector {
            name: "path_prefix_matches_percent_encoded".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("/%63heckout", &[], &[]),
            matches: true,
        },
        // Mixed valid + malformed percent-encoding (the PR #120 regression):
        // a valid escape inside the protected prefix must still be decoded
        // when a later malformed escape is present. gate.js.tftpl previously
        // called decodeURIComponent whole-string and reverted the entire URI
        // on the malformed escape, dropping the valid decode and evading the
        // rule; these vectors pin the per-byte behaviour both engines share.
        RuleVector {
            name: "path_prefix_matches_valid_escape_with_malformed_hex_later".into(),
            rule: serde_json::json!(["p", "/admin"]),
            request: req("/%61dmin/secret%zz", &[], &[]),
            matches: true,
        },
        RuleVector {
            name: "path_prefix_matches_valid_escape_with_non_utf8_byte_later".into(),
            rule: serde_json::json!(["p", "/admin"]),
            request: req("/%61dmin/secret%ff", &[], &[]),
            matches: true,
        },
        RuleVector {
            name: "path_prefix_matches_valid_escape_mid_string_with_malformed_later".into(),
            rule: serde_json::json!(["p", "/foocbar"]),
            request: req("/foo%63bar%zz", &[], &[]),
            matches: true,
        },
        // A `+`-signed two-character escape is not a valid escape in either
        // engine. Rust must not reach it through `u8::from_str_radix`, which
        // accepts a leading sign; gate.js.tftpl reads the two characters as
        // hex digits and refuses. Left literal, the prefix does not match.
        RuleVector {
            name: "path_prefix_no_match_signed_hex_escape_stays_literal".into(),
            rule: serde_json::json!(["p", "/admin"]),
            request: req("/%+41dmin", &[], &[]),
            matches: false,
        },
        RuleVector {
            name: "path_prefix_no_match_malformed_escape_in_prefix_does_not_recover".into(),
            rule: serde_json::json!(["p", "/admin"]),
            request: req("/%zzdmin/secret", &[], &[]),
            matches: false,
        },
        RuleVector {
            name: "path_prefix_matches_doubled_leading_slash".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("//checkout", &[], &[]),
            matches: true,
        },
        RuleVector {
            name: "path_prefix_matches_leading_dot_segment".into(),
            rule: serde_json::json!(["p", "/checkout"]),
            request: req("/./checkout", &[], &[]),
            matches: true,
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
    ];
    all.extend(rules_by_header());
    all
}

/// The user-agent, cookie and header matchers. Split from [`rules`] only to
/// keep each function under the line ceiling.
fn rules_by_header() -> Vec<RuleVector> {
    vec![
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
