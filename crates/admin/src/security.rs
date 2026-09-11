//! Security hardening for the admin surface: a per-response CSP nonce and the
//! set of hardening response headers applied to every admin HTML/JSON response.
//!
//! The Content-Security-Policy is strict by construction — the site is
//! self-hosted, server-rendered HTML with two same-origin stylesheets and one
//! same-origin inline poller script (allowed by nonce, not `'unsafe-inline'`).
//! Everything else is denied.

use axum::http::HeaderValue;
use axum::http::header::{self, HeaderMap, HeaderName};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A source of the per-response nonce's randomness, abstracted so [`nonce()`]'s
/// failure path can be exercised in tests. The production implementation is
/// `aws_lc_rs`'s `SystemRandom` — a `getrandom`-backed source that does not
/// error post-boot on the documented Lambda runtime.
trait NonceRng {
    /// Fills `dest` with 16 random bytes. Returns `true` on success; `false`
    /// signals the entropy source is unavailable and the caller must fail
    /// closed (emit no nonce rather than a constant).
    fn fill(&self, dest: &mut [u8; 16]) -> bool;
}

impl NonceRng for aws_lc_rs::rand::SystemRandom {
    fn fill(&self, dest: &mut [u8; 16]) -> bool {
        // Fully-qualified so it resolves to `SecureRandom::fill` rather than
        // the `NonceRng::fill` being defined (`self.fill(dest)` would recurse).
        aws_lc_rs::rand::SecureRandom::fill(self, dest).is_ok()
    }
}

/// A base64url, 128-bit random nonce for a single response's inline script.
///
/// The dashboard mounts the same `String` on both halves of the inline-script
/// authorization — the `<script nonce="…">` tag and the `script-src
/// 'nonce-{nonce}'` CSP header (`main.rs`'s `dashboard`) — so a browser
/// authorizes the poller because the two are byte-equal.
///
/// On the documented Lambda runtime `aws_lc_rs`'s `SystemRandom` (a
/// `getrandom`-backed source) does not error post-boot. If it ever does — the
/// OS entropy source is unavailable — this returns an *empty* `String`.
/// `apply_hardening` then stamps `script-src 'nonce-'`, whose `nonce-` token has
/// no base64 component. The CSP3 `nonce-source` grammar requires
/// `base64-value = 1*( … )` — at least one character — so the empty token
/// matches no `nonce-source` expression (§6.7.2.3 returns "Does Not Match" for
/// an empty request nonce). The dashboard's `<script nonce="">` poller
/// therefore does **not** execute — genuine fail-closed, the same path the
/// response middleware relies on for responses that carry no inline script.
#[must_use]
pub fn nonce() -> String {
    nonce_from(&aws_lc_rs::rand::SystemRandom::new())
}

/// Formats `rng`'s 16 random bytes as a base64url nonce, or `String::new()` on
/// RNG failure (see [`nonce`]). The `&impl NonceRng` parameter is the only seam
/// between the formatting logic and the randomness source; tests pass a
/// failing source here to force the failure path.
fn nonce_from(rng: &impl NonceRng) -> String {
    let mut bytes = [0u8; 16];
    if !rng.fill(&mut bytes) {
        return String::new();
    }
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Applies the hardening headers to a response's header map. `nonce` is the
/// per-response script nonce; pass an empty string for responses with no inline
/// script (the CSP then allows none). An existing `content-security-policy` is
/// left intact, so a handler that has already set a nonce'd policy (the
/// dashboard) is not clobbered when this runs again in a response-wide layer.
pub fn apply_hardening(headers: &mut HeaderMap, nonce: &str) {
    // Content-Security-Policy: default-deny, then narrowly allow what the admin
    // page actually uses. self stylesheets, self XHR (the poller), the nonce'd
    // inline script; no objects, no framing, no base-uri hijack, forms to self.
    let csp = format!(
        "default-src 'none'; \
         style-src 'self'; \
         script-src 'nonce-{nonce}'; \
         connect-src 'self'; \
         img-src 'self' data:; \
         font-src 'self'; \
         form-action 'self'; \
         base-uri 'none'; \
         frame-ancestors 'none'; \
         object-src 'none'"
    );
    let csp_name = HeaderName::from_static("content-security-policy");
    if !headers.contains_key(&csp_name)
        && let Ok(v) = HeaderValue::from_str(&csp)
    {
        headers.insert(csp_name, v);
    }
    // The control plane serves sensitive per-operator state, so no-store is the
    // default. A handler that has already set its own cache-control has made a
    // deliberate choice — the embedded static assets carry a validator so a
    // reload is a 304 rather than 88 KB of fonts — so leave it, the same way the
    // CSP above yields to a handler's own policy.
    if !headers.contains_key(header::CACHE_CONTROL) {
        for (name, value) in [
            (
                HeaderName::from_static("cache-control"),
                "no-cache, no-store, must-revalidate",
            ),
            (HeaderName::from_static("pragma"), "no-cache"),
            (HeaderName::from_static("expires"), "0"),
        ] {
            if let Ok(v) = HeaderValue::from_str(value) {
                headers.insert(name, v);
            }
        }
    }

    let pairs: [(HeaderName, &str); 5] = [
        // Belt-and-braces clickjacking defense alongside frame-ancestors.
        (HeaderName::from_static("x-frame-options"), "DENY"),
        (HeaderName::from_static("x-content-type-options"), "nosniff"),
        (HeaderName::from_static("referrer-policy"), "no-referrer"),
        (
            HeaderName::from_static("strict-transport-security"),
            "max-age=31536000; includeSubDomains",
        ),
        // Deny powerful browser features outright — the admin UI needs none.
        (
            HeaderName::from_static("permissions-policy"),
            "geolocation=(), microphone=(), camera=(), usb=(), payment=()",
        ),
    ];
    for (name, value) in pairs {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.insert(name, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NonceRng, apply_hardening, nonce, nonce_from};
    use axum::http::HeaderValue;
    use axum::http::header::{self, HeaderMap};

    /// A source that always reports an unavailable entropy source. Forces the
    /// failure path the production `SystemRandom` never hits on Lambda.
    struct FailingRng;
    impl NonceRng for FailingRng {
        fn fill(&self, _dest: &mut [u8; 16]) -> bool {
            false
        }
    }

    /// A source that fills every byte with one constant value, so the encoded
    /// output is deterministic and assertable.
    struct FixedRng(u8);
    impl NonceRng for FixedRng {
        fn fill(&self, dest: &mut [u8; 16]) -> bool {
            dest.fill(self.0);
            true
        }
    }

    /// base64url-no-pad of sixteen zero bytes — the fixed, globally-known 22-`A`
    /// constant the buggy `let _ = fill` path returned. The fail-closed path
    /// must never produce it.
    const ALL_ZERO_NONCE: &str = "AAAAAAAAAAAAAAAAAAAAAA";

    /// base64url-no-pad of sixteen `0x42` bytes, computed independently of the
    /// crate's own encoder (`0x42` ⇒ 6-bit groups `010000 010000 …` ⇒ `Q…`).
    /// Pins that the success path encodes the *filled* buffer, not `[0u8; 16]`.
    const FIXED_NONCE_0X42: &str = "QkJCQkJCQkJCQkJCQkJCQg";

    /// Reads back the (single) CSP header as `&str`, or `""` if absent — the
    /// caller's subsequent asserts make absence a failure either way.
    fn csp(headers: &HeaderMap) -> String {
        headers
            .get("content-security-policy")
            .map(|v| v.to_str().unwrap_or("").to_owned())
            .unwrap_or_default()
    }

    #[test]
    fn rng_failure_returns_empty_nonce() {
        assert_eq!(nonce_from(&FailingRng), "", "RNG failure must return empty");
    }

    #[test]
    fn success_encodes_the_filled_bytes_not_the_zero_buffer() {
        // A deterministic fill proves nonce_from emits the RNG output, not the
        // [0u8; 16] it was handed — the exact behavior `let _ = fill` broke.
        assert_eq!(nonce_from(&FixedRng(0x42)), FIXED_NONCE_0X42);
        assert_ne!(nonce_from(&FixedRng(0x42)), ALL_ZERO_NONCE);
    }

    #[test]
    fn public_nonce_is_nonempty_and_well_formed_on_a_working_rng() {
        // The entry point routes the production SystemRandom through
        // nonce_from; on a working RNG it must produce a value, never "" and
        // never the all-zero constant.
        let n = nonce();
        assert!(!n.is_empty(), "nonce() returned empty on a working RNG");
        assert_ne!(n, ALL_ZERO_NONCE, "nonce() returned the all-zero constant");
        assert_eq!(n.len(), 22);
        assert!(
            n.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "nonce must be base64url: {n:?}"
        );
    }

    #[test]
    fn empty_nonce_round_trip_is_the_documented_fail_closed_csp() {
        // apply_hardening("") stamps `script-src 'nonce-'`. The CSP3
        // `nonce-source` grammar requires `base64-value = 1*( … )` (≥1 char),
        // so a token with no base64 component matches no nonce-source
        // expression: the dashboard's `<script nonce="">` poller does not run.
        // This is the same path the response middleware uses for non-dashboard
        // responses (and the documented "CSP then allows none" behavior).
        let mut headers = HeaderMap::new();
        apply_hardening(&mut headers, "");
        let csp = csp(&headers);
        assert!(
            csp.contains("script-src 'nonce-'"),
            "empty nonce must yield a base64-less script-src, got {csp:?}"
        );
        assert!(
            !csp.contains(ALL_ZERO_NONCE),
            "empty-nonce CSP must not embed the all-zero constant"
        );
        // The other hardening directives still apply on the fail-closed path.
        assert!(csp.contains("default-src 'none'"));
        assert!(csp.contains("connect-src 'self'"));
        assert!(csp.contains("frame-ancestors 'none'"));
        assert!(csp.contains("object-src 'none'"));
    }

    #[test]
    fn no_store_is_the_default_but_a_handler_may_keep_its_own() {
        // Every control-plane response must be uncacheable.
        let mut headers = HeaderMap::new();
        apply_hardening(&mut headers, "n");
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .map(HeaderValue::as_bytes),
            Some(&b"no-cache, no-store, must-revalidate"[..])
        );
        assert!(headers.contains_key("pragma"));
        assert!(headers.contains_key("expires"));

        // The embedded static assets set their own, so a reload revalidates to
        // a 304 instead of resending the fonts. Overwriting it here would make
        // the validator useless: no-store stops the browser sending
        // If-None-Match at all.
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=300, must-revalidate"),
        );
        apply_hardening(&mut headers, "n");
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .map(HeaderValue::as_bytes),
            Some(&b"public, max-age=300, must-revalidate"[..])
        );
        assert!(
            !headers.contains_key("pragma"),
            "pragma would contradict it"
        );
        assert!(!headers.contains_key("expires"));

        // The hardening headers still apply either way.
        assert_eq!(
            headers.get("x-frame-options").map(HeaderValue::as_bytes),
            Some(&b"DENY"[..])
        );
    }
}
