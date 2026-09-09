//! Security hardening for the admin surface: a per-response CSP nonce and the
//! set of hardening response headers applied to every admin HTML/JSON response.
//!
//! The Content-Security-Policy is strict by construction — the site is
//! self-hosted, server-rendered HTML with two same-origin stylesheets and one
//! same-origin inline poller script (allowed by nonce, not `'unsafe-inline'`).
//! Everything else is denied.

use axum::http::HeaderValue;
use axum::http::header::{HeaderMap, HeaderName};

/// A base64url, 128-bit random nonce for a single response's inline script.
#[must_use]
pub fn nonce() -> String {
    use aws_lc_rs::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0u8; 16];
    // The system RNG only fails if the OS entropy source is unavailable, which
    // does not happen on Lambda; an all-zero nonce would merely fail closed
    // (the inline script would not run), never widen the policy.
    let _ = SystemRandom::new().fill(&mut bytes);
    base64url(&bytes)
}

/// Minimal base64url (no padding) — avoids adding a base64 dependency for a
/// 16-byte value.
fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
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
    let pairs: [(HeaderName, &str); 8] = [
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
        // The control plane serves sensitive per-operator state — never cache it
        // in the browser, API Gateway, or any intermediary.
        (
            HeaderName::from_static("cache-control"),
            "no-cache, no-store, must-revalidate",
        ),
        (HeaderName::from_static("pragma"), "no-cache"),
        (HeaderName::from_static("expires"), "0"),
    ];
    for (name, value) in pairs {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.insert(name, v);
        }
    }
}
