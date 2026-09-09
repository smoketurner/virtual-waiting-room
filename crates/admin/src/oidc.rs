//! OIDC Authorization Code + PKCE login for the admin control plane (ADR-0016).
//!
//! The flow mirrors the reference `openidconnect` axum example, adapted for
//! Lambda: the login transaction (PKCE verifier + nonce) and the session live in
//! `DynamoDB` (`crate::sessions`), not in process memory, so they survive the
//! login -> callback round-trip across cold starts.
//!
//! TLS uses rustls with the aws-lc-rs provider (installed in `main`); JWKS
//! signature verification uses `jsonwebtoken` with the aws-lc-rs backend.
use openidconnect::core::{CoreClient, CoreProviderMetadata};
use openidconnect::{
    ClientId, ClientSecret, EndpointMaybeSet, EndpointNotSet, EndpointSet, IssuerUrl, RedirectUrl,
};

/// A discovered, fully-configured OIDC client. The endpoint type parameters are
/// what `CoreClient::from_provider_metadata(..).set_redirect_uri(..)` produces:
/// auth + redirect set, token/userinfo maybe-set from discovery.
pub type OidcClient = CoreClient<
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointMaybeSet,
    EndpointMaybeSet,
>;

/// Configuration for the OIDC login flow, read from the environment at startup.
/// `client_secret` is loaded separately from an SSM `SecureString`.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uri: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("missing env var {0}")]
    MissingEnv(String),
    #[error("invalid issuer url: {0}")]
    Issuer(String),
    #[error("invalid redirect uri: {0}")]
    Redirect(String),
    #[error("provider discovery failed: {0}")]
    Discovery(String),
    #[error("http client build failed: {0}")]
    HttpClient(String),
}

impl OidcConfig {
    /// Reads issuer / client id / redirect uri from the environment. Issuer
    /// defaults to the Vouch US endpoint (ADR-0016).
    ///
    /// # Errors
    /// Returns [`OidcError::MissingEnv`] if `OIDC_CLIENT_ID` or
    /// `OIDC_REDIRECT_URI` is not set.
    pub fn from_env() -> Result<Self, OidcError> {
        let issuer =
            std::env::var("OIDC_ISSUER").unwrap_or_else(|_| "https://us.vouch.sh".to_string());
        let client_id = std::env::var("OIDC_CLIENT_ID")
            .map_err(|_| OidcError::MissingEnv("OIDC_CLIENT_ID".to_string()))?;
        let redirect_uri = std::env::var("OIDC_REDIRECT_URI")
            .map_err(|_| OidcError::MissingEnv("OIDC_REDIRECT_URI".to_string()))?;
        Ok(Self {
            issuer,
            client_id,
            redirect_uri,
        })
    }
}

/// Builds the reqwest client the OIDC calls use. Redirects are disabled: the
/// token exchange must see the provider's own response, not follow it.
///
/// # Errors
/// Returns [`OidcError::HttpClient`] if the client cannot be built.
pub fn http_client() -> Result<reqwest::Client, OidcError> {
    reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| OidcError::HttpClient(e.to_string()))
}

/// Discovers the provider metadata and builds the configured OIDC client.
///
/// # Errors
/// Returns [`OidcError`] if the issuer or redirect URI is invalid, or if
/// provider discovery fails.
pub async fn discover(
    config: &OidcConfig,
    secret: ClientSecret,
    http: &reqwest::Client,
) -> Result<OidcClient, OidcError> {
    let issuer_url =
        IssuerUrl::new(config.issuer.clone()).map_err(|e| OidcError::Issuer(e.to_string()))?;
    let metadata = CoreProviderMetadata::discover_async(issuer_url, http)
        .await
        .map_err(|e| OidcError::Discovery(e.to_string()))?;
    let redirect = RedirectUrl::new(config.redirect_uri.clone())
        .map_err(|e| OidcError::Redirect(e.to_string()))?;
    Ok(CoreClient::from_provider_metadata(
        metadata,
        ClientId::new(config.client_id.clone()),
        Some(secret),
    )
    .set_redirect_uri(redirect))
}
