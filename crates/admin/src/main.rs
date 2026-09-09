//! Lambda entry point for the admin control plane: an Axum router served through
//! API Gateway via `lambda_http`. Access is gated by an OIDC login session
//! (ADR-0016): unauthenticated requests to `/admin*` redirect to `/admin/login`,
//! which runs an Authorization Code + PKCE flow against the configured provider
//! (Vouch by default). The PKCE transaction and the session live in `DynamoDB`,
//! so they survive the login -> callback round-trip across Lambda cold starts.
//!
//! API Gateway prefixes the request path with the stage (e.g. `/dev/admin`).
//! `AWS_LAMBDA_HTTP_IGNORE_STAGE_IN_PATH=true` (set on the function in Terraform)
//! makes the runtime strip the stage before axum routes, so routes are declared
//! unprefixed.

use std::sync::Arc;

use admin::dynamo::DynamoStore;
use admin::oidc::{self, OidcClient, OidcConfig};
use admin::sessions::{AdminSession, PendingLogin, SessionStore};
use admin::templates::Dashboard;
use admin::{ApplyError, apply_message, apply_phase, apply_rate, apply_reset};
use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use lambda_http::Error;
use openidconnect::core::CoreResponseType;
use openidconnect::{
    AuthenticationFlow, AuthorizationCode, CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier,
    Scope, TokenResponse,
};
use rust_embed::RustEmbed;
use serde::Deserialize;

/// Name of the opaque session cookie.
const SESSION_COOKIE: &str = "vwr_admin_session";

/// Static assets (CSS) embedded into the binary at build time.
#[derive(RustEmbed)]
#[folder = "static/"]
struct StaticAssets;

struct AppState {
    store: DynamoStore,
    sessions: SessionStore,
    oidc: OidcClient,
    http: reqwest::Client,
    event_id: String,
}

type Shared = Arc<AppState>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    // Install the aws-lc-rs rustls provider process-wide so reqwest's
    // no-provider rustls path (and every TLS handshake) uses aws-lc-rs, not ring.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| Error::from("failed to install aws-lc-rs rustls provider"))?;

    let config = aws_config::load_from_env().await;
    let dynamo = aws_sdk_dynamodb::Client::new(&config);

    // Load the OIDC client secret from an SSM SecureString (ADR-0016).
    let ssm = aws_sdk_ssm::Client::new(&config);
    let secret_param = std::env::var("OIDC_CLIENT_SECRET_PARAM")?;
    let secret_value = ssm
        .get_parameter()
        .name(secret_param)
        .with_decryption(true)
        .send()
        .await
        .map_err(|e| Error::from(format!("ssm get_parameter: {e}")))?
        .parameter
        .and_then(|p| p.value)
        .ok_or_else(|| Error::from("OIDC client secret parameter has no value"))?;

    let oidc_config = OidcConfig::from_env().map_err(|e| Error::from(e.to_string()))?;
    let http = oidc::http_client().map_err(|e| Error::from(e.to_string()))?;
    let oidc = oidc::discover(
        &oidc_config,
        openidconnect::ClientSecret::new(secret_value),
        &http,
    )
    .await
    .map_err(|e| Error::from(e.to_string()))?;

    let state = Arc::new(AppState {
        store: DynamoStore::new(dynamo.clone(), std::env::var("COUNTERS_TABLE")?),
        sessions: SessionStore::new(dynamo, std::env::var("TOKENS_TABLE")?),
        oidc,
        http,
        event_id: std::env::var("EVENT_ID")?,
    });

    let app = Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/login", get(login))
        .route("/admin/callback", get(callback))
        .route("/admin/logout", get(logout))
        .route("/admin/phase", post(set_phase))
        .route("/admin/rate", post(set_rate))
        .route("/admin/message", post(set_message))
        .route("/admin/reset", post(reset))
        .route("/admin/rules", post(deferred))
        .route("/update_session", post(deferred))
        .route("/static/{*path}", get(static_asset))
        .with_state(state);

    lambda_http::run(app).await
}

// --- Auth helpers -------------------------------------------------------------

/// Extracts the session id from the request cookie header, if present.
fn session_id_from(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie::Cookie::split_parse(raw)
        .filter_map(Result::ok)
        .find(|c| c.name() == SESSION_COOKIE)
        .map(|c| c.value().to_string())
}

/// Returns the authenticated session, or `None` if the request is unauthenticated.
async fn authed(state: &Shared, headers: &HeaderMap) -> Option<AdminSession> {
    let id = session_id_from(headers)?;
    state.sessions.load_session(&id).await.ok().flatten()
}

// --- OIDC flow ----------------------------------------------------------------

/// Starts the login: build the authorize URL with PKCE, persist the transaction
/// keyed by CSRF state, redirect the browser to the provider.
async fn login(State(state): State<Shared>) -> Response {
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf, nonce) = state
        .oidc
        .authorize_url(
            AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .add_scope(Scope::new("openid".to_string()))
        .add_scope(Scope::new("email".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    let pending = PendingLogin {
        pkce_verifier: pkce_verifier.secret().clone(),
        nonce: nonce.secret().clone(),
    };
    if let Err(e) = state.sessions.put_pending(csrf.secret(), &pending).await {
        return server_error(&e.to_string());
    }
    Redirect::to(auth_url.as_str()).into_response()
}

#[derive(Deserialize)]
struct CallbackParams {
    code: String,
    state: String,
}

/// OIDC redirect target: exchange the code, verify the ID token, create a
/// session, set the cookie, redirect to the dashboard.
async fn callback(State(state): State<Shared>, Query(params): Query<CallbackParams>) -> Response {
    let Some(pending) = state
        .sessions
        .take_pending(&params.state)
        .await
        .ok()
        .flatten()
    else {
        return (StatusCode::BAD_REQUEST, "invalid or expired login state").into_response();
    };

    let exchange = match state
        .oidc
        .exchange_code(AuthorizationCode::new(params.code))
    {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("code exchange: {e}")).into_response(),
    };

    let token_response = match exchange
        .set_pkce_verifier(PkceCodeVerifier::new(pending.pkce_verifier))
        .request_async(&state.http)
        .await
    {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("token request: {e}")).into_response(),
    };

    let Some(id_token) = token_response.id_token() else {
        return (StatusCode::BAD_GATEWAY, "no id_token in response").into_response();
    };
    let claims = match id_token.claims(&state.oidc.id_token_verifier(), &Nonce::new(pending.nonce))
    {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("id_token: {e}")).into_response(),
    };

    let subject = claims.subject().as_str().to_string();
    let email = claims
        .email()
        .map(|e| e.as_str().to_string())
        .unwrap_or_default();

    let session_id = match state
        .sessions
        .create_session(&AdminSession { subject, email })
        .await
    {
        Ok(id) => id,
        Err(e) => return server_error(&e.to_string()),
    };

    let cookie = format!(
        "{SESSION_COOKIE}={session_id}; Path=/admin; HttpOnly; Secure; SameSite=Lax; Max-Age=28800"
    );
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, cookie),
            (header::LOCATION, "/admin".to_string()),
        ],
    )
        .into_response()
}

/// Clears the session and its cookie.
async fn logout(State(state): State<Shared>, headers: HeaderMap) -> Response {
    if let Some(id) = session_id_from(&headers) {
        let _ = state.sessions.delete_session(&id).await;
    }
    let cleared =
        format!("{SESSION_COOKIE}=; Path=/admin; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, cleared),
            (header::LOCATION, "/admin/login".to_string()),
        ],
    )
        .into_response()
}

// --- Admin routes (session-gated) ---------------------------------------------

/// Renders the dashboard from current control state. Unauthenticated -> login.
async fn dashboard(State(state): State<Shared>, headers: HeaderMap) -> Response {
    if authed(&state, &headers).await.is_none() {
        return Redirect::to("/admin/login").into_response();
    }
    match state.store_load().await {
        Ok(Some(view)) => match view.render() {
            Ok(html) => Html(html).into_response(),
            Err(e) => server_error(&format!("render: {e}")),
        },
        Ok(None) => (StatusCode::NOT_FOUND, "event not found").into_response(),
        Err(e) => server_error(&e),
    }
}

#[derive(Deserialize)]
struct PhaseForm {
    phase: String,
}

async fn set_phase(
    State(state): State<Shared>,
    headers: HeaderMap,
    Form(form): Form<PhaseForm>,
) -> Response {
    if authed(&state, &headers).await.is_none() {
        return Redirect::to("/admin/login").into_response();
    }
    finish(
        apply_phase(&state.store, &state.event_id, &form.phase)
            .await
            .map(|_| ()),
    )
}

#[derive(Deserialize)]
struct RateForm {
    rate: String,
}

async fn set_rate(
    State(state): State<Shared>,
    headers: HeaderMap,
    Form(form): Form<RateForm>,
) -> Response {
    if authed(&state, &headers).await.is_none() {
        return Redirect::to("/admin/login").into_response();
    }
    finish(
        apply_rate(&state.store, &state.event_id, &form.rate)
            .await
            .map(|_| ()),
    )
}

#[derive(Deserialize)]
struct MessageForm {
    message: String,
}

async fn set_message(
    State(state): State<Shared>,
    headers: HeaderMap,
    Form(form): Form<MessageForm>,
) -> Response {
    if authed(&state, &headers).await.is_none() {
        return Redirect::to("/admin/login").into_response();
    }
    finish(apply_message(&state.store, &state.event_id, &form.message).await)
}

async fn reset(State(state): State<Shared>, headers: HeaderMap) -> Response {
    if authed(&state, &headers).await.is_none() {
        return Redirect::to("/admin/login").into_response();
    }
    finish(apply_reset(&state.store, &state.event_id).await)
}

/// A route whose backing plane (authorizer / sessions) is not in the MVP.
async fn deferred() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "Not yet available — the authorizer and session plane ship after the MVP.",
    )
        .into_response()
}

/// Serves an embedded static asset (`/static/<path>`) with a guessed
/// content-type. Public (CSS carries no secrets). Unknown paths 404.
async fn static_asset(Path(path): Path<String>) -> Response {
    match StaticAssets::get(&path) {
        Some(file) => {
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            (
                [(header::CONTENT_TYPE, mime.as_ref().to_owned())],
                file.data.into_owned(),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

impl AppState {
    /// Loads control state and maps it to the dashboard view.
    async fn store_load(&self) -> Result<Option<Dashboard>, String> {
        use admin::Store;
        self.store
            .load(&self.event_id)
            .await
            .map(|opt| opt.as_ref().map(Dashboard::from_state))
            .map_err(|e| e.to_string())
    }
}

/// Turns an action result into a 303 redirect back to the dashboard on success
/// (POST/redirect/GET), or a 4xx/5xx with a plain-text reason on failure.
fn finish(result: Result<(), ApplyError>) -> Response {
    match result {
        Ok(()) => (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin")]).into_response(),
        Err(ApplyError::Action(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(ApplyError::Store(e)) => server_error(&e.to_string()),
    }
}

fn server_error(msg: &str) -> Response {
    tracing::error!(error = %msg, "admin handler error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
