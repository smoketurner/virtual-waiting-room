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

use admin::arrival::{ArrivalTime, arrival_layer};
use admin::dynamo::DynamoStore;
use admin::edge::KvsStore;
use admin::oidc::{self, OidcClient, OidcConfig};
use admin::scheduler::SchedulerStore;
use admin::sessions::{AdminSession, PendingLogin, SessionStore};
use admin::templates::Dashboard;
use admin::{
    ApplyError, EdgeConfigStore, apply_fail_open, apply_message, apply_pause, apply_phase,
    apply_rate, apply_recover, apply_reset, apply_resume, apply_set_rules, apply_start_time,
    format_rules, parse_rules,
};
use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lambda_http::Error;
use openidconnect::core::CoreResponseType;
use openidconnect::{
    AuthenticationFlow, AuthorizationCode, CsrfToken, Nonce, PkceCodeChallenge, PkceCodeVerifier,
    Scope, TokenResponse,
};
use rust_embed::RustEmbed;
use serde::Deserialize;
use tower::Layer as _;

/// Name of the opaque session cookie.
const SESSION_COOKIE: &str = "vwr_admin_session";
/// Name of the short-lived cookie binding a login to the browser that started
/// it: holds the CSRF state, checked against the callback's `state` param.
const STATE_COOKIE: &str = "vwr_admin_login_state";

/// Static assets (CSS) embedded into the binary at build time.
#[derive(RustEmbed)]
#[folder = "static/"]
struct StaticAssets;

struct AppState {
    store: DynamoStore,
    edge: KvsStore,
    /// The one-time seal schedule the operator's start time writes (issue
    /// #128).
    schedule: SchedulerStore,
    sessions: SessionStore,
    oidc: OidcClient,
    http: reqwest::Client,
    event_id: String,
    /// Emails permitted to hold an admin session. Empty = deny all (fail closed
    /// for a control plane): an OIDC identity not on this list is rejected.
    allowed_emails: Vec<String>,
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

    let kvs = aws_sdk_cloudfrontkeyvaluestore::Client::new(&config);
    let scheduler = aws_sdk_scheduler::Client::new(&config);

    let state = Arc::new(AppState {
        store: DynamoStore::new(dynamo.clone(), std::env::var("COUNTERS_TABLE")?),
        edge: KvsStore::new(kvs, std::env::var("EDGE_KVS_ARN")?),
        schedule: SchedulerStore::new(scheduler, std::env::var("SEAL_SCHEDULE_NAME")?),
        sessions: SessionStore::new(dynamo, std::env::var("TOKENS_TABLE")?),
        oidc,
        http,
        event_id: std::env::var("EVENT_ID")?,
        // Comma-separated allowlist; entries trimmed and lowercased. Empty (unset)
        // = deny all — the operator must configure who may log in.
        allowed_emails: std::env::var("OIDC_ALLOWED_EMAILS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty())
            .collect(),
    });

    let app = Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/state", get(state_json))
        .route("/admin/login", get(login))
        .route("/admin/callback", get(callback))
        .route("/admin/logout", get(logout))
        .route("/admin/phase", post(set_phase))
        .route("/admin/rate", post(set_rate))
        .route("/admin/message", post(set_message))
        .route("/admin/start_time", post(set_start_time))
        .route("/admin/reset", post(reset))
        .route("/admin/pause", post(pause))
        .route("/admin/resume", post(resume))
        .route("/admin/fail_open", post(fail_open))
        .route("/admin/recover", post(recover))
        .route("/admin/rules", post(set_rules))
        .route("/update_session", post(deferred))
        .route("/static/{*path}", get(static_asset))
        .with_state(state)
        // Security-headers middleware: apply the hardening + no-cache headers to
        // every response. The dashboard handler sets its own nonce'd CSP first;
        // apply_hardening leaves an existing CSP intact, so this adds the policy
        // only where a handler did not (everything but the dashboard).
        .layer(axum::middleware::map_response(harden_response))
        // Outermost: every handler below is served at one instant, stamped
        // before any other layer can await.
        .layer(axum::middleware::from_fn(arrival_layer));

    // Trim a trailing slash before routing so /admin/ resolves to the /admin
    // route (and /admin/phase/ to /admin/phase, etc.) — the same handler, not a
    // redirect. API Gateway collapses /admin/ onto the admin Lambda but forwards
    // the trailing slash; without this the axum router would 404 on it.
    let app = tower_http::normalize_path::NormalizePathLayer::trim_trailing_slash().layer(app);

    lambda_http::run(app).await
}

/// Response middleware: stamp the hardening + no-cache headers on every response
/// (empty nonce — a handler that needs an inline script sets its own nonce'd CSP,
/// which `apply_hardening` preserves).
async fn harden_response(mut response: Response) -> Response {
    admin::security::apply_hardening(response.headers_mut(), "");
    response
}

// --- Auth helpers -------------------------------------------------------------

/// Reads a named cookie's value from the request cookie header, if present.
fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie::Cookie::split_parse(raw)
        .filter_map(Result::ok)
        .find(|c| c.name() == name)
        .map(|c| c.value().to_string())
}

/// Extracts the session id from the request cookie header, if present.
fn session_id_from(headers: &HeaderMap) -> Option<String> {
    cookie_value(headers, SESSION_COOKIE)
}

/// Returns the authenticated session, or `None` if the request is unauthenticated.
async fn authed(state: &Shared, headers: &HeaderMap, now: ArrivalTime) -> Option<AdminSession> {
    let id = session_id_from(headers)?;
    state.sessions.load_session(&id, now).await.ok().flatten()
}

// --- OIDC flow ----------------------------------------------------------------

/// Starts the login: build the authorize URL with PKCE, persist the transaction
/// keyed by CSRF state, redirect the browser to the provider.
async fn login(State(state): State<Shared>, now: ArrivalTime) -> Response {
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf, nonce) = state
        .oidc
        .authorize_url(
            AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        // openidconnect adds the required `openid` scope automatically; only
        // the extra `email` scope is requested here.
        .add_scope(Scope::new("email".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    let pending = PendingLogin {
        pkce_verifier: pkce_verifier.secret().clone(),
        nonce: nonce.secret().clone(),
    };
    if let Err(e) = state
        .sessions
        .put_pending(csrf.secret(), &pending, now)
        .await
    {
        return server_error(&e.to_string());
    }
    // Bind this login to the browser that started it: a short-lived cookie
    // holding the CSRF state, required to match the callback's state param. A
    // state stolen from elsewhere cannot complete a login without this cookie.
    let state_cookie = format!(
        "{STATE_COOKIE}={}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600",
        csrf.secret()
    );
    (
        StatusCode::SEE_OTHER,
        [
            (header::SET_COOKIE, state_cookie),
            (header::LOCATION, auth_url.to_string()),
        ],
    )
        .into_response()
}

#[derive(Deserialize)]
struct CallbackParams {
    code: String,
    state: String,
}

/// OIDC redirect target: exchange the code, verify the ID token, create a
/// session, set the cookie, redirect to the dashboard.
async fn callback(
    State(state): State<Shared>,
    headers: HeaderMap,
    now: ArrivalTime,
    Query(params): Query<CallbackParams>,
) -> Response {
    // Browser binding: the callback must carry the login-state cookie set at
    // /admin/login, and it must equal the returned state. Rejects a state
    // replayed from a different browser.
    match cookie_value(&headers, STATE_COOKIE) {
        Some(bound) if bound == params.state => {}
        _ => return (StatusCode::BAD_REQUEST, "login state mismatch").into_response(),
    }

    let Some(pending) = state
        .sessions
        .take_pending(&params.state, now)
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

    // Operator allowlist: only configured emails may hold an admin session.
    // Empty allowlist = deny all (fail closed).
    if !state.allowed_emails.contains(&email.to_ascii_lowercase()) {
        tracing::warn!(subject = %subject, "admin login denied: email not in allowlist");
        return (StatusCode::FORBIDDEN, "not authorized for admin access").into_response();
    }

    let session_id = match state
        .sessions
        .create_session(&AdminSession { subject, email }, now)
        .await
    {
        Ok(id) => id,
        Err(e) => return server_error(&e.to_string()),
    };

    let cookie = format!(
        "{SESSION_COOKIE}={session_id}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=28800"
    );
    // Clear the one-shot login-state cookie now that it has been consumed.
    let clear_state = format!("{STATE_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
    // Two Set-Cookie headers: append rather than a header array (which would
    // insert-overwrite the first).
    let mut response = (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin")]).into_response();
    let set_cookie = |v: String| {
        v.parse()
            .map_err(|e| tracing::error!(error = %e, "invalid Set-Cookie value"))
            .ok()
    };
    if let Some(c) = set_cookie(cookie) {
        response.headers_mut().append(header::SET_COOKIE, c);
    }
    if let Some(c) = set_cookie(clear_state) {
        response.headers_mut().append(header::SET_COOKIE, c);
    }
    response
}

/// Clears the session and its cookie.
async fn logout(State(state): State<Shared>, headers: HeaderMap) -> Response {
    if let Some(id) = session_id_from(&headers) {
        let _ = state.sessions.delete_session(&id).await;
    }
    let cleared = format!("{SESSION_COOKIE}=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
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
async fn dashboard(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    match state.store_load(now).await {
        Ok(Some(mut view)) => {
            view.csp_nonce = admin::security::nonce();
            view.operator_email = session.email;
            // Rules live only in the KeyValueStore (issue #71), not in
            // ControlState, so the current ruleset is a second read. A
            // failure here must not break the whole dashboard — the operator
            // still needs to see phase/rate/message/admission — so it is
            // logged and the rules form is hidden rather than shown empty:
            // an empty textarea is indistinguishable from a real dormant
            // ruleset, and submitting it would overwrite the real one.
            match state.edge.read_config().await {
                Ok(cfg) => view.rules_text = format_rules(&cfg.rules),
                Err(e) => {
                    tracing::warn!(error = %e, "could not read the current ruleset");
                    view.rules_load_failed = true;
                }
            }
            match view.render() {
                Ok(html) => {
                    let mut response = Html(html).into_response();
                    admin::security::apply_hardening(response.headers_mut(), &view.csp_nonce);
                    response
                }
                Err(e) => server_error(&format!("render: {e}")),
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, "event not found").into_response(),
        Err(e) => server_error(&e),
    }
}

/// Current control state as JSON for the dashboard's poller. Session-gated like
/// the dashboard; returns the same view the HTML renders.
async fn state_json(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    if authed(&state, &headers, now).await.is_none() {
        return (StatusCode::UNAUTHORIZED, "not authenticated").into_response();
    }
    match state.store_load(now).await {
        Ok(Some(view)) => axum::Json(view).into_response(),
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
    now: ArrivalTime,
    Form(form): Form<PhaseForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_phase(
            &state.store,
            &state.event_id,
            &form.phase,
            &session.email,
            now,
        )
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
    now: ArrivalTime,
    Form(form): Form<RateForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_rate(
            &state.store,
            &state.event_id,
            &form.rate,
            &session.email,
            now,
        )
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
    now: ArrivalTime,
    Form(form): Form<MessageForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_message(
            &state.store,
            &state.event_id,
            &form.message,
            &session.email,
            now,
        )
        .await,
    )
}

/// The start-time form (issue #128). Both fields are plain form values, so the
/// control works with JavaScript disabled; `starts_at` empty clears the
/// schedule. The timezone is submitted rather than inferred because a plain
/// POST carries no browser zone, and validated server-side because its value
/// is whatever the client sent, not necessarily one the dropdown offered.
#[derive(Deserialize)]
struct StartTimeForm {
    starts_at: String,
    timezone: String,
}

async fn set_start_time(
    State(state): State<Shared>,
    headers: HeaderMap,
    now: ArrivalTime,
    Form(form): Form<StartTimeForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_start_time(
            &state.store,
            &state.schedule,
            &state.event_id,
            &form.starts_at,
            &form.timezone,
            &session.email,
            now,
        )
        .await,
    )
}

async fn reset(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(apply_reset(&state.store, &state.event_id, &session.email, now).await)
}

/// Holds admission while the queue keeps forming. Reversible, no confirmation.
async fn pause(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(apply_pause(&state.store, &state.event_id, &session.email, now).await)
}

/// Resume admission after a pause.
async fn resume(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(apply_resume(&state.store, &state.event_id, &session.email, now).await)
}

#[derive(Deserialize)]
struct FailOpenForm {
    minutes: String,
}

/// Engages fail-open (issue #71): break-glass, until the given duration
/// lapses on its own. Legal regardless of pause state.
async fn fail_open(
    State(state): State<Shared>,
    headers: HeaderMap,
    now: ArrivalTime,
    Form(form): Form<FailOpenForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_fail_open(
            &state.store,
            &state.edge,
            &state.event_id,
            &form.minutes,
            &session.email,
            now,
        )
        .await,
    )
}

/// Clears the fail-open epoch. Not "resume": under the split this only
/// clears the epoch, so a pause queued during the window still applies.
async fn recover(State(state): State<Shared>, headers: HeaderMap, now: ArrivalTime) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    finish(
        apply_recover(
            &state.store,
            &state.edge,
            &state.event_id,
            &session.email,
            now,
        )
        .await,
    )
}

#[derive(Deserialize)]
struct RulesForm {
    rules: String,
}

/// Replaces the edge gate's ruleset (issue #71). One rule per line, in the
/// same tag vocabulary as the `KeyValueStore` wire form: `p <prefix>`,
/// `c <name>`, `u <substring>`, `h <name> <value>`. Blank lines and lines
/// starting with `#` are ignored, so an operator can leave the form
/// human-readable. Validation (per-field bounds, rule count, byte ceiling) is
/// `apply_set_rules`'s job; a parse failure here is reported the same way —
/// a plain-text 400 naming exactly what was wrong, so it works with
/// JavaScript disabled.
async fn set_rules(
    State(state): State<Shared>,
    headers: HeaderMap,
    now: ArrivalTime,
    Form(form): Form<RulesForm>,
) -> Response {
    let Some(session) = authed(&state, &headers, now).await else {
        return Redirect::to("/admin/login").into_response();
    };
    let rules = match parse_rules(&form.rules) {
        Ok(rules) => rules,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    finish(
        apply_set_rules(
            &state.store,
            &state.edge,
            &state.event_id,
            rules,
            &session.email,
            now,
        )
        .await,
    )
}

/// A route whose backing plane (authorizer / sessions) is not in the MVP.
async fn deferred() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "Not yet available. The authorizer and session plane ship after the MVP.",
    )
        .into_response()
}

/// Serves an embedded static asset (`/static/<path>`) with a guessed
/// content-type. Public (CSS and fonts carry no secrets). Unknown paths 404.
///
/// The assets are compiled into the binary, so their content changes only when
/// the Lambda is redeployed. The `ETag` is the embedded file's own hash, which
/// makes a repeat request a 304 with no body — the fonts are the bulk of the
/// page weight, and the admin behaviour is uncached at the edge, so without a
/// validator every dashboard load pulls them through the Lambda again.
///
/// `max-age` is deliberately short. The URL carries no content hash, so a long
/// one would serve a stale stylesheet after a deploy; revalidation is what
/// keeps it correct, and the 304 is what makes it cheap.
async fn static_asset(headers: HeaderMap, Path(path): Path<String>) -> Response {
    let Some(file) = StaticAssets::get(&path) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };

    // The hash is over the file's own bytes, computed when it is embedded, so
    // editing an asset and redeploying changes the tag. base64url rather than
    // hex because an ETag is an opaque string and this is one call.
    let etag = format!(
        "\"{}\"",
        URL_SAFE_NO_PAD.encode(file.metadata.sha256_hash())
    );
    let cache_control = "public, max-age=300, must-revalidate";

    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| admin::if_none_match(v, &etag))
    {
        return (
            StatusCode::NOT_MODIFIED,
            [
                (header::ETAG, etag),
                (header::CACHE_CONTROL, cache_control.to_owned()),
            ],
        )
            .into_response();
    }

    let mime = mime_guess::from_path(&path).first_or_octet_stream();
    (
        [
            (header::CONTENT_TYPE, mime.as_ref().to_owned()),
            (header::ETAG, etag),
            (header::CACHE_CONTROL, cache_control.to_owned()),
        ],
        file.data.into_owned(),
    )
        .into_response()
}

impl AppState {
    /// Loads control state and maps it to the dashboard view, resolved at
    /// `now`.
    async fn store_load(&self, now: ArrivalTime) -> Result<Option<Dashboard>, String> {
        use admin::Store;
        let now = now.epoch_seconds();
        self.store
            .load(&self.event_id)
            .await
            .map(|opt| opt.as_ref().map(|state| Dashboard::from_state(state, now)))
            .map_err(|e| e.to_string())
    }
}

/// Turns an action result into a 303 redirect back to the dashboard on success
/// (POST/redirect/GET), or a status + plain-text reason on failure.
fn finish(result: Result<(), ApplyError>) -> Response {
    use admin::{ActionError, StoreError};
    match result {
        Ok(()) => (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin")]).into_response(),
        // Debounce rejection — a double-click / fast toggle.
        Err(ApplyError::Action(ActionError::TooFast)) => (
            StatusCode::TOO_MANY_REQUESTS,
            "Too soon after the previous change. Wait a moment and retry.",
        )
            .into_response(),
        Err(ApplyError::Action(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        // Lost race or no-op guard (e.g. pausing when already paused).
        Err(ApplyError::Store(StoreError::Conflict)) => (
            StatusCode::CONFLICT,
            "State changed underneath you (or there is no change to make). Reload and retry.",
        )
            .into_response(),
        Err(ApplyError::Store(e)) => server_error(&e.to_string()),
    }
}

fn server_error(msg: &str) -> Response {
    tracing::error!(error = %msg, "admin handler error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
