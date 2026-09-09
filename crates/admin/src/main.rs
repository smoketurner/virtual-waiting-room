//! Lambda entry point for the admin control plane: an Axum router over the
//! `SigV4` (`AWS_IAM`) admin routes, served through API Gateway via `lambda_http`.
//! API Gateway rejects unsigned requests before this runs, so the handlers
//! trust that reaching them means the caller was IAM-authorized.
use std::sync::Arc;

use admin::dynamo::DynamoStore;
use admin::templates::Dashboard;
use admin::{ApplyError, apply_message, apply_phase, apply_rate, apply_reset};
use askama::Template;
use axum::Form;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use lambda_http::Error;
use serde::Deserialize;

struct AppState {
    store: DynamoStore,
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

    let config = aws_config::load_from_env().await;
    let state = Arc::new(AppState {
        store: DynamoStore::new(
            aws_sdk_dynamodb::Client::new(&config),
            std::env::var("COUNTERS_TABLE")?,
        ),
        event_id: std::env::var("EVENT_ID")?,
    });

    let app = Router::new()
        .route("/admin", get(dashboard))
        .route("/metrics", get(dashboard))
        .route("/admin/metrics", get(dashboard))
        .route("/admin/phase", post(set_phase))
        .route("/admin/rate", post(set_rate))
        .route("/admin/message", post(set_message))
        .route("/admin/reset", post(reset))
        .route("/admin/rules", post(deferred))
        .route("/update_session", post(deferred))
        .with_state(state);

    lambda_http::run(app).await
}

/// Renders the dashboard from current control state.
async fn dashboard(State(state): State<Shared>) -> Response {
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

async fn set_phase(State(state): State<Shared>, Form(form): Form<PhaseForm>) -> Response {
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

async fn set_rate(State(state): State<Shared>, Form(form): Form<RateForm>) -> Response {
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

async fn set_message(State(state): State<Shared>, Form(form): Form<MessageForm>) -> Response {
    finish(apply_message(&state.store, &state.event_id, &form.message).await)
}

async fn reset(State(state): State<Shared>) -> Response {
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

/// Turns an action result into a redirect back to the dashboard on success, or
/// a 4xx/5xx with a plain-text reason on failure. A 303 keeps the POST/redirect/
/// GET pattern so a browser reload does not re-submit the action.
fn finish(result: Result<(), ApplyError>) -> Response {
    match result {
        Ok(()) => (StatusCode::SEE_OTHER, [("location", "/admin")]).into_response(),
        Err(ApplyError::Action(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(ApplyError::Store(e)) => server_error(&e.to_string()),
    }
}

fn server_error(msg: &str) -> Response {
    tracing::error!(error = %msg, "admin handler error");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
