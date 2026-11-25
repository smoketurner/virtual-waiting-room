//! The AWS side of the local stack: one plain-HTTP endpoint every binary's SDK
//! is pointed at, standing between it and the emulators.
//!
//! The Rust SDK has no setting for a custom certificate authority, and the AWS
//! Emulator only serves HTTPS with a self-signed one, so the binaries cannot
//! reach it directly. This endpoint takes their plain-HTTP calls and forwards
//! them over TLS with the emulator's CA trusted. Because every call passes
//! through, it is also where the run counts what DynamoDB was asked for and how
//! often it throttled — the numbers a deployed run would read from CloudWatch.
//!
//! `AmazonSQS.*` calls are not forwarded: the join queue lives in this process
//! (see `queue.rs`), and `open_event` is the one binary that speaks to it over
//! the wire.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};

use super::queue::JoinQueue;

/// AWS's documented example key pair. The emulator runs with signing off and
/// `ExtendDB`'s dev image accepts this pair, so nothing secret is involved.
pub const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
pub const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
pub const REGION: &str = "us-east-1";
pub const ACCOUNT_ID: &str = "000000000000";

/// The table names the binaries are configured with.
#[derive(Debug, Clone)]
pub struct Tables {
    pub counters: String,
    pub prequeue: String,
    pub positions: String,
    pub tokens: String,
}

impl Tables {
    #[must_use]
    pub fn new(prefix: &str) -> Self {
        Self {
            counters: format!("{prefix}-Counters"),
            prequeue: format!("{prefix}-PreQueue"),
            positions: format!("{prefix}-Positions"),
            tokens: format!("{prefix}-Tokens"),
        }
    }
}

/// Where DynamoDB calls go. Everything else goes to the AWS Emulator.
#[derive(Debug, Clone)]
pub struct Upstreams {
    pub emulator: String,
    pub dynamodb: String,
}

/// Calls the proxy has seen, per second and per operation.
#[derive(Debug, Default)]
pub struct AwsCounts {
    /// `(second, "DynamoDB.UpdateItem")` → calls.
    pub calls: BTreeMap<(u64, String), u64>,
    /// `(second, "DynamoDB.UpdateItem")` → throttled calls.
    pub throttled: BTreeMap<(u64, String), u64>,
    /// Errors other than a throttle or a conditional check, by operation and
    /// error type.
    pub errors: BTreeMap<String, u64>,
}

#[derive(Clone)]
struct ProxyState {
    client: reqwest::Client,
    upstreams: Upstreams,
    queue: Arc<JoinQueue>,
    counts: Arc<Mutex<AwsCounts>>,
    started: Instant,
}

/// Serves the endpoint on `listener` and returns the counts it keeps.
///
/// The listener is bound by the caller, so the join queue can be given this
/// endpoint's address before the endpoint is given the queue.
///
/// # Errors
///
/// The CA bundle cannot be read.
pub fn start_proxy(
    listener: tokio::net::TcpListener,
    upstreams: Upstreams,
    ca_pem: &[u8],
    queue: Arc<JoinQueue>,
    started: Instant,
) -> Result<Arc<Mutex<AwsCounts>>> {
    let ca = reqwest::Certificate::from_pem(ca_pem).context("read the emulator CA")?;
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .pool_max_idle_per_host(256)
        .build()
        .context("build the upstream client")?;
    let counts = Arc::new(Mutex::new(AwsCounts::default()));
    let state = ProxyState {
        client,
        upstreams,
        queue,
        counts: Arc::clone(&counts),
        started,
    };
    let app = axum::Router::new().fallback(proxy).with_state(state);
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(counts)
}

async fn proxy(State(state): State<ProxyState>, req: Request<Body>) -> Response<Body> {
    match forward(&state, req).await {
        Ok(response) => response,
        Err(e) => {
            let mut response = Response::new(Body::from(format!("{e:#}")));
            *response.status_mut() = StatusCode::BAD_GATEWAY;
            response
        }
    }
}

async fn forward(state: &ProxyState, req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, 64 * 1024 * 1024).await?;
    let target = parts
        .headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();

    if let Some(op) = target.strip_prefix("AmazonSQS.") {
        let (status, json) = state.queue.handle_wire(op, &body);
        return json_response(status, json, "application/x-amz-json-1.0");
    }

    let (service, op) = target
        .split_once('.')
        .unwrap_or(("unknown", target.as_str()));
    let label = if service.starts_with("DynamoDB") {
        format!("DynamoDB.{op}")
    } else {
        format!("{service}.{op}")
    };
    let base = if service.starts_with("DynamoDB") {
        &state.upstreams.dynamodb
    } else {
        &state.upstreams.emulator
    };
    let uri = format!(
        "{base}{}",
        parts.uri.path_and_query().map_or("/", |p| p.as_str())
    );

    let mut upstream = state.client.request(parts.method.clone(), uri);
    // The Host header is forwarded as the client sent it: the request is signed
    // over it, and an upstream that verifies signatures checks that value.
    for (name, value) in &parts.headers {
        if name.as_str() == "content-length" {
            continue;
        }
        upstream = upstream.header(name, value);
    }
    let reply = upstream.body(body).send().await?;
    let status = reply.status();
    let headers = reply.headers().clone();
    let bytes = reply.bytes().await?;

    record(state, &label, status, &bytes);

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    copy_headers(&headers, response.headers_mut());
    Ok(response)
}

fn record(state: &ProxyState, label: &str, status: StatusCode, body: &Bytes) {
    let second = state.started.elapsed().as_secs();
    let Ok(mut counts) = state.counts.lock() else {
        return;
    };
    *counts.calls.entry((second, label.to_owned())).or_default() += 1;
    if status.is_success() {
        return;
    }
    let kind = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("__type").and_then(|t| t.as_str()).map(str::to_owned))
        .unwrap_or_else(|| status.as_u16().to_string());
    let kind = kind.rsplit('#').next().unwrap_or(&kind).to_owned();
    match kind.as_str() {
        "ProvisionedThroughputExceededException" | "ThrottlingException" => {
            *counts
                .throttled
                .entry((second, label.to_owned()))
                .or_default() += 1;
        }
        // Expected outcomes of a guarded write, not faults.
        "ConditionalCheckFailedException" => {}
        _ => *counts.errors.entry(format!("{label} {kind}")).or_default() += 1,
    }
}

fn copy_headers(from: &reqwest::header::HeaderMap, to: &mut HeaderMap) {
    for (name, value) in from {
        if matches!(
            name.as_str(),
            "content-length" | "transfer-encoding" | "connection"
        ) {
            continue;
        }
        if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
            to.append(name.clone(), value);
        }
    }
}

fn json_response(
    status: StatusCode,
    json: serde_json::Value,
    content_type: &'static str,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::from(serde_json::to_vec(&json)?));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static(content_type));
    Ok(response)
}

/// SDK configuration for the harness's own setup, audit and operator actions,
/// through the proxy so they are counted with everything else.
pub async fn sdk_config(proxy: SocketAddr) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(format!("http://{proxy}"))
        .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
            ACCESS_KEY_ID,
            SECRET_ACCESS_KEY,
            None,
            None,
            "harness",
        ))
        .load()
        .await
}

/// Creates the four tables with the key schemas `core/main.tf` declares.
///
/// # Errors
///
/// A table already exists, or the backend refuses the call.
pub async fn create_tables(ddb: &aws_sdk_dynamodb::Client, tables: &Tables) -> Result<()> {
    for (name, key) in [
        (&tables.counters, "event_id"),
        (&tables.prequeue, "r"),
        (&tables.positions, "request_id"),
        (&tables.tokens, "request_id"),
    ] {
        ddb.create_table()
            .table_name(name)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(key)
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .send()
            .await
            .with_context(|| format!("create table {name}"))?;
    }
    for name in [
        &tables.counters,
        &tables.prequeue,
        &tables.positions,
        &tables.tokens,
    ] {
        wait_active(ddb, name).await?;
    }
    Ok(())
}

async fn wait_active(ddb: &aws_sdk_dynamodb::Client, name: &str) -> Result<()> {
    for _ in 0..120 {
        let out = ddb.describe_table().table_name(name).send().await?;
        let active = out
            .table()
            .and_then(|t| t.table_status())
            .is_some_and(|s| s.as_str() == "ACTIVE");
        if active {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!("table {name} did not become ACTIVE")
}

/// Writes the event item the way `aws_dynamodb_table_item.event` seeds it, with
/// a scheduled start: phase `pre_queue`, admission open, both sequences at
/// zero, the target rate, and `starts_at`.
///
/// # Errors
///
/// The write is refused.
pub async fn seed_event(
    ddb: &aws_sdk_dynamodb::Client,
    tables: &Tables,
    event_id: &str,
    target_rate: u32,
    starts_at: u64,
) -> Result<()> {
    let n = |v: u64| AttributeValue::N(v.to_string());
    ddb.put_item()
        .table_name(&tables.counters)
        .item("event_id", AttributeValue::S(format!("EVT#{event_id}")))
        .item("phase", AttributeValue::S("pre_queue".to_owned()))
        .item("admission_control", AttributeValue::S("open".to_owned()))
        .item("queue_counter", n(0))
        .item("serving_counter", n(0))
        .item("target_rate", n(u64::from(target_rate)))
        .item(wr_common::STARTS_AT_ATTR, n(starts_at))
        .send()
        .await
        .context("seed the event item")?;
    Ok(())
}

/// Puts the signing key where `generate_token` reads it.
///
/// # Errors
///
/// The emulator refuses the call.
pub async fn put_signing_key(ssm: &aws_sdk_ssm::Client, name: &str, key: &str) -> Result<()> {
    ssm.put_parameter()
        .name(name)
        .value(key)
        .r#type(aws_sdk_ssm::types::ParameterType::SecureString)
        .overwrite(true)
        .send()
        .await
        .context("put the signing key")?;
    Ok(())
}
