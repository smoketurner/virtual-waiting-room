//! The edge: what CloudFront and API Gateway do between a visitor and the
//! functions.
//!
//! - **Behaviours.** Paths are routed the way `edge/main.tf` orders its cache
//!   behaviours: the polled API, the join, the write API, the pages, and the
//!   protected default behind the gate.
//! - **Caching and collapsing.** The polled paths are cached for the 1 s
//!   minimum TTL — `/status` on its path alone, `/queue_num` on its query —
//!   and concurrent misses on one key make one origin fetch. Errors CloudFront
//!   caches (404, 429, 5xx) are held for the 1 s `error_caching_min_ttl`. This
//!   is one edge location; a real event spreads across many, each with its own
//!   cache.
//! - **API Gateway.** The account's 10,000 requests/s token bucket (5,000
//!   burst) and the stage's per-method throttles from `core/api.tf`; the join
//!   method's content-type rule and JSON schema; the REST proxy event the
//!   functions receive.
//! - **The gate.** The shipped CloudFront Function, run by the Node sidecar,
//!   decides every protected request.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;
use tokio::sync::OnceCell;

use super::lambda::Function;
use super::metrics::Metrics;
use super::queue::JoinQueue;

/// A request as a visitor's browser sends it.
#[derive(Debug, Clone, Default)]
pub struct ViewerRequest {
    pub method: &'static str,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub cookies: Vec<(String, String)>,
    pub body: Option<String>,
    pub content_type: Option<&'static str>,
    /// Navigation (`Accept: text/html`) or a script's fetch.
    pub navigation: bool,
    pub viewer_ip: String,
}

/// What the browser gets back.
#[derive(Debug, Clone)]
pub struct ViewerResponse {
    pub status: u16,
    pub body: Arc<String>,
    pub set_cookies: Vec<String>,
}

impl ViewerResponse {
    fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: Arc::new(body.into()),
            set_cookies: Vec::new(),
        }
    }

    #[must_use]
    pub fn json(&self) -> Option<serde_json::Value> {
        serde_json::from_str(&self.body).ok()
    }
}

/// A token bucket, as API Gateway throttles with.
#[derive(Debug)]
struct Bucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rate: f64, burst: f64) -> Self {
        Self {
            rate,
            burst,
            tokens: burst,
            last: Instant::now(),
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last = now;
    }
}

/// The throttle limits a run uses.
#[derive(Debug, Clone, Copy)]
pub struct Throttles {
    pub account_rate: f64,
    pub account_burst: f64,
    pub join_rate: f64,
    pub join_burst: f64,
    pub method_rate: f64,
    pub method_burst: f64,
}

struct Gateway {
    account: Bucket,
    methods: HashMap<&'static str, Bucket>,
}

impl Gateway {
    /// Takes one token from the method's bucket and the account's, or neither.
    fn admit(&mut self, method: &'static str) -> bool {
        let now = Instant::now();
        self.account.refill(now);
        let Some(bucket) = self.methods.get_mut(method) else {
            return false;
        };
        bucket.refill(now);
        if bucket.tokens >= 1.0 && self.account.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            self.account.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct Cached {
    response: ViewerResponse,
    expires: Instant,
}

type Slot = Arc<OnceCell<Cached>>;

pub struct Edge {
    gateway: Mutex<Gateway>,
    cache: Mutex<HashMap<String, Slot>>,
    read: Arc<Function>,
    generate_token: Arc<Function>,
    queue: Arc<JoinQueue>,
    gate: reqwest::Client,
    gate_url: String,
    event_id_max: usize,
    pub metrics: Arc<Metrics>,
    request_ids: AtomicU64,
}

/// The JavaScript API paths and their API Gateway method keys.
const STATUS: &str = "/_wr/v1/status";
const QUEUE_NUM: &str = "/_wr/v1/queue_num";
const JOIN: &str = "/_wr/v1/join";
const GENERATE_TOKEN: &str = "/_wr/v1/generate_token";

impl Edge {
    #[must_use]
    pub fn new(
        throttles: Throttles,
        read: Arc<Function>,
        generate_token: Arc<Function>,
        queue: Arc<JoinQueue>,
        gate_url: String,
        event_id: &str,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut methods = HashMap::new();
        methods.insert(JOIN, Bucket::new(throttles.join_rate, throttles.join_burst));
        for path in [STATUS, QUEUE_NUM, GENERATE_TOKEN] {
            methods.insert(
                path,
                Bucket::new(throttles.method_rate, throttles.method_burst),
            );
        }
        Self {
            gateway: Mutex::new(Gateway {
                account: Bucket::new(throttles.account_rate, throttles.account_burst),
                methods,
            }),
            cache: Mutex::new(HashMap::new()),
            read,
            generate_token,
            queue,
            // Idle connections are dropped well before Node's 60 s keep-alive
            // closes them, or a request can be written to a socket the
            // sidecar has just closed.
            gate: reqwest::Client::builder()
                .pool_max_idle_per_host(256)
                .pool_idle_timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_default(),
            gate_url,
            event_id_max: event_id.len(),
            metrics,
            request_ids: AtomicU64::new(0),
        }
    }

    /// One viewer request, end to end.
    pub async fn request(&self, req: ViewerRequest) -> ViewerResponse {
        let path = req.path.clone();
        let route: &'static str = match path.as_str() {
            STATUS => STATUS,
            QUEUE_NUM => QUEUE_NUM,
            JOIN => JOIN,
            GENERATE_TOKEN => GENERATE_TOKEN,
            p if p.starts_with("/_wr/") => "pages",
            _ => "protected",
        };
        let response = match route {
            STATUS => self.cached(route, STATUS.to_owned(), &req).await,
            QUEUE_NUM => {
                let mut key = QUEUE_NUM.to_owned();
                for (k, v) in &req.query {
                    if k == "request_id" || k == "event_id" {
                        key.push_str(&format!("&{k}={v}"));
                    }
                }
                self.cached(route, key, &req).await
            }
            JOIN => self.join(&req),
            GENERATE_TOKEN => self.api(route, &req).await,
            "pages" => ViewerResponse::new(200, "<html></html>"),
            _ => self.protected(&req).await,
        };
        self.metrics.viewer(route, response.status);
        response
    }

    async fn cached(
        &self,
        route: &'static str,
        key: String,
        req: &ViewerRequest,
    ) -> ViewerResponse {
        loop {
            let slot = {
                let Ok(mut cache) = self.cache.lock() else {
                    return ViewerResponse::new(500, "cache poisoned");
                };
                let fresh = cache
                    .get(&key)
                    .filter(|slot| slot.get().is_none_or(|c| c.expires > Instant::now()));
                if let Some(slot) = fresh {
                    Arc::clone(slot)
                } else {
                    let slot: Slot = Arc::new(OnceCell::new());
                    cache.insert(key.clone(), Arc::clone(&slot));
                    slot
                }
            };
            let mut fetched = false;
            let cached = slot
                .get_or_init(|| async {
                    fetched = true;
                    let response = self.api(route, req).await;
                    let ttl = match response.status {
                        200 | 404 | 429 | 500 | 502 | 503 | 504 => Duration::from_secs(1),
                        _ => Duration::ZERO,
                    };
                    Cached {
                        response,
                        expires: Instant::now() + ttl,
                    }
                })
                .await;
            if fetched {
                self.metrics.origin(route);
            }
            if cached.expires > Instant::now() || fetched {
                return cached.response.clone();
            }
            // An uncacheable answer someone else fetched: fetch our own.
        }
    }

    fn join(&self, req: &ViewerRequest) -> ViewerResponse {
        if !self.throttle(JOIN) {
            return ViewerResponse::new(429, r#"{"message":"Too Many Requests"}"#);
        }
        if req.content_type != Some("application/json") {
            return ViewerResponse::new(415, r#"{"message":"Unsupported Media Type"}"#);
        }
        let Some(body) = req.body.as_deref() else {
            return ViewerResponse::new(400, r#"{"message":"Invalid request body"}"#);
        };
        if !self.join_schema_accepts(body) {
            return ViewerResponse::new(400, r#"{"message":"Invalid request body"}"#);
        }
        self.queue.send(body.to_owned());
        self.metrics.origin(JOIN);
        ViewerResponse::new(200, "{}")
    }

    /// `aws_api_gateway_model.join`: exactly `request_id`, `event_id` and `h`,
    /// each a bounded string, `h` 43 base64url characters.
    fn join_schema_accepts(&self, body: &str) -> bool {
        let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(body)
        else {
            return false;
        };
        if map.len() != 3 {
            return false;
        }
        let field = |name: &str, min: usize, max: usize| {
            map.get(name)
                .and_then(|v| v.as_str())
                .is_some_and(|s| (min..=max).contains(&s.len()))
        };
        let h_ok = map.get("h").and_then(|v| v.as_str()).is_some_and(|h| {
            h.len() == 43
                && h.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        });
        field("request_id", 1, 36) && field("event_id", 1, self.event_id_max) && h_ok
    }

    fn throttle(&self, method: &'static str) -> bool {
        let admitted = self
            .gateway
            .lock()
            .map(|mut g| g.admit(method))
            .unwrap_or(false);
        if !admitted {
            self.metrics.throttled(method);
        }
        admitted
    }

    /// API Gateway in front of a Lambda proxy integration.
    async fn api(&self, route: &'static str, req: &ViewerRequest) -> ViewerResponse {
        if !self.throttle(route) {
            return ViewerResponse::new(429, r#"{"message":"Too Many Requests"}"#);
        }
        let function = if route == GENERATE_TOKEN {
            &self.generate_token
        } else {
            &self.read
        };
        let event = self.proxy_event(req);
        match function.invoke(&event).await {
            Ok(reply) => proxy_response(&reply),
            Err(_) => ViewerResponse::new(502, r#"{"message":"Internal server error"}"#),
        }
    }

    fn proxy_event(&self, req: &ViewerRequest) -> serde_json::Value {
        let id = self.request_ids.fetch_add(1, Ordering::Relaxed);
        let query: serde_json::Map<String, serde_json::Value> = req
            .query
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        let multi_query: serde_json::Map<String, serde_json::Value> = req
            .query
            .iter()
            .map(|(k, v)| (k.clone(), json!([v])))
            .collect();
        let mut headers = serde_json::Map::new();
        if let Some(ct) = req.content_type {
            headers.insert("content-type".to_owned(), json!(ct));
        }
        // `origin_request_policy.generate_token` forwards the viewer address.
        headers.insert(
            "cloudfront-viewer-address".to_owned(),
            json!(format!("{}:443", req.viewer_ip)),
        );
        let multi_headers: serde_json::Map<String, serde_json::Value> = headers
            .iter()
            .map(|(k, v)| (k.clone(), json!([v])))
            .collect();
        json!({
            "resource": req.path,
            "path": req.path,
            "httpMethod": req.method,
            "headers": headers,
            "multiValueHeaders": multi_headers,
            "queryStringParameters": if query.is_empty() { serde_json::Value::Null } else { json!(query) },
            "multiValueQueryStringParameters": if multi_query.is_empty() { serde_json::Value::Null } else { json!(multi_query) },
            "pathParameters": null,
            "stageVariables": null,
            "requestContext": {
                "resourcePath": req.path,
                "httpMethod": req.method,
                "path": format!("/dev{}", req.path),
                "stage": "dev",
                "requestId": format!("req-{id}"),
                "accountId": super::aws::ACCOUNT_ID,
                "apiId": "local",
                "identity": { "sourceIp": req.viewer_ip },
                "protocol": "HTTP/1.1",
            },
            "body": req.body,
            "isBase64Encoded": false,
        })
    }

    /// The protected behaviour: the gate decides, and a request it passes
    /// reaches the client origin, which counts it.
    async fn protected(&self, req: &ViewerRequest) -> ViewerResponse {
        let cookies: serde_json::Map<String, serde_json::Value> = req
            .cookies
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        let accept = if req.navigation {
            "text/html"
        } else {
            "application/json"
        };
        let viewer = json!({
            "method": req.method,
            "uri": req.path,
            "ip": req.viewer_ip,
            "headers": { "accept": accept },
            "cookies": cookies,
        });
        let decision = match self.ask_gate(&viewer).await {
            Ok(d) => d,
            Err(e) => return ViewerResponse::new(599, format!("gate sidecar: {e:#}")),
        };
        if decision.get("pass").and_then(serde_json::Value::as_bool) == Some(true) {
            if decision.get("failed").and_then(serde_json::Value::as_bool) == Some(true) {
                self.metrics.gate_failed_open();
            }
            self.metrics.origin("protected");
            return ViewerResponse::new(200, "origin");
        }
        let status = decision
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .and_then(|s| u16::try_from(s).ok())
            .unwrap_or(403);
        ViewerResponse::new(status, decision.to_string())
    }

    /// The sidecar is the harness's plumbing, not the system under test, so a
    /// connection that fails is retried rather than reported as a refusal.
    async fn ask_gate(&self, viewer: &serde_json::Value) -> Result<serde_json::Value> {
        let mut last = None;
        for _ in 0..3 {
            match self.gate.post(&self.gate_url).json(viewer).send().await {
                Ok(reply) => {
                    anyhow::ensure!(
                        reply.status().is_success(),
                        "gate answered {}",
                        reply.status()
                    );
                    return Ok(reply.json().await?);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.map_or_else(|| anyhow::anyhow!("gate unreachable"), anyhow::Error::from))
    }
}

/// Maps a Lambda proxy response back to what the viewer receives.
fn proxy_response(reply: &serde_json::Value) -> ViewerResponse {
    let status = reply
        .get("statusCode")
        .and_then(serde_json::Value::as_u64)
        .and_then(|s| u16::try_from(s).ok())
        .unwrap_or(502);
    let body = reply
        .get("body")
        .and_then(|b| b.as_str())
        .unwrap_or_default()
        .to_owned();
    let mut response = ViewerResponse::new(status, body);
    let mut cookies: Vec<String> = Vec::new();
    if let Some(list) = reply.get("cookies").and_then(|c| c.as_array()) {
        cookies.extend(list.iter().filter_map(|v| v.as_str().map(str::to_owned)));
    }
    for key in ["multiValueHeaders", "headers"] {
        if !cookies.is_empty() {
            break;
        }
        let Some(headers) = reply.get(key).and_then(|h| h.as_object()) else {
            continue;
        };
        for (name, value) in headers {
            if !name.eq_ignore_ascii_case("set-cookie") {
                continue;
            }
            match value {
                serde_json::Value::Array(values) => {
                    cookies.extend(values.iter().filter_map(|v| v.as_str().map(str::to_owned)));
                }
                other => cookies.extend(other.as_str().map(str::to_owned)),
            }
        }
    }
    response.set_cookies = cookies;
    response
}

/// Serves the edge over HTTP, for pages driven by the shipped `waiting.js`.
///
/// # Errors
///
/// The port cannot be bound.
pub async fn serve(edge: Arc<Edge>) -> Result<std::net::SocketAddr> {
    let app = axum::Router::new().fallback(over_http).with_state(edge);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(addr)
}

async fn over_http(
    axum::extract::State(edge): axum::extract::State<Arc<Edge>>,
    req: axum::http::Request<axum::body::Body>,
) -> axum::http::Response<axum::body::Body> {
    let (parts, body) = req.into_parts();
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let query = parts
        .uri
        .query()
        .unwrap_or_default()
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_owned(), percent_decode(v)))
        .collect();
    let cookies = header("cookie")
        .split(';')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    let body = axum::body::to_bytes(body, 1024 * 1024)
        .await
        .unwrap_or_default();
    let method = match parts.method.as_str() {
        "POST" => "POST",
        _ => "GET",
    };
    let request = ViewerRequest {
        method,
        path: parts.uri.path().to_owned(),
        query,
        cookies,
        body: (!body.is_empty()).then(|| String::from_utf8_lossy(&body).into_owned()),
        content_type: header("content-type")
            .starts_with("application/json")
            .then_some("application/json"),
        navigation: header("accept").contains("text/html"),
        viewer_ip: "10.255.0.1".to_owned(),
    };
    let response = edge.request(request).await;
    let mut out = axum::http::Response::new(axum::body::Body::from(response.body.to_string()));
    *out.status_mut() = axum::http::StatusCode::from_u16(response.status)
        .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
    for cookie in &response.set_cookies {
        if let Ok(value) = axum::http::HeaderValue::from_str(cookie) {
            out.headers_mut().append("set-cookie", value);
        }
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let decoded = (bytes[i] == b'%')
            .then(|| s.get(i + 1..i + 3))
            .flatten()
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        if let Some(b) = decoded {
            out.push(b);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
