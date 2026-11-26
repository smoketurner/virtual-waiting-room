//! Runs the shipped Lambda binaries as local processes.
//!
//! Each function gets a listener speaking the Lambda Runtime API — the same
//! `GET /runtime/invocation/next`, `POST .../response` protocol Lambda serves a
//! custom runtime — and a pool of its built `bootstrap` binary, each process
//! one execution environment. An invocation waits for a free environment, as
//! one does under reserved concurrency. Because every process keeps its own
//! memory, the per-environment caches the design leans on (`read`'s one-second
//! `Counters` copy, `generate_token`'s spent-shard memo) behave per
//! environment here as they do on Lambda.
//!
//! A pool starts at its configured size and grows, up to a ceiling, whenever an
//! invocation arrives with no environment idle — Lambda's scale-out, without
//! its cold-start latency or its scaling-rate limit. Environments never
//! recycle.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::{Body, Bytes};
use axum::extract::{Path as UrlPath, State};
use axum::http::{Response, StatusCode};
use axum::routing::{get, post};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, mpsc, oneshot};

type Reply = Result<Bytes, String>;

/// Why an invocation did not produce a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Failure {
    /// The function reported an error.
    FunctionError,
    /// No environment answered within the function's timeout. With the
    /// functions healthy this means the machine running the harness is
    /// saturated.
    Timeout,
    /// The pool shut down underneath the call.
    Dropped,
    /// The request or the response was not JSON.
    Unreadable,
}

struct Pending {
    id: String,
    payload: Vec<u8>,
    reply: oneshot::Sender<Reply>,
}

struct HostState {
    queue: Mutex<mpsc::Receiver<Pending>>,
    waiting: Mutex<HashMap<String, oneshot::Sender<Reply>>>,
    arn: String,
    /// How long an invocation may wait for an environment and run.
    timeout: Duration,
    /// Environments parked in `next`, waiting for work.
    idle: AtomicUsize,
}

/// How to start one more environment.
struct Launcher {
    binary: PathBuf,
    env: Vec<(String, String)>,
    log_dir: PathBuf,
    addr: std::net::SocketAddr,
    max: usize,
}

/// One function: its pool of environments and the way to invoke it.
pub struct Function {
    pub name: String,
    tx: mpsc::Sender<Pending>,
    next_id: AtomicU64,
    pub invocations: AtomicU64,
    pub errors: AtomicU64,
    children: Mutex<Vec<Child>>,
    host: Arc<HostState>,
    launcher: Launcher,
    started: AtomicUsize,
    /// Why invocations failed, by kind.
    pub failures: std::sync::Mutex<HashMap<Failure, u64>>,
}

impl Function {
    /// Invokes the function and returns its JSON response.
    ///
    /// # Errors
    ///
    /// The function reported an error, or answered too late or unreadably.
    pub async fn invoke(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        match self.call(payload).await {
            Ok(value) => Ok(value),
            Err(failure) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut failures) = self.failures.lock() {
                    *failures.entry(failure).or_default() += 1;
                }
                anyhow::bail!("{} failed: {failure:?}", self.name)
            }
        }
    }

    async fn call(&self, payload: &serde_json::Value) -> Result<serde_json::Value, Failure> {
        if self.host.idle.load(Ordering::Relaxed) == 0 {
            self.scale_out().await;
        }
        let id = format!(
            "{}-{}",
            self.name,
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let (reply, rx) = oneshot::channel();
        let payload = serde_json::to_vec(payload).map_err(|_| Failure::Unreadable)?;
        self.tx
            .send(Pending { id, payload, reply })
            .await
            .map_err(|_| Failure::Dropped)?;
        let bytes = tokio::time::timeout(self.host.timeout, rx)
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(|_| Failure::Dropped)?
            .map_err(|_| Failure::FunctionError)?;
        serde_json::from_slice(&bytes).map_err(|_| Failure::Unreadable)
    }

    /// Starts one more environment, unless the pool is at its ceiling.
    async fn scale_out(&self) {
        let mut children = self.children.lock().await;
        if children.len() >= self.launcher.max {
            return;
        }
        let index = children.len();
        match launch(&self.name, &self.launcher, index) {
            Ok(child) => {
                children.push(child);
                self.started.store(children.len(), Ordering::Relaxed);
            }
            Err(e) => eprintln!("could not start another {} environment: {e:#}", self.name),
        }
    }

    /// Environments started over the run.
    pub fn environments(&self) -> usize {
        self.started.load(Ordering::Relaxed)
    }

    /// Stops every environment.
    pub async fn shutdown(&self) {
        let mut children = self.children.lock().await;
        for child in children.iter_mut() {
            let _ = child.kill().await;
        }
    }
}

/// What a function's environments are started with.
pub struct Spec<'a> {
    pub name: &'a str,
    pub binary: &'a Path,
    pub environments: usize,
    /// The most environments demand may grow the pool to.
    pub max_environments: usize,
    pub env: Vec<(String, String)>,
    pub log_dir: &'a Path,
    /// The function's configured timeout: API Gateway's 29 s for the functions
    /// behind it.
    pub timeout: Duration,
}

/// Starts a function's runtime API listener and its environments.
///
/// # Errors
///
/// The listener cannot bind, or a process cannot start.
pub async fn start(spec: Spec<'_>) -> Result<Arc<Function>> {
    let (tx, rx) = mpsc::channel::<Pending>(65_536);
    let state = Arc::new(HostState {
        queue: Mutex::new(rx),
        waiting: Mutex::new(HashMap::new()),
        arn: format!(
            "arn:aws:lambda:{}:{}:function:{}",
            super::aws::REGION,
            super::aws::ACCOUNT_ID,
            spec.name
        ),
        timeout: spec.timeout,
        idle: AtomicUsize::new(0),
    });
    let app = axum::Router::new()
        .route("/2018-06-01/runtime/invocation/next", get(next))
        .route(
            "/2018-06-01/runtime/invocation/{id}/response",
            post(respond),
        )
        .route("/2018-06-01/runtime/invocation/{id}/error", post(fail))
        .route("/2018-06-01/runtime/init/error", post(init_error))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    let launcher = Launcher {
        binary: spec.binary.to_path_buf(),
        env: spec.env,
        log_dir: spec.log_dir.to_path_buf(),
        addr,
        max: spec.max_environments.max(spec.environments),
    };
    let mut children = Vec::with_capacity(spec.environments);
    for index in 0..spec.environments {
        children.push(launch(spec.name, &launcher, index)?);
    }

    Ok(Arc::new(Function {
        name: spec.name.to_owned(),
        tx,
        next_id: AtomicU64::new(0),
        invocations: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        started: AtomicUsize::new(children.len()),
        failures: std::sync::Mutex::new(HashMap::new()),
        children: Mutex::new(children),
        host: state,
        launcher,
    }))
}

fn launch(name: &str, launcher: &Launcher, index: usize) -> Result<Child> {
    let log: PathBuf = launcher.log_dir.join(format!("{name}-{index}.log"));
    let out = std::fs::File::create(&log).with_context(|| format!("create {}", log.display()))?;
    let err = out.try_clone()?;
    Command::new(&launcher.binary)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("AWS_LAMBDA_RUNTIME_API", launcher.addr.to_string())
        .env("AWS_LAMBDA_FUNCTION_NAME", name)
        .env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "256")
        .env("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        .env("AWS_LAMBDA_LOG_GROUP_NAME", format!("/aws/lambda/{name}"))
        .env("AWS_LAMBDA_LOG_STREAM_NAME", format!("env-{index}"))
        .envs(launcher.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("start {}", launcher.binary.display()))
}

async fn next(State(state): State<Arc<HostState>>) -> Response<Body> {
    state.idle.fetch_add(1, Ordering::Relaxed);
    let pending = {
        let mut queue = state.queue.lock().await;
        queue.recv().await
    };
    state.idle.fetch_sub(1, Ordering::Relaxed);
    let Some(pending) = pending else {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::GONE;
        return response;
    };
    state
        .waiting
        .lock()
        .await
        .insert(pending.id.clone(), pending.reply);
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
        + state.timeout.as_millis();
    let mut response = Response::new(Body::from(pending.payload));
    let headers = response.headers_mut();
    let pairs = [
        ("lambda-runtime-aws-request-id", pending.id.clone()),
        ("lambda-runtime-deadline-ms", deadline.to_string()),
        ("lambda-runtime-invoked-function-arn", state.arn.clone()),
        ("lambda-runtime-trace-id", format!("Root=1-{}", pending.id)),
        ("content-type", "application/json".to_owned()),
    ];
    for (name, value) in pairs {
        if let Ok(value) = value.parse() {
            headers.insert(name, value);
        }
    }
    response
}

async fn respond(
    State(state): State<Arc<HostState>>,
    UrlPath(id): UrlPath<String>,
    body: Bytes,
) -> StatusCode {
    if let Some(reply) = state.waiting.lock().await.remove(&id) {
        let _ = reply.send(Ok(body));
    }
    StatusCode::ACCEPTED
}

async fn fail(
    State(state): State<Arc<HostState>>,
    UrlPath(id): UrlPath<String>,
    body: Bytes,
) -> StatusCode {
    if let Some(reply) = state.waiting.lock().await.remove(&id) {
        let _ = reply.send(Err(String::from_utf8_lossy(&body).into_owned()));
    }
    StatusCode::ACCEPTED
}

async fn init_error(body: Bytes) -> StatusCode {
    eprintln!(
        "a function failed to initialise: {}",
        String::from_utf8_lossy(&body)
    );
    StatusCode::ACCEPTED
}

/// Builds each Lambda crate for this machine and copies its `bootstrap` out,
/// since every crate names its binary `bootstrap` and they would overwrite one
/// another in `target/release`.
///
/// # Errors
///
/// A build fails.
pub fn build(crates: &[&str], out_dir: &Path) -> Result<HashMap<String, PathBuf>> {
    std::fs::create_dir_all(out_dir)?;
    let target =
        std::env::var("CARGO_TARGET_DIR").map_or_else(|_| PathBuf::from("target"), PathBuf::from);
    let mut built = HashMap::new();
    for name in crates {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release", "-q", "-p", name, "--bin", "bootstrap"])
            .status()
            .with_context(|| format!("run cargo for {name}"))?;
        anyhow::ensure!(status.success(), "building {name} failed");
        let to = out_dir.join(name);
        std::fs::copy(target.join("release").join("bootstrap"), &to)
            .with_context(|| format!("copy the {name} binary"))?;
        built.insert((*name).to_owned(), to);
    }
    Ok(built)
}

impl harness::backend::Consumer for Function {
    fn deliver(&self, event: serde_json::Value) -> harness::backend::Reply<'_> {
        Box::pin(async move { self.invoke(&event).await })
    }
}

/// The shipped system behind the edge: the `read` and `generate_token`
/// binaries, and the shipped CloudFront Function run by the Node sidecar.
pub struct Shipped {
    read: Arc<Function>,
    generate_token: Arc<Function>,
    gate: reqwest::Client,
    gate_url: String,
}

impl Shipped {
    #[must_use]
    pub fn new(read: Arc<Function>, generate_token: Arc<Function>, gate_url: String) -> Self {
        Self {
            read,
            generate_token,
            // Idle connections are dropped well before Node's 60 s keep-alive
            // closes them, or a request can be written to a socket the
            // sidecar has just closed.
            gate: reqwest::Client::builder()
                .pool_max_idle_per_host(256)
                .pool_idle_timeout(Duration::from_secs(20))
                .build()
                .unwrap_or_default(),
            gate_url,
        }
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

impl harness::backend::Backend for Shipped {
    fn invoke(
        &self,
        api: harness::backend::Api,
        event: serde_json::Value,
    ) -> harness::backend::Reply<'_> {
        let function = match api {
            harness::backend::Api::Read => &self.read,
            harness::backend::Api::GenerateToken => &self.generate_token,
        };
        Box::pin(async move { function.invoke(&event).await })
    }

    fn gate(&self, viewer: serde_json::Value) -> harness::backend::Reply<'_> {
        Box::pin(async move { self.ask_gate(&viewer).await })
    }
}
