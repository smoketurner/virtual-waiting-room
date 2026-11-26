//! One visitor, following the protocol `infra/modules/edge/pages/waiting.js`
//! runs in a browser.
//!
//! This is a model of the page, not the page: it makes the same requests, in
//! the same order, at the same intervals, and reacts to each answer the way
//! the page does. The constants below are the page's; where the page's logic
//! has a branch that only changes what it renders, this leaves it out.
//!
//! Hidden tabs are not modelled: every visitor is in the foreground.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio::time::Instant;

use crate::edge::{Edge, ViewerRequest};
use crate::rng::Rng;

// waiting.js
const JITTER_FRACTION: f64 = 0.3;
const MAX_JOIN_ATTEMPTS: u32 = 8;
const MAX_REJOIN_CYCLES: u32 = 3;
const JOIN_BACKOFF_CEILING_MS: f64 = 120_000.0;

/// When a join is made: during the countdown, a throttled join is retried at
/// the floor rather than backed off.
#[derive(Debug, Clone, Copy)]
enum Joining {
    Countdown,
    Open,
}
const MAX_MISSES: u32 = 8;
const FIRST_ASK_TARGET_RPS: f64 = 5000.0;
const FIRST_ASK_MAX_SPREAD_MS: f64 = 120_000.0;
const RATE_WINDOW_MS: u64 = 60_000;
const RATE_MIN_SPAN_MS: u64 = 30_000;
const DEFAULT_POLL_MS: f64 = 5000.0;

/// How a visitor's visit ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Reached the protected origin with a session.
    Admitted,
    /// Left once it knew its position, never asking to be let in.
    NoShow,
    /// The page stopped, telling the visitor something went wrong.
    GaveUp(&'static str),
    /// The run ended first.
    StillWaiting,
}

/// Everything the report needs about one visitor. Times are milliseconds from
/// the start of the run.
#[derive(Debug, Clone)]
pub struct Record {
    pub request_id: String,
    pub arrived_ms: u64,
    pub joined_ms: Option<u64>,
    pub position: Option<u64>,
    pub live_join: bool,
    pub position_known_ms: Option<u64>,
    pub admitted_ms: Option<u64>,
    /// When the session `generate_token` minted ends, epoch seconds.
    pub session_ends: Option<u64>,
    pub outcome: Outcome,
    pub join_throttled: u32,
    pub capacity_refusals: u32,
    pub requests: u32,
}

/// What every visitor in a run shares.
pub struct World {
    pub edge: Arc<Edge>,
    pub started: Instant,
    pub deadline: Instant,
    pub protected_path: String,
}

#[derive(Debug, Clone, Copy)]
struct Policy {
    floor: f64,
    ceiling: f64,
    divisor: f64,
}

struct Visitor<'a> {
    world: &'a World,
    rng: Rng,
    request_id: String,
    secret: String,
    digest: String,
    ip: String,
    policy: Policy,
    next_ms: f64,
    backoff_ms: f64,
    event_id: Option<String>,
    joined: bool,
    join_attempts: u32,
    join_throttles: u32,
    join_blocked_until: Option<Instant>,
    rejoin_cycles: u32,
    misses: u32,
    first_ask_at: Option<Instant>,
    cursor_samples: Vec<(Instant, u64)>,
    capacity_misses: u32,
    capacity_refusals: u32,
    record: Record,
}

/// Runs one visitor from arrival to the end of its visit.
pub async fn visit(world: Arc<World>, mut rng: Rng, arrive_at: Instant, no_show: bool) -> Record {
    tokio::time::sleep_until(arrive_at).await;
    let mut id = [0u8; 16];
    rng.fill(&mut id);
    // UUID shape with the version and variant nibbles set (v4 here; the page
    // mints v7, and assign_position checks shape only).
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    let hex = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let request_id = format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    );
    let mut secret = [0u8; 32];
    rng.fill(&mut secret);
    let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    let digest = wr_common::PossessionSecret::parse(&secret)
        .map(|s| s.digest().as_str().to_owned())
        .unwrap_or_default();
    let octets = rng.next_u64().to_le_bytes();
    let ip = format!("10.{}.{}.{}", octets[0], octets[1], octets[2]);
    let arrived_ms = ms(world.started, Instant::now());

    let mut v = Visitor {
        world: &world,
        rng,
        request_id: request_id.clone(),
        secret,
        digest,
        ip,
        policy: Policy {
            floor: DEFAULT_POLL_MS,
            ceiling: DEFAULT_POLL_MS,
            divisor: 1.0,
        },
        next_ms: DEFAULT_POLL_MS,
        backoff_ms: 0.0,
        event_id: None,
        joined: false,
        join_attempts: 0,
        join_throttles: 0,
        join_blocked_until: None,
        rejoin_cycles: 0,
        misses: 0,
        first_ask_at: None,
        cursor_samples: Vec::new(),
        capacity_misses: 0,
        capacity_refusals: 0,
        record: Record {
            request_id,
            arrived_ms,
            joined_ms: None,
            position: None,
            live_join: false,
            position_known_ms: None,
            admitted_ms: None,
            session_ends: None,
            outcome: Outcome::StillWaiting,
            join_throttled: 0,
            capacity_refusals: 0,
            requests: 0,
        },
    };
    let outcome = v.run(no_show).await;
    v.record.outcome = outcome;
    v.record.join_throttled = v.join_throttles;
    v.record.capacity_refusals = v.capacity_refusals;
    v.record
}

fn ms(started: Instant, at: Instant) -> u64 {
    u64::try_from(at.duration_since(started).as_millis()).unwrap_or(u64::MAX)
}

impl Visitor<'_> {
    async fn send(&mut self, req: ViewerRequest) -> crate::edge::ViewerResponse {
        self.record.requests += 1;
        self.world.edge.request(req).await
    }

    fn get(&self, path: &str, query: Vec<(String, String)>) -> ViewerRequest {
        ViewerRequest {
            method: "GET",
            path: path.to_owned(),
            query,
            viewer_ip: self.ip.clone(),
            ..ViewerRequest::default()
        }
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> ViewerRequest {
        ViewerRequest {
            method: "POST",
            path: path.to_owned(),
            body: Some(body.to_string()),
            content_type: Some("application/json"),
            viewer_ip: self.ip.clone(),
            ..ViewerRequest::default()
        }
    }

    /// Waits the current interval, stretched by up to 30% as the page's
    /// `jitter()` does.
    async fn pause(&mut self) {
        let ms = self.next_ms * (1.0 + self.rng.unit() * JITTER_FRACTION);
        tokio::time::sleep(Duration::from_secs_f64(ms.max(0.0) / 1000.0)).await;
    }

    /// The page's failure backoff: double the interval, up to the ceiling.
    fn back_off(&mut self) {
        let doubled = if self.backoff_ms > 0.0 {
            self.backoff_ms * 2.0
        } else {
            self.next_ms * 2.0
        };
        self.backoff_ms = self.policy.ceiling.min(doubled);
        self.next_ms = self.backoff_ms;
    }

    fn adopt_policy(&mut self, p: Option<&serde_json::Value>) {
        let Some(p) = p else { return };
        let get = |k: &str, lo: f64, hi: f64| {
            p.get(k)
                .and_then(serde_json::Value::as_f64)
                .filter(|v| (lo..=hi).contains(v))
        };
        let (Some(f), Some(c), Some(d)) = (
            get("floor_ms", 1000.0, 300_000.0),
            get("ceiling_ms", 1000.0, 300_000.0),
            get("divisor", 1.0, 1000.0),
        ) else {
            return;
        };
        if c >= f {
            self.policy = Policy {
                floor: f,
                ceiling: c,
                divisor: d,
            };
        }
    }

    fn observe_cursor(&mut self, serving: u64) {
        let now = Instant::now();
        if self
            .cursor_samples
            .last()
            .is_some_and(|&(_, s)| serving < s)
        {
            self.cursor_samples.clear();
        }
        self.cursor_samples.push((now, serving));
        while self.cursor_samples.len() > 2
            && self.cursor_samples.first().is_some_and(|&(at, _)| {
                now.duration_since(at).as_millis() > u128::from(RATE_WINDOW_MS)
            })
        {
            self.cursor_samples.remove(0);
        }
    }

    fn rate(&self, target: f64) -> f64 {
        let (Some(&(t0, s0)), Some(&(t1, s1))) =
            (self.cursor_samples.first(), self.cursor_samples.last())
        else {
            return target;
        };
        let secs = t1.duration_since(t0).as_secs_f64();
        if secs < (RATE_MIN_SPAN_MS as f64) / 1000.0 || s1 <= s0 {
            return target;
        }
        (s1 - s0) as f64 / secs
    }

    fn interval_for(&self, ahead: u64, rate: f64) -> f64 {
        if rate <= 0.0 {
            return self.policy.floor;
        }
        let wait = ahead as f64 / rate;
        if wait <= 0.0 {
            return self.policy.floor;
        }
        (wait * 1000.0 / self.policy.divisor).clamp(self.policy.floor, self.policy.ceiling)
    }

    /// The page's `join()`.
    async fn join(&mut self, when: Joining) -> Result<(), &'static str> {
        if self.joined {
            return Ok(());
        }
        if self.join_blocked_until.is_some_and(|t| Instant::now() < t) {
            return Ok(());
        }
        if self.join_attempts >= MAX_JOIN_ATTEMPTS {
            return Err("join attempts exhausted");
        }
        let Some(event_id) = self.event_id.clone() else {
            return Ok(());
        };
        self.join_attempts += 1;
        let body = serde_json::json!({
            "request_id": self.request_id,
            "event_id": event_id,
            "h": self.digest,
        });
        let res = self.send(self.post("/_wr/v1/join", &body)).await;
        if (200..300).contains(&res.status) {
            self.joined = true;
            self.join_attempts = 0;
            if self.record.joined_ms.is_none() {
                self.record.joined_ms = Some(ms(self.world.started, Instant::now()));
            }
            return Ok(());
        }
        if res.status == 429 {
            self.join_attempts -= 1;
            self.join_throttles += 1;
        }
        // A registration throttled during the countdown does not grow: it has
        // to land before the open counts the cohort.
        let exponent = match when {
            Joining::Countdown if res.status == 429 => 1,
            Joining::Countdown | Joining::Open => (self.join_attempts + self.join_throttles).min(7),
        };
        let backoff = (self.policy.floor * 2f64.powi(i32::try_from(exponent).unwrap_or(7) - 1))
            .min(JOIN_BACKOFF_CEILING_MS)
            * (0.5 + self.rng.unit());
        self.join_blocked_until = Some(Instant::now() + Duration::from_secs_f64(backoff / 1000.0));
        Ok(())
    }

    fn forget_join(&mut self) {
        self.misses = 0;
        self.joined = false;
        self.join_attempts = 0;
        self.join_blocked_until = None;
    }

    /// The page's `tick()` loop. Returns how the visit ended.
    async fn run(&mut self, no_show: bool) -> Outcome {
        let mut known: Option<u64> = None;
        loop {
            if Instant::now() >= self.world.deadline {
                return Outcome::StillWaiting;
            }
            let status = self.send(self.get("/_wr/v1/status", Vec::new())).await;
            let Some(s) = (status.status == 200).then(|| status.json()).flatten() else {
                self.back_off();
                self.pause().await;
                continue;
            };
            self.backoff_ms = 0.0;
            let serving = s
                .get("serving_position")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            self.observe_cursor(serving);
            self.adopt_policy(s.get("poll_policy"));
            if let Some(id) = s.get("event_id").and_then(|v| v.as_str()) {
                self.event_id = Some(id.to_owned());
            }
            let state = s
                .get("serving_state")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            // The page's expectedRate: a full origin turns over once a session.
            let number = |k: &str| s.get(k).and_then(serde_json::Value::as_f64).unwrap_or(0.0);
            let session = number("session_seconds");
            let concurrency = number("origin_concurrency");
            let target = if session > 0.0 {
                concurrency / session
            } else {
                0.0
            };
            match state {
                "closed" => {
                    known = None;
                    self.next_ms = self.policy.ceiling;
                    if s.get("phase").and_then(|v| v.as_str()) == Some("pre_queue") {
                        if let Err(why) = self.join(Joining::Countdown).await {
                            return Outcome::GaveUp(why);
                        }
                        if !self.joined {
                            self.next_ms = self.policy.floor;
                        }
                    }
                    self.pause().await;
                    continue;
                }
                "paused" => {
                    self.next_ms = known.map_or(self.policy.ceiling, |p| {
                        self.interval_for(p.saturating_sub(serving), self.rate(target))
                    });
                    self.pause().await;
                    continue;
                }
                "fail_open" => return Outcome::Admitted,
                _ => {}
            }
            if self.first_ask_at.is_none() {
                let participants = s
                    .get("participant_count")
                    .and_then(serde_json::Value::as_f64)
                    .unwrap_or(0.0);
                let spread =
                    FIRST_ASK_MAX_SPREAD_MS.min(participants / FIRST_ASK_TARGET_RPS * 1000.0);
                let delay = (self.rng.unit() * spread).floor();
                self.first_ask_at = Some(Instant::now() + Duration::from_secs_f64(delay / 1000.0));
            }
            if let Err(why) = self.join(Joining::Open).await {
                return Outcome::GaveUp(why);
            }
            let position = if let Some(p) = known {
                p
            } else if !self.joined || self.first_ask_at.is_some_and(|t| Instant::now() < t) {
                // No accepted join yet, or inside the first-ask spread.
                self.next_ms = self.policy.floor;
                self.pause().await;
                continue;
            } else {
                let q = self
                    .send(self.get(
                        "/_wr/v1/queue_num",
                        vec![("request_id".to_owned(), self.request_id.clone())],
                    ))
                    .await;
                if q.status == 404 {
                    self.misses += 1;
                    self.next_ms = self.policy.floor;
                    if self.misses >= MAX_MISSES {
                        self.rejoin_cycles += 1;
                        if self.rejoin_cycles > MAX_REJOIN_CYCLES {
                            return Outcome::GaveUp("no position after re-joining");
                        }
                        self.forget_join();
                    }
                    self.pause().await;
                    continue;
                }
                self.misses = 0;
                let Some(body) = (q.status == 200).then(|| q.json()).flatten() else {
                    self.back_off();
                    self.pause().await;
                    continue;
                };
                let Some(p) = body.get("position").and_then(serde_json::Value::as_u64) else {
                    self.pause().await;
                    continue;
                };
                self.record.position = Some(p);
                self.record.live_join = body
                    .get("live_join")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                self.record.position_known_ms = Some(ms(self.world.started, Instant::now()));
                known = Some(p);
                if no_show {
                    return Outcome::NoShow;
                }
                p
            };

            if position < serving {
                match self.redeem().await {
                    Redeem::Admitted => return Outcome::Admitted,
                    Redeem::Stop(why) => return Outcome::GaveUp(why),
                    // The page's 425: a slot not yet free, asked again at the
                    // floor.
                    Redeem::NotYet => {
                        self.capacity_refusals += 1;
                        self.capacity_misses = 0;
                        self.next_ms = self.policy.floor;
                    }
                    Redeem::Busy => {
                        self.capacity_misses += 1;
                        let factor =
                            2f64.powi(i32::try_from(self.capacity_misses.min(6)).unwrap_or(6));
                        self.next_ms = self.policy.ceiling.min(self.policy.floor * factor);
                    }
                    Redeem::Again => {
                        self.capacity_misses = 0;
                        self.next_ms = self.policy.floor;
                    }
                }
            } else {
                let ahead = position.saturating_sub(serving);
                // The page: within one origin's worth of the front, the floor.
                self.next_ms = if ahead == 0 || ahead as f64 <= concurrency {
                    self.policy.floor
                } else {
                    self.interval_for(ahead, self.rate(target))
                };
            }
            self.pause().await;
        }
    }

    /// The page's `redeem()`, and the navigation a success makes.
    async fn redeem(&mut self) -> Redeem {
        let Some(event_id) = self.event_id.clone() else {
            return Redeem::Again;
        };
        let body = serde_json::json!({
            "request_id": self.request_id,
            "event_id": event_id,
            "secret": self.secret,
        });
        let res = self.send(self.post("/_wr/v1/generate_token", &body)).await;
        match res.status {
            200 if res
                .json()
                .and_then(|b| b.get("admitted").and_then(serde_json::Value::as_bool))
                == Some(true) =>
            {
                if self.record.session_ends.is_none() {
                    self.record.session_ends = res
                        .json()
                        .and_then(|b| b.get("expires_at").and_then(serde_json::Value::as_u64));
                }
                let cookies: Vec<(String, String)> = res
                    .set_cookies
                    .iter()
                    .filter_map(|c| c.split(';').next())
                    .filter_map(|kv| kv.split_once('='))
                    .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
                    .collect();
                let origin = self
                    .send(ViewerRequest {
                        method: "GET",
                        path: self.world.protected_path.clone(),
                        cookies,
                        navigation: true,
                        viewer_ip: self.ip.clone(),
                        ..ViewerRequest::default()
                    })
                    .await;
                if origin.status == 200 {
                    self.record.admitted_ms = Some(ms(self.world.started, Instant::now()));
                    Redeem::Admitted
                } else {
                    Redeem::Stop("the gate refused a fresh session")
                }
            }
            403 => Redeem::Stop("not the holder of this place"),
            410 => Redeem::Stop("visit already used"),
            425 => Redeem::NotYet,
            429 | 500..=599 => Redeem::Busy,
            _ => Redeem::Again,
        }
    }
}

enum Redeem {
    Admitted,
    NotYet,
    Busy,
    Again,
    Stop(&'static str),
}
