//! The join queue and the event source mapping that drains it.
//!
//! The AWS Emulator's SQS holds at most 1,000 messages per queue, a limit real
//! SQS does not have. A crowd that arrives faster than `assign_position` drains
//! would hit it and see joins refused that AWS would have taken, so the queue
//! is modelled here instead, with the semantics the design relies on:
//! at-least-once delivery, a visibility timeout that returns an unacknowledged
//! message to the queue, a receive count, a dead-letter queue after
//! `maxReceiveCount`, and batches of up to 100 collected over a one-second
//! window, the way `core/main.tf` configures the mapping.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use axum::http::StatusCode;
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::backend::Consumer;
use crate::rng::Rng;

/// `core/main.tf`: 6 × the 30 s function timeout + the 1 s batching window.
pub const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(181);
/// `core/main.tf`: the redrive policy's `maxReceiveCount`.
pub const MAX_RECEIVE_COUNT: u32 = 10;
/// `core/main.tf`: `batch_size`.
pub const BATCH_SIZE: usize = 100;
/// `core/main.tf`: `maximum_batching_window_in_seconds`.
pub const BATCH_WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
struct Message {
    id: String,
    body: String,
    receives: u32,
    sent_ms: u128,
}

#[derive(Debug, Default)]
struct State {
    ready: VecDeque<Message>,
    in_flight: HashMap<String, (Message, Instant)>,
    dead: Vec<Message>,
    next_id: u64,
    sent: u64,
    duplicated: u64,
}

/// What happened to the queue over a run.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueueTotals {
    pub sent: u64,
    pub duplicated: u64,
    pub dead_lettered: u64,
    pub left: u64,
}

pub struct JoinQueue {
    state: Mutex<State>,
    ready: Notify,
    url: String,
    /// Probability that a delivery is made twice (at-least-once).
    duplicate_rate: f64,
    rng: Mutex<Rng>,
    started: Instant,
}

impl JoinQueue {
    #[must_use]
    pub fn new(url: String, duplicate_rate: f64, seed: u64) -> Self {
        Self {
            state: Mutex::new(State::default()),
            ready: Notify::new(),
            url,
            duplicate_rate,
            rng: Mutex::new(Rng::new(seed)),
            started: Instant::now(),
        }
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// `SendMessage`, as API Gateway's direct integration makes it.
    pub fn send(&self, body: String) {
        if let Ok(mut s) = self.state.lock() {
            s.next_id += 1;
            s.sent += 1;
            let id = format!("m-{}", s.next_id);
            s.ready.push_back(Message {
                id,
                body,
                receives: 0,
                sent_ms: self.started.elapsed().as_millis(),
            });
        }
        self.ready.notify_one();
    }

    /// Messages waiting to be received: `ApproximateNumberOfMessages`.
    #[must_use]
    pub fn visible(&self) -> u64 {
        self.state
            .lock()
            .map(|s| s.ready.len() as u64)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn totals(&self) -> QueueTotals {
        self.state
            .lock()
            .map(|s| QueueTotals {
                sent: s.sent,
                duplicated: s.duplicated,
                dead_lettered: s.dead.len() as u64,
                left: (s.ready.len() + s.in_flight.len()) as u64,
            })
            .unwrap_or_default()
    }

    /// Returns expired in-flight messages to the queue, or to the dead-letter
    /// queue once they have been received `MAX_RECEIVE_COUNT` times.
    fn expire(state: &mut State, now: Instant) {
        let expired: Vec<String> = state
            .in_flight
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some((message, _)) = state.in_flight.remove(&id) {
                if message.receives >= MAX_RECEIVE_COUNT {
                    state.dead.push(message);
                } else {
                    state.ready.push_back(message);
                }
            }
        }
    }

    fn take(&self, max: usize) -> Vec<Message> {
        let Ok(mut s) = self.state.lock() else {
            return Vec::new();
        };
        let now = Instant::now();
        Self::expire(&mut s, now);
        let mut out = Vec::new();
        while out.len() < max {
            let Some(mut message) = s.ready.pop_front() else {
                break;
            };
            message.receives += 1;
            // At-least-once: now and then the same message is delivered again
            // while the first delivery is still in flight.
            let duplicate = self.duplicate_rate > 0.0
                && self.rng.lock().map(|mut r| r.unit()).unwrap_or(1.0) < self.duplicate_rate;
            if duplicate {
                s.duplicated += 1;
                let mut copy = message.clone();
                copy.id = format!("{}-dup{}", message.id, message.receives);
                copy.receives = 0;
                s.ready.push_back(copy);
            }
            s.in_flight.insert(
                message.id.clone(),
                (message.clone(), now + VISIBILITY_TIMEOUT),
            );
            out.push(message);
        }
        out
    }

    fn settle(&self, delivered: &[Message], failed: &[String]) {
        let Ok(mut s) = self.state.lock() else {
            return;
        };
        for message in delivered {
            if !failed.contains(&message.id) {
                s.in_flight.remove(&message.id);
            }
            // A failed record stays in flight until its visibility timeout
            // lapses, then returns, exactly as SQS treats a partial batch
            // failure.
        }
    }

    /// One poller of the event source mapping: collect a batch, invoke the
    /// function, delete what it processed. Runs until `stop` fires.
    pub async fn poll(&self, consumer: &dyn Consumer, stop: &tokio::sync::watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            let mut batch = self.take(BATCH_SIZE);
            if batch.is_empty() {
                tokio::select! {
                    () = self.ready.notified() => {}
                    () = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
                continue;
            }
            let window = Instant::now() + BATCH_WINDOW;
            while batch.len() < BATCH_SIZE && Instant::now() < window {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let more = self.take(BATCH_SIZE - batch.len());
                batch.extend(more);
            }
            let failed = match consumer.deliver(self.event(&batch)).await {
                Ok(reply) => batch_failures(&reply),
                // A failed invocation fails the whole batch.
                Err(_) => batch.iter().map(|m| m.id.clone()).collect(),
            };
            self.settle(&batch, &failed);
        }
    }

    fn event(&self, batch: &[Message]) -> serde_json::Value {
        let records: Vec<serde_json::Value> = batch
            .iter()
            .map(|m| {
                json!({
                    "messageId": m.id,
                    "receiptHandle": m.id,
                    "body": m.body,
                    "attributes": {
                        "ApproximateReceiveCount": m.receives.to_string(),
                        "SentTimestamp": m.sent_ms.to_string(),
                        "SenderId": "harness",
                        "ApproximateFirstReceiveTimestamp": m.sent_ms.to_string(),
                    },
                    "messageAttributes": {},
                    "md5OfBody": "",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:000000000000:join",
                    "awsRegion": "us-east-1",
                })
            })
            .collect();
        json!({ "Records": records })
    }

    /// The SQS operations a binary makes over the wire. Only `open_event`
    /// calls SQS, and only `GetQueueAttributes`.
    pub fn handle_wire(&self, op: &str, body: &[u8]) -> (StatusCode, serde_json::Value) {
        match op {
            "GetQueueAttributes" => {
                let url = serde_json::from_slice::<serde_json::Value>(body)
                    .ok()
                    .and_then(|v| {
                        v.get("QueueUrl")
                            .and_then(|u| u.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                if url != self.url {
                    return (
                        StatusCode::BAD_REQUEST,
                        json!({"__type": "com.amazonaws.sqs#QueueDoesNotExist", "message": url}),
                    );
                }
                (
                    StatusCode::OK,
                    json!({"Attributes": {"ApproximateNumberOfMessages": self.visible().to_string()}}),
                )
            }
            other => (
                StatusCode::BAD_REQUEST,
                json!({"__type": "com.amazonaws.sqs#UnsupportedOperation", "message": other}),
            ),
        }
    }
}

fn batch_failures(reply: &serde_json::Value) -> Vec<String> {
    reply
        .get("batchItemFailures")
        .and_then(|f| f.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|i| i.get("itemIdentifier").and_then(|v| v.as_str()))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}
