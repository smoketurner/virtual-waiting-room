//! One simulated waiting visitor, following the same state machine as
//! `waiting.js`.
//!
//! This is a model of the client, so it can disagree with the client and report
//! a number that is wrong in the client's favour. Two things keep it honest:
//! the cadence constants are the client's own, and [`Polling::EveryTick`]
//! reproduces the behaviour the client had before it learned to hold a position,
//! so a run can show the difference the change actually makes rather than
//! asserting it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::edge::{Disposition, Edge};

/// `waiting.js`: `POLL_MS`.
const POLL_MS: u64 = 5000;
/// `waiting.js`: `JITTER_MS`.
const JITTER_MS: u64 = 1500;
/// `waiting.js`: the TTL on both polled cache policies.
const POLLED_TTL: Duration = Duration::from_secs(1);
/// `waiting.js`: `FIRST_ASK_TARGET_RPS`.
const FIRST_ASK_TARGET_RPS: u64 = 5000;
/// `waiting.js`: `FIRST_ASK_MAX_SPREAD_MS`.
const FIRST_ASK_MAX_SPREAD_MS: u64 = 60_000;

/// The window a cohort of `participants` spreads its one position request over.
///
/// Mirrors `scheduleFirstAsk`. Without it a cohort asks in unison the moment
/// the event opens, and a run reports a spike the shipped client does not
/// actually produce.
#[must_use]
pub fn first_ask_spread_ms(participants: u64) -> u64 {
    (participants * 1000 / FIRST_ASK_TARGET_RPS).min(FIRST_ASK_MAX_SPREAD_MS)
}

/// How a visitor treats their queue position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Polling {
    /// Ask once and hold it, because a position cannot change. The shipped
    /// client.
    HoldPosition,
    /// Ask again on every poll. What the client did before, kept so a run can
    /// measure the cost of the difference instead of taking it on trust.
    EveryTick,
}

/// What one visitor did, summed across the run.
#[derive(Debug, Default)]
pub struct VisitorTally {
    pub status_requests: AtomicU64,
    pub queue_num_requests: AtomicU64,
}

/// A deterministic jitter source. Real jitter is `Math.random()`; a run that
/// cannot be repeated is a poor measuring instrument, so this is seeded per
/// visitor and produces the same schedule every time.
struct Jitter(u64);

impl Jitter {
    fn new(seed: u64) -> Self {
        // Odd seed keeps the generator's period full for every visitor index.
        Self(seed.wrapping_mul(2) | 1)
    }

    /// xorshift64: adequate for spreading polls, and not used for anything
    /// where the quality of the randomness matters.
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn interval(&mut self) -> Duration {
        Duration::from_millis(POLL_MS + self.next() % JITTER_MS)
    }
}

/// What every visitor in a run shares.
#[derive(Debug, Clone, Copy)]
pub struct RunSettings {
    pub polling: Polling,
    /// Window the cohort spreads its one position request over.
    pub spread_ms: u64,
    pub deadline: tokio::time::Instant,
}

/// Runs one visitor until the deadline, driving requests through the edge.
pub async fn run<F, Fut>(
    edge: Arc<Edge>,
    request_id: String,
    seed: u64,
    settings: RunSettings,
    origin: Arc<F>,
    tally: Arc<VisitorTally>,
) where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = (u16, String)> + Send,
{
    let mut jitter = Jitter::new(seed);
    let mut held_position: Option<u64> = None;
    let started = tokio::time::Instant::now();
    // Each visitor takes a random slice of the shared window, exactly as the
    // client does, so the cohort's one position request is spread rather than
    // arriving in unison.
    let first_ask_after = Duration::from_millis(if settings.spread_ms == 0 {
        0
    } else {
        jitter.next() % settings.spread_ms
    });

    // Visitors do not arrive in lockstep; without this the whole cohort polls
    // on the same tick and every entry expires for all of them at once, which
    // would flatter collapsing.
    tokio::time::sleep(Duration::from_millis(jitter.next() % POLL_MS)).await;

    while tokio::time::Instant::now() < settings.deadline {
        // /status: keyed on path alone, so the whole cohort shares one entry.
        let status_origin = Arc::clone(&origin);
        let (_, _, _) = edge
            .serve("/v1/status", "/v1/status", POLLED_TTL, move || {
                let o = status_origin;
                async move { o("/v1/status".to_owned()).await }
            })
            .await;
        tally.status_requests.fetch_add(1, Ordering::Relaxed);

        let ask = match settings.polling {
            // The old client asked on every tick and knew nothing of a spread.
            Polling::EveryTick => true,
            Polling::HoldPosition => {
                held_position.is_none() && started.elapsed() >= first_ask_after
            }
        };

        if ask {
            // /queue_num: the request id is in the cache key, so this entry is
            // this visitor's alone and no other visitor can ever hit it.
            let path = format!("/v1/queue_num?request_id={request_id}");
            let key = path.clone();
            let qn_origin = Arc::clone(&origin);
            let (status, body, disposition) = edge
                .serve("/v1/queue_num", &key, POLLED_TTL, move || {
                    let o = qn_origin;
                    async move { o(path).await }
                })
                .await;
            tally.queue_num_requests.fetch_add(1, Ordering::Relaxed);
            debug_assert_ne!(
                disposition,
                Disposition::Collapsed,
                "a per-visitor key cannot collapse with another visitor's"
            );

            if status == 200
                && let Some(position) = parse_position(&body)
            {
                held_position = Some(position);
            }
        }

        tokio::time::sleep(jitter.interval()).await;
    }
}

/// Pulls `position` out of a `/queue_num` body without a JSON dependency on the
/// response shape beyond the one field this needs.
fn parse_position(body: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("position")?
        .as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_is_repeatable_for_a_given_visitor() {
        // A measurement that cannot be repeated cannot be compared against the
        // next run, which is the whole point of having one.
        let mut a = Jitter::new(7);
        let mut b = Jitter::new(7);
        assert_eq!(a.interval(), b.interval());
        assert_eq!(a.interval(), b.interval());
    }

    #[test]
    fn jitter_stays_inside_the_clients_window() {
        let mut j = Jitter::new(3);
        for _ in 0..1000 {
            let ms = j.interval().as_millis() as u64;
            assert!((POLL_MS..POLL_MS + JITTER_MS).contains(&ms), "{ms}");
        }
    }

    #[test]
    fn different_visitors_get_different_schedules() {
        let mut a = Jitter::new(1);
        let mut b = Jitter::new(2);
        assert_ne!(a.interval(), b.interval());
    }

    #[test]
    fn position_is_read_from_a_queue_num_body() {
        assert_eq!(
            parse_position(r#"{"position":4242,"live_join":true}"#),
            Some(4242)
        );
        assert_eq!(parse_position(r#"{"error":"not registered"}"#), None);
        assert_eq!(parse_position("not json"), None);
    }
}
