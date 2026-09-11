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
    ///
    /// A position cannot be asked for before the seal — it does not exist until
    /// the seed does — so the whole cohort asks in the window just after it,
    /// spread only as far as the client's own spread reaches.
    HoldPosition,
    /// Ask again on every poll. What the client did before, kept so a run can
    /// measure the cost of the difference instead of taking it on trust.
    EveryTick,
    /// Fetch the registration's `(shard, local index)` once during the
    /// countdown, then compute the position locally from the seal outputs on
    /// `/status`.
    ///
    /// The row exists from the moment registration lands, so this ask is not
    /// tied to the seal, and afterwards the visitor needs nothing of their own
    /// — only the shared `/status` document, which is keyed on path and
    /// collapses.
    ///
    /// Measuring it is what showed that being free of the seal is worth less
    /// than it sounds. Asking on arrival reproduces the arrival rush, which for
    /// a scheduled event is a wall of people just before the start: against a
    /// late-arriving million, that peaks around six times higher than waiting
    /// for the seal and spreading deliberately. Given the same spread the two
    /// land within a few per cent of each other, which says the spread is doing
    /// the work and the timing is close to incidental.
    DerivePosition,
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

/// When a cohort turns up during the countdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Evenly across the countdown. Convenient, and not what happens.
    Uniform,
    /// Bunched towards the start, which is how a scheduled event actually
    /// fills: a trickle when the countdown opens and a wall of people in the
    /// last minutes before it starts.
    Late,
}

/// Where in the countdown one visitor arrives.
///
/// `Late` raises a uniform draw to a power below one, which pulls the mass
/// towards the end of the window. It is a shape, not a fitted model — the point
/// is to stop assuming the flattering case, not to predict a real audience.
fn arrival_offset_ms(arrival: Arrival, countdown_ms: u64, draw: u64) -> u64 {
    if countdown_ms == 0 {
        return 0;
    }
    let u = (draw % 1_000_000) as f64 / 1_000_000.0;
    let fraction = match arrival {
        Arrival::Uniform => u,
        Arrival::Late => u.powf(0.2),
    };
    (countdown_ms as f64 * fraction) as u64
}

/// What every visitor in a run shares.
#[derive(Debug, Clone, Copy)]
pub struct RunSettings {
    pub polling: Polling,
    /// Window the cohort spreads its one position request over.
    pub spread_ms: u64,
    /// How long the countdown runs before the seal. A position becomes askable
    /// only after this; a registration row is askable from the start.
    pub countdown_ms: u64,
    pub arrival: Arrival,
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
    // When this visitor turns up. Nothing they do can happen before it.
    let arrives_at = arrival_offset_ms(settings.arrival, settings.countdown_ms, jitter.next());

    let first_ask_after = Duration::from_millis(match settings.polling {
        // Readable the moment the row lands, so this ask is not tied to the
        // seal — but arriving is not the same as being spread. A real audience
        // turns up in a rush just before the start, so asking on arrival
        // reproduces that rush exactly. The deliberate spread is what flattens
        // a cohort, so apply it here too, starting from when each visitor
        // arrives rather than from the seal.
        Polling::DerivePosition => {
            arrives_at
                + if settings.spread_ms == 0 {
                    0
                } else {
                    jitter.next() % settings.spread_ms
                }
        }
        // Cannot be asked before the seal, so the wait is the countdown plus
        // this visitor's slice of the client's spread.
        Polling::HoldPosition => {
            settings.countdown_ms
                + if settings.spread_ms == 0 {
                    0
                } else {
                    jitter.next() % settings.spread_ms
                }
        }
        Polling::EveryTick => settings.countdown_ms,
    });

    // Nothing happens before this visitor turns up, and once they have, their
    // first poll lands somewhere inside the polling interval rather than on the
    // same tick as everyone else's.
    tokio::time::sleep(Duration::from_millis(arrives_at + jitter.next() % POLL_MS)).await;

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
            Polling::EveryTick => started.elapsed() >= first_ask_after,
            Polling::HoldPosition | Polling::DerivePosition => {
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
