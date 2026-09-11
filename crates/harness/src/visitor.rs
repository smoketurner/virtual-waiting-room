//! One simulated waiting visitor, following the same state machine as
//! `waiting.js`.
//!
//! This is a model of the client, so it can disagree with the client and report
//! a number that is wrong in the client's favour. Two things keep it honest:
//! the cadence constants are the client's own, and [`Polling::EveryTick`]
//! reproduces the behaviour the client had before it learned to hold a position,
//! so a run can show the difference the change actually makes rather than
//! asserting it.
//!
//! Deliberately absent from [`Polling::Backoff`]: Page Visibility. This model
//! is all-foreground — every visitor polls the whole run — so its numbers are
//! the hidden-share-0 row of `DESIGN.md` §12, not a claim that a real cohort
//! never backgrounds a tab. Browser throttling and the visibility handler
//! both change what a hidden tab costs, in *either* direction depending on
//! the browser (§12's iOS suspension row goes one way, Chrome's throttle the
//! other), and this harness has no way to observe either one.

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

/// `waiting.js` (#69): the deploy-time poll-policy defaults —
/// `terraform.tfvars`' `poll_floor_ms`/`poll_ceiling_ms`/`poll_divisor`.
const POLL_FLOOR_MS: u64 = 5000;
const POLL_CEILING_MS: u64 = 30_000;
const POLL_DIVISOR: u64 = 10;
/// `waiting.js`: `JITTER_FRACTION`, proportional jitter over the computed
/// interval rather than [`JITTER_MS`]'s flat window.
const JITTER_FRACTION: f64 = 0.3;

/// `waiting.js`: `intervalFor`. Proportional to the wait this visitor can
/// see, clamped to `[floor, ceiling]`; falls back to the floor with no wait
/// to be proportional to.
#[must_use]
pub fn interval_for(wait_seconds: f64, floor_ms: u64, ceiling_ms: u64, divisor: u64) -> u64 {
    // Deliberately not `wait_seconds <= 0.0`: that is false for NaN, and a
    // NaN wait must fall back to the floor exactly like a non-positive one.
    #[expect(
        clippy::neg_cmp_op_on_partial_ord,
        reason = "the negation is exactly what mirrors waiting.js's !(waitSeconds > 0), catching NaN"
    )]
    if !(wait_seconds > 0.0) {
        return floor_ms;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "divisor is a small configured value, far below f64's exact-integer range"
    )]
    let raw = (wait_seconds * 1000.0) / divisor as f64;
    #[expect(
        clippy::cast_precision_loss,
        reason = "floor_ms/ceiling_ms are milliseconds far below f64's exact-integer range"
    )]
    let clamped = raw.max(floor_ms as f64).min(ceiling_ms as f64);
    #[expect(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        reason = "clamped is finite and within [floor_ms, ceiling_ms], both valid u64"
    )]
    let out = clamped as u64;
    out
}

/// `waiting.js`: `intervalForPosition`.
#[must_use]
pub fn interval_for_position(ahead: u64, rate: u64) -> u64 {
    if rate == 0 {
        return POLL_FLOOR_MS;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "positions and rates far below f64's exact-integer range at any realistic event size"
    )]
    let wait_seconds = ahead as f64 / rate as f64;
    interval_for(wait_seconds, POLL_FLOOR_MS, POLL_CEILING_MS, POLL_DIVISOR)
}

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
    /// The adaptive client (#69): holds its position exactly like
    /// [`Polling::HoldPosition`], but spaces its polls out with distance from
    /// the front instead of polling at a fixed interval regardless of wait.
    Backoff,
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

    /// `waiting.js`: `jitter()` under the adaptive policy —
    /// `Math.round(nextIntervalMs * (1 + Math.random() * JITTER_FRACTION))`.
    fn proportional(&mut self, base_ms: u64) -> Duration {
        // next() % 1_000_000 / 1_000_000.0 mirrors Math.random()'s [0, 1) range
        // closely enough for spreading polls; exactness does not matter here.
        let fraction = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        #[expect(
            clippy::cast_precision_loss,
            reason = "base_ms is a poll interval in milliseconds, far below f64's exact-integer range"
        )]
        let scaled = (base_ms as f64 * (1.0 + fraction * JITTER_FRACTION)).round();
        #[expect(
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation,
            reason = "scaled is finite, non-negative (base_ms and fraction both >= 0), and well below u64::MAX"
        )]
        let ms = scaled as u64;
        Duration::from_millis(ms)
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
    /// Visitors admitted per second, for [`Polling::Backoff`]'s interval
    /// calculation. Ignored by every other polling mode.
    pub target_rate: u64,
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
        Polling::HoldPosition | Polling::Backoff => {
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

    let mut serving_position = 0u64;

    while tokio::time::Instant::now() < settings.deadline {
        // /status: keyed on path alone, so the whole cohort shares one entry.
        let status_origin = Arc::clone(&origin);
        let (_, status_body, _) = edge
            .serve("/v1/status", "/v1/status", POLLED_TTL, move || {
                let o = status_origin;
                async move { o("/v1/status".to_owned()).await }
            })
            .await;
        tally.status_requests.fetch_add(1, Ordering::Relaxed);
        if let Some(serving) = parse_serving_position(&status_body) {
            serving_position = serving;
        }
        let closed = is_closed(&status_body);

        let ask = match settings.polling {
            // The old client asked on every tick and knew nothing of a spread.
            Polling::EveryTick => started.elapsed() >= first_ask_after,
            Polling::HoldPosition | Polling::DerivePosition | Polling::Backoff => {
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

        let sleep = match settings.polling {
            Polling::Backoff => {
                // `waiting.js`: closed ⇒ the ceiling, unconditionally.
                // Otherwise ahead === 0 or unknown ⇒ the floor; otherwise
                // intervalForPosition. Mirrors the client's own clamp exactly.
                let interval_ms = if closed {
                    POLL_CEILING_MS
                } else {
                    match held_position {
                        Some(position) => {
                            let ahead = position.saturating_sub(serving_position);
                            if ahead == 0 {
                                POLL_FLOOR_MS
                            } else {
                                interval_for_position(ahead, settings.target_rate)
                            }
                        }
                        None => POLL_FLOOR_MS,
                    }
                };
                jitter.proportional(interval_ms)
            }
            _ => jitter.interval(),
        };
        tokio::time::sleep(sleep).await;
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

/// Pulls `serving_position` out of a `/status` body, the same way
/// `parse_position` reads `/queue_num`.
fn parse_serving_position(body: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()?
        .get("serving_position")?
        .as_u64()
}

/// Whether a `/status` body reports `serving_state: "closed"`.
///
/// `waiting.js` maps this row to the ceiling unconditionally — a closed event
/// has forgotten every visitor's position (`forgetPosition()`), so there is
/// nothing to be proportional to. Checked separately from
/// `held_position` because a real client can hold a position across other
/// serving states but never across `closed`.
fn is_closed(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    value.get("serving_state").and_then(|s| s.as_str()) == Some("closed")
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

    #[test]
    fn serving_position_is_read_from_a_status_body() {
        assert_eq!(
            parse_serving_position(r#"{"serving_position":90,"phase":"active"}"#),
            Some(90)
        );
        assert_eq!(parse_serving_position(r#"{"phase":"pre_queue"}"#), None);
        assert_eq!(parse_serving_position("not json"), None);
    }

    #[test]
    fn closed_is_read_from_a_status_body() {
        assert!(is_closed(r#"{"serving_state":"closed"}"#));
        assert!(!is_closed(r#"{"serving_state":"running"}"#));
        assert!(!is_closed(r#"{"serving_state":"paused"}"#));
        assert!(!is_closed(r#"{"phase":"active"}"#), "field absent");
        assert!(!is_closed("not json"));
    }

    #[test]
    fn interval_for_a_zero_or_negative_wait_is_the_floor() {
        assert_eq!(interval_for(0.0, 5000, 30_000, 10), 5000);
        assert_eq!(interval_for(-1.0, 5000, 30_000, 10), 5000);
    }

    #[test]
    fn interval_for_clamps_to_the_floor_and_ceiling() {
        assert_eq!(
            interval_for(0.001, 5000, 30_000, 10),
            5000,
            "below the floor"
        );
        assert_eq!(
            interval_for(1_000_000.0, 5000, 30_000, 10),
            30_000,
            "above the ceiling"
        );
        // A 100s wait at divisor 10 is exactly the midpoint: neither clamp
        // applies.
        assert_eq!(interval_for(100.0, 5000, 30_000, 10), 10_000);
    }

    #[test]
    fn interval_for_position_falls_back_to_the_floor_with_no_rate() {
        assert_eq!(interval_for_position(1000, 0), POLL_FLOOR_MS);
    }

    #[test]
    fn proportional_jitter_never_shrinks_the_interval() {
        let mut j = Jitter::new(11);
        for _ in 0..1000 {
            let ms = j.proportional(5000).as_millis() as u64;
            assert!((5000..=6500).contains(&ms), "{ms}");
        }
    }

    /// Simulates polling to admission with `interval_for_position` (no
    /// jitter): starting `ahead0` positions back at `rate` positions/sec,
    /// how many polls until `ahead` reaches zero.
    fn polls_to_admission(ahead0: u64, rate: u64) -> u64 {
        let mut ahead = ahead0;
        let mut polls = 0u64;
        while ahead > 0 {
            polls += 1;
            let interval_ms = interval_for_position(ahead, rate);
            let moved = (rate * interval_ms / 1000).max(1);
            ahead = ahead.saturating_sub(moved);
        }
        polls
    }

    /// The same descent at today's fixed floor-only interval — one poll
    /// every `POLL_FLOOR_MS`, moving `rate * POLL_FLOOR_MS / 1000` positions
    /// each time, regardless of how far away the front is.
    fn polls_to_admission_fixed(ahead0: u64, rate: u64) -> u64 {
        ahead0.div_ceil((rate * POLL_FLOOR_MS / 1000).max(1))
    }

    #[test]
    fn backoff_poll_count_matches_the_geometric_closed_form_away_from_the_clamps() {
        // Divisor 10, rate 1000/s: the floor/ceiling clamps engage at ahead
        // 50,000 and 300,000, so descending from 200,000 to the floor
        // threshold exercises intervalFor's unclamped middle branch only.
        // There, each poll's own interval is exactly the remaining wait
        // divided by the divisor, so the remaining wait shrinks by a factor
        // of (1 - 1/divisor) every poll — a geometric decay, whose poll
        // count to reach any fixed threshold is logarithmic in the starting
        // wait rather than linear in it.
        let rate = 1000u64;
        let divisor = 10u64;
        let ahead_floor_threshold = rate * POLL_FLOOR_MS * divisor / 1000; // 50_000
        let ahead0 = 200_000u64;

        let mut ahead = ahead0;
        let mut polls = 0u64;
        while ahead > ahead_floor_threshold {
            polls += 1;
            let interval_ms = interval_for_position(ahead, rate);
            let moved = rate * interval_ms / 1000;
            ahead = ahead.saturating_sub(moved);
        }

        let ratio = ahead_floor_threshold as f64 / ahead0 as f64;
        let expected = ratio.ln() / (1.0 - 1.0 / divisor as f64).ln();
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the closed form is a small positive poll count for these inputs"
        )]
        let expected = expected.ceil() as u64;
        // Rounding each interval down to a whole millisecond and each move
        // down to a whole position can only make the discrete simulation
        // run a little long against the continuous closed form.
        assert!(
            polls >= expected && polls <= expected + 3,
            "polls={polls} expected~={expected}"
        );
    }

    #[test]
    fn backoff_poll_count_over_a_full_wait_is_materially_below_a_fixed_interval() {
        // A visitor 1,000,000 positions back at a modest 100/s target rate —
        // deep enough to spend most of the wait ceiling-clamped, exactly the
        // realistic case: even bounded at the ceiling, polling less often
        // there is most of where the saving comes from over the full wait.
        let ahead0 = 1_000_000u64;
        let rate = 100u64;

        let backoff = polls_to_admission(ahead0, rate);
        let fixed = polls_to_admission_fixed(ahead0, rate);

        assert!(
            backoff.saturating_mul(3) < fixed,
            "backoff={backoff} fixed={fixed}"
        );
    }

    #[test]
    #[expect(
        clippy::panic,
        reason = "test-only parsing of variables.tf; any failure to parse is itself the signal something drifted"
    )]
    fn harness_constants_match_terraform_defaults() {
        // POLL_FLOOR_MS/POLL_CEILING_MS/POLL_DIVISOR are hand-copied from
        // infra/modules/core/variables.tf's poll_floor_ms/poll_ceiling_ms/
        // poll_divisor defaults, tied only by a doc comment. Pinning the
        // source file here means a Terraform default change is caught in
        // this test instead of silently invalidating every harness
        // measurement while the rest of the suite stays green.
        //
        // This pins `core`'s defaults only, not the dev root's
        // (infra/environments/dev/variables.tf, example.tfvars) — `core` is
        // the canonical default every other root inherits from, so it is the
        // right anchor, but a dev-root override diverging from it would not
        // trip this test.
        let variables_tf = include_str!("../../../infra/modules/core/variables.tf");
        for (name, expected) in [
            ("poll_floor_ms", POLL_FLOOR_MS),
            ("poll_ceiling_ms", POLL_CEILING_MS),
            ("poll_divisor", POLL_DIVISOR),
        ] {
            let block_start = variables_tf
                .find(&format!("variable \"{name}\" {{"))
                .unwrap_or_else(|| panic!("variable \"{name}\" not found in variables.tf"));
            let default_needle = "default     = ";
            let default_start = variables_tf[block_start..]
                .find(default_needle)
                .map(|i| block_start + i + default_needle.len())
                .unwrap_or_else(|| panic!("no default found for {name} in variables.tf"));
            let rest = &variables_tf[default_start..];
            let line_end = rest.find('\n').unwrap_or(rest.len());
            let actual: u64 = rest[..line_end].trim().parse().unwrap_or_else(|_| {
                panic!(
                    "could not parse {name}'s default as a number: {:?}",
                    &rest[..line_end]
                )
            });
            assert_eq!(
                actual, expected,
                "{name}'s Terraform default drifted from the harness constant"
            );
        }
    }

    // --- End-to-end: the actual saving, through the real run()/Edge path ----

    /// Runs `visitors` simulated visitors through the real `run()` loop and
    /// `Edge` cache for `run_secs`, and returns the total client requests
    /// across every endpoint.
    ///
    /// The origin reports `serving_state: "closed"` and refuses `/queue_num`
    /// with a non-200 for the first `closed_secs` of (virtual,
    /// `tokio::time`-paused) elapsed time, and reports `"running"` with a
    /// real position after — exercising `Polling::Backoff`'s `closed ⇒
    /// ceiling` branch when `closed_secs` outlasts `run_secs`, with
    /// `held_position` genuinely still `None` (a non-200 `/queue_num` does
    /// not set it), so the ceiling can only be coming from the `closed`
    /// check, not the position-based fallback.
    ///
    /// `settings.countdown_ms` is always 0 here: it drives visitor arrival
    /// spread and `first_ask_after`, neither of which this helper is
    /// isolating, and coupling it to `closed_secs` would delay most visitors'
    /// arrival past `run_secs` entirely. The branch under test reads the
    /// origin's own response, not that setting.
    ///
    /// Once running, a huge, unreachable `ahead` (the origin answers a
    /// position far beyond what `target_rate` could drain in `run_secs`)
    /// keeps `Polling::Backoff` clamped at the ceiling for the rest of the
    /// run, so its request count is deterministic: exactly one `/status` and
    /// one `/queue_num` per visitor, however the ceiling's jitter draws land.
    async fn measure_client_requests(
        polling: Polling,
        visitors: u64,
        run_secs: u64,
        closed_secs: u64,
    ) -> u64 {
        let edge = Arc::new(Edge::new(Duration::from_secs(10), run_secs));
        let tally = Arc::new(VisitorTally::default());
        let started = tokio::time::Instant::now();
        let origin = Arc::new(move |path: String| async move {
            let closed = started.elapsed().as_secs() < closed_secs;
            if path.starts_with("/v1/status") {
                if closed {
                    (
                        200,
                        r#"{"serving_state":"closed","serving_position":0}"#.to_owned(),
                    )
                } else {
                    (
                        200,
                        r#"{"serving_state":"running","serving_position":0,"target_rate":5}"#
                            .to_owned(),
                    )
                }
            } else if closed {
                // Mirrors read's real 409 ("event not yet open") before the
                // seal: no position is answered, so held_position must not
                // be set from this.
                (409, r#"{"error":"event not yet open"}"#.to_owned())
            } else {
                (200, r#"{"position":100000000,"live_join":true}"#.to_owned())
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(run_secs);
        let settings = RunSettings {
            polling,
            spread_ms: 0,
            countdown_ms: 0,
            arrival: Arrival::Uniform,
            deadline,
            target_rate: 5,
        };

        let mut tasks = Vec::with_capacity(visitors as usize);
        for n in 0..visitors {
            tasks.push(tokio::spawn(run(
                Arc::clone(&edge),
                format!("req-{n}"),
                n + 1,
                settings,
                Arc::clone(&origin),
                Arc::clone(&tally),
            )));
        }
        #[expect(
            clippy::unwrap_used,
            reason = "a panicked visitor task is a test bug, not an expected outcome"
        )]
        for task in tasks {
            task.await.unwrap();
        }

        let mut total = 0u64;
        for (_, client, _, _, _) in edge.snapshot().await {
            total += client;
        }
        total
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_issues_materially_fewer_client_requests_end_to_end() {
        // Closes the gap the tester flagged: the headline saving was only a
        // println! from a manual run, backed by a proxy test that re-derives
        // poll counts from a standalone simulation rather than driving the
        // real async run()/Edge path — so a wiring regression (target_rate
        // not reaching the client, say) would pass every existing test.
        //
        // 25s is shorter than the ceiling's minimum wake time (30s), so
        // Backoff is bounded at exactly one status poll and one queue_num
        // ask per visitor regardless of jitter — deterministic without
        // needing to control Math.random()'s Rust twin.
        const VISITORS: u64 = 10;
        const RUN_SECS: u64 = 25;

        let hold = measure_client_requests(Polling::HoldPosition, VISITORS, RUN_SECS, 0).await;
        let backoff = measure_client_requests(Polling::Backoff, VISITORS, RUN_SECS, 0).await;

        assert_eq!(
            backoff,
            VISITORS * 2,
            "ceiling-clamped Backoff must be exactly one /status + one /queue_num per visitor"
        );
        assert!(
            backoff.saturating_mul(2) < hold,
            "backoff={backoff} hold={hold}: expected backoff materially below hold-position"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_polls_a_closed_event_at_the_ceiling_not_the_floor() {
        // The test above never observes serving_state: "closed" (its
        // closed_secs is 0), so it does not exercise the closed ⇒ ceiling
        // branch above — re-inverting it back to the floor would
        // leave every existing test green. closed_secs longer than the run
        // means every /status answer is "closed" and /queue_num always
        // refuses with a 409, so held_position stays None throughout:
        // Backoff must fall through to the ceiling on the closed check
        // alone, with no position-based path (the other end of the `if
        // closed {} else {}` in the sleep arm) to coincidentally agree.
        const VISITORS: u64 = 10;
        const RUN_SECS: u64 = 25;
        const CLOSED_SECS: u64 = 9999; // outlasts the run: never leaves "closed"

        let total =
            measure_client_requests(Polling::Backoff, VISITORS, RUN_SECS, CLOSED_SECS).await;

        // Ceiling-clamped (>= 30s, jitter up to 39s) exceeds the 25s run, so
        // exactly one /status poll and one refused /queue_num ask per
        // visitor. Losing the closed check (falling through to
        // `None => POLL_FLOOR_MS`) would instead produce several rounds of
        // both per visitor in 25s.
        assert_eq!(
            total,
            VISITORS * 2,
            "closed ⇒ ceiling: exactly one status poll and one refused queue_num ask per visitor"
        );
    }
}
