//! What a run asks of the room, and the parts of a run every tool drives the
//! same way: when visitors arrive, how long the run may take, and the
//! controller's cadence.

use std::sync::Arc;
use std::time::Duration;

use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit};
use tokio::task::JoinHandle;

use crate::edge::Throttles;
use crate::rng::Rng;
use crate::visitor::{self, Record, World};

/// When visitors arrive, relative to the open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Over the countdown, most of them at its end.
    Late,
    /// Evenly over the countdown.
    Uniform,
    /// All at once, over the window after the open.
    Herd,
}

/// One run's crowd and origin.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Visitors who arrive as [`Self::arrival`] says.
    pub visitors: usize,
    /// Visitors who arrive after them, over the herd window starting
    /// [`Self::after_open_from`] after the open.
    pub after_open: usize,
    pub after_open_from: SignedDuration,
    /// How many sessions the origin takes at once.
    pub concurrency: u32,
    /// How long each session lasts.
    pub session: SignedDuration,
    /// From the start of the run to the open.
    pub countdown: SignedDuration,
    pub arrival: Arrival,
    /// How long a herd takes to arrive.
    pub herd_window: SignedDuration,
    /// The share of visitors who join and never come back.
    pub no_show: f64,
    /// The share of join messages SQS delivers twice.
    pub duplicate_rate: f64,
    /// API Gateway's limits, scaled for a scaled-down model of a larger event.
    pub scale: f64,
    pub seed: u64,
}

impl Scenario {
    /// When a visitor who drew `u` arrives, after the run starts.
    #[must_use]
    pub fn arrival_after(&self, u: f64) -> Duration {
        let countdown = self.countdown.as_secs_f64();
        let secs = match self.arrival {
            Arrival::Late => countdown * (1.0 - u.powi(5)),
            Arrival::Uniform => countdown * u,
            Arrival::Herd => countdown + self.herd_window.as_secs_f64() * u,
        };
        Duration::from_secs_f64(secs.max(0.0))
    }

    /// When the event opens for a run starting at `now`: the countdown later,
    /// rounded up to the whole second `starts_at` is stored as.
    #[must_use]
    pub fn open_at(&self, now: Timestamp) -> Timestamp {
        let at = now + self.countdown;
        at.round(
            TimestampRound::new()
                .smallest(Unit::Second)
                .mode(RoundMode::Ceil),
        )
        .unwrap_or(at)
    }

    /// How long the run may take: the last arrival, half as long again as a
    /// full origin takes to admit everyone, and five minutes, at most `max`.
    #[must_use]
    pub fn budget(&self, max: SignedDuration) -> SignedDuration {
        // A full origin turns over once a session length, and a slot is seen
        // free up to a pass after its session ends.
        let turnover = self.session + controller::INTERVAL;
        let everyone = self.visitors.saturating_add(self.after_open) as u64;
        let rounds = everyone.div_ceil(u64::from(self.concurrency.max(1)));
        let drain = turnover * i32::try_from(rounds).unwrap_or(i32::MAX);
        let last_arrival = match self.arrival {
            Arrival::Herd => self.countdown + self.herd_window,
            Arrival::Late | Arrival::Uniform if self.after_open > 0 => {
                self.countdown + self.after_open_from + self.herd_window
            }
            Arrival::Late | Arrival::Uniform => self.countdown,
        };
        (last_arrival + drain * 3 / 2 + SignedDuration::from_mins(5)).min(max)
    }

    /// AWS's default account limit and the stage throttles `core/api.tf`
    /// defaults to, scaled by [`Self::scale`].
    #[must_use]
    pub fn throttles(&self) -> Throttles {
        Throttles {
            account_rate: 10_000.0 * self.scale,
            account_burst: 5_000.0 * self.scale,
            join_rate: 7_000.0 * self.scale,
            join_burst: 3_500.0 * self.scale,
            method_rate: 10_000.0 * self.scale,
            method_burst: 5_000.0 * self.scale,
        }
    }

    /// Starts every visitor, each at its arrival.
    #[must_use]
    pub fn spawn_visitors(&self, world: &Arc<World>) -> Vec<JoinHandle<Record>> {
        let mut rng = Rng::new(self.seed);
        let mut handles = Vec::with_capacity(self.visitors.saturating_add(self.after_open));
        for i in 0..self.visitors.saturating_add(self.after_open) {
            let mut v = rng.fork();
            let after = if i < self.visitors {
                self.arrival_after(v.unit())
            } else {
                (self.countdown + self.after_open_from + self.herd_window.mul_f64(v.unit()))
                    .unsigned_abs()
            };
            let no_show = v.unit() < self.no_show;
            handles.push(tokio::spawn(visitor::visit(
                Arc::clone(world),
                v,
                world.started + after,
                no_show,
            )));
        }
        handles
    }
}

/// Waits for every visitor and collects their records.
pub async fn collect(handles: Vec<JoinHandle<Record>>) -> Vec<Record> {
    let mut records = Vec::with_capacity(handles.len());
    for h in handles {
        if let Ok(r) = h.await {
            records.push(r);
        }
    }
    records
}

/// Runs a controller pass every [`controller::INTERVAL`] until `stop`
/// changes, as the durable execution does, and returns each pass's outcome.
pub async fn run_controller<S: controller::Store + Sync>(
    store: &S,
    event_id: &str,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Vec<Result<controller::PassOutcome, String>> {
    let mut passes = Vec::new();
    loop {
        let outcome = controller::run_pass(store, event_id, crate::clock::now()).await;
        passes.push(outcome.map_err(|e| e.to_string()));
        tokio::select! {
            () = tokio::time::sleep(controller::INTERVAL.unsigned_abs()) => {}
            _ = stop.changed() => return passes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scenario(arrival: Arrival) -> Scenario {
        Scenario {
            visitors: 2_000,
            after_open: 0,
            after_open_from: SignedDuration::ZERO,
            concurrency: 400,
            session: SignedDuration::from_mins(2),
            countdown: SignedDuration::from_secs(30),
            arrival,
            herd_window: SignedDuration::from_secs(10),
            no_show: 0.0,
            duplicate_rate: 0.0,
            scale: 1.0,
            seed: 1,
        }
    }

    #[test]
    fn arrivals_fall_inside_their_window() {
        for u in [0.0, 0.5, 0.999] {
            assert!(scenario(Arrival::Late).arrival_after(u) <= Duration::from_secs(30));
            assert!(scenario(Arrival::Uniform).arrival_after(u) <= Duration::from_secs(30));
            let herd = scenario(Arrival::Herd).arrival_after(u);
            assert!(herd >= Duration::from_secs(30) && herd <= Duration::from_secs(40));
        }
    }

    #[test]
    fn the_open_falls_on_the_whole_second_at_or_after_the_countdown() {
        let s = scenario(Arrival::Late);
        let now = Timestamp::constant(1_000, 250_000_000);
        assert_eq!(s.open_at(now), Timestamp::constant(1_031, 0));
        assert_eq!(
            s.open_at(Timestamp::constant(1_000, 0)),
            Timestamp::constant(1_030, 0)
        );
    }

    #[test]
    fn the_budget_covers_the_drain_and_stops_at_the_cap() {
        // Five rounds of 130 s, half as long again, after a 30 s countdown,
        // plus five minutes.
        let budget = scenario(Arrival::Late).budget(SignedDuration::from_hours(1));
        assert_eq!(budget, SignedDuration::from_secs(30 + 975 + 300));
        assert_eq!(
            scenario(Arrival::Late).budget(SignedDuration::from_mins(10)),
            SignedDuration::from_mins(10)
        );
    }
}
