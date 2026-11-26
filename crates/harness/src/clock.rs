//! The time the models stamp events with: the wall clock when the run started,
//! advanced by tokio's clock. Under real time it follows the wall clock; under
//! paused time it moves only as the run does.

use std::sync::OnceLock;

use jiff::{SignedDuration, Timestamp};
use tokio::time::Instant;

static START: OnceLock<(Timestamp, std::time::Instant, Instant)> = OnceLock::new();

fn start() -> (Timestamp, std::time::Instant, Instant) {
    *START.get_or_init(|| (Timestamp::now(), std::time::Instant::now(), Instant::now()))
}

/// Now, on the run's clock. The first call fixes the start.
#[must_use]
pub fn now() -> Timestamp {
    let (wall, _, start) = start();
    let elapsed = SignedDuration::try_from(start.elapsed()).unwrap_or(SignedDuration::MAX);
    wall.checked_add(elapsed).unwrap_or(Timestamp::MAX)
}

/// The run's clock as a `std::time::Instant`, for code that ages a cache on
/// the environment's monotonic clock.
#[must_use]
pub fn monotonic() -> std::time::Instant {
    let (_, base, start) = start();
    base + start.elapsed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_clock_moves_with_tokio_time() {
        let before = now();
        tokio::time::sleep(std::time::Duration::from_secs(900)).await;
        assert_eq!(now().duration_since(before), SignedDuration::from_secs(900));
    }

    #[tokio::test(start_paused = true)]
    async fn the_monotonic_clock_moves_with_tokio_time() {
        let before = monotonic();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert_eq!(monotonic() - before, std::time::Duration::from_secs(5));
    }
}
