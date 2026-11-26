//! The time the models stamp events with: the wall clock when the run started,
//! advanced by tokio's clock. Under real time it follows the wall clock; under
//! paused time it moves only as the run does.

use std::sync::OnceLock;

use jiff::{SignedDuration, Timestamp};
use tokio::time::Instant;

static START: OnceLock<(Timestamp, Instant)> = OnceLock::new();

/// Now, on the run's clock. The first call fixes the start.
#[must_use]
pub fn now() -> Timestamp {
    let (wall, start) = *START.get_or_init(|| (Timestamp::now(), Instant::now()));
    let elapsed = SignedDuration::try_from(start.elapsed()).unwrap_or(SignedDuration::MAX);
    wall.checked_add(elapsed).unwrap_or(Timestamp::MAX)
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
}
