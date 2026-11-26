//! Per-second counts of what the edge saw: requests by path and status, what
//! reached an origin, and what API Gateway throttled. The report reads its
//! rates from here.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::Instant;

#[derive(Debug, Default)]
struct Series {
    /// `(second, route, status)` → requests from visitors.
    viewer: BTreeMap<(u64, &'static str, u16), u64>,
    /// `(second, route)` → requests that reached an origin (a cache miss, a
    /// join enqueued, a protected request the gate passed).
    origin: BTreeMap<(u64, &'static str), u64>,
    /// `(second, route)` → requests API Gateway answered 429.
    throttled: BTreeMap<(u64, &'static str), u64>,
}

pub struct Metrics {
    started: Instant,
    series: Mutex<Series>,
    gate_failed_open: AtomicU64,
}

impl Metrics {
    #[must_use]
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            series: Mutex::new(Series::default()),
            gate_failed_open: AtomicU64::new(0),
        }
    }

    fn second(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    pub fn viewer(&self, route: &'static str, status: u16) {
        let second = self.second();
        if let Ok(mut s) = self.series.lock() {
            *s.viewer.entry((second, route, status)).or_default() += 1;
        }
    }

    pub fn origin(&self, route: &'static str) {
        let second = self.second();
        if let Ok(mut s) = self.series.lock() {
            *s.origin.entry((second, route)).or_default() += 1;
        }
    }

    pub fn throttled(&self, route: &'static str) {
        let second = self.second();
        if let Ok(mut s) = self.series.lock() {
            *s.throttled.entry((second, route)).or_default() += 1;
        }
    }

    pub fn gate_failed_open(&self) {
        self.gate_failed_open.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn gate_failed_open_count(&self) -> u64 {
        self.gate_failed_open.load(Ordering::Relaxed)
    }

    /// Origin requests for `route`, per second from the start of the run.
    #[must_use]
    pub fn origin_series(&self, route: &str, seconds: u64) -> Vec<u64> {
        let mut out = vec![0; usize::try_from(seconds).unwrap_or(0) + 1];
        if let Ok(s) = self.series.lock() {
            for (&(second, r), &n) in &s.origin {
                if r == route
                    && let Some(slot) = usize::try_from(second).ok().and_then(|i| out.get_mut(i))
                {
                    *slot += n;
                }
            }
        }
        out
    }

    /// Visitor requests for `route`, total, and by status.
    #[must_use]
    pub fn viewer_totals(&self) -> BTreeMap<(&'static str, u16), u64> {
        let mut out = BTreeMap::new();
        if let Ok(s) = self.series.lock() {
            for (&(_, route, status), &n) in &s.viewer {
                *out.entry((route, status)).or_default() += n;
            }
        }
        out
    }

    /// The busiest second's visitor requests, across every route.
    #[must_use]
    pub fn peak_viewer_rate(&self) -> u64 {
        let mut per_second: BTreeMap<u64, u64> = BTreeMap::new();
        if let Ok(s) = self.series.lock() {
            for (&(second, _, _), &n) in &s.viewer {
                *per_second.entry(second).or_default() += n;
            }
        }
        per_second.values().copied().max().unwrap_or(0)
    }

    #[must_use]
    pub fn throttled_totals(&self) -> BTreeMap<&'static str, u64> {
        let mut out = BTreeMap::new();
        if let Ok(s) = self.series.lock() {
            for (&(_, route), &n) in &s.throttled {
                *out.entry(route).or_default() += n;
            }
        }
        out
    }
}
