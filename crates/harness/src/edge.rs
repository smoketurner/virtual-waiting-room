//! A model of the CDN in front of the read path, built to count what reaches
//! the origin.
//!
//! Not an emulator. It reproduces the three behaviours origin load actually
//! depends on, and nothing else:
//!
//! - **Cache key composition.** `/status` is keyed on path alone, so every
//!   waiting visitor shares one entry. `/queue_num` adds the request id,
//!   so every visitor has their own and no two ever share a hit. That single
//!   difference is what decides whether origin load is flat or proportional to
//!   the number of people waiting.
//! - **Request collapsing.** Concurrent misses for one key produce one origin
//!   fetch, and the rest wait for it. Without this a cohort's first poll after
//!   an expiry is one origin request per visitor, and a model that skipped it
//!   would report the fan-out as far worse than it is.
//! - **Negative caching.** Errors are cached for the larger of the error TTL
//!   and the response's own max-age, and a waiting visitor polls a 404 for as
//!   long as their position is unassigned, so leaving it out would overstate
//!   origin load for the whole join window.
//!
//! Deliberately absent: multiple edge locations, eviction under memory
//! pressure, and anything about latency or quotas. Those change the numbers on
//! real infrastructure; they do not change whether a cache key is shared.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, broadcast};

/// What the CDN did with one client request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Served from a stored entry.
    Hit,
    /// Fetched from the origin.
    Miss,
    /// Waited on another request's in-flight fetch of the same key.
    Collapsed,
}

/// One cached response.
#[derive(Debug, Clone)]
struct Entry {
    status: u16,
    body: String,
    expires_at: Instant,
}

/// A fetch already in flight for a key, so arrivals can wait rather than
/// starting a second one.
type Inflight = broadcast::Sender<(u16, String)>;

/// Per-endpoint tallies. Client requests are what visitors sent; origin
/// requests are what got through, which is the number the design's claims are
/// really about.
#[derive(Debug, Default)]
pub struct Counts {
    pub client: AtomicU64,
    pub origin: AtomicU64,
    pub hits: AtomicU64,
    pub collapsed: AtomicU64,
}

/// The cache, its in-flight fetches, and the tallies.
#[derive(Debug)]
pub struct Edge {
    entries: Mutex<HashMap<String, Entry>>,
    inflight: Mutex<HashMap<String, Inflight>>,
    counts: Mutex<HashMap<String, Arc<Counts>>>,
    /// Minimum lifetime for an error response, independent of its own headers.
    error_ttl: Duration,
    /// Origin requests bucketed by the second they happened in.
    ///
    /// A total says how much work an event costs; the peak second says whether
    /// it fits through a throttle. They can differ by orders of magnitude when
    /// a cohort does something in unison, which is exactly the case here.
    per_second: Vec<AtomicU64>,
    started: Instant,
}

impl Edge {
    #[must_use]
    pub fn new(error_ttl: Duration, run_seconds: u64) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            counts: Mutex::new(HashMap::new()),
            error_ttl,
            per_second: (0..=run_seconds).map(|_| AtomicU64::new(0)).collect(),
            started: Instant::now(),
        }
    }

    /// The busiest second of the run, and which second it was.
    #[must_use]
    pub fn peak_origin_per_second(&self) -> (u64, u64) {
        self.per_second
            .iter()
            .enumerate()
            .map(|(second, count)| (count.load(Ordering::Relaxed), second as u64))
            .max()
            .unwrap_or((0, 0))
    }

    /// Origin requests per second, in order.
    #[must_use]
    pub fn origin_timeline(&self) -> Vec<u64> {
        self.per_second
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }

    fn record_origin(&self) {
        let second = self.started.elapsed().as_secs() as usize;
        if let Some(bucket) = self.per_second.get(second) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Tallies for one endpoint, created on first use.
    pub async fn counts_for(&self, endpoint: &str) -> Arc<Counts> {
        let mut counts = self.counts.lock().await;
        Arc::clone(
            counts
                .entry(endpoint.to_owned())
                .or_insert_with(|| Arc::new(Counts::default())),
        )
    }

    /// Every endpoint's tallies, for the report.
    pub async fn snapshot(&self) -> Vec<(String, u64, u64, u64, u64)> {
        let counts = self.counts.lock().await;
        let mut rows: Vec<_> = counts
            .iter()
            .map(|(endpoint, c)| {
                (
                    endpoint.clone(),
                    c.client.load(Ordering::Relaxed),
                    c.origin.load(Ordering::Relaxed),
                    c.hits.load(Ordering::Relaxed),
                    c.collapsed.load(Ordering::Relaxed),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    /// Serves one request, going to the origin only when nothing usable is
    /// cached and no identical fetch is already running.
    ///
    /// `key` is the composed cache key — the caller decides what belongs in it,
    /// because that composition is the thing under test.
    pub async fn serve<F, Fut>(
        &self,
        endpoint: &str,
        key: &str,
        ttl: Duration,
        fetch: F,
    ) -> (u16, String, Disposition)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = (u16, String)>,
    {
        let counts = self.counts_for(endpoint).await;
        counts.client.fetch_add(1, Ordering::Relaxed);

        let now = Instant::now();
        {
            let entries = self.entries.lock().await;
            if let Some(entry) = entries.get(key)
                && entry.expires_at > now
            {
                counts.hits.fetch_add(1, Ordering::Relaxed);
                return (entry.status, entry.body.clone(), Disposition::Hit);
            }
        }

        // Join an identical fetch rather than starting a second one. The
        // receiver is taken while holding the lock so a fetch cannot finish and
        // be forgotten between the lookup and the subscribe.
        let waiter = {
            let inflight = self.inflight.lock().await;
            inflight.get(key).map(broadcast::Sender::subscribe)
        };
        if let Some(mut rx) = waiter {
            counts.collapsed.fetch_add(1, Ordering::Relaxed);
            return match rx.recv().await {
                Ok((status, body)) => (status, body, Disposition::Collapsed),
                // The leader died without publishing; report it rather than
                // silently making this look like a successful collapse.
                Err(_) => (502, String::from("collapse failed"), Disposition::Collapsed),
            };
        }

        let (tx, _) = broadcast::channel(1);
        {
            let mut inflight = self.inflight.lock().await;
            // Another task may have become leader while this one was deciding.
            if let Some(existing) = inflight.get(key) {
                let mut rx = existing.subscribe();
                drop(inflight);
                counts.collapsed.fetch_add(1, Ordering::Relaxed);
                return match rx.recv().await {
                    Ok((status, body)) => (status, body, Disposition::Collapsed),
                    Err(_) => (502, String::from("collapse failed"), Disposition::Collapsed),
                };
            }
            inflight.insert(key.to_owned(), tx.clone());
        }

        counts.origin.fetch_add(1, Ordering::Relaxed);
        self.record_origin();
        let (status, body) = fetch().await;

        // An error outlives its own max-age: a waiting visitor polls a 404 for
        // the whole time their position is unassigned, and caching it for only
        // the response TTL would send nearly every one of those polls through.
        let lifetime = if status >= 400 {
            ttl.max(self.error_ttl)
        } else {
            ttl
        };
        {
            let mut entries = self.entries.lock().await;
            entries.insert(
                key.to_owned(),
                Entry {
                    status,
                    body: body.clone(),
                    expires_at: Instant::now() + lifetime,
                },
            );
        }
        {
            let mut inflight = self.inflight.lock().await;
            inflight.remove(key);
        }
        let _ = tx.send((status, body.clone()));

        (status, body, Disposition::Miss)
    }
}
