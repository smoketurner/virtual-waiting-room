//! Regression coverage for `Edge::serve`'s request collapsing: a concurrent
//! cohort for one key in one TTL window must produce exactly one origin fetch.
//!
//! `harness` is a binary crate with no library target, so an integration test
//! cannot `use harness::...`. Instead this compiles the real source unchanged
//! via `#[path = "../src/edge.rs"] mod edge;`.
//!
//! The invariant this guards: a waiter whose cache-check missed but whose
//! "become leader" re-check lands after the leader has finished (inserted
//! `entries`, removed `inflight`) must not become a redundant leader and run a
//! second origin `fetch()` for the same key in the same TTL window.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::timeout;

#[path = "../src/edge.rs"]
mod edge;

use edge::{Disposition, Edge};

/// One cached 200 response body the fetch closures return.
const BODY: &str = "ok";

/// Builds a fresh `Edge` with a 10s error TTL and 60s of per-second buckets.
fn fresh_edge() -> Arc<Edge> {
    Arc::new(Edge::new(Duration::from_secs(10), 60))
}

/// Reads the (client, origin, hits, collapsed) tallies for `endpoint`.
async fn tallies(edge: &Edge, endpoint: &str) -> (u64, u64, u64, u64) {
    for (ep, client, origin, hits, collapsed) in edge.snapshot().await {
        if ep == endpoint {
            return (client, origin, hits, collapsed);
        }
    }
    (0, 0, 0, 0)
}

// --- Deterministic single-threaded path checks (no race involved) ---------

#[tokio::test(start_paused = true)]
async fn single_miss_fetches_origin_once() {
    let edge = fresh_edge();
    let calls = Arc::new(AtomicU64::new(0));

    let (status, body, disposition) = edge
        .serve("/v1/status", "/v1/status", Duration::from_secs(1), || {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                (200, String::from(BODY))
            }
        })
        .await;

    assert_eq!(status, 200);
    assert_eq!(body, BODY);
    assert_eq!(disposition, Disposition::Miss);
    assert_eq!(calls.load(Ordering::Relaxed), 1, "exactly one origin fetch");
    let (client, origin, hits, collapsed) = tallies(&edge, "/v1/status").await;
    assert_eq!((client, origin, hits, collapsed), (1, 1, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn second_request_within_ttl_is_a_hit() {
    let edge = fresh_edge();
    let calls = Arc::new(AtomicU64::new(0));

    let (_, _, first) = edge
        .serve("/v1/status", "/v1/status", Duration::from_secs(5), || {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                (200, String::from(BODY))
            }
        })
        .await;
    assert_eq!(first, Disposition::Miss);

    let (_, _, second) = edge
        .serve("/v1/status", "/v1/status", Duration::from_secs(5), || {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                (200, String::from(BODY))
            }
        })
        .await;
    assert_eq!(
        second,
        Disposition::Hit,
        "second request within TTL must hit"
    );
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "no second fetch for a cached entry"
    );
    let (client, origin, hits, collapsed) = tallies(&edge, "/v1/status").await;
    assert_eq!((client, origin, hits, collapsed), (2, 1, 1, 0));
}

#[tokio::test(start_paused = true)]
async fn error_response_is_negative_cached_for_error_ttl() {
    // ttl 1s, error_ttl 10s: a 404 must be cached for the larger 10s.
    let edge = Arc::new(Edge::new(Duration::from_secs(10), 60));
    let calls = Arc::new(AtomicU64::new(0));

    let (status, _, first) = edge
        .serve(
            "/v1/queue_num",
            "/v1/queue_num?request_id=a",
            Duration::from_secs(1),
            || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    (404, String::from("nope"))
                }
            },
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(first, Disposition::Miss);

    // Still within the 10s error TTL: must hit, no new fetch.
    let (_, _, second) = edge
        .serve(
            "/v1/queue_num",
            "/v1/queue_num?request_id=a",
            Duration::from_secs(1),
            || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    (404, String::from("nope"))
                }
            },
        )
        .await;
    assert_eq!(
        second,
        Disposition::Hit,
        "404 must be negative-cached for error_ttl"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn origin_timeline_and_peak_record_origin_requests() {
    // `peak_origin_per_second` and `origin_timeline` had no prior coverage
    // (the existing tests only read `snapshot()`'s client column). Asserts a
    // genuine miss bumps the per-second origin bucket exactly once.
    let edge = fresh_edge();
    let calls = Arc::new(AtomicU64::new(0));
    for n in 0..3u64 {
        // A distinct key per call forces three genuine misses (no shared
        // cache entry), so three origin requests get recorded.
        let key = format!("/v1/queue_num?request_id={n}");
        let calls = Arc::clone(&calls);
        edge.serve(
            "/v1/queue_num",
            key.as_str(),
            Duration::from_secs(60),
            || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    (200, String::from(BODY))
                }
            },
        )
        .await;
    }
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    let (peak, at) = edge.peak_origin_per_second();
    assert_eq!(
        peak, 3,
        "three origin requests land in the same fast second"
    );
    assert_eq!(
        at, 0,
        "the run started at t=0 and completed inside the first second"
    );

    let timeline = edge.origin_timeline();
    assert_eq!(timeline.len(), 61, "Edge::new(60) allocates 0..=60 buckets");
    assert_eq!(
        timeline[0], 3,
        "first-second bucket holds all three origin requests"
    );
    assert!(
        timeline[1..].iter().all(|&c| c == 0),
        "no origin requests outside the first second"
    );
}

// --- Concurrent collapsing: the invariant the fix protects ----------------

/// One cohort wave: `wave_size` tasks all call `serve` for the same key with a
/// yielding fetch (the yield injects a cooperative reschedule point that
/// widens the become-leader race window). Returns the fetch-call count and
/// the per-endpoint tallies.
async fn cohort(
    edge: Arc<Edge>,
    wave_size: usize,
    calls: Arc<AtomicU64>,
) -> (u64, u64, u64, u64, u64) {
    let mut tasks = Vec::with_capacity(wave_size);
    for _ in 0..wave_size {
        let edge = Arc::clone(&edge);
        let calls = Arc::clone(&calls);
        tasks.push(tokio::spawn(async move {
            edge.serve(
                "/v1/status",
                "/v1/status",
                Duration::from_secs(1),
                move || {
                    let calls = calls;
                    async move {
                        tokio::task::yield_now().await;
                        calls.fetch_add(1, Ordering::Relaxed);
                        (200, String::from(BODY))
                    }
                },
            )
            .await
        }));
    }
    #[expect(
        clippy::unwrap_used,
        reason = "a panicked serve task is a test bug, not an expected outcome"
    )]
    for task in tasks {
        let _ = task.await.unwrap();
    }
    let (client, origin, hits, collapsed) = tallies(&edge, "/v1/status").await;
    (
        calls.load(Ordering::Relaxed),
        client,
        origin,
        hits,
        collapsed,
    )
}

/// Runs `iterations` cohort waves and asserts the one-fetch-per-window
/// invariant each iteration. Drives the become-leader re-check under real
/// multi-thread concurrency.
async fn assert_one_fetch_per_cohort(worker_threads: usize, wave_size: usize, iterations: usize) {
    for _ in 0..iterations {
        let edge = fresh_edge();
        let calls = Arc::new(AtomicU64::new(0));
        let (fetches, client, origin, hits, collapsed) = cohort(edge, wave_size, calls).await;
        assert_eq!(
            fetches, 1,
            "worker_threads={worker_threads} wave_size={wave_size}: expected exactly one fetch, got {fetches} (origin={origin} hits={hits} collapsed={collapsed})"
        );
        assert_eq!(
            origin, 1,
            "worker_threads={worker_threads} wave_size={wave_size}: origin tally must be 1, got {origin}"
        );
        assert_eq!(
            client, wave_size as u64,
            "worker_threads={worker_threads} wave_size={wave_size}: client tally must equal the wave"
        );
        assert_eq!(
            origin + hits + collapsed,
            client,
            "worker_threads={worker_threads} wave_size={wave_size}: every client request must resolve to exactly one disposition (origin={origin} hits={hits} collapsed={collapsed} client={client})"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn concurrent_cohort_collapses_or_hits_never_redundant_fetch_16() {
    assert_one_fetch_per_cohort(16, 200, 1000).await;
}

// --- Deadlock / heavy-load sanity ----------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn heavy_concurrent_load_completes_without_deadlock() {
    // The fix acquires `entries` while holding `inflight` (nested lock). The
    // leader's finish acquires `entries` then `inflight` (never nested). This
    // exercises both orderings under load and asserts no deadlock and no
    // redundant fetch.
    const ITERATIONS: usize = 100;
    const WAVE: usize = 300;
    for _ in 0..ITERATIONS {
        let edge = fresh_edge();
        let calls = Arc::new(AtomicU64::new(0));
        let ran = timeout(Duration::from_secs(10), cohort(edge, WAVE, calls)).await;
        let (fetches, client, origin, hits, collapsed) =
            ran.expect("cohort deadlocked (nested lock?)");
        assert_eq!(fetches, 1, "no redundant fetch under heavy load");
        assert_eq!(origin, 1);
        assert_eq!(client, WAVE as u64);
        assert_eq!(origin + hits + collapsed, client);
    }
}
