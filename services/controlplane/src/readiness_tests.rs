//! Readiness: what it answers, how often it asks, and what it costs.
use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::store::StoreError;

/// A backend that answers on command and counts how often it is asked.
struct Fake {
    healthy: AtomicBool,
    hang: AtomicBool,
    asked: AtomicUsize,
}

impl Fake {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            healthy: AtomicBool::new(true),
            hang: AtomicBool::new(false),
            asked: AtomicUsize::new(0),
        })
    }

    fn asked(&self) -> usize {
        self.asked.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl HealthProbe for Fake {
    async fn health_check(&self) -> StoreResult<()> {
        self.asked.fetch_add(1, Ordering::Relaxed);
        if self.hang.load(Ordering::Relaxed) {
            // Longer than any timeout a test sets.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        if self.healthy.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(StoreError::Unexpected(anyhow::anyhow!(
                "connection refused"
            )))
        }
    }
}

fn readiness(store: Arc<Fake>, ttl: Duration) -> Readiness {
    Readiness::with_limits(store, Duration::from_millis(50), ttl)
}

#[tokio::test]
async fn a_healthy_store_is_ready() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    assert_eq!(readiness.check().await, Ok(()));
}

/// A load balancer has to be able to take this instance out.
#[tokio::test]
async fn a_failing_store_is_not_ready_and_says_why() {
    let store = Fake::new();
    store.healthy.store(false, Ordering::Relaxed);
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    match readiness.check().await {
        Err(NotReady::Store { detail }) => assert!(detail.contains("connection refused")),
        other => panic!("expected a store failure, got {other:?}"),
    }
}

/// **A probe that hangs is worse than one that fails.** The prober keeps
/// sending traffic to an instance nobody has heard from.
#[tokio::test]
async fn a_store_that_never_answers_is_not_ready() {
    let store = Fake::new();
    store.hang.store(true, Ordering::Relaxed);
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    let started = Instant::now();
    let outcome = readiness.check().await;

    assert_eq!(outcome, Err(NotReady::Timeout { after_ms: 50 }));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the check waited on the store instead of bounding it",
    );
}

/// **Readiness comes back on its own.** A transient outage must not need an
/// operator, or a restart, to clear.
#[tokio::test]
async fn readiness_recovers_when_the_store_does() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::ZERO);

    store.healthy.store(false, Ordering::Relaxed);
    assert!(readiness.check().await.is_err());

    store.healthy.store(true, Ordering::Relaxed);
    assert_eq!(readiness.check().await, Ok(()));
}

/// Probers multiply. Ten of them polling must not be ten queries on the pool
/// that real work needs.
#[tokio::test]
async fn probes_inside_the_window_share_one_answer() {
    let store = Fake::new();
    let readiness = readiness(Arc::clone(&store), Duration::from_secs(30));
    let now = Instant::now();

    for _ in 0..10 {
        assert_eq!(readiness.check_at(now).await, Ok(()));
    }

    assert_eq!(store.asked(), 1, "the cache did not hold");
}

/// And the window ends, or a failure would never be noticed.
#[tokio::test]
async fn the_store_is_asked_again_once_the_window_passes() {
    let store = Fake::new();
    let ttl = Duration::from_secs(1);
    let readiness = readiness(Arc::clone(&store), ttl);
    let now = Instant::now();

    readiness.check_at(now).await.expect("ready");
    readiness.check_at(now + ttl / 2).await.expect("ready");
    assert_eq!(store.asked(), 1);

    readiness.check_at(now + ttl).await.expect("ready");
    assert_eq!(store.asked(), 2);
}

/// A failure is cached like a success, so recovery is visible within the window
/// rather than immediately — the cost of not hammering a struggling database.
#[tokio::test]
async fn a_failure_is_cached_for_the_same_window() {
    let store = Fake::new();
    let ttl = Duration::from_secs(1);
    let readiness = readiness(Arc::clone(&store), ttl);
    let now = Instant::now();

    store.healthy.store(false, Ordering::Relaxed);
    assert!(readiness.check_at(now).await.is_err());

    store.healthy.store(true, Ordering::Relaxed);
    assert!(
        readiness.check_at(now + ttl / 2).await.is_err(),
        "the cached failure was not held",
    );
    assert_eq!(
        readiness.check_at(now + ttl).await,
        Ok(()),
        "recovery took longer than the window",
    );
}
