//! Process lifecycle: termination signals, readiness gating, and bounded drain.
//!
//! # Why this exists
//! Kubernetes, systemd, and `docker stop` all terminate a process with **SIGTERM**.
//! Waiting only on `ctrl_c()` (SIGINT) means those signals fall through to the
//! default handler, which kills the process immediately and drops every in-flight
//! publish, acknowledgement, and subscription write. Every rolling update would
//! abort in-flight work.
//!
//! # Shutdown order
//! The order matters more than the individual steps:
//!
//! 1. **Readiness goes false.** Load balancers and the Kubernetes endpoints
//!    controller observe `/ready` and stop routing new traffic here. This happens
//!    before anything stops working, so clients are steered away from a healthy
//!    instance rather than discovering a broken one.
//! 2. **Stop accepting new connections.** Already-accepted work is untouched.
//! 3. **Drain, bounded by a deadline.** In-flight connections finish on their own.
//! 4. **Force-cancel whatever is left, and say what it was.** A drain that silently
//!    hangs until SIGKILL is indistinguishable from the bug it was meant to fix, so
//!    the subsystems that missed the deadline are named in the logs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
// The budget must measure against the same clock that `tokio::time::timeout` uses,
// or the remaining-time arithmetic and the timeouts it drives disagree. This is
// identical to `std::time::Instant` at runtime and additionally tracks tokio's
// paused clock under test.
use tokio::time::Instant;

/// Readiness flag shared between the health endpoints and the shutdown path.
///
/// Cloning shares the underlying flag; every clone observes the same state.
#[derive(Clone, Debug)]
pub struct Readiness {
    ready: Arc<AtomicBool>,
}

impl Readiness {
    /// Create a flag that already reports ready.
    pub fn ready() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Create a flag that reports *not* ready until [`Readiness::mark_ready`].
    ///
    /// For work that must finish before traffic arrives. Reporting ready first
    /// and initialising afterwards is worse than a slow start: an orchestrator
    /// routes to the instance immediately, and requests land on state that does
    /// not exist yet.
    pub fn starting() -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Flip to ready once startup work has finished.
    ///
    /// Returns the previous state. Deliberately not the inverse of
    /// [`Readiness::begin_draining`]: a draining instance must never be brought
    /// back, so callers only use this during startup.
    pub fn mark_ready(&self) -> bool {
        self.ready.swap(true, Ordering::Release)
    }

    /// Whether `/ready` should report success.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Flip to not-ready.
    ///
    /// Call this *before* the listener stops, so traffic is steered away while the
    /// broker can still serve it. Returns the previous state so callers can tell a
    /// first shutdown from a repeated signal.
    pub fn begin_draining(&self) -> bool {
        self.ready.swap(false, Ordering::Release)
    }
}

impl Default for Readiness {
    fn default() -> Self {
        Self::ready()
    }
}

/// Resolves when the process is asked to terminate.
///
/// On Unix this is SIGTERM (Kubernetes, systemd, `docker stop`) or SIGINT (Ctrl-C).
/// On other platforms only Ctrl-C is available. The signal that fired is logged,
/// because "which signal did we get" is the first question when a pod is being
/// killed unexpectedly.
#[cfg(unix)]
pub async fn termination_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    // If SIGTERM can't be registered we still want SIGINT to work rather than
    // leaving the process with no shutdown path at all.
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::error!(
                error = %err,
                "failed to install SIGTERM handler; falling back to SIGINT only"
            );
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!(signal = "SIGINT", "termination signal received");
            return;
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!(signal = "SIGINT", "termination signal received");
        }
        _ = sigterm.recv() => {
            tracing::info!(signal = "SIGTERM", "termination signal received");
        }
    }
}

/// Resolves when the process is asked to terminate.
///
/// Non-Unix targets have no SIGTERM; Ctrl-C is the only portable trigger.
#[cfg(not(unix))]
pub async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!(signal = "CTRL_C", "termination signal received");
}

/// Tracks a drain against a total deadline shared by every subsystem.
///
/// Each subsystem gets whatever is left of the budget rather than its own full
/// copy, so N subsystems cannot stretch a 25s deadline into 25N seconds. Anything
/// that misses the deadline is recorded and reported together at the end.
pub struct DrainBudget {
    deadline: Duration,
    started: Instant,
    unfinished: Vec<&'static str>,
}

impl DrainBudget {
    pub fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            started: Instant::now(),
            unfinished: Vec::new(),
        }
    }

    /// Time left in the overall budget; zero once the deadline has passed.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.started.elapsed())
    }

    /// Await `task` within the remaining budget, recording `name` if it does not
    /// finish in time. Returns whether it finished.
    pub async fn drain<F>(&mut self, name: &'static str, task: F) -> bool
    where
        F: Future<Output = ()>,
    {
        let remaining = self.remaining();
        if remaining.is_zero() {
            self.unfinished.push(name);
            return false;
        }
        match tokio::time::timeout(remaining, task).await {
            Ok(()) => {
                tracing::debug!(subsystem = name, "drained");
                true
            }
            Err(_) => {
                self.unfinished.push(name);
                false
            }
        }
    }

    /// Subsystems that did not finish within the deadline.
    pub fn unfinished(&self) -> &[&'static str] {
        &self.unfinished
    }

    /// Log the outcome. Forced cancellation is a warning, not an info line: it
    /// means work was dropped and the operator needs to see it.
    pub fn report(&self) {
        let elapsed_ms = self.started.elapsed().as_millis();
        if self.unfinished.is_empty() {
            tracing::info!(elapsed_ms, "drain complete");
        } else {
            tracing::warn!(
                elapsed_ms,
                deadline_ms = self.deadline.as_millis(),
                unfinished = ?self.unfinished,
                "drain deadline expired; forcing cancellation"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_starts_ready_and_flips_once() {
        let readiness = Readiness::ready();
        assert!(readiness.is_ready());
        // The first call reports the previous state so a repeated signal is
        // distinguishable from the first one.
        assert!(readiness.begin_draining());
        assert!(!readiness.is_ready());
        assert!(!readiness.begin_draining());
    }

    #[test]
    fn readiness_clones_share_state() {
        let readiness = Readiness::ready();
        let clone = readiness.clone();
        readiness.begin_draining();
        assert!(!clone.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn drain_records_only_the_subsystem_that_timed_out() {
        let mut budget = DrainBudget::new(Duration::from_millis(1000));
        assert!(budget.drain("fast", async {}).await);
        assert!(
            !budget
                .drain("slow", tokio::time::sleep(Duration::from_secs(60)))
                .await
        );
        assert_eq!(budget.unfinished(), ["slow"]);
    }

    #[tokio::test(start_paused = true)]
    async fn budget_is_shared_across_subsystems_not_per_subsystem() {
        // Two subsystems that each hang must not consume a full deadline apiece.
        let mut budget = DrainBudget::new(Duration::from_millis(500));
        let started = Instant::now();
        budget
            .drain("first", tokio::time::sleep(Duration::from_secs(60)))
            .await;
        budget
            .drain("second", tokio::time::sleep(Duration::from_secs(60)))
            .await;
        assert!(started.elapsed() < Duration::from_millis(1000));
        assert_eq!(budget.unfinished(), ["first", "second"]);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_budget_records_without_awaiting() {
        let mut budget = DrainBudget::new(Duration::from_millis(10));
        budget
            .drain("first", tokio::time::sleep(Duration::from_secs(60)))
            .await;
        assert!(budget.remaining().is_zero());
        // Past the deadline nothing else is even polled.
        assert!(!budget.drain("second", std::future::pending()).await);
        assert_eq!(budget.unfinished(), ["first", "second"]);
    }
}
