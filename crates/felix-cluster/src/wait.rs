//! Polling for a real condition, with a deadline.
//!
//! The acceptance criterion this exists for: the harness waits on real readiness
//! rather than fixed sleeps. A timeout says what it was waiting for, because a
//! harness that fails with "timed out" tells a contributor nothing.
use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

/// How often a condition is re-checked. Short enough that start-up is not
/// dominated by the poll interval, long enough not to spin.
const POLL: Duration = Duration::from_millis(50);

/// Multiplier applied to every deadline here, from `FELIX_TEST_TIMEOUT_SCALE`.
///
/// The budgets in these tests are wall-clock constants chosen against a
/// developer machine, and what they are waiting for is almost always setup --
/// a leader to be elected, a replica to catch up -- rather than the thing under
/// test. On a shared CI runner, and especially under coverage instrumentation,
/// that setup takes longer for reasons that say nothing about the code, and a
/// failure there reports as the semantic having broken. It has cost real
/// investigation more than once.
///
/// So the budget is tunable where it runs slowly, rather than being raised for
/// everyone: a bigger constant would hide a genuine hang behind a longer wait
/// on the machines that are fast enough to notice. Unset means 1, so a
/// developer's run is unchanged.
static SCALE: std::sync::LazyLock<f64> = std::sync::LazyLock::new(|| {
    std::env::var("FELIX_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|scale| scale.is_finite() && *scale >= 1.0)
        .unwrap_or(1.0)
});

/// `timeout` scaled for the machine this is running on.
pub fn budget(timeout: Duration) -> Duration {
    timeout.mul_f64(*SCALE)
}

/// Poll `condition` until it is true, or fail saying what never happened.
pub async fn until<F, Fut>(timeout: Duration, what: &str, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let timeout = budget(timeout);
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        tokio::time::sleep(POLL).await;
    }
}
