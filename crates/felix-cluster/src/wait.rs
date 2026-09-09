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

/// Poll `condition` until it is true, or fail saying what never happened.
pub async fn until<F, Fut>(timeout: Duration, what: &str, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
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
