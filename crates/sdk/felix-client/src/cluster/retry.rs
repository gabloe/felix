//! How a [`super::ClusterClient`] paces its retries, and which failures it retries at all.

use std::time::Duration;

/// How long to wait between reconnection attempts, how many to make, and how
/// long the whole thing may take.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Attempts to reach *some* broker before giving up. Each attempt tries
    /// every endpoint.
    pub attempts: usize,
    /// The first retry waits somewhere in `[0, backoff]`. The ceiling doubles
    /// each attempt up to `max_backoff`.
    pub backoff: Duration,
    /// The longest any one wait can be.
    pub max_backoff: Duration,
    /// A ceiling on the whole operation, across every attempt and every sleep.
    ///
    /// Attempt counts alone do not bound time: five attempts against a broker
    /// that takes its full publish timeout to answer is minutes, which is not a
    /// number anybody chose.
    ///
    /// **`None` by default, and deliberately.** A deadline shorter than one
    /// attempt's own timeout prevents any retry at all — the first attempt
    /// spends the whole budget and the loop exits having tried once. The
    /// client's publish timeout is already tens of seconds, so any useful
    /// default here would have to be derived from that rather than picked, and
    /// picking one silently turns a client that recovers from a failover into
    /// one that does not.
    ///
    /// A caller that knows its own latency budget should set it.
    pub deadline: Option<Duration>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            // Enough to outlast a failover on a healthy cluster — the control
            // plane has to notice the leader is gone, which is bounded by its
            // expiry timeout — without waiting out a cluster that is simply
            // down.
            attempts: 5,
            backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
            deadline: None,
        }
    }
}

impl ReconnectPolicy {
    /// How long to wait before attempt `attempt` (0-based), jittered.
    ///
    /// **Full jitter: uniform over `[0, ceiling]`, not the ceiling itself.**
    /// Every client of a cluster notices a failover at the same moment, and an
    /// unjittered backoff has all of them retry in step — arriving together at
    /// whichever broker was just promoted, which is the moment it can least
    /// afford a thundering herd. The broker's own peer pool jitters its redials
    /// for exactly this reason.
    pub(crate) fn delay_before(&self, attempt: usize) -> Duration {
        let ceiling = self
            .backoff
            .saturating_mul(1u32 << attempt.min(16) as u32)
            .min(self.max_backoff);
        ceiling.mul_f64(jitter_fraction())
    }
}

/// Whether an error is worth another attempt.
///
/// **Unknown errors are retried.** The client protocol carries an error as a
/// string with no code, so this is matching on prose, and prose changes. A
/// misclassified retryable error costs one wasted attempt; a misclassified
/// terminal error costs the operation. Defaulting to "retry" puts the cheaper
/// mistake on the likely side.
///
/// Terminal means *no amount of waiting or reconnecting changes the answer*,
/// and the bar for that is higher on a cluster than it looks.
///
/// **"Not found" is not terminal here.** A broker learns its tenants,
/// namespaces and streams from the control plane, and opens a shard only once
/// it has been given it. A broker promoted a moment ago answers "stream not
/// found" for the stream it is about to serve -- being named leader and being
/// ready to serve are different moments. Treating that as terminal breaks
/// exactly the recovery this policy exists to provide, which is not
/// hypothetical: it did.
///
/// What is left is the credential. A permission the token does not carry is a
/// property of its claims rather than of any broker's state, so it fails the
/// same way everywhere and for as long as the token lives.
pub(crate) fn is_terminal(error: &anyhow::Error) -> bool {
    // Terminal by construction rather than by matching prose: the offset asked
    // for is not available, and asking again will not make it so.
    if error
        .downcast_ref::<crate::SubscribeCursorError>()
        .is_some()
    {
        return true;
    }
    format!("{error:#}").to_lowercase().contains("forbidden")
}

/// A uniform fraction in `[0, 1)`, without taking an RNG dependency.
///
/// The same approach the broker's reconnect backoff uses. Its only job is to
/// decorrelate clients that all woke up together, so it needs to be unbiased
/// rather than unpredictable.
fn jitter_fraction() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000_000) / 1_000_000.0
}
