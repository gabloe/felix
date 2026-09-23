// Retention: deleting the oldest sealed segments once a bound is exceeded.
//
// This runs on its own timer rather than from an append. Retention is bulk file
// deletion — it unlinks whole segments and their indexes — and putting that on
// the publish path would trade a bounded disk for an unbounded p999. The
// rollover work is the cautionary tale: storage work sharing a lock with
// appends is what makes appends wait on flushes.
//
// See `docs/durable-storage.md` for what a trimmed log means to a reader.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::disk_log::segments::RetentionOutcome;
use crate::{Result, StorageError, metrics_names};

/// Background task that enforces retention on a timer.
///
/// Holds only a weak reference to the log, so a dropped log stops the work
/// instead of being kept alive by its own housekeeping.
#[derive(Debug)]
pub struct RetentionTask {
    shutdown: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
}

impl RetentionTask {
    /// Run `sweep` every `interval` until shutdown.
    ///
    /// A failed sweep is logged and retried on the next tick rather than
    /// killing the task: a transient I/O error must not silently turn retention
    /// off for the rest of the process's life, because the symptom of that is a
    /// full disk hours later.
    pub fn spawn<F, Fut>(interval: Duration, sweep: F) -> Result<Self>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<RetentionOutcome>> + Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(StorageError::InvalidConfig(
                "retention needs a Tokio runtime; open the log from async code or leave retention_bytes and retention_age unset",
            ));
        }
        let shutdown = Arc::new(Notify::new());
        let signal = Arc::clone(&shutdown);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A stall must not queue up a burst of sweeps; one late sweep
            // reclaims exactly what five would have.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Consume the immediate first tick. A sweep racing `open` would
            // mean a freshly opened log could delete records before its caller
            // has read anything, which makes "what can I still read" depend on
            // scheduling. Callers wanting an immediate pass have
            // `enforce_retention_now`.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match sweep().await {
                            Ok(outcome) if outcome.segments_deleted > 0 => {
                                tracing::info!(
                                    segments = outcome.segments_deleted,
                                    bytes = outcome.bytes_reclaimed,
                                    base_offset = outcome.base_offset,
                                    "retention deleted segments"
                                );
                            }
                            Ok(_) => {}
                            Err(err) => {
                                metrics::counter!(metrics_names::RETENTION_FAILURES_TOTAL)
                                    .increment(1);
                                tracing::error!(error = %err, "retention sweep failed");
                            }
                        }
                    }
                    // No final sweep on shutdown: retention is not a durability
                    // obligation, and deleting data on the way out is the last
                    // thing a stopping process should do.
                    _ = signal.notified() => return,
                }
            }
        });
        Ok(Self { shutdown, handle })
    }

    /// Stop the task and wait for it to observe the signal.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        // `notify_waiters` does not latch, so a task not yet parked on
        // `notified()` would miss it; nudge until it lands.
        loop {
            if self.handle.is_finished() {
                break;
            }
            self.shutdown.notify_waiters();
            tokio::task::yield_now().await;
        }
        let _ = self.handle.await;
    }
}

/// Micros since the Unix epoch, matching `AppendRecord::timestamp_micros`.
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
