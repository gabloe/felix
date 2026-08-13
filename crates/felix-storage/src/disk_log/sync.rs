// Durability policy: when written bytes are pushed to stable storage, and who
// waits for it.
//
// The three `FsyncMode` variants differ only in *when* a flush is triggered:
//
// | mode                 | flush trigger                | acknowledged when          |
// |----------------------|------------------------------|----------------------------|
// | `None`               | segment seal, shutdown       | bytes reach the page cache |
// | `Periodic{interval}` | background timer             | bytes reach the page cache |
// | `OnCommit`           | the append itself            | bytes reach the device     |
//
// ## Group commit
//
// `OnCommit` is the expensive one, and the reason it is affordable at all is
// group commit. An `fsync` flushes *the whole file*, not one caller's bytes — so
// when N appends are in flight, one flush can satisfy all N. Every appender
// contends for the same flush lock; the winner flushes, and the losers wake to
// find their target already durable and return without flushing again. Under
// concurrency the device sees one flush per round trip rather than one per
// append, which is the difference between a few hundred and a few tens of
// thousands of durable appends per second on the same hardware.
//
// This is the same mechanism behind PostgreSQL's `commit_delay` group commit and
// the WAL group-commit path in MySQL and RocksDB. `felix_storage_sync_batch_appends`
// reports the fan-in actually achieved.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify, watch};

use crate::Result;
use crate::log::{FsyncMode, Offset};
use crate::{StorageError, metrics_names};

/// A flush that reached a bounded retry limit without advancing. Only reachable
/// if a flush implementation reports less progress than it made, which would
/// otherwise spin.
const MAX_FLUSH_ATTEMPTS: usize = 8;

/// Tracks how much of the log is durable and coordinates who flushes.
#[derive(Debug)]
pub struct Durability {
    mode: FsyncMode,
    /// Exclusive upper bound: every offset below this is on stable storage.
    /// A watch channel so waiters are woken by the flusher rather than polling.
    durable_tx: watch::Sender<Offset>,
    /// Held by whichever task is currently flushing. Everyone else queues here
    /// and finds the work already done when they get in.
    flush_lock: Mutex<()>,
    /// Appends currently waiting for durability, sampled to report fan-in.
    waiting: AtomicU64,
}

impl Durability {
    /// `durable_upto` is the exclusive offset bound recovered from disk — every
    /// record already on disk at open time is by definition durable.
    pub fn new(mode: FsyncMode, durable_upto: Offset) -> Self {
        Self {
            mode,
            durable_tx: watch::channel(durable_upto).0,
            flush_lock: Mutex::new(()),
            waiting: AtomicU64::new(0),
        }
    }

    pub fn mode(&self) -> FsyncMode {
        self.mode
    }

    /// Exclusive bound on durable offsets: everything below it survives a crash.
    pub fn durable_upto(&self) -> Offset {
        *self.durable_tx.borrow()
    }

    /// Publish flush progress. Monotonic: a late report cannot walk it back.
    pub fn note_durable(&self, durable_upto: Offset) {
        self.durable_tx.send_if_modified(|current| {
            if durable_upto > *current {
                *current = durable_upto;
                true
            } else {
                false
            }
        });
    }

    /// Whether an append must wait for a flush before it may be acknowledged.
    pub fn acknowledges_before_sync(&self) -> bool {
        !matches!(self.mode, FsyncMode::OnCommit)
    }

    /// Block until every offset below `target` is durable.
    ///
    /// `flush` performs one device flush and returns the exclusive offset bound
    /// it made durable. It is called by at most one task at a time; concurrent
    /// callers wait on that single flush.
    pub async fn ensure_durable<F, Fut>(&self, target: Offset, flush: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Offset>>,
    {
        let mut receiver = self.durable_tx.subscribe();
        if *receiver.borrow_and_update() >= target {
            return Ok(());
        }

        self.waiting.fetch_add(1, Ordering::Relaxed);
        let result = self.flush_until(target, flush, &mut receiver).await;
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn flush_until<F, Fut>(
        &self,
        target: Offset,
        flush: F,
        receiver: &mut watch::Receiver<Offset>,
    ) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Offset>>,
    {
        for _ in 0..MAX_FLUSH_ATTEMPTS {
            // Contend for the flusher role. Whoever wins flushes for everyone
            // queued behind them.
            let guard = self.flush_lock.lock().await;
            if *receiver.borrow_and_update() >= target {
                // Someone else's flush already covered us — the group-commit
                // fast path, and by far the common case under load.
                drop(guard);
                return Ok(());
            }

            // Sampled inside the lock so it reflects the appends this one flush
            // is about to satisfy.
            let fan_in = self.waiting.load(Ordering::Relaxed);
            let outcome = flush().await;
            drop(guard);

            match outcome {
                Ok(durable_upto) => {
                    self.note_durable(durable_upto);
                    metrics::histogram!(metrics_names::SYNC_BATCH_APPENDS)
                        .record(fan_in.max(1) as f64);
                    if durable_upto >= target {
                        return Ok(());
                    }
                }
                Err(err) => {
                    metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                    return Err(err);
                }
            }
        }

        Err(StorageError::SyncFailed(format!(
            "flush did not reach offset {target} after {MAX_FLUSH_ATTEMPTS} attempts"
        )))
    }
}

/// Background task that flushes on a timer for `FsyncMode::Periodic`.
///
/// Owns its own shutdown signal so a dropped log does not leave a task running
/// against a closed file, and so graceful shutdown can flush one last time
/// before the process exits.
#[derive(Debug)]
pub struct PeriodicSyncer {
    shutdown: Arc<Notify>,
    handle: tokio::task::JoinHandle<()>,
}

impl PeriodicSyncer {
    /// Run `flush` every `interval` until shutdown.
    ///
    /// Fails rather than panics when there is no Tokio runtime: `Periodic` mode
    /// needs a timer, and a log opened outside a runtime would otherwise die at
    /// an unrelated call site with a message about reactors.
    ///
    /// Flush failures are logged and retried on the next tick rather than
    /// killing the task: a transient I/O error must not silently disable
    /// durability for the rest of the process's life.
    pub fn spawn<F, Fut>(interval: Duration, flush: F) -> Result<Self>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Offset>> + Send,
    {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(StorageError::InvalidConfig(
                "FsyncMode::Periodic needs a Tokio runtime; open the log from async code or choose FsyncMode::None or OnCommit",
            ));
        }
        let shutdown = Arc::new(Notify::new());
        let signal = Arc::clone(&shutdown);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skipped ticks must not queue up into a burst of flushes after a
            // stall; one late flush is as good as five.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately and has nothing to flush.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = flush().await {
                            metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                            tracing::error!(error = %err, "periodic log flush failed");
                        }
                    }
                    _ = signal.notified() => {
                        // Final flush so a graceful stop loses nothing that a
                        // clean shutdown could have kept.
                        if let Err(err) = flush().await {
                            metrics::counter!(metrics_names::SYNC_FAILURES_TOTAL).increment(1);
                            tracing::error!(error = %err, "final log flush failed");
                        }
                        return;
                    }
                }
            }
        });
        Ok(Self { shutdown, handle })
    }

    /// Stop the task, waiting for its final flush to complete.
    pub async fn shutdown(self) {
        self.shutdown.notify_waiters();
        // The task may not be parked on `notified()` yet; `notify_waiters` does
        // not latch, so nudge it until it observes the signal.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn counting_flush(
        counter: Arc<AtomicUsize>,
        reach: Arc<AtomicU64>,
    ) -> impl Fn() -> std::pin::Pin<Box<dyn Future<Output = Result<Offset>> + Send>> {
        move || {
            let counter = Arc::clone(&counter);
            let reach = Arc::clone(&reach);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(reach.load(Ordering::SeqCst))
            })
        }
    }

    #[tokio::test]
    async fn an_already_durable_target_does_not_flush() {
        let durability = Durability::new(FsyncMode::OnCommit, 10);
        let flushes = Arc::new(AtomicUsize::new(0));
        durability
            .ensure_durable(
                10,
                counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(10))),
            )
            .await
            .expect("durable");
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_flush_advances_the_durable_bound() {
        let durability = Durability::new(FsyncMode::OnCommit, 0);
        let flushes = Arc::new(AtomicUsize::new(0));
        durability
            .ensure_durable(
                5,
                counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(5))),
            )
            .await
            .expect("durable");
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert_eq!(durability.durable_upto(), 5);
    }

    #[tokio::test]
    async fn concurrent_appends_share_one_flush() {
        let durability = Arc::new(Durability::new(FsyncMode::OnCommit, 0));
        let flushes = Arc::new(AtomicUsize::new(0));
        // Every flush makes all 32 offsets durable, as a real fsync of the whole
        // file would.
        let reach = Arc::new(AtomicU64::new(32));

        let mut tasks = Vec::new();
        for target in 1..=32u64 {
            let durability = Arc::clone(&durability);
            let flush = counting_flush(Arc::clone(&flushes), Arc::clone(&reach));
            tasks.push(tokio::spawn(async move {
                durability.ensure_durable(target, flush).await
            }));
        }
        for task in tasks {
            task.await.expect("join").expect("durable");
        }

        assert_eq!(durability.durable_upto(), 32);
        // The whole point: far fewer flushes than appends.
        let count = flushes.load(Ordering::SeqCst);
        assert!(count < 32, "expected group commit, got {count} flushes");
    }

    #[tokio::test]
    async fn a_flush_failure_propagates_to_the_caller() {
        let durability = Durability::new(FsyncMode::OnCommit, 0);
        let result = durability
            .ensure_durable(1, || async {
                Err(StorageError::SyncFailed("device gone".into()))
            })
            .await;
        assert!(matches!(result, Err(StorageError::SyncFailed(_))));
        // A failed flush must not advance the durable bound.
        assert_eq!(durability.durable_upto(), 0);
    }

    #[tokio::test]
    async fn a_flush_that_never_advances_gives_up_instead_of_spinning() {
        let durability = Durability::new(FsyncMode::OnCommit, 0);
        let flushes = Arc::new(AtomicUsize::new(0));
        let result = durability
            .ensure_durable(
                99,
                counting_flush(Arc::clone(&flushes), Arc::new(AtomicU64::new(1))),
            )
            .await;
        assert!(matches!(result, Err(StorageError::SyncFailed(_))));
        assert_eq!(flushes.load(Ordering::SeqCst), MAX_FLUSH_ATTEMPTS);
    }

    #[test]
    fn spawning_a_periodic_syncer_without_a_runtime_is_an_error() {
        let err = PeriodicSyncer::spawn(Duration::from_millis(1), || async { Ok(0) })
            .expect_err("no runtime");
        assert!(matches!(err, StorageError::InvalidConfig(_)));
        assert!(err.to_string().contains("Tokio runtime"), "{err}");
    }

    #[test]
    fn only_on_commit_defers_acknowledgement() {
        assert!(!Durability::new(FsyncMode::OnCommit, 0).acknowledges_before_sync());
        assert!(Durability::new(FsyncMode::None, 0).acknowledges_before_sync());
        assert!(
            Durability::new(
                FsyncMode::Periodic {
                    interval: Duration::from_millis(1)
                },
                0
            )
            .acknowledges_before_sync()
        );
    }

    #[test]
    fn the_durable_bound_never_moves_backwards() {
        let durability = Durability::new(FsyncMode::None, 5);
        durability.note_durable(9);
        assert_eq!(durability.durable_upto(), 9);
        durability.note_durable(2);
        assert_eq!(durability.durable_upto(), 9);
    }

    #[tokio::test(start_paused = true)]
    async fn the_periodic_syncer_flushes_on_its_interval() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&flushes);
        let syncer = PeriodicSyncer::spawn(Duration::from_millis(50), move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(0)
            }
        })
        .expect("spawn");

        tokio::time::sleep(Duration::from_millis(175)).await;
        let ticks = flushes.load(Ordering::SeqCst);
        assert!((3..=4).contains(&ticks), "got {ticks} flushes");

        syncer.shutdown().await;
        // Shutdown flushes once more so nothing pending is lost.
        assert!(flushes.load(Ordering::SeqCst) > ticks);
    }

    #[tokio::test]
    async fn the_periodic_syncer_survives_a_failing_flush() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let syncer = PeriodicSyncer::spawn(Duration::from_millis(5), move || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(StorageError::SyncFailed("transient".into()))
            }
        })
        .expect("spawn");

        tokio::time::sleep(Duration::from_millis(40)).await;
        syncer.shutdown().await;
        // It kept ticking rather than dying on the first error.
        assert!(attempts.load(Ordering::SeqCst) > 2);
    }
}
