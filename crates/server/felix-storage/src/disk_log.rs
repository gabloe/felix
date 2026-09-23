//! A crash-safe, disk-backed `AppendOnlyLog`.
//!
//! Module map:
//!
//! * `layout`    — `ShardKey` to directory name, safely.
//! * `segments`  — the segment set: rollover, offset routing, truncation.
//! * `recovery`  — startup discovery, validation and torn-tail repair.
//! * `sync`      — fsync policy and group commit.
//! * `retention` — deleting the oldest segments once a bound is exceeded.
//! * `epochs`    — where each leadership generation began.
//! * `append`    — the append path, and the rollover it may have to start.
//! * `flush`     — making the active segment durable.
//! * `provider`  — one log per shard under a common root.
//!
//! This file is the seam between them and the async `AppendOnlyLog` trait.
//!
//! ## Where the blocking happens
//!
//! Two kinds of blocking I/O live behind this API, and they are treated
//! differently on purpose:
//!
//! * A `write` into the page cache is sub-microsecond in the normal case, so the
//!   append path performs it inline. Handing it to `spawn_blocking` would add
//!   more scheduling latency than the syscall itself costs, and this project
//!   cares about p999.
//! * A *rollover* is the exception on that path: sealing a segment and creating
//!   its successor fsync two files and a directory. Appends that trigger one
//!   roll on a blocking thread first, so the flush cost never lands on a
//!   reactor worker.
//! * An `fsync` genuinely blocks, for milliseconds on real hardware. It always
//!   runs on `spawn_blocking` so it cannot stall a reactor thread — and because
//!   flushes are grouped, one blocking task serves many appends.
//! * `read_range` may touch cold blocks, so it runs entirely on `spawn_blocking`.
//!   It is a replay and catch-up path, not the publish hot path.

pub mod epochs;
pub mod layout;
pub mod recovery;
pub mod retention;
pub mod segments;
pub mod sync;

mod append;
mod flush;
mod provider;

pub use provider::DiskLogProvider;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::{Mutex, RwLock};

use crate::log::{
    AppendOnlyLog, AppendRecord, AppendResult, BoxFuture, Epoch, FsyncMode, LogConfig, LogRecord,
    Offset, ReadRange, SealedSegment, SegmentDescriptor,
};
use crate::segment::ReadBudget;
use crate::{Result, StorageError, metrics_names};

use append::RollState;
use segments::SegmentSet;
use sync::{Durability, PeriodicSyncer};

/// A durable, segmented, append-only log for one shard.
///
/// Cheap to clone: every clone shares the same files and the same in-memory
/// state. Cloning is how the provider hands the same shard to several callers
/// without two writers racing over one directory.
#[derive(Clone, Debug)]
pub struct DiskLog {
    inner: Arc<LogInner>,
}

impl DiskLog {
    /// Open (and recover) the log rooted at `dir`.
    ///
    /// `label` is the human-readable shard name used in errors and logs.
    ///
    /// Must be called from inside a Tokio runtime when the configured policy is
    /// [`FsyncMode::Periodic`], which needs a background timer; the other
    /// policies have no such requirement.
    pub fn open(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
    ) -> Result<Self> {
        Self::open_inner(dir.into(), label.into(), config, None)
    }

    /// Open a shard log, creating it to begin at `base_offset` if it does not
    /// exist yet.
    ///
    /// For a replica receiving history that starts partway through a stream:
    /// the records before `base_offset` are gone from every copy in the
    /// cluster, so a log that begins there is complete rather than truncated,
    /// and a read below it is `Trimmed` exactly as it would be on the leader.
    ///
    /// **An existing log is opened as it stands and `base_offset` is ignored.**
    /// A restart must not reinterpret a shard that is already here, and the
    /// base it was created at is recorded in its own first segment. Only an
    /// empty directory is a shard being placed for the first time.
    pub fn open_at(
        dir: impl Into<PathBuf>,
        label: impl Into<String>,
        config: LogConfig,
        base_offset: Offset,
    ) -> Result<Self> {
        Self::open_inner(dir.into(), label.into(), config, Some(base_offset))
    }

    pub fn label(&self) -> &str {
        &self.inner.label
    }

    pub fn config(&self) -> &LogConfig {
        &self.inner.config
    }

    /// Oldest offset still readable.
    pub fn base_offset(&self) -> Offset {
        self.inner.segments.read().base_offset()
    }

    /// Exclusive bound on durable offsets: everything below it survives a crash.
    pub fn durable_offset(&self) -> Offset {
        self.inner.durability.durable_upto()
    }

    /// How many flushes this log has issued.
    ///
    /// Group commit means one flush serves many waiting appends, so N appends
    /// that coalesce produce far fewer than N flushes. That ratio is the
    /// property, and counting is the only way to see it that does not also
    /// measure the machine: a wall-clock speedup cannot tell "the flushes
    /// coalesced" from "this box could not put enough appends in flight for
    /// them to".
    pub fn flushes(&self) -> u64 {
        self.inner.durability.flushes()
    }

    /// Bytes written but not yet flushed — the data a crash would lose now.
    pub fn unsynced_bytes(&self) -> u64 {
        self.inner.segments.read().active().unsynced_bytes()
    }

    /// Device flushes this log has performed, for tests asserting that group
    /// commit shared them.
    #[cfg(test)]
    pub(crate) fn flushes_performed(&self) -> u64 {
        self.inner
            .flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Every segment on disk, oldest first.
    pub fn segments(&self) -> Vec<SegmentDescriptor> {
        self.inner.segments.read().descriptors()
    }

    /// Assign offsets and write `records`, without waiting for durability.
    ///
    /// Split out of [`AppendOnlyLog::append`] so a caller can learn the offsets
    /// the moment they are consumed, rather than only once the batch is
    /// durable. Anything that has to stay consistent with the log's offset
    /// order — the broker's commit sequencer, for one — has to claim its place
    /// at *assignment* time: after this returns, the records exist on disk and
    /// hold their offsets whether or not the durability wait that follows
    /// succeeds, fails, or is cancelled.
    ///
    /// The returned [`PendingAppend`] must be passed to [`DiskLog::commit`] for
    /// the configured fsync policy to be honoured. Dropping it does not undo
    /// the write.
    pub async fn append_pending(&self, records: &[AppendRecord]) -> Result<PendingAppend> {
        let records = records.to_vec();
        let inner = Arc::clone(&self.inner);
        Self::write_batch(inner, records).await
    }

    /// Wait until a [`PendingAppend`] satisfies the configured fsync policy.
    pub async fn commit(&self, pending: &PendingAppend) -> Result<()> {
        if self.inner.durability.acknowledges_before_sync() {
            return Ok(());
        }
        self.inner.ensure_durable(pending.durable_target).await?;
        // `ensure_durable` returns immediately when the target is already
        // covered, which skips the check inside `flush`. A rollover that failed
        // since then still has to be reported rather than acknowledged.
        self.inner.check_roll_state()
    }

    /// Force a flush regardless of the configured policy.
    pub async fn sync(&self) -> Result<()> {
        self.inner
            .durability
            .force_flush(|| Arc::clone(&self.inner).flush())
            .await?;
        self.inner.check_roll_state()
    }

    /// Run one retention pass now, instead of waiting for the timer.
    ///
    /// Exists so retention is testable deterministically and so an operator can
    /// reclaim space without waiting out `retention_check_interval`. Returns
    /// what the pass reclaimed; a no-op when no bound is configured.
    pub async fn enforce_retention_now(&self) -> Result<segments::RetentionOutcome> {
        Arc::clone(&self.inner).sweep_retention().await
    }

    /// Discard every record and start again, empty, at `base_offset`.
    ///
    /// The one caller is a follower rebuilding a shard whose copy has
    /// diverged -- see `docs/replication-design.md`. The log stays open and
    /// every handle to it stays valid; a read in flight fails rather than
    /// returning records that no longer exist. The generation history goes
    /// with the records it described.
    pub async fn reset_to(&self, base_offset: Offset) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let _flush_guard = inner.durability.lock_flushes().await;
        let operation = Arc::clone(&inner);
        tokio::task::spawn_blocking(move || {
            let mut segments = operation.segments.write();
            segments.reset_to(base_offset)?;
            segments.active_mut().sync()?;
            operation.durability.reset_after_truncate(base_offset);
            let mut epochs = operation.epochs.lock();
            *epochs = epochs::EpochMap::default();
            epochs::store(&operation.dir, &epochs)?;
            Ok(())
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Stop background work and flush everything one last time.
    ///
    /// Call before dropping the process's last handle: without it, `Periodic`
    /// mode can lose up to one interval of writes that a clean stop could have
    /// kept.
    pub async fn shutdown(&self) -> Result<()> {
        // Retention first: it must not start deleting while the rest of
        // shutdown is flushing, and it has nothing to finish on the way out.
        let retention = self.inner.retention.lock().take();
        if let Some(retention) = retention {
            retention.shutdown().await;
        }
        let syncer = self.inner.syncer.lock().take();
        if let Some(syncer) = syncer {
            syncer.shutdown().await;
        }
        // A rollover in flight owns the retired segment and is the only thing
        // that will ever flush it. Dropping the runtime here would abandon it
        // half-installed, so wait it out first — it is bounded by two fsyncs.
        let roll = self.inner.roll_task.lock().take();
        if let Some(roll) = roll {
            // A panicked rollover has already recorded itself as `Failed`; the
            // `sync` below is what reports the problem.
            let _ = roll.await;
        }
        self.inner.check_roll_state()?;
        self.sync().await
    }

    fn open_inner(
        dir: PathBuf,
        label: String,
        config: LogConfig,
        base_offset: Option<Offset>,
    ) -> Result<Self> {
        config.validate()?;

        if let Some(base_offset) = base_offset.filter(|base| *base > 0) {
            recovery::place_empty_shard(&dir, &config, base_offset)?;
        }
        let recovered = recovery::recover_shard(&dir, &label, &config)?;
        if recovered.truncated_bytes > 0 {
            tracing::warn!(
                shard = %label,
                truncated_bytes = recovered.truncated_bytes,
                "recovered log after an unclean shutdown"
            );
        }
        // Read before the directory is handed to the segment set.
        let epochs = epochs::load(&dir);
        let epochs_dir = dir.clone();
        let segments = SegmentSet::new(
            dir,
            label.clone(),
            config.clone(),
            recovered.sealed,
            recovered.active,
        )?;

        // Everything that survived recovery is on disk, so the durable bound
        // starts at the recovered tail.
        let durable_upto = segments.tail_offset();
        let inner = Arc::new(LogInner {
            label,
            config: config.clone(),
            segments: RwLock::new(segments),
            roll_gate: tokio::sync::RwLock::new(()),
            durability: Durability::new(config.fsync_mode, durable_upto),
            syncer: Mutex::new(None),
            retention: Mutex::new(None),
            epochs: Mutex::new(epochs),
            dir: epochs_dir,
            roll_state: AtomicU8::new(RollState::Idle as u8),
            roll_failure: Mutex::new(None),
            #[cfg(test)]
            fail_seal: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            slow_inline_roll_millis: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            flushes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            inline_roll_active: std::sync::atomic::AtomicBool::new(false),
            roll_task: Mutex::new(None),
            pending_seal: Mutex::new(None),
        });

        if let FsyncMode::Periodic { interval } = config.fsync_mode {
            let weak = Arc::downgrade(&inner);
            let syncer = PeriodicSyncer::spawn(interval, move || {
                let weak = weak.clone();
                async move {
                    match weak.upgrade() {
                        // The log is gone; report the highest offset so the
                        // task simply stops doing work.
                        None => Ok(Offset::MAX),
                        Some(inner) => inner.durability.force_flush(|| inner.clone().flush()).await,
                    }
                }
            })?;
            *inner.syncer.lock() = Some(syncer);
        }

        if config.retention_bytes.is_some() || config.retention_age.is_some() {
            let weak = Arc::downgrade(&inner);
            let task =
                retention::RetentionTask::spawn(config.retention_check_interval, move || {
                    let weak = weak.clone();
                    async move {
                        match weak.upgrade() {
                            None => Ok(segments::RetentionOutcome::default()),
                            Some(inner) => inner.sweep_retention().await,
                        }
                    }
                })?;
            *inner.retention.lock() = Some(task);
        }

        Ok(Self { inner })
    }
}

impl AppendOnlyLog for DiskLog {
    fn append(&self, records: &[AppendRecord]) -> BoxFuture<'_, Result<AppendResult>> {
        // `records` is borrowed for the duration of the future, so the write
        // happens first and only offsets cross the await.
        let records = records.to_vec();
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let started = std::time::Instant::now();
            let pending = Self::write_batch(Arc::clone(&inner), records).await?;

            // `OnCommit` is the only policy that makes the caller wait. The
            // others acknowledge once the bytes are in the page cache and rely
            // on the periodic flush (or the operating system) from there.
            if !inner.durability.acknowledges_before_sync() {
                inner.ensure_durable(pending.durable_target).await?;
                // Checked again after the wait: `check_roll_state` at the top of
                // `write_batch` only covers rollovers that had already failed
                // when this append started, and this one may have been in flight
                // across the failure.
                inner.check_roll_state()?;
            }

            metrics::histogram!(metrics_names::APPEND_DURATION_SECONDS)
                .record(started.elapsed().as_secs_f64());
            Ok(pending.result)
        })
    }

    fn read_range(&self, range: ReadRange) -> BoxFuture<'_, Result<Vec<LogRecord>>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let started = std::time::Instant::now();
            let records = tokio::task::spawn_blocking(move || {
                let segments = inner.segments.read();
                let oldest = segments.base_offset();
                if range.start < oldest {
                    // Not an empty range: these offsets existed and are gone.
                    return Err(StorageError::Trimmed {
                        requested: range.start,
                        oldest,
                    });
                }
                segments.read(
                    range.start,
                    ReadBudget::new(range.max_bytes, inner.config.max_records_per_read),
                )
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;

            metrics::counter!(metrics_names::READ_RECORDS_TOTAL).increment(records.len() as u64);
            metrics::counter!(metrics_names::READ_BYTES_TOTAL)
                .increment(records.iter().map(|r| r.payload.len() as u64).sum::<u64>());
            metrics::histogram!(metrics_names::READ_DURATION_SECONDS)
                .record(started.elapsed().as_secs_f64());
            Ok(records)
        })
    }

    fn tail_offset(&self) -> BoxFuture<'_, Result<Offset>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { Ok(inner.segments.read().tail_offset()) })
    }

    fn record_generation(&self, generation: u64, start_offset: Offset) -> Result<bool> {
        let mut epochs = self.inner.epochs.lock();
        if !epochs.record(generation, start_offset) {
            return Ok(false);
        }
        epochs::store(&self.inner.dir, &epochs)?;
        Ok(true)
    }

    fn generations(&self) -> Vec<Epoch> {
        self.inner.epochs.lock().entries().to_vec()
    }

    fn generation_end(&self, generation: u64, tail: Offset) -> Option<Offset> {
        self.inner.epochs.lock().end_of(generation, tail)
    }

    fn truncate(&self, offset: Offset) -> BoxFuture<'_, Result<()>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            let _flush_guard = inner.durability.lock_flushes().await;
            let operation = Arc::clone(&inner);
            tokio::task::spawn_blocking(move || {
                let mut segments = operation.segments.write();
                segments.truncate(offset)?;
                segments.active_mut().sync()?;
                let tail = segments.tail_offset();
                operation.durability.reset_after_truncate(tail);
                // The history cannot outlive the records it describes, or it
                // would answer with a start offset the log no longer holds.
                let mut epochs = operation.epochs.lock();
                epochs.truncate_from(offset);
                epochs::store(&operation.dir, &epochs)?;
                Ok(())
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
        })
    }

    fn seal(&self) -> BoxFuture<'_, Result<SealedSegment>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut segments = inner.segments.write();
                let (descriptor, checksum) = segments.seal_active()?;
                // Start a fresh segment so the sealed one is immutable from here
                // on, which is what makes its checksum meaningful.
                if segments.active().record_count() > 0 {
                    segments.roll()?;
                }
                Ok(SealedSegment {
                    descriptor,
                    checksum,
                })
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
        })
    }
}

/// A batch that has been written and given offsets, but not yet flushed.
#[derive(Debug, Clone)]
pub struct PendingAppend {
    pub result: AppendResult,
    /// Exclusive offset bound this batch needs durable.
    durable_target: Offset,
}

impl PendingAppend {
    pub fn first_offset(&self) -> Offset {
        self.result.first_offset
    }

    pub fn last_offset(&self) -> Offset {
        self.result.last_offset
    }
}

/// Shared state behind every clone of a [`DiskLog`].
struct LogInner {
    label: String,
    config: LogConfig,
    /// Guards the segment set. A read lock serves range reads concurrently; a
    /// write lock serialises appends, which must assign offsets in order
    /// anyway.
    segments: RwLock<SegmentSet>,
    /// Held shared by appends, exclusively by an inline rollover.
    ///
    /// `segments` is synchronous, so a publisher queued on it parks a Tokio
    /// worker instead of yielding it — for the two device flushes a rollover
    /// holds it across, that stalls the whole runtime. Waiting on this gate
    /// instead yields. Layered above `segments` rather than replacing it so
    /// the uncontended append stays a `try_read`.
    roll_gate: tokio::sync::RwLock<()>,
    durability: Durability,
    /// `None` unless the fsync policy is `Periodic`. Taken on shutdown.
    syncer: Mutex<Option<PeriodicSyncer>>,
    retention: Mutex<Option<retention::RetentionTask>>,
    /// Where each leadership generation began, for repairing a divergence.
    ///
    /// Its own lock rather than living under `segments`: it is read and written
    /// on the replication path, not the append path, and the append path is
    /// where lock contention costs something.
    epochs: Mutex<epochs::EpochMap>,
    /// Where `epochs` is persisted, kept because the log needs it on truncation
    /// and nothing else hands it a directory.
    dir: PathBuf,
    /// Where the background rollover is in its lifecycle. At most one runs at
    /// a time, and a failure is terminal for the log.
    roll_state: AtomicU8,
    /// The in-flight rollover, so shutdown can wait for it to finish rather
    /// than dropping the runtime out from under a half-installed segment.
    roll_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// A retired segment that has been swapped out but not yet flushed.
    ///
    /// `flush` reports durability for the whole log, but it only ever syncs the
    /// *active* segment. With an inline rollover that was sound, because
    /// sealing happened before the replacement existed. A background rollover
    /// breaks it: between the swap and the seal there are records in the
    /// retired segment that no sync of the active segment covers, and reporting
    /// them durable would be a lie under `FsyncMode::OnCommit`. So `flush`
    /// syncs this handle too for as long as it is set.
    ///
    /// Cleared only after a *successful* seal. A failed one leaves it in place
    /// so every later flush keeps trying to cover those records rather than
    /// quietly reporting them durable.
    pending_seal: Mutex<Option<Arc<std::fs::File>>>,
    /// Why the log stopped accepting work, once a rollover has failed.
    ///
    /// Separate from `roll_state` because the two are read for different
    /// reasons and, critically, are written in a specific order: this is set
    /// *before* anything observable is relaxed, so there is no window in which
    /// a durability wait can see a cleared `pending_seal` and a state that is
    /// not yet `Failed`. `roll_state` remains the scheduler's view; this is the
    /// durability view.
    roll_failure: Mutex<Option<String>>,
    /// Forces the next seal to fail, so the failure path can be tested.
    #[cfg(test)]
    fail_seal: std::sync::atomic::AtomicBool,
    /// Milliseconds an inline rollover holds the segment lock, for tests.
    #[cfg(test)]
    slow_inline_roll_millis: std::sync::atomic::AtomicU64,
    /// Set while a stretched inline rollover holds the segment lock.
    #[cfg(test)]
    inline_roll_active: std::sync::atomic::AtomicBool,
    /// Device flushes performed, so tests can assert group-commit fan-in —
    /// per instance, where the global `SYNC_TOTAL` counter cannot isolate one
    /// log from the rest of a parallel test run.
    #[cfg(test)]
    flushes: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for LogInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogInner")
            .field("label", &self.label)
            .field("fsync_mode", &self.config.fsync_mode)
            .finish_non_exhaustive()
    }
}

/// Micros since the Unix epoch, matching `AppendRecord::timestamp_micros`.
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
