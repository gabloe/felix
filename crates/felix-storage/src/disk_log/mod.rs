// A crash-safe, disk-backed `AppendOnlyLog`.
//
// Module map:
//
// * `layout`    — `ShardKey` to directory name, safely.
// * `segments`  — the segment set: rollover, offset routing, truncation.
// * `recovery`  — startup discovery, validation and torn-tail repair.
// * `sync`      — fsync policy and group commit.
//
// This file is the seam between them and the async `AppendOnlyLog` trait.
//
// ## Where the blocking happens
//
// Two kinds of blocking I/O live behind this API, and they are treated
// differently on purpose:
//
// * A `write` into the page cache is sub-microsecond in the normal case, so the
//   append path performs it inline. Handing it to `spawn_blocking` would add
//   more scheduling latency than the syscall itself costs, and this project
//   cares about p999.
// * A *rollover* is the exception on that path: sealing a segment and creating
//   its successor fsync two files and a directory. Appends that trigger one
//   roll on a blocking thread first, so the flush cost never lands on a
//   reactor worker.
// * An `fsync` genuinely blocks, for milliseconds on real hardware. It always
//   runs on `spawn_blocking` so it cannot stall a reactor thread — and because
//   flushes are grouped, one blocking task serves many appends.
// * `read_range` may touch cold blocks, so it runs entirely on `spawn_blocking`.
//   It is a replay and catch-up path, not the publish hot path.

pub mod layout;
pub mod recovery;
pub mod segments;
pub mod sync;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::log::{
    AppendOnlyLog, AppendRecord, AppendResult, BoxFuture, FsyncMode, LogConfig, LogProvider,
    LogRecord, Offset, ReadRange, SealedSegment, SegmentDescriptor, ShardKey,
};
use crate::segment::{ReadBudget, io::sync_data};
use crate::{Result, StorageError, metrics_names};

use segments::SegmentSet;
use sync::{Durability, PeriodicSyncer};

/// Shared state behind every clone of a [`DiskLog`].
struct LogInner {
    label: String,
    config: LogConfig,
    /// Guards the segment set. A read lock serves range reads concurrently; a
    /// write lock serialises appends, which must assign offsets in order
    /// anyway.
    segments: RwLock<SegmentSet>,
    durability: Durability,
    /// `None` unless the fsync policy is `Periodic`. Taken on shutdown.
    syncer: Mutex<Option<PeriodicSyncer>>,
}

impl std::fmt::Debug for LogInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogInner")
            .field("label", &self.label)
            .field("fsync_mode", &self.config.fsync_mode)
            .finish_non_exhaustive()
    }
}

impl LogInner {
    /// Flush the active segment and report the exclusive offset bound now
    /// durable.
    ///
    /// The file handle and the offset it covers are captured under the lock,
    /// then the lock is released before the flush: an `fsync` must never be held
    /// across the lock that appends need.
    async fn flush(self: Arc<Self>) -> Result<Offset> {
        let (handle, segment_id, synced_bytes, durable_upto) = {
            let segments = self.segments.read();
            let active = segments.active();
            (
                active.sync_handle(),
                active.id(),
                active.size_bytes(),
                segments.tail_offset(),
            )
        };

        let started = std::time::Instant::now();
        let outcome = tokio::task::spawn_blocking(move || sync_data(&handle))
            .await
            .map_err(|err| StorageError::SyncFailed(format!("flush task failed: {err}")))?;
        outcome.map_err(|err| StorageError::SyncFailed(err.to_string()))?;

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        {
            let mut segments = self.segments.write();
            // A rollover may have swapped the active segment while the flush was
            // in flight. The old segment was sealed (and therefore synced) as
            // part of that roll, so there is nothing to record against it.
            if segments.active().id() == segment_id {
                segments.active_mut().mark_synced(synced_bytes);
            }
            metrics::gauge!(metrics_names::UNSYNCED_BYTES)
                .set(segments.active().unsynced_bytes() as f64);
        }

        Ok(durable_upto)
    }

    /// Wait until every offset below `target` is durable, flushing if needed.
    async fn ensure_durable(self: &Arc<Self>, target: Offset) -> Result<()> {
        self.durability
            .ensure_durable(target, || Arc::clone(self).flush())
            .await
    }
}

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
        config.validate()?;
        let dir = dir.into();
        let label = label.into();

        let recovered = recovery::recover_shard(&dir, &label, &config)?;
        if recovered.truncated_bytes > 0 {
            tracing::warn!(
                shard = %label,
                truncated_bytes = recovered.truncated_bytes,
                "recovered log after an unclean shutdown"
            );
        }
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
            durability: Durability::new(config.fsync_mode, durable_upto),
            syncer: Mutex::new(None),
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

        Ok(Self { inner })
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

    /// Bytes written but not yet flushed — the data a crash would lose now.
    pub fn unsynced_bytes(&self) -> u64 {
        self.inner.segments.read().active().unsynced_bytes()
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
        self.inner.ensure_durable(pending.durable_target).await
    }

    /// Force a flush regardless of the configured policy.
    pub async fn sync(&self) -> Result<()> {
        self.inner
            .durability
            .force_flush(|| Arc::clone(&self.inner).flush())
            .await?;
        Ok(())
    }

    /// Stop background work and flush everything one last time.
    ///
    /// Call before dropping the process's last handle: without it, `Periodic`
    /// mode can lose up to one interval of writes that a clean stop could have
    /// kept.
    pub async fn shutdown(&self) -> Result<()> {
        let syncer = self.inner.syncer.lock().take();
        if let Some(syncer) = syncer {
            syncer.shutdown().await;
        }
        self.sync().await
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

impl DiskLog {
    /// Roll if needed, then assign offsets and write the batch.
    async fn write_batch(
        inner: Arc<LogInner>,
        records: Vec<AppendRecord>,
    ) -> Result<PendingAppend> {
        if records.is_empty() {
            return Err(StorageError::InvalidRange);
        }

        // Rolling a segment seals one file, creates another, and fsyncs both
        // plus the directory entry. That is real blocking work, so it happens
        // on a blocking thread rather than inline on a reactor worker.
        //
        // Checked under a read lock and re-checked under the write lock:
        // another publisher may have rolled in between, and rolling twice would
        // leave an empty segment behind.
        if inner.segments.read().would_roll(&records) {
            let roller = Arc::clone(&inner);
            let batch = records.clone();
            tokio::task::spawn_blocking(move || {
                let mut segments = roller.segments.write();
                if segments.would_roll(&batch) {
                    segments.roll()?;
                }
                Ok::<(), StorageError>(())
            })
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;
        }

        let mut segments = inner.segments.write();
        let (first_offset, last_offset) = segments.append(&records)?;
        let durable_target = segments.tail_offset();
        Ok(PendingAppend {
            result: AppendResult {
                first_offset,
                last_offset,
            },
            durable_target,
        })
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

/// Opens one [`DiskLog`] per shard under a common root directory.
///
/// Repeated opens of the same shard return the same log. Two independent
/// writers over one directory would interleave offsets and corrupt the segment,
/// so the cache is a correctness requirement, not an optimisation.
#[derive(Debug)]
pub struct DiskLogProvider {
    root: PathBuf,
    config: LogConfig,
    open_logs: Mutex<HashMap<ShardKey, DiskLog>>,
}

impl DiskLogProvider {
    pub fn new(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        config.validate()?;
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            config,
            open_logs: Mutex::new(HashMap::new()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Open or return the cached log for `shard`.
    pub fn open_shard(&self, shard: &ShardKey) -> Result<DiskLog> {
        // Recovery runs under the lock: two callers racing to open the same new
        // shard must not both scan and both create segment zero.
        let mut open_logs = self.open_logs.lock();
        if let Some(log) = open_logs.get(shard) {
            return Ok(log.clone());
        }
        let log = DiskLog::open(
            layout::shard_dir(&self.root, shard),
            layout::shard_label(shard),
            self.config.clone(),
        )?;
        open_logs.insert(shard.clone(), log.clone());
        Ok(log)
    }

    /// Shard keys this provider currently has open.
    pub fn open_shards(&self) -> Vec<ShardKey> {
        self.open_logs.lock().keys().cloned().collect()
    }

    /// Flush and stop every open log. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let logs: Vec<DiskLog> = self.open_logs.lock().values().cloned().collect();
        let mut first_error = None;
        for log in logs {
            if let Err(err) = log.shutdown().await {
                tracing::error!(shard = %log.label(), error = %err, "failed to flush log on shutdown");
                first_error.get_or_insert(err);
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl LogProvider for DiskLogProvider {
    type Log = DiskLog;

    fn open(&self, shard: &ShardKey) -> BoxFuture<'_, Result<Self::Log>> {
        let shard = shard.clone();
        Box::pin(async move { self.open_shard(&shard) })
    }
}

#[cfg(test)]
mod tests;
