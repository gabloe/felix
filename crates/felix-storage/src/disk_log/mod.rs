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

use std::sync::atomic::{AtomicU8, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::log::{
    AppendOnlyLog, AppendRecord, AppendResult, BoxFuture, FsyncMode, LogConfig, LogProvider,
    LogRecord, Offset, ReadRange, SealedSegment, SegmentDescriptor, ShardKey,
};
use crate::segment::{ReadBudget, io::sync_data};
use crate::{Result, StorageError, metrics_names};

/// Bound on rollover retries in a single append.
///
/// Each retry means another publisher won the race to fill the segment; a
/// handful is generous, and failing loudly beats spinning under a pathological
/// interleaving.
const MAX_ROLL_ATTEMPTS: usize = 8;

use segments::{RollOutcome, SegmentSet};
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
}

/// Lifecycle of the background rollover.
///
/// Explicit rather than a boolean because the states are not symmetric: the
/// first two are recoverable and self-clearing, the last is terminal.
///
/// ```text
///   Idle ──► Preparing ──► Sealing ──► Idle
///              │              │
///              └──────────────┴──────► Failed  (terminal)
/// ```
///
/// * `Preparing` — building the replacement segment. Nothing is installed yet;
///   a crash here leaves an uninstalled segment that recovery discards.
/// * `Sealing` — the replacement is live and the retired segment is being
///   flushed. Reads already route across it, so a crash here loses nothing that
///   the fsync policy had promised.
/// * `Failed` — the retired segment could not be flushed. The log stops
///   accepting appends: a failed fsync means bytes that were reported durable
///   may not be, and continuing to append over that is worse than stopping.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum RollState {
    Idle = 0,
    Preparing = 1,
    Sealing = 2,
    Failed = 3,
}

impl RollState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Preparing,
            2 => Self::Sealing,
            3 => Self::Failed,
            _ => Self::Idle,
        }
    }
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
    /// Build the next segment and swap it in, with every fsync off the lock.
    ///
    /// Runs on a blocking thread. The segment lock is taken twice and held only
    /// for pointer work: once to install the replacement, once to record the
    /// retired segment as sealed. The two expensive halves — creating the new
    /// file and flushing the old one — happen with no lock held, so an append
    /// never queues behind a rollover's flushes.
    async fn roll_in_background(self: Arc<Self>) -> Result<()> {
        let inner = Arc::clone(&self);
        tokio::task::spawn_blocking(move || {
            // The plan is copied out under a read lock; the segment is built
            // with no lock at all. Creating it fsyncs its header and its parent
            // directory, so by the time anything can be appended here it is
            // durable -- and holding a lock across those two flushes would stall
            // every append waiting on the write lock, which is the entire cost
            // this design exists to remove.
            let plan = { inner.segments.read().roll_plan() };
            let prepared = plan.build()?;

            let retired = {
                let mut segments = inner.segments.write();
                match segments.commit_roll(prepared)? {
                    RollOutcome::Installed(retired) => {
                        // Published before the lock is released, so no flush can
                        // observe the new active segment without also seeing the
                        // retired one it has to cover.
                        *inner.pending_seal.lock() = Some(retired.sync_handle());
                        retired
                    }
                    // The tail moved past the offset this segment was built
                    // for. Delete it here — leaving it for recovery to clean up
                    // works, but only because recovery knows the rule.
                    RollOutcome::Stale(prepared) => {
                        drop(segments);
                        metrics::counter!(metrics_names::SEGMENT_ROLL_DISCARDED_TOTAL).increment(1);
                        return prepared.discard();
                    }
                }
            };

            inner
                .roll_state
                .store(RollState::Sealing as u8, Ordering::Release);

            // Flushed with no lock held. The segment is already listed as
            // sealed and readable — this only makes it durable.
            let mut retired = retired;
            match inner.seal_retired(&mut retired) {
                Ok(()) => {
                    // Only now: these records are on the device, so a flush no
                    // longer has to cover them.
                    *inner.pending_seal.lock() = None;
                    Ok::<(), StorageError>(())
                }
                Err(err) => {
                    // Order matters. `pending_seal` stays set and the failure is
                    // recorded before this task returns, so an `OnCommit` append
                    // already in flight cannot find a cleared `pending_seal`
                    // alongside a roll state that has not been marked failed
                    // yet, sync only the new segment, and acknowledge records
                    // that are still sitting in an unflushed retired one.
                    inner.record_roll_failure(&err);
                    Err(err)
                }
            }
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    /// Whether a background rollover is between its start and its completion.
    fn roll_pending(&self) -> bool {
        matches!(
            RollState::from_u8(self.roll_state.load(Ordering::Acquire)),
            RollState::Preparing | RollState::Sealing
        )
    }

    /// Seal the retired segment, with a hook tests use to force a failure.
    fn seal_retired(&self, retired: &mut crate::segment::SegmentWriter) -> Result<()> {
        #[cfg(test)]
        if self.fail_seal.load(Ordering::Acquire) {
            return Err(StorageError::SyncFailed(
                "injected seal failure".to_string(),
            ));
        }
        retired.seal().map(|_| ())
    }

    /// Record a failed rollover as terminal, and say why.
    ///
    /// Idempotent and first-writer-wins: the first failure is the interesting
    /// one, and a later flush failing for the same underlying reason should not
    /// overwrite it.
    fn record_roll_failure(&self, err: &StorageError) {
        let mut failure = self.roll_failure.lock();
        if failure.is_none() {
            *failure = Some(err.to_string());
        }
        self.roll_state
            .store(RollState::Failed as u8, Ordering::Release);
    }

    /// The terminal rollover error, if one has been recorded.
    ///
    /// Checked on every path that either accepts new work or reports
    /// durability — not just at the entry to an append. A rollover can fail
    /// while an append is already in flight, and that append must not be
    /// acknowledged on the strength of a flush that did not cover it.
    fn check_roll_state(&self) -> Result<()> {
        if let Some(reason) = self.roll_failure.lock().as_deref() {
            return Err(StorageError::SyncFailed(format!(
                "{}: a background segment rollover failed ({reason}); the log is no longer \
                 accepting appends",
                self.label
            )));
        }
        // A `Failed` state with no recorded reason means the rollover task
        // itself panicked or was cancelled. Still terminal.
        if RollState::from_u8(self.roll_state.load(Ordering::Acquire)) == RollState::Failed {
            return Err(StorageError::SyncFailed(format!(
                "{}: a background segment rollover failed; the log is no longer accepting appends",
                self.label
            )));
        }
        Ok(())
    }

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
        // Taken after the active handle, so a rollover that lands in between is
        // seen here rather than missed: `commit_roll` sets it before releasing
        // the lock this read just took.
        let retired = self.pending_seal.lock().clone();
        let had_pending_seal = retired.is_some();

        let started = std::time::Instant::now();
        let outcome = tokio::task::spawn_blocking(move || {
            // The retired segment first. `durable_upto` covers records in both,
            // and it may not be reported until every one of them is on disk.
            if let Some(retired) = retired {
                sync_data(&retired)?;
            }
            sync_data(&handle)
        })
        .await
        .map_err(|err| StorageError::SyncFailed(format!("flush task failed: {err}")))?;
        if let Err(err) = outcome {
            let err = StorageError::SyncFailed(err.to_string());
            // A flush that could not cover the retired segment is the same
            // failure as a seal that could not, and is equally terminal.
            if had_pending_seal {
                self.record_roll_failure(&err);
            }
            return Err(err);
        }

        metrics::counter!(metrics_names::SYNC_TOTAL).increment(1);
        metrics::histogram!(metrics_names::SYNC_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        {
            let mut segments = self.segments.write();
            // A rollover may have swapped the active segment while the flush was
            // in flight. Its records were covered by the `pending_seal` sync
            // above, and the retired writer is owned by the rollover task, so
            // there is nothing to record against it here.
            if segments.active().id() == segment_id {
                segments.active_mut().mark_synced(synced_bytes);
            }
            metrics::gauge!(metrics_names::UNSYNCED_BYTES)
                .set(segments.active().unsynced_bytes() as f64);
        }

        // Rechecked after the flush, not only before it. A rollover can fail
        // while this flush is in flight, and `durable_upto` spans the retired
        // segment as well as the active one -- reporting it would acknowledge
        // records that the failed seal left unflushed.
        self.check_roll_state()?;

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
            roll_state: AtomicU8::new(RollState::Idle as u8),
            roll_failure: Mutex::new(None),
            #[cfg(test)]
            fail_seal: std::sync::atomic::AtomicBool::new(false),
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
        inner.check_roll_state()?;

        // The hard-limit fallback. A background roll normally replaces the
        // segment well before this, so reaching here means the log filled
        // faster than a replacement could be built — rare, and still correct.
        for attempt in 0..MAX_ROLL_ATTEMPTS {
            // While a background rollover is building the replacement, the
            // segment is allowed to grow past its configured size rather than
            // blocking here. That headroom is what gives the preparation time
            // to finish; without it the inline path below wins every race.
            let roll_pending = inner.roll_pending();
            if inner
                .segments
                .read()
                .would_roll_within(&records, roll_pending)
            {
                let roller = Arc::clone(&inner);
                let batch = records.clone();
                tokio::task::spawn_blocking(move || {
                    let mut segments = roller.segments.write();
                    // Re-checked under the write lock: another publisher may
                    // have rolled already, and rolling twice leaves an empty
                    // segment behind.
                    if segments.would_roll_within(&batch, roller.roll_pending()) {
                        segments.roll()?;
                    }
                    Ok::<(), StorageError>(())
                })
                .await
                .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;
            }

            let mut segments = inner.segments.write();
            if segments.would_roll_within(&records, inner.roll_pending()) {
                // Filled again in the gap. Drop the lock and roll off-thread.
                drop(segments);
                debug_assert!(attempt + 1 < MAX_ROLL_ATTEMPTS, "rollover retry starved");
                continue;
            }
            let (first_offset, last_offset) = segments.append(&records)?;
            let durable_target = segments.tail_offset();
            let prepare_roll = segments.should_prepare_roll();
            drop(segments);

            // Start the replacement while the current segment still has room,
            // so the flushes it costs never land on an append. `Idle ->
            // Preparing` is a compare-exchange, which is what keeps exactly one
            // rollover in flight and leaves `Failed` terminal.
            if prepare_roll
                && inner
                    .roll_state
                    .compare_exchange(
                        RollState::Idle as u8,
                        RollState::Preparing as u8,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                let roller = Arc::clone(&inner);
                metrics::counter!(metrics_names::SEGMENT_ROLL_BACKGROUND_TOTAL).increment(1);
                let handle = tokio::spawn(async move {
                    match Arc::clone(&roller).roll_in_background().await {
                        Ok(()) => roller
                            .roll_state
                            .store(RollState::Idle as u8, Ordering::Release),
                        Err(err) => {
                            // Terminal. A rollover fails on a failed fsync or a
                            // failed file creation, and both mean the log can no
                            // longer honour what it has already acknowledged.
                            // Retrying would just fail again, quietly, forever.
                            tracing::error!(
                                shard = %roller.label,
                                error = %err,
                                "background segment rollover failed; the log will reject further appends",
                            );
                            metrics::counter!(metrics_names::SEGMENT_ROLL_FAILED_TOTAL)
                                .increment(1);
                            // Usually already recorded, by the seal that failed.
                            // This covers a failure earlier in the rollover --
                            // building the replacement, or installing it.
                            roller.record_roll_failure(&err);
                        }
                    }
                });
                // Replaces a handle only when the previous roll has finished,
                // because the compare-exchange above admits one at a time.
                *inner.roll_task.lock() = Some(handle);
            }

            return Ok(PendingAppend {
                result: AppendResult {
                    first_offset,
                    last_offset,
                },
                durable_target,
            });
        }

        Err(StorageError::Unsupported(
            "append could not secure segment capacity; rollover kept losing the race",
        ))
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
