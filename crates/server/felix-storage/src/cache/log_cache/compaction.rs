//! Rewriting a cache shard's log down to its live set.
//!
//! A log that only ever grows makes "the cache is a log" a slow leak.
//! Compaction writes the live entries into a fresh log in a sibling directory
//! and swaps the two, so no record is ever edited in place.

use std::path::Path;

use bytes::Bytes;

use crate::disk_log::DiskLog;
use crate::io::sync_dir;
use crate::log::{AppendOnlyLog, AppendRecord, Offset};
use crate::{Result, StorageError};

use super::CacheOp;
use super::shard::{CacheShard, Entry, Index, ShardState, now_millis};

/// How much larger than its live bytes a log may grow before it is compacted.
///
/// A multiple rather than a fixed size, so the cost is proportional to the
/// garbage: a cache that is mostly live is never compacted however big it is,
/// and one that is mostly overwrites is compacted however small.
const COMPACT_WHEN_TIMES_LIVE: u64 = 4;

/// Below this there is nothing worth reclaiming, whatever the ratio says. Stops
/// a cache holding a handful of keys from compacting on every other write.
const COMPACT_FLOOR_BYTES: u64 = 1024 * 1024;

impl CacheShard {
    /// Compact when worthwhile — but only from an apply whose record is the
    /// newest in the log, with nothing staged behind it.
    ///
    /// The gate is load-bearing: compaction swaps the shard directory, and a
    /// record another writer has staged but not yet committed lives only in
    /// the old directory. Swapping under it would discard the record while its
    /// writer is told the write succeeded. `sequenced_through == our_end`
    /// rules out staged writers (staging advances it under this lock), and
    /// `tail == our_end` rules out records appended outside the write path.
    pub(super) async fn maybe_compact(
        &self,
        state: &mut ShardState,
        our_end: Offset,
    ) -> Result<()> {
        if !Self::should_compact(&state.index) {
            return Ok(());
        }
        if state.sequenced_through != Some(our_end) {
            return Ok(());
        }
        let tail = state.log.tail_offset().await?;
        if tail != our_end {
            return Ok(());
        }
        self.compact(state).await?;
        // Compaction re-appended the live set outside the reserve path, so
        // the sequence restarts at the new tail. The caller's own turn is
        // still held; the generation bump makes its release a no-op.
        let new_tail = state.log.tail_offset().await?;
        self.sequencer.reset(new_tail);
        state.sequenced_through = Some(new_tail);
        Ok(())
    }

    /// True when the log holds enough garbage to be worth rewriting.
    fn should_compact(index: &Index) -> bool {
        index.log_bytes > COMPACT_FLOOR_BYTES
            && index.log_bytes > index.live_bytes.saturating_mul(COMPACT_WHEN_TIMES_LIVE)
    }

    /// Rewrite the live set into a fresh log and swap it in.
    ///
    /// **Records are never rewritten**, which is the invariant the whole storage
    /// layer rests on. Compaction honours it: it writes new segments in a new
    /// directory and swaps directories, and never edits a byte in place. A crash
    /// at any point leaves either the old log or the new one whole, because the
    /// swap is two renames and the old log is not removed until the new one is
    /// in position.
    pub(super) async fn compact(&self, state: &mut ShardState) -> Result<()> {
        let now = now_millis();
        let mut live: Vec<(String, Bytes, u64)> = Vec::with_capacity(state.index.entries.len());
        for (key, entry) in &state.index.entries {
            if entry.is_expired(now) {
                // Expired entries are exactly what compaction is for: reclaimed
                // here rather than carried into the new log.
                continue;
            }
            if let Some(value) = self.read_value(state, *entry).await? {
                live.push((key.clone(), value, entry.expires_at_millis));
            }
        }

        let staging = self.dir.with_extension("compacting");
        if staging.exists() {
            // Left by a crash mid-compaction. It was never swapped in, so it
            // holds nothing the current log does not.
            std::fs::remove_dir_all(&staging).map_err(StorageError::Io)?;
        }
        // The compacted log continues the offset space rather than restarting
        // it. An offset has to name the same record for the life of the shard:
        // replication ships records at their offsets, so a leader that renumbered
        // on compaction would make its offset 0 a different record from every
        // follower's, with no way for either to tell. Continuing from the tail
        // makes compaction an append of the live set, which is the one shape
        // the rest of the storage layer already assumes.
        let resume_at = state.log.tail_offset().await?;
        let fresh = DiskLog::open_at(
            staging.clone(),
            self.label.clone(),
            self.config.clone(),
            resume_at,
        )?;

        let mut index = Index::default();
        for (key, value, expires_at_millis) in live {
            let payload = CacheOp::Put {
                key: key.clone(),
                value,
                expires_at_millis,
            }
            .encode();
            let bytes = payload.len() as u64;
            let appended = fresh
                .append(&[AppendRecord {
                    payload,
                    timestamp_micros: now * 1000,
                }])
                .await?;
            index.entries.insert(
                key,
                Entry {
                    offset: appended.first_offset,
                    expires_at_millis,
                    bytes,
                },
            );
            index.live_bytes += bytes;
            index.log_bytes += bytes;
        }
        fresh.shutdown().await?;
        state.log.shutdown().await?;

        // Each rename is synced before the next, so a crash lands on one of
        // the two states `recover_interrupted_swap` knows how to read. Without
        // the syncs the renames can reach disk in either order, or not at all,
        // and the window in between is total loss for the shard: the directory
        // is missing, and an unguarded open would create it empty.
        let retired = self.dir.with_extension("retired");
        if retired.exists() {
            std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;
        }
        let parent = self.dir.parent().map(Path::to_path_buf);
        std::fs::rename(&self.dir, &retired).map_err(StorageError::Io)?;
        if let Some(parent) = &parent {
            sync_dir(parent).map_err(StorageError::Io)?;
        }
        std::fs::rename(&staging, &self.dir).map_err(StorageError::Io)?;
        if let Some(parent) = &parent {
            sync_dir(parent).map_err(StorageError::Io)?;
        }
        std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;

        state.log = DiskLog::open(self.dir.clone(), self.label.clone(), self.config.clone())?;
        index.covered_through = Some(state.log.tail_offset().await?);
        state.index = index;
        Ok(())
    }
}

/// Finish a compaction swap that a crash interrupted.
///
/// Compaction renames the shard directory aside to `.retired`, renames the
/// compacted one into its place, then deletes the retired copy. A crash
/// between the first two leaves the shard directory missing and all of its
/// data in `.retired` — and an open that ignored that would create the
/// directory empty and the next compaction would delete the only copy.
///
/// The retired directory is the pre-compaction state, so restoring it loses
/// the compaction and nothing else.
pub(super) fn recover_interrupted_swap(dir: &Path) -> Result<()> {
    let retired = dir.with_extension("retired");
    if dir.exists() || !retired.exists() {
        return Ok(());
    }
    tracing::warn!(
        dir = %dir.display(),
        "a compaction was interrupted; restoring the shard from its retired copy",
    );
    std::fs::rename(&retired, dir).map_err(StorageError::Io)?;
    if let Some(parent) = dir.parent() {
        sync_dir(parent).map_err(StorageError::Io)?;
    }
    Ok(())
}
