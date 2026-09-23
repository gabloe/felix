//! A cache that is a log.
//!
//! `put` and `delete` append a record; an in-memory index maps each key to the
//! offset of the record that currently defines it; `get` reads the log at that
//! offset. See `docs/cache-on-log.md` for the design and the reasoning behind
//! each part of it.
//!
//! This is what makes "one core log, many semantics" true of the cache rather
//! than aspirational: crash safety, group commit, the fsync policy, and
//! eventually replication are all inherited from the log rather than
//! reimplemented beside it.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

use crate::commit_order::CommitSequencer;
use crate::disk_log::{DiskLog, layout};
use crate::log::{AppendOnlyLog, AppendRecord, LogConfig, Offset, ReadRange, ShardKey};
use crate::segment::io::sync_dir;
use crate::{CacheChange, CacheObserver, CacheSnapshotEntry, Result, StorageApi, StorageError};

mod record;
pub use record::CacheOp;

/// How much larger than its live bytes a log may grow before it is compacted.
///
/// A multiple rather than a fixed size, so the cost is proportional to the
/// garbage: a cache that is mostly live is never compacted however big it is,
/// and one that is mostly overwrites is compacted however small.
const COMPACT_WHEN_TIMES_LIVE: u64 = 4;

/// Below this there is nothing worth reclaiming, whatever the ratio says. Stops
/// a cache holding a handful of keys from compacting on every other write.
const COMPACT_FLOOR_BYTES: u64 = 1024 * 1024;

/// Where one key's current value lives.
#[derive(Debug, Clone, Copy)]
struct Entry {
    offset: Offset,
    /// Absolute Unix milliseconds; zero means it never expires.
    expires_at_millis: u64,
    /// What this record costs on disk, for deciding when to compact.
    bytes: u64,
}

impl Entry {
    fn is_expired(&self, now_millis: u64) -> bool {
        self.expires_at_millis != 0 && self.expires_at_millis <= now_millis
    }
}

/// The index over one cache's log, and the accounting compaction needs.
#[derive(Debug, Default)]
struct Index {
    entries: HashMap<String, Entry>,
    /// Bytes held by records the index still points at.
    live_bytes: u64,
    /// Bytes appended since the log was last compacted, live or not.
    log_bytes: u64,
    /// The offset this index has read up to. Records at or past it are not
    /// reflected here yet.
    ///
    /// `None` means nothing has been read, which is not the same as having read
    /// an empty log: a shard whose log begins at a trimmed base has no offset
    /// zero to start from.
    covered_through: Option<u64>,
}

/// One cache: its log, and the index derived from it.
struct CacheShard {
    dir: PathBuf,
    label: String,
    config: LogConfig,
    /// Guards the log handle and the index. A write holds it twice, briefly —
    /// once to stage (claim an offset, no fsync) and once to apply — never
    /// across the fsync, which is what lets concurrent writers share one
    /// group-committed flush instead of each paying a whole device flush.
    state: Mutex<ShardState>,
    /// Re-serialises the post-durability half of writes into disk-offset
    /// order. Fsyncs overlap; what `get` and the watch observer see does not.
    sequencer: CommitSequencer,
}

struct ShardState {
    log: DiskLog,
    index: Index,
    /// Exclusive end of the last offset range a writer here has reserved with
    /// the sequencer. Offsets past it were appended by someone else — recovery
    /// on open, compaction, or a leader shipping records to this follower —
    /// and `ensure_index` resolves that gap so the sequence can walk past it.
    /// `None` until the first `ensure_index` aligns the sequencer to the tail.
    sequenced_through: Option<u64>,
}

/// A cache backed by the same log streams are.
#[derive(Debug)]
pub struct LogCache {
    root: PathBuf,
    config: LogConfig,
    shards: SyncMutex<HashMap<CacheId, Arc<CacheShard>>>,
    /// Told about every applied write, while the shard's write lock is held —
    /// which is what makes the order it sees the shard's order.
    observer: SyncMutex<Option<Arc<dyn CacheObserver>>>,
}

impl std::fmt::Debug for CacheShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheShard")
            .field("label", &self.label)
            .finish()
    }
}

/// Tenant, namespace, cache, and shard. The shard is part of the identity
/// because each one is a separate log in a separate directory.
type CacheId = (String, String, String, u32);

impl LogCache {
    /// Open the cache store rooted at `root`.
    ///
    /// Callers pass the *cache* root, which is deliberately not the stream root:
    /// a shard directory is named from a hash of its tenant, namespace and
    /// stream, so a cache and a stream sharing a name would otherwise interleave
    /// their records in one directory.
    pub fn open(root: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(StorageError::Io)?;
        Ok(Self {
            root,
            config,
            shards: SyncMutex::new(HashMap::new()),
            observer: SyncMutex::new(None),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The log backing one cache shard.
    ///
    /// For replication, which ships a shard's records to followers and needs
    /// the same log the cache writes to — a second log over the same directory
    /// would interleave offsets and corrupt the segment.
    ///
    /// Returns the log as it stands now, and callers must fetch it again per
    /// pass rather than holding it. Compaction swaps the shard directory, so a
    /// handle kept across one keeps reading the retired log. That is harmless
    /// but not useful: since compaction re-appends the live set at the tail,
    /// the retired log holds only records the caller already shipped.
    pub async fn shard_log(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<DiskLog> {
        Ok(self
            .shard(tenant, namespace, cache, shard)?
            .current_log()
            .await)
    }

    /// Flush every open cache. Call once during graceful shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let shards: Vec<Arc<CacheShard>> = self.shards.lock().values().cloned().collect();
        for shard in shards {
            shard.state.lock().await.log.shutdown().await?;
        }
        Ok(())
    }

    /// Like [`LogCache::shard_log`], but creates the shard's log beginning at
    /// `base_offset` when it is not there yet.
    ///
    /// For a follower being given a cache whose early history the leader has
    /// already compacted away: its log starts where the surviving records do.
    /// An existing shard keeps the base recorded in its own first segment.
    pub async fn shard_log_at(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: u64,
    ) -> Result<DiskLog> {
        Ok(self
            .shard_with_base(tenant, namespace, cache, shard, Some(base_offset))?
            .current_log()
            .await)
    }

    fn shard(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Arc<CacheShard>> {
        self.shard_with_base(tenant, namespace, cache, shard, None)
    }

    fn shard_with_base(
        &self,
        tenant: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: Option<u64>,
    ) -> Result<Arc<CacheShard>> {
        let id = (
            tenant.to_string(),
            namespace.to_string(),
            cache.to_string(),
            shard,
        );
        // Opening runs under the lock: two callers racing to open the same new
        // cache must not both replay the log and both create segment zero.
        let mut shards = self.shards.lock();
        if let Some(open) = shards.get(&id) {
            return Ok(Arc::clone(open));
        }
        let key = ShardKey {
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: cache.to_string(),
            shard,
        };
        let dir = layout::shard_dir(&self.root, &key);
        let label = layout::shard_label(&key);
        recover_interrupted_swap(&dir)?;
        let log = match base_offset {
            Some(base) => DiskLog::open_at(dir.clone(), label.clone(), self.config.clone(), base)?,
            None => DiskLog::open(dir.clone(), label.clone(), self.config.clone())?,
        };
        let open = Arc::new(CacheShard {
            dir,
            label,
            config: self.config.clone(),
            state: Mutex::new(ShardState {
                log,
                index: Index::default(),
                sequenced_through: None,
            }),
            // Aligned to the log's tail by the first `ensure_index`; until
            // then nothing can reserve, because every writer passes through
            // `ensure_index` first.
            sequencer: CommitSequencer::new(0),
        });
        shards.insert(id, Arc::clone(&open));
        Ok(open)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

/// How much of the log one replay or compaction pass reads at a time.
///
/// Bounds peak memory during a rebuild: a cache far larger than this costs many
/// reads, not one enormous allocation.
const SCAN_CHUNK_BYTES: usize = 4 * 1024 * 1024;

impl CacheShard {
    /// The log this shard is writing to right now.
    ///
    /// Takes the state lock so it cannot observe compaction halfway through the
    /// directory swap.
    async fn current_log(&self) -> DiskLog {
        self.state.lock().await.log.clone()
    }

    /// Rebuild the index by replaying the log.
    ///
    /// Run once, lazily, the first time a cache is touched after opening. The
    /// index is derived and never trusted from disk -- the same rule the segment
    /// indexes follow, and for the same reason: anything recomputable from the
    /// log must be, because then it cannot be stale in a way that matters.
    async fn ensure_index(&self, state: &mut ShardState) -> Result<()> {
        let tail = state.log.tail_offset().await?;
        // Align the sequencer with records that did not come through the
        // write path — recovery on open, compaction, or a leader shipping
        // records to this shard as a follower. Without this, the next writer
        // would reserve its range past a gap nobody will ever resolve, and
        // wait on a turn that cannot arrive.
        match state.sequenced_through {
            None => {
                // First touch after open. Nothing can be in flight yet:
                // every writer passes through here, under this lock, before
                // it reserves anything.
                self.sequencer.reset(tail);
                state.sequenced_through = Some(tail);
            }
            Some(through) if tail > through => {
                // Out-of-band records hold [through, tail). Reserving and
                // immediately releasing the range resolves it, and the
                // resolution waits its turn behind any writer still in
                // flight below it.
                drop(self.sequencer.reserve(through, tail));
                state.sequenced_through = Some(tail);
            }
            _ => {}
        }
        // Fold nothing a writer has staged but not yet committed: staged
        // records become visible in their writers' apply step, after their
        // fsync, or a reader could see a put that a crash then loses. With
        // writers in flight the applied sequence is the visibility frontier;
        // idle, it equals the tail.
        let applied = self.sequencer.next_offset();
        let stop = if state.sequenced_through == Some(applied) {
            tail
        } else {
            applied
        };
        // Records can reach this log without going through the write path (see
        // above), so this catches up rather than building once and trusting
        // itself forever -- the same rule the segment indexes follow.
        let resume = state.index.covered_through;
        if resume == Some(stop) {
            return Ok(());
        }
        let (mut offset, mut index) = match resume {
            Some(covered) => (covered, std::mem::take(&mut state.index)),
            None => (state.log.base_offset(), Index::default()),
        };
        'scan: while offset < stop {
            let records = state
                .log
                .read_range(ReadRange {
                    start: offset,
                    max_bytes: SCAN_CHUNK_BYTES,
                })
                .await?;
            if records.is_empty() {
                break;
            }
            for record in &records {
                if record.offset >= stop {
                    // A read is bounded by bytes, not offset, so it can hand
                    // back staged records past the frontier.
                    break 'scan;
                }
                let bytes = record.payload.len() as u64;
                index.log_bytes += bytes;
                let op = record::CacheOp::decode(&record.payload)
                    .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?;
                match op {
                    CacheOp::Put {
                        key,
                        expires_at_millis,
                        ..
                    } => {
                        if let Some(previous) = index.entries.insert(
                            key,
                            Entry {
                                offset: record.offset,
                                expires_at_millis,
                                bytes,
                            },
                        ) {
                            index.live_bytes -= previous.bytes;
                        }
                        index.live_bytes += bytes;
                    }
                    CacheOp::Delete { key } => {
                        if let Some(previous) = index.entries.remove(&key) {
                            index.live_bytes -= previous.bytes;
                        }
                    }
                }
                offset = record.offset + 1;
            }
        }
        index.covered_through = Some(stop.max(offset));
        state.index = index;
        Ok(())
    }

    /// Fold one committed record into the index.
    ///
    /// The caller holds the state lock and has waited its turn, so applies
    /// land strictly in disk-offset order — which is what keeps "a later
    /// offset wins" true for the index without the lock spanning the fsync.
    fn apply_op(state: &mut ShardState, op: &CacheOp, offset: Offset, bytes: u64) {
        state.index.log_bytes += bytes;
        // Advance the watermark so the next `ensure_index` does not rescan
        // this record; never move it backwards, and never invent one, because
        // a watermark that skips unread history is worse than none.
        if state
            .index
            .covered_through
            .is_some_and(|covered| covered <= offset)
        {
            state.index.covered_through = Some(offset + 1);
        }
        match op {
            CacheOp::Put {
                key,
                expires_at_millis,
                ..
            } => {
                let entry = Entry {
                    offset,
                    expires_at_millis: *expires_at_millis,
                    bytes,
                };
                if let Some(previous) = state.index.entries.insert(key.clone(), entry) {
                    state.index.live_bytes -= previous.bytes;
                }
                state.index.live_bytes += bytes;
            }
            CacheOp::Delete { key } => {
                if let Some(previous) = state.index.entries.remove(key) {
                    state.index.live_bytes -= previous.bytes;
                }
            }
        }
    }

    /// Read the record the index points at, and hand back its value.
    async fn read_value(&self, state: &ShardState, entry: Entry) -> Result<Option<Bytes>> {
        let records = state
            .log
            .read_range(ReadRange {
                start: entry.offset,
                // One record. The cap has to exceed it, and a record larger
                // than this could not have been appended in the first place.
                max_bytes: SCAN_CHUNK_BYTES,
            })
            .await?;
        let Some(found) = records.first() else {
            // The index points past the end of the log. Nothing can make that
            // right, and returning a miss would hide it.
            return Err(StorageError::NotFound);
        };
        match record::CacheOp::decode(&found.payload)
            .map_err(|err| StorageError::Corruption(err.in_shard(&self.label)))?
        {
            CacheOp::Put { value, .. } => Ok(Some(value)),
            // The index only ever points at a put; a tombstone here means the
            // index and the log disagree, which is a bug rather than a miss.
            CacheOp::Delete { .. } => Err(StorageError::NotFound),
        }
    }

    /// True when the log holds enough garbage to be worth rewriting.
    fn should_compact(index: &Index) -> bool {
        index.log_bytes > COMPACT_FLOOR_BYTES
            && index.log_bytes > index.live_bytes.saturating_mul(COMPACT_WHEN_TIMES_LIVE)
    }

    /// Compact when worthwhile — but only from an apply whose record is the
    /// newest in the log, with nothing staged behind it.
    ///
    /// The gate is load-bearing: compaction swaps the shard directory, and a
    /// record another writer has staged but not yet committed lives only in
    /// the old directory. Swapping under it would discard the record while its
    /// writer is told the write succeeded. `sequenced_through == our_end`
    /// rules out staged writers (staging advances it under this lock), and
    /// `tail == our_end` rules out records appended outside the write path.
    async fn maybe_compact(&self, state: &mut ShardState, our_end: Offset) -> Result<()> {
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

    /// Rewrite the live set into a fresh log and swap it in.
    ///
    /// **Records are never rewritten**, which is the invariant the whole storage
    /// layer rests on. Compaction honours it: it writes new segments in a new
    /// directory and swaps directories, and never edits a byte in place. A crash
    /// at any point leaves either the old log or the new one whole, because the
    /// swap is two renames and the old log is not removed until the new one is
    /// in position.
    async fn compact(&self, state: &mut ShardState) -> Result<()> {
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
fn recover_interrupted_swap(dir: &Path) -> Result<()> {
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

#[async_trait]
impl StorageApi for LogCache {
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) {
        if let Err(err) = self
            .put_checked(tenant_id, namespace, cache, shard, key, value, ttl)
            .await
        {
            // The trait returns nothing, so a failed write can only be reported
            // here. Worth a loud line: the caller has been told the write
            // succeeded and it did not.
            tracing::error!(
                tenant_id, namespace, cache, key, error = %err,
                "cache write failed after the client was acknowledged",
            );
        }
    }

    async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Option<Bytes> {
        match self
            .get_checked(tenant_id, namespace, cache, shard, key)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, key, error = %err,
                    "cache read failed; reporting a miss",
                );
                None
            }
        }
    }

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Option<Bytes> {
        match self
            .delete_checked(tenant_id, namespace, cache, shard, key)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, key, error = %err,
                    "cache delete failed",
                );
                None
            }
        }
    }

    async fn shard_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Option<DiskLog> {
        match LogCache::shard_log(self, tenant_id, namespace, cache, shard).await {
            Ok(log) => Some(log),
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, shard, error = %err,
                    "could not open a cache shard's log for replication",
                );
                None
            }
        }
    }

    async fn shard_log_at(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        base_offset: u64,
    ) -> Option<DiskLog> {
        match LogCache::shard_log_at(self, tenant_id, namespace, cache, shard, base_offset).await {
            Ok(log) => Some(log),
            Err(err) => {
                tracing::error!(
                    tenant_id, namespace, cache, shard, error = %err,
                    "could not open a cache shard's log to bootstrap it",
                );
                None
            }
        }
    }

    fn set_change_observer(&self, observer: Arc<dyn CacheObserver>) -> bool {
        *self.observer.lock() = Some(observer);
        true
    }

    async fn live_entries(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        self.live_entries_checked(tenant_id, namespace, cache, shard)
            .await
    }

    async fn len(&self) -> usize {
        let shards: Vec<Arc<CacheShard>> = self.shards.lock().values().cloned().collect();
        let now = now_millis();
        let mut total = 0;
        for shard in shards {
            let mut state = shard.state.lock().await;
            if shard.ensure_index(&mut state).await.is_err() {
                continue;
            }
            // Expired entries are gone as far as any reader is concerned, so
            // counting them would report a size nothing can observe.
            total += state
                .index
                .entries
                .values()
                .filter(|entry| !entry.is_expired(now))
                .count();
        }
        total
    }

    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

impl LogCache {
    /// `put`, with the failure the trait cannot express.
    ///
    /// The write has two halves, and the state lock spans neither fsync nor
    /// wait. Stage under a short lock (claim an offset and a turn), commit
    /// outside it (the fsync, group-committed with every concurrent writer),
    /// then wait the turn out and apply under the lock again. Returning only
    /// after `commit` keeps the ack durability-gated; applying only after
    /// `wait` keeps index and watch order equal to disk order.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<()> {
        let shard_index = shard;
        let shard = self.shard(tenant_id, namespace, cache, shard_index)?;
        let expires_at_millis = ttl.map_or(0, |ttl| now_millis() + ttl.as_millis() as u64);
        let op = CacheOp::Put {
            key: key.to_string(),
            value: value.clone(),
            expires_at_millis,
        };

        let (pending, log, bytes, turn) = {
            let mut state = shard.state.lock().await;
            shard.ensure_index(&mut state).await?;
            let payload = op.encode();
            let bytes = payload.len() as u64;
            let pending = state
                .log
                .append_pending(&[AppendRecord {
                    payload,
                    timestamp_micros: now_millis() * 1000,
                }])
                .await?;
            // Claimed the moment the offsets are consumed. The guard releases
            // the range on every exit path — error, cancellation mid-await —
            // so a failed commit cannot strand the writers queued behind it.
            let turn = shard
                .sequencer
                .reserve(pending.first_offset(), pending.last_offset() + 1);
            state.sequenced_through = Some(pending.last_offset() + 1);
            (pending, state.log.clone(), bytes, turn)
        };

        log.commit(&pending).await?;
        turn.wait().await;

        let mut state = shard.state.lock().await;
        let offset = pending.first_offset();
        CacheShard::apply_op(&mut state, &op, offset, bytes);
        // Observed under the apply lock, in turn order: that is what makes
        // the order watchers see the shard's disk order, and it fires only
        // for a write that is already durable.
        self.notify(CacheChange {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            shard: shard_index,
            key: key.to_string(),
            value: Some(value),
            offset,
            expires_at_millis,
        });
        shard
            .maybe_compact(&mut state, pending.last_offset() + 1)
            .await?;
        Ok(())
    }

    /// `get`, with the failure the trait cannot express.
    pub async fn get_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let Some(entry) = state.index.entries.get(key).copied() else {
            return Ok(None);
        };
        if entry.is_expired(now_millis()) {
            // Lazy expiry, as the in-memory cache always did: reported absent
            // now, and the space reclaimed when the log is next compacted.
            return Ok(None);
        }
        shard.read_value(&state, entry).await
    }

    /// Every live key in one shard of one cache.
    ///
    /// Expired entries are excluded, for the same reason `len` excludes them: a
    /// key nothing can read is not a key. Order is unspecified — the index is a
    /// hash map — so a caller that needs one sorts.
    pub async fn keys(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<String>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let now = now_millis();
        Ok(state
            .index
            .entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired(now))
            .map(|(key, _)| key.clone())
            .collect())
    }

    /// `delete`, with the failure the trait cannot express.
    ///
    /// Same two-half shape as [`LogCache::put_checked`]. The previous value is
    /// read at staging time; against concurrent writers to the same key, the
    /// delete's place in the shard's history is its disk offset.
    pub async fn delete_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let shard_index = shard;
        let shard = self.shard(tenant_id, namespace, cache, shard_index)?;
        let op = CacheOp::Delete {
            key: key.to_string(),
        };

        let (previous, pending, log, bytes, turn) = {
            let mut state = shard.state.lock().await;
            shard.ensure_index(&mut state).await?;
            let Some(entry) = state.index.entries.get(key).copied() else {
                return Ok(None);
            };
            let previous = if entry.is_expired(now_millis()) {
                None
            } else {
                shard.read_value(&state, entry).await?
            };
            let payload = op.encode();
            let bytes = payload.len() as u64;
            let pending = state
                .log
                .append_pending(&[AppendRecord {
                    payload,
                    timestamp_micros: now_millis() * 1000,
                }])
                .await?;
            let turn = shard
                .sequencer
                .reserve(pending.first_offset(), pending.last_offset() + 1);
            state.sequenced_through = Some(pending.last_offset() + 1);
            (previous, pending, state.log.clone(), bytes, turn)
        };

        log.commit(&pending).await?;
        turn.wait().await;

        let mut state = shard.state.lock().await;
        let offset = pending.first_offset();
        CacheShard::apply_op(&mut state, &op, offset, bytes);
        self.notify(CacheChange {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            shard: shard_index,
            key: key.to_string(),
            value: None,
            offset,
            expires_at_millis: 0,
        });
        Ok(previous)
    }

    fn notify(&self, change: CacheChange) {
        let observer = self.observer.lock().clone();
        if let Some(observer) = observer {
            observer.cache_changed(change);
        }
    }

    /// Every live key in one shard with its current value and offset.
    ///
    /// Expired entries are excluded, as they are from every other read. The
    /// offsets are what let a watcher join this snapshot to live delivery
    /// without doubling: a change at or past the snapshot's tail arrives live,
    /// and one below it is already in here.
    pub async fn live_entries_checked(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let now = now_millis();
        let mut entries = Vec::new();
        for (key, entry) in &state.index.entries {
            if entry.is_expired(now) {
                continue;
            }
            if let Some(value) = shard.read_value(&state, *entry).await? {
                entries.push(CacheSnapshotEntry {
                    key: key.clone(),
                    value,
                    offset: entry.offset,
                    expires_at_millis: entry.expires_at_millis,
                });
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
#[path = "log_cache/tests.rs"]
mod tests;
