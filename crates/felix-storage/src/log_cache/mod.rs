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

use crate::disk_log::{DiskLog, layout};
use crate::log::{AppendOnlyLog, AppendRecord, LogConfig, Offset, ReadRange, ShardKey};
use crate::{Result, StorageApi, StorageError};

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
    /// Held across a write and across compaction. A cache write is serialised
    /// here anyway, so compaction adds no new contention -- only a longer hold.
    state: Mutex<ShardState>,
}

struct ShardState {
    log: DiskLog,
    index: Index,
}

/// A cache backed by the same log streams are.
#[derive(Debug)]
pub struct LogCache {
    root: PathBuf,
    config: LogConfig,
    shards: SyncMutex<HashMap<CacheId, Arc<CacheShard>>>,
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
            }),
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
        // Records can reach this log without going through `write`: a follower
        // is shipped them directly, and it may later be promoted and asked to
        // serve them. So this catches up to the tail rather than building once
        // and trusting itself forever -- the same rule the segment indexes
        // follow, for the same reason.
        let resume = state.index.covered_through;
        if resume == Some(tail) {
            return Ok(());
        }
        let (mut offset, mut index) = match resume {
            Some(covered) => (covered, std::mem::take(&mut state.index)),
            None => (state.log.base_offset(), Index::default()),
        };
        while offset < tail {
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
        index.covered_through = Some(tail.max(offset));
        state.index = index;
        Ok(())
    }

    /// Append one record and fold it into the index.
    async fn write(&self, state: &mut ShardState, op: CacheOp) -> Result<()> {
        let payload = op.encode();
        let bytes = payload.len() as u64;
        let appended = state
            .log
            .append(&[AppendRecord {
                payload,
                timestamp_micros: now_millis() * 1000,
            }])
            .await?;

        state.index.log_bytes += bytes;
        // Every caller reaches here through `ensure_index`, so the watermark is
        // already set; advancing it keeps the next read from rescanning a record
        // this just applied. Left alone when it is unset rather than invented,
        // because a watermark that skips unread history is worse than none.
        if state.index.covered_through.is_some() {
            state.index.covered_through = Some(appended.first_offset + 1);
        }
        match op {
            CacheOp::Put {
                key,
                expires_at_millis,
                ..
            } => {
                let entry = Entry {
                    offset: appended.first_offset,
                    expires_at_millis,
                    bytes,
                };
                if let Some(previous) = state.index.entries.insert(key, entry) {
                    state.index.live_bytes -= previous.bytes;
                }
                state.index.live_bytes += bytes;
            }
            CacheOp::Delete { key } => {
                if let Some(previous) = state.index.entries.remove(&key) {
                    state.index.live_bytes -= previous.bytes;
                }
            }
        }
        Ok(())
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

        let retired = self.dir.with_extension("retired");
        if retired.exists() {
            std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;
        }
        std::fs::rename(&self.dir, &retired).map_err(StorageError::Io)?;
        std::fs::rename(&staging, &self.dir).map_err(StorageError::Io)?;
        std::fs::remove_dir_all(&retired).map_err(StorageError::Io)?;

        state.log = DiskLog::open(self.dir.clone(), self.label.clone(), self.config.clone())?;
        index.covered_through = Some(state.log.tail_offset().await?);
        state.index = index;
        Ok(())
    }
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
        let shard = self.shard(tenant_id, namespace, cache, shard)?;
        let mut state = shard.state.lock().await;
        shard.ensure_index(&mut state).await?;
        let expires_at_millis = ttl.map_or(0, |ttl| now_millis() + ttl.as_millis() as u64);
        shard
            .write(
                &mut state,
                CacheOp::Put {
                    key: key.to_string(),
                    value,
                    expires_at_millis,
                },
            )
            .await?;
        if CacheShard::should_compact(&state.index) {
            shard.compact(&mut state).await?;
        }
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

    /// `delete`, with the failure the trait cannot express.
    pub async fn delete_checked(
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
        let previous = if entry.is_expired(now_millis()) {
            None
        } else {
            shard.read_value(&state, entry).await?
        };
        shard
            .write(
                &mut state,
                CacheOp::Delete {
                    key: key.to_string(),
                },
            )
            .await?;
        Ok(previous)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
