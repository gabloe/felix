//! Where records are kept: a log-structured segment store, and the caches
//! projected from it.
//!
//! **Start at [`disk_log::DiskLog`].** That is the durable log a stream shard
//! is made of — append, read a range, recover a torn tail. [`segment`] is the
//! byte format underneath it, [`EphemeralCache`] is the in-memory store a
//! broker without durable storage uses instead, and [`LogCache`] is the
//! key-to-latest-value projection that makes a cache out of a log.
//!
//! Three properties everything here rests on, each explained in
//! `docs/durable-storage.md`:
//!
//! - **Records below the high-water mark are never rewritten**, which is what
//!   lets recovery trust "valid bytes end at EOF".
//! - **A torn tail is repaired; interior corruption is fatal.** Refusing to
//!   start beats silently losing acknowledged records.
//! - **Indexes are derived, never trusted** — a missing, short or stale index
//!   is rebuilt from the segment it describes.
//!
//! [`CommitSequencer`] is the odd one out: it orders publishes rather than
//! storing them, and lives here because the order is per durable log and is
//! shared with the cache write path.
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::fmt::Debug;
use std::time::{Duration, Instant};

pub mod commit_order;
pub mod counter_log;
pub mod disk_log;
pub mod ephemeral_cache;
pub mod log;
pub mod log_cache;
pub mod metrics_names;
pub mod segment;
pub mod tiered;
#[cfg(target_os = "linux")]
mod uring_fsync;
pub use commit_order::{CommitSequencer, CommitTurn};
pub use counter_log::CounterStore;
pub use disk_log::{DiskLog, DiskLogProvider};
pub use ephemeral_cache::EphemeralCache;
pub use log_cache::LogCache;
pub use segment::{Corruption, CorruptionKind, CorruptionSite};

/// One applied cache write, as an observer sees it.
#[derive(Debug, Clone)]
pub struct CacheChange {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
    pub shard: u32,
    pub key: String,
    /// The value the key now holds; `None` means the key was deleted.
    pub value: Option<Bytes>,
    /// The log offset the write was appended at.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}

/// Sees every write a cache store applies locally, in the order the shard's
/// log applied them.
///
/// Called with the shard's write lock held — that hold is what makes the
/// per-shard order a guarantee rather than a race — so an implementation must
/// not block: hand the change to a queue and return.
///
/// Replication is deliberately outside this seam: records shipped to a
/// follower reach its log without passing through `put`, so a follower's
/// observer stays silent. Watches are served where writes are applied.
pub trait CacheObserver: Debug + Send + Sync {
    fn cache_changed(&self, change: CacheChange);
}

/// One key's current state, as a watch snapshot reports it.
#[derive(Debug, Clone)]
pub struct CacheSnapshotEntry {
    pub key: String,
    pub value: Bytes,
    /// The log offset of the record that currently defines the key.
    pub offset: u64,
    /// Absolute Unix milliseconds; zero means it never expires.
    pub expires_at_millis: u64,
}

// A cache entry is identified by tenant, namespace, cache, shard, and key --
// five fields before the value and its TTL. Bundling them into a struct would
// move the argument list rather than shorten it, and every caller has the parts
// separately anyway.
#[allow(clippy::too_many_arguments)]
#[async_trait()]
pub trait StorageApi: Debug + Send + Sync {
    /// `shard` is which shard of the cache the key belongs to.
    ///
    /// Resolved by the caller, not here: the shard count lives in the routing
    /// table and storage must not depend on it. A caller that owns exactly one
    /// shard of everything passes `0`, which is what an unsharded cache always
    /// was.
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    );

    async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Option<Bytes>;

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        shard: u32,
        key: &str,
    ) -> Option<Bytes>;

    /// The log backing one cache shard, when the cache is log-backed.
    ///
    /// `None` for a cache with no log, which has nothing to replicate. Exposed
    /// on the trait rather than reached for by downcast because replication is
    /// a legitimate second reader of the same log, and it must be the same log
    /// the cache writes to.
    async fn shard_log(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Option<crate::disk_log::DiskLog> {
        None
    }

    /// The log backing one cache shard, created to begin at `base_offset` if it
    /// does not exist yet. See [`StorageApi::shard_log`].
    async fn shard_log_at(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
        _base_offset: u64,
    ) -> Option<crate::disk_log::DiskLog> {
        None
    }

    /// Install the observer every applied write is reported to.
    ///
    /// `false` means this store cannot observe writes, and the caller must not
    /// offer watches over it. The default is exactly that: a watch's contract
    /// is built on log offsets, and a store with no log has none to report.
    fn set_change_observer(&self, _observer: std::sync::Arc<dyn CacheObserver>) -> bool {
        false
    }

    /// Every live key in one shard with its current value and offset, for a
    /// watch that must begin from current state.
    ///
    /// An error, never an empty answer, when the store cannot serve it: a
    /// watcher told a shard is empty would trust a snapshot it never got.
    async fn live_entries(
        &self,
        _tenant_id: &str,
        _namespace: &str,
        _cache: &str,
        _shard: u32,
    ) -> Result<Vec<CacheSnapshotEntry>> {
        Err(StorageError::Unsupported("cache watch snapshot"))
    }

    async fn len(&self) -> usize;

    async fn is_empty(&self) -> bool;
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Unsupported(&'static str),
    /// The log was asked to open with settings that cannot work. Raised at open
    /// time, never on the append path.
    InvalidConfig(&'static str),
    InvalidRange,
    /// The requested offset was discarded by retention or truncation. Distinct
    /// from an empty range, which is a valid answer for a reader that has caught
    /// up with the tail.
    Trimmed {
        requested: u64,
        oldest: u64,
    },
    NotFound,
    /// On-disk bytes did not decode. Carries the specific invariant that was
    /// violated plus the shard/segment/position it was found at, because
    /// "corruption detected" is not enough to act on at 3am.
    Corruption(Corruption),
    /// A durable append could not be acknowledged. Distinct from `Io` so callers
    /// can tell "the write never happened" from "the write may have happened but
    /// we could not confirm it".
    SyncFailed(String),
    Io(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            StorageError::InvalidConfig(detail) => write!(f, "invalid configuration: {detail}"),
            StorageError::InvalidRange => write!(f, "invalid range"),
            StorageError::Trimmed { requested, oldest } => write!(
                f,
                "offset {requested} is no longer available; the log starts at {oldest}"
            ),
            StorageError::NotFound => write!(f, "not found"),
            StorageError::Corruption(detail) => write!(f, "corruption detected: {detail}"),
            StorageError::SyncFailed(detail) => write!(f, "durability sync failed: {detail}"),
            StorageError::Io(err) => write!(f, "io error: {err}"),
        }
    }
}

impl From<Corruption> for StorageError {
    fn from(err: Corruption) -> Self {
        StorageError::Corruption(err)
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err)
    }
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    // Stored value plus optional expiration.
    value: Bytes,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
    key: String,
}

impl CacheKey {
    pub fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            cache: cache.into(),
            key: key.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn cache_ttl_expiry() {
        // Ensure TTL logic expires keys after the deadline.
        let cache = EphemeralCache::new();
        cache
            .put(
                "t1",
                "default",
                "primary",
                0,
                "k",
                Bytes::from_static(b"v"),
                Some(Duration::from_millis(10)),
            )
            .await;
        sleep(Duration::from_millis(15)).await;
        assert!(
            cache
                .get("t1", "default", "primary", 0, "k")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn put_get_delete_round_trip() {
        let cache = EphemeralCache::new();
        cache
            .put(
                "t1",
                "default",
                "primary",
                0,
                "k",
                Bytes::from_static(b"value"),
                None,
            )
            .await;
        assert_eq!(
            cache.get("t1", "default", "primary", 0, "k").await,
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(
            cache.delete("t1", "default", "primary", 0, "k").await,
            Some(Bytes::from_static(b"value"))
        );
        assert!(
            cache
                .get("t1", "default", "primary", 0, "k")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn len_and_is_empty_reflect_state() {
        let cache = EphemeralCache::new();
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
        cache
            .put(
                "t1",
                "default",
                "primary",
                0,
                "k1",
                Bytes::from_static(b"a"),
                None,
            )
            .await;
        assert!(!cache.is_empty().await);
        assert_eq!(cache.len().await, 1);
        cache.delete("t1", "default", "primary", 0, "k1").await;
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
    }

    #[tokio::test]
    async fn capacity_enforces_placeholder_eviction() {
        let cache = EphemeralCache::with_capacity(1);
        cache
            .put(
                "t1",
                "default",
                "primary",
                0,
                "k1",
                Bytes::from_static(b"a"),
                None,
            )
            .await;
        cache
            .put(
                "t1",
                "default",
                "primary",
                0,
                "k2",
                Bytes::from_static(b"b"),
                None,
            )
            .await;
        assert_eq!(cache.len().await, 1);
    }

    #[test]
    fn cache_key_construction() {
        let key = CacheKey::new("tenant1", "ns1", "cache1", "key1");
        assert_eq!(key.tenant_id, "tenant1");
        assert_eq!(key.namespace, "ns1");
        assert_eq!(key.cache, "cache1");
        assert_eq!(key.key, "key1");
    }

    #[test]
    fn cache_key_equality() {
        let key1 = CacheKey::new("t1", "ns", "c", "k");
        let key2 = CacheKey::new("t1", "ns", "c", "k");
        let key3 = CacheKey::new("t2", "ns", "c", "k");
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn storage_error_display() {
        let err = StorageError::Unsupported("feature");
        assert!(err.to_string().contains("feature"));

        let err = StorageError::InvalidRange;
        assert!(err.to_string().contains("invalid range"));

        let err = StorageError::NotFound;
        assert!(err.to_string().contains("not found"));

        let err =
            StorageError::Corruption(Corruption::new(CorruptionKind::IndexVersion { found: 9 }));
        assert!(err.to_string().contains("corruption"));
        assert!(err.to_string().contains("unsupported index version 9"));

        let err = StorageError::SyncFailed("disk full".into());
        assert!(err.to_string().contains("disk full"));
    }

    #[test]
    fn storage_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let storage_err = StorageError::from(io_err);
        assert!(matches!(storage_err, StorageError::Io(_)));
    }

    #[test]
    fn storage_error_source() {
        let io_err = std::io::Error::other("test");
        let storage_err = StorageError::from(io_err);
        assert!(storage_err.source().is_some());

        let storage_err = StorageError::NotFound;
        assert!(storage_err.source().is_none());
    }

    #[tokio::test]
    async fn get_nonexistent_key_returns_none() {
        let cache = EphemeralCache::new();
        assert!(cache.get("t1", "ns", "c", 0, "nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_key_returns_none() {
        let cache = EphemeralCache::new();
        assert!(
            cache
                .delete("t1", "ns", "c", 0, "nonexistent")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn put_overwrites_existing_value() {
        let cache = EphemeralCache::new();
        cache
            .put("t1", "ns", "c", 0, "k", Bytes::from_static(b"v1"), None)
            .await;
        cache
            .put("t1", "ns", "c", 0, "k", Bytes::from_static(b"v2"), None)
            .await;
        assert_eq!(
            cache.get("t1", "ns", "c", 0, "k").await,
            Some(Bytes::from_static(b"v2"))
        );
        assert_eq!(cache.len().await, 1);
    }
}
