// Simple in-memory cache with optional TTL expiry.
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub mod log;
pub mod tiered;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Unsupported(&'static str),
    InvalidRange,
    NotFound,
    Corruption,
    Io(std::io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            StorageError::InvalidRange => write!(f, "invalid range"),
            StorageError::NotFound => write!(f, "not found"),
            StorageError::Corruption => write!(f, "corruption detected"),
            StorageError::Io(err) => write!(f, "io error: {err}"),
        }
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

/// Simple in-memory cache with optional TTL expiry.
///
/// ```
/// use bytes::Bytes;
/// use felix_storage::EphemeralCache;
///
/// let cache = EphemeralCache::new();
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     cache.put("k", Bytes::from_static(b"v"), None).await;
///     assert_eq!(cache.get("k").await, Some(Bytes::from_static(b"v")));
/// });
/// ```
#[derive(Debug)]
pub struct EphemeralCache {
    // RwLock allows concurrent readers while updates take exclusive access.
    inner: RwLock<HashMap<String, CacheEntry>>,
    // Optional size cap to enable future eviction policies.
    max_entries: Option<usize>,
}

impl EphemeralCache {
    // Use Default to centralize initialization.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(max_entries: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: Some(max_entries),
        }
    }

    pub async fn put(&self, key: impl Into<String>, value: Bytes, ttl: Option<Duration>) {
        // Compute expiry once so reads only compare Instants.
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let entry = CacheEntry { value, expires_at };
        let mut guard = self.inner.write().await;
        guard.insert(key.into(), entry);
        if let Some(max_entries) = self.max_entries
            && guard.len() > max_entries
        {
            // Placeholder eviction: remove an arbitrary key until capped.
            if let Some(key) = guard.keys().next().cloned() {
                guard.remove(&key);
            }
        }
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        // Take a write lock so we can evict expired entries.
        let mut guard = self.inner.write().await;
        if let Some(entry) = guard.get(key) {
            if let Some(expires_at) = entry.expires_at {
                // Lazy-expire on read to avoid a background sweeper.
                if Instant::now() >= expires_at {
                    guard.remove(key);
                    return None;
                }
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub async fn delete(&self, key: &str) -> Option<Bytes> {
        // Remove and return the stored value, if any.
        self.inner
            .write()
            .await
            .remove(key)
            .map(|entry| entry.value)
    }

    pub async fn len(&self) -> usize {
        // Only a read lock is needed for length.
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        // Match len() with a direct emptiness check for callers.
        self.inner.read().await.is_empty()
    }
}

impl Default for EphemeralCache {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn cache_ttl_expiry() {
        // Ensure TTL logic expires keys after the deadline.
        let cache = EphemeralCache::new();
        cache
            .put(
                "k",
                Bytes::from_static(b"v"),
                Some(Duration::from_millis(10)),
            )
            .await;
        sleep(Duration::from_millis(15)).await;
        assert!(cache.get("k").await.is_none());
    }

    #[tokio::test]
    async fn put_get_delete_round_trip() {
        let cache = EphemeralCache::new();
        cache.put("k", Bytes::from_static(b"value"), None).await;
        assert_eq!(cache.get("k").await, Some(Bytes::from_static(b"value")));
        assert_eq!(cache.delete("k").await, Some(Bytes::from_static(b"value")));
        assert!(cache.get("k").await.is_none());
    }

    #[tokio::test]
    async fn len_and_is_empty_reflect_state() {
        let cache = EphemeralCache::new();
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
        cache.put("k1", Bytes::from_static(b"a"), None).await;
        assert!(!cache.is_empty().await);
        assert_eq!(cache.len().await, 1);
        cache.delete("k1").await;
        assert!(cache.is_empty().await);
        assert_eq!(cache.len().await, 0);
    }

    #[tokio::test]
    async fn capacity_enforces_placeholder_eviction() {
        let cache = EphemeralCache::with_capacity(1);
        cache.put("k1", Bytes::from_static(b"a"), None).await;
        cache.put("k2", Bytes::from_static(b"b"), None).await;
        assert_eq!(cache.len().await, 1);
    }
}
