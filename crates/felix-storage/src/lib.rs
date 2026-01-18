// Simple in-memory cache with optional TTL expiry.
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

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
#[derive(Debug, Default)]
pub struct EphemeralCache {
    // RwLock allows concurrent readers while updates take exclusive access.
    inner: RwLock<HashMap<String, CacheEntry>>,
}

impl EphemeralCache {
    // Use Default to centralize initialization.
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put(&self, key: impl Into<String>, value: Bytes, ttl: Option<Duration>) {
        // Compute expiry once so reads only compare Instants.
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let entry = CacheEntry { value, expires_at };
        self.inner.write().await.insert(key.into(), entry);
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
}
