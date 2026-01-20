use crate::{CacheEntry, CacheKey, StorageApi};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Simple in-memory cache with optional TTL expiry.
///
/// ```
/// use bytes::Bytes;
/// use felix_storage::*;
///
/// let cache = EphemeralCache::new();
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     cache
///         .put("t1", "default", "primary", "k", Bytes::from_static(b"v"), None)
///         .await;
///     assert_eq!(
///         cache.get("t1", "default", "primary", "k").await,
///         Some(Bytes::from_static(b"v"))
///     );
/// });
/// ```
#[derive(Debug)]
pub struct EphemeralCache {
    // RwLock allows concurrent readers while updates take exclusive access.
    inner: RwLock<HashMap<CacheKey, CacheEntry>>,
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
}

impl From<EphemeralCache> for Box<dyn StorageApi + Send> {
    fn from(value: EphemeralCache) -> Self {
        Box::new(value)
    }
}

#[async_trait()]
impl StorageApi for EphemeralCache {
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) {
        // Compute expiry once so reads only compare Instants.
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let entry = CacheEntry { value, expires_at };
        let mut guard = self.inner.write().await;
        guard.insert(CacheKey::new(tenant_id, namespace, cache, key), entry);
        if let Some(max_entries) = self.max_entries
            && guard.len() > max_entries
        {
            // Placeholder eviction: remove an arbitrary key until capped.
            if let Some(key) = guard.keys().next().cloned() {
                guard.remove(&key);
            }
        }
    }

    async fn get(&self, tenant_id: &str, namespace: &str, cache: &str, key: &str) -> Option<Bytes> {
        // Take a write lock so we can evict expired entries.
        let mut guard = self.inner.write().await;
        let scoped_key = CacheKey::new(tenant_id, namespace, cache, key);
        if let Some(entry) = guard.get(&scoped_key) {
            if let Some(expires_at) = entry.expires_at {
                // Lazy-expire on read to avoid a background sweeper.
                if Instant::now() >= expires_at {
                    guard.remove(&scoped_key);
                    return None;
                }
            }
            return Some(entry.value.clone());
        }
        None
    }

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Option<Bytes> {
        // Remove and return the stored value, if any.
        self.inner
            .write()
            .await
            .remove(&CacheKey::new(tenant_id, namespace, cache, key))
            .map(|entry| entry.value)
    }

    async fn len(&self) -> usize {
        // Only a read lock is needed for length.
        self.inner.read().await.len()
    }

    async fn is_empty(&self) -> bool {
        // Match len() with a direct emptiness check for callers.
        self.inner.read().await.is_empty()
    }
}

unsafe impl Send for EphemeralCache {}

impl Default for EphemeralCache {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            max_entries: None,
        }
    }
}
