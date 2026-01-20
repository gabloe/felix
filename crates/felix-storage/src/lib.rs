// Simple in-memory cache with optional TTL expiry.
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt;
use std::fmt::Debug;
use std::time::{Duration, Instant};

pub mod ephemeral_cache;
pub mod log;
pub mod tiered;
pub use ephemeral_cache::EphemeralCache;

#[async_trait()]
pub trait StorageApi: Debug + Send + Sync {
    async fn put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    );

    async fn get(&self, tenant_id: &str, namespace: &str, cache: &str, key: &str) -> Option<Bytes>;

    async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Option<Bytes>;

    async fn len(&self) -> usize;

    async fn is_empty(&self) -> bool;
}

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
                "k",
                Bytes::from_static(b"v"),
                Some(Duration::from_millis(10)),
            )
            .await;
        sleep(Duration::from_millis(15)).await;
        assert!(cache.get("t1", "default", "primary", "k").await.is_none());
    }

    #[tokio::test]
    async fn put_get_delete_round_trip() {
        let cache = EphemeralCache::new();
        cache
            .put(
                "t1",
                "default",
                "primary",
                "k",
                Bytes::from_static(b"value"),
                None,
            )
            .await;
        assert_eq!(
            cache.get("t1", "default", "primary", "k").await,
            Some(Bytes::from_static(b"value"))
        );
        assert_eq!(
            cache.delete("t1", "default", "primary", "k").await,
            Some(Bytes::from_static(b"value"))
        );
        assert!(cache.get("t1", "default", "primary", "k").await.is_none());
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
                "k1",
                Bytes::from_static(b"a"),
                None,
            )
            .await;
        assert!(!cache.is_empty().await);
        assert_eq!(cache.len().await, 1);
        cache.delete("t1", "default", "primary", "k1").await;
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
                "k2",
                Bytes::from_static(b"b"),
                None,
            )
            .await;
        assert_eq!(cache.len().await, 1);
    }
}
