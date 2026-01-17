// In-memory metadata store for small config blobs.
use std::collections::HashMap;
use tokio::sync::RwLock;

/// In-memory metadata store for small config blobs.
///
/// ```
/// use felix_metadata::InMemoryMetadata;
///
/// let store = InMemoryMetadata::new();
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     store.put("cluster/region", b"us-east".to_vec()).await;
///     let value = store.get("cluster/region").await;
///     assert_eq!(value, Some(b"us-east".to_vec()));
/// });
/// ```
#[derive(Debug, Default)]
pub struct InMemoryMetadata {
    // RwLock keeps reads cheap while writes remain exclusive.
    inner: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryMetadata {
    // Default-backed constructor for ergonomics.
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put(&self, key: impl Into<String>, value: Vec<u8>) {
        // Overwrite existing values atomically under the write lock.
        self.inner.write().await.insert(key.into(), value);
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        // Clone the bytes so callers can own the value.
        self.inner.read().await.get(key).cloned()
    }

    pub async fn delete(&self, key: &str) -> Option<Vec<u8>> {
        // Return the removed value so callers can verify deletes.
        self.inner.write().await.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_round_trip() {
        // Basic metadata write/read flow.
        let store = InMemoryMetadata::new();
        store.put("cluster/region", b"us-east".to_vec()).await;
        assert_eq!(store.get("cluster/region").await, Some(b"us-east".to_vec()));
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let store = InMemoryMetadata::new();
        store.put("k", b"v".to_vec()).await;
        assert_eq!(store.delete("k").await, Some(b"v".to_vec()));
        assert_eq!(store.get("k").await, None);
    }

    #[tokio::test]
    async fn put_overwrites_existing_value() {
        let store = InMemoryMetadata::new();
        store.put("k", b"v1".to_vec()).await;
        store.put("k", b"v2".to_vec()).await;
        assert_eq!(store.get("k").await, Some(b"v2".to_vec()));
    }
}
