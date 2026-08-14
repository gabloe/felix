// Tenant / namespace / stream / cache registries.
//
// These maps mirror control-plane state and gate every data-path operation. Lock
// order is tenants -> namespaces -> streams -> topics; keep it that way.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::broker::{Broker, CacheMetadata, StreamHandle, StreamMetadata};
use crate::error::{BrokerError, Result};
use crate::keys::{CacheKey, CacheKeyRef, NamespaceKey, NamespaceKeyRef, StreamKey, StreamKeyRef};
use crate::stream_state::StreamState;

/// Byte ceiling on the replay ring refill at startup.
///
/// The record count is already capped by the ring's capacity; this bounds the
/// payload bytes a single pathological stream can pull in while a broker is
/// starting, so one stream of very large records cannot stall registration of
/// every other stream behind it.
const HYDRATE_MAX_BYTES: usize = 64 * 1024 * 1024;

impl Broker {
    pub async fn register_stream(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
        metadata: StreamMetadata,
    ) -> Result<()> {
        // Fast-path guard: reject unknown scopes before attempting to create the stream.
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        let stream = stream.into();
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let namespace_key = NamespaceKey::new(tenant_id.clone(), namespace.clone());
        // Fast-path guard: reject unknown namespace.
        if !self.namespaces.read().await.contains_key(&namespace_key) {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id,
                namespace,
            });
        }
        let key = StreamKey::new(tenant_id, namespace, stream);

        // Cursor sequence numbers and durable offsets share one identity. An
        // existing ephemeral stream may already have cursor history that was
        // never written to disk, so toggling durability in place would make
        // those two sequences disagree. Require an explicit remove/recreate,
        // which also invalidates old handles and subscriptions.
        if let Some(existing) = self.streams.read().await.get(&key)
            && existing.durable != metadata.durable
        {
            return Err(Self::durability_change_error(&key, existing, &metadata));
        }

        // Open durable storage before touching the registries: a stream that
        // cannot be persisted must not become publishable, or the first publish
        // would be acknowledged against a guarantee that does not exist.
        let durable = self
            .open_durable_log(&key.tenant_id, &key.namespace, &key.stream, &metadata)
            .await?;

        // Keep the documented registry lock order. Recheck after acquiring the
        // write lock so concurrent first registrations cannot race a durability
        // transition past the fast-path check above.
        let mut streams = self.streams.write().await;
        if let Some(existing) = streams.get(&key)
            && existing.durable != metadata.durable
        {
            return Err(Self::durability_change_error(&key, existing, &metadata));
        }
        let mut topics = self.topics.write().await;
        let handle_id = self.next_stream_handle.fetch_add(1, Ordering::Relaxed);
        let state = topics
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(StreamState::new(
                    handle_id,
                    self.topic_capacity,
                    self.subscriber_queue_policy,
                    durable,
                ))
            })
            .clone();
        streams.insert(key, metadata);
        drop(topics);
        drop(streams);

        // A durable stream that recovered records keeps counting cursors from
        // where the log left off, and refills its replay ring from disk so a
        // subscriber's pre-restart cursor still resolves to the same records.
        // Reading only the ring's worth of tail keeps this bounded: startup
        // cost does not grow with the size of the log.
        if let Some(log) = &state.durable {
            let tail = log.tail_offset().await?;
            let from = tail.saturating_sub(self.log_capacity as u64);
            let recent = log.read_from(from, HYDRATE_MAX_BYTES).await?;
            state.hydrate(recent, tail, self.log_capacity);
        }
        Ok(())
    }

    /// Open the disk log for a stream, or `None` when it is not durable.
    async fn open_durable_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        metadata: &StreamMetadata,
    ) -> Result<Option<crate::durable::StreamLog>> {
        if !metadata.durable {
            return Ok(None);
        }
        let Some(storage) = &self.durable_storage else {
            return Err(BrokerError::DurableStorageNotConfigured {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        };
        // The broker keeps one log per stream today. `metadata.shards` is
        // carried through to the shard key so that when the data path is
        // sharded, existing single-shard directories keep their identity.
        Ok(Some(storage.open_stream(tenant_id, namespace, stream, 0)?))
    }

    pub async fn register_cache(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
        metadata: CacheMetadata,
    ) -> Result<()> {
        // Fast-path guard: reject unknown scopes before attempting to create the cache.
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        let cache = cache.into();
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let namespace_key = NamespaceKey::new(tenant_id.clone(), namespace.clone());
        if !self.namespaces.read().await.contains_key(&namespace_key) {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id,
                namespace,
            });
        }
        let key = CacheKey::new(tenant_id, namespace, cache);
        self.caches.write().await.insert(key, metadata);
        Ok(())
    }

    fn durability_change_error(
        key: &StreamKey,
        current: &StreamMetadata,
        requested: &StreamMetadata,
    ) -> BrokerError {
        BrokerError::DurabilityChangeRequiresRecreate {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            current: current.durable,
            requested: requested.durable,
        }
    }

    pub async fn remove_cache(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
    ) -> Result<bool> {
        // Fast-path guard: reject unknown scopes before attempting to remove the cache.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id.to_string()));
        }
        self.assert_namespace_exists(tenant_id, namespace).await?;
        let key = CacheKey::new(tenant_id, namespace, cache);
        Ok(self.caches.write().await.remove(&key).is_some())
    }

    pub async fn remove_stream(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<bool> {
        // Fast-path guard: reject unknown scopes before attempting to remove the stream.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id.to_string()));
        }
        // Fast-path guard: reject unknown namespace.
        self.assert_namespace_exists(tenant_id, namespace).await?;
        let key = StreamKey::new(tenant_id, namespace, stream);
        let removed = self.streams.write().await.remove(&key).is_some();
        if removed {
            let mut topics = self.topics.write().await;
            if let Some(state) = topics.get(&key) {
                state.deactivate();
            }
            topics.remove(&key);
        }
        Ok(removed)
    }

    pub async fn stream_exists(&self, tenant_id: &str, namespace: &str, stream: &str) -> bool {
        // Scope checks are in-memory and intended for per-request enforcement.
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return false;
        }
        self.streams
            .read()
            .await
            .contains_key(&StreamKeyRef::new(tenant_id, namespace, stream))
    }

    pub async fn cache_exists(&self, tenant_id: &str, namespace: &str, cache: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return false;
        }
        self.caches
            .read()
            .await
            .contains_key(&CacheKeyRef::new(tenant_id, namespace, cache))
    }

    pub async fn namespace_exists(&self, tenant_id: &str, namespace: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        self.namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
    }

    pub async fn register_tenant(&self, tenant_id: impl Into<String>) -> Result<bool> {
        let tenant_id = tenant_id.into();
        let mut guard = self.tenants.write().await;
        // Fast-path guard: no-op if tenant already exists.
        if guard.contains_key(&tenant_id) {
            return Ok(false);
        }
        guard.insert(tenant_id, ());
        Ok(true)
    }

    pub async fn remove_tenant(&self, tenant_id: &str) -> Result<bool> {
        let mut guard = self.tenants.write().await;
        // Fast-path guard: no-op if tenant doesn't even exist.
        if !guard.contains_key(tenant_id) {
            return Ok(false);
        }
        Ok(guard.remove(tenant_id).is_some())
    }

    pub async fn register_namespace(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<bool> {
        let tenant_id = tenant_id.into();
        let namespace = namespace.into();
        // Fast-path guard: reject unknown tenant before creating namespace.
        if !self.tenants.read().await.contains_key(&tenant_id) {
            return Err(BrokerError::TenantNotFound(tenant_id));
        }
        let key = NamespaceKey::new(tenant_id, namespace);
        let mut guard = self.namespaces.write().await;
        // Fast-path guard: no-op if namespace already exists.
        if guard.contains_key(&key) {
            return Ok(false);
        }
        guard.insert(key, ());
        Ok(true)
    }

    pub async fn remove_namespace(&self, tenant_id: &str, namespace: &str) -> Result<bool> {
        let key = NamespaceKey::new(tenant_id, namespace);
        let mut guard = self.namespaces.write().await;
        // Fast-path guard: no-op if namespace doesn't even exist.
        if !guard.contains_key(&key) {
            return Ok(false);
        }
        Ok(guard.remove(&key).is_some())
    }

    pub(crate) async fn get_stream_state(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let guard = self.topics.read().await;
        #[cfg(feature = "perf_debug")]
        {
            let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
            metrics::histogram!("felix_perf_topics_read_lock_wait_ns").record(wait_ns as f64);
        }
        guard
            .get(&StreamKeyRef::new(tenant_id, namespace, stream))
            .cloned()
            .ok_or_else(|| BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            })
    }

    pub async fn resolve_stream_handle(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<StreamHandle> {
        let state = self.get_stream_state(tenant_id, namespace, stream).await?;
        if !state.active.load(Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(state.handle_id));
        }
        Ok(StreamHandle { state })
    }

    /// It is up to the caller to check for the error or not.
    async fn assert_namespace_exists(&self, tenant_id: &str, namespace: &str) -> Result<()> {
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKeyRef::new(tenant_id, namespace))
        {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
            });
        }

        Ok(())
    }

    // get_or_create_topic removed; stream creation handled inline for cursor/log support.
}
