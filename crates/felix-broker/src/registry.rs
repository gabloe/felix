// Tenant / namespace / stream / cache registries.
//
// These maps mirror control-plane state and gate every data-path operation. Lock
// order is tenants -> namespaces -> streams -> topics; keep it that way.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::broker::{Broker, CacheMetadata, StreamHandle, StreamMetadata};
use crate::error::{BrokerError, Result};
use crate::keys::{
    CacheKey, CacheKeyRef, NamespaceKey, NamespaceKeyRef, StreamKey, StreamKeyRef, TopicKey,
    TopicKeyRef,
};
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

        // Everything fallible happens before the stream is registered.
        //
        // Opening the log and refilling the replay ring both read from disk and
        // both can fail. Registering first and hydrating after left a window
        // where a durable stream was already publishable with an empty ring: a
        // hydration failure meant the stream existed, accepted writes, and
        // answered `CursorTooOld` for every pre-restart cursor, with nothing
        // upstream aware that it was never initialised.
        //
        // The loop exists because the state has to be built without holding the
        // registry locks — hydration awaits — so another registration can win
        // the race in between. The retry then takes the fast path.
        loop {
            // Already live: nothing to build, just refresh the metadata.
            //
            // Registration opens shard 0 only. A broker opens the other shards
            // of a stream when it is asked to serve one, because it may own any
            // subset of them and opening all of them would create a log per
            // shard on every broker in the cluster.
            let topic = TopicKey::new(&key.tenant_id, &key.namespace, &key.stream, 0);
            if let Some(state) = self.topics.read().await.get(&topic).cloned() {
                let mut streams = self.streams.write().await;
                if let Some(existing) = streams.get(&key)
                    && existing.durable != metadata.durable
                {
                    return Err(Self::durability_change_error(&key, existing, &metadata));
                }
                // The live state carries the consistency the publish path reads,
                // so refreshing the map alone left a raised stream acknowledging
                // on the leader while the catalog said a majority was required —
                // until the broker restarted.
                state.set_consistency(metadata.consistency);
                streams.insert(key, metadata);
                return Ok(());
            }

            let durable = self
                .open_durable_log(&key.tenant_id, &key.namespace, &key.stream, 0, &metadata)
                .await?;
            let handle_id = self.next_stream_handle.fetch_add(1, Ordering::Relaxed);
            let state = Arc::new(StreamState::new(
                handle_id,
                self.topic_capacity,
                self.subscriber_queue_policy,
                durable,
                metadata.consistency,
            ));
            self.hydrate_durable_stream(&state).await?;

            // Keep the documented registry lock order: streams before topics.
            let mut streams = self.streams.write().await;
            if let Some(existing) = streams.get(&key)
                && existing.durable != metadata.durable
            {
                return Err(Self::durability_change_error(&key, existing, &metadata));
            }
            let mut topics = self.topics.write().await;
            if topics.contains_key(&topic) {
                // Lost the race while hydrating; the winner's state is the live
                // one, so discard this one and take the fast path.
                drop(topics);
                drop(streams);
                continue;
            }
            topics.insert(topic, state);
            streams.insert(key.clone(), metadata);
            return Ok(());
        }
    }

    /// Refill a durable stream's replay ring from disk before it is published.
    ///
    /// A durable stream that recovered records keeps counting cursors from
    /// where the log left off, so a subscriber's pre-restart cursor still
    /// resolves to the same records.
    ///
    /// The ring must end at the tail. A single read is bounded by the storage
    /// layer's per-read record and byte caps, so asking for the ring's worth of
    /// history can come back short — and keeping that *earliest* prefix while
    /// still advancing `next_seq` to the real tail leaves a hole. A cursor
    /// pointing into the hole is then above the ring's oldest entry, so it is
    /// accepted rather than rejected, and replay answers with silence: the
    /// subscriber reads "you are caught up" while records are missing. Losing
    /// history loudly is fine; losing it quietly is not.
    ///
    /// So this pages forward to the tail and keeps the newest `log_capacity`
    /// records, dropping older ones as it goes. Memory stays bounded by the
    /// ring, and whatever the ring ends up holding is contiguous with
    /// `next_seq`. If the budget cannot cover even part of the window the ring
    /// is left empty, and every pre-restart cursor gets `CursorTooOld` — the
    /// honest answer.
    async fn hydrate_durable_stream(&self, state: &Arc<StreamState>) -> Result<()> {
        let Some(log) = &state.durable else {
            return Ok(());
        };
        let tail = log.tail_offset().await?;
        let capacity = self.log_capacity;
        let mut cursor = tail.saturating_sub(capacity as u64);
        let mut window: VecDeque<felix_storage::log::LogRecord> = VecDeque::new();
        let mut bytes = 0usize;

        while cursor < tail {
            let page = log.read_from(cursor, HYDRATE_MAX_BYTES).await?;
            let Some(last) = page.last() else {
                // The tail moved out from under the read, or the range was
                // trimmed. Stop with what is in hand rather than spinning.
                break;
            };
            cursor = last.offset + 1;
            for record in page {
                bytes += record.payload.len();
                window.push_back(record);
                // Keep the *newest* end of the window under both bounds.
                while window.len() > capacity || (bytes > HYDRATE_MAX_BYTES && window.len() > 1) {
                    if let Some(dropped) = window.pop_front() {
                        bytes -= dropped.payload.len();
                    }
                }
            }
        }

        // Only hand over a window that actually reaches the tail. Anything else
        // would reintroduce the hole this method exists to avoid.
        let reaches_tail = window.back().is_some_and(|last| last.offset + 1 == tail);
        let contiguous = if reaches_tail {
            window.into_iter().collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        state.hydrate(contiguous, tail, capacity);
        Ok(())
    }

    /// Open the disk log for a stream, or `None` when it is not durable.
    async fn open_durable_log(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
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
        // One log per shard. Shard 0 keeps the directory a single-shard stream
        // has always had, so nothing needs migrating; every other shard gets its
        // own, which is what the replication driver has always opened.
        Ok(Some(
            storage.open_stream(tenant_id, namespace, stream, shard)?,
        ))
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
            // Every shard of the stream, not just the one registration opened:
            // a broker may have been asked to serve several.
            topics.retain(|topic, state| {
                let same_stream = topic.tenant_id == key.tenant_id
                    && topic.namespace == key.namespace
                    && topic.stream == key.stream;
                if same_stream {
                    state.deactivate();
                }
                !same_stream
            });
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
        shard: u32,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let found = {
            let guard = self.topics.read().await;
            #[cfg(feature = "perf_debug")]
            {
                let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
                metrics::histogram!("felix_perf_topics_read_lock_wait_ns").record(wait_ns as f64);
            }
            guard
                .get(&TopicKeyRef::new(tenant_id, namespace, stream, shard))
                .cloned()
        };
        if let Some(state) = found {
            return Ok(state);
        }
        // A shard this broker has not opened yet. Registration opens shard 0;
        // any other arrives here the first time the broker is asked to serve
        // it, because ownership is decided by the control plane long after the
        // stream was registered and a broker may hold any subset.
        self.open_stream_shard(tenant_id, namespace, stream, shard)
            .await
    }

    /// Build, hydrate and install the state for one shard.
    ///
    /// Hydration happens **before** the state is visible, for the same reason
    /// registration does it in that order: a durable shard that is publishable
    /// with an empty replay ring answers `CursorTooOld` for every pre-existing
    /// cursor, and nothing upstream would know it was never initialised.
    async fn open_stream_shard(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        let stream_key = StreamKeyRef::new(tenant_id, namespace, stream);
        let metadata = self.streams.read().await.get(&stream_key).cloned().ok_or(
            BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            },
        )?;
        if shard >= metadata.shards.max(1) {
            // Asking for a shard the stream does not have is a routing bug, and
            // opening a log for it would create a directory nothing will ever
            // read.
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: format!("{stream} (shard {shard} of {})", metadata.shards),
            });
        }

        let topic = TopicKey::new(tenant_id, namespace, stream, shard);
        let durable = self
            .open_durable_log(tenant_id, namespace, stream, shard, &metadata)
            .await?;
        let handle_id = self.next_stream_handle.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(StreamState::new(
            handle_id,
            self.topic_capacity,
            self.subscriber_queue_policy,
            durable,
            metadata.consistency,
        ));
        self.hydrate_durable_stream(&state).await?;

        let mut topics = self.topics.write().await;
        // No retry loop: a caller that lost the race takes the winner's state
        // rather than building another, so there is nothing to go round for.
        if let Some(existing) = topics.get(&topic) {
            return Ok(Arc::clone(existing));
        }
        topics.insert(topic, Arc::clone(&state));
        Ok(state)
    }

    pub async fn resolve_stream_handle(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        shard: u32,
    ) -> Result<StreamHandle> {
        let state = self
            .get_stream_state(tenant_id, namespace, stream, shard)
            .await?;
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
