// In-process pub/sub broker with a tiny cache hook.
// The broker enforces tenant/namespace/stream existence via local registries
// that are kept in sync by the control plane watcher.
use bytes::Bytes;
use felix_storage::StorageApi;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{RwLock, broadcast};

pub mod timings;

pub type Result<T> = std::result::Result<T, BrokerError>;

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("topic capacity too large")]
    CapacityTooLarge,
    #[error("cursor too old (oldest {oldest}, requested {requested})")]
    CursorTooOld { oldest: u64, requested: u64 },
    #[error("stream not found: tenant={tenant_id} namespace={namespace} stream={stream}")]
    StreamNotFound {
        tenant_id: String,
        namespace: String,
        stream: String,
    },
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("namespace not found: tenant={tenant_id} namespace={namespace}")]
    NamespaceNotFound {
        tenant_id: String,
        namespace: String,
    },
}

const DEFAULT_TOPIC_CAPACITY: usize = 1024;
const DEFAULT_LOG_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    next_seq: u64,
}

impl Cursor {
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[derive(Debug)]
struct LogEntry {
    seq: u64,
    payload: Bytes,
}

#[derive(Debug)]
struct StreamState {
    // Broadcast channel for live subscribers.
    sender: broadcast::Sender<Bytes>,
    // In-memory log for cursor-based replay.
    log_state: Mutex<LogState>,
}

#[derive(Debug)]
struct LogState {
    // Bounded log; oldest entries are dropped as new ones arrive.
    log: VecDeque<LogEntry>,
    // Next sequence number to assign.
    next_seq: u64,
}

impl StreamState {
    fn new(topic_capacity: usize) -> Self {
        // We throw away the receiver because you should subscribe to the
        // sender. You can see this in the subscribe and subscribe_with_cursor
        // functions.
        let (sender, _) = broadcast::channel(topic_capacity);
        Self {
            sender,
            log_state: Mutex::new(LogState {
                log: VecDeque::new(),
                next_seq: 0,
            }),
        }
    }

    fn append(&self, payload: Bytes, log_capacity: usize) -> u64 {
        let mut validate_data = false;
        let mut state = self.log_state.lock().unwrap_or_else(|poisoned| {
            #[cfg(debug_assertions)]
            println!("The lock was poisoned, attempting to fix data corruption.");
            validate_data = true;
            poisoned.into_inner()
        });

        let seq = state.next_seq;

        // TODO : Figure out if this is enough, or even correct.
        // Can only validate the data if we have some.
        if validate_data && !state.log.is_empty() {
            let top_seq = state.log.back().unwrap().seq;
            if top_seq < seq {
                // We updated the next_seq before the push, we could leave it but, it would leave
                // a potential gap if we ever flush to persistent storage.
                state.next_seq -= 1;
            } else if top_seq > seq {
                // This seems impossible to me.
                panic!("seq {seq} is less than the current items seq {top_seq}")
            }
        }

        state.next_seq += 1;
        state.log.push_back(LogEntry { seq, payload });
        // Keep only the newest entries up to the configured capacity.
        while state.log.len() > log_capacity {
            state.log.pop_front();
        }
        seq
    }

    fn snapshot_range(&self, from_seq: u64, to_seq: u64) -> (u64, Vec<Bytes>) {
        let state = self.log_state.lock().expect("log lock");

        // We return this to let the caller know if they need to indicate
        // they are requesting entries which are too far back in time.
        let oldest = state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq);

        let backlog = state
            .log
            .iter()
            .filter(|entry| entry.seq >= from_seq && entry.seq <= to_seq)
            .map(|entry| entry.payload.clone())
            .collect();
        (oldest, backlog)
    }

    fn snapshot_from(&self, from_seq: u64) -> (u64, Vec<Bytes>) {
        self.snapshot_range(from_seq, u64::MAX)
    }

    fn tail_seq(&self) -> u64 {
        let state = self.log_state.lock().expect("log lock");
        // The cursor tail points to the next sequence to be published.
        state.next_seq
    }
}

/// In-process broker for pub/sub messaging.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_storage::EphemeralCache;
///
/// let broker = Broker::new(EphemeralCache::new().into());
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     broker
///         .register_tenant("t1")
///         .await
///         .expect("tenant");
///     broker
///         .register_namespace("t1", "default")
///         .await
///         .expect("namespace");
///     broker
///         .register_stream("t1", "default", "topic", Default::default())
///         .await
///         .expect("register");
///     let mut sub = broker
///         .subscribe("t1", "default", "topic")
///         .await
///         .expect("subscribe");
///     broker
///         .publish("t1", "default", "topic", Bytes::from_static(b"hello"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"hello"));
/// });
/// ```
#[derive(Debug)]
pub struct Broker {
    // Map of stream key -> stream state (broadcast + log).
    topics: RwLock<HashMap<StreamKey, Arc<StreamState>>>,
    // Map of stream key -> metadata for existence checks.
    streams: RwLock<HashMap<StreamKey, StreamMetadata>>,
    // Map of cache key -> metadata for existence checks.
    caches: RwLock<HashMap<CacheKey, CacheMetadata>>,
    // Map of tenant id -> active marker.
    tenants: RwLock<HashMap<String, ()>>,
    // Map of namespace key -> active marker.
    namespaces: RwLock<HashMap<NamespaceKey, ()>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: Box<dyn StorageApi + Send>,
    // Broadcast channel capacity for each topic.
    topic_capacity: usize,
    // Per-topic in-memory log capacity.
    log_capacity: usize,
}

unsafe impl Send for Broker {}

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub durable: bool,
    pub shards: u32,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

impl NamespaceKey {
    pub fn new(tenant_id: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

impl StreamKey {
    pub fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            stream: stream.into(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
}

impl CacheKey {
    pub fn new(
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        cache: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            namespace: namespace.into(),
            cache: cache.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheMetadata;

impl Default for StreamMetadata {
    fn default() -> Self {
        Self {
            durable: false,
            shards: 1,
        }
    }
}

impl Broker {
    // Start with an empty topic table and default capacity.
    pub fn new(cache: Box<dyn StorageApi + Send>) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            caches: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            namespaces: RwLock::new(HashMap::new()),
            cache,
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            log_capacity: DEFAULT_LOG_CAPACITY,
        }
    }

    pub fn with_topic_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        // Keep a single capacity value so new topics match existing ones.
        self.topic_capacity = capacity;
        Ok(self)
    }

    pub fn with_log_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(BrokerError::CapacityTooLarge);
        }
        self.log_capacity = capacity;
        Ok(self)
    }

    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        let payloads = [payload];
        self.publish_batch(tenant_id, namespace, stream, &payloads)
            .await
    }

    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Bytes],
    ) -> Result<usize> {
        self.assert_stream_exists(tenant_id, namespace, stream)
            .await?;

        // Fan-out to current subscribers.
        // TODO: handle backpressure issues gracefully. Too much backpressure will cause performance degradation and possibly send errors.
        let sample = timings::should_sample();
        let lookup_start = sample.then(Instant::now);
        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;

        if let Some(start) = lookup_start {
            let lookup_ns = start.elapsed().as_nanos() as u64;
            timings::record_lookup_ns(lookup_ns);
            metrics::histogram!("broker_publish_lookup_ns").record(lookup_ns as f64);
        }

        let append_start = sample.then(Instant::now);
        // Append to the in-memory log first so cursors can replay.
        for payload in payloads {
            stream_state.append(payload.clone(), self.log_capacity);
        }

        if let Some(start) = append_start {
            let append_ns = start.elapsed().as_nanos() as u64;
            timings::record_append_ns(append_ns);
            metrics::histogram!("broker_publish_append_ns").record(append_ns as f64);
        }

        let send_start = sample.then(Instant::now);
        let mut sent = 0usize;
        // Broadcast to current subscribers; lagging receivers may drop.
        for payload in payloads {
            sent += stream_state.sender.send(payload.clone()).unwrap_or(0);
        }
        if let Some(start) = send_start {
            let send_ns = start.elapsed().as_nanos() as u64;
            timings::record_send_ns(send_ns);
            metrics::histogram!("broker_publish_send_ns").record(send_ns as f64);
        }
        Ok(sent)
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<broadcast::Receiver<Bytes>> {
        // Fast-path guard: reject unknown scopes before opening a receiver.
        self.assert_stream_exists(tenant_id, namespace, stream)
            .await?;

        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;
        Ok(stream_state.sender.subscribe())
    }

    // Return a cursor positioned at the tail of the stream log.
    pub async fn cursor_tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Cursor> {
        // Fast-path guard: reject unknown scopes before touching the stream map.
        self.assert_stream_exists(tenant_id, namespace, stream)
            .await?;
        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;

        Ok(Cursor {
            next_seq: stream_state.tail_seq(),
        })
    }

    /// Allows us to subscribe from a previous point in time. If that point in time
    /// is too far back we return an error.
    pub async fn subscribe_with_cursor(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        cursor: Cursor,
    ) -> Result<(Vec<Bytes>, broadcast::Receiver<Bytes>)> {
        // Fast-path guard: reject unknown scopes before touching the stream map.
        self.assert_stream_exists(tenant_id, namespace, stream)
            .await?;

        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;

        let (oldest, backlog) = stream_state.snapshot_from(cursor.next_seq);
        // TODO: Should we really return an error? Would this not just be all entries?
        if cursor.next_seq < oldest {
            return Err(BrokerError::CursorTooOld {
                oldest,
                requested: cursor.next_seq,
            });
        }
        // Return backlog for replay plus a live subscription for new events.
        Ok((backlog, stream_state.sender.subscribe()))
    }

    pub fn cache(&self) -> &(dyn StorageApi + Send) {
        // Expose the cache for demos and integrations.
        self.cache.as_ref()
    }

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
        self.streams.write().await.insert(key.clone(), metadata);
        let mut guard = self.topics.write().await;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(StreamState::new(self.topic_capacity)));
        Ok(())
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
        let namespace_key = NamespaceKey::new(tenant_id, namespace);
        self.assert_namespace_exists(tenant_id, namespace, namespace_key)
            .await?;
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
        let namespace_key = NamespaceKey::new(tenant_id, namespace);
        // Fast-path guard: reject unknown namespace.
        self.assert_namespace_exists(tenant_id, namespace, namespace_key)
            .await?;
        let key = StreamKey::new(tenant_id, namespace, stream);
        let removed = self.streams.write().await.remove(&key).is_some();
        if removed {
            self.topics.write().await.remove(&key);
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
            .contains_key(&NamespaceKey::new(tenant_id, namespace))
        {
            return false;
        }
        self.streams
            .read()
            .await
            .contains_key(&StreamKey::new(tenant_id, namespace, stream))
    }

    pub async fn cache_exists(&self, tenant_id: &str, namespace: &str, cache: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        if !self
            .namespaces
            .read()
            .await
            .contains_key(&NamespaceKey::new(tenant_id, namespace))
        {
            return false;
        }
        self.caches
            .read()
            .await
            .contains_key(&CacheKey::new(tenant_id, namespace, cache))
    }

    pub async fn namespace_exists(&self, tenant_id: &str, namespace: &str) -> bool {
        if !self.tenants.read().await.contains_key(tenant_id) {
            return false;
        }
        self.namespaces
            .read()
            .await
            .contains_key(&NamespaceKey::new(tenant_id, namespace))
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

    async fn get_stream_state(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> std::result::Result<Arc<StreamState>, BrokerError> {
        let guard = self.topics.read().await;
        guard
            .get(&StreamKey::new(tenant_id, namespace, stream))
            .cloned()
            .ok_or_else(|| BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            })
    }

    /// It is up to the caller to check for the error or not.
    async fn assert_stream_exists(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<()> {
        if !self.stream_exists(tenant_id, namespace, stream).await {
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        }

        Ok(())
    }

    /// It is up to the caller to check for the error or not.
    async fn assert_namespace_exists(
        &self,
        tenant_id: &str,
        namespace: &str,
        namespace_key: NamespaceKey,
    ) -> Result<()> {
        if !self.namespaces.read().await.contains_key(&namespace_key) {
            return Err(BrokerError::NamespaceNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
            });
        }

        Ok(())
    }

    // get_or_create_topic removed; stream creation handled inline for cursor/log support.
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        // Basic pub/sub flow with a single subscriber.
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "orders")
            .await
            .expect("subscribe");
        broker
            .publish("t1", "default", "orders", Bytes::from_static(b"hello"))
            .await
            .expect("publish");
        let msg = sub.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn publish_without_subscribers_returns_zero() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "empty", StreamMetadata::default())
            .await
            .expect("register");
        let delivered = broker
            .publish("t1", "default", "empty", Bytes::from_static(b"payload"))
            .await
            .expect("publish");
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn stream_delivers_in_order_to_single_subscriber() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "ordered", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "ordered")
            .await
            .expect("subscribe");
        broker
            .publish("t1", "default", "ordered", Bytes::from_static(b"one"))
            .await
            .expect("publish");
        broker
            .publish("t1", "default", "ordered", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
    }

    #[tokio::test]
    async fn lagging_subscriber_drops_messages() {
        let broker = Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(1)
            .expect("capacity");
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "laggy", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "laggy")
            .await
            .expect("subscribe");
        broker
            .publish("t1", "default", "laggy", Bytes::from_static(b"one"))
            .await
            .expect("publish");
        broker
            .publish("t1", "default", "laggy", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        match sub.recv().await {
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            other => panic!("expected lagged error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_payload() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub_a = broker
            .subscribe("t1", "default", "orders")
            .await
            .expect("subscribe");
        let mut sub_b = broker
            .subscribe("t1", "default", "orders")
            .await
            .expect("subscribe");
        broker
            .publish("t1", "default", "orders", Bytes::from_static(b"fanout"))
            .await
            .expect("publish");
        assert_eq!(
            sub_a.recv().await.expect("recv"),
            Bytes::from_static(b"fanout")
        );
        assert_eq!(
            sub_b.recv().await.expect("recv"),
            Bytes::from_static(b"fanout")
        );
    }

    #[tokio::test]
    async fn cursor_replays_log_then_streams_new_events() {
        let broker = Broker::new(EphemeralCache::new().into());

        broker.register_tenant("t1").await.expect("tenant");

        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("register");
        let cursor = broker
            .cursor_tail("t1", "default", "orders")
            .await
            .expect("cursor");
        broker
            .publish("t1", "default", "orders", Bytes::from_static(b"one"))
            .await
            .expect("publish");
        broker
            .publish("t1", "default", "orders", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        let (backlog, mut sub) = broker
            .subscribe_with_cursor("t1", "default", "orders", cursor)
            .await
            .expect("subscribe");
        assert_eq!(
            backlog,
            vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")]
        );
        broker
            .publish("t1", "default", "orders", Bytes::from_static(b"three"))
            .await
            .expect("publish");
        assert_eq!(
            sub.recv().await.expect("recv"),
            Bytes::from_static(b"three")
        );
    }

    #[test]
    fn zero_capacity_is_rejected() {
        let broker = Broker::new(EphemeralCache::new().into());
        let err = broker.with_topic_capacity(0).expect_err("capacity");
        assert!(matches!(err, BrokerError::CapacityTooLarge));
    }

    #[tokio::test]
    async fn existence_checks_reflect_registrations() {
        let broker = Broker::new(EphemeralCache::new().into());
        assert!(!broker.namespace_exists("t1", "default").await);
        assert!(!broker.cache_exists("t1", "default", "primary").await);
        assert!(!broker.stream_exists("t1", "default", "orders").await);

        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_cache("t1", "default", "primary", CacheMetadata)
            .await
            .expect("cache");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("stream");

        assert!(broker.namespace_exists("t1", "default").await);
        assert!(broker.cache_exists("t1", "default", "primary").await);
        assert!(broker.stream_exists("t1", "default", "orders").await);
    }

    #[tokio::test]
    async fn register_namespace_requires_tenant() {
        let broker = Broker::new(EphemeralCache::new().into());
        let err = broker
            .register_namespace("missing", "default")
            .await
            .expect_err("tenant");
        assert!(matches!(err, BrokerError::TenantNotFound(_)));
    }
}
