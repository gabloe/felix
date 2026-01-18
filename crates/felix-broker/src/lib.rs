// In-process pub/sub broker with a tiny cache hook.
use bytes::Bytes;
use felix_storage::EphemeralCache;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, broadcast};

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
    sender: broadcast::Sender<Bytes>,
    log_state: Mutex<LogState>,
}

#[derive(Debug)]
struct LogState {
    log: VecDeque<LogEntry>,
    next_seq: u64,
}

impl StreamState {
    fn new(topic_capacity: usize) -> Self {
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
        let mut state = self.log_state.lock().expect("log lock");
        let seq = state.next_seq;
        state.next_seq += 1;
        state.log.push_back(LogEntry { seq, payload });
        while state.log.len() > log_capacity {
            state.log.pop_front();
        }
        seq
    }

    fn snapshot_from(&self, from_seq: u64) -> (u64, Vec<Bytes>) {
        let state = self.log_state.lock().expect("log lock");
        let oldest = state
            .log
            .front()
            .map(|entry| entry.seq)
            .unwrap_or(state.next_seq);
        let backlog = state
            .log
            .iter()
            .filter(|entry| entry.seq >= from_seq)
            .map(|entry| entry.payload.clone())
            .collect();
        (oldest, backlog)
    }

    fn tail_seq(&self) -> u64 {
        let state = self.log_state.lock().expect("log lock");
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
/// let broker = Broker::new(EphemeralCache::new());
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
    // Map of tenant id -> active marker.
    tenants: RwLock<HashMap<String, ()>>,
    // Map of namespace key -> active marker.
    namespaces: RwLock<HashMap<NamespaceKey, ()>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: EphemeralCache,
    // Broadcast channel capacity for each topic.
    topic_capacity: usize,
    // Per-topic in-memory log capacity.
    log_capacity: usize,
}

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
    pub fn new(cache: EphemeralCache) -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
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
        if !self.stream_exists(tenant_id, namespace, stream).await {
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        }
        // Fan-out to current subscribers; ignore backpressure errors for now.
        let stream_state = {
            let guard = self.topics.read().await;
            guard
                .get(&StreamKey::new(tenant_id, namespace, stream))
                .cloned()
        }
        .ok_or_else(|| BrokerError::StreamNotFound {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        })?;
        stream_state.append(payload.clone(), self.log_capacity);
        Ok(stream_state.sender.send(payload).unwrap_or(0))
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<broadcast::Receiver<Bytes>> {
        if !self.stream_exists(tenant_id, namespace, stream).await {
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        }
        let stream_state = {
            let guard = self.topics.read().await;
            guard
                .get(&StreamKey::new(tenant_id, namespace, stream))
                .cloned()
        }
        .ok_or_else(|| BrokerError::StreamNotFound {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        })?;
        Ok(stream_state.sender.subscribe())
    }

    pub async fn cursor_tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Cursor> {
        if !self.stream_exists(tenant_id, namespace, stream).await {
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        }
        let stream_state = {
            let guard = self.topics.read().await;
            guard
                .get(&StreamKey::new(tenant_id, namespace, stream))
                .cloned()
        }
        .ok_or_else(|| BrokerError::StreamNotFound {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        })?;
        Ok(Cursor {
            next_seq: stream_state.tail_seq(),
        })
    }

    pub async fn subscribe_with_cursor(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        cursor: Cursor,
    ) -> Result<(Vec<Bytes>, broadcast::Receiver<Bytes>)> {
        if !self.stream_exists(tenant_id, namespace, stream).await {
            return Err(BrokerError::StreamNotFound {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            });
        }
        let stream_state = {
            let guard = self.topics.read().await;
            guard
                .get(&StreamKey::new(tenant_id, namespace, stream))
                .cloned()
        }
        .ok_or_else(|| BrokerError::StreamNotFound {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
        })?;
        let (oldest, backlog) = stream_state.snapshot_from(cursor.next_seq);
        if cursor.next_seq < oldest {
            return Err(BrokerError::CursorTooOld {
                oldest,
                requested: cursor.next_seq,
            });
        }
        Ok((backlog, stream_state.sender.subscribe()))
    }

    pub fn cache(&self) -> &EphemeralCache {
        // Expose the cache for demos and integrations.
        &self.cache
    }

    pub async fn register_stream(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
        stream: impl Into<String>,
        metadata: StreamMetadata,
    ) -> Result<()> {
        let key = StreamKey::new(tenant_id, namespace, stream);
        self.streams.write().await.insert(key.clone(), metadata);
        let mut guard = self.topics.write().await;
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(StreamState::new(self.topic_capacity)));
        Ok(())
    }

    pub async fn remove_stream(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<bool> {
        let key = StreamKey::new(tenant_id, namespace, stream);
        let removed = self.streams.write().await.remove(&key).is_some();
        if removed {
            self.topics.write().await.remove(&key);
        }
        Ok(removed)
    }

    pub async fn stream_exists(&self, tenant_id: &str, namespace: &str, stream: &str) -> bool {
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

    pub async fn register_tenant(&self, tenant_id: impl Into<String>) -> Result<()> {
        self.tenants.write().await.insert(tenant_id.into(), ());
        Ok(())
    }

    pub async fn remove_tenant(&self, tenant_id: &str) -> Result<bool> {
        Ok(self.tenants.write().await.remove(tenant_id).is_some())
    }

    pub async fn register_namespace(
        &self,
        tenant_id: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<()> {
        let key = NamespaceKey::new(tenant_id, namespace);
        self.namespaces.write().await.insert(key, ());
        Ok(())
    }

    pub async fn remove_namespace(&self, tenant_id: &str, namespace: &str) -> Result<bool> {
        let key = NamespaceKey::new(tenant_id, namespace);
        Ok(self.namespaces.write().await.remove(&key).is_some())
    }

    // get_or_create_topic removed; stream creation handled inline for cursor/log support.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        // Basic pub/sub flow with a single subscriber.
        let broker = Broker::new(EphemeralCache::new());
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
        let broker = Broker::new(EphemeralCache::new());
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
    async fn multiple_subscribers_receive_payload() {
        let broker = Broker::new(EphemeralCache::new());
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
        let broker = Broker::new(EphemeralCache::new());
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
        let broker = Broker::new(EphemeralCache::new());
        let err = broker.with_topic_capacity(0).expect_err("capacity");
        assert!(matches!(err, BrokerError::CapacityTooLarge));
    }
}
