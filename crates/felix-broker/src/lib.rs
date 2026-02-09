// In-process pub/sub broker with a tiny cache hook.
// The broker enforces tenant/namespace/stream existence via local registries
// that are kept in sync by the control plane watcher.
use ahash::RandomState;
use arc_swap::ArcSwap;
use bytes::Bytes;
use felix_storage::StorageApi;
use hashbrown::HashMap;
use parking_lot::Mutex;
use slab::Slab;
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};

pub mod timings;

pub type Result<T> = std::result::Result<T, BrokerError>;

#[cfg(feature = "telemetry")]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg(not(feature = "telemetry"))]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        NoopHistogram
    };
}

#[cfg(not(feature = "telemetry"))]
#[derive(Copy, Clone)]
struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
impl NoopHistogram {
    fn record(&self, _value: f64) {}
}

#[cfg(feature = "telemetry")]
#[inline]
fn t_should_sample() -> bool {
    timings::should_sample()
}

#[cfg(not(feature = "telemetry"))]
#[inline]
fn t_should_sample() -> bool {
    false
}

#[cfg(feature = "telemetry")]
#[inline]
fn t_now_if(sample: bool) -> Option<Instant> {
    sample.then(Instant::now)
}

#[cfg(not(feature = "telemetry"))]
#[inline]
fn t_now_if(_sample: bool) -> Option<Instant> {
    None
}

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
const DEFAULT_SUB_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
static GLOBAL_SUB_QUEUE_DEPTH: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubQueuePolicy {
    Block,
    DropNew,
    DropOld,
}

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
    // Snapshot used by publish hot path: lock-free read, no per-publish allocation.
    subscribers_snapshot: ArcSwap<Vec<SubscriberEntry>>,
    // Inner registry mutated only on subscribe/unsubscribe paths.
    subscribers: Mutex<SubscriberRegistry>,
    // In-memory log for cursor-based replay.
    log_state: Mutex<LogState>,
    // Per-subscriber bounded queue depth.
    subscriber_queue_capacity: usize,
    // Queue admission policy when subscriber queue is full.
    subscriber_queue_policy: SubQueuePolicy,
    // Approximate number of queued items across subscribers in this stream.
    queued_items: Arc<AtomicUsize>,
}

#[derive(Debug, Default)]
struct SubscriberRegistry {
    senders: Slab<mpsc::Sender<DeliveryEnvelope>>,
}

#[derive(Debug, Clone)]
struct SubscriberEntry {
    id: usize,
    sender: mpsc::Sender<DeliveryEnvelope>,
}

#[derive(Debug, Clone)]
pub struct DeliveryEnvelope {
    payload: Bytes,
    enqueued_at: Instant,
}

impl DeliveryEnvelope {
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    pub fn enqueued_at(&self) -> Instant {
        self.enqueued_at
    }
}

#[derive(Debug)]
struct LogState {
    // Bounded log; oldest entries are dropped as new ones arrive.
    log: VecDeque<LogEntry>,
    // Next sequence number to assign.
    next_seq: u64,
}

impl StreamState {
    fn new(subscriber_queue_capacity: usize, subscriber_queue_policy: SubQueuePolicy) -> Self {
        Self {
            subscribers_snapshot: ArcSwap::from_pointee(Vec::new()),
            subscribers: Mutex::new(SubscriberRegistry::default()),
            log_state: Mutex::new(LogState {
                log: VecDeque::new(),
                next_seq: 0,
            }),
            subscriber_queue_capacity,
            subscriber_queue_policy,
            queued_items: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn register_subscriber(&self) -> (u64, SubscriptionReceiver) {
        let mut state = self.subscribers.lock();
        let (tx, rx) = mpsc::channel(self.subscriber_queue_capacity);
        let id = state.senders.insert(tx);
        self.rebuild_subscriber_snapshot(&state);
        (
            id as u64,
            SubscriptionReceiver::new(rx, Arc::clone(&self.queued_items)),
        )
    }

    fn remove_subscriber(&self, id: u64) {
        let mut state = self.subscribers.lock();
        let id = id as usize;
        if state.senders.contains(id) {
            state.senders.remove(id);
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    fn remove_subscribers(&self, subscriber_ids: &[u64]) {
        let mut state = self.subscribers.lock();
        let mut removed = false;
        for subscriber_id in subscriber_ids {
            let id = *subscriber_id as usize;
            if state.senders.contains(id) {
                state.senders.remove(id);
                removed = true;
            }
        }
        if removed {
            self.rebuild_subscriber_snapshot(&state);
        }
    }

    #[inline]
    fn subscriber_snapshot(&self) -> Arc<Vec<SubscriberEntry>> {
        self.subscribers_snapshot.load_full()
    }

    fn rebuild_subscriber_snapshot(&self, state: &SubscriberRegistry) {
        let mut snapshot = Vec::with_capacity(state.senders.len());
        for (id, sender) in state.senders.iter() {
            snapshot.push(SubscriberEntry {
                id,
                sender: sender.clone(),
            });
        }
        self.subscribers_snapshot.store(Arc::new(snapshot));
    }

    #[cfg(test)]
    fn subscriber_count(&self) -> usize {
        let state = self.subscribers.lock();
        state.senders.len()
    }

    fn append_batch(&self, payloads: &[Bytes], log_capacity: usize) {
        if payloads.is_empty() {
            return;
        }

        // Hot path: one lock per publish batch (instead of per payload).
        #[cfg(feature = "perf_debug")]
        let lock_wait_start = std::time::Instant::now();
        let mut state = self.log_state.lock();
        #[cfg(feature = "perf_debug")]
        {
            let wait_ns = lock_wait_start.elapsed().as_nanos() as u64;
            metrics::histogram!("felix_perf_log_lock_wait_ns").record(wait_ns as f64);
        }
        #[cfg(debug_assertions)]
        {
            // Debug-only invariant check kept outside the append loop.
            if let Some(last) = state.log.back() {
                debug_assert!(last.seq < state.next_seq);
            }
        }

        for payload in payloads {
            let seq = state.next_seq;
            state.next_seq = state
                .next_seq
                .checked_add(1)
                .expect("log sequence overflow");
            state.log.push_back(LogEntry {
                seq,
                payload: payload.clone(),
            });
        }

        // Trim once after append to keep the newest `log_capacity` entries.
        let overflow = state.log.len().saturating_sub(log_capacity);
        if overflow > 0 {
            state.log.drain(..overflow);
        }
    }

    fn snapshot_range(&self, from_seq: u64, to_seq: u64) -> (u64, Vec<Bytes>) {
        let state = self.log_state.lock();

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
        let state = self.log_state.lock();
        // The cursor tail points to the next sequence to be published.
        state.next_seq
    }

    fn increment_queue_depth(&self) {
        let _ = self.queued_items.fetch_add(1, Ordering::Relaxed) + 1;
        let global = GLOBAL_SUB_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("felix_sub_queue_len").set(global as f64);
        metrics::counter!("felix_sub_queue_enqueued_total").increment(1);
    }
}

/// RAII handle that unregisters a stream subscriber on drop.
#[derive(Debug)]
pub struct SubscriptionGuard {
    stream_state: Weak<StreamState>,
    subscriber_id: u64,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(stream_state) = self.stream_state.upgrade() {
            stream_state.remove_subscriber(self.subscriber_id);
        }
    }
}

/// Receiver wrapper that keeps the unsubscribe guard alive for the receiver lifetime.
#[derive(Debug)]
pub struct Subscription {
    receiver: SubscriptionReceiver,
    guard: SubscriptionGuard,
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.receiver
            .recv()
            .await
            .map(DeliveryEnvelope::into_payload)
    }

    pub fn try_recv(&mut self) -> std::result::Result<Bytes, mpsc::error::TryRecvError> {
        self.receiver.try_recv().map(DeliveryEnvelope::into_payload)
    }

    pub fn into_parts(self) -> (SubscriptionReceiver, SubscriptionGuard) {
        (self.receiver, self.guard)
    }
}

#[derive(Debug)]
pub struct SubscriptionReceiver {
    receiver: mpsc::Receiver<DeliveryEnvelope>,
    queued_items: Arc<AtomicUsize>,
}

impl SubscriptionReceiver {
    fn new(receiver: mpsc::Receiver<DeliveryEnvelope>, queued_items: Arc<AtomicUsize>) -> Self {
        Self {
            receiver,
            queued_items,
        }
    }

    pub async fn recv(&mut self) -> Option<DeliveryEnvelope> {
        let value = self.receiver.recv().await?;
        self.decrement_depth(1);
        Some(value)
    }

    pub fn try_recv(&mut self) -> std::result::Result<DeliveryEnvelope, mpsc::error::TryRecvError> {
        match self.receiver.try_recv() {
            Ok(value) => {
                self.decrement_depth(1);
                Ok(value)
            }
            Err(err) => Err(err),
        }
    }

    fn decrement_depth(&self, n: usize) {
        for _ in 0..n {
            if self
                .queued_items
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
                .is_ok()
            {
                metrics::counter!("felix_sub_queue_dequeued_total").increment(1);
            }
            if let Ok(prev) =
                GLOBAL_SUB_QUEUE_DEPTH
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            {
                metrics::gauge!("felix_sub_queue_len").set((prev.saturating_sub(1)) as f64);
            }
        }
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) {
        let remaining = self.receiver.len();
        if remaining > 0 {
            self.decrement_depth(remaining);
        }
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
    // Map of stream key -> stream state (subscriber registry + log).
    topics: RwLock<HashMap<StreamKey, Arc<StreamState>, RandomState>>,
    // Map of stream key -> metadata for existence checks.
    streams: RwLock<HashMap<StreamKey, StreamMetadata, RandomState>>,
    // Map of cache key -> metadata for existence checks.
    caches: RwLock<HashMap<CacheKey, CacheMetadata, RandomState>>,
    // Map of tenant id -> active marker.
    tenants: RwLock<HashMap<String, (), RandomState>>,
    // Map of namespace key -> active marker.
    namespaces: RwLock<HashMap<NamespaceKey, (), RandomState>>,
    // Ephemeral cache used by demos and simple workflows.
    cache: Box<dyn StorageApi + Send>,
    // Per-subscriber queue capacity for each stream.
    topic_capacity: usize,
    // Per-topic in-memory log capacity.
    log_capacity: usize,
    // Subscriber queue backpressure policy.
    subscriber_queue_policy: SubQueuePolicy,
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

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
struct NamespaceKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
}

impl<'a> NamespaceKeyRef<'a> {
    fn new(tenant_id: &'a str, namespace: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
        }
    }
}

impl<'a> hashbrown::Equivalent<NamespaceKey> for NamespaceKeyRef<'a> {
    fn equivalent(&self, key: &NamespaceKey) -> bool {
        self.tenant_id == key.tenant_id && self.namespace == key.namespace
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

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
struct StreamKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    stream: &'a str,
}

impl<'a> StreamKeyRef<'a> {
    fn new(tenant_id: &'a str, namespace: &'a str, stream: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            stream,
        }
    }
}

impl<'a> hashbrown::Equivalent<StreamKey> for StreamKeyRef<'a> {
    fn equivalent(&self, key: &StreamKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.stream == key.stream
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

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
struct CacheKeyRef<'a> {
    tenant_id: &'a str,
    namespace: &'a str,
    cache: &'a str,
}

impl<'a> CacheKeyRef<'a> {
    fn new(tenant_id: &'a str, namespace: &'a str, cache: &'a str) -> Self {
        Self {
            tenant_id,
            namespace,
            cache,
        }
    }
}

impl<'a> hashbrown::Equivalent<CacheKey> for CacheKeyRef<'a> {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.tenant_id == key.tenant_id
            && self.namespace == key.namespace
            && self.cache == key.cache
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
            topics: RwLock::new(HashMap::with_hasher(RandomState::new())),
            streams: RwLock::new(HashMap::with_hasher(RandomState::new())),
            caches: RwLock::new(HashMap::with_hasher(RandomState::new())),
            tenants: RwLock::new(HashMap::with_hasher(RandomState::new())),
            namespaces: RwLock::new(HashMap::with_hasher(RandomState::new())),
            cache,
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            log_capacity: DEFAULT_LOG_CAPACITY,
            subscriber_queue_policy: DEFAULT_SUB_QUEUE_POLICY,
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

    pub fn with_subscriber_queue_policy(mut self, policy: SubQueuePolicy) -> Self {
        self.subscriber_queue_policy = policy;
        self
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
        // We intentionally avoid a global broadcast channel here:
        // each subscriber has a bounded queue and publish uses try_send so a slow consumer
        // drops locally instead of stalling all publishers.
        let sample = t_should_sample();
        let lookup_start = t_now_if(sample);
        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;

        if let Some(start) = lookup_start {
            let lookup_ns = start.elapsed().as_nanos() as u64;
            timings::record_lookup_ns(lookup_ns);
            t_histogram!("broker_publish_lookup_ns").record(lookup_ns as f64);
        }

        let append_start = t_now_if(sample);
        // Append to the in-memory log first so cursors can replay.
        stream_state.append_batch(payloads, self.log_capacity);

        if let Some(start) = append_start {
            let append_ns = start.elapsed().as_nanos() as u64;
            timings::record_append_ns(append_ns);
            t_histogram!("broker_publish_append_ns").record(append_ns as f64);
        }

        let send_start = t_now_if(sample);
        let senders = stream_state.subscriber_snapshot();
        let fanout = senders.len();
        #[cfg(feature = "telemetry")]
        let payload_bytes: usize = payloads.iter().map(Bytes::len).sum();
        #[cfg(feature = "telemetry")]
        let fanout_label = fanout.to_string();
        #[cfg(feature = "telemetry")]
        let payload_bytes_label = payload_bytes.to_string();
        #[cfg(not(feature = "telemetry"))]
        let _ = fanout;

        let fanout_start = t_now_if(sample);
        let mut closed_subscribers = Vec::new();
        let mut sent = 0usize;
        let mut payload_times: SmallVec<[Instant; 8]> = SmallVec::with_capacity(payloads.len());
        payload_times.extend(payloads.iter().map(|_| Instant::now()));
        let enqueue_start = t_now_if(sample);
        'subscribers: for subscriber in senders.iter() {
            for (payload_idx, payload) in payloads.iter().enumerate() {
                let enqueued_at = payload_times[payload_idx];
                match stream_state.subscriber_queue_policy {
                    SubQueuePolicy::Block => {
                        metrics::counter!("felix_sub_shared_payload_clones_total").increment(1);
                        let envelope = DeliveryEnvelope {
                            payload: payload.clone(),
                            enqueued_at,
                        };
                        if subscriber.sender.send(envelope).await.is_ok() {
                            stream_state.increment_queue_depth();
                            sent += 1;
                        } else {
                            closed_subscribers.push(subscriber.id as u64);
                            break;
                        }
                    }
                    SubQueuePolicy::DropNew => {
                        if payload_idx == 0 && payloads.len() > 1 {
                            match subscriber.sender.try_reserve_many(payloads.len()) {
                                Ok(mut permits) => {
                                    for (payload_idx, payload) in payloads.iter().enumerate() {
                                        let Some(permit) = permits.next() else {
                                            break;
                                        };
                                        let enqueued_at = payload_times[payload_idx];
                                        metrics::counter!("felix_sub_shared_payload_clones_total")
                                            .increment(1);
                                        let envelope = DeliveryEnvelope {
                                            payload: payload.clone(),
                                            enqueued_at,
                                        };
                                        permit.send(envelope);
                                        stream_state.increment_queue_depth();
                                        sent += 1;
                                    }
                                    continue 'subscribers;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    closed_subscribers.push(subscriber.id as u64);
                                    continue 'subscribers;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    // Fall back to per-payload reservation to preserve drop-new semantics.
                                }
                            }
                        }
                        match subscriber.sender.try_reserve() {
                            Ok(permit) => {
                                metrics::counter!("felix_sub_shared_payload_clones_total")
                                    .increment(1);
                                let envelope = DeliveryEnvelope {
                                    payload: payload.clone(),
                                    enqueued_at,
                                };
                                permit.send(envelope);
                                stream_state.increment_queue_depth();
                                sent += 1;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                metrics::counter!("felix_sub_queue_dropped_total").increment(1);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                closed_subscribers.push(subscriber.id as u64);
                                break;
                            }
                        }
                    }
                    SubQueuePolicy::DropOld => {
                        // tokio::mpsc does not expose drop-head; emulate with drop-new semantics.
                        if payload_idx == 0 && payloads.len() > 1 {
                            match subscriber.sender.try_reserve_many(payloads.len()) {
                                Ok(mut permits) => {
                                    for (payload_idx, payload) in payloads.iter().enumerate() {
                                        let Some(permit) = permits.next() else {
                                            break;
                                        };
                                        let enqueued_at = payload_times[payload_idx];
                                        metrics::counter!("felix_sub_shared_payload_clones_total")
                                            .increment(1);
                                        let envelope = DeliveryEnvelope {
                                            payload: payload.clone(),
                                            enqueued_at,
                                        };
                                        permit.send(envelope);
                                        stream_state.increment_queue_depth();
                                        sent += 1;
                                    }
                                    continue 'subscribers;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    closed_subscribers.push(subscriber.id as u64);
                                    continue 'subscribers;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    // Fall back to per-payload reservation to preserve drop-old emulation.
                                }
                            }
                        }
                        match subscriber.sender.try_reserve() {
                            Ok(permit) => {
                                metrics::counter!("felix_sub_shared_payload_clones_total")
                                    .increment(1);
                                let envelope = DeliveryEnvelope {
                                    payload: payload.clone(),
                                    enqueued_at,
                                };
                                permit.send(envelope);
                                stream_state.increment_queue_depth();
                                sent += 1;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                metrics::counter!("felix_sub_queue_dropped_total").increment(1);
                                metrics::counter!("felix_sub_queue_drop_old_emulated_total")
                                    .increment(1);
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                closed_subscribers.push(subscriber.id as u64);
                                break;
                            }
                        }
                    }
                }
            }
        }
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_enqueue_ns(enqueue_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_enqueue_ns",
                    "fanout" => fanout_label.clone(),
                    "payload_bytes" => payload_bytes_label.clone()
                )
                .record(enqueue_ns as f64);
            }
        }

        if !closed_subscribers.is_empty() {
            closed_subscribers.sort_unstable();
            closed_subscribers.dedup();
            stream_state.remove_subscribers(&closed_subscribers);
        }
        if let Some(start) = fanout_start {
            let fanout_ns = start.elapsed().as_nanos() as u64;
            timings::record_fanout_ns(fanout_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_fanout_total_ns",
                    "fanout" => fanout_label.clone(),
                    "payload_bytes" => payload_bytes_label.clone()
                )
                .record(fanout_ns as f64);
            }
        }
        if let Some(start) = send_start {
            let send_ns = start.elapsed().as_nanos() as u64;
            timings::record_send_ns(send_ns);
            #[cfg(feature = "telemetry")]
            {
                t_histogram!(
                    "broker_publish_send_ns",
                    "fanout" => fanout_label,
                    "payload_bytes" => payload_bytes_label
                )
                .record(send_ns as f64);
            }
        }
        Ok(sent)
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        // Fast-path guard: reject unknown scopes before opening a receiver.
        self.assert_stream_exists(tenant_id, namespace, stream)
            .await?;

        let stream_state = self.get_stream_state(tenant_id, namespace, stream).await?;
        let (subscriber_id, receiver) = stream_state.register_subscriber();
        Ok(Subscription {
            receiver,
            guard: SubscriptionGuard {
                stream_state: Arc::downgrade(&stream_state),
                subscriber_id,
            },
        })
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
    ) -> Result<(Vec<Bytes>, Subscription)> {
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
        let (subscriber_id, receiver) = stream_state.register_subscriber();
        Ok((
            backlog,
            Subscription {
                receiver,
                guard: SubscriptionGuard {
                    stream_state: Arc::downgrade(&stream_state),
                    subscriber_id,
                },
            },
        ))
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
        guard.entry(key).or_insert_with(|| {
            Arc::new(StreamState::new(
                self.topic_capacity,
                self.subscriber_queue_policy,
            ))
        });
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

    async fn get_stream_state(
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

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;
    use std::time::Instant;

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

    #[test]
    fn append_batch_keeps_monotonic_sequences_and_trims_once() {
        let stream = StreamState::new(8, SubQueuePolicy::DropNew);
        let first = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"d"),
            Bytes::from_static(b"e"),
        ];
        stream.append_batch(&first, 3);
        let second = vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")];
        stream.append_batch(&second, 3);

        let state = stream.log_state.lock();
        let seqs = state.log.iter().map(|entry| entry.seq).collect::<Vec<_>>();
        let payloads = state
            .log
            .iter()
            .map(|entry| entry.payload.clone())
            .collect::<Vec<_>>();

        assert_eq!(state.next_seq, 7);
        assert_eq!(seqs, vec![4, 5, 6]);
        assert_eq!(
            payloads,
            vec![
                Bytes::from_static(b"e"),
                Bytes::from_static(b"f"),
                Bytes::from_static(b"g")
            ]
        );
    }

    #[tokio::test]
    async fn slow_subscriber_drops_messages_without_blocking_publish() {
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
        let delivered = broker
            .publish("t1", "default", "laggy", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        assert_eq!(delivered, 0);
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
        assert!(sub.try_recv().is_err());
    }

    #[tokio::test]
    async fn block_policy_backpressures_publish_when_queue_is_full() {
        let broker = Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(1)
            .expect("capacity")
            .with_subscriber_queue_policy(SubQueuePolicy::Block);
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "blocky", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "blocky")
            .await
            .expect("subscribe");

        broker
            .publish("t1", "default", "blocky", Bytes::from_static(b"one"))
            .await
            .expect("publish");

        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            broker.publish("t1", "default", "blocky", Bytes::from_static(b"two")),
        )
        .await;
        assert!(blocked.is_err(), "publish should block on full queue");
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
        let sent = broker
            .publish("t1", "default", "blocky", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        assert_eq!(sent, 1);
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"two"));
    }

    #[tokio::test]
    async fn drop_old_policy_is_emulated_as_drop_new() {
        let broker = Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(1)
            .expect("capacity")
            .with_subscriber_queue_policy(SubQueuePolicy::DropOld);
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "drop_old", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "drop_old")
            .await
            .expect("subscribe");

        broker
            .publish("t1", "default", "drop_old", Bytes::from_static(b"one"))
            .await
            .expect("publish");
        let delivered = broker
            .publish("t1", "default", "drop_old", Bytes::from_static(b"two"))
            .await
            .expect("publish");
        assert_eq!(delivered, 0);
        assert_eq!(sub.recv().await.expect("recv"), Bytes::from_static(b"one"));
        assert!(sub.try_recv().is_err());
    }

    #[tokio::test]
    async fn small_queue_does_not_grow_unbounded_under_burst() {
        let broker = Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(2)
            .expect("capacity")
            .with_subscriber_queue_policy(SubQueuePolicy::DropNew);
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "bounded", StreamMetadata::default())
            .await
            .expect("register");
        let mut sub = broker
            .subscribe("t1", "default", "bounded")
            .await
            .expect("subscribe");

        for i in 0..100 {
            let payload = Bytes::from(format!("msg-{i}"));
            let _ = broker
                .publish("t1", "default", "bounded", payload)
                .await
                .expect("publish");
        }

        // Queue is capped at 2; only the earliest buffered items are still available.
        let _ = sub.recv().await.expect("recv");
        let _ = sub.recv().await.expect("recv");
        assert!(sub.try_recv().is_err());
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
    async fn subscribe_drop_unregisters_subscriber() {
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

        let stream_state = broker
            .get_stream_state("t1", "default", "orders")
            .await
            .expect("stream state");
        assert_eq!(stream_state.subscriber_count(), 0);

        let sub = broker
            .subscribe("t1", "default", "orders")
            .await
            .expect("subscribe");
        assert_eq!(stream_state.subscriber_count(), 1);
        drop(sub);
        assert_eq!(stream_state.subscriber_count(), 0);
    }

    #[tokio::test]
    #[ignore = "microbenchmark: run explicitly for perf validation"]
    async fn perf_hot_path_payload_4096_fanout_1_batch_64_binary() {
        let broker = Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(16_384)
            .expect("capacity");
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("stream");

        let mut sub = broker
            .subscribe("t1", "default", "orders")
            .await
            .expect("subscribe");
        let iterations = 200usize;
        let payloads: Vec<Bytes> = (0..64).map(|_| Bytes::from(vec![0xAB; 4096])).collect();
        let expected = iterations * payloads.len();

        let drain = tokio::spawn(async move {
            for _ in 0..expected {
                let _ = sub.recv().await;
            }
        });

        let stream_state = broker
            .get_stream_state("t1", "default", "orders")
            .await
            .expect("stream_state");

        let mut snapshot_ns = 0u128;
        let mut publish_ns = 0u128;
        let mut encode_ns = 0u128;
        let mut write_ns = 0u128;

        for _ in 0..iterations {
            let start = Instant::now();
            let _ = stream_state.subscriber_snapshot();
            snapshot_ns += start.elapsed().as_nanos();

            let start = Instant::now();
            broker
                .publish_batch("t1", "default", "orders", &payloads)
                .await
                .expect("publish");
            publish_ns += start.elapsed().as_nanos();

            let start = Instant::now();
            let frame = felix_wire::binary::encode_event_batch_bytes(1, &payloads)
                .expect("encode binary event batch");
            encode_ns += start.elapsed().as_nanos();

            let start = Instant::now();
            let mut io_buf = Vec::with_capacity(frame.len());
            io_buf.extend_from_slice(frame.as_ref());
            write_ns += start.elapsed().as_nanos();
        }

        drain.await.expect("drain");

        println!(
            "perf payload=4096 fanout=1 batch=64 binary=true iterations={} snapshot_avg_us={:.2} publish_avg_us={:.2} encode_avg_us={:.2} write_avg_us={:.2}",
            iterations,
            snapshot_ns as f64 / iterations as f64 / 1_000.0,
            publish_ns as f64 / iterations as f64 / 1_000.0,
            encode_ns as f64 / iterations as f64 / 1_000.0,
            write_ns as f64 / iterations as f64 / 1_000.0,
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

    #[tokio::test]
    async fn register_stream_requires_namespace() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        let err = broker
            .register_stream("t1", "missing", "orders", StreamMetadata::default())
            .await
            .expect_err("namespace");
        assert!(matches!(err, BrokerError::NamespaceNotFound { .. }));
    }

    #[tokio::test]
    async fn publish_to_nonexistent_stream_errors() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        let err = broker
            .publish("t1", "default", "missing", Bytes::from_static(b"data"))
            .await
            .expect_err("stream");
        assert!(matches!(err, BrokerError::StreamNotFound { .. }));
    }

    #[tokio::test]
    async fn subscribe_to_nonexistent_stream_errors() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        let err = broker
            .subscribe("t1", "default", "missing")
            .await
            .expect_err("stream");
        assert!(matches!(err, BrokerError::StreamNotFound { .. }));
    }

    #[tokio::test]
    async fn remove_tenant_succeeds() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        let removed = broker.remove_tenant("t1").await.expect("remove");
        assert!(removed);
        // Removing again returns false
        let removed_again = broker.remove_tenant("t1").await.expect("remove");
        assert!(!removed_again);
    }

    #[tokio::test]
    async fn remove_namespace_succeeds() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        assert!(broker.namespace_exists("t1", "default").await);
        let removed = broker
            .remove_namespace("t1", "default")
            .await
            .expect("remove");
        assert!(removed);
        assert!(!broker.namespace_exists("t1", "default").await);
    }

    #[tokio::test]
    async fn remove_stream_succeeds() {
        let broker = Broker::new(EphemeralCache::new().into());
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "orders", StreamMetadata::default())
            .await
            .expect("stream");
        assert!(broker.stream_exists("t1", "default", "orders").await);
        broker
            .remove_stream("t1", "default", "orders")
            .await
            .expect("remove");
        assert!(!broker.stream_exists("t1", "default", "orders").await);
    }

    #[tokio::test]
    async fn cursor_methods() {
        let cursor = Cursor { next_seq: 42 };
        assert_eq!(cursor.next_seq(), 42);
    }

    #[tokio::test]
    async fn broker_error_display() {
        let err = BrokerError::CapacityTooLarge;
        assert!(err.to_string().contains("capacity"));

        let err = BrokerError::CursorTooOld {
            oldest: 10,
            requested: 5,
        };
        assert!(err.to_string().contains("10"));
        assert!(err.to_string().contains("5"));

        let err = BrokerError::TenantNotFound("t1".to_string());
        assert!(err.to_string().contains("t1"));

        let err = BrokerError::NamespaceNotFound {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
        };
        assert!(err.to_string().contains("t1"));
        assert!(err.to_string().contains("ns1"));

        let err = BrokerError::StreamNotFound {
            tenant_id: "t1".to_string(),
            namespace: "ns1".to_string(),
            stream: "s1".to_string(),
        };
        assert!(err.to_string().contains("t1"));
        assert!(err.to_string().contains("ns1"));
        assert!(err.to_string().contains("s1"));
    }
}
