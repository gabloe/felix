// The `Broker` aggregate: shared state, construction, and the publish/subscribe
// data path. Registry administration lives in `registry.rs` as a second impl block.

use ahash::RandomState;
use bytes::Bytes;
use felix_storage::StorageApi;
use hashbrown::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use crate::config::{
    DEFAULT_LOG_CAPACITY, DEFAULT_SUB_QUEUE_POLICY, DEFAULT_TOPIC_CAPACITY, SubQueuePolicy,
};
use crate::delivery::{DeliveryEnvelope, QueuedDelivery};
use crate::error::{BrokerError, Result};
use crate::keys::{CacheKey, NamespaceKey, StreamKey};
use crate::stream_state::{Cursor, StreamState};
use crate::subscription::{Subscription, SubscriptionGuard};
use crate::telemetry::{t_now_if, t_should_sample};
use crate::timings;

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
    pub(crate) topics: RwLock<HashMap<StreamKey, Arc<StreamState>, RandomState>>,
    // Map of stream key -> metadata for existence checks.
    pub(crate) streams: RwLock<HashMap<StreamKey, StreamMetadata, RandomState>>,
    // Map of cache key -> metadata for existence checks.
    pub(crate) caches: RwLock<HashMap<CacheKey, CacheMetadata, RandomState>>,
    // Map of tenant id -> active marker.
    pub(crate) tenants: RwLock<HashMap<String, (), RandomState>>,
    // Map of namespace key -> active marker.
    pub(crate) namespaces: RwLock<HashMap<NamespaceKey, (), RandomState>>,
    // Ephemeral cache used by demos and simple workflows.
    pub(crate) cache: Box<dyn StorageApi + Send>,
    // Per-subscriber queue capacity for each stream.
    pub(crate) topic_capacity: usize,
    // Per-topic in-memory log capacity.
    pub(crate) log_capacity: usize,
    // Subscriber queue backpressure policy.
    pub(crate) subscriber_queue_policy: SubQueuePolicy,
    pub(crate) next_stream_handle: AtomicU64,
}

// `Broker` is `Send + Sync` from its fields alone: every field is an `RwLock`,
// an atomic, a `usize`, or `Box<dyn StorageApi + Send>` — and `StorageApi`
// already requires `Send + Sync`. The compiler's auto-impls cover this, so no
// `unsafe impl` is needed. This assertion fails the build if a future field
// breaks the property instead of letting it be papered over again.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Broker>();
};

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub durable: bool,
    pub shards: u32,
}

#[derive(Clone, Debug)]
pub struct StreamHandle {
    pub(crate) state: Arc<StreamState>,
}

impl StreamHandle {
    pub fn id(&self) -> u64 {
        self.state.handle_id
    }

    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Acquire)
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
            next_stream_handle: AtomicU64::new(1),
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
        let sample = t_should_sample();
        let lookup_start = t_now_if(sample);
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream)
            .await?;
        if let Some(start) = lookup_start {
            let lookup_ns = start.elapsed().as_nanos() as u64;
            timings::record_lookup_ns(lookup_ns);
            t_histogram!("broker_publish_lookup_ns").record(lookup_ns as f64);
        }
        self.publish_batch_to_handle(&handle, payloads).await
    }

    pub async fn publish_batch_to_handle(
        &self,
        handle: &StreamHandle,
        payloads: &[Bytes],
    ) -> Result<usize> {
        if !handle.state.active.load(Ordering::Acquire) {
            return Err(BrokerError::StreamHandleInactive(handle.id()));
        }

        // Fan-out to current subscribers.
        // We intentionally avoid a global broadcast channel here:
        // each subscriber has a bounded queue and publish uses try_send so a slow consumer
        // drops locally instead of stalling all publishers.
        let sample = t_should_sample();
        let stream_state = &handle.state;
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
        if payloads.is_empty() {
            return Ok(0);
        }
        let envelope = DeliveryEnvelope::new(payloads);
        let item_count = envelope.len();
        let enqueue_start = t_now_if(sample);
        for subscriber in senders.iter() {
            match stream_state.subscriber_queue_policy {
                SubQueuePolicy::Block => {
                    if let Ok(permit) = subscriber.sender.reserve().await {
                        metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                        stream_state.increment_queue_depth(item_count);
                        permit.send(QueuedDelivery::new(
                            envelope.clone(),
                            Arc::clone(&stream_state.queued_items),
                        ));
                        sent += item_count;
                    } else {
                        closed_subscribers.push(subscriber.id as u64);
                    }
                }
                SubQueuePolicy::DropNew | SubQueuePolicy::DropOld => {
                    match subscriber.sender.try_reserve() {
                        Ok(permit) => {
                            metrics::counter!("felix_sub_shared_batch_handles_total").increment(1);
                            stream_state.increment_queue_depth(item_count);
                            permit.send(QueuedDelivery::new(
                                envelope.clone(),
                                Arc::clone(&stream_state.queued_items),
                            ));
                            sent += item_count;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            metrics::counter!("felix_subscribe_dropped_total")
                                .increment(item_count as u64);
                            metrics::counter!("felix_sub_queue_dropped_total")
                                .increment(item_count as u64);
                            if matches!(
                                stream_state.subscriber_queue_policy,
                                SubQueuePolicy::DropOld
                            ) {
                                metrics::counter!("felix_sub_queue_drop_old_emulated_total")
                                    .increment(item_count as u64);
                            }
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            closed_subscribers.push(subscriber.id as u64);
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
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream)
            .await?;
        let stream_state = handle.state;
        let (subscriber_id, receiver) = stream_state.register_subscriber();
        Ok(Subscription {
            receiver,
            guard: SubscriptionGuard {
                stream_state: Arc::downgrade(&stream_state),
                subscriber_id,
            },
            pending: VecDeque::new(),
        })
    }

    // Return a cursor positioned at the tail of the stream log.
    pub async fn cursor_tail(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Cursor> {
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream)
            .await?;

        Ok(Cursor {
            next_seq: handle.state.tail_seq(),
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
        let handle = self
            .resolve_stream_handle(tenant_id, namespace, stream)
            .await?;
        let stream_state = handle.state;

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
                pending: VecDeque::new(),
            },
        ))
    }

    pub fn cache(&self) -> &(dyn StorageApi + Send) {
        // Expose the cache for demos and integrations.
        self.cache.as_ref()
    }
}
