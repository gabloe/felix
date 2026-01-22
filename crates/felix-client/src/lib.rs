// High-level client for talking to a Felix broker.
// Provides both an in-process wrapper and a QUIC-based network client.
//
// IMPORTANT CLIENT-SIDE DESIGN INTENT
// ----------------------------------
// This crate is intentionally *not* a general-purpose “do anything concurrently”
// QUIC client. It is a latency-oriented client with explicit serialization
// points to avoid hidden contention:
//
// - Quinn `SendStream` is effectively a single-writer resource. If multiple tasks
//   write concurrently, Quinn (or our layers) must serialize those writes via
//   internal locking, which becomes a performance cliff under load. Quinn has a mutex
//   per `SendStream` for this purpose. We want to avoid that lock contention entirely.
//
// Therefore, for correctness + predictable performance, we build explicit
// single-writer loops and communicate via bounded queues.
//
// If we want more parallelism, we need to scale by increasing:
// - number of connections (pool size), and/or
// - number of independent streams (streams-per-connection),
// not by writing concurrently to the same `SendStream`.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use felix_wire::{AckMode, Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot};

pub mod timings;

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(feature = "telemetry")]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        metrics::counter!($($tt)*)
    };
}

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(not(feature = "telemetry"))]
macro_rules! t_counter {
    ($($tt:tt)*) => {
        NoopCounter
    };
}

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(feature = "telemetry")]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        metrics::histogram!($($tt)*)
    };
}

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(not(feature = "telemetry"))]
macro_rules! t_histogram {
    ($($tt:tt)*) => {
        NoopHistogram
    };
}

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(feature = "telemetry")]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        metrics::gauge!($($tt)*)
    };
}

#[cfg_attr(not(feature = "telemetry"), allow(unused_macros))]
#[cfg(not(feature = "telemetry"))]
macro_rules! t_gauge {
    ($($tt:tt)*) => {
        NoopGauge
    };
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct NoopCounter;

#[cfg(not(feature = "telemetry"))]
impl NoopCounter {
    fn increment(&self, _value: u64) {}
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct NoopHistogram;

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
impl NoopHistogram {
    fn record(&self, _value: f64) {}
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct NoopGauge;

#[cfg(not(feature = "telemetry"))]
impl NoopGauge {
    fn set(&self, _value: f64) {}
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

/*
CLIENT DESIGN NOTES (felix-client)

This crate provides two client flavors:

1) InProcessClient
   - Thin wrapper around an in-memory `felix_broker::Broker`.
   - Useful for tests, benchmarks, and embedding Felix into a single process.
   - No transport concerns (no framing, flow control, backpressure across the network).

2) Client (QUIC network client)
   - Speaks `felix-wire` over QUIC.
   - QUIC multiplexing is powerful, but the write-side of a Quinn `SendStream` is
     effectively a single-writer resource: concurrent writes from multiple tasks
     create lock contention and can introduce expensive serialization.

Key decisions in this implementation:

A) Single-writer per QUIC stream
   - Cache: each cache worker owns exactly one bi-directional stream and performs
     strictly sequential request/response round-trips on that stream.
   - Publish: `Publisher` owns one bi-directional stream and a single writer task
     serializes publishes, optionally waiting for acks.
   - Subscribe: subscribe control happens on a short-lived bi stream; events arrive
     on a server-opened uni stream, which we route based on an initial
     `EventStreamHello { subscription_id }`.

B) Connection pooling to reduce HOL and improve concurrency
   - Cache ops are latency-sensitive and can become head-of-line blocked if a slow
     cache response sits ahead of faster ones on the same stream.
   - We pool cache connections, then open multiple streams per connection, and
     assign each stream a single-writer worker.
   - Subscriptions are round-robined across event connections; each subscription
     still gets its own server-opened uni stream for events.

C) Backpressure via bounded queues
   - Cache workers use bounded channels to apply pressure back to callers.
   - Publisher uses a bounded queue so we fail fast rather than buffer unboundedly.

D) Protocol invariants we rely on
   - Any acked publish (AckMode != None) must have a request_id.
   - Acks must match request_id (server is allowed to pipeline / reorder).
   - For subscriptions, the first frame on the uni stream must be EventStreamHello.
*/

/// Client wrapper that talks to an in-process broker.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_client::InProcessClient;
/// use felix_storage::EphemeralCache;
/// use std::sync::Arc;
///
/// let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
/// let client = InProcessClient::new(broker.clone());
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
///         .register_stream("t1", "default", "updates", Default::default())
///         .await
///         .expect("register");
///     let mut sub = client.subscribe("t1", "default", "updates").await.expect("subscribe");
///     client
///         .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"payload"));
/// });
/// ```
#[derive(Clone)]
pub struct InProcessClient {
    // Keep an Arc so callers can clone the client cheaply.
    broker: Arc<Broker>,
}

impl InProcessClient {
    // Construct a client that shares the broker running in this process.
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    // Forward publish calls directly to the broker.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        self.broker
            .publish(tenant_id, namespace, stream, payload)
            .await
            .map_err(Into::into)
    }

    // Subscribe to a topic and return a broadcast receiver.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<broadcast::Receiver<Bytes>> {
        self.broker
            .subscribe(tenant_id, namespace, stream)
            .await
            .map_err(Into::into)
    }
}

/// Network client that speaks felix-wire over QUIC.
pub struct Client {
    // We keep three QUIC clients primarily to allow different transport tuning knobs per workload.

    // Publish streams are pooled for higher throughput.
    _publish_client: QuicClient,
    // Cache streams are pooled separately for lower latency round trips.
    _cache_client: QuicClient,
    // Event streams are pooled for subscriptions.
    _event_client: QuicClient,

    // Publish worker pool: multiple streams across multiple connections.
    publish_workers: Arc<Vec<PublishWorker>>,
    publish_sharding: PublishSharding,

    // Per-stream cache workers: each owns exactly one bi-directional QUIC stream and
    // serializes cache round-trips (encode -> write -> read -> decode).
    cache_workers: Vec<CacheWorker>,

    // Connection pool for subscription event streams.
    event_connections: Vec<QuicConnection>,

    // For each event connection, a router task accepts uni streams, reads EventStreamHello,
    // and hands the RecvStream to the matching Subscription.
    event_stream_routers: Vec<mpsc::Sender<EventRouterCommand>>,
    subscription_counter: AtomicU64,
    cache_request_counter: AtomicU64,
    event_pool_size: usize,
    cache_worker_rr: AtomicUsize,

    // NOTE: the semantics of these counters are currently muddled.
    // - cache_conn_counts is incremented but never decremented.
    // - a single cache worker currently resets the entire connection count on exit.
    // We probably need track inflight (inc on dispatch, dec on completion), and never
    // reset a shared connection counter from one worker.
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
    event_conn_counts: Arc<Vec<AtomicUsize>>,
}

impl Client {
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_transport(addr, server_name, client_config, TransportConfig::default())
            .await
    }

    pub async fn connect_with_transport(
        addr: SocketAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("bind addr");
        let publish_client = QuicClient::bind(bind_addr, client_config.clone(), transport.clone())?;
        let publish_pool_size = std::env::var("FELIX_PUB_CONN_POOL")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_CONN_POOL);
        let publish_streams_per_conn = std::env::var("FELIX_PUB_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_STREAMS_PER_CONN);
        if publish_pool_size == 0 || publish_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("publish pool misconfigured"));
        }
        let publish_chunk_bytes = std::env::var("FELIX_PUBLISH_CHUNK_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_CHUNK_BYTES);
        let mut publish_connections = Vec::with_capacity(publish_pool_size);
        for _ in 0..publish_pool_size {
            let connection = publish_client.connect(addr, server_name).await?;
            publish_connections.push(connection);
        }
        let mut publish_workers = Vec::with_capacity(publish_pool_size * publish_streams_per_conn);
        for connection in &publish_connections {
            for _ in 0..publish_streams_per_conn {
                let (send, recv) = connection.open_bi().await?;
                let (tx, rx) = mpsc::channel(PUBLISH_QUEUE_DEPTH);
                let handle =
                    tokio::spawn(run_publisher_writer(send, recv, rx, publish_chunk_bytes));
                publish_workers.push(PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                });
            }
        }
        let publish_sharding = PublishSharding::from_env().unwrap_or(PublishSharding::HashStream);
        // Cache connections are pooled to avoid head-of-line blocking.
        // DESIGN NOTE:
        // We pool *connections* and then open multiple *streams per connection*.
        // This avoids (a) creating a new QUIC connection per cache op and
        // (b) HOL blocking between independent cache ops on a single stream.
        let cache_pool_size = std::env::var("FELIX_CACHE_CONN_POOL")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_CONN_POOL);
        let cache_transport = cache_transport_config(transport.clone());
        let cache_client = QuicClient::bind(bind_addr, client_config.clone(), cache_transport)?;
        let mut cache_connections = Vec::with_capacity(cache_pool_size);
        for _ in 0..cache_pool_size {
            let connection = cache_client.connect(addr, server_name).await?;
            cache_connections.push(connection);
        }
        // Each cache connection runs multiple independent bi-directional streams.
        let cache_streams_per_conn = std::env::var("FELIX_CACHE_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_STREAMS_PER_CONN);
        if cache_pool_size == 0 || cache_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("cache pool misconfigured"));
        }
        let mut cache_workers = Vec::with_capacity(cache_pool_size * cache_streams_per_conn);
        let mut cache_conn_counts = Vec::with_capacity(cache_pool_size);
        for _ in 0..cache_pool_size {
            cache_conn_counts.push(AtomicUsize::new(0));
        }
        let cache_conn_counts = Arc::new(cache_conn_counts);
        for (conn_index, connection) in cache_connections.iter().enumerate() {
            for _ in 0..cache_streams_per_conn {
                let (send, recv) = connection.open_bi().await?;
                let (tx, rx) = mpsc::channel(CACHE_WORKER_QUEUE_DEPTH);
                tokio::spawn(run_cache_worker(
                    conn_index,
                    send,
                    recv,
                    rx,
                    Arc::clone(&cache_conn_counts),
                ));
                cache_workers.push(CacheWorker { tx, conn_index });
            }
        }
        // Event connections are reserved for subscription streams.
        let event_pool_size = std::env::var("FELIX_EVENT_CONN_POOL")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_EVENT_CONN_POOL);
        let event_transport = event_transport_config(transport);
        let event_client = QuicClient::bind(bind_addr, client_config, event_transport)?;
        let mut event_connections = Vec::with_capacity(event_pool_size);
        for _ in 0..event_pool_size {
            let connection = event_client.connect(addr, server_name).await?;
            event_connections.push(connection);
        }
        let mut event_stream_routers = Vec::with_capacity(event_pool_size);
        for connection in &event_connections {
            event_stream_routers.push(spawn_event_router(connection.clone()));
        }
        let mut event_conn_counts = Vec::with_capacity(event_pool_size);
        for _ in 0..event_pool_size {
            event_conn_counts.push(AtomicUsize::new(0));
        }
        Ok(Self {
            _publish_client: publish_client,
            _cache_client: cache_client,
            _event_client: event_client,
            publish_workers: Arc::new(publish_workers),
            publish_sharding,
            cache_workers,
            event_connections,
            subscription_counter: AtomicU64::new(1),
            cache_request_counter: AtomicU64::new(1),
            event_pool_size,
            cache_worker_rr: AtomicUsize::new(0),
            cache_conn_counts,
            event_stream_routers,
            event_conn_counts: Arc::new(event_conn_counts),
        })
    }

    pub async fn publisher(&self) -> Result<Publisher> {
        Ok(Publisher {
            inner: Arc::new(PublisherInner {
                workers: Arc::clone(&self.publish_workers),
                sharding: self.publish_sharding,
                rr: AtomicUsize::new(0),
            }),
        })
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        // Round-robin subscriptions across the event connection pool.
        let requested_id = self.subscription_counter.fetch_add(1, Ordering::Relaxed);
        let connection_index = requested_id as usize % self.event_pool_size;
        let connection = &self.event_connections[connection_index];
        let (mut send, mut recv) = connection.open_bi().await?;
        let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
        write_message(
            &mut send,
            Message::Subscribe {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                subscription_id: Some(requested_id),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv, &mut frame_scratch).await?;
        let subscription_id = match response {
            Some(Message::Subscribed { subscription_id }) => {
                if subscription_id != requested_id {
                    return Err(anyhow::anyhow!(
                        "subscription id mismatch: requested {requested_id} got {subscription_id}"
                    ));
                }
                subscription_id
            }
            Some(Message::Ok) => {
                return Err(anyhow::anyhow!(
                    "subscribe response missing subscription id"
                ));
            }
            other => return Err(anyhow::anyhow!("subscribe failed: {other:?}")),
        };
        let tenant_id = Arc::<str>::from(tenant_id);
        let namespace = Arc::<str>::from(namespace);
        let stream = Arc::<str>::from(stream);
        let (stream_tx, stream_rx) = oneshot::channel();
        self.event_stream_routers[connection_index]
            .send(EventRouterCommand::Register {
                subscription_id,
                response: stream_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("event stream router closed"))?;
        let recv = stream_rx.await.context("event stream response dropped")??;
        let current = self.event_conn_counts[connection_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_event_conn_subscriptions", "conn" => connection_index.to_string())
            .set(current as f64);
        t_counter!("felix_client_event_conn_subscriptions_total", "conn" => connection_index.to_string())
            .increment(1);
        Ok(Subscription {
            recv,
            frame_scratch: BytesMut::with_capacity(64 * 1024),
            current_batch: None,
            current_index: 0,
            last_poll: None,
            tenant_id,
            namespace,
            stream,
            subscription_id: Some(subscription_id),
            event_conn_index: connection_index,
            event_conn_counts: Arc::clone(&self.event_conn_counts),
        })
    }

    pub async fn cache_put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CachePut {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            value,
            request_id: Some(request_id),
            ttl_ms,
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Put {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Track the connection since it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!("felix_client_cache_conn_ops_total", "conn" => conn_index.to_string())
            .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache put response dropped"))?
    }

    pub async fn cache_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        // Cache ops are delegated to a pool of single-writer cache workers.
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        let (worker, conn_index) = self.cache_worker();
        worker
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;

        // Now it's *actually* enqueued, so it counts as inflight.
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        t_counter!("felix_client_cache_conn_ops_total", "conn" => conn_index.to_string())
            .increment(1);
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache get response dropped"))?
    }

    fn cache_worker(&self) -> (&CacheWorker, usize) {
        // Round-robin pick only.
        //
        // IMPORTANT: do not mutate metrics/counters here.
        // We only consider an op "in-flight" once it is successfully enqueued.
        let index = self.cache_worker_rr.fetch_add(1, Ordering::Relaxed) % self.cache_workers.len();
        let worker = &self.cache_workers[index];
        (worker, worker.conn_index)
    }

    pub fn cache_conn_counts(&self) -> Vec<usize> {
        self.cache_conn_counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect()
    }
}

struct CacheWorker {
    tx: mpsc::Sender<CacheRequest>,
    conn_index: usize,
}

struct PublishWorker {
    tx: mpsc::Sender<PublishRequest>,
    handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
    request_counter: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
enum PublishSharding {
    RoundRobin,
    HashStream,
}

impl PublishSharding {
    fn from_env() -> Option<Self> {
        let value = std::env::var("FELIX_PUB_SHARDING").ok()?;
        match value.as_str() {
            "rr" => Some(Self::RoundRobin),
            "hash_stream" => Some(Self::HashStream),
            _ => None,
        }
    }
}

enum EventRouterCommand {
    Register {
        subscription_id: u64,
        response: oneshot::Sender<Result<RecvStream>>,
    },
}

enum CacheRequest {
    Put {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<()>>,
    },
    Get {
        request_id: u64,
        message: Message,
        response: oneshot::Sender<Result<Option<Bytes>>>,
    },
}

fn spawn_event_router(connection: QuicConnection) -> mpsc::Sender<EventRouterCommand> {
    let (tx, rx) = mpsc::channel(EVENT_ROUTER_QUEUE_DEPTH);
    tokio::spawn(run_event_router(connection, rx));
    tx
}

async fn run_event_router(connection: QuicConnection, mut rx: mpsc::Receiver<EventRouterCommand>) {
    let mut pending_waiters: HashMap<u64, oneshot::Sender<Result<RecvStream>>> = HashMap::new();
    let mut pending_streams: HashMap<u64, RecvStream> = HashMap::new();
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);

    let max_pending = read_usize_env("FELIX_EVENT_ROUTER_MAX_PENDING")
        .unwrap_or(DEFAULT_EVENT_ROUTER_MAX_PENDING);

    // POTENTIAL ISSUE:
    // `pending_waiters` and `pending_streams` can grow without bound if:
    // - the server sends EventStreamHello for subscription_ids the client never registers, or
    // - the client registers but the server never sends the stream.
    // We should probably:
    // - add a maximum map size + eviction (LRU), and/or
    // - add per-entry timeouts and periodically purge stale entries.
    loop {
        tokio::select! {
            command = rx.recv() => {
                match command {
                    Some(EventRouterCommand::Register { subscription_id, response }) => {
                        if pending_waiters.len() + pending_streams.len() >= max_pending {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "event router pending limit reached ({max_pending}); refusing registration"
                            )));
                            continue;
                        }
                        if let Some(stream) = pending_streams.remove(&subscription_id) {
                            let _ = response.send(Ok(stream));
                            continue;
                        }
                        if pending_waiters.contains_key(&subscription_id) {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "duplicate subscription registration for {subscription_id}"
                            )));
                            continue;
                        }
                        pending_waiters.insert(subscription_id, response);
                    }
                    None => {
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!("event stream router closed")));
                        }
                        break;
                    }
                }
            }
            stream = connection.accept_uni() => {
                let mut recv = match stream {
                    Ok(recv) => recv,
                    Err(err) => {
                        let message = err.to_string();
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!(message.clone())));
                        }
                        break;
                    }
                };
                if pending_waiters.len() + pending_streams.len() >= max_pending {
                    // Best-effort: drop the stream to cap memory growth under overload.
                    continue;
                }
                let subscription_id = match read_message(&mut recv, &mut frame_scratch).await {
                    Ok(Some(Message::EventStreamHello { subscription_id })) => subscription_id,
                    Ok(Some(_)) => continue,
                    Ok(None) => continue,
                    Err(_) => continue,
                };
                if let Some(waiter) = pending_waiters.remove(&subscription_id) {
                    let _ = waiter.send(Ok(recv));
                    continue;
                }
                if pending_streams.contains_key(&subscription_id) {
                    continue;
                }
                pending_streams.insert(subscription_id, recv);
            }
        }
    }
}

async fn run_cache_worker(
    conn_index: usize,
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<CacheRequest>,
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
) {
    // Single writer for a cache stream; handles sequential request/response pairs.
    let sample = t_should_sample();
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    #[cfg(feature = "telemetry")]
    if sample {
        let open_ns = 0;
        timings::record_cache_open_stream_ns(open_ns);
    }
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(request) = rx.recv().await {
        let result = handle_cache_request(&mut send, &mut recv, request, &mut frame_scratch).await;

        // Decrement "in-flight ops" gauge for this connection, saturating at 0.
        let counter = &cache_conn_counts[conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
                        .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }

        if result.is_err() {
            break;
        }
    }
    let _ = send.finish();
    // We should probably use a connection-level atomic that we decremented when work completes so
    // that we can track inflight ops more accurately.
}

async fn handle_cache_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
    request: CacheRequest,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    let sample = t_should_sample();
    match request {
        CacheRequest::Put {
            request_id,
            message,
            response,
        } => {
            let result =
                cache_round_trip(send, recv, message, sample, request_id, frame_scratch).await;
            match result {
                Ok(_) => {
                    let _ = response.send(Ok(()));
                    Ok(())
                }
                Err(err) => {
                    let _ = response.send(Err(err));
                    Err(anyhow::anyhow!("cache stream failed"))
                }
            }
        }
        CacheRequest::Get {
            request_id,
            message,
            response,
        } => {
            let result =
                cache_round_trip(send, recv, message, sample, request_id, frame_scratch).await;
            match result {
                Ok(value) => {
                    let _ = response.send(Ok(value));
                    Ok(())
                }
                Err(err) => {
                    let _ = response.send(Err(err));
                    Err(anyhow::anyhow!("cache stream failed"))
                }
            }
        }
    }
}

async fn cache_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Bytes>> {
    // Encode -> write -> read -> decode in one stream round trip.
    let encode_start = t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = encode_start;
    let frame = message.encode().context("encode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = encode_start {
        let encode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_encode_ns(encode_ns);
    }
    let write_start = t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = write_start;
    write_frame_parts(send, &frame).await?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = write_start {
        let write_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_write_ns(write_ns);
    }
    let frame = match read_frame_cache_timed_into(recv, sample, frame_scratch).await? {
        Some(frame) => frame,
        None => return Err(anyhow::anyhow!("cache response closed")),
    };
    let decode_start = t_now_if(sample);
    #[cfg(not(feature = "telemetry"))]
    let _ = decode_start;
    let response = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_decode_ns(decode_ns);
    }
    match response {
        Message::CacheOk {
            request_id: resp_id,
        } => {
            if resp_id != request_id {
                return Err(anyhow::anyhow!("cache put request id mismatch"));
            }
            Ok(None)
        }
        Message::Ok => Err(anyhow::anyhow!(
            "cache response missing request id (protocol violation)"
        )),
        Message::CacheValue {
            value,
            request_id: resp_id,
            ..
        } => {
            if let Some(resp_id) = resp_id
                && resp_id != request_id
            {
                return Err(anyhow::anyhow!("cache get request id mismatch"));
            }
            Ok(value)
        }
        Message::Error { message } => Err(anyhow::anyhow!("cache error: {message}")),
        other => Err(anyhow::anyhow!("cache response unexpected: {other:?}")),
    }
}

#[derive(Clone)]
pub struct Publisher {
    inner: Arc<PublisherInner>,
}

struct PublisherInner {
    workers: Arc<Vec<PublishWorker>>,
    sharding: PublishSharding,
    rr: AtomicUsize,
}

enum PublishRequest {
    Message {
        message: Message,
        ack: AckMode,
        request_id: Option<u64>,
        response: oneshot::Sender<Result<()>>,
    },
    BinaryBytes {
        bytes: Bytes,
        item_count: usize,
        sample: bool,
        response: oneshot::Sender<Result<()>>,
    },
    Finish {
        response: oneshot::Sender<Result<()>>,
    },
}

const PUBLISH_QUEUE_DEPTH: usize = 1024;
const CACHE_WORKER_QUEUE_DEPTH: usize = 1024;
const EVENT_ROUTER_QUEUE_DEPTH: usize = 1024;
const DEFAULT_PUBLISH_CHUNK_BYTES: usize = 16 * 1024;
const DEFAULT_PUB_CONN_POOL: usize = 4;
const DEFAULT_PUB_STREAMS_PER_CONN: usize = 2;
const DEFAULT_EVENT_CONN_POOL: usize = 8;
const DEFAULT_CACHE_CONN_POOL: usize = 8;
const DEFAULT_CACHE_STREAMS_PER_CONN: usize = 4;
const DEFAULT_EVENT_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_EVENT_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_EVENT_SEND_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_CACHE_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_CACHE_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;
#[cfg(feature = "telemetry")]
const BENCH_EMBED_TS_ENV: &str = "FELIX_BENCH_EMBED_TS";
#[cfg(feature = "telemetry")]
const DECODE_ERROR_LOG_LIMIT: usize = 20;

// See also DEFAULT_MAX_FRAME_BYTES and DEFAULT_EVENT_ROUTER_MAX_PENDING below.

/// Hard safety cap for any single felix-wire frame.
///
/// Rationale:
/// - `read_frame_into` and friends allocate a buffer sized by `header.length`.
/// - Without a cap, a malicious / buggy peer can advertise an enormous length and
///   trigger OOM or allocator churn (DoS).
///
/// Override with `FELIX_MAX_FRAME_BYTES`.
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Upper bound on how many pending subscription registrations/streams the event
/// router will hold.
///
/// Rationale:
/// - `pending_waiters` grows when the app registers but the server never opens the uni stream.
/// - `pending_streams` grows when the server opens uni streams for ids the app never registers.
///
/// Either case can happen due to bugs or a malicious peer; we cap memory usage.
/// Override with `FELIX_EVENT_ROUTER_MAX_PENDING`.
const DEFAULT_EVENT_ROUTER_MAX_PENDING: usize = 16 * 1024;

impl Publisher {
    fn select_worker(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<&PublishWorker> {
        let workers = &self.inner.workers;
        if workers.is_empty() {
            return Err(anyhow::anyhow!("publish pool is empty"));
        }
        let index = match self.inner.sharding {
            PublishSharding::RoundRobin => {
                self.inner.rr.fetch_add(1, Ordering::Relaxed) % workers.len()
            }
            PublishSharding::HashStream => {
                let mut hasher = DefaultHasher::new();
                tenant_id.hash(&mut hasher);
                namespace.hash(&mut hasher);
                stream.hash(&mut hasher);
                (hasher.finish() as usize) % workers.len()
            }
        };
        Ok(&workers[index])
    }

    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payload = maybe_append_publish_ts(payload);
        // Enqueue publish on the single-writer publisher task.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        #[cfg(feature = "telemetry")]
        let sample = t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message: Message::Publish {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payload,
                    request_id,
                    ack: Some(ack),
                },
                ack,
                request_id,
                response: response_tx,
            })
            .await
            .context("enqueue publish")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx.await.context("publish response dropped")?
    }

    pub async fn publish_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads = maybe_append_publish_ts_batch(payloads);
        // Batch publish uses the same queue/writer as single messages.
        let (response_tx, response_rx) = oneshot::channel();
        let request_id = if ack == AckMode::None {
            None
        } else {
            Some(worker.request_counter.fetch_add(1, Ordering::Relaxed))
        };
        #[cfg(feature = "telemetry")]
        let sample = t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let enqueue_start = t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::Message {
                message: Message::PublishBatch {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payloads,
                    request_id,
                    ack: Some(ack),
                },
                ack,
                request_id,
                response: response_tx,
            })
            .await
            .context("enqueue publish batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx
            .await
            .context("publish batch response dropped")?
    }

    pub async fn publish_batch_binary(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> Result<()> {
        let worker = self.select_worker(tenant_id, namespace, stream)?;
        let payloads_with_ts;
        let payloads = if bench_embed_ts_enabled() {
            payloads_with_ts = payloads
                .iter()
                .map(|payload| maybe_append_publish_ts(payload.clone()))
                .collect::<Vec<_>>();
            &payloads_with_ts
        } else {
            payloads
        };
        #[cfg(feature = "telemetry")]
        let sample = t_should_sample();
        #[cfg(not(feature = "telemetry"))]
        let sample = false;
        #[cfg(not(feature = "telemetry"))]
        let _ = sample;
        #[cfg(feature = "telemetry")]
        let start = t_now_if(sample);
        let (bytes, stats) = felix_wire::binary::encode_publish_batch_bytes_with_stats(
            tenant_id, namespace, stream, payloads,
        )?;
        #[cfg(not(feature = "telemetry"))]
        let _ = stats;
        #[cfg(feature = "telemetry")]
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            timings::record_binary_encode_ns(encode_ns);
            t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        #[cfg(feature = "telemetry")]
        if stats.reallocs > 0 {
            let counters = frame_counters();
            counters
                .binary_encode_reallocs
                .fetch_add(stats.reallocs, Ordering::Relaxed);
        }
        let (response_tx, response_rx) = oneshot::channel();
        #[cfg(feature = "telemetry")]
        let enqueue_start = t_now_if(sample);
        worker
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                item_count: payloads.len(),
                sample,
                response: response_tx,
            })
            .await
            .context("enqueue binary batch")?;
        #[cfg(feature = "telemetry")]
        if let Some(start) = enqueue_start {
            let enqueue_ns = start.elapsed().as_nanos() as u64;
            timings::record_publish_enqueue_wait_ns(enqueue_ns);
            t_histogram!("client_pub_enqueue_wait_ns").record(enqueue_ns as f64);
        }
        response_rx.await.context("binary batch response dropped")?
    }

    pub async fn finish(&self) -> Result<()> {
        let mut handles = Vec::new();
        for worker in self.inner.workers.iter() {
            let handle = {
                let mut guard = worker.handle.lock().await;
                guard.take()
            };
            if let Some(handle) = handle {
                let (response_tx, response_rx) = oneshot::channel();
                if worker
                    .tx
                    .send(PublishRequest::Finish {
                        response: response_tx,
                    })
                    .await
                    .is_ok()
                {
                    response_rx
                        .await
                        .context("publisher finish response dropped")??;
                }
                handles.push(handle);
            }
        }
        for handle in handles {
            handle.await.context("publisher writer task")??;
        }
        Ok(())
    }
}

async fn run_publisher_writer(
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    _chunk_bytes: usize,
) -> Result<()> {
    // Single writer: serialize publish requests over one bi-directional stream.
    let mut ack_scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message {
                message,
                ack,
                request_id,
                response,
            } => match message {
                Message::PublishBatch {
                    tenant_id,
                    namespace,
                    stream,
                    payloads,
                    request_id: msg_request_id,
                    ack: msg_ack,
                } => {
                    #[cfg(feature = "telemetry")]
                    let sample = t_should_sample();
                    #[cfg(not(feature = "telemetry"))]
                    let sample = false;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = sample;
                    let json_len = felix_wire::text::publish_batch_json_len(
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        msg_request_id,
                        msg_ack,
                    )?;
                    let mut buf = BytesMut::with_capacity(FrameHeader::LEN + json_len);
                    buf.resize(FrameHeader::LEN, 0);
                    #[cfg(feature = "telemetry")]
                    let encode_start = t_now_if(sample);
                    let stats = felix_wire::text::write_publish_batch_json(
                        &mut buf,
                        &tenant_id,
                        &namespace,
                        &stream,
                        &payloads,
                        msg_request_id,
                        msg_ack,
                    )?;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = stats;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        timings::record_text_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if stats.reallocs > 0 {
                        let counters = frame_counters();
                        counters
                            .text_encode_reallocs
                            .fetch_add(stats.reallocs, Ordering::Relaxed);
                    }
                    #[cfg(feature = "telemetry")]
                    let build_start = t_now_if(sample);
                    let payload_len = buf.len() - FrameHeader::LEN;
                    let header = FrameHeader::new(0, payload_len as u32);
                    let mut header_bytes = [0u8; FrameHeader::LEN];
                    header.encode_into(&mut header_bytes);
                    buf[..FrameHeader::LEN].copy_from_slice(&header_bytes);
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = build_start {
                        let build_ns = start.elapsed().as_nanos() as u64;
                        timings::record_text_batch_build_ns(build_ns);
                        t_histogram!("client_text_batch_build_ns").record(build_ns as f64);
                    }
                    let bytes = buf.freeze();
                    #[cfg(feature = "telemetry")]
                    let write_start = t_now_if(sample);
                    #[cfg(feature = "telemetry")]
                    let await_start = t_now_if(sample);
                    let write_result = send
                        .write_all(&bytes)
                        .await
                        .context("write publish batch frame");
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = await_start {
                        let await_ns = start.elapsed().as_nanos() as u64;
                        timings::record_send_await_ns(await_ns);
                        t_histogram!("client_send_await_ns").record(await_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let result = match write_result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .bytes_out
                                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            }
                            maybe_wait_for_ack(&mut recv, ack, request_id, &mut ack_scratch).await
                        }
                        Err(err) => Err(err),
                    };
                    let item_count = payloads.len() as u64;
                    #[cfg(not(feature = "telemetry"))]
                    let _ = item_count;
                    match result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_ok
                                    .fetch_add(item_count, Ordering::Relaxed);
                            }
                            let _ = response.send(Ok(()));
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                                counters
                                    .pub_items_out_err
                                    .fetch_add(item_count, Ordering::Relaxed);
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            drain_publish_queue(&mut rx, &message).await;
                            return Err(anyhow::anyhow!(message));
                        }
                    }
                }
                other => {
                    #[cfg(feature = "telemetry")]
                    let sample = t_should_sample();
                    #[cfg(feature = "telemetry")]
                    let encode_start = t_now_if(sample);
                    let frame = match other.encode().context("encode message") {
                        Ok(frame) => frame,
                        Err(err) => {
                            let _ = response.send(Err(err));
                            continue;
                        }
                    };
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = encode_start {
                        let encode_ns = start.elapsed().as_nanos() as u64;
                        timings::record_encode_ns(encode_ns);
                        t_histogram!("felix_client_encode_ns").record(encode_ns as f64);
                    }
                    #[cfg(feature = "telemetry")]
                    let write_start = t_now_if(sample);
                    let write_result = write_frame_parts(&mut send, &frame).await;
                    #[cfg(feature = "telemetry")]
                    if let Some(start) = write_start {
                        let write_ns = start.elapsed().as_nanos() as u64;
                        timings::record_write_ns(write_ns);
                        t_histogram!("felix_client_write_ns").record(write_ns as f64);
                    }
                    let result = match write_result {
                        Ok(()) => {
                            maybe_wait_for_ack(&mut recv, ack, request_id, &mut ack_scratch).await
                        }
                        Err(err) => Err(err),
                    };
                    let (batch_count, item_count) = match &other {
                        Message::Publish { .. } => (1u64, 1u64),
                        Message::PublishBatch { payloads, .. } => (1u64, payloads.len() as u64),
                        _ => (0u64, 0u64),
                    };
                    #[cfg(not(feature = "telemetry"))]
                    let _ = (batch_count, item_count);
                    match result {
                        Ok(()) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                                if batch_count > 0 {
                                    counters
                                        .pub_batches_out_ok
                                        .fetch_add(batch_count, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_ok
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                            }
                            let _ = response.send(Ok(()));
                        }
                        Err(err) => {
                            #[cfg(feature = "telemetry")]
                            {
                                let counters = frame_counters();
                                counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                                if batch_count > 0 {
                                    counters
                                        .pub_batches_out_err
                                        .fetch_add(batch_count, Ordering::Relaxed);
                                    counters
                                        .pub_items_out_err
                                        .fetch_add(item_count, Ordering::Relaxed);
                                }
                            }
                            let message = err.to_string();
                            let _ = response.send(Err(err));
                            drain_publish_queue(&mut rx, &message).await;
                            return Err(anyhow::anyhow!(message));
                        }
                    }
                }
            },
            PublishRequest::BinaryBytes {
                bytes,
                item_count,
                sample,
                response,
            } => {
                #[cfg(not(feature = "telemetry"))]
                let _ = (item_count, sample);
                #[cfg(feature = "telemetry")]
                let write_start = t_now_if(sample);
                #[cfg(feature = "telemetry")]
                let chunk_start = t_now_if(sample);
                let result = send
                    .write_all(&bytes)
                    .await
                    .context("write binary batch frame");
                #[cfg(feature = "telemetry")]
                if let Some(start) = chunk_start {
                    let await_ns = start.elapsed().as_nanos() as u64;
                    timings::record_send_await_ns(await_ns);
                    t_histogram!("client_send_await_ns").record(await_ns as f64);
                }
                #[cfg(feature = "telemetry")]
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    t_histogram!("felix_client_write_ns").record(write_ns as f64);
                }
                match result {
                    Ok(()) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.pub_frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .pub_items_out_ok
                                .fetch_add(item_count as u64, Ordering::Relaxed);
                            counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
                            counters
                                .bytes_out
                                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        }
                        let _ = response.send(Ok(()));
                    }
                    Err(err) => {
                        #[cfg(feature = "telemetry")]
                        {
                            let counters = frame_counters();
                            counters.pub_frames_out_err.fetch_add(1, Ordering::Relaxed);
                            counters.pub_batches_out_err.fetch_add(1, Ordering::Relaxed);
                            counters
                                .pub_items_out_err
                                .fetch_add(item_count as u64, Ordering::Relaxed);
                        }
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        drain_publish_queue(&mut rx, &message).await;
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
            PublishRequest::Finish { response } => {
                let result = finish_publisher_stream(&mut send, &mut recv).await;
                match result {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                        return Ok(());
                    }
                    Err(err) => {
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
        }
    }
    finish_publisher_stream(&mut send, &mut recv).await
}

async fn finish_publisher_stream(send: &mut SendStream, recv: &mut RecvStream) -> Result<()> {
    send.finish()?;
    let mut buf = [0u8; 8192];
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

async fn maybe_wait_for_ack(
    recv: &mut RecvStream,
    ack: AckMode,
    request_id: Option<u64>,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    // AckMode::None is fire-and-forget; otherwise wait for PublishOk/PublishError.
    if ack == AckMode::None {
        return Ok(());
    }
    let request_id =
        request_id.ok_or_else(|| anyhow::anyhow!("missing request_id for acked publish"))?;
    let response = read_ack_message_with_timing(recv, frame_scratch).await?;
    match response {
        Some(Message::PublishOk { request_id: ack_id }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.ack_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                counters.ack_items_in_ok.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }
        Some(Message::PublishError {
            request_id: ack_id,
            message,
        }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.ack_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                counters.ack_items_in_ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(anyhow::anyhow!("publish failed: {message}"))
        }
        other => Err(anyhow::anyhow!("publish failed: {other:?}")),
    }
}

async fn drain_publish_queue(rx: &mut mpsc::Receiver<PublishRequest>, message: &str) {
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message { response, .. }
            | PublishRequest::BinaryBytes { response, .. }
            | PublishRequest::Finish { response } => {
                let _ = response.send(Err(anyhow::anyhow!(message.to_string())));
            }
        }
    }
}

pub struct Subscription {
    recv: RecvStream,
    frame_scratch: BytesMut,
    current_batch: Option<Vec<Bytes>>,
    current_index: usize,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    last_poll: Option<Instant>,
    tenant_id: Arc<str>,
    namespace: Arc<str>,
    stream: Arc<str>,
    subscription_id: Option<u64>,
    event_conn_index: usize,
    event_conn_counts: Arc<Vec<AtomicUsize>>,
}

pub struct Event {
    pub tenant_id: Arc<str>,
    pub namespace: Arc<str>,
    pub stream: Arc<str>,
    pub payload: Bytes,
}

#[cfg(feature = "telemetry")]
#[derive(Default)]
struct FrameCounters {
    frames_in_ok: AtomicU64,
    frames_in_err: AtomicU64,
    frames_out_ok: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    pub_frames_out_ok: AtomicU64,
    pub_frames_out_err: AtomicU64,
    pub_items_out_ok: AtomicU64,
    pub_items_out_err: AtomicU64,
    pub_batches_out_ok: AtomicU64,
    pub_batches_out_err: AtomicU64,
    sub_frames_in_ok: AtomicU64,
    sub_items_in_ok: AtomicU64,
    sub_batches_in_ok: AtomicU64,
    ack_frames_in_ok: AtomicU64,
    ack_items_in_ok: AtomicU64,
    binary_encode_reallocs: AtomicU64,
    text_encode_reallocs: AtomicU64,
}

#[cfg(not(feature = "telemetry"))]
#[allow(dead_code)]
#[derive(Default)]
struct FrameCounters;

#[derive(Debug, Clone)]
pub struct FrameCountersSnapshot {
    pub frames_in_ok: u64,
    pub frames_in_err: u64,
    pub frames_out_ok: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub pub_frames_out_ok: u64,
    pub pub_frames_out_err: u64,
    pub sub_frames_in_ok: u64,
    pub ack_frames_in_ok: u64,
    pub pub_items_out_ok: u64,
    pub pub_items_out_err: u64,
    pub pub_batches_out_ok: u64,
    pub pub_batches_out_err: u64,
    pub sub_items_in_ok: u64,
    pub sub_batches_in_ok: u64,
    pub ack_items_in_ok: u64,
    pub binary_encode_reallocs: u64,
    pub text_encode_reallocs: u64,
}

#[cfg(feature = "telemetry")]
static FRAME_COUNTERS: OnceLock<FrameCounters> = OnceLock::new();
#[cfg(feature = "telemetry")]
static DECODE_ERROR_LOGS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "telemetry")]
fn frame_counters() -> &'static FrameCounters {
    FRAME_COUNTERS.get_or_init(FrameCounters::default)
}

pub fn frame_counters_snapshot() -> FrameCountersSnapshot {
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        FrameCountersSnapshot {
            frames_in_ok: counters.frames_in_ok.load(Ordering::Relaxed),
            frames_in_err: counters.frames_in_err.load(Ordering::Relaxed),
            frames_out_ok: counters.frames_out_ok.load(Ordering::Relaxed),
            bytes_in: counters.bytes_in.load(Ordering::Relaxed),
            bytes_out: counters.bytes_out.load(Ordering::Relaxed),
            pub_frames_out_ok: counters.pub_frames_out_ok.load(Ordering::Relaxed),
            pub_frames_out_err: counters.pub_frames_out_err.load(Ordering::Relaxed),
            sub_frames_in_ok: counters.sub_frames_in_ok.load(Ordering::Relaxed),
            ack_frames_in_ok: counters.ack_frames_in_ok.load(Ordering::Relaxed),
            pub_items_out_ok: counters.pub_items_out_ok.load(Ordering::Relaxed),
            pub_items_out_err: counters.pub_items_out_err.load(Ordering::Relaxed),
            pub_batches_out_ok: counters.pub_batches_out_ok.load(Ordering::Relaxed),
            pub_batches_out_err: counters.pub_batches_out_err.load(Ordering::Relaxed),
            sub_items_in_ok: counters.sub_items_in_ok.load(Ordering::Relaxed),
            sub_batches_in_ok: counters.sub_batches_in_ok.load(Ordering::Relaxed),
            ack_items_in_ok: counters.ack_items_in_ok.load(Ordering::Relaxed),
            binary_encode_reallocs: counters.binary_encode_reallocs.load(Ordering::Relaxed),
            text_encode_reallocs: counters.text_encode_reallocs.load(Ordering::Relaxed),
        }
    }
    #[cfg(not(feature = "telemetry"))]
    {
        FrameCountersSnapshot {
            frames_in_ok: 0,
            frames_in_err: 0,
            frames_out_ok: 0,
            bytes_in: 0,
            bytes_out: 0,
            pub_frames_out_ok: 0,
            pub_frames_out_err: 0,
            sub_frames_in_ok: 0,
            ack_frames_in_ok: 0,
            pub_items_out_ok: 0,
            pub_items_out_err: 0,
            pub_batches_out_ok: 0,
            pub_batches_out_err: 0,
            sub_items_in_ok: 0,
            sub_batches_in_ok: 0,
            ack_items_in_ok: 0,
            binary_encode_reallocs: 0,
            text_encode_reallocs: 0,
        }
    }
}

pub fn reset_frame_counters() {
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.store(0, Ordering::Relaxed);
        counters.frames_in_err.store(0, Ordering::Relaxed);
        counters.frames_out_ok.store(0, Ordering::Relaxed);
        counters.bytes_in.store(0, Ordering::Relaxed);
        counters.bytes_out.store(0, Ordering::Relaxed);
        counters.pub_frames_out_ok.store(0, Ordering::Relaxed);
        counters.pub_frames_out_err.store(0, Ordering::Relaxed);
        counters.pub_items_out_ok.store(0, Ordering::Relaxed);
        counters.pub_items_out_err.store(0, Ordering::Relaxed);
        counters.pub_batches_out_ok.store(0, Ordering::Relaxed);
        counters.pub_batches_out_err.store(0, Ordering::Relaxed);
        counters.sub_frames_in_ok.store(0, Ordering::Relaxed);
        counters.sub_items_in_ok.store(0, Ordering::Relaxed);
        counters.sub_batches_in_ok.store(0, Ordering::Relaxed);
        counters.ack_frames_in_ok.store(0, Ordering::Relaxed);
        counters.ack_items_in_ok.store(0, Ordering::Relaxed);
        counters.binary_encode_reallocs.store(0, Ordering::Relaxed);
        counters.text_encode_reallocs.store(0, Ordering::Relaxed);
    }
}

impl Subscription {
    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        #[cfg(feature = "telemetry")]
        {
            let now = Instant::now();
            if let Some(last) = self.last_poll {
                let gap_ns = now.duration_since(last).as_nanos() as u64;
                t_histogram!("client_sub_consumer_gap_ns").record(gap_ns as f64);
                timings::record_sub_consumer_gap_ns(gap_ns);
            }
            self.last_poll = Some(now);
        }
        // Read from a cached batch first, then from the QUIC stream.
        if let Some(payload) = self.next_from_batch() {
            record_e2e_latency(&payload);
            return Ok(Some(self.event_from_payload(payload)));
        }
        #[cfg(feature = "telemetry")]
        let sample = t_should_sample();
        #[cfg(feature = "telemetry")]
        let read_start = t_now_if(sample);
        let frame = match read_frame_into(&mut self.recv, &mut self.frame_scratch, true).await? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        #[cfg(feature = "telemetry")]
        {
            t_histogram!("client_sub_frame_bytes").record(frame.header.length as f64);
            if let Some(start) = read_start {
                let read_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_read_wait_ns(read_ns);
                t_histogram!("sub_read_wait_ns").record(read_ns as f64);
            }
        }
        #[cfg(feature = "telemetry")]
        let decode_start = t_now_if(sample);
        if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH != 0 {
            let batch = match felix_wire::binary::decode_event_batch(&frame)
                .context("decode binary event batch")
            {
                Ok(batch) => batch,
                Err(err) => {
                    #[cfg(feature = "telemetry")]
                    {
                        let counters = frame_counters();
                        counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                    }
                    log_decode_error("binary_event_batch", &err, &frame);
                    return Err(err);
                }
            };
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
                counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                counters
                    .sub_items_in_ok
                    .fetch_add(batch.payloads.len() as u64, Ordering::Relaxed);
            }
            if let Some(expected) = self.subscription_id
                && expected != batch.subscription_id
            {
                return Err(anyhow::anyhow!(
                    "subscription id mismatch: expected {expected} got {}",
                    batch.subscription_id
                ));
            }
            #[cfg(feature = "telemetry")]
            if let Some(start) = decode_start {
                let decode_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_decode_ns(decode_ns);
                t_histogram!("sub_decode_ns").record(decode_ns as f64);
            }
            return Ok(self.store_batch(batch.payloads));
        }
        let message = match Message::decode(frame.clone()).context("decode message") {
            Ok(message) => message,
            Err(err) => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
                }
                log_decode_error("event_message", &err, &frame);
                return Err(err);
            }
        };
        #[cfg(feature = "telemetry")]
        {
            let counters = frame_counters();
            counters.sub_frames_in_ok.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(feature = "telemetry")]
        if let Some(start) = decode_start {
            let decode_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_decode_ns(decode_ns);
            t_histogram!("sub_decode_ns").record(decode_ns as f64);
        }
        match message {
            Message::Event { payload, .. } => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters.sub_items_in_ok.fetch_add(1, Ordering::Relaxed);
                }
                Ok(self.store_batch(vec![Bytes::from(payload)]))
            }
            Message::EventBatch { payloads, .. } => {
                #[cfg(feature = "telemetry")]
                {
                    let counters = frame_counters();
                    counters.sub_batches_in_ok.fetch_add(1, Ordering::Relaxed);
                    counters
                        .sub_items_in_ok
                        .fetch_add(payloads.len() as u64, Ordering::Relaxed);
                }
                let batch = payloads.into_iter().map(Bytes::from).collect();
                Ok(self.store_batch(batch))
            }
            _ => Err(anyhow::anyhow!("unexpected message on subscription stream")),
        }
    }

    fn store_batch(&mut self, batch: Vec<Bytes>) -> Option<Event> {
        // Store a new batch and return the first event immediately.
        if batch.is_empty() {
            return None;
        }
        self.current_index = 0;
        self.current_batch = Some(batch);
        self.next_from_batch().map(|payload| {
            record_e2e_latency(&payload);
            self.event_from_payload(payload)
        })
    }

    fn next_from_batch(&mut self) -> Option<Bytes> {
        let batch = self.current_batch.as_ref()?;
        if self.current_index >= batch.len() {
            self.current_batch = None;
            return None;
        }
        let payload = batch[self.current_index].clone();
        self.current_index += 1;
        if let Some(batch) = self.current_batch.as_ref()
            && self.current_index >= batch.len()
        {
            self.current_batch = None;
        }
        Some(payload)
    }

    fn event_from_payload(&self, payload: Bytes) -> Event {
        Event {
            tenant_id: Arc::clone(&self.tenant_id),
            namespace: Arc::clone(&self.namespace),
            stream: Arc::clone(&self.stream),
            payload,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Update connection-level subscription counts for metrics.
        let counter = &self.event_conn_counts[self.event_conn_index];
        let mut current = counter.load(Ordering::Relaxed);
        while current > 0 {
            match counter.compare_exchange(
                current,
                current - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    t_gauge!(
                        "felix_client_event_conn_subscriptions",
                        "conn" => self.event_conn_index.to_string()
                    )
                    .set((current - 1) as f64);
                    break;
                }
                Err(next) => current = next,
            }
        }
    }
}

fn event_transport_config(mut base: TransportConfig) -> TransportConfig {
    base.receive_window = DEFAULT_EVENT_CONN_RECV_WINDOW;
    base.stream_receive_window = DEFAULT_EVENT_STREAM_RECV_WINDOW;
    base.send_window = DEFAULT_EVENT_SEND_WINDOW;
    if let Some(value) = read_u64_env("FELIX_EVENT_CONN_RECV_WINDOW") {
        base.receive_window = value;
    }
    if let Some(value) = read_u64_env("FELIX_EVENT_STREAM_RECV_WINDOW") {
        base.stream_receive_window = value;
    }
    if let Some(value) = read_u64_env("FELIX_EVENT_SEND_WINDOW") {
        base.send_window = value;
    }
    base
}

fn cache_transport_config(mut base: TransportConfig) -> TransportConfig {
    base.receive_window = DEFAULT_CACHE_CONN_RECV_WINDOW;
    base.stream_receive_window = DEFAULT_CACHE_STREAM_RECV_WINDOW;
    base.send_window = DEFAULT_CACHE_SEND_WINDOW;
    if let Some(value) = read_u64_env("FELIX_CACHE_CONN_RECV_WINDOW") {
        base.receive_window = value;
    }
    if let Some(value) = read_u64_env("FELIX_CACHE_STREAM_RECV_WINDOW") {
        base.stream_receive_window = value;
    }
    if let Some(value) = read_u64_env("FELIX_CACHE_SEND_WINDOW") {
        base.send_window = value;
    }
    base
}

fn read_u64_env(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn read_usize_env(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

#[cfg(feature = "telemetry")]
fn read_bool_env(key: &str) -> bool {
    std::env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[cfg(feature = "telemetry")]
fn bench_embed_ts_enabled() -> bool {
    read_bool_env(BENCH_EMBED_TS_ENV)
}

#[cfg(not(feature = "telemetry"))]
fn bench_embed_ts_enabled() -> bool {
    false
}

#[cfg(feature = "telemetry")]
fn bench_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

#[cfg(feature = "telemetry")]
fn maybe_append_publish_ts(mut payload: Vec<u8>) -> Vec<u8> {
    if !bench_embed_ts_enabled() {
        return payload;
    }
    let ts = bench_now_ns().to_le_bytes();
    payload.extend_from_slice(&ts);
    payload
}

#[cfg(not(feature = "telemetry"))]
fn maybe_append_publish_ts(payload: Vec<u8>) -> Vec<u8> {
    payload
}

#[cfg(feature = "telemetry")]
fn maybe_append_publish_ts_batch(payloads: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    if !bench_embed_ts_enabled() {
        return payloads;
    }
    payloads.into_iter().map(maybe_append_publish_ts).collect()
}

#[cfg(not(feature = "telemetry"))]
fn maybe_append_publish_ts_batch(payloads: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    payloads
}

#[cfg(feature = "telemetry")]
fn record_e2e_latency(payload: &Bytes) {
    if !bench_embed_ts_enabled() || payload.len() < std::mem::size_of::<u64>() {
        return;
    }
    let mut ts_bytes = [0u8; 8];
    let start = payload.len() - std::mem::size_of::<u64>();
    ts_bytes.copy_from_slice(&payload[start..]);
    let publish_ts = u64::from_le_bytes(ts_bytes);
    let now = bench_now_ns();
    if now >= publish_ts {
        let delta = now - publish_ts;
        t_histogram!("client_e2e_latency_ns").record(delta as f64);
        timings::record_e2e_latency_ns(delta);
    }
}

#[cfg(not(feature = "telemetry"))]
fn record_e2e_latency(_payload: &Bytes) {}

#[cfg(feature = "telemetry")]
fn log_decode_error(context: &str, err: &anyhow::Error, frame: &Frame) {
    let count = DECODE_ERROR_LOGS.fetch_add(1, Ordering::Relaxed);
    if count >= DECODE_ERROR_LOG_LIMIT {
        return;
    }
    let preview_len = frame.payload.len().min(64);
    let preview = &frame.payload[..preview_len];
    let hex = preview
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let printable = preview
        .iter()
        .map(|b| {
            let c = *b as char;
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '.'
            }
        })
        .collect::<String>();
    eprintln!(
        "felix-client decode error: context={context} err={err} frame_len={} payload_len={} preview_hex=\"{hex}\" preview_printable=\"{printable}\"",
        frame.header.length,
        frame.payload.len()
    );
}

#[cfg(not(feature = "telemetry"))]
fn log_decode_error(_context: &str, _err: &anyhow::Error, _frame: &Frame) {}

async fn read_message(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    let frame = match read_frame_into(recv, frame_scratch, false).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    match Message::decode(frame.clone()).context("decode message") {
        Ok(message) => Ok(Some(message)),
        Err(err) => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters.frames_in_err.fetch_add(1, Ordering::Relaxed);
            }
            log_decode_error("read_message", &err, &frame);
            Err(err)
        }
    }
}

async fn read_ack_message_with_timing(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    #[cfg(feature = "telemetry")]
    let sample = t_should_sample();
    #[cfg(feature = "telemetry")]
    #[cfg(feature = "telemetry")]
    #[cfg(feature = "telemetry")]
    let read_start = t_now_if(sample);
    let frame = match read_frame_into(recv, frame_scratch, false).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_read_wait_ns(read_ns);
        t_histogram!("client_ack_read_wait_ns").record(read_ns as f64);
    }
    #[cfg(feature = "telemetry")]
    let decode_start = t_now_if(sample);
    let message = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_decode_ns(decode_ns);
        t_histogram!("client_ack_decode_ns").record(decode_ns as f64);
    }
    Ok(Some(message))
}

async fn read_frame_into(
    recv: &mut RecvStream,
    scratch: &mut BytesMut,
    record_read_await: bool,
) -> Result<Option<Frame>> {
    #[cfg(not(feature = "telemetry"))]
    let _ = record_read_await;
    #[cfg(feature = "telemetry")]
    let sample = record_read_await && t_should_sample();
    #[cfg(feature = "telemetry")]
    let header_start = t_now_if(sample);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    #[cfg(feature = "telemetry")]
    let header_await = header_start.map(|start| start.elapsed().as_nanos() as u64);

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;

    // Safety: we enforce a max frame size (`FELIX_MAX_FRAME_BYTES`) before allocating.
    let max_frame_bytes =
        read_usize_env("FELIX_MAX_FRAME_BYTES").unwrap_or(DEFAULT_MAX_FRAME_BYTES);
    if length > max_frame_bytes {
        return Err(anyhow::anyhow!(
            "frame too large: {length} bytes (cap {max_frame_bytes}); refusing"
        ));
    }

    // Reuse the scratch buffer to avoid per-frame allocations.
    scratch.clear();
    scratch.resize(length, 0u8);
    #[cfg(feature = "telemetry")]
    let payload_start = t_now_if(sample);
    recv.read_exact(&mut scratch[..])
        .await
        .context("read frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let payload_await = payload_start.map(|start| start.elapsed().as_nanos() as u64);
        if let Some(header_ns) = header_await {
            let payload_ns = payload_await.unwrap_or(0);
            let total = header_ns.saturating_add(payload_ns);
            timings::record_sub_read_await_ns(total);
            t_histogram!("client_sub_read_await_ns").record(total as f64);
        }
    }

    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.fetch_add(1, Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }
    Ok(Some(frame))
}

async fn read_frame_cache_timed_into(
    recv: &mut RecvStream,
    sample: bool,
    scratch: &mut BytesMut,
) -> Result<Option<Frame>> {
    #[cfg(not(feature = "telemetry"))]
    let _ = sample;
    #[cfg(feature = "telemetry")]
    let read_start = t_now_if(sample);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_wait_ns(read_ns);
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;

    // Safety: we enforce a max frame size (`FELIX_MAX_FRAME_BYTES`) before allocating.
    let max_frame_bytes =
        read_usize_env("FELIX_MAX_FRAME_BYTES").unwrap_or(DEFAULT_MAX_FRAME_BYTES);
    if length > max_frame_bytes {
        return Err(anyhow::anyhow!(
            "frame too large: {length} bytes (cap {max_frame_bytes}); refusing"
        ));
    }

    #[cfg(feature = "telemetry")]
    #[cfg(feature = "telemetry")]
    #[cfg(feature = "telemetry")]
    let drain_start = t_now_if(sample);
    scratch.clear();
    scratch.resize(length, 0u8);
    recv.read_exact(&mut scratch[..])
        .await
        .context("read frame payload")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = drain_start {
        let drain_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_drain_ns(drain_ns);
    }

    let frame = Frame {
        header,
        payload: scratch.split().freeze(),
    };
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_in_ok.fetch_add(1, Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }
    Ok(Some(frame))
}

async fn write_frame_parts(send: &mut SendStream, frame: &Frame) -> Result<()> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    frame.header.encode_into(&mut header_bytes);
    send.write_all(&header_bytes)
        .await
        .context("write frame header")?;
    send.write_all(&frame.payload)
        .await
        .context("write frame payload")?;
    #[cfg(feature = "telemetry")]
    {
        let counters = frame_counters();
        counters.frames_out_ok.fetch_add(1, Ordering::Relaxed);
        let bytes = (FrameHeader::LEN + frame.payload.len()) as u64;
        counters.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }
    Ok(())
}

async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame_parts(send, &frame).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicServer, TransportConfig};
    use quinn::ClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn in_process_publish_and_subscribe() {
        // Smoke-test the in-process path without any network transport.
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "updates", Default::default())
            .await
            .expect("register");
        let client = InProcessClient::new(broker);
        let mut receiver = client
            .subscribe("t1", "default", "updates")
            .await
            .expect("subscribe");
        client
            .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
            .await
            .expect("publish");
        let msg = receiver.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"payload"));
    }

    #[tokio::test]
    async fn clients_share_broker_state() {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await.expect("tenant");
        broker
            .register_namespace("t1", "default")
            .await
            .expect("namespace");
        broker
            .register_stream("t1", "default", "shared", Default::default())
            .await
            .expect("register");
        let publisher = InProcessClient::new(broker.clone());
        let subscriber = InProcessClient::new(broker);
        let mut receiver = subscriber
            .subscribe("t1", "default", "shared")
            .await
            .expect("subscribe");
        publisher
            .publish(
                "t1",
                "default",
                "shared",
                Bytes::from_static(b"from-publisher"),
            )
            .await
            .expect("publish");
        let msg = receiver.recv().await.expect("recv");
        assert_eq!(msg, Bytes::from_static(b"from-publisher"));
    }

    #[tokio::test]
    async fn cache_worker_exits_on_stream_error() -> Result<()> {
        unsafe {
            std::env::set_var("FELIX_PUB_CONN_POOL", "1");
            std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "1");
            std::env::set_var("FELIX_CACHE_CONN_POOL", "1");
            std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "1");
            std::env::set_var("FELIX_EVENT_CONN_POOL", "1");
        }

        let (server_config, cert) = build_server_config()?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let mut connections = Vec::new();
            for _ in 0..3 {
                let connection = server.accept().await?;
                connections.push(connection);
            }

            let mut closed = false;
            for connection in connections {
                let result = timeout(Duration::from_secs(1), connection.accept_bi()).await;
                if let Ok(Ok((mut send, mut recv))) = result {
                    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
                    let _ = read_frame_into(&mut recv, &mut frame_scratch, false).await;
                    let _ = send.finish();
                    closed = true;
                }
            }

            if !closed {
                return Err(anyhow::anyhow!("server did not accept cache stream"));
            }

            Result::<()>::Ok(())
        });

        let client = Client::connect_with_transport(
            addr,
            "localhost",
            build_client_config(cert)?,
            TransportConfig::default(),
        )
        .await?;

        let err = client
            .cache_get("t1", "default", "cache", "key")
            .await
            .expect_err("cache should fail");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("cache response closed")
                || err_msg.contains("cache worker closed")
                || err_msg.contains("connection lost"),
            "unexpected cache error: {err_msg}"
        );

        let err = client
            .cache_get("t1", "default", "cache", "key")
            .await
            .expect_err("cache worker should be closed");
        assert!(err.to_string().contains("cache worker closed"));

        server_task.await.context("server task join")??;
        Ok(())
    }

    fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
                .context("build server config")?;
        Ok((server_config, cert_der))
    }

    fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert)?;
        Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
    }
}
