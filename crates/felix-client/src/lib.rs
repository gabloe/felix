// High-level client for talking to a Felix broker.
// Provides both an in-process wrapper and a QUIC-based network client.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use felix_wire::{AckMode, Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot};

pub mod timings;

/// Client wrapper that talks to an in-process broker.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_client::InProcessClient;
/// use felix_storage::EphemeralCache;
/// use std::sync::Arc;
///
/// let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
    _control_client: QuicClient,
    _cache_client: QuicClient,
    _event_client: QuicClient,
    control_connection: QuicConnection,
    cache_workers: Vec<CacheWorker>,
    event_connections: Vec<QuicConnection>,
    subscription_counter: AtomicU64,
    cache_request_counter: AtomicU64,
    event_pool_size: usize,
    cache_worker_rr: AtomicUsize,
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
        let control_client = QuicClient::bind(bind_addr, client_config.clone(), transport.clone())?;
        let control_connection = control_client.connect(addr, server_name).await?;
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
        let cache_streams_per_conn = std::env::var("FELIX_CACHE_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_STREAMS_PER_CONN);
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
        let mut event_conn_counts = Vec::with_capacity(event_pool_size);
        for _ in 0..event_pool_size {
            event_conn_counts.push(AtomicUsize::new(0));
        }
        Ok(Self {
            _control_client: control_client,
            _cache_client: cache_client,
            _event_client: event_client,
            control_connection,
            cache_workers,
            event_connections,
            subscription_counter: AtomicU64::new(1),
            cache_request_counter: AtomicU64::new(1),
            event_pool_size,
            cache_worker_rr: AtomicUsize::new(0),
            cache_conn_counts,
            event_conn_counts: Arc::new(event_conn_counts),
        })
    }

    pub async fn publisher(&self) -> Result<Publisher> {
        let (send, recv) = self.control_connection.open_bi().await?;
        let (tx, rx) = mpsc::channel(PUBLISH_QUEUE_DEPTH);
        let chunk_bytes = std::env::var("FELIX_PUBLISH_CHUNK_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_CHUNK_BYTES);
        let handle = tokio::spawn(run_publisher_writer(send, recv, rx, chunk_bytes));
        Ok(Publisher {
            inner: Arc::new(PublisherInner {
                tx,
                handle: tokio::sync::Mutex::new(Some(handle)),
            }),
        })
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        let requested_id = self.subscription_counter.fetch_add(1, Ordering::Relaxed);
        let connection_index = requested_id as usize % self.event_pool_size;
        let connection = &self.event_connections[connection_index];
        let (mut send, mut recv) = connection.open_bi().await?;
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
        let response = read_message(&mut recv).await?;
        let subscription_id = match response {
            Some(Message::Subscribed { subscription_id }) => {
                if subscription_id != requested_id {
                    return Err(anyhow::anyhow!(
                        "subscription id mismatch: requested {requested_id} got {subscription_id}"
                    ));
                }
                Some(subscription_id)
            }
            Some(Message::Ok) => None,
            other => return Err(anyhow::anyhow!("subscribe failed: {other:?}")),
        };
        let mut recv = connection.accept_uni().await?;
        let tenant_id = Arc::<str>::from(tenant_id);
        let namespace = Arc::<str>::from(namespace);
        let stream = Arc::<str>::from(stream);
        if subscription_id.is_some()
            && let Some(first) = read_message(&mut recv).await?
        {
            match first {
                Message::EventStreamHello {
                    subscription_id: hello_id,
                } => {
                    if let Some(expected) = subscription_id
                        && expected != hello_id
                    {
                        return Err(anyhow::anyhow!(
                            "subscription id mismatch: expected {expected} got {hello_id}"
                        ));
                    }
                }
                Message::Event { .. } | Message::EventBatch { .. } => {
                    return Err(anyhow::anyhow!(
                        "missing event stream hello for subscription"
                    ));
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "unexpected message on subscription stream: {other:?}"
                    ));
                }
            }
        }
        let current = self.event_conn_counts[connection_index].fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("felix_client_event_conn_subscriptions", "conn" => connection_index.to_string())
            .set(current as f64);
        metrics::counter!("felix_client_event_conn_subscriptions_total", "conn" => connection_index.to_string())
            .increment(1);
        Ok(Subscription {
            recv,
            current_batch: None,
            current_index: 0,
            tenant_id,
            namespace,
            stream,
            subscription_id,
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
        self.cache_worker()
            .tx
            .send(CacheRequest::Put {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;
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
        let request_id = self.cache_request_counter.fetch_add(1, Ordering::Relaxed);
        let message = Message::CacheGet {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            cache: cache.to_string(),
            key: key.to_string(),
            request_id: Some(request_id),
        };
        let (response_tx, response_rx) = oneshot::channel();
        self.cache_worker()
            .tx
            .send(CacheRequest::Get {
                request_id,
                message,
                response: response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("cache worker closed"))?;
        response_rx
            .await
            .map_err(|_| anyhow::anyhow!("cache get response dropped"))?
    }

    fn cache_worker(&self) -> &CacheWorker {
        let index =
            self.cache_worker_rr.fetch_add(1, Ordering::Relaxed) % self.cache_workers.len().max(1);
        let conn_index = self.cache_workers[index].conn_index;
        let current = self.cache_conn_counts[conn_index].fetch_add(1, Ordering::Relaxed) + 1;
        metrics::gauge!("felix_client_cache_conn_ops", "conn" => conn_index.to_string())
            .set(current as f64);
        metrics::counter!("felix_client_cache_conn_ops_total", "conn" => conn_index.to_string())
            .increment(1);
        &self.cache_workers[index]
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

async fn run_cache_worker(
    conn_index: usize,
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<CacheRequest>,
    cache_conn_counts: Arc<Vec<AtomicUsize>>,
) {
    let sample = timings::should_sample();
    if sample {
        let open_ns = 0;
        timings::record_cache_open_stream_ns(open_ns);
    }
    while let Some(request) = rx.recv().await {
        let result = handle_cache_request(&mut send, &mut recv, request).await;
        if result.is_err() {
            break;
        }
    }
    let _ = send.finish();
    cache_conn_counts[conn_index].store(0, Ordering::Relaxed);
}

async fn handle_cache_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
    request: CacheRequest,
) -> Result<()> {
    let sample = timings::should_sample();
    match request {
        CacheRequest::Put {
            request_id,
            message,
            response,
        } => {
            let result = cache_round_trip(send, recv, message, sample, request_id).await;
            let _ = response.send(result.map(|_| ()));
        }
        CacheRequest::Get {
            request_id,
            message,
            response,
        } => {
            let result = cache_round_trip(send, recv, message, sample, request_id).await;
            let _ = response.send(result);
        }
    }
    Ok(())
}

async fn cache_round_trip(
    send: &mut SendStream,
    recv: &mut RecvStream,
    message: Message,
    sample: bool,
    request_id: u64,
) -> Result<Option<Bytes>> {
    let encode_start = sample.then(Instant::now);
    let frame = message.encode().context("encode message")?;
    if let Some(start) = encode_start {
        let encode_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_encode_ns(encode_ns);
    }
    let encoded = frame.encode();
    let write_start = sample.then(Instant::now);
    send.write_all(&encoded).await.context("write frame")?;
    if let Some(start) = write_start {
        let write_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_write_ns(write_ns);
    }
    let frame = match read_frame_cache_timed(recv, sample).await? {
        Some(frame) => frame,
        None => return Err(anyhow::anyhow!("cache response closed")),
    };
    let decode_start = sample.then(Instant::now);
    let response = Message::decode(frame).context("decode message")?;
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
        Message::Ok => Ok(None),
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
    tx: mpsc::Sender<PublishRequest>,
    handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
}

enum PublishRequest {
    Message {
        message: Message,
        ack: AckMode,
        response: oneshot::Sender<Result<()>>,
    },
    BinaryBytes {
        bytes: Bytes,
        sample: bool,
        response: oneshot::Sender<Result<()>>,
    },
    Finish {
        response: oneshot::Sender<Result<()>>,
    },
}

const PUBLISH_QUEUE_DEPTH: usize = 1024;
const CACHE_WORKER_QUEUE_DEPTH: usize = 1024;
const DEFAULT_PUBLISH_CHUNK_BYTES: usize = 16 * 1024;
const DEFAULT_EVENT_CONN_POOL: usize = 8;
const DEFAULT_CACHE_CONN_POOL: usize = 8;
const DEFAULT_CACHE_STREAMS_PER_CONN: usize = 4;
const DEFAULT_EVENT_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_EVENT_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_EVENT_SEND_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_CACHE_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_CACHE_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;

impl Publisher {
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .tx
            .send(PublishRequest::Message {
                message: Message::Publish {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payload,
                    ack: Some(ack),
                },
                ack,
                response: response_tx,
            })
            .await
            .context("enqueue publish")?;
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
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .tx
            .send(PublishRequest::Message {
                message: Message::PublishBatch {
                    tenant_id: tenant_id.to_string(),
                    namespace: namespace.to_string(),
                    stream: stream.to_string(),
                    payloads,
                    ack: Some(ack),
                },
                ack,
                response: response_tx,
            })
            .await
            .context("enqueue publish batch")?;
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
        let sample = timings::should_sample();
        let start = sample.then(Instant::now);
        let bytes =
            felix_wire::binary::encode_publish_batch_bytes(tenant_id, namespace, stream, payloads)?;
        if let Some(start) = start {
            let encode_ns = start.elapsed().as_nanos() as u64;
            timings::record_encode_ns(encode_ns);
            metrics::histogram!("felix_client_encode_ns").record(encode_ns as f64);
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.inner
            .tx
            .send(PublishRequest::BinaryBytes {
                bytes,
                sample,
                response: response_tx,
            })
            .await
            .context("enqueue binary batch")?;
        response_rx.await.context("binary batch response dropped")?
    }

    pub async fn finish(&self) -> Result<()> {
        let handle = {
            let mut guard = self.inner.handle.lock().await;
            guard.take()
        };
        if handle.is_none() {
            return Ok(());
        }
        let handle = handle.expect("handle checked");
        let (response_tx, response_rx) = oneshot::channel();
        if self
            .inner
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
        handle.await.context("publisher writer task")??;
        Ok(())
    }
}

async fn run_publisher_writer(
    mut send: SendStream,
    mut recv: RecvStream,
    mut rx: mpsc::Receiver<PublishRequest>,
    chunk_bytes: usize,
) -> Result<()> {
    while let Some(request) = rx.recv().await {
        match request {
            PublishRequest::Message {
                message,
                ack,
                response,
            } => {
                let sample = timings::should_sample();
                let encode_start = sample.then(Instant::now);
                let frame = match message.encode().context("encode message") {
                    Ok(frame) => frame,
                    Err(err) => {
                        let _ = response.send(Err(err));
                        continue;
                    }
                };
                let encoded = frame.encode();
                if let Some(start) = encode_start {
                    let encode_ns = start.elapsed().as_nanos() as u64;
                    timings::record_encode_ns(encode_ns);
                    metrics::histogram!("felix_client_encode_ns").record(encode_ns as f64);
                }
                let write_start = sample.then(Instant::now);
                let write_result = send.write_all(&encoded).await.context("write frame");
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    metrics::histogram!("felix_client_write_ns").record(write_ns as f64);
                }
                let result = match write_result {
                    Ok(()) => maybe_wait_for_ack(&mut recv, ack).await,
                    Err(err) => Err(err),
                };
                match result {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                    }
                    Err(err) => {
                        let message = err.to_string();
                        let _ = response.send(Err(err));
                        drain_publish_queue(&mut rx, &message).await;
                        return Err(anyhow::anyhow!(message));
                    }
                }
            }
            PublishRequest::BinaryBytes {
                bytes,
                sample,
                response,
            } => {
                let chunk_bytes = chunk_bytes.max(1);
                let write_start = sample.then(Instant::now);
                let mut result = Ok(());
                if bytes.len() <= chunk_bytes {
                    let chunk_start = sample.then(Instant::now);
                    result = send
                        .write_all(&bytes)
                        .await
                        .context("write binary batch frame");
                    if let Some(start) = chunk_start {
                        let await_ns = start.elapsed().as_nanos() as u64;
                        timings::record_send_await_ns(await_ns);
                        metrics::histogram!("client_send_await_ns").record(await_ns as f64);
                    }
                } else {
                    let mut offset = 0usize;
                    while offset < bytes.len() {
                        let end = (offset + chunk_bytes).min(bytes.len());
                        let chunk = bytes.slice(offset..end);
                        let chunk_start = sample.then(Instant::now);
                        result = send
                            .write_all(&chunk)
                            .await
                            .context("write binary batch chunk");
                        if let Some(start) = chunk_start {
                            let await_ns = start.elapsed().as_nanos() as u64;
                            timings::record_send_await_ns(await_ns);
                            metrics::histogram!("client_send_await_ns").record(await_ns as f64);
                        }
                        if result.is_err() {
                            break;
                        }
                        offset = end;
                    }
                }
                if let Some(start) = write_start {
                    let write_ns = start.elapsed().as_nanos() as u64;
                    timings::record_write_ns(write_ns);
                    metrics::histogram!("felix_client_write_ns").record(write_ns as f64);
                }
                match result {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                    }
                    Err(err) => {
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
    let _ = recv.read_to_end(usize::MAX).await;
    Ok(())
}

async fn maybe_wait_for_ack(recv: &mut RecvStream, ack: AckMode) -> Result<()> {
    if ack == AckMode::None {
        return Ok(());
    }
    let response = read_ack_message_with_timing(recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow::anyhow!("publish failed: {response:?}"));
    }
    Ok(())
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
    current_batch: Option<Vec<Bytes>>,
    current_index: usize,
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

impl Subscription {
    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        if let Some(payload) = self.next_from_batch() {
            return Ok(Some(self.event_from_payload(payload)));
        }
        let sample = timings::should_sample();
        let read_start = sample.then(Instant::now);
        let frame = match read_frame(&mut self.recv).await? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        if let Some(start) = read_start {
            let read_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_read_wait_ns(read_ns);
            metrics::histogram!("sub_read_wait_ns").record(read_ns as f64);
        }
        let decode_start = sample.then(Instant::now);
        if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH != 0 {
            let batch = felix_wire::binary::decode_event_batch(&frame)
                .context("decode binary event batch")?;
            if let Some(expected) = self.subscription_id
                && expected != batch.subscription_id
            {
                return Err(anyhow::anyhow!(
                    "subscription id mismatch: expected {expected} got {}",
                    batch.subscription_id
                ));
            }
            if let Some(start) = decode_start {
                let decode_ns = start.elapsed().as_nanos() as u64;
                timings::record_sub_decode_ns(decode_ns);
                metrics::histogram!("sub_decode_ns").record(decode_ns as f64);
            }
            return Ok(self.store_batch(batch.payloads));
        }
        let message = Message::decode(frame).context("decode message")?;
        if let Some(start) = decode_start {
            let decode_ns = start.elapsed().as_nanos() as u64;
            timings::record_sub_decode_ns(decode_ns);
            metrics::histogram!("sub_decode_ns").record(decode_ns as f64);
        }
        match message {
            Message::Event { payload, .. } => Ok(self.store_batch(vec![Bytes::from(payload)])),
            Message::EventBatch { payloads, .. } => {
                let batch = payloads.into_iter().map(Bytes::from).collect();
                Ok(self.store_batch(batch))
            }
            _ => Err(anyhow::anyhow!("unexpected message on subscription stream")),
        }
    }

    fn store_batch(&mut self, batch: Vec<Bytes>) -> Option<Event> {
        if batch.is_empty() {
            return None;
        }
        self.current_index = 0;
        self.current_batch = Some(batch);
        self.next_from_batch()
            .map(|payload| self.event_from_payload(payload))
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
                    metrics::gauge!(
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

async fn read_message(recv: &mut RecvStream) -> Result<Option<Message>> {
    let frame = match read_frame(recv).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
}

async fn read_ack_message_with_timing(recv: &mut RecvStream) -> Result<Option<Message>> {
    let sample = timings::should_sample();
    let read_start = sample.then(Instant::now);
    let frame = match read_frame(recv).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_read_wait_ns(read_ns);
        metrics::histogram!("client_ack_read_wait_ns").record(read_ns as f64);
    }
    let decode_start = sample.then(Instant::now);
    let message = Message::decode(frame).context("decode message")?;
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_decode_ns(decode_ns);
        metrics::histogram!("client_ack_decode_ns").record(decode_ns as f64);
    }
    Ok(Some(message))
}

async fn read_frame(recv: &mut RecvStream) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

async fn read_frame_cache_timed(recv: &mut RecvStream, sample: bool) -> Result<Option<Frame>> {
    let read_start = sample.then(Instant::now);
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_wait_ns(read_ns);
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;

    let drain_start = sample.then(Instant::now);
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    if let Some(start) = drain_start {
        let drain_ns = start.elapsed().as_nanos() as u64;
        timings::record_cache_read_drain_ns(drain_ns);
    }

    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    let encoded = frame.encode();
    send.write_all(&encoded).await.context("write frame")
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;

    #[tokio::test]
    async fn in_process_publish_and_subscribe() {
        // Smoke-test the in-process path without any network transport.
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
}
