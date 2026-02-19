//! QUIC network client and connection pool setup.
//!
//! # Purpose
//! Establishes the pooled QUIC connections and streams used by the client for
//! publish, cache, and subscription workloads, and wires them to background
//! worker tasks that handle the wire protocol.
//!
//! # Design notes
//! Publish, cache, and event streams are separated to avoid head-of-line
//! blocking between workloads and to allow distinct transport tuning.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use felix_wire::Message;
use quinn::{RecvStream, SendStream};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::client::cache::{CacheRequest, CacheWorker, run_cache_worker};
use crate::client::event_router::{EventRouterCommand, spawn_event_router};
use crate::client::publisher::{PublishWorker, run_publisher_writer};
use crate::client::sharding::PublishSharding;
use crate::client::subscription::{Subscription, SubscriptionPipelineConfig};
use crate::config::{
    CACHE_WORKER_QUEUE_DEPTH, ClientConfig, ClientSubQueuePolicy, PUBLISH_QUEUE_DEPTH,
    cache_transport_config, event_transport_config, runtime_config,
};
use crate::wire::{read_message, write_message};

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
    auth_tenant_id: String,
    auth_token: String,
}

impl Client {
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_transport(addr, server_name, client_config, TransportConfig::default())
            .await
    }

    pub async fn connect_with_transport(
        addr: SocketAddr,
        server_name: &str,
        client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        client_config.install();
        let auth_tenant_id = client_config
            .auth_tenant_id
            .clone()
            .context("FELIX_AUTH_TENANT must be set")?;
        let auth_token = client_config
            .auth_token
            .clone()
            .context("FELIX_AUTH_TOKEN must be set")?;
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("bind addr");
        let publish_client =
            QuicClient::bind(bind_addr, client_config.quinn.clone(), transport.clone())?;
        let publish_pool_size = client_config.publish_conn_pool;
        let publish_streams_per_conn = client_config.publish_streams_per_conn;
        if publish_pool_size == 0 || publish_streams_per_conn == 0 {
            return Err(anyhow::anyhow!("publish pool misconfigured"));
        }
        let publish_chunk_bytes = client_config.publish_chunk_bytes;
        let mut publish_connections = Vec::with_capacity(publish_pool_size);
        for _ in 0..publish_pool_size {
            let connection = publish_client.connect(addr, server_name).await?;
            debug!("client established publish connection");
            publish_connections.push(connection);
        }
        let mut publish_workers = Vec::with_capacity(publish_pool_size * publish_streams_per_conn);
        for connection in &publish_connections {
            for _ in 0..publish_streams_per_conn {
                let (mut send, mut recv) = connection.open_bi().await?;
                debug!("client opened publish stream");
                authenticate_stream(&mut send, &mut recv, &auth_tenant_id, &auth_token).await?;
                debug!("client publish stream authenticated");
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
        let publish_sharding = client_config.publish_sharding;
        // Cache connections are pooled to avoid head-of-line blocking.
        // DESIGN NOTE:
        // We pool *connections* and then open multiple *streams per connection*.
        // This avoids (a) creating a new QUIC connection per cache op and
        // (b) HOL blocking between independent cache ops on a single stream.
        let cache_pool_size = client_config.cache_conn_pool;
        let cache_transport = cache_transport_config(transport.clone(), &client_config);
        let cache_client =
            QuicClient::bind(bind_addr, client_config.quinn.clone(), cache_transport)?;
        let mut cache_connections = Vec::with_capacity(cache_pool_size);
        for _ in 0..cache_pool_size {
            let connection = cache_client.connect(addr, server_name).await?;
            debug!("client established cache connection");
            cache_connections.push(connection);
        }
        // Each cache connection runs multiple independent bi-directional streams.
        let cache_streams_per_conn = client_config.cache_streams_per_conn;
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
                let (mut send, mut recv) = connection.open_bi().await?;
                debug!(conn_index, "client opened cache stream");
                authenticate_stream(&mut send, &mut recv, &auth_tenant_id, &auth_token).await?;
                debug!(conn_index, "client cache stream authenticated");
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
        let event_pool_size = client_config.event_conn_pool;
        let event_transport = event_transport_config(transport, &client_config);
        let event_client = QuicClient::bind(bind_addr, client_config.quinn, event_transport)?;
        let mut event_connections = Vec::with_capacity(event_pool_size);
        for _ in 0..event_pool_size {
            let connection = event_client.connect(addr, server_name).await?;
            debug!("client established event connection");
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
            auth_tenant_id,
            auth_token,
        })
    }

    pub async fn publisher(&self) -> Result<super::publisher::Publisher> {
        Ok(super::publisher::Publisher {
            inner: Arc::new(super::publisher::PublisherInner::new(
                Arc::clone(&self.publish_workers),
                self.publish_sharding,
            )),
        })
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        // Round-robin subscriptions across the event connection pool.
        let requested_id = self.subscription_counter.fetch_add(1, Ordering::Relaxed);
        let connection_index = requested_id as usize % self.event_pool_size;
        let connection = &self.event_connections[connection_index];
        let (mut send, mut recv) = connection.open_bi().await?;
        authenticate_stream(&mut send, &mut recv, &self.auth_tenant_id, &self.auth_token).await?;
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
        t_gauge!(
            "felix_client_event_conn_subscriptions",
            "conn" => connection_index.to_string()
        )
        .set(current as f64);
        t_counter!(
            "felix_client_event_conn_subscriptions_total",
            "conn" => connection_index.to_string()
        )
        .increment(1);
        Ok(Subscription::spawn_pipeline(SubscriptionPipelineConfig {
            recv,
            queue_capacity: client_sub_queue_capacity(),
            queue_policy: client_sub_queue_policy(),
            subscription_id,
            tenant_id,
            namespace,
            stream,
            event_conn_index: connection_index,
            event_conn_counts: Arc::clone(&self.event_conn_counts),
        }))
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
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
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
        t_counter!(
            "felix_client_cache_conn_ops_total",
            "conn" => conn_index.to_string()
        )
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

async fn authenticate_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
    tenant_id: &str,
    token: &str,
) -> Result<()> {
    write_message(
        send,
        Message::Auth {
            tenant_id: tenant_id.to_string(),
            token: token.to_string(),
        },
    )
    .await
    .context("send auth")?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    match read_message(recv, &mut scratch).await? {
        Some(Message::Ok) => Ok(()),
        Some(Message::Error { message }) => Err(anyhow::anyhow!("auth rejected: {message}")),
        Some(other) => Err(anyhow::anyhow!("unexpected auth response: {other:?}")),
        None => Err(anyhow::anyhow!("auth response missing")),
    }
}

fn client_sub_queue_capacity() -> usize {
    runtime_config().client_sub_queue_capacity.max(1)
}

fn client_sub_queue_policy() -> ClientSubQueuePolicy {
    runtime_config().client_sub_queue_policy
}
