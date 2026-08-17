//! QUIC network client and connection pool setup.
//!
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
use felix_wire::{CursorErrorReason, Message, StartPosition};
use quinn::{RecvStream, SendStream};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::client::cache::{CacheRequest, CacheWorker, run_cache_worker_with_limit};
use crate::client::event_router::{EventRouterCommand, spawn_event_router_with_config};
use crate::client::publisher::{PublishWorker, run_publisher_writer_with_limit};
use crate::client::sharding::PublishSharding;
use crate::client::subscription::{Subscription, SubscriptionPipelineConfig};
use crate::config::{
    CACHE_WORKER_QUEUE_DEPTH, ClientConfig, ClientRuntimeConfig, cache_transport_config,
    event_transport_config,
};
use crate::wire::{read_message_with_limit, write_message};

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
    publish_admission: Arc<super::publisher::PublishAdmission>,

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
    runtime_config: ClientRuntimeConfig,
}

/// Periodic path stats for client-side connections, mirroring the broker's
/// `FELIX_CONN_STATS_MS` logging. The client is the sender on the publish path,
/// so its cwnd/rtt is invisible from broker-side stats. Off unless set.
fn spawn_conn_stats_logger(connection: &QuicConnection, role: &'static str) {
    let Some(interval_ms) = std::env::var("FELIX_CONN_STATS_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    else {
        return;
    };
    let connection = connection.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
        loop {
            ticker.tick().await;
            if connection.close_reason().is_some() {
                break;
            }
            let stats = connection.stats();
            tracing::info!(
                role,
                conn = connection.info().id.0,
                mtu = stats.path.current_mtu,
                cwnd = stats.path.cwnd,
                rtt_us = stats.path.rtt.as_micros() as u64,
                congestion_events = stats.path.congestion_events,
                lost_packets = stats.path.lost_packets,
                udp_tx_bytes = stats.udp_tx.bytes,
                udp_tx_datagrams = stats.udp_tx.datagrams,
                tx_data_blocked = stats.frame_tx.data_blocked,
                tx_stream_data_blocked = stats.frame_tx.stream_data_blocked,
                "client quic connection path stats"
            );
        }
    });
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
        let runtime_config = client_config.runtime_config();
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
        let publish_queue_depth = client_config.publish_queue_depth.max(1);
        let publish_admission = Arc::new(super::publisher::PublishAdmission::new(
            client_config.publish_inflight_bytes,
        ));
        let mut publish_connections = Vec::with_capacity(publish_pool_size);
        for _ in 0..publish_pool_size {
            let connection = publish_client.connect(addr, server_name).await?;
            debug!("client established publish connection");
            spawn_conn_stats_logger(&connection, "publish");
            publish_connections.push(connection);
        }
        let mut publish_workers = Vec::with_capacity(publish_pool_size * publish_streams_per_conn);
        for connection in &publish_connections {
            for _ in 0..publish_streams_per_conn {
                let (mut send, mut recv) = connection.open_bi().await?;
                debug!("client opened publish stream");
                let server_flags = authenticate_stream(
                    &mut send,
                    &mut recv,
                    &auth_tenant_id,
                    &auth_token,
                    runtime_config.max_frame_bytes,
                )
                .await?;
                debug!(server_flags, "client publish stream authenticated");
                let (tx, rx) = mpsc::channel(publish_queue_depth);
                // Not colocated with the transport drivers (unlike the
                // subscription read pump): publisher writers block in
                // `write_all` against a full send window, and parking them on
                // the I/O thread starves the drivers they wait on (measured 5x
                // throughput loss).
                let handle = tokio::spawn(run_publisher_writer_with_limit(
                    send,
                    recv,
                    rx,
                    publish_chunk_bytes,
                    runtime_config.max_frame_bytes,
                ));
                publish_workers.push(PublishWorker {
                    tx,
                    handle: tokio::sync::Mutex::new(Some(handle)),
                    request_counter: AtomicU64::new(1),
                    server_flags,
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
                let _ = authenticate_stream(
                    &mut send,
                    &mut recv,
                    &auth_tenant_id,
                    &auth_token,
                    runtime_config.max_frame_bytes,
                )
                .await?;
                debug!(conn_index, "client cache stream authenticated");
                let (tx, rx) = mpsc::channel(CACHE_WORKER_QUEUE_DEPTH);
                tokio::spawn(run_cache_worker_with_limit(
                    conn_index,
                    send,
                    recv,
                    rx,
                    Arc::clone(&cache_conn_counts),
                    runtime_config.max_frame_bytes,
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
            event_stream_routers.push(spawn_event_router_with_config(
                connection.clone(),
                runtime_config.event_router_max_pending,
                runtime_config.max_frame_bytes,
            ));
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
            publish_admission,
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
            runtime_config,
        })
    }

    pub async fn publisher(&self) -> Result<super::publisher::Publisher> {
        Ok(super::publisher::Publisher {
            inner: Arc::new(super::publisher::PublisherInner::with_runtime_config(
                Arc::clone(&self.publish_workers),
                self.publish_sharding,
                Arc::clone(&self.publish_admission),
                self.runtime_config.bench_embed_ts,
            )),
        })
    }

    /// Subscribe from the live tail, delivering only what is published from now.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        self.subscribe_from(tenant_id, namespace, stream, None)
            .await
    }

    /// Subscribe from a chosen position, resuming a durable stream.
    ///
    /// `start: None` is exactly [`Client::subscribe`]. Pass
    /// `Some(StartPosition::Offset(n))` to resume at the first record not yet
    /// seen -- an application that checkpoints the offset of the last event it
    /// handled resumes at that offset plus one.
    ///
    /// Fails with a `CursorTooOld` error if retention has already discarded the
    /// requested offset, rather than silently restarting at the tail: a resume
    /// that quietly skips records is the failure mode this exists to remove.
    pub async fn subscribe_from(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        start: Option<StartPosition>,
    ) -> Result<Subscription> {
        if tenant_id != self.auth_tenant_id {
            return Err(anyhow::anyhow!(
                "tenant mismatch: client auth is scoped to {}",
                self.auth_tenant_id
            ));
        }
        // Round-robin subscriptions across the event connection pool. This local
        // counter picks the connection only -- it must NOT be used as the
        // subscription id itself. It starts at 1 in every Client instance, so two
        // independent clients against the same broker would both request id 1, 2,
        // ... and collide: the broker keys its own subscription/lane bookkeeping on
        // the id the client asks for, and the client silently discards any event
        // batch whose subscription_id doesn't match (see subscription.rs), so a
        // collision manifests as events vanishing rather than any visible error.
        // The broker assigns globally-unique ids from its own atomic counter when we
        // send `subscription_id: None`, so let it do that and use what it returns.
        let rr = self.subscription_counter.fetch_add(1, Ordering::Relaxed);
        let connection_index = rr as usize % self.event_pool_size;
        let connection = &self.event_connections[connection_index];
        let (mut send, mut recv) = connection.open_bi().await?;
        let server_flags = authenticate_stream(
            &mut send,
            &mut recv,
            &self.auth_tenant_id,
            &self.auth_token,
            self.runtime_config.max_frame_bytes,
        )
        .await?;

        // A broker that predates resume ignores the unknown `start` field and
        // subscribes at the tail, then answers `Subscribed` -- so the client
        // would report success while silently losing everything published
        // during the disconnect. That is worse than an error, because the
        // application has no way to notice. `Latest` is safe to send either
        // way: it is what an old broker does anyway.
        let needs_replay = matches!(
            start,
            Some(StartPosition::Earliest) | Some(StartPosition::Offset(_))
        );
        if needs_replay && !felix_wire::supports(server_flags, felix_wire::FLAG_EVENT_BATCH_OFFSETS)
        {
            return Err(anyhow::anyhow!(
                "broker does not support resumable subscriptions (negotiated flags {server_flags:#06x}); \
                 resuming from a position would silently start at the tail instead"
            ));
        }
        let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
        write_message(
            &mut send,
            Message::Subscribe {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                subscription_id: None,
                start,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message_with_limit(
            &mut recv,
            &mut frame_scratch,
            self.runtime_config.max_frame_bytes,
        )
        .await?;
        let subscription_id = match response {
            Some(Message::Subscribed { subscription_id }) => subscription_id,
            Some(Message::Ok) => {
                return Err(anyhow::anyhow!(
                    "subscribe response missing subscription id"
                ));
            }
            Some(Message::SubscribeCursorError {
                reason,
                requested,
                available,
            }) => {
                // Surfaced as a typed error rather than a debug-formatted
                // message, because the two reasons have opposite remedies and an
                // application has to be able to tell them apart in code.
                return Err(SubscribeCursorError {
                    reason,
                    requested,
                    available,
                }
                .into());
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
            connection: connection.clone(),
            queue_capacity: self.runtime_config.client_sub_queue_capacity.max(1),
            queue_policy: self.runtime_config.client_sub_queue_policy,
            subscription_id,
            tenant_id,
            namespace,
            stream,
            event_conn_index: connection_index,
            event_conn_counts: Arc::clone(&self.event_conn_counts),
            max_frame_bytes: self.runtime_config.max_frame_bytes,
            #[cfg(feature = "telemetry")]
            bench_embed_ts: self.runtime_config.bench_embed_ts,
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

/// Authenticate a stream and negotiate frame-flag capabilities.
///
/// Returns the flag bits the broker supports. Capability negotiation rides on
/// the auth handshake because it is already the first round trip on every
/// stream, so it costs no extra latency.
///
/// A broker that predates negotiation ignores `client_flags` (serde skips
/// unknown fields) and answers with a plain `Ok`. That silence is not treated as
/// "supports everything" — it resolves to [`ORIGINAL_V1_FLAGS`], the three bits
/// that existed before negotiation, which is the only assumption that is safe
/// against a broker we cannot interrogate.
async fn authenticate_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
    tenant_id: &str,
    token: &str,
    max_frame_bytes: usize,
) -> Result<u16> {
    write_message(
        send,
        Message::Auth {
            tenant_id: tenant_id.to_string(),
            token: token.to_string(),
            client_flags: Some(felix_wire::KNOWN_FLAGS),
        },
    )
    .await
    .context("send auth")?;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    match read_message_with_limit(recv, &mut scratch, max_frame_bytes).await? {
        Some(Message::AuthOk { server_flags }) => Ok(server_flags),
        // Legacy broker: no advertisement, so assume only the original bits.
        Some(Message::Ok) => Ok(felix_wire::ORIGINAL_V1_FLAGS),
        Some(Message::Error { message }) => Err(anyhow::anyhow!("auth rejected: {message}")),
        Some(other) => Err(anyhow::anyhow!("unexpected auth response: {other:?}")),
        None => Err(anyhow::anyhow!("auth response missing")),
    }
}

/// A resume could not start where it asked to.
///
/// Typed rather than a formatted string so callers can branch: `TooOld` means
/// retention has passed the requested offset and the application must decide
/// whether to accept the gap or start from `earliest`; `InFuture` means the
/// offset does not exist yet, which usually means a checkpoint was written from
/// a different stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub struct SubscribeCursorError {
    pub reason: CursorErrorReason,
    /// The offset that was asked for.
    pub requested: u64,
    /// The nearest offset that would have worked: the oldest retained for
    /// `TooOld`, the current tail for `InFuture`.
    pub available: u64,
}

impl std::fmt::Display for SubscribeCursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            CursorErrorReason::TooOld => write!(
                f,
                "offset {} is no longer retained; oldest available is {}",
                self.requested, self.available
            ),
            CursorErrorReason::InFuture => write!(
                f,
                "offset {} is past the end of the stream; tail is {}",
                self.requested, self.available
            ),
        }
    }
}
