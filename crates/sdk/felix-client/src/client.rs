//! [`Client`]: one broker, reached over pooled QUIC connections.
//!
//! Publish, cache and event traffic each get their own connection pool, so
//! one workload cannot head-of-line block another and each can be tuned
//! separately. The files below split `Client`'s API by area; the struct and
//! its fields live here.

mod cache;
mod cache_watch;
mod connect;
mod discovery;
mod groups;
mod publish;
mod subscribe;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};

use felix_transport::{QuicClient, QuicConnection};
use tokio::sync::mpsc;

use crate::cache::CacheWorker;
use crate::config::ClientRuntimeConfig;
use crate::connection::{Credentials, EventRouterCommand};
use crate::publish::{PublishAdmission, PublishSharding, PublishWorker};

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
    publish_admission: Arc<PublishAdmission>,

    // Per-stream cache workers: each owns exactly one bi-directional QUIC stream and
    // serializes cache round-trips (encode -> write -> read -> decode).
    cache_workers: Vec<CacheWorker>,

    // Connection pool for subscription event streams.
    event_connections: Vec<QuicConnection>,

    // The distinct broker addresses this client's pools were placed on, in
    // bind order. Recorded at connect because the publish and cache
    // connections are consumed into workers and cannot be asked later.
    listeners: Vec<SocketAddr>,

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
    credentials: Credentials,
    runtime_config: ClientRuntimeConfig,
    /// Optional requests this broker said it implements.
    server_features: u32,
}

#[cfg(test)]
mod tests;
