//! QUIC connection accept loop and per-connection worker setup.
//!
//! # Purpose and responsibility
//! Accepts incoming QUIC connections, configures per-connection publish workers,
//! and spawns stream handlers for publish, subscribe, and cache workloads.
//!
//! # Where it fits in Felix
//! The broker's QUIC transport entrypoint; it wires network connections to the
//! publish/subscribe protocol handlers.
//!
//! # Key invariants and assumptions
//! - Each connection gets its own publish worker pool for isolation.
//! - Ingress depth counters must be decremented when queues drain or close.
//!
//! # Security considerations
//! - Authentication is performed per stream via the `BrokerAuth` handler.
//! - Errors are logged without leaking payload contents.
use anyhow::Result;
use felix_broker::Broker;
use felix_broker::timings as broker_publish_timings;
use felix_transport::{QuicConnection, QuicServer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::auth::BrokerAuth;
use crate::config::BrokerConfig;
use crate::timings;

use super::GLOBAL_INGRESS_DEPTH;
use super::handlers::publish::{
    PublishContext, PublishJob, decrement_depth, reset_local_depth_only,
};

use super::streams::{handle_stream, handle_uni_stream};

/// Serve incoming QUIC connections for the broker.
///
/// # What it does
/// Runs the accept loop, spawns a task per connection, and configures timing
/// telemetry based on the broker configuration.
///
/// # Why it exists
/// Centralizes connection lifecycle handling and isolates per-connection work.
///
/// # Invariants
/// - Timing telemetry is disabled when `disable_timings` is set.
/// - Each accepted connection is handled in its own task.
///
/// # Errors
/// - Propagates QUIC accept errors from the server.
///
/// # Example
/// ```rust,no_run
/// use std::sync::Arc;
/// use broker::transport::quic::conn::serve;
/// # async fn run(server: Arc<felix_transport::QuicServer>, broker: Arc<felix_broker::Broker>, config: broker::config::BrokerConfig, auth: Arc<broker::auth::BrokerAuth>) {
/// let _ = serve(server, broker, config, auth).await;
/// # }
/// ```
pub async fn serve(
    server: Arc<QuicServer>,
    broker: Arc<Broker>,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
) -> Result<()> {
    // Main accept loop: spawn a task per incoming QUIC connection.
    if config.disable_timings {
        timings::set_enabled(false);
        broker_publish_timings::set_enabled(false);
    }
    loop {
        // Accept the next QUIC connection.
        let connection = server.accept().await?;
        let broker = Arc::clone(&broker);
        let config = config.clone();
        let auth = Arc::clone(&auth);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(broker, connection, config, auth).await {
                tracing::warn!(error = %err, "quic connection handler failed");
            }
        });
    }
}

/// Handle a single QUIC connection and its streams.
///
/// # What it does
/// Creates per-connection publish workers and dispatches incoming bi/uni streams
/// to their respective handlers.
///
/// # Why it exists
/// Keeps per-connection state (publish queues and depth counters) scoped to the
/// connection lifecycle.
///
/// # Invariants
/// - `worker_count` and `publish_queue_depth` are at least 1.
/// - Global ingress depth counters are decremented when workers exit.
///
/// # Errors
/// - Returns on connection-level QUIC errors.
pub(crate) async fn handle_connection(
    broker: Arc<Broker>,
    connection: QuicConnection,
    config: BrokerConfig,
    auth: Arc<BrokerAuth>,
) -> Result<()> {
    // One QUIC connection can multiplex multiple streams.
    let worker_count = config.pub_workers_per_conn.max(1);
    let publish_queue_depth = config.pub_queue_depth.max(1);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker_txs = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(publish_queue_depth);
        let queue_depth_worker = Arc::clone(&queue_depth);
        let broker_for_worker = Arc::clone(&broker);
        // Per-connection publish worker:
        // - Serializes calls into broker.publish_batch (reduces internal contention).
        // - Provides a single place to account ingress queue depth and to complete commit-ack oneshots.
        // - Note: this is per-connection, not global. Global fairness is handled upstream by QUIC
        //   scheduling and per-connection backpressure.
        tokio::spawn(async move {
            while let Some(job) = publish_rx.recv().await {
                // Decrement local + global ingress depth when a job is consumed.
                let _ = decrement_depth(
                    &queue_depth_worker,
                    &GLOBAL_INGRESS_DEPTH,
                    "felix_broker_ingress_queue_depth",
                );
                // Publish the batch and forward the result to the caller if requested.
                let result = broker_for_worker
                    .publish_batch(&job.tenant_id, &job.namespace, &job.stream, &job.payloads)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
                if let Some(response) = job.response {
                    let _ = response.send(result);
                }
            }
            // Drain remaining depth on worker shutdown to keep counters consistent.
            reset_local_depth_only(
                &queue_depth_worker,
                &GLOBAL_INGRESS_DEPTH,
                "felix_broker_ingress_queue_depth",
            );
        });
        worker_txs.push(publish_tx);
    }
    let publish_ctx = PublishContext {
        workers: Arc::new(worker_txs),
        worker_count,
        depth: Arc::clone(&queue_depth),
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
    };
    loop {
        // Accept both bidirectional control streams and uni-directional publish streams.
        tokio::select! {
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        // Reset ingress depth when connection shuts down unexpectedly.
                        reset_local_depth_only(
                            &queue_depth,
                            &GLOBAL_INGRESS_DEPTH,
                            "felix_broker_ingress_queue_depth",
                        );
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let connection = connection.clone();
                let config = config.clone();
                let auth = Arc::clone(&auth);
                let publish_ctx = publish_ctx.clone();
                tokio::spawn(async move {
                    // Dispatch the bidirectional control stream handler.
                    if let Err(err) = handle_stream(
                        broker,
                        connection,
                        config,
                        auth,
                        publish_ctx,
                        send,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic stream handler failed");
                    }
                });
            }
            result = connection.accept_uni() => {
                let recv = match result {
                    Ok(recv) => recv,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        // Reset ingress depth when connection shuts down unexpectedly.
                        reset_local_depth_only(
                            &queue_depth,
                            &GLOBAL_INGRESS_DEPTH,
                            "felix_broker_ingress_queue_depth",
                        );
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                let config = config.clone();
                let auth = Arc::clone(&auth);
                let publish_ctx = publish_ctx.clone();
                tokio::spawn(async move {
                    // Dispatch the unidirectional publish stream handler.
                    if let Err(err) = handle_uni_stream(
                        broker,
                        config,
                        auth,
                        publish_ctx,
                        recv,
                    )
                    .await
                    {
                        tracing::warn!(error = %err, "quic uni stream handler failed");
                    }
                });
            }
        }
    }
}
