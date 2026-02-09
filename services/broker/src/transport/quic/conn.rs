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
use super::handlers::publish::{PublishContext, PublishJob, decrement_depth};

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
/// use broker::transport::quic::serve;
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
    let publish_ctx = build_publish_context(Arc::clone(&broker), &config);
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
        let publish_ctx = publish_ctx.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(broker, connection, config, auth, publish_ctx).await
            {
                tracing::warn!(error = %err, "quic connection handler failed");
            }
        });
    }
}

fn build_publish_context(broker: Arc<Broker>, config: &BrokerConfig) -> PublishContext {
    // NOTE: This is intentionally global for the process (not per-connection).
    // With per-connection worker pools, adding more publisher connections multiplied
    // concurrent broker.publish_batch callers and caused lock contention on shared broker state.
    let worker_count = config.pub_workers_per_conn.max(1);
    let publish_queue_depth = config.pub_queue_depth.max(1);
    let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker_txs = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        #[cfg(not(feature = "perf_debug"))]
        let _ = worker_id;
        #[cfg(feature = "perf_debug")]
        let worker_label = worker_id.to_string();
        let (publish_tx, mut publish_rx) = mpsc::channel::<PublishJob>(publish_queue_depth);
        let queue_depth_worker = Arc::clone(&queue_depth);
        let broker_for_worker = Arc::clone(&broker);
        tokio::spawn(async move {
            while let Some(job) = publish_rx.recv().await {
                #[cfg(feature = "perf_debug")]
                metrics::counter!(
                    "felix_perf_publish_worker_wakeups_total",
                    "worker" => worker_label.clone()
                )
                .increment(1);
                let _ = decrement_depth(
                    &queue_depth_worker,
                    &GLOBAL_INGRESS_DEPTH,
                    "felix_broker_ingress_queue_depth",
                );
                #[cfg(feature = "perf_debug")]
                let worker_start = std::time::Instant::now();
                let result = broker_for_worker
                    .publish_batch(&job.tenant_id, &job.namespace, &job.stream, &job.payloads)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
                #[cfg(feature = "perf_debug")]
                {
                    let ns = worker_start.elapsed().as_nanos() as u64;
                    metrics::histogram!("felix_perf_pub_worker_ns", "worker" => worker_label.clone())
                        .record(ns as f64);
                    metrics::counter!(
                        "felix_perf_publish_worker_jobs_total",
                        "worker" => worker_label.clone()
                    )
                    .increment(1);
                }
                if let Some(response) = job.response {
                    let _ = response.send(result);
                }
            }
        });
        worker_txs.push(publish_tx);
    }
    PublishContext {
        workers: Arc::new(worker_txs),
        worker_count,
        depth: queue_depth,
        wait_timeout: Duration::from_millis(config.publish_queue_wait_timeout_ms),
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
    publish_ctx: PublishContext,
) -> Result<()> {
    loop {
        // Accept both bidirectional control streams and uni-directional publish streams.
        tokio::select! {
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, stats = ?connection.stats(), "quic connection closed");
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
                        tracing::info!(error = %err, stats = ?connection.stats(), "quic connection closed");
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result};
    use bytes::Bytes;
    use felix_storage::EphemeralCache;
    use felix_transport::TransportConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn build_publish_context_clamps_worker_and_queue_minimums() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "demo", Default::default())
            .await?;

        let mut config = BrokerConfig::from_env()?;
        config.pub_workers_per_conn = 0;
        config.pub_queue_depth = 0;
        config.publish_queue_wait_timeout_ms = 17;
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config);
        assert_eq!(publish_ctx.worker_count, 1);
        assert_eq!(publish_ctx.workers.len(), 1);
        assert_eq!(publish_ctx.wait_timeout, Duration::from_millis(17));

        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
                payloads: vec![Bytes::from_static(b"ok")],
                response: Some(response_tx),
            })
            .await
            .expect("enqueue publish");
        response_rx.await.expect("worker response")?;
        Ok(())
    }

    #[tokio::test]
    async fn build_publish_context_worker_returns_publish_error() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        let config = BrokerConfig::from_env()?;
        let publish_ctx = build_publish_context(broker, &config);

        let (response_tx, response_rx) = oneshot::channel();
        publish_ctx.workers[0]
            .send(PublishJob {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payloads: vec![Bytes::from_static(b"payload")],
                response: Some(response_tx),
            })
            .await
            .expect("enqueue publish");
        let err = response_rx
            .await
            .expect("worker response")
            .expect_err("publish should fail");
        assert!(err.to_string().contains("stream"));
        Ok(())
    }

    #[tokio::test]
    async fn handle_connection_returns_ok_on_closed_connection() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;

        let config = BrokerConfig::from_env()?;
        let publish_ctx = build_publish_context(Arc::clone(&broker), &config);
        let auth = Arc::new(BrokerAuth::new("http://127.0.0.1".to_string()));

        let rcgen::CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            handle_connection(broker, connection, config, auth, publish_ctx).await
        });

        let mut roots = RootCertStore::empty();
        roots.add(cert_der)?;
        let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
        let client = felix_transport::QuicClient::bind(
            "0.0.0.0:0".parse()?,
            quinn,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;
        drop(connection);

        let result = tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .context("handle connection timeout")??;
        assert!(result.is_ok());
        Ok(())
    }
}
