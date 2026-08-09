//! Broker service main entry point.
//!
//! This binary is the runnable “broker node” for Felix. It wires together:
//! - **Core broker logic** (`felix_broker::Broker`) backed by an in-memory cache implementation
//!   in this MVP (`felix_storage::EphemeralCache`).
//! - **QUIC transport server** (`felix_transport::QuicServer`) and the broker’s QUIC accept loop
//!   (`broker::quic::serve`).
//! - **Observability**: a Prometheus metrics endpoint (and optionally tracing/OTel plumbing)
//!   via the local `observability` module.
//! - **Control-plane sync**: optional periodic synchronization of scope metadata from a
//!   configured control-plane endpoint.
//!
//! ## Process lifecycle
//! - The broker starts long-running background tasks (QUIC accept loop, metrics server,
//!   and optional control-plane sync).
//! - The process remains alive until the provided shutdown future completes. In
//!   production that is SIGTERM or SIGINT: SIGTERM is what Kubernetes, systemd, and
//!   `docker stop` send, so handling only SIGINT would abort in-flight work on every
//!   rolling update.
//! - Shutdown then runs a bounded drain, in order: readiness goes false so load
//!   balancers stop routing here, the listener stops admitting new connections,
//!   in-flight connections finish, and finally the metrics server stops. Anything
//!   still running when `FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS` expires is force-cancelled
//!   and named in a warning. See `felix_common::lifecycle`.
//!
//! ## TLS note
//! `build_server_config()` currently creates a **dev-only self-signed** certificate for QUIC.
//! Production deployments should use a real certificate chain and should not re-generate keys
//! on each start.

mod controlplane;
mod observability;
#[cfg(test)]
mod test_support;

use anyhow::{Context, Result};
use broker::{auth::BrokerAuth, config, quic};
use felix_broker::Broker;
use felix_common::lifecycle::{self, DrainBudget, Readiness};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::PrivatePkcs8KeyDer;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// Tokio async runtime entry point. The broker is primarily I/O-bound (QUIC + HTTP metrics)
// and runs multiple background tasks concurrently.
#[tokio::main]
async fn main() -> Result<()> {
    // Default shutdown trigger: SIGTERM or SIGINT. SIGTERM is what Kubernetes,
    // systemd, and `docker stop` actually send; SIGINT only covers an interactive
    // Ctrl-C. `run_with_shutdown` is written so we can reuse the same startup logic
    // in tests or alternative hosting environments by passing a different future.
    run_with_shutdown(lifecycle::termination_signal()).await
}

/// Start the broker and run until the provided `shutdown` future resolves.
///
/// This indirection makes the process lifecycle explicit and testable:
/// - In production, `shutdown` is typically CTRL-C.
/// - In tests, callers can pass a bounded timer or a oneshot receiver.
///
/// The function is responsible for spawning background tasks and ensuring they are
/// cancelled when shutdown is requested.
async fn run_with_shutdown<F>(shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-broker");
    // Observability is initialized first so any subsequent startup logs/metrics are captured.

    let config = config::BrokerConfig::from_env_or_yaml()?;
    // Lifecycle coordination. `readiness` gates `/ready`; the two tokens separate
    // "stop admitting connections" from "stop serving probes", because the metrics
    // endpoint has to outlive the drain — that is how an operator watches the drain
    // happen. `connections` tracks per-connection tasks so the drain can wait for
    // in-flight work instead of aborting it.
    let readiness = Readiness::ready();
    let accept_shutdown = CancellationToken::new();
    let sync_shutdown = CancellationToken::new();
    let metrics_shutdown = CancellationToken::new();
    let connections = TaskTracker::new();
    tracing::info!(
        lanes = config.subscriber_writer_lanes.max(1),
        queue_bound = config.subscriber_lane_queue_depth.max(1),
        queue_mode = ?config.subscriber_lane_queue_policy,
        shard = ?config.subscriber_lane_shard,
        single_writer_per_conn = config.subscriber_single_writer_per_conn,
        "sub egress lanes ENABLED"
    );
    // Configuration is resolved from environment variables (and optionally a YAML file).
    // Keep this early so the remainder of startup is entirely driven by `config`.

    // Start an in-process broker.
    // For the MVP we use an in-memory cache backend; production will typically select a
    // durable/clustered backend via configuration.
    let broker = Broker::new(EphemeralCache::new().into())
        .with_topic_capacity(config.subscriber_queue_capacity.max(1))
        .context("configure subscriber queue depth")?
        .with_subscriber_queue_policy(config.subscriber_queue_policy);
    tracing::info!("broker started");
    let controlplane_url = config
        .controlplane_url
        .clone()
        .context("FELIX_CONTROLPLANE_URL must be set for auth")?;
    let auth = Arc::new(BrokerAuth::new(controlplane_url));

    // Start the Prometheus metrics HTTP server. This is separate from QUIC traffic and
    // intentionally lightweight so metrics remain available even under load.
    let metrics_task = {
        let metrics_shutdown = metrics_shutdown.clone();
        tokio::spawn(observability::serve_metrics(
            metrics_handle,
            config.metrics_bind,
            readiness.clone(),
            async move { metrics_shutdown.cancelled().await },
        ))
    };

    // Build and bind the QUIC listener. `build_server_config` currently uses a self-signed
    // certificate suitable for local development.
    let bind_addr = config.quic_bind;
    let server_config = build_server_config().context("build QUIC server config")?;

    // Apply transport-level configuration (flow control windows, pooling behavior, etc.)
    // derived from broker config.
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());
    let quic_server = Arc::new(
        QuicServer::bind(bind_addr, server_config, transport).context("bind QUIC listener")?,
    );
    tracing::info!(addr = %quic_server.local_addr()?, "quic listener started");

    // Start accepting QUIC connections in a background task.
    // If the accept loop exits due to an error, we log and continue shutdown normally.
    let broker = Arc::new(broker);
    let accept_task = {
        let quic_server = Arc::clone(&quic_server);
        let broker = Arc::clone(&broker);
        let quic_config = config.clone();
        let auth = Arc::clone(&auth);
        let accept_shutdown = accept_shutdown.clone();
        let connections = connections.clone();
        tokio::spawn(async move {
            if let Err(err) = quic::serve_with_shutdown(
                quic_server,
                broker,
                quic_config,
                auth,
                accept_shutdown,
                connections,
            )
            .await
            {
                tracing::warn!(error = %err, "quic accept loop exited");
            }
        })
    };

    // Optional: start a periodic control-plane sync to keep tenant/namespace/stream metadata
    // refreshed. When disabled, the broker relies solely on local registrations.
    let controlplane_task = if let Some(base_url) = config.controlplane_url.clone() {
        let interval_ms = config.controlplane_sync_interval_ms;
        let broker = Arc::clone(&broker);
        let sync_shutdown = sync_shutdown.clone();
        Some(tokio::spawn(async move {
            // `start_sync` polls forever, so cancellation is what ends it. Dropping
            // it mid-iteration is safe: the sync is a read-only metadata refresh
            // whose cursor only advances on success, so an interrupted iteration is
            // the same case as a failed one and is simply re-fetched on next start.
            tokio::select! {
                _ = sync_shutdown.cancelled() => {
                    tracing::info!("control plane sync stopped");
                }
                result = controlplane::start_sync(
                    broker,
                    base_url,
                    Duration::from_millis(interval_ms),
                ) => {
                    if let Err(err) = result {
                        tracing::warn!(error = %err, "control plane sync exited");
                    }
                }
            }
        }))
    } else {
        tracing::info!("control plane sync disabled (FELIX_CONTROLPLANE_URL not set)");
        None
    };

    // Block until the shutdown signal resolves so the process stays alive.
    shutdown.await;

    // Step 1: stop advertising readiness. Load balancers and the Kubernetes
    // endpoints controller drop this instance from rotation while it can still
    // serve, so new traffic is steered elsewhere rather than hitting a closing
    // listener. This must happen before anything stops working.
    readiness.begin_draining();
    tracing::info!("readiness set to draining");

    // Step 2: stop admitting new connections. In-flight ones are untouched.
    accept_shutdown.cancel();

    // Step 3: drain in-flight work against a single shared deadline.
    let mut budget = DrainBudget::new(Duration::from_millis(config.shutdown_drain_timeout_ms));
    tracing::info!(
        deadline_ms = config.shutdown_drain_timeout_ms,
        "draining in-flight work"
    );

    // Closing the tracker is what lets `wait()` resolve; without it the wait would
    // hang until the deadline even with no connections left.
    connections.close();
    budget.drain("quic_connections", connections.wait()).await;

    let mut accept_task = accept_task;
    if !budget
        .drain("quic_accept_loop", async {
            let _ = (&mut accept_task).await;
        })
        .await
    {
        accept_task.abort();
    }

    let mut controlplane_task = controlplane_task;
    if let Some(task) = &mut controlplane_task {
        sync_shutdown.cancel();
        if !budget
            .drain("controlplane_sync", async {
                let _ = (&mut *task).await;
            })
            .await
        {
            task.abort();
        }
    }

    // Step 4: metrics last, so `/ready` keeps reporting "draining" and `/metrics`
    // stays scrapeable for the whole drain. This is the window in which an operator
    // can actually see what the broker is doing while it shuts down.
    metrics_shutdown.cancel();
    let mut metrics_task = metrics_task;
    if !budget
        .drain("metrics_server", async {
            let _ = (&mut metrics_task).await;
        })
        .await
    {
        metrics_task.abort();
    }

    budget.report();
    tracing::info!("broker stopped");
    Ok(())
}

/// Build the QUIC server TLS configuration.
///
/// Current behavior:
/// - Generates a fresh self-signed certificate for `localhost` at startup.
/// - Configures Quinn/Rustls with that certificate.
///
/// This is convenient for local development but **not appropriate for production**.
/// Production should load a real certificate chain and private key (and should avoid
/// regenerating keys on each start).
fn build_server_config() -> Result<ServerConfig> {
    // Dev-only self-signed TLS config for QUIC endpoints.
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok(ServerConfig::with_single_cert(
        vec![cert_der],
        key_der.into(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }

        fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }

    // Basic sanity check that TLS config generation succeeds.
    #[test]
    fn build_server_config_smoke() -> Result<()> {
        let _config = build_server_config()?;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn run_with_shutdown_starts_and_stops() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::unset("FELIX_CP_URL");
        let _g4 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let _ = shutdown_tx.send(());
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown timeout")?;
        result?;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn run_with_shutdown_controlplane_enabled() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::set("FELIX_CP_URL", "http://127.0.0.1:1");
        let _g4 = EnvGuard::set("FELIX_CP_SYNC_INTERVAL_MS", "1");
        let _g5 = EnvGuard::set("FELIX_CONTROLPLANE_URL", "http://127.0.0.1:1");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown timeout")?;
        result?;
        Ok(())
    }
}
