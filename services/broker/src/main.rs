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
//! - The process remains alive until the provided shutdown future completes (by default,
//!   CTRL-C / SIGINT).
//! - On shutdown, we abort background tasks and await them best-effort to avoid leaving
//!   tasks detached.
//!
//! ## TLS note
//! `build_server_config()` currently creates a **dev-only self-signed** certificate for QUIC.
//! Production deployments should use a real certificate chain and should not re-generate keys
//! on each start.

mod controlplane;
mod observability;

use anyhow::{Context, Result};
use broker::{config, quic};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::future::Future;
use std::sync::Arc;

// Tokio async runtime entry point. The broker is primarily I/O-bound (QUIC + HTTP metrics)
// and runs multiple background tasks concurrently.
#[tokio::main]
async fn main() -> Result<()> {
    // Default shutdown trigger: CTRL-C (SIGINT). `run_with_shutdown` is written so we can
    // reuse the same startup logic in tests or alternative hosting environments by passing
    // a different shutdown future.
    run_with_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
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

    // Start an in-process broker.
    // For the MVP we use an in-memory cache backend; production will typically select a
    // durable/clustered backend via configuration.
    let broker = Broker::new(EphemeralCache::new().into());
    tracing::info!("broker started");

    let config = config::BrokerConfig::from_env_or_yaml()?;
    // Configuration is resolved from environment variables (and optionally a YAML file).
    // Keep this early so the remainder of startup is entirely driven by `config`.

    // Start the Prometheus metrics HTTP server. This is separate from QUIC traffic and
    // intentionally lightweight so metrics remain available even under load.
    let metrics_task = tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

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
        tokio::spawn(async move {
            if let Err(err) = quic::serve(quic_server, broker, quic_config).await {
                tracing::warn!(error = %err, "quic accept loop exited");
            }
        })
    };

    // Optional: start a periodic control-plane sync to keep tenant/namespace/stream metadata
    // refreshed. When disabled, the broker relies solely on local registrations.
    let controlplane_task = if let Some(base_url) = config.controlplane_url.clone() {
        let interval_ms = config.controlplane_sync_interval_ms;
        let broker = Arc::clone(&broker);
        Some(tokio::spawn(async move {
            if let Err(err) = controlplane::start_sync(
                broker,
                base_url,
                std::time::Duration::from_millis(interval_ms),
            )
            .await
            {
                tracing::warn!(error = %err, "control plane sync exited");
            }
        }))
    } else {
        tracing::info!("control plane sync disabled (FELIX_CONTROLPLANE_URL not set)");
        None
    };

    // Block until the shutdown signal resolves so the process stays alive.
    // After this point we initiate cooperative shutdown by aborting background tasks.
    shutdown.await;
    // Abort background tasks. We use `abort()` (rather than graceful coordination) because:
    // - This is an MVP wiring layer.
    // - Individual subsystems may also implement their own shutdown in the future.
    accept_task.abort();
    metrics_task.abort();
    if let Some(task) = &controlplane_task {
        task.abort();
    }
    // Best-effort join to avoid leaving detached tasks running during tests.
    // The `Result` values are intentionally ignored because cancellation is expected.
    let _ = accept_task.await;
    let _ = metrics_task.await;
    if let Some(task) = controlplane_task {
        let _ = task.await;
    }
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
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
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
    async fn run_with_shutdown_starts_and_stops() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::unset("FELIX_CP_URL");
        let _g4 = EnvGuard::unset("FELIX_CONTROLPLANE_URL");

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
}
