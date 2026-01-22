// Broker service main entry point.
mod controlplane;
mod observability;

use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
#[cfg(feature = "filesystem_storage")]
use felix_storage::SimpleFileStorage;

use broker::config::BrokerConfig;
#[cfg(not(feature = "filesystem_storage"))]
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::task::JoinHandle;

fn get_broker() -> Result<Arc<Broker>> {
    #[cfg(feature = "filesystem_storage")]
    let broker = Broker::new(SimpleFileStorage::new("./".into())?.into());
    #[cfg(not(feature = "filesystem_storage"))]
    // Start an in-process broker with an ephemeral cache backend. TODO: support other storage backends via config.
    Ok(Arc::new(Broker::new(EphemeralCache::new().into())))
}

fn get_server(config: &BrokerConfig) -> Result<Arc<QuicServer>> {
    let bind_addr = config.quic_bind;
    let server_config = build_server_config().context("build QUIC server config")?;
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());
    Ok(Arc::new(
        QuicServer::bind(bind_addr, server_config, transport).context("bind QUIC listener")?,
    ))
}

fn start_telemetry(config: &BrokerConfig) {
    let metrics_handle = observability::init_observability("felix-broker");

    // Expose Prometheus metrics on the configured bind address.
    tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));
}

/// This creates a background task which will start listening for connections
/// and handling them.
fn start_listener(
    config: &BrokerConfig,
    quic_server: Arc<QuicServer>,
    broker: Arc<Broker>,
) -> JoinHandle<()> {
    let config = config.clone();
    tokio::spawn(async move {
        if let Err(err) = quic::serve(quic_server, broker, config).await {
            tracing::warn!(error = %err, "quic accept loop exited");
        }
    })
}

fn start_control_plane(config: &BrokerConfig, broker: Arc<Broker>) {
    // Start control plane sync task if configured to keep scope metadata fresh.
    if let Some(base_url) = config.controlplane_url.clone() {
        let interval_ms = config.controlplane_sync_interval_ms;
        tokio::spawn(async move {
            if let Err(err) = controlplane::start_sync(
                broker,
                base_url,
                std::time::Duration::from_millis(interval_ms),
            )
            .await
            {
                tracing::warn!(error = %err, "control plane sync exited");
            }
        });
    } else {
        tracing::info!("control plane sync disabled (FELIX_CP_URL not set)");
    }
}

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

#[tokio::main]
async fn main() -> Result<()> {
    let config = BrokerConfig::from_env_or_yaml()?;
    start_telemetry(&config);

    let broker = get_broker()?;
    tracing::info!("broker started");

    let quic_server = get_server(&config)?;
    tracing::info!(addr = %quic_server.local_addr()?, "quic listener started");

    // Start accepting QUIC connections in a background task.
    let accept_task = start_listener(&config, quic_server, broker.clone());

    start_control_plane(&config, broker);

    // Block until SIGINT so the process stays alive.
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Cancel signal received, stopping the listener.");
    accept_task.abort();
    tracing::info!("broker stopped");
    Ok(())
}
