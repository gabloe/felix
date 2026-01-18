// Broker service main entry point.
mod controlplane;

use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure logging from environment for easy local tweaking.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Start an in-process broker with an ephemeral cache backend.
    let broker = Broker::new(EphemeralCache::new());
    tracing::info!("broker started");

    let bind_addr = std::env::var("FELIX_QUIC_BIND")
        .unwrap_or_else(|_| "0.0.0.0:5000".to_string())
        .parse::<SocketAddr>()
        .context("parse FELIX_QUIC_BIND")?;
    let server_config = build_server_config().context("build QUIC server config")?;
    let quic_server = Arc::new(
        QuicServer::bind(bind_addr, server_config, TransportConfig::default())
            .context("bind QUIC listener")?,
    );
    tracing::info!(addr = %quic_server.local_addr()?, "quic listener started");

    let broker = Arc::new(broker);
    let accept_task = {
        let quic_server = Arc::clone(&quic_server);
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(err) = quic::serve(quic_server, broker).await {
                tracing::warn!(error = %err, "quic accept loop exited");
            }
        })
    };

    if let Ok(base_url) = std::env::var("FELIX_CP_URL") {
        let interval_ms = std::env::var("FELIX_CP_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2000);
        let broker = Arc::clone(&broker);
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

    // Block until SIGINT so the process stays alive.
    let _ = tokio::signal::ctrl_c().await;
    accept_task.abort();
    tracing::info!("broker stopped");
    Ok(())
}

fn build_server_config() -> Result<ServerConfig> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    Ok(ServerConfig::with_single_cert(
        vec![cert_der],
        key_der.into(),
    )?)
}
