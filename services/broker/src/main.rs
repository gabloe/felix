// Broker service main entry point.
mod controlplane;
mod observability;

use anyhow::{Context, Result};
use broker::{config, quic};
use felix_broker::{Broker, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ServerConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let metrics_handle = observability::init_observability("felix-broker");

    // Start an in-process broker with an ephemeral cache backend. TODO: support other storage backends via config.
    let broker = Broker::new(EphemeralCache::new().into());
    tracing::info!("broker started");
    
    // For testing: optionally pre-register a tenant/namespace/stream from environment variables
    if let Ok(tenant) = std::env::var("FELIX_TEST_TENANT") {
        let namespace = std::env::var("FELIX_TEST_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let stream = std::env::var("FELIX_TEST_STREAM").unwrap_or_else(|_| "test-stream".to_string());
        
        broker.register_tenant(&tenant).await.context("register test tenant")?;
        broker.register_namespace(&tenant, &namespace).await.context("register test namespace")?;
        broker.register_stream(&tenant, &namespace, &stream, Default::default()).await.context("register test stream")?;
        
        tracing::info!(
            tenant = %tenant,
            namespace = %namespace,
            stream = %stream,
            "pre-registered test tenant/namespace/stream"
        );
    }

    let config = config::BrokerConfig::from_env_or_yaml()?;
    // Expose Prometheus metrics on the configured bind address.
    tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

    let bind_addr = config.quic_bind;
    let server_config = build_server_config().context("build QUIC server config")?;
    let transport = broker::transport::cache_transport_config(&config, TransportConfig::default());
    let quic_server = Arc::new(
        QuicServer::bind(bind_addr, server_config, transport).context("bind QUIC listener")?,
    );
    tracing::info!(addr = %quic_server.local_addr()?, "quic listener started");

    // Start accepting QUIC connections in a background task.
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

    // Start control plane sync task if configured to keep scope metadata fresh.
    if let Some(base_url) = config.controlplane_url.clone() {
        let interval_ms = config.controlplane_sync_interval_ms;
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
    // Dev-only self-signed TLS config for QUIC endpoints.
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    Ok(ServerConfig::with_single_cert(
        vec![cert_der],
        key_der.into(),
    )?)
}
