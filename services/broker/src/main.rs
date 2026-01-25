// Broker service main entry point.
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

#[tokio::main]
async fn main() -> Result<()> {
    run_with_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

async fn run_with_shutdown<F>(shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-broker");

    // Start an in-process broker with an ephemeral cache backend. TODO: support other storage backends via config.
    let broker = Broker::new(EphemeralCache::new().into());
    tracing::info!("broker started");

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
    shutdown.await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, http::StatusCode};
    use serial_test::serial;
    use tokio::net::TcpListener;

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

    async fn start_error_server() -> Result<String> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        Ok(format!("http://{}", addr))
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_without_controlplane() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::unset("FELIX_CP_URL");
        let _g4 = EnvGuard::unset("FELIX_BROKER_CONFIG");
        run_with_shutdown(async {}).await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_with_controlplane() -> Result<()> {
        let _g1 = EnvGuard::set("FELIX_QUIC_BIND", "127.0.0.1:0");
        let _g2 = EnvGuard::set("FELIX_BROKER_METRICS_BIND", "127.0.0.1:0");
        let _g3 = EnvGuard::unset("FELIX_BROKER_CONFIG");
        let base_url = start_error_server().await?;
        let _g4 = EnvGuard::set("FELIX_CP_URL", &base_url);
        run_with_shutdown(async {}).await?;
        Ok(())
    }

    #[test]
    fn build_server_config_smoke() -> Result<()> {
        let _config = build_server_config()?;
        Ok(())
    }
}
