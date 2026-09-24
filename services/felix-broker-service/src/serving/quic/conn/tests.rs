use super::*;
use anyhow::{Context, Result};
use felix_storage::EphemeralCache;
use felix_transport::TransportConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;

#[tokio::test]
async fn handle_connection_returns_ok_on_closed_connection() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let config = BrokerConfig::default();
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    let auth = Arc::new(BrokerAuth::new("http://127.0.0.1".to_string()));

    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
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
        handle_connection_with_shutdown(
            broker,
            connection,
            config,
            auth,
            publish_ctx,
            CancellationToken::new(),
        )
        .await
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let quinn = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let client =
        felix_transport::QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
    let connection = client.connect(addr, "localhost").await?;
    drop(connection);

    let result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .context("handle connection timeout")??;
    assert!(result.is_ok());
    Ok(())
}
