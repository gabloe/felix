// Console demo that exercises cache Put/Get over QUIC using felix-wire frames.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
use felix_client::Client;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    println!("== Felix QUIC Cache Demo ==");
    println!("Goal: demonstrate cache Put/Get over QUIC (not pub/sub).");

    println!("Step 1/4: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker));

    println!("Step 2/4: connecting QUIC client.");
    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;

    println!("Step 3/4: writing cache entry with TTL.");
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "demo-key",
            b"cached".to_vec(),
            Some(500),
        )
        .await?;
    println!("Cache put response: Ok");

    let get_response = client
        .cache_get("t1", "default", "primary", "demo-key")
        .await?;
    println!(
        "Cache get response: {}",
        format_cache_response(&get_response)
    );

    println!("Step 4/4: waiting for TTL expiry and reading again.");
    tokio::time::sleep(Duration::from_millis(650)).await;
    let expired_response = client
        .cache_get("t1", "default", "primary", "demo-key")
        .await?;
    println!(
        "Cache get after TTL: {}",
        format_cache_response(&expired_response)
    );
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

fn format_cache_response(response: &Option<Vec<u8>>) -> String {
    match response {
        Some(bytes) => format!(
            "CacheValue {{ value: {:?} }}",
            String::from_utf8_lossy(bytes)
        ),
        None => "CacheValue { value: None }".to_string(),
    }
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
}
