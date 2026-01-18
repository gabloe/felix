// Console demo that exercises cache Put/Get over QUIC using felix-wire frames.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::Message;
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
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker));

    println!("Step 2/4: connecting QUIC client.");
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    println!("Step 3/4: writing cache entry with TTL.");
    let put_response = request(
        &connection,
        Message::CachePut {
            key: "demo-key".to_string(),
            value: b"cached".to_vec(),
            ttl_ms: Some(500),
        },
    )
    .await?;
    println!("Cache put response: {put_response:?}");

    let get_response = request(
        &connection,
        Message::CacheGet {
            key: "demo-key".to_string(),
        },
    )
    .await?;
    println!(
        "Cache get response: {}",
        format_cache_response(&get_response)
    );

    println!("Step 4/4: waiting for TTL expiry and reading again.");
    tokio::time::sleep(Duration::from_millis(650)).await;
    let expired_response = request(
        &connection,
        Message::CacheGet {
            key: "demo-key".to_string(),
        },
    )
    .await?;
    println!(
        "Cache get after TTL: {}",
        format_cache_response(&expired_response)
    );

    drop(connection);
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

async fn request(
    connection: &felix_transport::QuicConnection,
    message: Message,
) -> Result<Option<Message>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(&mut send, message).await?;
    send.finish()?;
    quic::read_message(&mut recv).await
}

fn format_cache_response(response: &Option<Message>) -> String {
    match response {
        Some(Message::CacheValue { key, value }) => match value {
            Some(bytes) => format!(
                "CacheValue {{ key: {:?}, value: {:?} }}",
                key,
                String::from_utf8_lossy(bytes)
            ),
            None => format!("CacheValue {{ key: {key:?}, value: None }}"),
        },
        other => format!("{other:?}"),
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
