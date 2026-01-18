use anyhow::{Context, Result, anyhow};
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
    println!("== Felix Conformance Runner ==");
    let broker = Arc::new(Broker::new(EphemeralCache::new()));
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), Arc::clone(&broker)));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    run_pubsub(&connection).await?;
    run_cache(&connection).await?;

    drop(connection);
    server_task.abort();
    println!("Conformance checks passed.");
    Ok(())
}

async fn run_pubsub(connection: &felix_transport::QuicConnection) -> Result<()> {
    println!("Running pub/sub checks...");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            stream: "conformance".to_string(),
        },
    )
    .await?;
    sub_send.finish()?;
    let response = quic::read_message(&mut sub_recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow!("subscribe failed: {response:?}"));
    }

    publish(connection, b"alpha").await?;
    publish(connection, b"beta").await?;

    let mut received = Vec::new();
    for _ in 0..2 {
        let message = quic::read_message(&mut sub_recv)
            .await?
            .ok_or_else(|| anyhow!("subscription ended early"))?;
        match message {
            Message::Event { payload, .. } => {
                received.push(payload);
            }
            other => return Err(anyhow!("unexpected message: {other:?}")),
        }
    }

    if received != [b"alpha".to_vec(), b"beta".to_vec()] {
        return Err(anyhow!("unexpected event order: {received:?}"));
    }
    Ok(())
}

async fn publish(connection: &felix_transport::QuicConnection, payload: &[u8]) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Publish {
            stream: "conformance".to_string(),
            payload: payload.to_vec(),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow!("publish failed: {response:?}"));
    }
    Ok(())
}

async fn run_cache(connection: &felix_transport::QuicConnection) -> Result<()> {
    println!("Running cache checks...");
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::CachePut {
            key: "conformance-key".to_string(),
            value: b"value".to_vec(),
            ttl_ms: Some(100),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow!("cache put failed: {response:?}"));
    }

    let value = cache_get(connection, "conformance-key").await?;
    if value != Some(b"value".to_vec()) {
        return Err(anyhow!("cache get mismatch: {value:?}"));
    }

    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = cache_get(connection, "conformance-key").await?;
    if expired.is_some() {
        return Err(anyhow!("cache entry should be expired"));
    }
    Ok(())
}

async fn cache_get(
    connection: &felix_transport::QuicConnection,
    key: &str,
) -> Result<Option<Vec<u8>>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::CacheGet {
            key: key.to_string(),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    match response {
        Some(Message::CacheValue { value, .. }) => Ok(value),
        other => Err(anyhow!("unexpected cache response: {other:?}")),
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
