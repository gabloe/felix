// Console demo that exercises pub/sub over QUIC using felix-wire frames.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::{Broker, StreamMetadata};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::Message;
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Keep the demo output readable and step-by-step.
    println!("== Felix QUIC Pub/Sub Demo ==");
    println!("Goal: demonstrate publish/subscribe over QUIC (not cache).");
    println!("This demo spins up an in-process broker + QUIC server, then runs a client.");

    println!("Step 1/6: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo-topic", StreamMetadata::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker));

    println!("Step 2/6: connecting QUIC client.");
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    println!("Step 3/6: opening a subscription stream.");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "demo-topic".to_string(),
        },
    )
    .await?;
    sub_send.finish()?;
    let response = quic::read_message(&mut sub_recv).await?;
    println!("Subscribe response: {response:?}");

    println!("Step 4/6: publishing two messages on the same stream.");
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let recv_task = tokio::spawn(async move {
        while let Ok(Some(message)) = quic::read_message(&mut sub_recv).await {
            if let Message::Event {
                stream, payload, ..
            } = message
            {
                let _ = event_tx.send((stream, payload));
            }
        }
    });

    publish(&connection, "t1", "default", "demo-topic", b"hello").await?;
    publish(&connection, "t1", "default", "demo-topic", b"world").await?;

    println!("Step 5/6: receiving events.");
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(1), event_rx.recv()).await {
            Ok(Some((stream, payload))) => {
                println!("Event on {}: {}", stream, String::from_utf8_lossy(&payload));
            }
            Ok(None) => {
                println!("Event stream closed early.");
                break;
            }
            Err(_) => {
                println!("Timed out waiting for event.");
                break;
            }
        }
    }
    println!("Shutting down demo.");
    recv_task.abort();
    drop(connection);
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

async fn publish(
    connection: &felix_transport::QuicConnection,
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payload: &[u8],
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: tenant_id.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            payload: payload.to_vec(),
            ack: None,
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    println!("Publish response: {response:?}");
    Ok(())
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
