// Console demo that exercises the broker protocol over QUIC.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::Message;
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::RootCertStore;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Keep the demo output readable and step-by-step.
    println!("== Felix QUIC Broker Demo ==");
    println!("Goal: publish and subscribe over QUIC using felix-wire frames.");

    println!("Step 1/4: booting QUIC server.");
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

    println!("Step 3/4: subscribing and publishing messages.");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            stream: "demo-topic".to_string(),
        },
    )
    .await?;
    sub_send.finish()?;
    let response = quic::read_message(&mut sub_recv).await?;
    println!("Subscribe response: {:?}", response);

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let recv_task = tokio::spawn(async move {
        while let Ok(Some(message)) = quic::read_message(&mut sub_recv).await {
            if let Message::Event { stream, payload } = message {
                let _ = event_tx.send((stream, payload));
            }
        }
    });

    publish(&connection, "demo-topic", b"hello").await?;
    publish(&connection, "demo-topic", b"world").await?;

    println!("Step 4/4: shutting down.");
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(1), event_rx.recv()).await {
            Ok(Some((stream, payload))) => {
                println!(
                    "Event on {}: {}",
                    stream,
                    String::from_utf8_lossy(&payload)
                );
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
    recv_task.abort();
    drop(connection);
    server_task.abort();
    println!("Demo complete.");
    Ok(())
}

async fn publish(
    connection: &felix_transport::QuicConnection,
    stream: &str,
    payload: &[u8],
) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Publish {
            stream: stream.to_string(),
            payload: payload.to_vec(),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message(&mut recv).await?;
    println!("Publish response: {:?}", response);
    Ok(())
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.serialize_der()?);
    let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
    let server_config = quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
        .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
}
