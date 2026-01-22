// Console demo that exercises pub/sub over QUIC using felix-wire frames.
// Shows the end-to-end flow: broker boot, subscribe, publish, receive.
use anyhow::{Context, Result};
use broker::quic;
use felix_broker::{Broker, StreamMetadata};
use felix_client::{Client, ClientConfig};
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::AckMode;
use quinn::ClientConfig as QuinnClientConfig;
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

    // Create the broker with an ephemeral in-memory cache.
    println!("Step 1/6: booting in-process broker + QUIC server.");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));

    // Seed tenant/namespace so scoped streams are accepted.
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    // Register the demo stream so publishes and subscriptions succeed.
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

    // Start the broker with QUIC transport in a background task to accept client connections.
    let config = broker::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(quic::serve(Arc::clone(&server), broker, config));

    println!("Step 2/6: connecting QUIC client.");
    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;

    println!("Step 3/6: opening a subscription stream.");
    let mut subscription = client.subscribe("t1", "default", "demo-topic").await?;
    println!("Subscribe response: Subscribed");

    println!("Step 4/6: publishing two messages on the same stream.");
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "demo-topic",
            b"hello".to_vec(),
            AckMode::PerMessage,
        )
        .await?;
    publisher
        .publish(
            "t1",
            "default",
            "demo-topic",
            b"world".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    println!("Step 5/6: receiving events.");
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(1), subscription.next_event()).await {
            Ok(Ok(Some(event))) => {
                println!(
                    "Event on {}: {}",
                    event.stream,
                    String::from_utf8_lossy(event.payload.as_ref())
                );
            }
            Ok(Ok(None)) => {
                println!("Event stream closed early.");
                break;
            }
            Ok(Err(err)) => {
                println!("Event stream error: {err}");
                break;
            }
            Err(_) => {
                println!("Timed out waiting for event.");
                break;
            }
        }
    }
    println!("Shutting down demo.");
    server_task.abort();
    println!("Demo complete.");
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
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    ClientConfig::from_env_or_yaml(quinn, None)
}
