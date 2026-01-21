use anyhow::{Context, Result};
use felix_broker::{Broker, StreamMetadata};
use felix_client::Client;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn text_publish_batch_large_payload_no_drop() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_EVENT_QUEUE_DEPTH", "10000");
    }
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_topic_capacity(6000)?
            .with_log_capacity(6000)?,
    );
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "latency", StreamMetadata::default())
        .await?;

    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let broker_config = broker::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(broker::quic::serve(
        Arc::clone(&server),
        broker,
        broker_config,
    ));

    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;
    let mut sub = client.subscribe("t1", "default", "latency").await?;
    let publisher = client.publisher().await?;

    let payload = large_payload();
    let total = 5000usize;
    let batch_size = 64usize;
    let mut remaining = total;
    while remaining > 0 {
        let count = remaining.min(batch_size);
        let payloads = (0..count).map(|_| payload.clone()).collect::<Vec<_>>();
        publisher
            .publish_batch(
                "t1",
                "default",
                "latency",
                payloads,
                felix_wire::AckMode::None,
            )
            .await?;
        remaining -= count;
    }

    let recv_all = async {
        let mut received = 0usize;
        while received < total {
            if let Some(_event) = sub.next_event().await? {
                received += 1;
            } else {
                break;
            }
        }
        Result::<usize>::Ok(received)
    };
    let received = timeout(Duration::from_secs(10), recv_all)
        .await
        .context("receive timeout")??;

    let broker_counters = broker::quic::frame_counters_snapshot();
    let client_counters = felix_client::frame_counters_snapshot();
    assert_eq!(received, total);
    assert_eq!(broker_counters.frames_in_err, 0);
    assert_eq!(client_counters.frames_in_err, 0);

    server_task.abort();
    Ok(())
}

fn large_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(4096);
    for _ in 0..16 {
        for byte in 0u8..=255 {
            payload.push(byte);
        }
    }
    payload
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
