use super::*;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::{Duration, timeout};

use crate::wire::read_frame_into;

#[tokio::test]
async fn in_process_publish_and_subscribe() {
    // Smoke-test the in-process path without any network transport.
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await
        .expect("register");
    let client = InProcessClient::new(broker);
    let mut receiver = client
        .subscribe("t1", "default", "updates")
        .await
        .expect("subscribe");
    client
        .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
        .await
        .expect("publish");
    let msg = receiver.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"payload"));
}

#[tokio::test]
async fn clients_share_broker_state() {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "shared", Default::default())
        .await
        .expect("register");
    let publisher = InProcessClient::new(broker.clone());
    let subscriber = InProcessClient::new(broker);
    let mut receiver = subscriber
        .subscribe("t1", "default", "shared")
        .await
        .expect("subscribe");
    publisher
        .publish(
            "t1",
            "default",
            "shared",
            Bytes::from_static(b"from-publisher"),
        )
        .await
        .expect("publish");
    let msg = receiver.recv().await.expect("recv");
    assert_eq!(msg, Bytes::from_static(b"from-publisher"));
}

#[tokio::test]
async fn cache_worker_exits_on_stream_error() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "1");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "1");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_EVENT_CONN_POOL", "1");
    }

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..3 {
            let connection = server.accept().await?;
            connections.push(connection);
        }

        let mut closed = false;
        for connection in connections {
            let result = timeout(Duration::from_secs(1), connection.accept_bi()).await;
            if let Ok(Ok((mut send, mut recv))) = result {
                let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
                let _ = read_frame_into(&mut recv, &mut frame_scratch, false).await;
                let _ = send.finish();
                closed = true;
            }
        }

        if !closed {
            return Err(anyhow::anyhow!("server did not accept cache stream"));
        }

        Result::<()>::Ok(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config(cert)?,
        TransportConfig::default(),
    )
    .await?;

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache should fail");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("cache response closed")
            || err_msg.contains("cache worker closed")
            || err_msg.contains("connection lost"),
        "unexpected cache error: {err_msg}"
    );

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache worker should be closed");
    assert!(err.to_string().contains("cache worker closed"));

    server_task.await.context("server task join")??;
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
