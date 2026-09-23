use super::*;
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_storage::EphemeralCache;
use felix_transport::TransportConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::PrivatePkcs8KeyDer;
use tokio::sync::oneshot;

#[tokio::test]
async fn build_publish_context_clamps_worker_and_queue_minimums() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "demo", Default::default())
        .await?;

    let config = BrokerConfig {
        pub_workers_per_conn: 0,
        pub_queue_depth: 0,
        publish_queue_wait_timeout_ms: 17,
        ..BrokerConfig::default()
    };
    let publish_ctx =
        build_publish_context(Arc::clone(&broker), &config, ClusterContext::default());
    assert_eq!(publish_ctx.worker_count, 1);
    assert_eq!(publish_ctx.workers.len(), 1);
    assert_eq!(publish_ctx.wait_timeout, Duration::from_millis(17));

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "demo".to_string(),
            },
            payloads: vec![Bytes::from_static(b"ok")],
            response: Some(response_tx),
            admission_permit: None,
        })
        .await
        .expect("enqueue publish");
    response_rx.await.expect("worker response")?;
    Ok(())
}

#[tokio::test]
async fn build_publish_context_worker_returns_publish_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let config = BrokerConfig::default();
    let publish_ctx = build_publish_context(broker, &config, ClusterContext::default());

    let (response_tx, response_rx) = oneshot::channel();
    publish_ctx.workers[0]
        .send(PublishJob {
            target: PublishTarget::Named {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
            },
            payloads: vec![Bytes::from_static(b"payload")],
            response: Some(response_tx),
            admission_permit: None,
        })
        .await
        .expect("enqueue publish");
    let err = response_rx
        .await
        .expect("worker response")
        .expect_err("publish should fail");
    assert!(err.to_string().contains("stream"));
    Ok(())
}

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
