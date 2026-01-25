// Tests cover the control/uni loops, writer/ack waiter branches, and telemetry paths.
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{Frame, Message};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::time::timeout;

use super::ack_waiter::run_ack_waiter_loop;
use super::control::run_control_loop;
use super::frame_source::{DelayFrameSource, FrameSource, TestFrameSource};
use super::handlers::{handle_stream, handle_uni_stream};
use super::hooks::test_hooks;
use super::uni::run_uni_loop;
use super::writer::run_writer_loop;
use crate::config::BrokerConfig;
use crate::timings;
use crate::transport::quic::handlers::publish::{
    AckTimeoutState, AckWaiterMessage, Outgoing, PublishContext, PublishJob,
};
use crate::transport::quic::telemetry;
use crate::transport::quic::{ACK_HI_WATER, ACK_LO_WATER};

#[tokio::test]
async fn cache_put_get_round_trip() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", felix_broker::CacheMetadata)
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            value: Bytes::from_static(b"cached"),
            request_id: None,
            ttl_ms: None,
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert_eq!(response, Some(Message::Ok));

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert_eq!(
        response,
        Some(Message::CacheValue {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "demo-key".to_string(),
            value: Some(Bytes::from_static(b"cached")),
            request_id: None,
        })
    );

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn publish_rejects_unknown_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(
        response,
        Some(Message::PublishError { request_id: 1, .. })
    ));

    broker.register_tenant("t1").await.expect("tenant");
    broker
        .register_namespace("t1", "default")
        .await
        .expect("namespace");
    broker
        .register_stream("t1", "default", "missing", Default::default())
        .await
        .expect("register");
    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "missing".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert_eq!(response, Some(Message::PublishOk { request_id: 2 }));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn publish_ack_on_commit_smoke() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let mut config = BrokerConfig::from_env()?;
    config.ack_on_commit = true;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert_eq!(response, Some(Message::PublishOk { request_id: 1 }));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn publish_sharding_preserves_stream_order() -> Result<()> {
    unsafe {
        std::env::set_var("FELIX_BROKER_PUB_WORKERS_PER_CONN", "4");
        std::env::set_var("FELIX_BROKER_PUB_QUEUE_DEPTH", "1024");
        std::env::set_var("FELIX_PUB_CONN_POOL", "1");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "2");
        std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
    }

    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "alpha", Default::default())
        .await?;
    broker
        .register_stream("t1", "default", "beta", Default::default())
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = felix_client::Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config(cert)?,
        TransportConfig::default(),
    )
    .await?;

    let mut sub_alpha = client.subscribe("t1", "default", "alpha").await?;
    let mut sub_beta = client.subscribe("t1", "default", "beta").await?;
    let publisher = client.publisher().await?;

    let total = 200u64;
    for i in 0..total {
        let payload = i.to_be_bytes().to_vec();
        publisher
            .publish(
                "t1",
                "default",
                "alpha",
                payload.clone(),
                felix_wire::AckMode::None,
            )
            .await?;
        publisher
            .publish("t1", "default", "beta", payload, felix_wire::AckMode::None)
            .await?;
    }

    for i in 0..total {
        let next = timeout(Duration::from_secs(2), sub_alpha.next_event()).await??;
        let event = next.expect("alpha event");
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&event.payload[..8]);
        assert_eq!(u64::from_be_bytes(raw), i);
    }

    for i in 0..total {
        let next = timeout(Duration::from_secs(2), sub_beta.next_event()).await??;
        let event = next.expect("beta event");
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&event.payload[..8]);
        assert_eq!(u64::from_be_bytes(raw), i);
    }

    publisher.finish().await?;
    server_task.abort();
    Ok(())
}

async fn build_publish_context(broker: Arc<Broker>) -> PublishContext {
    let (tx, mut rx) = mpsc::channel::<PublishJob>(8);
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let result = broker
                .publish_batch(
                    job.tenant_id.as_str(),
                    job.namespace.as_str(),
                    job.stream.as_str(),
                    &job.payloads,
                )
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from);
            if let Some(response) = job.response {
                let _ = response.send(result);
            }
        }
    });
    PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
    }
}

fn frame_from_message(message: Message) -> Frame {
    message.encode().expect("encode message")
}

fn invalid_json_frame() -> Frame {
    Frame::new(0, Bytes::from_static(b"not-json")).expect("frame")
}

fn binary_publish_batch_frame(
    tenant_id: &str,
    namespace: &str,
    stream: &str,
    payloads: &[Bytes],
) -> Frame {
    let payloads_vec: Vec<Vec<u8>> = payloads.iter().map(|payload| payload.to_vec()).collect();
    let bytes =
        felix_wire::binary::encode_publish_batch_bytes(tenant_id, namespace, stream, &payloads_vec)
            .expect("encode binary batch");
    Frame::decode(bytes).expect("decode frame")
}

#[tokio::test]
async fn control_loop_handles_publish_and_cache_requests() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    broker
        .register_cache("t1", "default", "primary", felix_broker::CacheMetadata)
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![
        Ok(Some(frame_from_message(Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::None),
        }))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::None),
        }))),
        Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: Some(10),
            ttl_ms: None,
        }))),
        Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "key".to_string(),
            request_id: Some(11),
        }))),
        Ok(Some(frame_from_message(Message::Ok))),
    ];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_handles_binary_and_decode_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![Ok(Some(binary)), Ok(Some(invalid_json_frame()))];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    assert!(
        run_control_loop(
            &mut source,
            Arc::clone(&broker),
            connection,
            BrokerConfig::from_env()?,
            publish_ctx,
            HashMap::new(),
            String::new(),
            out_ack_tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ack_throttle_rx,
            ack_throttle_tx,
            ack_timeout_state,
            cancel_tx,
            cancel_rx,
            Arc::new(Semaphore::new(8)),
            ack_waiter_tx,
            Duration::from_millis(10),
            &mut scratch,
        )
        .await
        .is_err()
    );
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_handles_cancel_and_graceful_close() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let cancel_tx_clone = cancel_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = cancel_tx_clone.send(true);
    });
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_best_effort_full_and_closed() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", felix_broker::CacheMetadata)
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let cache_put = frame_from_message(Message::CachePut {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: "key".to_string(),
        value: Bytes::from_static(b"value"),
        request_id: None,
        ttl_ms: None,
    });
    let mut source = TestFrameSource::new(vec![Ok(Some(cache_put.clone()))]);
    let connection_clone = connection.clone();

    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    out_ack_tx
        .try_send(Outgoing::CacheMessage(Message::Ok))
        .expect("fill channel");
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection,
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx.clone(),
        ack_throttle_tx.clone(),
        Arc::clone(&ack_timeout_state),
        cancel_tx.clone(),
        cancel_rx.clone(),
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx.clone(),
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    drop(out_ack_rx);

    let mut source = TestFrameSource::new(vec![Ok(Some(cache_put))]);
    let (closed_tx, _closed_rx) = mpsc::channel(1);
    drop(_closed_rx);
    let err = run_control_loop(
        &mut source,
        Arc::clone(&broker),
        connection_clone,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::clone(&broker)).await,
        HashMap::new(),
        String::new(),
        closed_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(8)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await;
    assert!(err.is_err());
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn uni_loop_publish_and_errors() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let frames = vec![
        Ok(Some(binary)),
        Ok(Some(frame_from_message(Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::None),
        }))),
        Ok(Some(frame_from_message(Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec(), b"b".to_vec()],
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::None),
        }))),
        Ok(None),
    ];
    let mut source = TestFrameSource::new(frames);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![Ok(Some(frame_from_message(Message::Ok)))]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        BrokerConfig::from_env()?,
        build_publish_context(Arc::clone(&broker)).await,
        HashMap::new(),
        String::new(),
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![Ok(Some(invalid_json_frame()))]);
    assert!(
        run_uni_loop(
            &mut source,
            Arc::clone(&broker),
            BrokerConfig::from_env()?,
            build_publish_context(Arc::clone(&broker)).await,
            HashMap::new(),
            String::new(),
            &mut scratch,
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn writer_loop_branches() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    test_hooks::reset();
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        for _ in 0..4 {
            let Ok((_send, mut recv)) = connection.accept_bi().await else {
                break;
            };
            let mut buf = vec![0u8; 1024];
            let _ = recv.read(&mut buf).await;
        }
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_throttle_reset(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx
        .send(Outgoing::Message(Message::PublishOk { request_id: 1 }))
        .await?;
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_write_message_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx
        .send(Outgoing::Message(Message::PublishOk { request_id: 2 }))
        .await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_cache_encode_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    let (send, _recv) = connection.open_bi().await?;
    let (out_ack_tx, out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    test_hooks::set_force_write_frame_error(true);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    drop(out_ack_tx);
    writer.await.expect("writer");
    test_hooks::reset();

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_branches() -> Result<()> {
    test_hooks::reset();
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(8);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let ack_waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx.clone(),
        Duration::from_millis(5),
    ));

    let waiters = Arc::new(Semaphore::new(10));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 1,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));

    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 2,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 3,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 4,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;

    let (tx_batch_ok, rx_batch_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 5,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_ok.send(Ok(()));

    let (tx_batch_err, rx_batch_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 6,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_batch_drop, rx_batch_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 7,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_batch_timeout, rx_batch_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 8,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_batch_timeout = tx_batch_timeout;

    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    ack_waiter.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_cancel_branch() -> Result<()> {
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(1);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx,
        Duration::from_millis(50),
    ));
    let waiters = Arc::new(Semaphore::new(1));
    let (_tx, rx) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 9,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = cancel_tx.send(true);
    waiter.await.expect("waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn handle_stream_drain_timeout_branch() -> Result<()> {
    test_hooks::reset();
    test_hooks::set_force_drain_timeout(true);
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (send, recv) = connection.accept_bi().await?;
        let config = BrokerConfig::from_env()?;
        let publish_ctx =
            build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await;
        handle_stream(
            Arc::new(Broker::new(EphemeralCache::new().into())),
            connection,
            config,
            publish_ctx,
            send,
            recv,
        )
        .await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, _recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(&mut send, Message::Ok).await?;
    send.finish()?;

    server_task.await.context("server task")??;
    test_hooks::reset();
    Ok(())
}

#[tokio::test]
async fn control_stream_rejects_unexpected_message() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(&mut send, Message::Ok).await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Error { .. }) | None));

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn cache_put_unknown_cache_closes_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "demo-key".to_string(),
            value: Bytes::from_static(b"cached"),
            request_id: None,
            ttl_ms: None,
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Error { .. })));
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(response.is_none());

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn cache_get_unknown_cache_closes_stream() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let max_frame_bytes = config.max_frame_bytes;
    let mut frame_scratch = BytesMut::with_capacity(max_frame_bytes.min(64 * 1024));
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "missing".to_string(),
            key: "demo-key".to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(matches!(response, Some(Message::Error { .. })));
    let response = crate::transport::quic::read_message_limited(
        &mut recv,
        max_frame_bytes,
        &mut frame_scratch,
    )
    .await?;
    assert!(response.is_none());

    drop(connection);
    server_task.abort();
    Ok(())
}

#[tokio::test]
async fn uni_stream_rejects_non_publish() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;

    let config = BrokerConfig::from_env()?;
    let server_task = tokio::spawn(crate::transport::quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut send = connection.open_uni().await?;
    let frame = Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "primary".to_string(),
        key: "demo-key".to_string(),
        request_id: None,
    }
    .encode()?;
    let bytes = frame.encode();
    send.write_all(&bytes).await?;
    send.finish()?;

    drop(connection);
    server_task.abort();
    Ok(())
}

struct PendingFrameSource {
    ready: Arc<AtomicBool>,
}

impl FrameSource for PendingFrameSource {
    fn next_frame<'a>(
        &'a mut self,
        _max_frame_bytes: usize,
        _scratch: &'a mut BytesMut,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + 'a>>
    {
        let ready = Arc::clone(&self.ready);
        Box::pin(async move {
            while !ready.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            Ok(None)
        })
    }
}

fn spawn_ack_waiter_with_closed_out_ack(
    ack_wait_timeout: Duration,
) -> (
    mpsc::Sender<AckWaiterMessage>,
    Arc<Semaphore>,
    tokio::task::JoinHandle<()>,
) {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let handle = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        ack_wait_timeout,
    ));
    (ack_waiter_tx, Arc::new(Semaphore::new(1)), handle)
}

#[tokio::test]
async fn delay_frame_source_returns_none() -> Result<()> {
    let mut source = DelayFrameSource {
        delay: Duration::from_millis(1),
    };
    let mut scratch = BytesMut::new();
    let frame = source.next_frame(1024, &mut scratch).await?;
    assert!(frame.is_none());
    Ok(())
}

#[tokio::test]
async fn pending_frame_source_returns_none() -> Result<()> {
    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let mut scratch = BytesMut::new();
    ready.store(true, Ordering::Relaxed);
    let frame = source.next_frame(1024, &mut scratch).await?;
    assert!(frame.is_none());
    Ok(())
}

#[test]
fn should_reset_throttle_true_when_crossing_low_water() {
    assert!(super::hooks::should_reset_throttle(Some((
        ACK_HI_WATER,
        ACK_LO_WATER - 1
    ))));
}

#[tokio::test]
#[serial]
async fn writer_loop_cancel_breaks_on_cancel() -> Result<()> {
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut buf = vec![0u8; 16];
        let _ = recv.read(&mut buf).await;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        cancel_tx.clone(),
        cancel_rx,
    ));
    let _ = cancel_tx.send(true);
    writer.await.expect("writer");
    drop(out_ack_tx);

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn writer_loop_records_timings_when_sampled() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut buf = vec![0u8; 4096];
        while let Some(n) = recv.read(&mut buf).await? {
            if n == 0 {
                break;
            }
        }
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (send, _recv) = connection.open_bi().await?;

    let (out_ack_tx, out_ack_rx) = mpsc::channel(256);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let writer = tokio::spawn(run_writer_loop(
        send,
        out_ack_rx,
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        ack_throttle_tx,
        cancel_tx,
        cancel_rx,
    ));
    for i in 0..50u64 {
        out_ack_tx
            .send(Outgoing::Message(Message::PublishOk { request_id: i }))
            .await?;
    }
    for _ in 0..50u64 {
        out_ack_tx.send(Outgoing::CacheMessage(Message::Ok)).await?;
    }
    drop(out_ack_tx);
    writer.await.expect("writer");

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_cancel_changed_breaks() -> Result<()> {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(1);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx.clone(),
        cancel_rx,
        Duration::from_millis(50),
    ));
    let _ = cancel_tx.send(true);
    drop(ack_waiter_tx);
    waiter.await.expect("waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_loop_logs_on_enqueue_failure() -> Result<()> {
    let (out_ack_tx, out_ack_rx) = mpsc::channel(1);
    drop(out_ack_rx);
    let (ack_waiter_tx, ack_waiter_rx) = mpsc::channel(16);
    let (ack_throttle_tx, _ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let ack_waiter = tokio::spawn(run_ack_waiter_loop(
        ack_waiter_rx,
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Duration::from_millis(5),
    ));

    let waiters = Arc::new(Semaphore::new(10));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 10,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));

    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 11,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 12,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 13,
            payload_len: 3,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;

    let (tx_batch_ok, rx_batch_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 14,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_ok.send(Ok(()));

    let (tx_batch_err, rx_batch_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 15,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_batch_err.send(Err(anyhow::anyhow!("nope")));

    let (_tx_batch_drop, rx_batch_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 16,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;

    let (tx_batch_timeout, rx_batch_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 17,
            payload_bytes: vec![1, 2],
            response_rx: rx_batch_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_batch_timeout = tx_batch_timeout;

    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    ack_waiter.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_error() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 21,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_dropped() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 22,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_timeout() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::Publish {
            request_id: 23,
            payload_len: 1,
            start: telemetry::t_instant_now(),
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_ok() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_ok, rx_ok) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 24,
            payload_bytes: vec![1],
            response_rx: rx_ok,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_ok.send(Ok(()));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_error() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_err, rx_err) = oneshot::channel();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 25,
            payload_bytes: vec![1],
            response_rx: rx_err,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _ = tx_err.send(Err(anyhow::anyhow!("nope")));
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_dropped() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (_tx_drop, rx_drop) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 26,
            payload_bytes: vec![1],
            response_rx: rx_drop,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
#[serial]
async fn ack_waiter_enqueue_failure_publish_batch_timeout() -> Result<()> {
    let (ack_waiter_tx, waiters, handle) =
        spawn_ack_waiter_with_closed_out_ack(Duration::from_millis(5));
    let (tx_timeout, rx_timeout) = oneshot::channel::<Result<()>>();
    ack_waiter_tx
        .send(AckWaiterMessage::PublishBatch {
            request_id: 27,
            payload_bytes: vec![1],
            response_rx: rx_timeout,
            permit: waiters.clone().acquire_owned().await?,
        })
        .await?;
    let _hold_timeout = tx_timeout;
    drop(ack_waiter_tx);
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.await.expect("ack waiter");
    Ok(())
}

#[tokio::test]
async fn control_loop_pre_canceled_exits() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut source = TestFrameSource::new(vec![Ok(None)]);
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let _ = cancel_tx.send(true);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cancel_changed_breaks() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let cancel_tx_clone = cancel_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = cancel_tx_clone.send(true);
    });
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cancel_changed_continues() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let ready = Arc::new(AtomicBool::new(false));
    let mut source = PendingFrameSource {
        ready: Arc::clone(&ready),
    };
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(1);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(1);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let cancel_tx_clone = cancel_tx.clone();
    let ready_clone = Arc::clone(&ready);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let _ = cancel_tx_clone.send(false);
        ready_clone.store(true, Ordering::Relaxed);
    });
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_subscribe_done_true() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![Ok(Some(frame_from_message(Message::Subscribe {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "missing".to_string(),
        subscription_id: None,
    })))];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn control_loop_cache_timings_recorded() -> Result<()> {
    timings::enable_collection(1);
    timings::set_enabled(true);
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", felix_broker::CacheMetadata)
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let mut frames = Vec::new();
    for idx in 0..20u64 {
        frames.push(Ok(Some(frame_from_message(Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: format!("key-{idx}"),
            value: Bytes::from_static(b"value"),
            request_id: Some(idx),
            ttl_ms: None,
        }))));
        frames.push(Ok(Some(frame_from_message(Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: format!("key-{idx}"),
            request_id: Some(idx + 100),
        }))));
    }
    frames.push(Ok(None));
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_get_missing_no_request_id_returns_true() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![Ok(Some(frame_from_message(Message::CacheGet {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "missing".to_string(),
        key: "key".to_string(),
        request_id: None,
    })))];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_cache_put_missing_no_request_id_returns_true() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![Ok(Some(frame_from_message(Message::CachePut {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        cache: "missing".to_string(),
        key: "key".to_string(),
        value: Bytes::from_static(b"value"),
        request_id: None,
        ttl_ms: None,
    })))];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    tokio::spawn(async move { while out_ack_rx.recv().await.is_some() {} });
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn control_loop_error_message_returns_false() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        Result::<()>::Ok(())
    });
    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    let frames = vec![Ok(Some(frame_from_message(Message::Error {
        message: "bad".to_string(),
    })))];
    let mut source = TestFrameSource::new(frames);
    let (out_ack_tx, _out_ack_rx) = mpsc::channel(8);
    let (ack_throttle_tx, ack_throttle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (ack_waiter_tx, _ack_waiter_rx) = mpsc::channel(8);
    let ack_timeout_state = Arc::new(Mutex::new(AckTimeoutState::new(std::time::Instant::now())));
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let result = run_control_loop(
        &mut source,
        broker,
        connection,
        BrokerConfig::from_env()?,
        build_publish_context(Arc::new(Broker::new(EphemeralCache::new().into()))).await,
        HashMap::new(),
        String::new(),
        out_ack_tx,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        ack_throttle_rx,
        ack_throttle_tx,
        ack_timeout_state,
        cancel_tx,
        cancel_rx,
        Arc::new(Semaphore::new(1)),
        ack_waiter_tx,
        Duration::from_millis(10),
        &mut scratch,
    )
    .await?;
    assert!(!result);
    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
async fn uni_loop_breaks_on_enqueue_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let (tx, rx) = mpsc::channel::<PublishJob>(1);
    drop(rx);
    let publish_ctx = PublishContext {
        workers: Arc::new(vec![tx]),
        worker_count: 1,
        depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        wait_timeout: Duration::from_millis(50),
    };
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    let binary =
        binary_publish_batch_frame("t1", "default", "updates", &[Bytes::from_static(b"one")]);
    let mut source = TestFrameSource::new(vec![Ok(Some(binary))]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        BrokerConfig::from_env()?,
        publish_ctx.clone(),
        HashMap::new(),
        String::new(),
        &mut scratch,
    )
    .await?;

    let mut source = TestFrameSource::new(vec![Ok(Some(frame_from_message(Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: b"payload".to_vec(),
        request_id: Some(1),
        ack: Some(felix_wire::AckMode::None),
    })))]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        BrokerConfig::from_env()?,
        publish_ctx.clone(),
        HashMap::new(),
        String::new(),
        &mut scratch,
    )
    .await?;

    let mut source =
        TestFrameSource::new(vec![Ok(Some(frame_from_message(Message::PublishBatch {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payloads: vec![b"a".to_vec()],
            request_id: Some(2),
            ack: Some(felix_wire::AckMode::None),
        })))]);
    run_uni_loop(
        &mut source,
        Arc::clone(&broker),
        BrokerConfig::from_env()?,
        publish_ctx,
        HashMap::new(),
        String::new(),
        &mut scratch,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn handle_uni_stream_smoke() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream("t1", "default", "updates", Default::default())
        .await?;
    let publish_ctx = build_publish_context(Arc::clone(&broker)).await;
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let recv = connection.accept_uni().await?;
        handle_uni_stream(broker, BrokerConfig::from_env()?, publish_ctx, recv).await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let mut send = connection.open_uni().await?;
    let frame = Message::Publish {
        tenant_id: "t1".to_string(),
        namespace: "default".to_string(),
        stream: "updates".to_string(),
        payload: b"payload".to_vec(),
        request_id: None,
        ack: Some(felix_wire::AckMode::None),
    }
    .encode()?;
    send.write_all(&frame.encode()).await?;
    send.finish()?;

    server_task.await.context("server task")??;
    Ok(())
}

#[tokio::test]
#[serial]
async fn handle_stream_drain_timeout_sleep_branch() -> Result<()> {
    test_hooks::reset();
    let (server_config, cert) = build_server_config()?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let (send, recv) = connection.accept_bi().await?;
        let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_stream("t1", "default", "updates", Default::default())
            .await?;
        let (tx, mut rx) = mpsc::channel::<PublishJob>(8);
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Some(response) = job.response {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        drop(response);
                    });
                }
            }
        });
        let publish_ctx = PublishContext {
            workers: Arc::new(vec![tx]),
            worker_count: 1,
            depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wait_timeout: Duration::from_millis(50),
        };
        let mut config = BrokerConfig::from_env()?;
        config.ack_on_commit = true;
        config.ack_wait_timeout_ms = 1000;
        config.control_stream_drain_timeout_ms = 1;
        handle_stream(broker, connection, config, publish_ctx, send, recv).await?;
        Result::<()>::Ok(())
    });

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_quinn_client_config(cert)?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;
    let (mut send, _recv) = connection.open_bi().await?;
    crate::transport::quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "updates".to_string(),
            payload: b"payload".to_vec(),
            request_id: Some(1),
            ack: Some(felix_wire::AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;

    server_task.await.context("server task")??;
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

fn build_quinn_client_config(cert: CertificateDer<'static>) -> Result<QuinnClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    Ok(quinn)
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<felix_client::ClientConfig> {
    let quinn = build_quinn_client_config(cert)?;
    felix_client::ClientConfig::from_env_or_yaml(quinn, None)
}
