// Unit and integration tests for the subscribe path: lane routing, writer loops,
// connection accounting, batching, and drop behavior.

use super::*;
use crate::transport::quic::handlers::publish::AckTimeoutState;
use anyhow::Context;
use bytes::{Bytes, BytesMut};
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::io::AsyncReadExt;

fn make_server_config() -> anyhow::Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let cert = generate_simple_self_signed(vec!["localhost".into()])
        .context("generate self-signed cert")?;
    let cert_der = cert.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server_config = quinn::ServerConfig::with_single_cert(
        vec![cert_der.clone()],
        PrivateKeyDer::Pkcs8(key_der),
    )?;
    Ok((server_config, cert_der))
}

fn make_client_config(cert: CertificateDer<'static>) -> anyhow::Result<quinn::ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert).context("add root cert")?;
    Ok(quinn::ClientConfig::with_root_certificates(
        std::sync::Arc::new(roots),
    )?)
}

fn test_config() -> crate::config::BrokerConfig {
    crate::config::BrokerConfig {
        quic_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        metrics_bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        controlplane_url: None,
        controlplane_sync_interval_ms: 2000,
        ack_on_commit: false,
        max_frame_bytes: 16 * 1024 * 1024,
        publish_queue_wait_timeout_ms: 2000,
        ack_wait_timeout_ms: 2000,
        disable_timings: false,
        control_stream_drain_timeout_ms: 50,
        shutdown_drain_timeout_ms: 25_000,
        cache_conn_recv_window: 256 * 1024 * 1024,
        cache_stream_recv_window: 64 * 1024 * 1024,
        cache_send_window: 256 * 1024 * 1024,
        event_batch_max_events: 1,
        event_batch_max_bytes: 64 * 1024,
        event_batch_max_delay_us: 250,
        fanout_batch_size: 1,
        pub_workers_per_conn: 1,
        pub_queue_depth: 8,
        pub_inflight_bytes: 64 * 1024 * 1024,
        pub_conn_inflight_bytes: 16 * 1024 * 1024,
        pub_ingress_wait: false,
        core_shards: 0,
        subscriber_queue_capacity: 8,
        max_subscriptions_per_conn: 4096,
        subscriber_queue_policy: felix_broker::SubQueuePolicy::DropNew,
        subscriber_writer_lanes: 4,
        subscriber_lane_queue_depth: 8192,
        subscriber_lane_queue_policy: felix_broker::SubQueuePolicy::Block,
        max_subscriber_writer_lanes: 8,
        subscriber_lane_shard: crate::config::SubscriberLaneShard::Auto,
        subscriber_single_writer_per_conn: true,
        subscriber_flush_max_items: 64,
        subscriber_flush_max_delay_us: 200,
        subscriber_max_bytes_per_write: 256 * 1024,
        sub_streams_per_conn: 4,
        sub_stream_mode: crate::config::SubStreamMode::PerSubscriber,
    }
}

fn make_payload(payload: &[u8]) -> Bytes {
    Bytes::from(payload.to_vec())
}

fn decode_delivery_payloads(frame: &felix_wire::Frame) -> Result<Vec<Bytes>> {
    if frame.header.flags & felix_wire::FLAG_BINARY_EVENT_BATCH_SHARED != 0 {
        return Ok(felix_wire::binary::decode_shared_event_batch(frame)?.payloads);
    }
    Ok(felix_wire::binary::decode_event_batch(frame)?.payloads)
}

async fn spawn_event_writer(
    rx: mpsc::Receiver<Bytes>,
    config: EventWriterConfig,
) -> Result<(
    tokio::task::JoinHandle<Result<()>>,
    felix_transport::QuicConnection,
)> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let event_send = connection.open_uni().await?;
        run_event_writer(event_send, rx, config).await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    Ok((server_task, connection))
}

#[tokio::test]
async fn write_parts_preserves_order_and_bytes() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(1024);
    let payloads = vec![Bytes::from_static(b"abc"), Bytes::from_static(b"defg")];
    let expected = felix_wire::binary::encode_event_batch_bytes(88, &payloads)?;
    let parts = felix_wire::binary::encode_event_batch_parts(88, &payloads)?;

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });

    let mut out = vec![0u8; expected.len()];
    rx.read_exact(&mut out).await?;
    assert_eq!(out, expected.as_ref());

    write_task.await??;
    Ok(())
}

#[tokio::test]
async fn write_parts_total_bytes_sanity() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(64 * 1024);
    let payloads = (0..128)
        .map(|_| Bytes::from(vec![0xCD; 128]))
        .collect::<Vec<_>>();
    let parts = felix_wire::binary::encode_event_batch_parts(3, &payloads)?;
    let expected_bytes = parts.frame_len();

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
    let mut read = 0usize;
    let mut buf = vec![0u8; 4096];
    while read < expected_bytes {
        let n = rx.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        read += n;
    }
    assert_eq!(read, expected_bytes);
    write_task.await??;
    Ok(())
}

#[tokio::test]
async fn writer_parts_match_legacy_encoded_bytes() -> Result<()> {
    let (mut tx, mut rx) = tokio::io::duplex(16 * 1024);
    let payloads = vec![
        Bytes::from(vec![0x11; 17]),
        Bytes::from(vec![0x22; 256]),
        Bytes::from(vec![0x33; 3]),
    ];
    let expected = felix_wire::binary::encode_event_batch_bytes(17, &payloads)?;
    let parts = felix_wire::binary::encode_event_batch_parts(17, &payloads)?;

    let write_task = tokio::spawn(async move { write_parts_to(&mut tx, &parts).await });
    let mut out = vec![0u8; expected.len()];
    rx.read_exact(&mut out).await?;

    write_task.await??;
    assert_eq!(out, expected.as_ref());
    Ok(())
}

#[tokio::test]
async fn run_event_writer_single_closes_on_channel_close() -> Result<()> {
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 1,
        max_events: 1,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"hello")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
    assert_eq!(batch.subscription_id, 1);
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"hello");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_single_binary_uses_batch_encoding() -> Result<()> {
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 9,
        max_events: 1,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"bin")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode batch")?;
    assert_eq!(batch.subscription_id, 9);
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"bin");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_batches_with_pending_payload() -> Result<()> {
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 7,
        max_events: 10,
        max_bytes: 5,
        flush_delay: Duration::from_millis(50),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"aaaa")).await?;
    tx.send(make_payload(b"bbb")).await?;
    tx.send(make_payload(b"c")).await?;
    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.subscription_id, 7);
    assert_eq!(batch1.payloads.len(), 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"aaaa");

    let frame2 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
    assert_eq!(batch2.subscription_id, 7);
    assert_eq!(batch2.payloads.len(), 2);
    assert_eq!(batch2.payloads[0].as_ref(), b"bbb");
    assert_eq!(batch2.payloads[1].as_ref(), b"c");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_sends_event_stream_binary_batch() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let handled = handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(7),
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await?;
        Result::<bool>::Ok(handled)
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    match ack {
        Outgoing::Message(Message::Subscribed { subscription_id }) => {
            assert_eq!(subscription_id, 7);
        }
        _ => panic!("unexpected ack"),
    }

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let hello = crate::transport::quic::codec::read_message_limited(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("hello");
    match hello {
        Message::EventStreamHello { subscription_id } => {
            assert_eq!(subscription_id, 7);
        }
        other => panic!("unexpected hello: {other:?}"),
    }

    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"hello"),
        )
        .await?;

    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let payloads = decode_delivery_payloads(&frame).context("decode batch")?;
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].as_ref(), b"hello");

    let _ = felix_broker::timings::take_samples();
    let handled = server_task.await.context("server join")??;
    assert!(handled);
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_errors_when_stream_missing() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &WriterLaneManager::new(&test_config()),
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "missing".to_string(),
            Some(11),
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let _connection = client.connect(addr, "localhost").await?;

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("ack missing")?;
    match ack {
        Outgoing::Message(Message::Error { message }) => {
            assert!(message.contains("stream not found"));
        }
        _ => panic!("unexpected ack"),
    }

    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_batches_by_bytes() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.fanout_batch_size = 10;
    config.event_batch_max_events = 10;
    config.event_batch_max_bytes = 6;

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            config,
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(21),
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let _ = out_ack_rx.recv().await;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let _ = crate::transport::quic::codec::read_message_limited(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("hello");

    broker
        .publish("t1", "default", "orders", bytes::Bytes::from_static(b"aa"))
        .await?;
    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"bbbb"),
        )
        .await?;
    broker
        .publish(
            "t1",
            "default",
            "orders",
            bytes::Bytes::from_static(b"ccccc"),
        )
        .await?;

    let frame1 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let payloads1 = decode_delivery_payloads(&frame1).expect("batch1");
    assert_eq!(payloads1.len(), 2);
    assert_eq!(payloads1[0].as_ref(), b"aa");
    assert_eq!(payloads1[1].as_ref(), b"bbbb");

    let frame2 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let payloads2 = decode_delivery_payloads(&frame2).expect("batch2");
    assert_eq!(payloads2.len(), 1);
    assert_eq!(payloads2[0].as_ref(), b"ccccc");

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn lane_fanout_preserves_order_for_multiple_subscribers() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(8);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.subscriber_writer_lanes = 4;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.fanout_batch_size = 1;
    config.event_batch_max_events = 1;

    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        for sub_id in [31_u64, 32_u64] {
            let connection = server.accept().await?;
            handle_subscribe_message(
                broker_for_server.clone(),
                connection,
                config.clone(),
                &Arc::new(SubscriptionLimiter::new()),
                &server_lane_manager,
                &out_ack_tx,
                &out_ack_depth,
                &ack_throttle_tx,
                &ack_timeout_state,
                &cancel_tx,
                "t1".to_string(),
                "default".to_string(),
                "orders".to_string(),
                Some(sub_id),
                None,
                felix_wire::ORIGINAL_V1_FLAGS,
            )
            .await?;
        }
        Result::<()>::Ok(())
    });

    let client1 = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        make_client_config(cert.clone())?,
        transport.clone(),
    )?;
    let client2 = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection1 = client1.connect(addr, "localhost").await?;
    let connection2 = client2.connect(addr, "localhost").await?;

    let mut subscribed = Vec::new();
    for _ in 0..2 {
        let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
            .await
            .context("ack timeout")?
            .context("ack missing")?;
        match ack {
            Outgoing::Message(Message::Subscribed { subscription_id }) => {
                subscribed.push(subscription_id);
            }
            Outgoing::Message(other) => panic!("unexpected ack message: {other:?}"),
            Outgoing::CacheMessage(other) => panic!("unexpected cache ack: {other:?}"),
            Outgoing::PublishAck { request_id, .. } => {
                panic!("unexpected publish ack: {request_id}")
            }
        }
    }
    subscribed.sort_unstable();
    assert_eq!(subscribed, vec![31, 32]);

    let mut event_recv_1 = tokio::time::timeout(Duration::from_secs(1), connection1.accept_uni())
        .await
        .context("accept uni timeout 1")??;
    let mut event_recv_2 = tokio::time::timeout(Duration::from_secs(1), connection2.accept_uni())
        .await
        .context("accept uni timeout 2")??;
    let mut scratch_1 = BytesMut::new();
    let mut scratch_2 = BytesMut::new();
    let _ = crate::transport::quic::codec::read_message_limited(
        &mut event_recv_1,
        16 * 1024,
        &mut scratch_1,
    )
    .await?
    .expect("hello1");
    let _ = crate::transport::quic::codec::read_message_limited(
        &mut event_recv_2,
        16 * 1024,
        &mut scratch_2,
    )
    .await?
    .expect("hello2");

    let expected = (0..20)
        .map(|i| format!("msg-{i}").into_bytes())
        .collect::<Vec<_>>();
    for payload in &expected {
        broker
            .publish(
                "t1",
                "default",
                "orders",
                bytes::Bytes::copy_from_slice(payload.as_slice()),
            )
            .await?;
    }

    let mut recv_1 = Vec::with_capacity(expected.len());
    let mut recv_2 = Vec::with_capacity(expected.len());
    for _ in 0..expected.len() {
        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv_1,
            16 * 1024,
            &mut scratch_1,
        )
        .await?
        .expect("event frame 1");
        let payloads = decode_delivery_payloads(&frame).context("decode batch 1")?;
        recv_1.push(payloads[0].to_vec());

        let frame = crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv_2,
            16 * 1024,
            &mut scratch_2,
        )
        .await?
        .expect("event frame 2");
        let payloads = decode_delivery_payloads(&frame).context("decode batch 2")?;
        recv_2.push(payloads[0].to_vec());
    }

    assert_eq!(recv_1, expected);
    assert_eq!(recv_2, expected);

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn lane_selection_auto_pins_shared_connection_to_one_lane() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.subscriber_single_writer_per_conn = true;
    let manager = WriterLaneManager::new(&config);

    let lane_a = manager.select_lane(100, Some(7));
    let lane_b = manager.select_lane(200, Some(7));
    let lane_c = manager.select_lane(300, Some(7));
    assert_eq!(lane_a, lane_b);
    assert_eq!(lane_b, lane_c);

    Ok(())
}

#[tokio::test]
async fn lane_selection_auto_uses_subscriber_hash_when_conn_pin_disabled() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::Auto;
    config.subscriber_single_writer_per_conn = false;
    let manager = WriterLaneManager::new(&config);

    let lane_a = manager.select_lane(100, Some(7));
    let lane_b = manager.select_lane(200, Some(7));
    let lane_c = manager.select_lane(100, Some(7));
    assert_eq!(lane_a, lane_c);
    assert_ne!(lane_a, lane_b);

    Ok(())
}

#[tokio::test]
async fn round_robin_pin_keeps_subscriber_sticky() -> Result<()> {
    let mut config = test_config();
    config.subscriber_writer_lanes = 4;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
    let manager = WriterLaneManager::new(&config);

    let first = manager.select_lane(42, None);
    let second = manager.select_lane(42, None);
    let third = manager.select_lane(42, None);
    assert_eq!(first, second);
    assert_eq!(second, third);

    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_hashed_pool_with_generated_id() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let mut config = test_config();
    config.sub_stream_mode = crate::config::SubStreamMode::HashedPool;
    let broker_for_server = broker.clone();
    let lane_manager = WriterLaneManager::new(&test_config());
    let server_lane_manager = Arc::clone(&lane_manager);
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        handle_subscribe_message(
            broker_for_server,
            connection,
            config,
            &Arc::new(SubscriptionLimiter::new()),
            &server_lane_manager,
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            None,
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let subscription_id = match out_ack_rx.recv().await.context("missing ack")? {
        Outgoing::Message(Message::Subscribed { subscription_id }) => subscription_id,
        _ => panic!("unexpected ack"),
    };
    assert!(subscription_id > 0);

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let hello = crate::transport::quic::codec::read_message_limited(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("hello");
    match hello {
        Message::EventStreamHello {
            subscription_id: hello_id,
        } => assert_eq!(hello_id, subscription_id),
        other => panic!("unexpected hello: {other:?}"),
    }

    broker
        .publish("t1", "default", "orders", bytes::Bytes::from_static(b"ok"))
        .await?;
    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("event frame");
    let payloads = decode_delivery_payloads(&frame).context("decode batch")?;
    assert_eq!(payloads[0].as_ref(), b"ok");

    let _ = felix_broker::timings::take_samples();
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn handle_subscribe_message_open_uni_failure_sends_error_ack() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(tokio::sync::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);

    let broker_for_server = broker.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle_subscribe_message(
            broker_for_server,
            connection,
            test_config(),
            &Arc::new(SubscriptionLimiter::new()),
            &WriterLaneManager::new(&test_config()),
            &out_ack_tx,
            &out_ack_depth,
            &ack_throttle_tx,
            &ack_timeout_state,
            &cancel_tx,
            "t1".to_string(),
            "default".to_string(),
            "orders".to_string(),
            Some(900),
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    drop(connection);

    let ack = tokio::time::timeout(Duration::from_secs(1), out_ack_rx.recv())
        .await
        .context("ack timeout")?
        .context("missing ack")?;
    match ack {
        Outgoing::Message(Message::Error { message }) => {
            assert!(!message.is_empty());
        }
        _ => panic!("expected error ack"),
    }
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn write_parts_many_writes_two_frames_in_order() -> Result<()> {
    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;
    let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();

    let server_task = tokio::spawn(async move {
        let connection = server.accept().await?;
        let mut send = connection.open_uni().await?;
        let _ = opened_tx.send(());
        let frames = vec![
            felix_wire::binary::encode_event_batch_parts(1, &[Bytes::from_static(b"aa")])?,
            felix_wire::binary::encode_event_batch_parts(2, &[Bytes::from_static(b"bbb")])?,
        ];
        let result = write_parts_many(&mut send, frames).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        result
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;
    opened_rx.await.context("uni stream not opened")?;
    let mut recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 =
        crate::transport::quic::codec::read_frame_limited_into(&mut recv, 16 * 1024, &mut scratch)
            .await?
            .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode frame1");
    assert_eq!(batch1.subscription_id, 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"aa");

    let frame2 =
        crate::transport::quic::codec::read_frame_limited_into(&mut recv, 16 * 1024, &mut scratch)
            .await?
            .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode frame2");
    assert_eq!(batch2.subscription_id, 2);
    assert_eq!(batch2.payloads[0].as_ref(), b"bbb");

    let total = server_task.await.context("server join")??;
    assert!(total > 0);
    Ok(())
}

#[tokio::test]
async fn run_event_writer_flushes_by_count_and_deadline() -> Result<()> {
    let (tx, rx) = mpsc::channel(8);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 44,
        max_events: 2,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(20),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };
    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    tx.send(make_payload(b"a")).await?;
    tx.send(make_payload(b"b")).await?;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("count-based frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode count batch");
    assert_eq!(batch.payloads.len(), 2);
    assert_eq!(batch.payloads[0].as_ref(), b"a");
    assert_eq!(batch.payloads[1].as_ref(), b"b");

    tx.send(make_payload(b"deadline")).await?;
    let frame = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("deadline frame");
    let batch = felix_wire::binary::decode_event_batch(&frame).expect("decode deadline batch");
    assert_eq!(batch.payloads.len(), 1);
    assert_eq!(batch.payloads[0].as_ref(), b"deadline");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

#[tokio::test]
async fn run_event_writer_flushes_on_channel_close() -> Result<()> {
    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 55,
        max_events: 8,
        max_bytes: 1024,
        flush_delay: Duration::from_secs(5),
        single_event_mode: false,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    tx.send(make_payload(b"closed")).await?;
    drop(tx);
    // Keep the client connection open until the writer has flushed: dropping it
    // first races CONNECTION_CLOSE against the flush and fails intermittently.
    server_task.await.context("server join")??;
    drop(connection);
    Ok(())
}

#[tokio::test]
async fn run_event_writer_single_event_mode_writes_multiple_frames() -> Result<()> {
    let (tx, rx) = mpsc::channel(4);
    let config = EventWriterConfig {
        offsets_enabled: false,
        subscription_id: 66,
        max_events: 2,
        max_bytes: 1024,
        flush_delay: Duration::from_millis(10),
        single_event_mode: true,
        flush_max_items: 64,
        flush_max_delay: Duration::from_micros(200),
        max_bytes_per_write: 256 * 1024,
    };

    let (server_task, connection) = spawn_event_writer(rx, config).await?;
    let accept_uni = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni());
    tx.send(make_payload(b"one")).await?;
    tx.send(make_payload(b"two")).await?;

    let mut event_recv = accept_uni.await.context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).expect("decode batch1");
    assert_eq!(batch1.payloads.len(), 1);
    assert_eq!(batch1.payloads[0].as_ref(), b"one");

    let frame2 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).expect("decode batch2");
    assert_eq!(batch2.payloads.len(), 1);
    assert_eq!(batch2.payloads[0].as_ref(), b"two");

    drop(tx);
    server_task.await.context("server join")??;
    Ok(())
}

// Unique per call, which `SystemTime::now()` is not: consecutive reads can
// return an identical value (observed on macOS, 0 ns between reads). These
// tests key into the process-global `ACTIVE_SUB_CONN_COUNTS`, so colliding
// ids meant tests running in parallel clobbered each other's entries — a
// ~1-in-30 flake that never reproduced when a test ran alone.
//
// The high bit is set so these can never collide with a real quinn
// `stable_id()` in the same process either.
fn unique_test_connection_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    0x8000_0000_0000_0000 | NEXT.fetch_add(1, Ordering::Relaxed)
}

#[test]
fn connection_subscriber_register_unregister_tracks_counts() {
    let connection_id = unique_test_connection_id();

    connection_subscriber_unregister(None);
    connection_subscriber_register(Some(connection_id));
    connection_subscriber_register(Some(connection_id));
    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    let count = map
        .get(&connection_id)
        .expect("connection count should exist");
    assert_eq!(*count, 2);
    drop(count);

    connection_subscriber_unregister(Some(connection_id));
    let count = map
        .get(&connection_id)
        .expect("connection count should still exist");
    assert_eq!(*count, 1);
    drop(count);

    connection_subscriber_unregister(Some(connection_id));
    assert!(map.get(&connection_id).is_none());
}

// Regression test for the leak the soak harness found (#154).
//
// Subscription teardown enqueues `LaneCommand::Unregister` and then *immediately*
// calls `unregister_subscriber`, which removes the `subscriber_connections` entry.
// The lane worker dequeues afterwards, so when it used to look the connection up
// there it found nothing and skipped cleanup entirely — leaving an entry in
// `ACTIVE_SUB_CONN_COUNTS` and a per-connection metric series behind for every
// subscriber connection ever made. Over a long-lived broker with connection churn
// that is unbounded growth, and it made `felix_sub_active_connections` permanently
// wrong. The command now carries `connection_id` so the lookup is not needed.
#[tokio::test]
async fn lane_unregister_cleans_up_after_teardown_already_removed_the_mapping() {
    let manager = WriterLaneManager::new(&test_config());
    let connection_id = unique_test_connection_id();
    let subscriber_id = connection_id ^ 0x5555;

    connection_subscriber_register(Some(connection_id));
    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    assert!(map.get(&connection_id).is_some());

    // Reproduce the race: teardown has already dropped the mapping the worker
    // used to depend on, before the worker gets to the command.
    manager.subscriber_connections.remove(&subscriber_id);

    manager
        .enqueue(
            0,
            LaneCommand::Unregister {
                subscriber_id,
                connection_id: Some(connection_id),
            },
        )
        .await
        .expect("enqueue unregister");

    // The worker runs on its own task, so poll rather than assume immediacy.
    let mut cleared = false;
    for _ in 0..200 {
        if map.get(&connection_id).is_none() {
            cleared = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        cleared,
        "lane worker must release the connection's subscriber count even when \
         teardown already removed the subscriber_connections entry"
    );
}

#[test]
fn connection_subscriber_unregister_no_map_is_noop() {
    let connection_id = unique_test_connection_id();
    connection_subscriber_unregister(Some(connection_id));
}

#[tokio::test]
async fn connection_id_hash_falls_back_to_subscriber_hash_without_connection() {
    let mut config = test_config();
    config.subscriber_writer_lanes = 8;
    config.max_subscriber_writer_lanes = 8;
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::ConnectionIdHash;
    config.subscriber_single_writer_per_conn = false;
    let manager = WriterLaneManager::new(&config);

    let without_conn = manager.select_lane(555, None);
    let expected = manager.lane_for_subscriber(555);
    assert_eq!(without_conn, expected);

    let with_conn_a = manager.select_lane(555, Some(42));
    let with_conn_b = manager.select_lane(999, Some(42));
    assert_eq!(with_conn_a, with_conn_b);
}

#[tokio::test]
async fn unregister_subscriber_clears_internal_maps() {
    let mut config = test_config();
    config.subscriber_lane_shard = crate::config::SubscriberLaneShard::RoundRobinPin;
    let manager = WriterLaneManager::new(&config);
    manager.subscriber_pins.insert(7, 2);
    manager.subscriber_connections.insert(7, 88);
    manager.connection_lanes.insert(88, 1);

    manager.unregister_subscriber(7, Some(88));

    assert!(manager.subscriber_pins.get(&7).is_none());
    assert!(manager.subscriber_connections.get(&7).is_none());
    assert!(manager.connection_lanes.get(&88).is_none());
}

#[tokio::test]
async fn writer_lane_tasks_do_not_retain_manager() {
    let manager = WriterLaneManager::new(&test_config());
    let weak_manager = Arc::downgrade(&manager);

    drop(manager);

    assert!(
        weak_manager.upgrade().is_none(),
        "writer lane tasks must not keep a connection's manager alive"
    );
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn manager_drop_releases_remaining_connection_counts() {
    let manager = WriterLaneManager::new(&test_config());
    let connection_id = unique_test_connection_id();
    let subscriber_id = connection_id ^ 0xaaaa;

    connection_subscriber_register(Some(connection_id));
    manager
        .subscriber_connections
        .insert(subscriber_id, connection_id);

    drop(manager);

    let map = ACTIVE_SUB_CONN_COUNTS
        .get()
        .expect("counts map should be initialized");
    assert!(
        map.get(&connection_id).is_none(),
        "dropping the manager must release registrations whose queued unregister did not run"
    );
}

#[tokio::test]
async fn concurrent_lanes_share_one_connection_writer() {
    let manager = WriterLaneManager::new(&test_config());
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            manager.ensure_connection_writer(42, None)
        }));
    }

    let mut senders = Vec::new();
    for task in tasks {
        senders.push(task.await.expect("connection writer task"));
    }

    assert_eq!(manager.connection_writers.len(), 1);
    assert!(
        senders[1..]
            .iter()
            .all(|sender| sender.same_channel(&senders[0]))
    );
}

#[tokio::test]
async fn run_connection_writer_coalesces_multiple_deliveries() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders").await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let frame_a = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    let frame_b = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"bb")])?;
    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: frame_a,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery a")?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: frame_b,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery b")?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");
    let frame2 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame2");
    let batch2 = felix_wire::binary::decode_event_batch(&frame2).context("decode batch2")?;
    assert_eq!(batch2.payloads[0].as_ref(), b"bb");

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::timings::take_samples();
    Ok(())
}

#[tokio::test]
async fn run_connection_writer_handles_write_error() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders").await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;
    drop(client_conn);

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let frame = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    let now = Instant::now();
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::timings::take_samples();
    Ok(())
}

#[tokio::test]
async fn run_connection_writer_unregister_drops_late_deliveries() -> Result<()> {
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_stream(
            "t1",
            "default",
            "orders",
            felix_broker::StreamMetadata::default(),
        )
        .await?;
    let subscription = broker.subscribe("t1", "default", "orders").await?;
    let (_rx, guard) = subscription.into_parts();

    let (server_config, cert) = make_server_config()?;
    let transport = TransportConfig::default();
    let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move { server.accept().await });
    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let client_conn = client.connect(addr, "localhost").await?;

    let connection = server_task.await.context("server join")??;
    let connection_id = connection.info().id.0;
    let event_send = connection.open_uni().await?;

    let (tx, rx) = mpsc::channel(8);
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

    tx.send(ConnectionCommand::Register {
        subscriber_id: 1,
        connection: connection.clone(),
        connection_id: Some(connection_id),
        event_send,
        guard,
    })
    .await
    .context("register")?;

    let now = Instant::now();
    let frame = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"a")])?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame1 = crate::transport::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");

    tx.send(ConnectionCommand::Unregister { subscriber_id: 1 })
        .await
        .context("unregister")?;

    let late = felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"late")])?;
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: late,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("late delivery")?;

    let no_frame = tokio::time::timeout(
        Duration::from_millis(150),
        crate::transport::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await;
    match no_frame {
        Err(_) => {}
        Ok(Ok(None)) => {}
        Ok(Ok(Some(_))) => panic!("unexpected late frame"),
        Ok(Err(err)) => return Err(err),
    }

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::timings::take_samples();
    Ok(())
}
