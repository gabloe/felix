//! Subscribe requests on the control stream, end to end over QUIC.

use super::*;

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
        Outgoing::Message(Message::Subscribed {
            subscription_id, ..
        }) => {
            assert_eq!(subscription_id, 7);
        }
        _ => panic!("unexpected ack"),
    }

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let hello =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
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

    let frame = crate::serving::quic::codec::read_frame_limited_into(
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
    let _ =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
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

    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
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

    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
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
            None,
            felix_wire::ORIGINAL_V1_FLAGS,
        )
        .await
    });

    let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
    let connection = client.connect(addr, "localhost").await?;

    let subscription_id = match out_ack_rx.recv().await.context("missing ack")? {
        Outgoing::Message(Message::Subscribed {
            subscription_id, ..
        }) => subscription_id,
        _ => panic!("unexpected ack"),
    };
    assert!(subscription_id > 0);

    let mut event_recv = tokio::time::timeout(Duration::from_secs(1), connection.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let hello =
        crate::serving::quic::codec::read_message_limited(&mut event_recv, 16 * 1024, &mut scratch)
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
    let frame = crate::serving::quic::codec::read_frame_limited_into(
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
