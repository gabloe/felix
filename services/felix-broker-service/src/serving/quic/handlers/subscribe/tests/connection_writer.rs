//! The shared per-connection writer.

use super::*;

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
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
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
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");
    let frame2 = crate::serving::quic::codec::read_frame_limited_into(
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
    let _ = crate::observability::timings::take_samples();
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
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
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
    let _ = crate::observability::timings::take_samples();
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
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
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
    let frame1 = crate::serving::quic::codec::read_frame_limited_into(
        &mut event_recv,
        16 * 1024,
        &mut scratch,
    )
    .await?
    .expect("frame1");
    let batch1 = felix_wire::binary::decode_event_batch(&frame1).context("decode batch1")?;
    assert_eq!(batch1.payloads[0].as_ref(), b"a");

    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        last: None,
    })
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
        crate::serving::quic::codec::read_frame_limited_into(
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
    let _ = crate::observability::timings::take_samples();
    Ok(())
}

/// A subscription that ends right after its last frames still gets them.
/// The feeder queues the last deliveries and then the unregister, and when
/// the writer picks all of them up in one batch it must write the frames
/// before letting the stream go -- the last one is what tells a client whose
/// shard moved where to resume.
#[tokio::test]
async fn run_connection_writer_writes_queued_frames_before_an_unregister() -> Result<()> {
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
    let subscription = broker.subscribe("t1", "default", "orders", 0).await?;
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

    // Queued before the writer runs, so it drains all three as one batch.
    let (tx, rx) = mpsc::channel(8);
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
    tx.send(ConnectionCommand::Delivery {
        subscriber_id: 1,
        frame: felix_wire::binary::encode_event_batch_bytes(1, &[Bytes::from_static(b"last")])?,
        item_count: 1,
        first_enqueued_at: now,
        enqueue_at: now,
    })
    .await
    .context("delivery")?;
    tx.send(ConnectionCommand::Unregister {
        subscriber_id: 1,
        last: None,
    })
    .await
    .context("unregister")?;
    let writer_task = tokio::spawn(run_connection_writer(connection_id, rx, 64 * 1024));

    let mut event_recv = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut scratch = BytesMut::new();
    let frame = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the last frame never arrived")??
    .context("the stream ended without the last frame")?;
    let batch = felix_wire::binary::decode_event_batch(&frame).context("decode")?;
    assert_eq!(batch.payloads[0].as_ref(), b"last");
    let end = tokio::time::timeout(
        Duration::from_secs(2),
        crate::serving::quic::codec::read_frame_limited_into(
            &mut event_recv,
            16 * 1024,
            &mut scratch,
        ),
    )
    .await
    .context("the stream was not finished")??;
    assert!(end.is_none(), "nothing follows the unregister");

    drop(tx);
    writer_task.await.context("writer join")?;
    let _ = crate::observability::timings::take_samples();
    Ok(())
}
