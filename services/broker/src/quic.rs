// QUIC transport adapter for the broker.
// Decodes felix-wire frames, enforces stream scope, and fans out events to subscribers.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
#[cfg(test)]
use felix_broker::CacheMetadata;
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::broadcast;

pub async fn serve(server: Arc<QuicServer>, broker: Arc<Broker>) -> Result<()> {
    loop {
        let connection = server.accept().await?;
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(broker, connection).await {
                tracing::warn!(error = %err, "quic connection handler failed");
            }
        });
    }
}

async fn handle_connection(broker: Arc<Broker>, connection: QuicConnection) -> Result<()> {
    loop {
        // Accept both bi-directional control streams and uni-directional publish streams.
        tokio::select! {
            result = connection.accept_bi() => {
                let (send, recv) = match result {
                    Ok(streams) => streams,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(err) = handle_stream(broker, send, recv).await {
                        tracing::warn!(error = %err, "quic stream handler failed");
                    }
                });
            }
            result = connection.accept_uni() => {
                let recv = match result {
                    Ok(recv) => recv,
                    Err(err) => {
                        tracing::info!(error = %err, "quic connection closed");
                        return Ok(());
                    }
                };
                let broker = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(err) = handle_uni_stream(broker, recv).await {
                        tracing::warn!(error = %err, "quic uni stream handler failed");
                    }
                });
            }
        }
    }
}

async fn handle_stream(
    broker: Arc<Broker>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    loop {
        let frame = match read_frame(&mut recv).await? {
            Some(frame) => frame,
            None => break,
        };
        // Binary batches bypass JSON decoding for high-throughput publish.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let batch = felix_wire::binary::decode_publish_batch(&frame)
                .context("decode binary publish batch")?;
            let span = tracing::info_span!(
                "publish_batch_binary",
                tenant_id = %batch.tenant_id,
                namespace = %batch.namespace,
                stream = %batch.stream,
                count = batch.payloads.len()
            );
            let _enter = span.enter();
            for payload in batch.payloads {
                broker
                    .publish(
                        &batch.tenant_id,
                        &batch.namespace,
                        &batch.stream,
                        Bytes::from(payload),
                    )
                    .await?;
            }
            continue;
        }
        let message = Message::decode(frame).context("decode message")?;
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ack,
            } => {
                // Per-message publish path with optional ack.
                let start = Instant::now();
                let span = tracing::trace_span!(
                    "publish",
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream
                );
                let _enter = span.enter();
                match broker
                    .publish(
                        &tenant_id,
                        &namespace,
                        &stream,
                        Bytes::from(payload.clone()),
                    )
                    .await
                {
                    Ok(_) => {
                        metrics::counter!("felix_publish_requests_total", "result" => "ok")
                            .increment(1);
                        metrics::counter!("felix_publish_bytes_total")
                            .increment(payload.len() as u64);
                        metrics::histogram!("felix_publish_latency_ms")
                            .record(start.elapsed().as_secs_f64() * 1000.0);
                        if ack.unwrap_or(felix_wire::AckMode::PerMessage)
                            != felix_wire::AckMode::None
                        {
                            write_message(&mut send, Message::Ok).await?;
                        }
                    }
                    Err(err) => {
                        metrics::counter!("felix_publish_requests_total", "result" => "error")
                            .increment(1);
                        if ack.unwrap_or(felix_wire::AckMode::PerMessage)
                            != felix_wire::AckMode::None
                        {
                            write_message(
                                &mut send,
                                Message::Error {
                                    message: err.to_string(),
                                },
                            )
                            .await?;
                        }
                    }
                }
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ack,
            } => {
                // Batch publish trades latency for throughput, returns a single ack.
                let span = tracing::trace_span!(
                    "publish_batch",
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream,
                    count = payloads.len()
                );
                let _enter = span.enter();
                let mut ok = true;
                for payload in payloads {
                    if broker
                        .publish(
                            &tenant_id,
                            &namespace,
                            &stream,
                            Bytes::from(payload.clone()),
                        )
                        .await
                        .is_err()
                    {
                        ok = false;
                        break;
                    }
                    metrics::counter!("felix_publish_requests_total", "result" => "ok")
                        .increment(1);
                    metrics::counter!("felix_publish_bytes_total").increment(payload.len() as u64);
                }
                let ack_mode = ack.unwrap_or(felix_wire::AckMode::PerBatch);
                if ack_mode != felix_wire::AckMode::None {
                    if ok {
                        write_message(&mut send, Message::Ok).await?;
                    } else {
                        write_message(
                            &mut send,
                            Message::Error {
                                message: "publish batch failed".to_string(),
                            },
                        )
                        .await?;
                    }
                }
            }
            Message::Subscribe {
                tenant_id,
                namespace,
                stream,
            } => {
                // Subscribe keeps the stream open and pushes events until the client closes.
                let span = tracing::trace_span!(
                    "subscribe",
                    tenant_id = %tenant_id,
                    namespace = %namespace,
                    stream = %stream
                );
                let _enter = span.enter();
                let mut receiver = match broker.subscribe(&tenant_id, &namespace, &stream).await {
                    Ok(receiver) => receiver,
                    Err(err) => {
                        metrics::counter!("felix_subscribe_requests_total", "result" => "error")
                            .increment(1);
                        write_message(
                            &mut send,
                            Message::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                        send.finish()?;
                        let _ = send.stopped().await;
                        return Ok(());
                    }
                };
                metrics::counter!("felix_subscribe_requests_total", "result" => "ok").increment(1);
                write_message(&mut send, Message::Ok).await?;
                let batch_size = fanout_batch_size();
                if batch_size <= 1 {
                    // Latency-focused path: send each event immediately.
                    loop {
                        match receiver.recv().await {
                            Ok(payload) => {
                                let message = Message::Event {
                                    tenant_id: tenant_id.clone(),
                                    namespace: namespace.clone(),
                                    stream: stream.clone(),
                                    payload: payload.to_vec(),
                                };
                                metrics::counter!("felix_subscribe_bytes_total")
                                    .increment(payload.len() as u64);
                                if let Err(err) = write_message(&mut send, message).await {
                                    tracing::info!(error = %err, "subscription stream closed");
                                    return Ok(());
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                continue;
                            }
                        }
                    }
                } else {
                    // Throughput-focused path: batch subscriber sends to reduce overhead.
                    loop {
                        let payload = match receiver.recv().await {
                            Ok(payload) => payload,
                            Err(broadcast::error::RecvError::Closed) => break,
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                continue;
                            }
                        };
                        let mut batch = Vec::with_capacity(batch_size.max(1));
                        batch.push(payload);
                        for _ in 1..batch_size {
                            match receiver.try_recv() {
                                Ok(payload) => batch.push(payload),
                                Err(broadcast::error::TryRecvError::Empty) => break,
                                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                                    metrics::counter!("felix_subscribe_dropped_total").increment(1);
                                    continue;
                                }
                                Err(broadcast::error::TryRecvError::Closed) => break,
                            }
                        }
                        for payload in batch {
                            let message = Message::Event {
                                tenant_id: tenant_id.clone(),
                                namespace: namespace.clone(),
                                stream: stream.clone(),
                                payload: payload.to_vec(),
                            };
                            metrics::counter!("felix_subscribe_bytes_total")
                                .increment(payload.len() as u64);
                            if let Err(err) = write_message(&mut send, message).await {
                                tracing::info!(error = %err, "subscription stream closed");
                                return Ok(());
                            }
                        }
                    }
                }
                return Ok(());
            }
            Message::CachePut {
                tenant_id,
                namespace,
                cache,
                key,
                value,
                ttl_ms,
            } => {
                if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                    write_message(
                        &mut send,
                        Message::Error {
                            message: format!(
                                "cache scope not found: {tenant_id}/{namespace}/{cache}"
                            ),
                        },
                    )
                    .await?;
                    send.finish()?;
                    let _ = send.stopped().await;
                    return Ok(());
                }
                let ttl = ttl_ms.map(Duration::from_millis);
                broker
                    .cache()
                    .put(tenant_id, namespace, cache, key, Bytes::from(value), ttl)
                    .await;
                write_message(&mut send, Message::Ok).await?;
                send.finish()?;
                let _ = send.stopped().await;
                return Ok(());
            }
            Message::CacheGet {
                tenant_id,
                namespace,
                cache,
                key,
            } => {
                if !broker.cache_exists(&tenant_id, &namespace, &cache).await {
                    write_message(
                        &mut send,
                        Message::Error {
                            message: format!(
                                "cache scope not found: {tenant_id}/{namespace}/{cache}"
                            ),
                        },
                    )
                    .await?;
                    send.finish()?;
                    let _ = send.stopped().await;
                    return Ok(());
                }
                let value = broker
                    .cache()
                    .get(&tenant_id, &namespace, &cache, &key)
                    .await
                    .map(|b| b.to_vec());
                write_message(
                    &mut send,
                    Message::CacheValue {
                        tenant_id,
                        namespace,
                        cache,
                        key,
                        value,
                    },
                )
                .await?;
                send.finish()?;
                let _ = send.stopped().await;
                return Ok(());
            }
            Message::CacheValue { .. } | Message::Event { .. } | Message::Ok => {
                write_message(
                    &mut send,
                    Message::Error {
                        message: "unexpected message type".to_string(),
                    },
                )
                .await?;
                send.finish()?;
                return Ok(());
            }
            Message::Error { .. } => {
                send.finish()?;
                return Ok(());
            }
        }
    }
    send.finish()?;
    let _ = send.stopped().await;
    Ok(())
}

async fn handle_uni_stream(broker: Arc<Broker>, mut recv: RecvStream) -> Result<()> {
    loop {
        let frame = match read_frame(&mut recv).await? {
            Some(frame) => frame,
            None => break,
        };
        // Uni streams are publish-only; responses are not sent.
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_BATCH != 0 {
            let batch = felix_wire::binary::decode_publish_batch(&frame)
                .context("decode binary publish batch")?;
            for payload in batch.payloads {
                broker
                    .publish(
                        &batch.tenant_id,
                        &batch.namespace,
                        &batch.stream,
                        Bytes::from(payload),
                    )
                    .await?;
            }
            continue;
        }
        let message = Message::decode(frame).context("decode message")?;
        match message {
            Message::Publish {
                tenant_id,
                namespace,
                stream,
                payload,
                ..
            } => {
                broker
                    .publish(&tenant_id, &namespace, &stream, Bytes::from(payload))
                    .await?;
            }
            Message::PublishBatch {
                tenant_id,
                namespace,
                stream,
                payloads,
                ..
            } => {
                for payload in payloads {
                    broker
                        .publish(&tenant_id, &namespace, &stream, Bytes::from(payload))
                        .await?;
                }
            }
            _ => {
                tracing::debug!("ignored non-publish message on uni stream");
            }
        }
    }
    Ok(())
}

pub async fn read_message(recv: &mut RecvStream) -> Result<Option<Message>> {
    let frame = match read_frame(recv).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
}

pub async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    write_frame(send, frame).await
}

async fn read_frame(recv: &mut RecvStream) -> Result<Option<Frame>> {
    let mut header_bytes = [0u8; FrameHeader::LEN];
    match recv.read_exact(&mut header_bytes).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(ReadExactError::ReadError(err)) => return Err(err.into()),
    }

    let header = FrameHeader::decode(Bytes::copy_from_slice(&header_bytes))
        .context("decode frame header")?;
    let length = usize::try_from(header.length).context("frame length")?;
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload)
        .await
        .context("read frame payload")?;
    Ok(Some(Frame {
        header,
        payload: Bytes::from(payload),
    }))
}

async fn write_frame(send: &mut SendStream, frame: Frame) -> Result<()> {
    send.write_all(&frame.encode()).await.context("write frame")
}

fn fanout_batch_size() -> usize {
    std::env::var("FELIX_FANOUT_BATCH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;
    use felix_transport::{QuicClient, TransportConfig};
    use quinn::ClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;

    #[tokio::test]
    async fn cache_put_get_round_trip() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
        broker.register_tenant("t1").await?;
        broker.register_namespace("t1", "default").await?;
        broker
            .register_cache("t1", "default", "primary", CacheMetadata)
            .await?;
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(serve(Arc::clone(&server), Arc::clone(&broker)));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CachePut {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: b"cached".to_vec(),
                ttl_ms: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert_eq!(response, Some(Message::Ok));

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CacheGet {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert_eq!(
            response,
            Some(Message::CacheValue {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                cache: "primary".to_string(),
                key: "demo-key".to_string(),
                value: Some(b"cached".to_vec()),
            })
        );

        drop(connection);
        server_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn publish_rejects_unknown_stream() -> Result<()> {
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
        let (server_config, cert) = build_server_config()?;
        let server = Arc::new(QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?);
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(serve(Arc::clone(&server), Arc::clone(&broker)));

        let client = QuicClient::bind(
            "0.0.0.0:0".parse()?,
            build_client_config(cert)?,
            TransportConfig::default(),
        )?;
        let connection = client.connect(addr, "localhost").await?;

        let (mut send, mut recv) = connection.open_bi().await?;
        write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                ack: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert!(matches!(response, Some(Message::Error { .. })));

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
        write_message(
            &mut send,
            Message::Publish {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "missing".to_string(),
                payload: b"payload".to_vec(),
                ack: None,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert_eq!(response, Some(Message::Ok));

        drop(connection);
        server_task.abort();
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
}
