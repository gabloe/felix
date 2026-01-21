use anyhow::{Context, Result, anyhow};
use broker::quic;
use bytes::Bytes;
use felix_broker::{Broker, CacheMetadata};
use felix_client::Client;
use felix_storage::EphemeralCache;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, FLAG_BINARY_EVENT_BATCH, Frame, FrameHeader, Message};
use quinn::{ClientConfig, ReadExactError, RecvStream};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const MAX_TEST_FRAME_BYTES: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    println!("== Felix Conformance Runner ==");
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    broker.register_tenant("t1").await?;
    broker.register_namespace("t1", "default").await?;
    broker
        .register_cache("t1", "default", "primary", CacheMetadata)
        .await?;
    broker
        .register_stream("t1", "default", "conformance", Default::default())
        .await?;
    let (server_config, cert) = build_server_config().context("build server config")?;
    let server = Arc::new(QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?);
    let addr = server.local_addr()?;
    let config = broker::config::BrokerConfig::from_env()?;
    let server_task = tokio::spawn(quic::serve(
        Arc::clone(&server),
        Arc::clone(&broker),
        config,
    ));

    let client = QuicClient::bind(
        "0.0.0.0:0".parse()?,
        build_client_config(cert.clone())?,
        TransportConfig::default(),
    )?;
    let connection = client.connect(addr, "localhost").await?;

    run_pubsub(&connection).await?;
    run_cache(&connection).await?;
    run_client_pubsub(addr, cert.clone()).await?;
    run_client_cache(addr, cert).await?;

    drop(connection);
    server_task.abort();
    println!("Conformance checks passed.");
    Ok(())
}

async fn run_pubsub(connection: &felix_transport::QuicConnection) -> Result<()> {
    println!("Running pub/sub checks...");
    let (mut sub_send, mut sub_recv) = connection.open_bi().await?;
    quic::write_message(
        &mut sub_send,
        Message::Subscribe {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "conformance".to_string(),
            subscription_id: None,
        },
    )
    .await?;
    sub_send.finish()?;
    let response = quic::read_message_limited(&mut sub_recv, MAX_TEST_FRAME_BYTES).await?;
    let expected_id = match response {
        Some(Message::Subscribed { subscription_id }) => Some(subscription_id),
        Some(Message::Ok) => None,
        other => return Err(anyhow!("subscribe failed: {other:?}")),
    };
    let mut event_recv = connection.accept_uni().await?;

    publish(connection, b"alpha").await?;
    publish(connection, b"beta").await?;

    let mut pending = VecDeque::new();
    if let Some(frame) = read_frame(&mut event_recv).await? {
        handle_event_frame(expected_id, frame, &mut pending, true)?;
    }

    let mut received = Vec::new();
    for _ in 0..2 {
        let payload = if let Some(payload) = pending.pop_front() {
            payload
        } else {
            let frame = read_frame(&mut event_recv)
                .await?
                .ok_or_else(|| anyhow!("subscription ended early"))?;
            handle_event_frame(expected_id, frame, &mut pending, false)?;
            pending
                .pop_front()
                .ok_or_else(|| anyhow!("subscription ended early"))?
        };
        received.push(payload);
    }

    if received != [b"alpha".to_vec(), b"beta".to_vec()] {
        return Err(anyhow!("unexpected event order: {received:?}"));
    }
    Ok(())
}

async fn publish(connection: &felix_transport::QuicConnection, payload: &[u8]) -> Result<()> {
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::Publish {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "conformance".to_string(),
            payload: payload.to_vec(),
            request_id: Some(request_id),
            ack: Some(AckMode::PerMessage),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES).await?;
    if response != Some(Message::PublishOk { request_id }) {
        return Err(anyhow!("publish failed: {response:?}"));
    }
    Ok(())
}

async fn run_cache(connection: &felix_transport::QuicConnection) -> Result<()> {
    println!("Running cache checks...");
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::CachePut {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: "conformance-key".to_string(),
            value: Bytes::from_static(b"value"),
            request_id: None,
            ttl_ms: Some(100),
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES).await?;
    if response != Some(Message::Ok) {
        return Err(anyhow!("cache put failed: {response:?}"));
    }

    let value = cache_get(connection, "conformance-key").await?;
    if value != Some(Bytes::from_static(b"value")) {
        return Err(anyhow!("cache get mismatch: {value:?}"));
    }

    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = cache_get(connection, "conformance-key").await?;
    if expired.is_some() {
        return Err(anyhow!("cache entry should be expired"));
    }
    Ok(())
}

async fn run_client_pubsub(
    addr: std::net::SocketAddr,
    cert: CertificateDer<'static>,
) -> Result<()> {
    println!("Running client pub/sub checks...");
    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;
    let mut subscription = client.subscribe("t1", "default", "conformance").await?;
    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "conformance",
            b"client-alpha".to_vec(),
            AckMode::None,
        )
        .await?;
    let event = subscription
        .next_event()
        .await?
        .ok_or_else(|| anyhow!("client subscription ended early"))?;
    if event.payload != Bytes::from_static(b"client-alpha") {
        return Err(anyhow!("client event mismatch: {:?}", event.payload));
    }
    publisher.finish().await?;
    Ok(())
}

async fn run_client_cache(addr: std::net::SocketAddr, cert: CertificateDer<'static>) -> Result<()> {
    println!("Running client cache checks...");
    let client = Client::connect(addr, "localhost", build_client_config(cert)?).await?;
    client
        .cache_put(
            "t1",
            "default",
            "primary",
            "client-key",
            Bytes::from_static(b"value"),
            Some(100),
        )
        .await?;
    let value = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    if value != Some(Bytes::from_static(b"value")) {
        return Err(anyhow!("client cache get mismatch: {value:?}"));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let expired = client
        .cache_get("t1", "default", "primary", "client-key")
        .await?;
    if expired.is_some() {
        return Err(anyhow!("client cache entry should be expired"));
    }
    Ok(())
}

async fn cache_get(
    connection: &felix_transport::QuicConnection,
    key: &str,
) -> Result<Option<Bytes>> {
    let (mut send, mut recv) = connection.open_bi().await?;
    quic::write_message(
        &mut send,
        Message::CacheGet {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            cache: "primary".to_string(),
            key: key.to_string(),
            request_id: None,
        },
    )
    .await?;
    send.finish()?;
    let response = quic::read_message_limited(&mut recv, MAX_TEST_FRAME_BYTES).await?;
    match response {
        Some(Message::CacheValue { value, .. }) => Ok(value),
        other => Err(anyhow!("unexpected cache response: {other:?}")),
    }
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

fn handle_event_frame(
    expected_id: Option<u64>,
    frame: Frame,
    pending: &mut VecDeque<Vec<u8>>,
    allow_hello: bool,
) -> Result<()> {
    if frame.header.flags & FLAG_BINARY_EVENT_BATCH != 0 {
        if expected_id.is_some() && allow_hello {
            return Err(anyhow!("missing event stream hello for subscription"));
        }
        let batch =
            felix_wire::binary::decode_event_batch(&frame).context("decode binary event batch")?;
        if let Some(expected) = expected_id
            && expected != batch.subscription_id
        {
            return Err(anyhow!(
                "subscription id mismatch: expected {expected} got {}",
                batch.subscription_id
            ));
        }
        for payload in batch.payloads {
            pending.push_back(payload.to_vec());
        }
        return Ok(());
    }

    let message = Message::decode(frame).context("decode event message")?;
    match message {
        Message::EventStreamHello { subscription_id } => {
            if !allow_hello {
                return Err(anyhow!("unexpected hello after subscription start"));
            }
            if let Some(expected) = expected_id {
                if expected != subscription_id {
                    return Err(anyhow!(
                        "subscription id mismatch: expected {expected} got {subscription_id}"
                    ));
                }
            } else {
                return Err(anyhow!("unexpected hello for legacy subscription"));
            }
        }
        Message::Event { payload, .. } => {
            if expected_id.is_some() && allow_hello {
                return Err(anyhow!("missing event stream hello for subscription"));
            }
            pending.push_back(payload);
        }
        Message::EventBatch { payloads, .. } => {
            if expected_id.is_some() && allow_hello {
                return Err(anyhow!("missing event stream hello for subscription"));
            }
            for payload in payloads {
                pending.push_back(payload);
            }
        }
        other => return Err(anyhow!("unexpected message: {other:?}")),
    }
    Ok(())
}
