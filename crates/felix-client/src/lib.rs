// High-level client for talking to a Felix broker.
// Provides both an in-process wrapper and a QUIC-based network client.
use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
use felix_transport::{QuicClient, QuicConnection, TransportConfig};
use felix_wire::{AckMode, Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Client wrapper that talks to an in-process broker.
///
/// ```
/// use bytes::Bytes;
/// use felix_broker::Broker;
/// use felix_client::InProcessClient;
/// use felix_storage::EphemeralCache;
/// use std::sync::Arc;
///
/// let broker = Arc::new(Broker::new(EphemeralCache::new()));
/// let client = InProcessClient::new(broker.clone());
/// let rt = tokio::runtime::Runtime::new().expect("rt");
/// rt.block_on(async {
///     broker
///         .register_tenant("t1")
///         .await
///         .expect("tenant");
///     broker
///         .register_namespace("t1", "default")
///         .await
///         .expect("namespace");
///     broker
///         .register_stream("t1", "default", "updates", Default::default())
///         .await
///         .expect("register");
///     let mut sub = client.subscribe("t1", "default", "updates").await.expect("subscribe");
///     client
///         .publish("t1", "default", "updates", Bytes::from_static(b"payload"))
///         .await
///         .expect("publish");
///     let msg = sub.recv().await.expect("recv");
///     assert_eq!(msg, Bytes::from_static(b"payload"));
/// });
/// ```
#[derive(Clone)]
pub struct InProcessClient {
    // Keep an Arc so callers can clone the client cheaply.
    broker: Arc<Broker>,
}

impl InProcessClient {
    // Construct a client that shares the broker running in this process.
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    // Forward publish calls directly to the broker.
    pub async fn publish(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Bytes,
    ) -> Result<usize> {
        self.broker
            .publish(tenant_id, namespace, stream, payload)
            .await
            .map_err(Into::into)
    }

    // Subscribe to a topic and return a broadcast receiver.
    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<broadcast::Receiver<Bytes>> {
        self.broker
            .subscribe(tenant_id, namespace, stream)
            .await
            .map_err(Into::into)
    }
}

/// Network client that speaks felix-wire over QUIC.
pub struct Client {
    _client: QuicClient,
    connection: QuicConnection,
}

impl Client {
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
    ) -> Result<Self> {
        Self::connect_with_transport(addr, server_name, client_config, TransportConfig::default())
            .await
    }

    pub async fn connect_with_transport(
        addr: SocketAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        let bind_addr: SocketAddr = "0.0.0.0:0".parse().expect("bind addr");
        let client = QuicClient::bind(bind_addr, client_config, transport)?;
        let connection = client.connect(addr, server_name).await?;
        Ok(Self {
            _client: client,
            connection,
        })
    }

    pub async fn publisher(&self) -> Result<Publisher> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok(Publisher {
            send,
            recv: Some(recv),
        })
    }

    pub async fn subscribe(
        &self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
    ) -> Result<Subscription> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        write_message(
            &mut send,
            Message::Subscribe {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        if response != Some(Message::Ok) {
            return Err(anyhow::anyhow!("subscribe failed: {response:?}"));
        }
        Ok(Subscription { recv })
    }

    pub async fn cache_put(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CachePut {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key: key.to_string(),
                value,
                ttl_ms,
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        if response != Some(Message::Ok) {
            return Err(anyhow::anyhow!("cache put failed: {response:?}"));
        }
        Ok(())
    }

    pub async fn cache_get(
        &self,
        tenant_id: &str,
        namespace: &str,
        cache: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        write_message(
            &mut send,
            Message::CacheGet {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                cache: cache.to_string(),
                key: key.to_string(),
            },
        )
        .await?;
        send.finish()?;
        match read_message(&mut recv).await? {
            Some(Message::CacheValue { value, .. }) => Ok(value),
            other => Err(anyhow::anyhow!("cache get failed: {other:?}")),
        }
    }
}

pub struct Publisher {
    send: SendStream,
    recv: Option<RecvStream>,
}

impl Publisher {
    pub async fn publish(
        &mut self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payload: Vec<u8>,
        ack: AckMode,
    ) -> Result<()> {
        write_message(
            &mut self.send,
            Message::Publish {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                payload,
                ack: Some(ack),
            },
        )
        .await?;
        self.maybe_wait_for_ack(ack).await
    }

    pub async fn publish_batch(
        &mut self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        ack: AckMode,
    ) -> Result<()> {
        write_message(
            &mut self.send,
            Message::PublishBatch {
                tenant_id: tenant_id.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                payloads,
                ack: Some(ack),
            },
        )
        .await?;
        self.maybe_wait_for_ack(ack).await
    }

    pub async fn publish_batch_binary(
        &mut self,
        tenant_id: &str,
        namespace: &str,
        stream: &str,
        payloads: &[Vec<u8>],
    ) -> Result<()> {
        let frame =
            felix_wire::binary::encode_publish_batch(tenant_id, namespace, stream, payloads)?;
        self.send
            .write_all(&frame.encode())
            .await
            .context("write binary batch frame")
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.send.finish()?;
        if let Some(recv) = self.recv.as_mut() {
            let _ = recv.read_to_end(usize::MAX).await;
        }
        Ok(())
    }

    async fn maybe_wait_for_ack(&mut self, ack: AckMode) -> Result<()> {
        if ack == AckMode::None {
            return Ok(());
        }
        let recv = self
            .recv
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("ack requested without recv stream"))?;
        let response = read_message(recv).await?;
        if response != Some(Message::Ok) {
            return Err(anyhow::anyhow!("publish failed: {response:?}"));
        }
        Ok(())
    }
}

pub struct Subscription {
    recv: RecvStream,
}

pub struct Event {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub payload: Vec<u8>,
}

impl Subscription {
    pub async fn next_event(&mut self) -> Result<Option<Event>> {
        match read_message(&mut self.recv).await? {
            Some(Message::Event {
                tenant_id,
                namespace,
                stream,
                payload,
            }) => Ok(Some(Event {
                tenant_id,
                namespace,
                stream,
                payload,
            })),
            Some(_) => Err(anyhow::anyhow!("unexpected message on subscription stream")),
            None => Ok(None),
        }
    }
}

async fn read_message(recv: &mut RecvStream) -> Result<Option<Message>> {
    let frame = match read_frame(recv).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    Message::decode(frame).map(Some).context("decode message")
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

async fn write_message(send: &mut SendStream, message: Message) -> Result<()> {
    let frame = message.encode().context("encode message")?;
    let encoded = frame.encode();
    send.write_all(&encoded).await.context("write frame")
}

#[cfg(test)]
mod tests {
    use super::*;
    use felix_storage::EphemeralCache;

    #[tokio::test]
    async fn in_process_publish_and_subscribe() {
        // Smoke-test the in-process path without any network transport.
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
        let broker = Arc::new(Broker::new(EphemeralCache::new()));
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
}
