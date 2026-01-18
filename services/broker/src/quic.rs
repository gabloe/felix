use anyhow::{Context, Result};
use bytes::Bytes;
use felix_broker::Broker;
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::{Frame, FrameHeader, Message};
use quinn::{ReadExactError, RecvStream, SendStream};
use std::sync::Arc;
use std::time::Duration;
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
        let (send, recv) = match connection.accept_bi().await {
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
}

async fn handle_stream(
    broker: Arc<Broker>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let message = match read_message(&mut recv).await? {
        Some(message) => message,
        None => return Ok(()),
    };
    match message {
        Message::Publish { stream, payload } => {
            broker
                .publish(&stream, Bytes::from(payload))
                .await
                .context("publish")?;
            write_message(&mut send, Message::Ok).await?;
            send.finish()?;
            let _ = send.stopped().await;
        }
        Message::Subscribe { stream } => {
            let mut receiver = broker.subscribe(&stream).await.context("subscribe")?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                match receiver.recv().await {
                    Ok(payload) => {
                        let message = Message::Event {
                            stream: stream.clone(),
                            payload: payload.to_vec(),
                        };
                        if let Err(err) = write_message(&mut send, message).await {
                            tracing::info!(error = %err, "subscription stream closed");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }
        Message::CachePut { key, value, ttl_ms } => {
            let ttl = ttl_ms.map(Duration::from_millis);
            broker.cache().put(key, Bytes::from(value), ttl).await;
            write_message(&mut send, Message::Ok).await?;
            send.finish()?;
            let _ = send.stopped().await;
        }
        Message::CacheGet { key } => {
            let value = broker.cache().get(&key).await.map(|b| b.to_vec());
            write_message(&mut send, Message::CacheValue { key, value }).await?;
            send.finish()?;
            let _ = send.stopped().await;
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
        }
        Message::Error { .. } => {
            send.finish()?;
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
                key: "demo-key".to_string(),
            },
        )
        .await?;
        send.finish()?;
        let response = read_message(&mut recv).await?;
        assert_eq!(
            response,
            Some(Message::CacheValue {
                key: "demo-key".to_string(),
                value: Some(b"cached".to_vec()),
            })
        );

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
