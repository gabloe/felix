// Ack parsing and publish ack handling helpers.
use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_wire::{AckMode, Message};
use quinn::RecvStream;

#[cfg(feature = "telemetry")]
use crate::counters::frame_counters;
#[cfg(feature = "telemetry")]
use crate::timings;
use crate::wire::frame_io::read_frame_into;

pub(crate) async fn read_ack_message_with_timing(
    recv: &mut RecvStream,
    frame_scratch: &mut BytesMut,
) -> Result<Option<Message>> {
    #[cfg(feature = "telemetry")]
    let sample = crate::t_should_sample();
    #[cfg(feature = "telemetry")]
    let read_start = crate::t_now_if(sample);
    let frame = match read_frame_into(recv, frame_scratch, false).await? {
        Some(frame) => frame,
        None => return Ok(None),
    };
    #[cfg(feature = "telemetry")]
    if let Some(start) = read_start {
        let read_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_read_wait_ns(read_ns);
        t_histogram!("client_ack_read_wait_ns").record(read_ns as f64);
    }
    #[cfg(feature = "telemetry")]
    let decode_start = crate::t_now_if(sample);
    let message = Message::decode(frame).context("decode message")?;
    #[cfg(feature = "telemetry")]
    if let Some(start) = decode_start {
        let decode_ns = start.elapsed().as_nanos() as u64;
        timings::record_ack_decode_ns(decode_ns);
        t_histogram!("client_ack_decode_ns").record(decode_ns as f64);
    }
    Ok(Some(message))
}

pub(crate) async fn maybe_wait_for_ack(
    recv: &mut RecvStream,
    ack: AckMode,
    request_id: Option<u64>,
    frame_scratch: &mut BytesMut,
) -> Result<()> {
    // AckMode::None is fire-and-forget; otherwise wait for PublishOk/PublishError.
    if ack == AckMode::None {
        return Ok(());
    }
    let request_id =
        request_id.ok_or_else(|| anyhow::anyhow!("missing request_id for acked publish"))?;
    let response = read_ack_message_with_timing(recv, frame_scratch).await?;
    match response {
        Some(Message::PublishOk { request_id: ack_id }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        }
        Some(Message::PublishError {
            request_id: ack_id,
            message,
        }) if ack_id == request_id => {
            #[cfg(feature = "telemetry")]
            {
                let counters = frame_counters();
                counters
                    .ack_frames_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                counters
                    .ack_items_in_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Err(anyhow::anyhow!("publish failed: {message}"))
        }
        other => Err(anyhow::anyhow!("publish failed: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result};
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;

    async fn open_ack_stream(
        message: Option<Message>,
    ) -> Result<(
        RecvStream,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    )> {
        let bytes = message.map(|message| {
            let frame = message.encode().context("encode")?;
            Ok::<_, anyhow::Error>(frame.encode().to_vec())
        });
        open_ack_stream_bytes(bytes.transpose()?).await
    }

    async fn open_ack_stream_bytes(
        bytes: Option<Vec<u8>>,
    ) -> Result<(
        RecvStream,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    )> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
                .context("server config")?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, _recv) = connection.accept_bi().await?;
            if let Some(bytes) = bytes {
                send.write_all(&bytes).await.context("write ack")?;
            }
            send.finish().context("finish ack")?;
            let _ = shutdown_rx.await;
            Result::<()>::Ok(())
        });

        let mut roots = RootCertStore::empty();
        roots.add(cert_der)?;
        let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
        let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;
        let connection = client.connect(addr, "localhost").await?;
        let (mut send, recv) = connection.open_bi().await?;
        send.finish().context("finish client send")?;
        Ok((recv, shutdown_tx, server_task))
    }

    #[tokio::test]
    async fn maybe_wait_for_ack_none_returns_ok() -> Result<()> {
        let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        maybe_wait_for_ack(&mut recv, AckMode::None, None, &mut scratch).await?;
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn maybe_wait_for_ack_missing_request_id() -> Result<()> {
        let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        assert!(
            maybe_wait_for_ack(&mut recv, AckMode::PerMessage, None, &mut scratch)
                .await
                .is_err()
        );
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn maybe_wait_for_ack_ok() -> Result<()> {
        crate::timings::enable_collection(1);
        let request_id = 42;
        let (mut recv, shutdown_tx, server_task) =
            open_ack_stream(Some(Message::PublishOk { request_id })).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        maybe_wait_for_ack(
            &mut recv,
            AckMode::PerMessage,
            Some(request_id),
            &mut scratch,
        )
        .await?;
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn maybe_wait_for_ack_error() -> Result<()> {
        crate::timings::enable_collection(1);
        let request_id = 7;
        let (mut recv, shutdown_tx, server_task) = open_ack_stream(Some(Message::PublishError {
            request_id,
            message: "nope".into(),
        }))
        .await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        assert!(
            maybe_wait_for_ack(
                &mut recv,
                AckMode::PerMessage,
                Some(request_id),
                &mut scratch,
            )
            .await
            .is_err()
        );
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn maybe_wait_for_ack_unexpected_message() -> Result<()> {
        crate::timings::enable_collection(1);
        let request_id = 9;
        let (mut recv, shutdown_tx, server_task) =
            open_ack_stream(Some(Message::PublishOk { request_id: 8 })).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        assert!(
            maybe_wait_for_ack(
                &mut recv,
                AckMode::PerMessage,
                Some(request_id),
                &mut scratch,
            )
            .await
            .is_err()
        );
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn read_ack_message_with_timing_none() -> Result<()> {
        crate::timings::enable_collection(1);
        let (mut recv, shutdown_tx, server_task) = open_ack_stream(None).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        let response = read_ack_message_with_timing(&mut recv, &mut scratch).await?;
        assert!(response.is_none());
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn read_ack_message_with_timing_decode_error() -> Result<()> {
        crate::timings::enable_collection(1);
        let payload = b"not-json";
        let mut header_bytes = [0u8; felix_wire::FrameHeader::LEN];
        let header = felix_wire::FrameHeader::new(0, payload.len() as u32);
        header.encode_into(&mut header_bytes);
        let mut bytes = Vec::with_capacity(header_bytes.len() + payload.len());
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(payload);
        let (mut recv, shutdown_tx, server_task) = open_ack_stream_bytes(Some(bytes)).await?;
        let mut scratch = BytesMut::with_capacity(64 * 1024);
        assert!(
            read_ack_message_with_timing(&mut recv, &mut scratch)
                .await
                .is_err()
        );
        let _ = shutdown_tx.send(());
        server_task.await.context("server task join")??;
        Ok(())
    }
}
