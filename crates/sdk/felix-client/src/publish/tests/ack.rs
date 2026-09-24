use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::{AckMode, Message};
use quinn::RecvStream;

use crate::publish::ack::{maybe_wait_for_ack, read_ack_message_with_timing};
use crate::test_support::{build_server_config, quinn_client_config};

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
    let (server_config, cert_der) = build_server_config()?;
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

    let quinn = quinn_client_config(cert_der)?;
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
    let (mut recv, shutdown_tx, server_task) =
        open_ack_stream(Some(Message::publish_error(request_id, "nope"))).await?;
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
    let response = read_ack_message_with_timing(
        &mut recv,
        &mut scratch,
        crate::config::DEFAULT_MAX_FRAME_BYTES,
    )
    .await?;
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
        read_ack_message_with_timing(
            &mut recv,
            &mut scratch,
            crate::config::DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .is_err()
    );
    let _ = shutdown_tx.send(());
    server_task.await.context("server task join")??;
    Ok(())
}
