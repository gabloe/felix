use anyhow::{Context, Result};
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::time::{Duration, timeout};
use tracing::debug;

use crate::Client;
use crate::frame_io::{read_message, write_message};
use crate::test_support::{
    build_client_config_with_overrides, build_server_config, set_client_env_with_event_pool,
};

#[tokio::test]
#[serial_test::serial]
async fn cache_worker_exits_on_stream_error() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();

    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<bool> {
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                debug!("test server failed to accept bi stream");
                return Ok(false);
            };
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let auth_msg = read_message(&mut recv, &mut frame_scratch).await;
            debug!(?auth_msg, "test server read auth message");
            let ok_result = write_message(&mut send, Message::Ok).await;
            debug!(?ok_result, "test server sent auth ok");
            let request = timeout(
                Duration::from_millis(200),
                read_message(&mut recv, &mut frame_scratch),
            )
            .await;
            let Ok(Ok(Some(message))) = request else {
                debug!("test server did not receive cache request");
                let _ = send.finish();
                return Ok(false);
            };
            match message {
                Message::CacheGet { .. } | Message::CachePut { .. } => {
                    let write_result = write_message(
                        &mut send,
                        Message::Error {
                            message: "cache failure".to_string(),
                        },
                    )
                    .await;
                    debug!(?write_result, "test server sent cache error");
                    let _ = send.finish();
                    Ok(true)
                }
                _ => {
                    debug!(?message, "test server received unexpected message");
                    let _ = send.finish();
                    Ok(false)
                }
            }
        }

        let mut tasks: Vec<tokio::task::JoinHandle<Result<bool>>> = Vec::new();
        let accept_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let now = tokio::time::Instant::now();
            if now >= accept_deadline {
                break;
            }
            let remaining = accept_deadline.saturating_duration_since(now);
            let result = timeout(remaining, server.accept()).await;
            let Ok(Ok(connection)) = result else {
                break;
            };
            debug!("test server accepted connection");
            tasks.push(tokio::spawn(handle_connection(connection)));
        }

        let mut closed = false;
        for task in tasks {
            if task.await?? {
                closed = true;
            }
        }

        if !closed {
            return Err(anyhow::anyhow!("server did not accept cache stream"));
        }

        Result::<()>::Ok(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache should fail");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("cache response closed")
            || err_msg.contains("cache worker closed")
            || err_msg.contains("connection lost")
            || err_msg.contains("cache error"),
        "unexpected cache error: {err_msg}"
    );

    let err = client
        .cache_get("t1", "default", "cache", "key")
        .await
        .expect_err("cache worker should be closed");
    assert!(err.to_string().contains("cache worker closed"));

    server_task.await.context("server task join")??;
    Ok(())
}
