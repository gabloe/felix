//! Felix client integration and unit tests.
//!
//! # Purpose
//! Exercise client-side publish/subscribe/cache flows against an in-process broker or
//! lightweight QUIC server, covering success and error paths for:
//! - publish acks and error mapping
//! - subscription lifecycle and decode failures
//! - cache request/response handling
//!
//! These tests also validate config parsing and env overrides.
use super::*;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use felix_broker::Broker;
use felix_storage::EphemeralCache;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::{AckMode, FrameHeader, Message};
use quinn::ClientConfig as QuinnClientConfig;
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use tracing::debug;

use crate::wire::{read_frame_into, read_message, write_message};

struct EnvGuard;

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("FELIX_PUB_CONN_POOL");
            std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
            std::env::remove_var("FELIX_CACHE_CONN_POOL");
            std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
            std::env::remove_var("FELIX_EVENT_CONN_POOL");
            std::env::remove_var("FELIX_AUTH_TENANT");
            std::env::remove_var("FELIX_AUTH_TOKEN");
            std::env::remove_var("FELIX_CLIENT_CONFIG");
        }
    }
}

fn set_client_env_with_event_pool(event_pool: usize) -> EnvGuard {
    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "1");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "1");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "1");
        std::env::set_var("FELIX_EVENT_CONN_POOL", event_pool.to_string());
        std::env::set_var("FELIX_AUTH_TENANT", "t1");
        std::env::set_var("FELIX_AUTH_TOKEN", "demo-token");
        std::env::remove_var("FELIX_CLIENT_CONFIG");
    }
    EnvGuard
}

fn set_client_env() -> EnvGuard {
    set_client_env_with_event_pool(1)
}

#[tokio::test]
async fn in_process_publish_and_subscribe() {
    // Smoke-test the in-process path without any network transport.
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
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
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
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

#[tokio::test]
#[serial_test::serial]
async fn quic_publish_subscribe_cache_success() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            loop {
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    break;
                };
                let auth = read_message(&mut recv, &mut frame_scratch).await?;
                match auth {
                    Some(Message::Auth { .. }) => {
                        write_message(&mut send, Message::Ok).await?;
                    }
                    _ => {
                        write_message(
                            &mut send,
                            Message::Error {
                                message: "missing auth".to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                }
                loop {
                    let next = read_message(&mut recv, &mut frame_scratch).await?;
                    match next {
                        Some(Message::Publish {
                            request_id: Some(id),
                            ..
                        }) => {
                            write_message(&mut send, Message::PublishOk { request_id: id }).await?;
                        }
                        Some(Message::Subscribe {
                            subscription_id, ..
                        }) => {
                            let sub_id = subscription_id.unwrap_or(0);
                            write_message(
                                &mut send,
                                Message::Subscribed {
                                    subscription_id: sub_id,
                                },
                            )
                            .await?;
                            let mut uni = connection.open_uni().await?;
                            write_message(
                                &mut uni,
                                Message::EventStreamHello {
                                    subscription_id: sub_id,
                                },
                            )
                            .await?;
                            write_message(
                                &mut uni,
                                Message::EventBatch {
                                    tenant_id: "t1".to_string(),
                                    namespace: "default".to_string(),
                                    stream: "updates".to_string(),
                                    payloads: vec![b"a".to_vec(), b"b".to_vec()],
                                },
                            )
                            .await?;
                            let _ = uni.finish();
                        }
                        Some(Message::CachePut { request_id, .. }) => {
                            let id = request_id.unwrap_or(1);
                            write_message(&mut send, Message::CacheOk { request_id: id }).await?;
                        }
                        Some(Message::CacheGet { request_id, .. }) => {
                            write_message(
                                &mut send,
                                Message::CacheValue {
                                    tenant_id: "t1".to_string(),
                                    namespace: "default".to_string(),
                                    cache: "cache".to_string(),
                                    key: "key".to_string(),
                                    value: Some(Bytes::from_static(b"value")),
                                    request_id,
                                },
                            )
                            .await?;
                        }
                        None => break,
                        _ => {}
                    }
                }
                let _ = send.finish();
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            task.await??;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;

    let publisher = client.publisher().await?;
    publisher
        .publish(
            "t1",
            "default",
            "updates",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await?;

    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let first = subscription.next_event().await?.expect("event");
    let second = subscription.next_event().await?.expect("event");
    assert_eq!(first.payload, Bytes::from_static(b"a"));
    assert_eq!(second.payload, Bytes::from_static(b"b"));

    client
        .cache_put(
            "t1",
            "default",
            "cache",
            "key",
            Bytes::from_static(b"value"),
            None,
        )
        .await?;
    let value = client
        .cache_get("t1", "default", "cache", "key")
        .await?
        .expect("value");
    assert_eq!(value, Bytes::from_static(b"value"));

    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscribe_rejects_mismatched_subscription_id() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            loop {
                let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                    break;
                };
                let _ = read_message(&mut recv, &mut frame_scratch).await?;
                write_message(&mut send, Message::Ok).await?;
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                if let Some(Message::Subscribe {
                    subscription_id, ..
                }) = next
                {
                    let wrong = subscription_id.unwrap_or(1) + 1;
                    write_message(
                        &mut send,
                        Message::Subscribed {
                            subscription_id: wrong,
                        },
                    )
                    .await?;
                }
                let _ = recv.read_to_end(usize::MAX).await;
                let _ = send.finish();
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            task.await??;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;

    let err = match client.subscribe("t1", "default", "updates").await {
        Ok(_) => anyhow::bail!("expected subscription mismatch error"),
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("subscription id mismatch") || message.contains("subscribe failed"),
        "unexpected subscribe error: {message}"
    );

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_stream_close_returns_none() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let result = timeout(Duration::from_secs(1), subscription.next_event()).await??;
    assert!(result.is_none());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_mismatched_id_errors() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(7);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        // Encode a binary event batch with the wrong subscription id to force an error.
                        let payloads = vec![Bytes::from_static(b"a")];
                        let encoded =
                            felix_wire::binary::encode_event_batch_bytes(sub_id + 1, &payloads)?;
                        uni.write_all(&encoded).await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected subscription id mismatch"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("subscription id mismatch"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_decode_error_on_invalid_frame() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        // Send an invalid binary message frame to trigger decode error handling.
                        let header = FrameHeader::new(0, 2);
                        let mut header_bytes = [0u8; FrameHeader::LEN];
                        header.encode_into(&mut header_bytes);
                        uni.write_all(&header_bytes).await?;
                        uni.write_all(&[0, 5]).await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected decode error"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_legacy_event_paths_and_unexpected_message_error() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;

                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        write_message(
                            &mut uni,
                            Message::Event {
                                tenant_id: "t1".to_string(),
                                namespace: "default".to_string(),
                                stream: "updates".to_string(),
                                payload: b"legacy-one".to_vec(),
                            },
                        )
                        .await?;
                        write_message(
                            &mut uni,
                            Message::EventBatch {
                                tenant_id: "t1".to_string(),
                                namespace: "default".to_string(),
                                stream: "updates".to_string(),
                                payloads: vec![b"legacy-two".to_vec(), b"legacy-three".to_vec()],
                            },
                        )
                        .await?;
                        // Force the "unexpected message" branch in Subscription::next_event.
                        write_message(&mut uni, Message::Ok).await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;

    let first = subscription.next_event().await?.expect("first event");
    assert_eq!(first.payload, Bytes::from_static(b"legacy-one"));

    let second = subscription.next_event().await?.expect("second event");
    assert_eq!(second.payload, Bytes::from_static(b"legacy-two"));

    // This one comes from the cached batch path.
    let third = subscription.next_event().await?.expect("third event");
    assert_eq!(third.payload, Bytes::from_static(b"legacy-three"));

    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected unexpected-message error"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("unexpected message on subscription stream")
    );

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_empty_event_batch_returns_none() -> Result<()> {
    let _env_guard = set_client_env();

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;

                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        write_message(
                            &mut uni,
                            Message::EventBatch {
                                tenant_id: "t1".to_string(),
                                namespace: "default".to_string(),
                                stream: "updates".to_string(),
                                payloads: Vec::new(),
                            },
                        )
                        .await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let value = timeout(Duration::from_secs(1), subscription.next_event()).await??;
    assert!(value.is_none());

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_decode_error_on_invalid_payload() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(1);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        // Set the binary event-batch flag with an invalid payload to force
                        // decode_event_batch failure in Subscription::next_event.
                        let header = FrameHeader::new(felix_wire::FLAG_BINARY_EVENT_BATCH, 2);
                        let mut header_bytes = [0u8; FrameHeader::LEN];
                        header.encode_into(&mut header_bytes);
                        uni.write_all(&header_bytes).await?;
                        uni.write_all(&[0, 0]).await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let err = match subscription.next_event().await {
        Ok(_) => anyhow::bail!("expected binary batch decode error"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("decode binary event batch"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn subscription_binary_batch_success_records_decode_timing() -> Result<()> {
    let _env_guard = set_client_env();
    crate::timings::enable_collection(1);
    crate::timings::set_enabled(true);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                return Ok(());
            };
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            loop {
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                match next {
                    Some(Message::Subscribe {
                        subscription_id, ..
                    }) => {
                        let sub_id = subscription_id.unwrap_or(42);
                        write_message(
                            &mut send,
                            Message::Subscribed {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let mut uni = connection.open_uni().await?;
                        write_message(
                            &mut uni,
                            Message::EventStreamHello {
                                subscription_id: sub_id,
                            },
                        )
                        .await?;
                        let payloads = vec![
                            Bytes::from_static(b"binary-a"),
                            Bytes::from_static(b"binary-b"),
                        ];
                        let encoded =
                            felix_wire::binary::encode_event_batch_bytes(sub_id, &payloads)?;
                        uni.write_all(&encoded).await?;
                        let _ = uni.finish();
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 1)?,
        TransportConfig::default(),
    )
    .await?;
    let mut subscription = client.subscribe("t1", "default", "updates").await?;
    let first = subscription.next_event().await?.expect("first");
    assert_eq!(first.payload, Bytes::from_static(b"binary-a"));
    let second = subscription.next_event().await?.expect("second");
    assert_eq!(second.payload, Bytes::from_static(b"binary-b"));

    let _ = shutdown_tx.send(());
    server_task.abort();
    crate::timings::set_enabled(false);
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn publish_reports_server_error() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let server_task = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let connection = server.accept().await?;
            tasks.push(tokio::spawn(async move {
                let (mut send, mut recv) = connection.accept_bi().await?;
                let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
                let _ = read_message(&mut recv, &mut frame_scratch).await?;
                write_message(&mut send, Message::Ok).await?;
                let next = read_message(&mut recv, &mut frame_scratch).await?;
                if let Some(Message::Publish { request_id, .. }) = next {
                    let id = request_id.unwrap_or(1);
                    write_message(
                        &mut send,
                        Message::PublishError {
                            request_id: id,
                            message: "denied".to_string(),
                        },
                    )
                    .await?;
                }
                Ok::<(), anyhow::Error>(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    let publisher = client.publisher().await?;
    let err = publisher
        .publish(
            "t1",
            "default",
            "updates",
            b"payload".to_vec(),
            AckMode::PerMessage,
        )
        .await
        .expect_err("publish error");
    assert!(!err.to_string().is_empty());

    server_task.abort();
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn publish_batch_ack_succeeds() -> Result<()> {
    let _env_guard = set_client_env_with_event_pool(0);

    let (server_config, cert) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        async fn handle_connection(connection: felix_transport::QuicConnection) -> Result<()> {
            let (mut send, mut recv) = connection.accept_bi().await?;
            let mut frame_scratch = BytesMut::with_capacity(64 * 1024);
            let _ = read_message(&mut recv, &mut frame_scratch).await?;
            write_message(&mut send, Message::Ok).await?;
            let next = read_frame_into(&mut recv, &mut frame_scratch, false).await?;
            if next.is_some() {
                write_message(&mut send, Message::PublishOk { request_id: 1 }).await?;
                let _ = send.finish();
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        }

        let mut tasks = Vec::new();
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
            tasks.push(tokio::spawn(handle_connection(connection)));
        }
        for task in tasks {
            drop(task);
        }
        let _ = shutdown_rx.await;
        Ok::<(), anyhow::Error>(())
    });

    let client = Client::connect_with_transport(
        addr,
        "localhost",
        build_client_config_with_overrides(cert, 0)?,
        TransportConfig::default(),
    )
    .await?;

    let publisher = client.publisher().await?;
    publisher
        .publish_batch(
            "t1",
            "default",
            "updates",
            vec![b"a".to_vec(), b"b".to_vec()],
            AckMode::PerBatch,
        )
        .await?;

    let _ = shutdown_tx.send(());
    server_task.abort();
    Ok(())
}

fn build_server_config() -> Result<(quinn::ServerConfig, CertificateDer<'static>)> {
    let rcgen::CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
    let server_config =
        quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
    Ok((server_config, cert_der))
}

fn build_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(cert)?;
    let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut config = ClientConfig::from_env_or_yaml(quinn, None)?;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("test-token".to_string());
    config.publish_conn_pool = 1;
    config.publish_streams_per_conn = 1;
    config.cache_conn_pool = 1;
    config.cache_streams_per_conn = 1;
    config.event_conn_pool = 1;
    Ok(config)
}

fn build_client_config_with_overrides(
    cert: CertificateDer<'static>,
    event_pool: usize,
) -> Result<ClientConfig> {
    let mut config = build_client_config(cert)?;
    config.event_conn_pool = event_pool;
    config.auth_tenant_id = Some("t1".to_string());
    config.auth_token = Some("demo-token".to_string());
    Ok(config)
}

// ===== Config tests =====

#[allow(deprecated)]
#[test]
fn config_optimized_defaults() {
    use crate::config::*;
    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::optimized_defaults(quinn);

    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(
        config.publish_streams_per_conn,
        DEFAULT_PUB_STREAMS_PER_CONN
    );
    assert_eq!(config.publish_chunk_bytes, DEFAULT_PUBLISH_CHUNK_BYTES);
    assert_eq!(config.cache_conn_pool, DEFAULT_CACHE_CONN_POOL);
    assert_eq!(
        config.cache_streams_per_conn,
        DEFAULT_CACHE_STREAMS_PER_CONN
    );
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
    assert_eq!(
        config.event_conn_recv_window,
        DEFAULT_EVENT_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.event_stream_recv_window,
        DEFAULT_EVENT_STREAM_RECV_WINDOW
    );
    assert_eq!(config.event_send_window, DEFAULT_EVENT_SEND_WINDOW);
    assert_eq!(
        config.cache_conn_recv_window,
        DEFAULT_CACHE_CONN_RECV_WINDOW
    );
    assert_eq!(
        config.cache_stream_recv_window,
        DEFAULT_CACHE_STREAM_RECV_WINDOW
    );
    assert_eq!(config.cache_send_window, DEFAULT_CACHE_SEND_WINDOW);
    assert_eq!(
        config.event_router_max_pending,
        DEFAULT_EVENT_ROUTER_MAX_PENDING
    );
    assert_eq!(
        config.client_sub_queue_capacity,
        DEFAULT_CLIENT_SUB_QUEUE_CAPACITY
    );
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
    assert!(!config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_from_env_variables() {
    use crate::config::*;

    unsafe {
        std::env::set_var("FELIX_PUB_CONN_POOL", "2");
        std::env::set_var("FELIX_PUB_STREAMS_PER_CONN", "3");
        std::env::set_var("FELIX_PUBLISH_CHUNK_BYTES", "32768");
        std::env::set_var("FELIX_PUBLISH_SHARDING", "rr");
        std::env::set_var("FELIX_CACHE_CONN_POOL", "4");
        std::env::set_var("FELIX_CACHE_STREAMS_PER_CONN", "5");
        std::env::set_var("FELIX_EVENT_CONN_POOL", "6");
        std::env::set_var("FELIX_EVENT_CONN_RECV_WINDOW", "1024");
        std::env::set_var("FELIX_EVENT_STREAM_RECV_WINDOW", "2048");
        std::env::set_var("FELIX_EVENT_SEND_WINDOW", "4096");
        std::env::set_var("FELIX_CACHE_CONN_RECV_WINDOW", "8192");
        std::env::set_var("FELIX_CACHE_STREAM_RECV_WINDOW", "16384");
        std::env::set_var("FELIX_CACHE_SEND_WINDOW", "32768");
        std::env::set_var("FELIX_EVENT_ROUTER_MAX_PENDING", "1000");
        std::env::set_var("FELIX_CLIENT_SUB_QUEUE_CAPACITY", "2048");
        std::env::set_var("FELIX_CLIENT_SUB_QUEUE_POLICY", "block");
        std::env::set_var("FELIX_MAX_FRAME_BYTES", "8388608");
        std::env::set_var("FELIX_BENCH_EMBED_TS", "true");
    }

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");

    assert_eq!(config.publish_conn_pool, 2);
    assert_eq!(config.publish_streams_per_conn, 3);
    assert_eq!(config.publish_chunk_bytes, 32768);
    assert_eq!(config.cache_conn_pool, 4);
    assert_eq!(config.cache_streams_per_conn, 5);
    assert_eq!(config.event_conn_pool, 6);
    assert_eq!(config.event_conn_recv_window, 1024);
    assert_eq!(config.event_stream_recv_window, 2048);
    assert_eq!(config.event_send_window, 4096);
    assert_eq!(config.cache_conn_recv_window, 8192);
    assert_eq!(config.cache_stream_recv_window, 16384);
    assert_eq!(config.cache_send_window, 32768);
    assert_eq!(config.event_router_max_pending, 1000);
    assert_eq!(config.client_sub_queue_capacity, 2048);
    assert_eq!(config.client_sub_queue_policy, ClientSubQueuePolicy::Block);
    assert_eq!(config.max_frame_bytes, 8388608);
    assert!(config.bench_embed_ts);

    // Clean up
    unsafe {
        std::env::remove_var("FELIX_PUB_CONN_POOL");
        std::env::remove_var("FELIX_PUB_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_PUBLISH_CHUNK_BYTES");
        std::env::remove_var("FELIX_PUBLISH_SHARDING");
        std::env::remove_var("FELIX_CACHE_CONN_POOL");
        std::env::remove_var("FELIX_CACHE_STREAMS_PER_CONN");
        std::env::remove_var("FELIX_EVENT_CONN_POOL");
        std::env::remove_var("FELIX_EVENT_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_EVENT_SEND_WINDOW");
        std::env::remove_var("FELIX_CACHE_CONN_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_STREAM_RECV_WINDOW");
        std::env::remove_var("FELIX_CACHE_SEND_WINDOW");
        std::env::remove_var("FELIX_EVENT_ROUTER_MAX_PENDING");
        std::env::remove_var("FELIX_CLIENT_SUB_QUEUE_CAPACITY");
        std::env::remove_var("FELIX_CLIENT_SUB_QUEUE_POLICY");
        std::env::remove_var("FELIX_MAX_FRAME_BYTES");
        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
fn config_from_yaml_file() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let yaml = r#"
publish_conn_pool: 10
publish_streams_per_conn: 20
publish_chunk_bytes: 65536
publish_sharding: "hash_stream"
cache_conn_pool: 30
cache_streams_per_conn: 40
event_conn_pool: 50
event_conn_recv_window: 10240
event_stream_recv_window: 20480
event_send_window: 40960
cache_conn_recv_window: 81920
cache_stream_recv_window: 163840
cache_send_window: 327680
event_router_max_pending: 2000
client_sub_queue_capacity: 3072
client_sub_queue_policy: drop_old
max_frame_bytes: 16777216
bench_embed_ts: true
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    assert_eq!(config.publish_conn_pool, 10);
    assert_eq!(config.publish_streams_per_conn, 20);
    assert_eq!(config.publish_chunk_bytes, 65536);
    assert_eq!(config.cache_conn_pool, 30);
    assert_eq!(config.cache_streams_per_conn, 40);
    assert_eq!(config.event_conn_pool, 50);
    assert_eq!(config.event_conn_recv_window, 10240);
    assert_eq!(config.event_stream_recv_window, 20480);
    assert_eq!(config.event_send_window, 40960);
    assert_eq!(config.cache_conn_recv_window, 81920);
    assert_eq!(config.cache_stream_recv_window, 163840);
    assert_eq!(config.cache_send_window, 327680);
    assert_eq!(config.event_router_max_pending, 2000);
    assert_eq!(config.client_sub_queue_capacity, 3072);
    assert_eq!(
        config.client_sub_queue_policy,
        ClientSubQueuePolicy::DropOld
    );
    assert_eq!(config.max_frame_bytes, 16777216);
    assert!(config.bench_embed_ts);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_yaml_overrides_ignore_zero_values() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    unsafe {
        std::env::remove_var("FELIX_PUB_CONN_POOL");
        std::env::remove_var("FELIX_CACHE_CONN_POOL");
        std::env::remove_var("FELIX_EVENT_CONN_POOL");
    }

    let yaml = r#"
publish_conn_pool: 0
cache_conn_pool: 5
event_conn_pool: 0
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let config = ClientConfig::from_env_or_yaml(quinn, Some(path)).expect("config");

    // Zero values should be ignored, defaults used
    assert_eq!(config.publish_conn_pool, DEFAULT_PUB_CONN_POOL);
    assert_eq!(config.cache_conn_pool, 5);
    assert_eq!(config.event_conn_pool, DEFAULT_EVENT_CONN_POOL);
}

#[allow(deprecated)]
#[test]
fn config_invalid_yaml_file_returns_error() {
    use crate::config::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    let invalid_yaml = r#"
publish_conn_pool: [invalid
"#;

    let mut temp_file = NamedTempFile::new().expect("temp file");
    temp_file.write_all(invalid_yaml.as_bytes()).expect("write");
    let path = temp_file.path().to_str().expect("path");

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some(path));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_nonexistent_file_returns_error() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let result = ClientConfig::from_env_or_yaml(quinn, Some("/nonexistent/path/config.yaml"));

    assert!(result.is_err());
}

#[allow(deprecated)]
#[test]
fn config_transport_configs() {
    use crate::config::*;

    let quinn = quinn::ClientConfig::with_platform_verifier();
    let mut config = ClientConfig::optimized_defaults(quinn);
    config.event_conn_recv_window = 12345;
    config.event_stream_recv_window = 23456;
    config.event_send_window = 34567;
    config.cache_conn_recv_window = 45678;
    config.cache_stream_recv_window = 56789;
    config.cache_send_window = 67890;

    let base_transport = TransportConfig::default();

    let event_transport = event_transport_config(base_transport.clone(), &config);
    assert_eq!(event_transport.receive_window, 12345);
    assert_eq!(event_transport.stream_receive_window, 23456);
    assert_eq!(event_transport.send_window, 34567);

    let cache_transport = cache_transport_config(base_transport, &config);
    assert_eq!(cache_transport.receive_window, 45678);
    assert_eq!(cache_transport.stream_receive_window, 56789);
    assert_eq!(cache_transport.send_window, 67890);
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_bool_parsing() {
    unsafe {
        // Test various true values
        for val in &["1", "true", "TRUE", "yes", "YES"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(config.bench_embed_ts, "Failed for value: {}", val);
        }

        // Test false values
        for val in &["0", "false", "FALSE", "no", "NO", "random"] {
            std::env::set_var("FELIX_BENCH_EMBED_TS", val);
            let quinn = quinn::ClientConfig::with_platform_verifier();
            let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
            assert!(!config.bench_embed_ts, "Failed for value: {}", val);
        }

        std::env::remove_var("FELIX_BENCH_EMBED_TS");
    }
}

#[allow(deprecated)]
#[test]
#[serial_test::serial]
fn config_env_sharding_variants() {
    use crate::client::sharding::PublishSharding;

    unsafe {
        // Test round robin
        std::env::set_var("FELIX_PUB_SHARDING", "rr");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::RoundRobin
        ));

        // Test hash_stream
        std::env::set_var("FELIX_PUB_SHARDING", "hash_stream");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        // Test invalid value (should default to HashStream)
        std::env::set_var("FELIX_PUB_SHARDING", "invalid");
        let quinn = quinn::ClientConfig::with_platform_verifier();
        let config = ClientConfig::from_env_or_yaml(quinn, None).expect("config");
        assert!(matches!(
            config.publish_sharding,
            PublishSharding::HashStream
        ));

        std::env::remove_var("FELIX_PUB_SHARDING");
    }
}
