//! Event stream router for subscription uni streams.
//!
//! # Purpose
//! Accepts incoming uni streams from the server, decodes the subscription id
//! from the EventStreamHello frame, and hands the stream to the waiting
//! subscription task.
//!
//! # Design notes
//! The router maintains pending maps for streams and registrations to handle
//! out-of-order arrivals while enforcing an upper bound on queued state.
use bytes::BytesMut;
use felix_transport::QuicConnection;
use felix_wire::Message;
use quinn::RecvStream;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::config::{EVENT_ROUTER_QUEUE_DEPTH, runtime_config};
use crate::wire::read_message;

pub(crate) enum EventRouterCommand {
    Register {
        subscription_id: u64,
        response: oneshot::Sender<anyhow::Result<RecvStream>>,
    },
}

pub(crate) fn spawn_event_router(connection: QuicConnection) -> mpsc::Sender<EventRouterCommand> {
    let (tx, rx) = mpsc::channel(EVENT_ROUTER_QUEUE_DEPTH);
    tokio::spawn(run_event_router(connection, rx));
    tx
}

pub(crate) async fn run_event_router(
    connection: QuicConnection,
    mut rx: mpsc::Receiver<EventRouterCommand>,
) {
    let mut pending_waiters: HashMap<u64, oneshot::Sender<anyhow::Result<RecvStream>>> =
        HashMap::new();
    let mut pending_streams: HashMap<u64, RecvStream> = HashMap::new();
    let mut frame_scratch = BytesMut::with_capacity(64 * 1024);

    let max_pending = runtime_config().event_router_max_pending;

    // POTENTIAL ISSUE:
    // `pending_waiters` and `pending_streams` can grow without bound if:
    // - the server sends EventStreamHello for subscription_ids the client never registers, or
    // - the client registers but the server never sends the stream.
    // We should probably:
    // - add a maximum map size + eviction (LRU), and/or
    // - add per-entry timeouts and periodically purge stale entries.
    loop {
        tokio::select! {
            command = rx.recv() => {
                match command {
                    Some(EventRouterCommand::Register { subscription_id, response }) => {
                        if pending_waiters.len() + pending_streams.len() >= max_pending {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "event router pending limit reached ({max_pending}); refusing registration"
                            )));
                            continue;
                        }
                        if let Some(stream) = pending_streams.remove(&subscription_id) {
                            let _ = response.send(Ok(stream));
                            continue;
                        }
                        if pending_waiters.contains_key(&subscription_id) {
                            let _ = response.send(Err(anyhow::anyhow!(
                                "duplicate subscription registration for {subscription_id}"
                            )));
                            continue;
                        }
                        pending_waiters.insert(subscription_id, response);
                    }
                    None => {
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!("event stream router closed")));
                        }
                        break;
                    }
                }
            }
            stream = connection.accept_uni() => {
                let mut recv = match stream {
                    Ok(recv) => recv,
                    Err(err) => {
                        let message = err.to_string();
                        for (_, waiter) in pending_waiters.drain() {
                            let _ = waiter.send(Err(anyhow::anyhow!(message.clone())));
                        }
                        break;
                    }
                };
                if pending_waiters.len() + pending_streams.len() >= max_pending {
                    // Best-effort: drop the stream to cap memory growth under overload.
                    continue;
                }
                let subscription_id = match read_message(&mut recv, &mut frame_scratch).await {
                    Ok(Some(Message::EventStreamHello { subscription_id })) => subscription_id,
                    Ok(Some(_)) => continue,
                    Ok(None) => continue,
                    Err(_) => continue,
                };
                if let Some(waiter) = pending_waiters.remove(&subscription_id) {
                    let _ = waiter.send(Ok(recv));
                    continue;
                }
                if pending_streams.contains_key(&subscription_id) {
                    continue;
                }
                pending_streams.insert(subscription_id, recv);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use felix_transport::{QuicClient, QuicServer, TransportConfig};
    use felix_wire::Message;
    use quinn::ClientConfig as QuinnClientConfig;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use std::sync::Arc;

    async fn quic_pair() -> Result<(
        QuicConnection,
        QuicConnection,
        tokio::sync::oneshot::Sender<()>,
    )> {
        let rcgen::CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let cert_der = cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let server_config =
            quinn::ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())?;
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;

        let mut roots = RootCertStore::empty();
        roots.add(cert_der)?;
        let quinn = QuinnClientConfig::with_root_certificates(Arc::new(roots))?;
        let client = QuicClient::bind("0.0.0.0:0".parse()?, quinn, TransportConfig::default())?;

        let (server_tx, server_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let connection = server.accept().await?;
            let _ = server_tx.send(connection);
            let _ = shutdown_rx.await;
            Ok::<(), anyhow::Error>(())
        });
        let client_conn = client.connect(addr, "localhost").await?;
        let server_conn = server_rx.await.expect("server conn");
        Ok((client_conn, server_conn, shutdown_tx))
    }

    #[tokio::test]
    async fn duplicate_registration_is_rejected() -> Result<()> {
        let quinn = quinn::ClientConfig::try_with_platform_verifier()?;
        crate::config::ClientConfig::optimized_defaults(quinn).install();
        let (client_conn, server_conn, shutdown_tx) = quic_pair().await?;
        let router = spawn_event_router(client_conn);

        let (tx1, rx1) = oneshot::channel();
        router
            .send(EventRouterCommand::Register {
                subscription_id: 1,
                response: tx1,
            })
            .await
            .expect("send");
        let (tx2, rx2) = oneshot::channel();
        router
            .send(EventRouterCommand::Register {
                subscription_id: 1,
                response: tx2,
            })
            .await
            .expect("send");
        let err = rx2.await.expect("response").expect_err("duplicate");
        assert!(
            err.to_string()
                .contains("duplicate subscription registration")
        );
        let mut uni = server_conn.open_uni().await?;
        crate::wire::write_message(&mut uni, Message::EventStreamHello { subscription_id: 1 })
            .await?;
        let _ = uni.finish();
        assert!(rx1.await.expect("response").is_ok());
        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn stream_arrives_before_registration_is_delivered() -> Result<()> {
        let quinn = quinn::ClientConfig::try_with_platform_verifier()?;
        crate::config::ClientConfig::optimized_defaults(quinn).install();
        let (client_conn, server_conn, shutdown_tx) = quic_pair().await?;
        let router = spawn_event_router(client_conn);

        let mut uni = server_conn.open_uni().await?;
        crate::wire::write_message(
            &mut uni,
            Message::EventStreamHello {
                subscription_id: 77,
            },
        )
        .await?;
        let _ = uni.finish();

        let (tx, rx) = oneshot::channel();
        router
            .send(EventRouterCommand::Register {
                subscription_id: 77,
                response: tx,
            })
            .await
            .expect("send");
        assert!(rx.await.expect("response").is_ok());
        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn router_drop_notifies_pending_waiters() -> Result<()> {
        let quinn = quinn::ClientConfig::try_with_platform_verifier()?;
        crate::config::ClientConfig::optimized_defaults(quinn).install();
        let (client_conn, _server_conn, shutdown_tx) = quic_pair().await?;
        let router = spawn_event_router(client_conn);

        let (tx, rx) = oneshot::channel();
        router
            .send(EventRouterCommand::Register {
                subscription_id: 55,
                response: tx,
            })
            .await
            .expect("send");
        drop(router);

        let err = rx.await.expect("response").expect_err("router closed");
        assert!(err.to_string().contains("event stream router closed"));
        let _ = shutdown_tx.send(());
        Ok(())
    }
}
