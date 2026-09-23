use anyhow::Result;
use felix_transport::{QuicClient, QuicServer, TransportConfig};
use felix_wire::Message;
use tokio::sync::oneshot;

use super::*;
use crate::test_support::{build_server_config, quinn_client_config};

async fn quic_pair() -> Result<(
    QuicConnection,
    QuicConnection,
    tokio::sync::oneshot::Sender<()>,
)> {
    let (server_config, cert_der) = build_server_config()?;
    let server = QuicServer::bind(
        "127.0.0.1:0".parse()?,
        server_config,
        TransportConfig::default(),
    )?;
    let addr = server.local_addr()?;

    let quinn = quinn_client_config(cert_der)?;
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
    crate::frame_io::write_message(&mut uni, Message::EventStreamHello { subscription_id: 1 })
        .await?;
    let _ = uni.finish();
    assert!(rx1.await.expect("response").is_ok());
    let _ = shutdown_tx.send(());
    Ok(())
}

#[tokio::test]
async fn stream_arrives_before_registration_is_delivered() -> Result<()> {
    let (client_conn, server_conn, shutdown_tx) = quic_pair().await?;
    let router = spawn_event_router(client_conn);

    let mut uni = server_conn.open_uni().await?;
    crate::frame_io::write_message(
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
