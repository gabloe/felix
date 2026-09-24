//! A broker that answers every publish and subscribe with one scripted
//! message, and counts what it was sent.
//!
//! It advertises every frame flag and only `FEATURE_ERROR_CODES`, so a
//! [`ClusterClient`](crate::ClusterClient) skips discovery and sends its
//! publishes as acked binary batches, answered here with coded binary acks.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use bytes::BytesMut;
use felix_transport::{QuicServer, TransportConfig};
use felix_wire::Message;
use rustls::pki_types::CertificateDer;

use crate::frame_io::{read_frame_into, write_message};
use crate::test_support::build_server_config;

/// The answer to one request, given its request id (0 for a subscribe).
type Script = Arc<dyn Fn(u64) -> Message + Send + Sync>;

pub(super) struct StubBroker {
    pub(super) addr: SocketAddr,
    publishes: Arc<AtomicUsize>,
    subscribes: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl StubBroker {
    /// A stub with its own certificate, returned for the client to trust.
    pub(super) fn start(
        script: impl Fn(u64) -> Message + Send + Sync + 'static,
    ) -> Result<(Self, CertificateDer<'static>)> {
        let (server_config, cert) = build_server_config()?;
        Ok((Self::start_with(server_config, script)?, cert))
    }

    /// A stub sharing a certificate with another, so one client trusts both.
    pub(super) fn start_with(
        server_config: quinn::ServerConfig,
        script: impl Fn(u64) -> Message + Send + Sync + 'static,
    ) -> Result<Self> {
        let server = QuicServer::bind(
            "127.0.0.1:0".parse()?,
            server_config,
            TransportConfig::default(),
        )?;
        let addr = server.local_addr()?;
        let publishes = Arc::new(AtomicUsize::new(0));
        let subscribes = Arc::new(AtomicUsize::new(0));
        let script: Script = Arc::new(script);
        let counts = (Arc::clone(&publishes), Arc::clone(&subscribes));
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                let script = Arc::clone(&script);
                let counts = (Arc::clone(&counts.0), Arc::clone(&counts.1));
                tokio::spawn(async move {
                    while let Ok((send, recv)) = connection.accept_bi().await {
                        let script = Arc::clone(&script);
                        let counts = (Arc::clone(&counts.0), Arc::clone(&counts.1));
                        tokio::spawn(serve_stream(send, recv, script, counts));
                    }
                });
            }
        });
        Ok(Self {
            addr,
            publishes,
            subscribes,
            task,
        })
    }

    pub(super) fn publishes(&self) -> usize {
        self.publishes.load(Ordering::SeqCst)
    }

    pub(super) fn subscribes(&self) -> usize {
        self.subscribes.load(Ordering::SeqCst)
    }
}

impl Drop for StubBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    script: Script,
    (publishes, subscribes): (Arc<AtomicUsize>, Arc<AtomicUsize>),
) -> Result<()> {
    let mut scratch = BytesMut::with_capacity(64 * 1024);
    while let Some(frame) = read_frame_into(&mut recv, &mut scratch, false).await? {
        if frame.header.flags & felix_wire::FLAG_BINARY_PUBLISH_ACKED != 0 {
            let id = felix_wire::binary::decode_acked_publish_batch(&frame)?.request_id;
            publishes.fetch_add(1, Ordering::SeqCst);
            send.write_all(&binary_ack(id, script(id))?).await?;
            continue;
        }
        match Message::decode(frame)? {
            Message::Auth { .. } => {
                let answer = Message::AuthOk {
                    server_flags: felix_wire::KNOWN_FLAGS,
                    server_features: Some(felix_wire::FEATURE_ERROR_CODES),
                    listener_ports: None,
                };
                write_message(&mut send, answer).await?;
            }
            Message::Subscribe { .. } => {
                subscribes.fetch_add(1, Ordering::SeqCst);
                write_message(&mut send, script(0)).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// A scripted publish answer as the binary ack the client expects.
fn binary_ack(request_id: u64, answer: Message) -> Result<bytes::Bytes> {
    let bytes = match answer {
        Message::PublishError {
            message,
            code,
            retry,
            ..
        } => {
            let code = code.map(|code| {
                let retry = retry.unwrap_or_else(|| code.default_retry());
                (code, retry)
            });
            felix_wire::binary::encode_publish_ack_bytes_coded(
                request_id,
                Some(&message),
                code.as_ref().map(|(code, retry)| (code, *retry)),
                None,
            )?
        }
        _ => felix_wire::binary::encode_publish_ack_bytes(request_id, None)?,
    };
    Ok(bytes)
}
