//! The inbound half of the internal transport: the listener peers connect to.
//!
//! Separate from the client-facing listener, and that separation is the point.
//! Three things keep the two roles apart, in the order a connection meets them:
//!
//! 1. **A separate port.** Startup refuses a configuration where the two share
//!    one (see [`super::config::PeerTransportConfig`]).
//! 2. **ALPN.** Both ends negotiate `felix-internal/1`; the client-facing
//!    endpoint negotiates none, so there is no protocol in common and TLS
//!    refuses the handshake. A client pointed at this port never reaches frame
//!    decoding, and never touches broker state.
//! 3. **A distinct magic.** A client frame fails to decode here rather than
//!    parsing into something plausible.
//!
//! None of that is peer *authentication*, which is mTLS in M8.1 (#136).
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_transport::{QuicConnection, QuicServer};
use felix_wire::internal::{ErrorCode, ForwardPublishError, HelloOk, InternalMessage};
use tokio_util::sync::CancellationToken;

use super::codec::{read_frame, write_frame};
use super::config::{INTERNAL_ALPN, PeerTransportConfig};
use super::metrics;
use super::tls;

/// What answers a peer's request.
///
/// Implemented by the broker in M4.3 (#106). Until then the listener answers
/// every forward with `Unavailable`, which is the honest answer: this broker
/// can be reached, and cannot yet apply a forwarded write.
#[async_trait::async_trait]
pub trait PeerRequestHandler: Send + Sync + 'static {
    /// Answer one request. Must always produce a terminal response — the
    /// protocol has no "no answer" outcome, and a requester that gets none has
    /// to wait out its timeout.
    async fn handle(&self, request: InternalMessage) -> InternalMessage;
}

/// Answers every forwarded request with `Unavailable`.
///
/// The listener is useful before forwarding is wired: peers can establish
/// connections, negotiate, and observe a live broker, and they are told plainly
/// that it cannot serve rather than being left to time out.
pub struct UnavailableHandler;

#[async_trait::async_trait]
impl PeerRequestHandler for UnavailableHandler {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        InternalMessage::ForwardPublishError(ForwardPublishError {
            correlation_id: request.correlation_id(),
            code: ErrorCode::Unavailable,
            detail: "this broker does not apply forwarded publishes yet".to_string(),
        })
    }
}

/// The broker-internal listener.
pub struct PeerServer {
    server: QuicServer,
    node_id: String,
    handler: Arc<dyn PeerRequestHandler>,
}

impl PeerServer {
    /// Bind the internal listener.
    pub fn bind(
        node_id: String,
        config: &PeerTransportConfig,
        handler: Arc<dyn PeerRequestHandler>,
    ) -> Result<Self> {
        let server = QuicServer::bind(config.bind, tls::server_config()?, config.quic_transport())
            .context("bind internal QUIC listener")?;
        Ok(Self {
            server,
            node_id,
            handler,
        })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.server.local_addr()
    }

    /// Accept peer connections until cancelled.
    pub async fn serve(self, shutdown: CancellationToken) {
        loop {
            let connection = tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = self.server.accept() => match accepted {
                    Ok(connection) => connection,
                    Err(err) => {
                        // An accept failure is per-connection; the endpoint is
                        // still bound, so stopping the loop would silently take
                        // this broker out of the cluster.
                        tracing::debug!(error = %err, "internal accept failed");
                        metrics::record_inbound_rejected("handshake");
                        continue;
                    }
                },
            };

            // A backstop, not the enforcement: TLS has already refused any
            // connection that did not negotiate the internal ALPN, so this only
            // fires if that ever stops being true.
            if connection.negotiated_protocol().as_deref() != Some(INTERNAL_ALPN) {
                tracing::warn!(
                    peer = %connection.info().peer_addr,
                    "refusing connection on the internal listener: not an internal peer",
                );
                metrics::record_inbound_rejected("alpn");
                connection.close(1u32.into(), b"internal listener requires felix-internal/1");
                continue;
            }

            // Close this connection when the broker shuts down, from a task of
            // its own. Cancellation alone is not enough: a handler already
            // running is not interruptible, so a peer would otherwise wait out
            // its request timeout to learn this broker had gone.
            let closer = connection.clone();
            let closing = shutdown.clone();
            connection.spawn_pump(async move {
                closing.cancelled().await;
                closer.close(0u32.into(), b"broker shutting down");
            });

            let node_id = self.node_id.clone();
            let handler = Arc::clone(&self.handler);
            let shutdown = shutdown.clone();
            let served = connection.clone();
            connection.spawn_pump(async move {
                serve_connection(served, node_id, handler, shutdown).await;
            });
        }
    }
}

/// Serve every stream a peer opens on one connection.
async fn serve_connection(
    connection: QuicConnection,
    node_id: String,
    handler: Arc<dyn PeerRequestHandler>,
    shutdown: CancellationToken,
) {
    loop {
        let stream = tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = connection.accept_bi() => match accepted {
                Ok(stream) => stream,
                // The peer closed, or the connection dropped. Either way this
                // connection is finished; the accept loop keeps running.
                Err(_) => break,
            },
        };

        let node_id = node_id.clone();
        let handler = Arc::clone(&handler);
        let shutdown = shutdown.clone();
        connection.spawn_pump(async move {
            let (mut send, mut recv) = stream;
            loop {
                let request = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    frame = read_frame(&mut recv) => match frame {
                        Ok(Some(request)) => request,
                        Ok(None) => break,
                        Err(err) => {
                            // A frame this broker cannot decode means the peer
                            // is wrong about the protocol, not about this
                            // message, so the stream ends rather than skipping
                            // ahead to a boundary it cannot find.
                            tracing::debug!(error = %err, "internal stream ended");
                            metrics::record_served(metrics::OUTCOME_ERROR);
                            break;
                        }
                    },
                };

                let response = match request {
                    // Answered here rather than by the handler: identity is the
                    // transport's to assert, and it must work before the broker
                    // is able to serve anything.
                    InternalMessage::Hello(hello) => {
                        tracing::debug!(peer = %hello.node_id, "internal peer connected");
                        InternalMessage::HelloOk(HelloOk {
                            correlation_id: hello.correlation_id,
                            node_id: node_id.clone(),
                        })
                    }
                    request => handler.handle(request).await,
                };

                if write_frame(&mut send, &response).await.is_err() {
                    metrics::record_served(metrics::OUTCOME_ERROR);
                    break;
                }
                metrics::record_served(metrics::OUTCOME_OK);
            }
            let _ = send.finish();
        });
    }
}
