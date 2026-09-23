//! The accepting side: [`QuicServer`].

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use quinn::{Endpoint, ServerConfig};

use crate::config::TransportConfig;
use crate::connection::QuicConnection;
use crate::io_runtime::{EndpointRole, quinn_runtime};
use crate::socket::{effective_udp_buffer_bytes, warn_if_udp_buffers_were_clamped};

/// QUIC server endpoint wrapper.
///
/// ```no_run
/// use felix_transport::{QuicServer, TransportConfig};
/// use quinn::ServerConfig;
/// use std::net::SocketAddr;
///
/// fn server_config() -> ServerConfig {
///     // Provide a real TLS config when wiring this up in a service.
///     unimplemented!()
/// }
///
/// let bind: SocketAddr = "127.0.0.1:0".parse().expect("addr");
/// let transport = TransportConfig::default();
/// let _server = QuicServer::bind(bind, server_config(), transport).expect("bind");
/// ```
#[derive(Debug)]
pub struct QuicServer {
    endpoint: Endpoint,
    // Retain for debugging/metrics; Quinn owns the active config.
    _transport: TransportConfig,
    // Variant of the server config used for loopback peers; see
    // [`TransportConfig::loopback_initial_mtu`].
    loopback_config: Option<Arc<ServerConfig>>,
    // I/O runtime for this endpoint's quinn drivers (None when isolation is
    // disabled); handed to each connection for pump colocation.
    io_handle: Option<tokio::runtime::Handle>,
}

impl QuicServer {
    pub fn bind(
        addr: SocketAddr,
        mut server_config: ServerConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        // Apply transport defaults before binding the endpoint. The socket is
        // bound first: the loopback MTU guarantee depends on the buffer sizes
        // the OS actually granted it.
        let socket = transport.bind_udp_socket(addr)?;
        let quinn_transport = transport.quinn_transport_config();
        let loopback_config = transport
            .loopback_initial_mtu({
                warn_if_udp_buffers_were_clamped(
                    &socket,
                    transport
                        .udp_recv_buffer_bytes
                        .min(transport.udp_send_buffer_bytes),
                );
                effective_udp_buffer_bytes(&socket)
            })
            .map(|mtu| {
                let mut config = server_config.clone();
                config
                    .transport_config(Arc::new(transport.quinn_transport_config_for_loopback(mtu)));
                Arc::new(config)
            });
        server_config.transport_config(Arc::new(quinn_transport));
        let (runtime, io_handle) = quinn_runtime(EndpointRole::Server);
        let endpoint = Endpoint::new(
            transport.quinn_endpoint_config(),
            Some(server_config),
            socket,
            runtime,
        )
        .context("bind QUIC server")?;
        Ok(Self {
            endpoint,
            _transport: transport,
            loopback_config,
            io_handle,
        })
    }

    pub async fn accept(&self) -> Result<QuicConnection> {
        // Block until a client connects and finishes the handshake.
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow!("no incoming QUIC connections"))?;
        let connection = match &self.loopback_config {
            Some(config) if incoming.remote_address().ip().is_loopback() => incoming
                .accept_with(Arc::clone(config))
                .context("accept loopback QUIC connection")?
                .await
                .context("accept QUIC connection")?,
            _ => incoming.await.context("accept QUIC connection")?,
        };
        Ok(QuicConnection::new(connection, self.io_handle.clone()))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .context("read QUIC local address")
    }
}
