// QUIC transport configuration and primitives.
use anyhow::{Context, Result, anyhow};
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

/// Transport-level configuration defaults.
///
/// ```
/// use felix_transport::TransportConfig;
///
/// let config = TransportConfig::default();
/// assert!(config.max_frame_bytes > 0);
/// ```
#[derive(Debug, Clone)]
pub struct TransportConfig {
    // Max payload size enforced by higher layers.
    pub max_frame_bytes: usize,
    // Max concurrent streams per connection.
    pub max_streams: u16,
    // Connection-level flow control window.
    pub receive_window: u64,
    // Per-stream receive window.
    pub stream_receive_window: u64,
    // Connection-level send window.
    pub send_window: u64,
    // Starting datagram size before path MTU discovery completes.
    // RFC-safe default (1200); raise only on known-good paths (loopback, jumbo LAN)
    // to skip the discovery ramp entirely.
    pub initial_mtu: u16,
    // Upper bound for path MTU discovery probing. Probes are loss-tolerant, so a
    // high bound is safe on any network and lets loopback (~16 KiB) and jumbo-frame
    // LANs (~9 KiB) converge to their real MTU instead of quinn's 1452 default.
    pub mtu_discovery_upper_bound: u16,
    // Largest UDP datagram the endpoint will accept (receive side). Must be at
    // least as large as the peer's discovered MTU for large datagrams to flow.
    pub max_udp_payload_size: u16,
    // Requested SO_SNDBUF/SO_RCVBUF. Applied best-effort: halved until the OS
    // accepts, so an unconfigurable host degrades gracefully.
    pub udp_send_buffer_bytes: usize,
    pub udp_recv_buffer_bytes: usize,
    // Optional initial congestion window (bytes). None keeps quinn's RFC default.
    // Setting this high removes the slow-start ramp on trusted low-loss paths.
    pub initial_congestion_window_bytes: Option<u64>,
}

// Keep defaults large enough for most dev/test workloads.
const DEFAULT_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_STREAMS: u16 = 1024;
const DEFAULT_RECEIVE_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_STREAM_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;
const DEFAULT_SEND_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_INITIAL_MTU: u16 = 1200;
// Bound MTU discovery at 16 KiB: it matches loopback (lo0 = 16384) and jumbo-frame
// ceilings, and measured best for byte-heavy workloads. A far-away bound (e.g. the
// QUIC max 65527) makes the search converge slower without ever finding a larger
// path. Small-message workloads can prefer ~4096 (finer ACK clocking) via
// FELIX_MTU_UPPER_BOUND.
const DEFAULT_MTU_DISCOVERY_UPPER_BOUND: u16 = 16384;
const DEFAULT_MAX_UDP_PAYLOAD_SIZE: u16 = 65527;
const DEFAULT_UDP_BUFFER_BYTES: usize = 8 * 1024 * 1024;

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

impl Default for TransportConfig {
    fn default() -> Self {
        // Environment overrides act as process-wide tuning levers so every
        // endpoint (broker, client, demos) picks them up without plumbing.
        let initial_mtu = env_u64("FELIX_INITIAL_MTU")
            .map(|value| value.clamp(1200, 65527) as u16)
            .unwrap_or(DEFAULT_INITIAL_MTU);
        let mtu_discovery_upper_bound = env_u64("FELIX_MTU_UPPER_BOUND")
            .map(|value| value.clamp(1200, 65527) as u16)
            .unwrap_or(DEFAULT_MTU_DISCOVERY_UPPER_BOUND);
        let max_udp_payload_size = env_u64("FELIX_MAX_UDP_PAYLOAD")
            .map(|value| value.clamp(1200, 65527) as u16)
            .unwrap_or(DEFAULT_MAX_UDP_PAYLOAD_SIZE);
        let udp_send_buffer_bytes = env_u64("FELIX_UDP_SEND_BUFFER")
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_UDP_BUFFER_BYTES);
        let udp_recv_buffer_bytes = env_u64("FELIX_UDP_RECV_BUFFER")
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_UDP_BUFFER_BYTES);
        let initial_congestion_window_bytes = env_u64("FELIX_INITIAL_CWND");
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_streams: DEFAULT_MAX_STREAMS,
            receive_window: DEFAULT_RECEIVE_WINDOW,
            stream_receive_window: DEFAULT_STREAM_RECEIVE_WINDOW,
            send_window: DEFAULT_SEND_WINDOW,
            initial_mtu,
            mtu_discovery_upper_bound,
            max_udp_payload_size,
            udp_send_buffer_bytes,
            udp_recv_buffer_bytes,
            initial_congestion_window_bytes,
        }
    }
}

impl TransportConfig {
    fn quinn_transport_config(&self) -> quinn::TransportConfig {
        // Translate Felix defaults into Quinn transport settings.
        let mut config = quinn::TransportConfig::default();
        let streams = quinn::VarInt::from_u32(self.max_streams as u32);
        config.max_concurrent_bidi_streams(streams);
        config.max_concurrent_uni_streams(streams);
        let stream_window =
            quinn::VarInt::from_u64(self.stream_receive_window).expect("stream receive window");
        let receive_window = quinn::VarInt::from_u64(self.receive_window).expect("receive window");
        config.stream_receive_window(stream_window);
        config.receive_window(receive_window);
        config.send_window(self.send_window);
        // Path MTU: start safe, probe high. Fewer, larger datagrams directly
        // reduce per-byte syscall and crypto costs on high-MTU paths.
        let initial_mtu = self.initial_mtu.clamp(1200, self.max_udp_payload_size);
        config.initial_mtu(initial_mtu);
        let mtu_bound = self
            .mtu_discovery_upper_bound
            .clamp(initial_mtu, self.max_udp_payload_size);
        let mut mtud = quinn::MtuDiscoveryConfig::default();
        mtud.upper_bound(mtu_bound);
        config.mtu_discovery_config(Some(mtud));
        // Quinn follows RFC 9002 by default and raises the minimum window to
        // two datagrams when path-MTU discovery finds a larger MTU. Keep that
        // safe default unless a trusted low-loss deployment explicitly opts
        // into a larger initial burst.
        if let Some(window) = self.initial_congestion_window_bytes {
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(window);
            config.congestion_controller_factory(Arc::new(cubic));
        }
        config
    }

    fn quinn_endpoint_config(&self) -> quinn::EndpointConfig {
        let mut config = quinn::EndpointConfig::default();
        // Accept datagrams up to the configured bound (quinn's default of 1472
        // would silently cap peers that discovered a larger path MTU).
        if let Err(err) = config.max_udp_payload_size(self.max_udp_payload_size) {
            tracing::warn!(error = %err, "invalid max_udp_payload_size; using quinn default");
        }
        config
    }

    fn bind_udp_socket(&self, addr: SocketAddr) -> Result<std::net::UdpSocket> {
        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
                .context("create UDP socket")?;
        // Large socket buffers absorb bursts at high message rates; drops here
        // surface as QUIC retransmits and latency spikes. Halve until the OS
        // accepts the size so hosts with low limits still work.
        let mut send_bytes = self.udp_send_buffer_bytes;
        while send_bytes >= 64 * 1024 && socket.set_send_buffer_size(send_bytes).is_err() {
            send_bytes /= 2;
        }
        let mut recv_bytes = self.udp_recv_buffer_bytes;
        while recv_bytes >= 64 * 1024 && socket.set_recv_buffer_size(recv_bytes).is_err() {
            recv_bytes /= 2;
        }
        socket.bind(&addr.into()).context("bind UDP socket")?;
        socket
            .set_nonblocking(true)
            .context("set UDP socket nonblocking")?;
        Ok(socket.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Stable connection identifier used for tracing/logging.
///
/// ```
/// use felix_transport::ConnectionId;
///
/// let id = ConnectionId(7);
/// assert_eq!(id.0, 7);
/// ```
pub struct ConnectionId(pub u64);

#[derive(Debug, Clone)]
/// Metadata about a live QUIC connection.
///
/// ```
/// use felix_transport::{ConnectionId, ConnectionInfo};
/// use std::net::SocketAddr;
///
/// let info = ConnectionInfo {
///     id: ConnectionId(42),
///     peer_addr: "127.0.0.1:4433".parse::<SocketAddr>().expect("addr"),
/// };
/// assert_eq!(info.id.0, 42);
/// ```
pub struct ConnectionInfo {
    pub id: ConnectionId,
    pub peer_addr: SocketAddr,
}

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
}

impl QuicServer {
    pub fn bind(
        addr: SocketAddr,
        mut server_config: ServerConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        // Apply transport defaults before binding the endpoint.
        let quinn_transport = transport.quinn_transport_config();
        server_config.transport_config(Arc::new(quinn_transport));
        let socket = transport.bind_udp_socket(addr)?;
        let endpoint = Endpoint::new(
            transport.quinn_endpoint_config(),
            Some(server_config),
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .context("bind QUIC server")?;
        Ok(Self {
            endpoint,
            _transport: transport,
        })
    }

    pub async fn accept(&self) -> Result<QuicConnection> {
        // Block until a client connects and finishes the handshake.
        let connecting = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow!("no incoming QUIC connections"))?;
        let connection = connecting.await.context("accept QUIC connection")?;
        Ok(QuicConnection::new(connection))
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .context("read QUIC local address")
    }
}

/// QUIC client endpoint wrapper.
///
/// ```no_run
/// use felix_transport::{QuicClient, TransportConfig};
/// use quinn::ClientConfig;
/// use std::net::SocketAddr;
///
/// fn client_config() -> ClientConfig {
///     // Provide a real TLS config when wiring this up in a service.
///     unimplemented!()
/// }
///
/// let bind: SocketAddr = "0.0.0.0:0".parse().expect("addr");
/// let transport = TransportConfig::default();
/// let _client = QuicClient::bind(bind, client_config(), transport).expect("bind");
/// ```
#[derive(Debug)]
pub struct QuicClient {
    endpoint: Endpoint,
    // Retain for debugging/metrics; Quinn owns the active config.
    _transport: TransportConfig,
}

impl QuicClient {
    pub fn bind(
        addr: SocketAddr,
        mut client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        // Apply transport defaults before binding the endpoint.
        let quinn_transport = transport.quinn_transport_config();
        client_config.transport_config(Arc::new(quinn_transport));
        let socket = transport.bind_udp_socket(addr)?;
        let mut endpoint = Endpoint::new(
            transport.quinn_endpoint_config(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .context("bind QUIC client")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint,
            _transport: transport,
        })
    }

    pub async fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<QuicConnection> {
        // Initiate and await a QUIC handshake.
        let connecting = self
            .endpoint
            .connect(addr, server_name)
            .context("initiate QUIC connection")?;
        let connection = connecting.await.context("establish QUIC connection")?;
        Ok(QuicConnection::new(connection))
    }
}

/// Active QUIC connection wrapper with convenience helpers.
///
/// ```no_run
/// use felix_transport::QuicConnection;
///
/// async fn open_streams(connection: QuicConnection) -> anyhow::Result<()> {
///     let (_send, _recv) = connection.open_bi().await?;
///     let _send_only = connection.open_uni().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct QuicConnection {
    inner: Connection,
    // Stable id and peer metadata for tracing.
    info: ConnectionInfo,
}

impl QuicConnection {
    fn new(connection: Connection) -> Self {
        // Quinn exposes a stable connection id for logging.
        let info = ConnectionInfo {
            id: ConnectionId(u64::try_from(connection.stable_id()).expect("stable id fits u64")),
            peer_addr: connection.remote_address(),
        };
        Self {
            inner: connection,
            info,
        }
    }

    pub fn info(&self) -> &ConnectionInfo {
        &self.info
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.inner.stats()
    }

    /// Open a bidirectional stream to the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn open(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let (_send, _recv) = connection.open_bi().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream)> {
        self.inner.open_bi().await.context("open bidi stream")
    }

    /// Open a unidirectional send stream to the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn open(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let _send = connection.open_uni().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn open_uni(&self) -> Result<SendStream> {
        self.inner.open_uni().await.context("open uni stream")
    }

    /// Accept the next bidirectional stream from the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn accept(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let (_send, _recv) = connection.accept_bi().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream)> {
        self.inner.accept_bi().await.context("accept bidi stream")
    }

    /// Accept the next unidirectional receive stream from the peer.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// async fn accept(connection: QuicConnection) -> anyhow::Result<()> {
    ///     let _recv = connection.accept_uni().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn accept_uni(&self) -> Result<RecvStream> {
        self.inner.accept_uni().await.context("accept uni stream")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use rcgen::generate_simple_self_signed;
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::sync::Arc;

    #[test]
    fn default_transport_config() {
        // Basic sanity checks on defaults.
        let config = TransportConfig::default();
        assert!(config.max_frame_bytes > 0);
        assert!(config.max_streams > 0);
    }

    #[test]
    fn connection_info_holds_fields() {
        let info = ConnectionInfo {
            id: ConnectionId(42),
            peer_addr: "127.0.0.1:1234".parse().expect("addr"),
        };
        assert_eq!(info.id, ConnectionId(42));
        assert_eq!(info.peer_addr, "127.0.0.1:1234".parse().unwrap());
    }

    fn make_server_config() -> Result<(ServerConfig, CertificateDer<'static>)> {
        let cert = generate_simple_self_signed(vec!["localhost".into()])
            .context("generate self-signed cert")?;
        let cert_der = cert.cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server_config = ServerConfig::with_single_cert(vec![cert_der.clone()], key_der.into())
            .context("build server config")?;
        Ok((server_config, cert_der))
    }

    fn make_client_config(cert: CertificateDer<'static>) -> Result<ClientConfig> {
        let mut roots = RootCertStore::empty();
        roots.add(cert).context("add root cert")?;
        Ok(ClientConfig::with_root_certificates(Arc::new(roots))?)
    }

    #[tokio::test]
    async fn quic_smoke_test() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let buf = recv.read_to_end(1024).await?;
            send.write_all(&buf).await?;
            send.finish()?;
            send.stopped().await?;
            Result::<()>::Ok(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        assert_eq!(connection.info().peer_addr, addr);
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(b"ping").await?;
        send.finish()?;
        let response = recv.read_to_end(1024).await?;
        assert_eq!(response, b"ping");

        server_task.await.context("server task join")??;
        Ok(())
    }

    #[tokio::test]
    async fn quic_uni_stream_smoke() -> Result<()> {
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let mut recv = connection.accept_uni().await?;
            let buf = recv.read_to_end(1024).await?;
            Result::<Vec<u8>>::Ok(buf)
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let mut send = connection.open_uni().await?;
        send.write_all(b"uni").await?;
        send.finish()?;

        let received = server_task.await.context("server task join")??;
        assert_eq!(received, b"uni");
        Ok(())
    }

    #[test]
    fn transport_config_custom_values() {
        let config = TransportConfig {
            max_frame_bytes: 8 * 1024 * 1024,
            max_streams: 2048,
            receive_window: 128 * 1024 * 1024,
            stream_receive_window: 32 * 1024 * 1024,
            send_window: 128 * 1024 * 1024,
            ..TransportConfig::default()
        };
        assert_eq!(config.max_frame_bytes, 8 * 1024 * 1024);
        assert_eq!(config.max_streams, 2048);
        assert_eq!(config.receive_window, 128 * 1024 * 1024);
        assert_eq!(config.stream_receive_window, 32 * 1024 * 1024);
        assert_eq!(config.send_window, 128 * 1024 * 1024);
    }

    #[test]
    fn transport_config_mtu_defaults_are_probe_high_start_safe() {
        let config = TransportConfig::default();
        // Start at the RFC-safe minimum unless explicitly overridden.
        assert!(config.initial_mtu >= 1200);
        // Probe up to the QUIC maximum so high-MTU paths (loopback, jumbo LAN)
        // converge to their real MTU.
        assert!(config.mtu_discovery_upper_bound >= config.initial_mtu);
        assert!(config.max_udp_payload_size >= config.mtu_discovery_upper_bound);
        // Building quinn configs must not panic for the defaults.
        let _ = config.quinn_transport_config();
        let _ = config.quinn_endpoint_config();
    }

    #[test]
    fn transport_config_initial_cwnd_applies_without_panic() {
        let config = TransportConfig {
            initial_congestion_window_bytes: Some(4 * 1024 * 1024),
            ..TransportConfig::default()
        };
        let _ = config.quinn_transport_config();
    }

    #[tokio::test]
    async fn quic_server_local_addr() -> Result<()> {
        let (server_config, _cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport)?;
        let addr = server.local_addr()?;
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert!(addr.port() > 0);
        Ok(())
    }

    #[test]
    fn connection_id_equality() {
        let id1 = ConnectionId(42);
        let id2 = ConnectionId(42);
        let id3 = ConnectionId(43);
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }
}
