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
    pub max_frame_bytes: usize,
    pub max_streams: u16,
}

impl Default for TransportConfig {
    fn default() -> Self {
        // Keep defaults large enough for most dev/test workloads.
        Self {
            max_frame_bytes: 4 * 1024 * 1024,
            max_streams: 1024,
        }
    }
}

impl TransportConfig {
    fn quinn_transport_config(&self) -> quinn::TransportConfig {
        let mut config = quinn::TransportConfig::default();
        let streams = quinn::VarInt::from_u32(self.max_streams as u32);
        config.max_concurrent_bidi_streams(streams);
        config.max_concurrent_uni_streams(streams);
        config
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
    _transport: TransportConfig,
}

impl QuicServer {
    pub fn bind(
        addr: SocketAddr,
        mut server_config: ServerConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        let quinn_transport = transport.quinn_transport_config();
        server_config.transport_config(Arc::new(quinn_transport));
        let endpoint = Endpoint::server(server_config, addr).context("bind QUIC server")?;
        Ok(Self {
            endpoint,
            _transport: transport,
        })
    }

    pub async fn accept(&self) -> Result<QuicConnection> {
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
    _transport: TransportConfig,
}

impl QuicClient {
    pub fn bind(
        addr: SocketAddr,
        mut client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        let quinn_transport = transport.quinn_transport_config();
        client_config.transport_config(Arc::new(quinn_transport));
        let mut endpoint = Endpoint::client(addr).context("bind QUIC client")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint,
            _transport: transport,
        })
    }

    pub async fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<QuicConnection> {
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
#[derive(Debug)]
pub struct QuicConnection {
    inner: Connection,
    info: ConnectionInfo,
}

impl QuicConnection {
    fn new(connection: Connection) -> Self {
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
        let cert_der = CertificateDer::from(cert.serialize_der()?);
        let key_der = PrivatePkcs8KeyDer::from(cert.get_key_pair().serialize_der());
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
}
