// QUIC transport configuration and primitives.
use anyhow::{Context, Result, anyhow};
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

/// Whether an endpoint accepts connections or makes them.
///
/// Assignment is not round-robin across both: a server endpoint multiplexes
/// every connection and drives traffic in both directions, so it gets a
/// runtime to itself, while client endpoints share the rest. Letting a client
/// endpoint land on the server's runtime measured **5-6x slower** — the two
/// halves of a request/response ping-pong end up serialized on one thread. In
/// a standalone broker or a standalone client this is a no-op; it matters when
/// both live in one process, as in the benchmark harness and the in-process
/// client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointRole {
    Server,
    Client,
}

/// Dedicated runtimes for quinn's driver tasks. Drivers do bounded work per
/// poll and reschedule themselves, so their re-poll latency is the pipeline's
/// byte-rate ceiling; on a runtime shared with app tasks that latency grows
/// with load, and because wakeups scale with datagram count it caps throughput
/// per *byte* (measured ~7.5x below capacity). Each endpoint gets a
/// single-threaded runtime so driver self-wakes re-poll immediately and never
/// migrate cores; see [`EndpointRole`] for which endpoints share one.
/// `FELIX_IO_RUNTIME_THREADS` sets the pool size (default: 2); `0` restores
/// drivers to the app runtime.
fn io_runtime_index(role: EndpointRole, sequence: usize, pool_len: usize) -> usize {
    if pool_len <= 1 {
        return 0;
    }
    match role {
        // The final runtime is permanently reserved for clients. Server
        // creation history must never let a server drift onto it.
        EndpointRole::Server => sequence % (pool_len - 1),
        EndpointRole::Client => pool_len - 1,
    }
}

fn io_runtime_handle(role: EndpointRole) -> Option<tokio::runtime::Handle> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static IO_RUNTIMES: OnceLock<Vec<tokio::runtime::Runtime>> = OnceLock::new();
    static NEXT_SERVER: AtomicUsize = AtomicUsize::new(0);
    static NEXT_CLIENT: AtomicUsize = AtomicUsize::new(0);
    let pool = IO_RUNTIMES.get_or_init(|| {
        // Two, not one per core. A bigger pool cannot make a single endpoint
        // faster -- an endpoint's driver is one task on one runtime -- and it
        // actively hurts, because endpoints that talk to each other end up on
        // different threads and every message pays cross-thread wakes. Measured
        // at 6 runtimes before endpoints were grouped by role: slow mode on
        // every run. Two is what the roles need: servers on one, clients on the
        // other.
        // Default on only where this is validated. On Linux, running quinn's
        // drivers on a dedicated runtime loses a delivery wakeup: the client's
        // QUIC layer receives and ACKs a stream frame that the application is
        // never woken to read, and the connection sits idle until it times
        // out. Reproduced deterministically on a current-thread app runtime
        // and intermittently (1 in 3) on a multi-threaded one; every other
        // subsystem was ruled out by A/B (pump colocation off, ACK frequency
        // off, MTU pinned) and it never occurs with the pool disabled. The
        // measured gain is macOS-only so far, so the unvalidated platform
        // keeps the stock behaviour rather than trading correctness for an
        // unmeasured speedup. `FELIX_IO_RUNTIME_THREADS` opts in anywhere.
        let default_threads = if cfg!(target_os = "macos") { 2 } else { 0 };
        let threads = std::env::var("FELIX_IO_RUNTIME_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(default_threads);
        (0..threads)
            .filter_map(|index| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name(format!("felix-quic-io-{index}"))
                    .on_thread_start(|| {
                        // High QoS: OS demotion of these threads turns wakeup
                        // latency directly into a throughput ceiling.
                        #[cfg(target_os = "macos")]
                        unsafe {
                            libc::pthread_set_qos_class_self_np(
                                libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
                                0,
                            );
                        }
                    })
                    .enable_all()
                    .build()
                    .map_err(|err| {
                        tracing::warn!(error = %err, "failed to build QUIC I/O runtime; drivers will share the app runtime");
                        err
                    })
                    .ok()
            })
            .collect()
    });
    if pool.is_empty() {
        return None;
    }
    // Keep the role partition stable for the life of the process. In
    // particular, repeated in-process benchmark cases create many sequential
    // server endpoints; creation history must not eventually place one on the
    // client runtime and recreate the measured 5-6x lockstep mode.
    let seq = match role {
        EndpointRole::Server => NEXT_SERVER.fetch_add(1, Ordering::Relaxed),
        EndpointRole::Client => {
            // Every client endpoint shares one runtime, deliberately. A client's
            // publish and event endpoints carry the two halves of the same
            // request/response flow, so splitting them across threads makes each
            // message pay two cross-thread wakes and the pipeline drops into
            // lockstep. Measured on the benchmark harness: co-located clients
            // ~123K msg/s, spread across 6 runtimes ~21K on every single run.
            NEXT_CLIENT.fetch_add(1, Ordering::Relaxed)
        }
    };
    let index = io_runtime_index(role, seq, pool.len());
    // Which endpoints share a runtime decides whether their drivers hand off
    // in-thread or pay a cross-thread wake per datagram, so this assignment is
    // load-bearing for throughput, not just bookkeeping.
    tracing::debug!(
        ?role,
        endpoint_seq = seq,
        io_runtime = index,
        pool = pool.len(),
        "assigned QUIC endpoint to I/O runtime"
    );
    Some(pool[index].handle().clone())
}

/// `quinn::Runtime` that runs driver tasks on the dedicated I/O runtime.
/// Timers and the UDP socket are created in that runtime's context so they
/// register with its reactor; application-facing stream futures are unaffected.
#[derive(Debug)]
struct IoRuntime {
    handle: tokio::runtime::Handle,
}

impl quinn::Runtime for IoRuntime {
    fn new_timer(&self, i: std::time::Instant) -> std::pin::Pin<Box<dyn quinn::AsyncTimer>> {
        let _guard = self.handle.enter();
        quinn::TokioRuntime.new_timer(i)
    }

    fn spawn(&self, future: std::pin::Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.handle.spawn(future);
    }

    fn wrap_udp_socket(
        &self,
        t: std::net::UdpSocket,
    ) -> std::io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        let _guard = self.handle.enter();
        quinn::TokioRuntime.wrap_udp_socket(t)
    }
}

/// The quinn runtime new endpoints should use, honouring the isolation switch.
/// Also returns the chosen handle so the endpoint can offer it to pump tasks.
fn quinn_runtime(role: EndpointRole) -> (Arc<dyn quinn::Runtime>, Option<tokio::runtime::Handle>) {
    match io_runtime_handle(role) {
        Some(handle) => (
            Arc::new(IoRuntime {
                handle: handle.clone(),
            }),
            Some(handle),
        ),
        None => (Arc::new(quinn::TokioRuntime), None),
    }
}

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

// Largest UDP payload that fits a 16 KiB loopback interface MTU (macOS lo0 is
// 16384; Linux lo is 65536) after IPv6 headers (48 bytes; IPv4 needs only 28).
const LOOPBACK_UDP_PAYLOAD: u16 = 16336;

impl TransportConfig {
    /// The initial MTU to use for a connection whose peer is a loopback
    /// address, or `None` to keep the configured default.
    ///
    /// Running at the real loopback MTU matters beyond skipping the discovery
    /// ramp: quinn's black-hole detector can misread a congestive loss burst
    /// (all lost packets full-MTU, which is what overflowing the peer's UDP
    /// socket buffer looks like at high rate) as an MTU black hole and drop
    /// the path MTU to `min_mtu` (1200 by default). Recovery probes are only
    /// sent when the connection has nothing else to transmit, so a busy
    /// connection that collapses stays collapsed — measured on loopback as
    /// ~13x the datagrams per byte and a 5-6x throughput drop for the life of
    /// the load. Guaranteeing the loopback MTU (see
    /// [`Self::quinn_transport_config_for_loopback`]) leaves the detector
    /// nothing to collapse to, which removes the failure mode on the one path
    /// where it was both most likely and least recoverable. An explicit
    /// `FELIX_INITIAL_MTU` wins over this.
    ///
    /// `effective_buffer_bytes` is the smaller of the socket's *achieved*
    /// send/receive buffers, and gates the whole guarantee: full-size
    /// datagrams are only safe when the kernel buffers hold a real burst of
    /// them. Linux silently clamps `SO_RCVBUF` to `net.core.rmem_max`
    /// (~208 KB on stock hosts and CI runners) — roughly 26 jumbo datagrams
    /// of headroom, which sustained load overflows catastrophically, and the
    /// pinned `min_mtu` would then also forbid the one thing that helps a
    /// tiny buffer: smaller datagrams. Below the threshold the connection
    /// keeps the stock RFC-safe path (initial 1200, normal discovery, short
    /// black-hole cooldown).
    fn loopback_initial_mtu(&self, effective_buffer_bytes: usize) -> Option<u16> {
        if env_u64("FELIX_INITIAL_MTU").is_some() {
            return None;
        }
        let target = LOOPBACK_UDP_PAYLOAD
            .min(self.mtu_discovery_upper_bound)
            .min(self.max_udp_payload_size);
        // 64 full-size datagrams of burst headroom (~1 MiB at 16336). Hosts
        // tuned per docs/storage-performance.md sysctl guidance qualify;
        // stock-limit Linux hosts do not.
        if effective_buffer_bytes < usize::from(target).saturating_mul(64) {
            return None;
        }
        (target > self.initial_mtu).then_some(target)
    }

    fn quinn_transport_config(&self) -> quinn::TransportConfig {
        self.quinn_transport_config_inner(self.initial_mtu, None)
    }

    /// Loopback variant: start at `initial_mtu` and also *guarantee* it via
    /// quinn's `min_mtu`. The guarantee is what defuses the black-hole
    /// detector — its reset target is `min_mtu`, not `initial_mtu`, so
    /// raising only the start size still collapses to 1200 on a false
    /// verdict. Loopback is the one path where the larger payload size is
    /// guaranteed by construction; never set `min_mtu` for a real network.
    fn quinn_transport_config_for_loopback(&self, mtu: u16) -> quinn::TransportConfig {
        self.quinn_transport_config_inner(mtu, Some(mtu))
    }

    fn quinn_transport_config_inner(
        &self,
        initial_mtu: u16,
        guaranteed_mtu: Option<u16>,
    ) -> quinn::TransportConfig {
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
        let initial_mtu = initial_mtu.clamp(1200, self.max_udp_payload_size);
        config.initial_mtu(initial_mtu);
        if let Some(guaranteed) = guaranteed_mtu {
            config.min_mtu(guaranteed.clamp(1200, initial_mtu));
        }
        // With a guaranteed MTU there is nothing above it to discover, so pin
        // the probe bound to it. This is load-bearing, not an optimization: a
        // probe is full-MTU, bypasses the congestion check, and counts against
        // the window once in flight — on a quiet connection at the two-segment
        // initial window, a doomed probe toward a higher bound starves every
        // ordinary small send behind it ("blocked by congestion control")
        // until its retransmits exhaust, and the search then starts over.
        let mtu_bound = match guaranteed_mtu {
            Some(_) => initial_mtu,
            None => self
                .mtu_discovery_upper_bound
                .clamp(initial_mtu, self.max_udp_payload_size),
        };
        let mut mtud = quinn::MtuDiscoveryConfig::default();
        mtud.upper_bound(mtu_bound);
        // Quinn's MTU black-hole detector cannot tell "large packets are being
        // silently eaten by the path" from "a congestive loss burst dropped a
        // window of large packets". When a sender overruns the receiver's UDP
        // socket buffer (easy at high rate: the standing queue sits within a
        // couple MB of the buffer size, so one scheduling stall overflows it),
        // every lost packet is full-MTU, the detector calls it a black hole,
        // and the path MTU collapses to `initial_mtu`. With quinn's default
        // 60 s cooldown the connection then pays ~13x the datagrams (and
        // syscalls) per byte for a minute — measured as a 5-6x throughput
        // collapse that is indistinguishable from a scheduling defect. A short
        // cooldown re-probes within seconds and restores the discovered MTU;
        // on a genuine black-hole path the extra cost is one loss-tolerant
        // probe packet per cooldown.
        let cooldown_ms = env_u64("FELIX_MTU_BLACK_HOLE_COOLDOWN_MS")
            .map(|value| value.max(100))
            .unwrap_or(2_000);
        mtud.black_hole_cooldown(std::time::Duration::from_millis(cooldown_ms));
        config.mtu_discovery_config(Some(mtud));
        // ACK frequency extension (quinn peers only): cap ACK delay well below
        // the RFC's 25 ms — a window-limited sender resumes only on an ACK, so
        // delayed ACKs stall the whole pipeline — and ACK less often than every
        // other packet, since each reverse-path ACK costs a datagram plus its
        // wakeup chain (+15% throughput measured at threshold 20).
        if env_u64("FELIX_ACK_FREQ_DISABLE").is_none() {
            let mut ack_frequency = quinn::AckFrequencyConfig::default();
            ack_frequency.max_ack_delay(Some(std::time::Duration::from_millis(2)));
            let threshold = env_u64("FELIX_ACK_ELICITING_THRESHOLD").unwrap_or(20);
            ack_frequency.ack_eliciting_threshold(
                quinn::VarInt::from_u64(threshold.min(u32::MAX as u64)).expect("threshold fits"),
            );
            config.ack_frequency_config(Some(ack_frequency));
        }
        // Quinn follows RFC 9002 by default and raises the minimum window to
        // two datagrams when path-MTU discovery finds a larger MTU. Keep that
        // safe default unless a trusted low-loss deployment explicitly opts
        // into a larger initial burst.
        if let Some(window) = self.initial_congestion_window_bytes {
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(window);
            config.congestion_controller_factory(Arc::new(cubic));
        } else if u64::from(initial_mtu) * 2 > 14_720 {
            // Quinn's default initial congestion window is a flat 14,720 bytes
            // (RFC 9002's constant, sized for ~1200-byte datagrams) and its
            // send path reserves a full segment per datagram — so an initial
            // MTU larger than the window deadlocks the connection before the
            // first packet ("blocked by congestion control", forever). Scale
            // the window with the datagram size using RFC 9002's own formula.
            let mtu = u64::from(initial_mtu);
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(14_720u64.clamp(2 * mtu, 10 * mtu));
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

/// The smaller of the socket's achieved send/receive buffers. Read back from
/// the socket rather than taken from config: Linux accepts an oversized
/// `SO_RCVBUF`/`SO_SNDBUF` and silently clamps it to `net.core.rmem_max` /
/// `wmem_max`, so the configured size says nothing about what was granted.
fn effective_udp_buffer_bytes(socket: &std::net::UdpSocket) -> usize {
    let socket = socket2::SockRef::from(socket);
    let send = socket.send_buffer_size().unwrap_or(0);
    let recv = socket.recv_buffer_size().unwrap_or(0);
    send.min(recv)
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
            .loopback_initial_mtu(effective_udp_buffer_bytes(&socket))
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
    // Variant of the client config used when connecting to a loopback peer;
    // see [`TransportConfig::loopback_initial_mtu`].
    loopback_config: Option<ClientConfig>,
    // See `QuicServer::io_handle`.
    io_handle: Option<tokio::runtime::Handle>,
}

impl QuicClient {
    pub fn bind(
        addr: SocketAddr,
        mut client_config: ClientConfig,
        transport: TransportConfig,
    ) -> Result<Self> {
        // Apply transport defaults before binding the endpoint. The socket is
        // bound first: the loopback MTU guarantee depends on the buffer sizes
        // the OS actually granted it.
        let socket = transport.bind_udp_socket(addr)?;
        let quinn_transport = transport.quinn_transport_config();
        let loopback_config = transport
            .loopback_initial_mtu(effective_udp_buffer_bytes(&socket))
            .map(|mtu| {
                let mut config = client_config.clone();
                config
                    .transport_config(Arc::new(transport.quinn_transport_config_for_loopback(mtu)));
                config
            });
        client_config.transport_config(Arc::new(quinn_transport));
        let (runtime, io_handle) = quinn_runtime(EndpointRole::Client);
        let mut endpoint = Endpoint::new(transport.quinn_endpoint_config(), None, socket, runtime)
            .context("bind QUIC client")?;
        endpoint.set_default_client_config(client_config);
        Ok(Self {
            endpoint,
            _transport: transport,
            loopback_config,
            io_handle,
        })
    }

    pub async fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<QuicConnection> {
        // Initiate and await a QUIC handshake.
        let connecting = match &self.loopback_config {
            Some(config) if addr.ip().is_loopback() => self
                .endpoint
                .connect_with(config.clone(), addr, server_name)
                .context("initiate loopback QUIC connection")?,
            _ => self
                .endpoint
                .connect(addr, server_name)
                .context("initiate QUIC connection")?,
        };
        let connection = connecting.await.context("establish QUIC connection")?;
        Ok(QuicConnection::new(connection, self.io_handle.clone()))
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
    // The I/O runtime this connection's quinn drivers run on, if isolated.
    io_handle: Option<tokio::runtime::Handle>,
}

impl QuicConnection {
    fn new(connection: Connection, io_handle: Option<tokio::runtime::Handle>) -> Self {
        // Quinn exposes a stable connection id for logging.
        let info = ConnectionInfo {
            id: ConnectionId(u64::try_from(connection.stable_id()).expect("stable id fits u64")),
            peer_addr: connection.remote_address(),
        };
        Self {
            inner: connection,
            info,
            io_handle,
        }
    }

    pub fn info(&self) -> &ConnectionInfo {
        &self.info
    }

    /// Spawn a task colocated with this connection's quinn drivers.
    ///
    /// For pump tasks woken by the transport per datagram or per write (stream
    /// readers, connection writers): same-thread wakeups are task switches
    /// instead of cross-core kernel round trips, and that latency is the
    /// pipeline's clock under smooth arrival. Falls back to `tokio::spawn`
    /// when driver isolation is disabled.
    pub fn spawn_pump<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        static COLOCATE: OnceLock<bool> = OnceLock::new();
        let colocate = *COLOCATE.get_or_init(|| {
            std::env::var("FELIX_PUMP_COLOCATE")
                .map(|value| value != "0")
                .unwrap_or(true)
        });
        match (&self.io_handle, colocate) {
            (Some(handle), true) => handle.spawn(future),
            _ => tokio::spawn(future),
        }
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.inner.stats()
    }

    /// Why the connection closed, or `None` while it is still live. Lets a
    /// task holding a clone notice the close and exit instead of keeping the
    /// handle alive forever.
    pub fn close_reason(&self) -> Option<quinn::ConnectionError> {
        self.inner.close_reason()
    }

    /// Close the connection, telling the peer why.
    ///
    /// Distinct from dropping the connection: a close sends CONNECTION_CLOSE
    /// immediately, so the peer learns this was deliberate rather than waiting
    /// for an idle timeout to decide the server vanished. That distinction is
    /// what makes a graceful drain observable from the client side.
    ///
    /// ```no_run
    /// use felix_transport::QuicConnection;
    ///
    /// fn shutdown(connection: &QuicConnection) {
    ///     connection.close(0u32.into(), b"shutting down");
    /// }
    /// ```
    pub fn close(&self, code: quinn::VarInt, reason: &[u8]) {
        self.inner.close(code, reason);
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

    /// Minimal reproduction of the delivery pattern Felix's subscription path
    /// uses: a sender writing many small chunks to one uni stream, and a
    /// reader colocated with the connection's drivers forwarding each item
    /// through a bounded channel. Isolating quinn's drivers onto a dedicated
    /// runtime lost a wakeup here on Linux — the receiver ACKed bytes it never
    /// delivered to the application — so this covers that path without the
    /// broker or client in the picture.
    #[tokio::test]
    async fn many_small_frames_reach_a_colocated_reader() -> Result<()> {
        const N: u64 = 400;
        let (server_config, cert) = make_server_config()?;
        let transport = TransportConfig::default();
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;

        let server_task = tokio::spawn(async move {
            let connection = server.accept().await?;
            let mut send = connection.open_uni().await?;
            for i in 0..N {
                send.write_all(&i.to_be_bytes()).await?;
                // One write per item, as the delivery writer does.
                tokio::task::yield_now().await;
            }
            send.finish()?;
            let _ = send.stopped().await;
            Result::<()>::Ok(())
        });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let mut recv = connection.accept_uni().await?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);
        connection.spawn_pump(async move {
            let mut buf = [0u8; 8];
            loop {
                if recv.read_exact(&mut buf).await.is_err() {
                    break;
                }
                if tx.send(u64::from_be_bytes(buf)).await.is_err() {
                    break;
                }
            }
        });

        for expected in 0..N {
            let value = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
                .await
                .with_context(|| format!("delivery stalled waiting for item {expected}"))?
                .with_context(|| format!("stream ended early at item {expected}"))?;
            assert_eq!(value, expected);
        }

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
    fn io_runtime_assignment_keeps_roles_disjoint() {
        for sequence in 0..32 {
            assert_eq!(io_runtime_index(EndpointRole::Server, sequence, 2), 0);
            assert_eq!(io_runtime_index(EndpointRole::Client, sequence, 2), 1);
        }

        let server_indices: Vec<_> = (0..6)
            .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, 4))
            .collect();
        assert_eq!(server_indices, vec![0, 1, 2, 0, 1, 2]);
        assert_eq!(io_runtime_index(EndpointRole::Client, 99, 4), 3);
        assert_eq!(io_runtime_index(EndpointRole::Server, 99, 1), 0);
        assert_eq!(io_runtime_index(EndpointRole::Client, 99, 1), 0);
    }

    #[test]
    fn loopback_initial_mtu_respects_configured_bounds() {
        // Plenty of socket buffer: a typical macOS 8 MiB grant.
        const BIG_BUFFER: usize = 8 * 1024 * 1024;
        // A stock-Linux clamp (net.core.rmem_max ~208 KB, doubled by the
        // kernel): far too little burst headroom for jumbo datagrams.
        const CLAMPED_BUFFER: usize = 416 * 1024;

        // Default config: loopback connections start at the loopback payload
        // size instead of the RFC-safe 1200.
        let config = TransportConfig::default();
        assert_eq!(
            config.loopback_initial_mtu(BIG_BUFFER),
            Some(LOOPBACK_UDP_PAYLOAD)
        );

        // A clamped socket buffer disables the guarantee entirely: 26 jumbo
        // datagrams of headroom overflow under sustained load, and the pinned
        // min_mtu would forbid falling back to smaller datagrams.
        assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), None);

        // A lowered discovery bound caps the loopback start size with it (and
        // shrinks the buffer headroom it needs).
        let config = TransportConfig {
            mtu_discovery_upper_bound: 4096,
            ..TransportConfig::default()
        };
        assert_eq!(config.loopback_initial_mtu(BIG_BUFFER), Some(4096));
        assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), Some(4096));

        // No boost when the configured initial MTU is already at least as big.
        let config = TransportConfig {
            initial_mtu: 16354,
            ..TransportConfig::default()
        };
        assert_eq!(config.loopback_initial_mtu(8 * 1024 * 1024), None);
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
