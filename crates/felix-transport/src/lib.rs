//! The QUIC transport every Felix connection runs over.
//!
//! **Start at [`QuicServer`] and [`QuicClient`].** A server accepts
//! connections and a client makes them; both hand back a [`QuicConnection`],
//! which is what the broker and the client SDK open streams on.
//! [`TransportConfig`] carries the tuning — congestion window, path MTU,
//! socket buffers, stream limits.
//!
//! This crate deliberately knows nothing about Felix messages. Framing and
//! message types live in `felix-wire`; this layer moves bytes and manages
//! connection and stream lifetime, so the protocol can change without
//! touching transport tuning and the reverse.
//!
//! One sizing rule is load-bearing enough to state here: a server endpoint
//! multiplexes every connection and drives traffic both ways, so it gets a
//! runtime to itself while client endpoints share the rest. Putting a client
//! endpoint on the server's runtime measured 5-6x slower, because the two
//! halves of a request/response ping-pong then serialize on one thread. See
//! [`Role`] and [`plan_server_endpoints`].
use anyhow::{Context, Result, anyhow};
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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

/// How many server endpoints this process has said it will bind.
///
/// One unless a process declares otherwise, which is what a single-listener
/// broker and every test binding one server are.
static PLANNED_SERVER_ENDPOINTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1);

/// The I/O runtime pool a process binding `server_endpoints` needs.
///
/// One runtime per server endpoint, because an endpoint's driver is a single
/// task and two endpoints sharing a runtime share its one thread, plus the one
/// reserved for every client endpoint.
///
/// Note this reproduces the historical default exactly: a process with one
/// server endpoint needs 2, which is what macOS defaulted to when a broker
/// could only have one listener.
pub const fn required_io_runtime_threads(server_endpoints: usize) -> usize {
    server_endpoints + 1
}

/// Declare how many server endpoints this process will bind, before it binds
/// the first one.
///
/// The pool is sized from this rather than from a separate setting. The two
/// numbers have to hold a relationship -- a pool smaller than the endpoints
/// using it silently puts several drivers on one thread, which is the ceiling
/// the endpoints were split up to escape -- and a relationship that must hold
/// is not something to leave two knobs free to break.
///
/// Has no effect once the pool exists: it is built on the first endpoint, and
/// tokio runtimes are not resized. Called after that, it warns rather than
/// pretending.
pub fn plan_server_endpoints(count: usize) {
    use std::sync::atomic::Ordering;
    PLANNED_SERVER_ENDPOINTS.store(count.max(1), Ordering::Relaxed);
    if io_runtime_pool_built() {
        tracing::warn!(
            planned = count,
            "plan_server_endpoints called after the I/O runtime pool was built; \
             the pool keeps the size it was created with",
        );
    }
}

/// Built once, on the first endpoint. Module scope so [`plan_server_endpoints`]
/// can tell whether it is already too late to size it.
static IO_RUNTIMES: OnceLock<Vec<tokio::runtime::Runtime>> = OnceLock::new();

fn io_runtime_pool_built() -> bool {
    IO_RUNTIMES.get().is_some()
}

fn io_runtime_handle(role: EndpointRole) -> Option<tokio::runtime::Handle> {
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        // Driver isolation is a macOS optimization; on Linux it is a
        // pessimization, so it is defaulted on only where it measures faster.
        //
        // The ~7.5x per-byte ceiling this pool was built to fix is specific to
        // macOS: on Linux the same benchmark at the pre-fix baseline already
        // sustains ~643 MB/s (628K msg/s x 1 KiB), above what macOS reaches
        // even with every fix applied. Isolating drivers there only adds a
        // cross-thread hop per datagram, and it measures that way — p50
        // latency 86 us -> 152 us and fanout-10 throughput 1.48M -> 1.09M
        // msg/s, consistent across pool sizes 1/2/4/8 and with pump
        // colocation both on and off.
        //
        // `FELIX_IO_RUNTIME_THREADS` overrides on any platform.
        //
        // Sized from the endpoints that will use it, not fixed at 2. A broker
        // binding N client listeners has N+1 server endpoints with the internal
        // one, and a pool of 2 puts every one of their drivers on the same
        // thread -- `io_runtime_index` gives servers `sequence % (pool_len - 1)`,
        // which is `% 1` for a pool of 2. That is exactly the single feeder
        // multiple listeners exist to escape.
        let planned = PLANNED_SERVER_ENDPOINTS.load(Ordering::Relaxed).max(1);
        let default_threads = if cfg!(target_os = "macos") {
            required_io_runtime_threads(planned)
        } else {
            0
        };
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
    // How often to send a keep-alive on an otherwise idle connection.
    //
    // Load-bearing, not a tuning knob. QUIC closes a connection that has been
    // idle for `max_idle_timeout`, and a subscription to a quiet stream is
    // exactly that: the broker sends nothing, the client sends nothing, no
    // packets flow, and the connection dies underneath a subscriber that is
    // still perfectly healthy. Without this, a stream with a 30-second gap
    // between records loses every subscriber.
    //
    // Must stay comfortably below `max_idle_timeout`; quinn only sends these
    // when the connection is otherwise silent, so a busy connection pays
    // nothing.
    pub keep_alive_interval: Option<std::time::Duration>,
    // How long a silent connection survives.
    //
    // Set explicitly rather than inherited so the relationship with
    // `keep_alive_interval` is visible in one place: changing this without
    // changing that is how idle subscriptions start dying again.
    pub max_idle_timeout: Option<std::time::Duration>,
}

// Keep defaults large enough for most dev/test workloads.
const DEFAULT_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_STREAMS: u16 = 1024;
const DEFAULT_RECEIVE_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_STREAM_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;
const DEFAULT_SEND_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_INITIAL_MTU: u16 = 1200;
// Bound MTU discovery below Linux's UDP GSO ceiling, for the same reason
// `LOOPBACK_PINNED_MTU_CAP` exists: a batch is one IP datagram, so
// `mtu * segments <= 65535`, and quinn batches up to 10 -- making 6553 the true
// ceiling. Above it the kernel rejects every batch with `EMSGSIZE`, which quinn
// does not treat as a GSO failure (it falls back only on EIO/EINVAL), so
// delivery stalls and stays stalled.
//
// This bound used to be 16384, which is above that ceiling. Loopback was
// already capped, but a *routed* path was not -- and a jumbo-frame network,
// which is what you buy for throughput, is exactly where discovery climbs past
// 6553 and the stall is permanent. Every perf session set
// `FELIX_MTU_UPPER_BOUND=4096` by hand to avoid it; that is now the default.
//
// 4096 rather than 6553: `MAX_TRANSMIT_SEGMENTS` is private to quinn, so the
// ceiling cannot be derived through its API, and 4096 still holds if the batch
// size rises to 15 where 6553 breaks the moment it moves. It is also the
// fastest configuration measured on Linux (round 18) and converges faster than
// a far-away bound, which never finds a larger path anyway.
//
// macOS has no GSO and no such limit, and 16336 is measured good there over
// hundreds of runs -- the same split `LOOPBACK_PINNED_MTU_CAP` makes.
const DEFAULT_MTU_DISCOVERY_UPPER_BOUND: u16 =
    mtu_discovery_upper_bound_for(cfg!(target_os = "macos"));

/// The default bound, as a function of the platform, so both branches can be
/// tested from either one.
///
/// A `cfg!` expression would make the Linux value unreachable on a macOS
/// developer machine -- and the value that matters is the Linux one, because
/// Linux is where GSO makes it load-bearing. A test that silently passes on the
/// host doing the editing is worth very little.
const fn mtu_discovery_upper_bound_for(macos: bool) -> u16 {
    if macos { 16384 } else { 4096 }
}
const DEFAULT_MAX_UDP_PAYLOAD_SIZE: u16 = 65527;
const DEFAULT_UDP_BUFFER_BYTES: usize = 8 * 1024 * 1024;
// Three keep-alives fit inside the idle window, so a subscription survives two
// lost packets before the connection is declared dead. Both are quinn's own
// idle default and a third of it; what matters is the ratio.
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

fn env_millis(name: &str) -> Option<Duration> {
    Some(Duration::from_millis(env_u64(name)?))
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
            keep_alive_interval: env_millis("FELIX_KEEPALIVE_MS")
                .or(Some(DEFAULT_KEEP_ALIVE_INTERVAL)),
            max_idle_timeout: env_millis("FELIX_MAX_IDLE_TIMEOUT_MS")
                .or(Some(DEFAULT_MAX_IDLE_TIMEOUT)),
        }
    }
}

// Largest UDP payload that fits a 16 KiB loopback interface MTU (macOS lo0 is
// 16384; Linux lo is 65536) after IPv6 headers (48 bytes; IPv4 needs only 28).
const LOOPBACK_UDP_PAYLOAD: u16 = 16336;

// Linux UDP GSO puts a whole `sendmsg` batch in one IP datagram, so
// `mtu * segments <= 65535`; quinn batches up to 10, making 6553 the true
// ceiling. Above it the kernel rejects every batch, with an error quinn does not
// treat as a GSO failure (it only falls back on EIO/EINVAL), so delivery stalls
// and stays stalled. 4096 holds margin to 15 segments and is also Linux's
// fastest. macOS has no GSO and no such limit.
const LOOPBACK_PINNED_MTU_CAP: u16 = if cfg!(target_os = "macos") {
    LOOPBACK_UDP_PAYLOAD
} else {
    4096
};

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
    /// send/receive buffers. It gates the guarantee as a proxy for "this host
    /// was tuned": Linux clamps `SO_RCVBUF` to `net.core.rmem_max` (~208 KB
    /// stock), and an untuned host keeps the RFC-safe path instead.
    fn loopback_initial_mtu(&self, effective_buffer_bytes: usize) -> Option<u16> {
        if env_u64("FELIX_INITIAL_MTU").is_some() {
            return None;
        }
        // The gate asks one question -- was this host tuned? -- so it is
        // measured against the jumbo payload and nothing configurable. Reading
        // it from `target` instead made the answer move with the MTU knobs: when
        // `mtu_discovery_upper_bound` became 4096 by default on Linux, the
        // requirement fell from ~1 MiB to 256 KiB and a stock host (~416 KiB)
        // newly qualified. That is the precise thing the previous comment here
        // said must not happen, and the routed-path change that lowered the
        // bound had no business altering who gets a loopback pin.
        if effective_buffer_bytes
            < usize::from(LOOPBACK_UDP_PAYLOAD.min(self.max_udp_payload_size)).saturating_mul(64)
        {
            return None;
        }
        let target = LOOPBACK_UDP_PAYLOAD
            .min(self.mtu_discovery_upper_bound)
            .min(self.max_udp_payload_size);
        let target = target.min(LOOPBACK_PINNED_MTU_CAP);
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
        if let Some(interval) = self.keep_alive_interval {
            config.keep_alive_interval(Some(interval));
        }
        if let Some(timeout) = self.max_idle_timeout {
            // Falls back to quinn's own default if the value does not fit a
            // VarInt, rather than failing to build a transport over a knob.
            if let Ok(timeout) = timeout.try_into() {
                config.max_idle_timeout(Some(timeout));
            }
        }
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

/// Say so when the OS granted far less socket buffer than was asked for.
///
/// Linux accepts an oversized `SO_RCVBUF`/`SO_SNDBUF` and silently clamps it to
/// `net.core.rmem_max` / `wmem_max`, which ship at around 208 KB. Against the
/// 8 MiB Felix asks for that is a fortieth, and the consequence is not an
/// error: bursts overflow the socket, the drops surface as QUIC retransmits,
/// and throughput is a fraction of what the host can do. Every perf session had
/// to raise these to 26 MiB before any other number meant anything.
///
/// Nothing here can fix it -- the limit belongs to the host -- so the only
/// useful thing is to stop it being silent. Once per endpoint, at `warn`,
/// naming the sysctls: a broker that is quietly at a fortieth of its capacity
/// should not look identical to one that is not.
fn warn_if_udp_buffers_were_clamped(socket: &std::net::UdpSocket, requested: usize) {
    let granted = effective_udp_buffer_bytes(socket);
    // Half is the threshold rather than any shortfall: the bind loop above
    // halves on rejection, so landing one step down is the mechanism working,
    // not the host being untuned.
    if granted == 0 || granted >= requested / 2 {
        return;
    }
    tracing::warn!(
        requested_bytes = requested,
        granted_bytes = granted,
        "the OS granted far less UDP socket buffer than requested; bursts will be \
         dropped at the socket and surface as QUIC retransmits. On Linux raise \
         net.core.rmem_max and net.core.wmem_max (perf sessions use 26 MiB)",
    );
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

    /// The ALPN protocol the handshake settled on, if any.
    ///
    /// `None` means the peer offered no ALPN, which TLS treats as success. An
    /// endpoint that uses ALPN to separate roles must therefore check this
    /// rather than assume the handshake did it — see the broker's internal
    /// listener.
    /// The certificate chain the peer presented, leaf first, when the
    /// endpoint's TLS config asked for one.
    pub fn peer_certificates(&self) -> Option<Vec<rustls::pki_types::CertificateDer<'static>>> {
        self.inner
            .peer_identity()?
            .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
            .ok()
            .map(|certs| *certs)
    }

    pub fn negotiated_protocol(&self) -> Option<Vec<u8>> {
        self.inner
            .handshake_data()?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .ok()?
            .protocol
    }

    /// Resolve when the connection closes, with the reason.
    ///
    /// Lets one task own detection of a lost connection, so every request
    /// waiting on it is failed at the moment it drops rather than at its own
    /// timeout.
    pub async fn closed(&self) -> quinn::ConnectionError {
        self.inner.closed().await
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

    /// The default keep-alive must fit inside the idle window with room to lose
    /// a packet or two.
    ///
    /// These two numbers are a pair. Raising the idle timeout without raising
    /// the keep-alive is harmless; lowering the idle timeout below the
    /// keep-alive silently reintroduces the bug this exists to prevent.
    #[test]
    fn the_keep_alive_fits_inside_the_idle_window() {
        let config = TransportConfig::default();
        let keep_alive = config.keep_alive_interval.expect("a keep-alive by default");
        let idle = config.max_idle_timeout.expect("an idle timeout by default");
        assert!(
            keep_alive * 3 <= idle,
            "keep-alive {keep_alive:?} leaves no margin inside idle timeout {idle:?}",
        );
    }

    /// **A connection with nothing to say must stay up.**
    ///
    /// A subscription to a quiet stream sends nothing in either direction, so
    /// without a keep-alive QUIC closes it on the idle timer and the subscriber
    /// is disconnected while perfectly healthy. Run against a deliberately tiny
    /// idle window so the test is seconds rather than a minute.
    #[tokio::test]
    async fn an_idle_connection_survives_with_a_keep_alive() -> Result<()> {
        let idle = Duration::from_millis(600);
        let transport = TransportConfig {
            max_idle_timeout: Some(idle),
            keep_alive_interval: Some(idle / 4),
            ..TransportConfig::default()
        };

        let (server_config, cert) = make_server_config()?;
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;
        let accepted = tokio::spawn(async move { server.accept().await });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let server_side = accepted.await??;

        // Several idle windows with no traffic at all.
        tokio::time::sleep(idle * 5).await;

        assert!(
            connection.close_reason().is_none(),
            "an idle connection was closed despite a keep-alive: {:?}",
            connection.close_reason(),
        );
        assert!(server_side.close_reason().is_none());
        Ok(())
    }

    /// The same connection without a keep-alive, to show the first test is
    /// asserting something. This is the behaviour every Felix client had.
    #[tokio::test]
    async fn an_idle_connection_dies_without_a_keep_alive() -> Result<()> {
        let idle = Duration::from_millis(600);
        let transport = TransportConfig {
            max_idle_timeout: Some(idle),
            keep_alive_interval: None,
            ..TransportConfig::default()
        };

        let (server_config, cert) = make_server_config()?;
        let server = QuicServer::bind("127.0.0.1:0".parse()?, server_config, transport.clone())?;
        let addr = server.local_addr()?;
        let accepted = tokio::spawn(async move { server.accept().await });

        let client = QuicClient::bind("0.0.0.0:0".parse()?, make_client_config(cert)?, transport)?;
        let connection = client.connect(addr, "localhost").await?;
        let _server_side = accepted.await??;

        tokio::time::sleep(idle * 5).await;

        assert!(
            connection.close_reason().is_some(),
            "expected the idle timeout to close this connection",
        );
        Ok(())
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
    /// Same delivery pattern, but the reader runs on the application runtime
    /// while the connection's drivers run on the dedicated I/O runtime — the
    /// arrangement every broker-side stream reader uses. This is the
    /// configuration that stalls on Linux.
    #[tokio::test]
    async fn many_small_frames_reach_an_app_runtime_reader() -> Result<()> {
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
        // Plain spawn: application runtime, not the connection's I/O runtime.
        tokio::spawn(async move {
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

    /// The pool must give every server endpoint its own runtime, or several
    /// listeners' drivers land on one thread -- the single feeder that binding
    /// several listeners exists to escape.
    #[test]
    fn a_derived_pool_gives_every_server_endpoint_its_own_runtime() {
        for endpoints in 1..=8 {
            let pool = required_io_runtime_threads(endpoints);
            let assigned: std::collections::HashSet<_> = (0..endpoints)
                .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, pool))
                .collect();
            assert_eq!(
                assigned.len(),
                endpoints,
                "{endpoints} endpoints shared runtimes in a pool of {pool}: {assigned:?}",
            );
            // And never the one reserved for clients.
            let client = io_runtime_index(EndpointRole::Client, 0, pool);
            assert!(
                !assigned.contains(&client),
                "a server took the client runtime"
            );
        }
    }

    /// The historical default was right for the broker it was written for: one
    /// server endpoint needs two runtimes. Deriving must not change that.
    #[test]
    fn one_server_endpoint_still_wants_the_historical_pool_of_two() {
        assert_eq!(required_io_runtime_threads(1), 2);
    }

    /// The defect this replaced: a pool of 2 gives every server `% 1`.
    #[test]
    fn a_pool_of_two_collapses_every_listener_onto_one_runtime() {
        let assigned: std::collections::HashSet<_> = (0..4)
            .map(|sequence| io_runtime_index(EndpointRole::Server, sequence, 2))
            .collect();
        assert_eq!(assigned.len(), 1, "expected the documented collapse");
    }

    #[test]
    fn loopback_initial_mtu_respects_configured_bounds() {
        // Plenty of socket buffer: a typical macOS 8 MiB grant.
        const BIG_BUFFER: usize = 8 * 1024 * 1024;
        // A stock-Linux clamp (net.core.rmem_max ~208 KB, doubled by the
        // kernel), i.e. a host nobody has tuned.
        const CLAMPED_BUFFER: usize = 416 * 1024;

        // What the guarantee pins where it applies: the full loopback payload
        // on macOS, the platform cap elsewhere.
        const EXPECTED: u16 = LOOPBACK_PINNED_MTU_CAP;

        // Default config: loopback connections start above the RFC-safe 1200.
        let config = TransportConfig::default();
        assert_eq!(config.loopback_initial_mtu(BIG_BUFFER), Some(EXPECTED));

        // An untuned host keeps the stock path on every platform. The gate is
        // measured against the jumbo payload and nothing configurable, so this
        // answer does not move when the MTU knobs do -- which is what broke
        // when the discovery bound's default dropped to 4096 on Linux and a
        // stock host newly qualified.
        assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), None);

        // A lowered discovery bound caps the loopback start size with it.
        let config = TransportConfig {
            mtu_discovery_upper_bound: 4096,
            ..TransportConfig::default()
        };
        assert_eq!(config.loopback_initial_mtu(BIG_BUFFER), Some(4096));
        // But it does not buy the guarantee on a host that has not been tuned.
        // Asking for a smaller pin is not evidence of headroom, and the pin
        // exists to survive bursts an untuned socket buffer cannot absorb.
        // This case used to answer `Some(4096)`, back when the gate scaled with
        // the requested size; making the gate mean one thing costs it.
        assert_eq!(config.loopback_initial_mtu(CLAMPED_BUFFER), None);

        // No boost when the configured initial MTU is already at least as big.
        let config = TransportConfig {
            initial_mtu: 16354,
            ..TransportConfig::default()
        };
        assert_eq!(config.loopback_initial_mtu(8 * 1024 * 1024), None);
    }

    /// Arithmetic only — the Linux stall this guards needs a sustained batch
    /// run on a tuned host, which nothing in this workspace performs.
    #[test]
    fn loopback_guarantee_is_capped_off_macos() {
        const BIG_BUFFER: usize = 8 * 1024 * 1024;
        let pinned = TransportConfig::default()
            .loopback_initial_mtu(BIG_BUFFER)
            .expect("tuned host takes the loopback path");

        if cfg!(target_os = "macos") {
            assert_eq!(pinned, LOOPBACK_UDP_PAYLOAD);
        } else {
            assert!(
                pinned <= 4096,
                "pinning min_mtu at {pinned} collapses throughput on Linux"
            );
        }
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

#[cfg(test)]
mod gso_ceiling_tests {
    use super::*;

    /// quinn's `MAX_TRANSMIT_SEGMENTS`, which is private to quinn and so cannot
    /// be read through its API. Restated here because the whole bound depends
    /// on it, and a change to it upstream is exactly what would break us.
    const QUINN_MAX_TRANSMIT_SEGMENTS: u32 = 10;
    /// One `sendmsg` batch is one IP datagram, whatever GSO splits it into.
    const IP_DATAGRAM_MAX: u32 = 65535;

    /// **The default MTU bound must keep a GSO batch inside one IP datagram.**
    ///
    /// Above it Linux rejects every batch with `EMSGSIZE`, and quinn falls back
    /// off segmentation only on `EIO`/`EINVAL` -- so the transmit is dropped
    /// after quinn has counted it as sent, and delivery stalls permanently
    /// rather than degrading. The bound was 16384 for a while, which is over
    /// the line; loopback was capped separately but a routed jumbo-frame path
    /// was not.
    ///
    /// The investigation that found this said no test could catch a regression
    /// in the invariant. One can catch the part that matters: that the default
    /// we ship still fits.
    #[test]
    fn the_default_mtu_bound_fits_a_gso_batch() {
        // The non-macOS value specifically, whatever host is running this:
        // macOS has no GSO and no aggregate limit, so its 16336 is fine and
        // would make this vacuous on a developer's machine.
        let bound = mtu_discovery_upper_bound_for(false);
        let aggregate = u32::from(bound) * QUINN_MAX_TRANSMIT_SEGMENTS;
        assert!(
            aggregate <= IP_DATAGRAM_MAX,
            "an MTU of {bound} batches to {aggregate} bytes, \
             over the {IP_DATAGRAM_MAX} an IP datagram holds. Linux answers EMSGSIZE, \
             quinn does not recognise it as a GSO failure, and delivery stalls for good.",
        );
    }

    /// And with margin: the ceiling moves if quinn's batch size does.
    ///
    /// 6553 is the exact limit at 10 segments and breaks the moment that rises.
    /// The margin is the reason the default is 4096 rather than the largest
    /// value that happens to work today.
    #[test]
    fn the_default_mtu_bound_survives_a_larger_batch() {
        let grown = QUINN_MAX_TRANSMIT_SEGMENTS + 5;
        let aggregate = u32::from(mtu_discovery_upper_bound_for(false)) * grown;
        assert!(
            aggregate <= IP_DATAGRAM_MAX,
            "the default has no margin: at {grown} segments it batches to {aggregate} bytes. \
             Pick a bound that survives quinn changing MAX_TRANSMIT_SEGMENTS, because \
             nothing here will notice when it does.",
        );
    }
}

#[cfg(test)]
mod loopback_gate_tests {
    use super::*;

    /// Both platforms, from either platform.
    ///
    /// The regression this guards against was invisible on macOS: the default
    /// discovery bound stays 16384 there, so the gate never moved and the test
    /// that caught it passed locally while failing on Linux CI. Constructing
    /// the config explicitly checks the behaviour that matters wherever this
    /// runs.
    fn config_with(bound: u16) -> TransportConfig {
        TransportConfig {
            mtu_discovery_upper_bound: bound,
            ..TransportConfig::default()
        }
    }

    /// A stock Linux host: `net.core.rmem_max` ~208 KB, doubled by the kernel.
    const UNTUNED: usize = 416 * 1024;
    const TUNED: usize = 8 * 1024 * 1024;

    /// **Lowering the discovery bound must not hand the pin to an untuned
    /// host.**
    ///
    /// The gate is a proxy for "was this host tuned", and a proxy that moves
    /// with an unrelated knob is not one. When 0.5.0 dropped the bound's
    /// default to 4096 on Linux for a *routed*-path hazard, the requirement
    /// fell from ~1 MiB of socket buffer to 256 KiB and every stock Linux host
    /// silently started pinning the loopback MTU.
    #[test]
    fn an_untuned_host_is_refused_whatever_the_discovery_bound_says() {
        for bound in [16384, 4096, 2048] {
            assert_eq!(
                config_with(bound).loopback_initial_mtu(UNTUNED),
                None,
                "an untuned host qualified with the bound at {bound}: the gate \
                 is tracking the MTU knob instead of the host",
            );
        }
    }

    /// And a tuned host still gets it, capped by whichever bound is lower.
    #[test]
    fn a_tuned_host_gets_the_pin_capped_by_the_bound() {
        assert_eq!(
            config_with(4096).loopback_initial_mtu(TUNED),
            Some(4096),
            "a tuned host lost the guarantee",
        );
        assert_eq!(
            config_with(2048).loopback_initial_mtu(TUNED),
            Some(2048),
            "an explicitly lowered bound should still cap the pin",
        );
    }
}
