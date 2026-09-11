//! Configuration for the broker-internal transport.
//!
//! Separate from the client-facing transport on purpose, and every knob here is
//! read from a `FELIX_INTERNAL_*` variable. Sharing a name with the client-side
//! setting would make it possible to widen a peer limit while believing a client
//! limit had been widened.
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

/// The ALPN protocol internal endpoints negotiate.
///
/// The client-facing listener uses no ALPN, so this is what separates the two
/// roles at the TLS layer rather than at the first frame. A connection that
/// reaches the internal listener without it is refused before any broker state
/// is touched.
pub const INTERNAL_ALPN: &[u8] = b"felix-internal/1";

/// How many connections the pool may hold to one peer.
///
/// One is enough for correctness and is the default: a QUIC connection
/// multiplexes streams, so a second buys parallelism across congestion-control
/// state rather than across requests. It exists as a knob because a single
/// connection is also a single loss domain.
const DEFAULT_CONNS_PER_PEER: usize = 1;

/// Multiplexed request streams per connection.
///
/// Requests are spread across these round-robin. More than one because a QUIC
/// stream is ordered: a large forwarded batch would otherwise hold up every
/// smaller request queued behind it on the same stream.
const DEFAULT_STREAMS_PER_CONN: usize = 4;

/// Requests allowed in flight to one peer at a time.
///
/// The bound is what stops an unhealthy peer from consuming this broker: a peer
/// that accepts frames and never answers otherwise accumulates one waiter per
/// forwarded publish for as long as the timeout allows.
const DEFAULT_MAX_INFLIGHT_PER_PEER: usize = 1024;

/// How long a forwarded request waits for its terminal response.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;

/// How long a connection may sit unused before it is closed.
///
/// Rebalancing changes which peers this broker talks to, and a connection to a
/// peer it no longer forwards to is a file descriptor and a keepalive with
/// nothing to do.
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;

/// Reconnect backoff bounds. The first retry is fast because the common cause
/// is a peer restarting, and the ceiling keeps a durably dead peer from being
/// dialled in a tight loop.
const DEFAULT_RECONNECT_BASE_MS: u64 = 50;
const DEFAULT_RECONNECT_MAX_MS: u64 = 5_000;

/// How long the handshake has to complete before the connection is abandoned.
///
/// Deliberately well under the publish quorum timeout, which is also 5s. A peer
/// that is gone does not refuse on every platform -- where the kernel returns no
/// refusal, a dial runs this timeout out instead -- and that dial sits on the
/// critical path of the replication pass, which is what releases a `Quorum`
/// publish. A handshake timeout as long as the publish budget lets one dead
/// replica spend a publish's entire patience before the majority that is up
/// gets to count.
///
/// Still generous for the handshake itself: a broker-internal dial is a round
/// trip on a local network, not seconds.
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 2_000;

/// Broker-internal transport settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerTransportConfig {
    /// Where the internal listener binds. Distinct from `quic_bind`, and
    /// startup refuses if they are equal.
    pub bind: SocketAddr,
    pub conns_per_peer: usize,
    pub streams_per_conn: usize,
    pub max_inflight_per_peer: usize,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,
    pub handshake_timeout: Duration,
}

impl Default for PeerTransportConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:5001".parse().expect("literal address"),
            conns_per_peer: DEFAULT_CONNS_PER_PEER,
            streams_per_conn: DEFAULT_STREAMS_PER_CONN,
            max_inflight_per_peer: DEFAULT_MAX_INFLIGHT_PER_PEER,
            request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            idle_timeout: Duration::from_millis(DEFAULT_IDLE_TIMEOUT_MS),
            reconnect_base: Duration::from_millis(DEFAULT_RECONNECT_BASE_MS),
            reconnect_max: Duration::from_millis(DEFAULT_RECONNECT_MAX_MS),
            handshake_timeout: Duration::from_millis(DEFAULT_HANDSHAKE_TIMEOUT_MS),
        }
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse::<usize>().ok()
}

fn env_millis(name: &str) -> Option<Duration> {
    Some(Duration::from_millis(
        std::env::var(name).ok()?.parse::<u64>().ok()?,
    ))
}

impl PeerTransportConfig {
    /// Read the internal transport settings, validated against the
    /// client-facing bind.
    pub fn from_env(client_bind: SocketAddr) -> std::io::Result<Self> {
        let mut config = Self::default();
        if let Some(bind) = std::env::var("FELIX_INTERNAL_BIND")
            .ok()
            .filter(|v| !v.is_empty())
        {
            config.bind = bind.parse().map_err(|_| {
                std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("FELIX_INTERNAL_BIND is not a valid host:port address: {bind}"),
                )
            })?;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_CONNS_PER_PEER").filter(|v| *v > 0) {
            config.conns_per_peer = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_STREAMS_PER_CONN").filter(|v| *v > 0) {
            config.streams_per_conn = value;
        }
        if let Some(value) = env_usize("FELIX_INTERNAL_MAX_INFLIGHT").filter(|v| *v > 0) {
            config.max_inflight_per_peer = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_REQUEST_TIMEOUT_MS") {
            config.request_timeout = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_IDLE_TIMEOUT_MS") {
            config.idle_timeout = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_RECONNECT_BASE_MS") {
            config.reconnect_base = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_RECONNECT_MAX_MS") {
            config.reconnect_max = value;
        }
        if let Some(value) = env_millis("FELIX_INTERNAL_HANDSHAKE_TIMEOUT_MS") {
            config.handshake_timeout = value;
        }
        config.validate(client_bind)?;
        Ok(config)
    }

    /// Refuse a configuration in which the two roles could be reached at the
    /// same place.
    ///
    /// Sharing a port is not merely a conflicting bind: it would put client
    /// traffic and peer traffic on one listener, which is the separation the
    /// internal protocol exists to keep.
    fn validate(&self, client_bind: SocketAddr) -> std::io::Result<()> {
        if self.bind.port() == client_bind.port() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "FELIX_INTERNAL_BIND ({}) and FELIX_QUIC_BIND ({client_bind}) share a port; \
                     the internal and client-facing listeners must be separate",
                    self.bind
                ),
            ));
        }
        if self.reconnect_base > self.reconnect_max {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "FELIX_INTERNAL_RECONNECT_BASE_MS exceeds FELIX_INTERNAL_RECONNECT_MAX_MS",
            ));
        }
        Ok(())
    }

    /// Transport settings for both internal endpoints.
    ///
    /// How long a peer connection may hear nothing before it is declared dead.
    ///
    /// **Shorter than `request_timeout`, and that ordering is the point.** A
    /// broker that is killed leaves its peers holding connections that look
    /// open: nothing is torn down, because nothing is left to tear them down.
    /// Until QUIC gives up on one, every request sent over it waits out
    /// `request_timeout` in full -- and those requests are not idle bookkeeping.
    /// They are a forwarded publish, and a replication pass whose completion is
    /// what releases a `Quorum` publish. With the idle window longer than the
    /// request timeout, a dead peer costs the whole request timeout every time;
    /// with it shorter, the connection fails first and the pool's backoff takes
    /// over.
    ///
    /// Derived from `request_timeout` rather than chosen, so the two cannot be
    /// tuned apart.
    ///
    /// Three quarters, not half: the window has to be long enough that a broker
    /// merely *starved* is not mistaken for one that is gone. A loaded CI runner
    /// under coverage instrumentation can leave a healthy process unscheduled
    /// for seconds, and closing its peer connections on that basis would make
    /// replication churn exactly when the machine can least afford it. Three
    /// quarters still leaves a quarter of the request's patience to spare.
    pub fn peer_idle_timeout(&self) -> Duration {
        self.request_timeout / 4 * 3
    }

    /// Peer connections are long-lived and can be quiet for long stretches
    /// between rebalances, so they need a keep-alive to survive the idle window
    /// above at all. A fifth of it leaves four chances to be heard from before a
    /// healthy but quiet connection would be closed.
    pub fn quic_transport(&self) -> felix_transport::TransportConfig {
        felix_transport::TransportConfig {
            max_idle_timeout: Some(self.peer_idle_timeout()),
            keep_alive_interval: Some(self.peer_idle_timeout() / 5),
            ..Default::default()
        }
    }

    /// Backoff for attempt `attempt` (0-based), capped and jittered.
    ///
    /// Jitter is not decoration here: every broker in a cluster notices the same
    /// peer restart at the same moment, and an unjittered backoff would have all
    /// of them redial it in step.
    pub fn reconnect_delay(&self, attempt: u32) -> Duration {
        let exponential = self
            .reconnect_base
            .saturating_mul(1u32 << attempt.min(16))
            .min(self.reconnect_max);
        let jitter = fastrand_fraction();
        // Full jitter: uniform over [0, exponential]. Decorrelates redials even
        // when every broker starts its backoff in the same millisecond.
        exponential.mul_f64(jitter)
    }
}

/// A uniform fraction in [0, 1), without pulling in an RNG dependency.
fn fastrand_fraction() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    // Mix so successive calls in the same microsecond do not correlate.
    let mixed = nanos
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(31)
        .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    (mixed >> 11) as f64 / (1u64 << 53) as f64
}
