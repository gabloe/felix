pub mod durable;

pub use durable::DurableStorageConfig;

use anyhow::{Context, Result};
use felix_broker::SubQueuePolicy;
use serde::{Deserialize, Serialize};

/// The config-file spelling of a queue policy, for `--print-config`.
fn queue_policy<S: serde::Serializer>(
    value: &SubQueuePolicy,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    // Named here rather than by deriving on `SubQueuePolicy`, which lives in
    // `felix-broker` and has no serde dependency. A printing feature is not a
    // reason to give a core crate one.
    serializer.serialize_str(match value {
        SubQueuePolicy::DropNew => "drop_new",
        SubQueuePolicy::DropOld => "drop_old",
        SubQueuePolicy::Block => "block",
    })
}

/// Never write a secret into a dump meant to be shared.
///
/// `--print-config` is for pasting into an issue or a ticket, so a credential
/// that appears in it has been published. Shown as a fixed marker rather than
/// omitted: an operator has to be able to see that a token *is* set.
fn redacted<S: serde::Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(if value.is_empty() {
        "<unset>"
    } else {
        "<redacted>"
    })
}
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;

/// Identity this broker claims in the cluster.
///
/// Present only when `FELIX_NODE_ID` is set. Membership is opt-in because a
/// single-node broker has no cluster to join, and registering one would put a
/// node in the catalog that placement would then try to use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MembershipConfig {
    /// Stable across restarts. This is the identity, not the process.
    pub node_id: String,
    /// `host:port` peers reach this broker on. Not the bind address: a broker
    /// bound to 0.0.0.0 has to advertise something routable.
    pub advertise_addr: String,
    /// `host:port` *clients* reach this broker on, if it offers itself as one.
    ///
    /// Optional, and left unset by default. A broker that does not advertise
    /// one is not offered to clients looking for somewhere to connect, which is
    /// the right answer for a broker behind a load balancer whose own address
    /// no client should hold, and the only safe answer for one whose operator
    /// has not said where clients reach it.
    pub client_advertise_addr: Option<String>,
    pub region: String,
    /// Where this broker's refresh token lives, when it has one.
    ///
    /// A path rather than a value, and that is forced by rotation: refreshing
    /// spends the token and mints a replacement, so whatever the broker was
    /// given at startup stops working the first time it refreshes. It has to
    /// write the replacement somewhere it will read on restart, or a restart
    /// presents a spent token — which the control plane correctly reads as a
    /// replay and answers by revoking the whole chain, locking the broker out
    /// for good.
    ///
    /// `None` means no refresh: the broker runs on the token it was given and
    /// falls out of the cluster when that expires, which is the behaviour every
    /// deployment had before refresh existed.
    pub refresh_token_file: Option<std::path::PathBuf>,
    /// Where the *access* token was read from, when it came from a file.
    ///
    /// Two jobs. It is the path re-read when something outside the broker
    /// rotates the credential -- a Vault agent, SPIRE, a sidecar -- so that
    /// rotation takes effect without a restart, the way the refresh token file
    /// already does. And it is what makes an expiring credential legitimate
    /// without `refresh_token_file`: a file is a seam something else can write,
    /// where a token passed by value is not.
    pub node_token_file: Option<std::path::PathBuf>,
}

/// Warn when peers would be told to connect somewhere nothing is listening.
///
/// `NodeSpec.advertise_addr` is the *internal* listener's address, so a broker
/// that advertises a port it does not bind is reachable by the catalog and
/// unreachable in fact. Not fatal: a deployment may map ports, and refusing to
/// start on a legitimate NAT would be worse than saying so.
fn warn_on_unreachable_advertise(
    membership: &MembershipConfig,
    peer: &crate::peer::PeerTransportConfig,
) {
    // Port 0 is an ephemeral bind, so there is nothing to compare against.
    if peer.bind.port() == 0 {
        return;
    }
    let Ok(advertised) = membership.advertise_addr.parse::<SocketAddr>() else {
        return;
    };
    if advertised.port() != peer.bind.port() {
        tracing::warn!(
            advertise_addr = %membership.advertise_addr,
            internal_bind = %peer.bind,
            "FELIX_NODE_ADVERTISE_ADDR names a different port than the internal \
             listener binds; peers will be told to connect where nothing is listening \
             unless the ports are mapped",
        );
    }
}

/// Broker service configuration sourced from environment variables.
///
/// `Serialize` is for `--print-config`, and it is *derived* rather than written
/// out by hand so the dump cannot drift from the struct — a listing that quietly
/// stops mentioning a setting is the same class of problem as a documented
/// variable nothing reads.
#[derive(Debug, Clone, Serialize)]
pub struct BrokerConfig {
    /// QUIC listener bind address. With `quic_listeners > 1` this is the *first*
    /// of a consecutive run of ports; see [`BrokerConfig::quic_binds`].
    pub quic_bind: SocketAddr,
    /// An explicit `FELIX_IO_RUNTIME_THREADS`, when the operator set one.
    ///
    /// Kept so `validate` can see it: the pool size and the listener count must
    /// hold a relationship, and a pool too small for the listeners silently puts
    /// every listener's driver on one thread. Unset means derived.
    pub io_runtime_threads: Option<usize>,
    /// How many client-facing QUIC listeners to bind, on consecutive ports from
    /// `quic_bind`.
    ///
    /// One socket means one `quinn::Endpoint`, and an endpoint's driver is a
    /// single task that reads every inbound datagram and routes it by connection
    /// id. That task cannot use more than one core, and it is what holds a
    /// broker to ~900 MB/s while the rest of the machine idles (#557). Separate
    /// ports are separate sockets, and separate sockets are separate drivers.
    pub quic_listeners: usize,
    /// Metrics HTTP listener bind address.
    pub metrics_bind: SocketAddr,
    /// Optional control-plane base URL.
    pub controlplane_url: Option<String>,
    /// Credential this broker presents to the control plane; empty when none
    /// was given.
    ///
    /// Every control-plane call carries it: the metadata feeds, and -- for a
    /// cluster member -- registration, heartbeat, the assignment watch and
    /// replica reports. A member cannot start without one, since a broker
    /// that will fail every call on a loop is worse than one that refuses. A
    /// standalone broker may run without it, but then its metadata sync is
    /// refused, and it is told so at startup.
    ///
    /// Never printed. `--print-config` exists to be pasted into an issue.
    #[serde(serialize_with = "redacted")]
    pub controlplane_token: String,
    /// Poll interval for control-plane changes.
    pub controlplane_sync_interval_ms: u64,
    /// Cluster membership identity, when this broker joins one.
    pub membership: Option<MembershipConfig>,
    /// Broker-internal transport, present only when this broker joins a cluster.
    /// A broker with no peers has nothing to listen for.
    pub peer_transport: Option<crate::peer::PeerTransportConfig>,
    /// If true, publish acks are sent after commit.
    pub ack_on_commit: bool,
    /// Max frame size accepted on QUIC streams.
    pub max_frame_bytes: usize,
    /// Max time to wait when backpressuring publish enqueue.
    pub publish_queue_wait_timeout_ms: u64,
    /// Max time to wait for ack-on-commit publish completion.
    pub ack_wait_timeout_ms: u64,
    /// How long a consumer group's claim on a record stands before the record
    /// is handed to someone else.
    ///
    /// Too short redelivers work that is still being done; too long leaves a
    /// dead consumer's records stuck for that long. Thirty seconds is a
    /// starting point, not a considered default for any particular workload.
    pub group_visibility_timeout_ms: u64,
    /// Most times a consumer group hands out a record before giving up on it.
    ///
    /// Without a bound a record that always fails is redelivered for ever and
    /// the group never gets past it. A record given up on is recorded as a dead
    /// letter and the group moves on; the record itself stays in the log.
    pub group_max_attempts: u32,
    /// Longest a consumer group's poll may wait for work before answering
    /// empty.
    ///
    /// A cap on what a client asks for, not a default: a client that wants a
    /// shorter wait gets one. It exists so a client cannot hold a broker stream
    /// open indefinitely.
    pub group_max_wait_ms: u64,
    /// Disable timing collection for lower overhead.
    pub disable_timings: bool,
    /// Max time to wait for control-stream writer to drain.
    pub control_stream_drain_timeout_ms: u64,
    /// Total budget for draining in-flight work after SIGTERM/SIGINT before
    /// remaining tasks are force-cancelled.
    pub shutdown_drain_timeout_ms: u64,
    /// How long to keep accepting connections after readiness goes false, so a
    /// load balancer polling `/ready` has time to stop routing here.
    pub shutdown_predrain_ms: u64,
    /// Cache connection flow-control window.
    pub cache_conn_recv_window: u64,
    /// Cache stream flow-control window.
    pub cache_stream_recv_window: u64,
    /// Cache connection send window.
    pub cache_send_window: u64,
    /// Max events per batched subscription frame.
    pub event_batch_max_events: usize,
    /// Max bytes per batched subscription frame.
    pub event_batch_max_bytes: usize,
    /// Max delay before flushing a subscription batch.
    pub event_batch_max_delay_us: u64,
    /// Fanout batch size for subscription sending.
    pub fanout_batch_size: usize,
    /// Publish worker count per QUIC connection.
    pub pub_workers_per_conn: usize,
    /// Durable publishes one worker may have awaiting their device flush at
    /// once. Offsets are still claimed serially, so this does not affect the
    /// order records land in -- it decides how many flushes group commit gets
    /// to coalesce. `1` restores the old behaviour of one flush at a time.
    pub pub_flush_concurrency: usize,
    /// Per-worker publish queue depth.
    pub pub_queue_depth: usize,
    /// Shared in-flight publish byte budget across all publish workers (process-wide).
    pub pub_inflight_bytes: usize,
    /// Per-connection share of the in-flight publish byte budget. Bounds how much of the
    /// process-wide `pub_inflight_bytes` budget a single connection can occupy at once, so one
    /// connection can't starve every other connection's publishes under load.
    pub pub_conn_inflight_bytes: usize,
    /// If true, un-acked publishes wait (bounded) for ingress capacity instead of shedding.
    /// Off by default: fire-and-forget load should shed visibly under overload.
    pub pub_ingress_wait: bool,
    /// Number of core-pinned shard executors owning stream work (0 = disabled).
    /// When enabled, publish workers and subscription lane feeders run on the
    /// shard owning their stream, keeping the per-message path core-local.
    pub core_shards: usize,
    /// Per-subscriber queue capacity in broker core.
    pub subscriber_queue_capacity: usize,
    /// Max concurrent subscriptions a single QUIC connection may hold. Prevents a single
    /// connection from unboundedly growing broker memory via subscriber queues/writer-lane
    /// registrations.
    pub max_subscriptions_per_conn: usize,
    /// Subscriber queue policy for publish->fanout enqueue.
    #[serde(serialize_with = "queue_policy")]
    pub subscriber_queue_policy: SubQueuePolicy,
    /// Number of outbound subscriber writer lanes.
    pub subscriber_writer_lanes: usize,
    /// Bounded queue depth per writer lane.
    pub subscriber_lane_queue_depth: usize,
    /// Queue policy for lane ingress.
    #[serde(serialize_with = "queue_policy")]
    pub subscriber_lane_queue_policy: SubQueuePolicy,
    /// Upper bound to prevent over-sharding lane counts that can regress p99/p999 under load.
    pub max_subscriber_writer_lanes: usize,
    /// Deterministic policy for assigning subscribers to writer lanes.
    pub subscriber_lane_shard: SubscriberLaneShard,
    /// If true, route all subscribers on the same QUIC connection to one writer lane.
    pub subscriber_single_writer_per_conn: bool,
    /// Max queued items drained per lane flush.
    pub subscriber_flush_max_items: usize,
    /// Max time spent waiting for a lane flush fill.
    pub subscriber_flush_max_delay_us: u64,
    /// Upper bound for coalesced bytes per write call.
    pub subscriber_max_bytes_per_write: usize,
    /// Number of delivery streams to use per connection in hashed-pool mode.
    pub sub_streams_per_conn: usize,
    /// Strategy for mapping subscribers to streams.
    pub sub_stream_mode: SubStreamMode,
    /// How long a publish to a `Quorum` stream waits for a majority before the
    /// broker says it cannot vouch for the write.
    pub publish_quorum_timeout_ms: u64,
    /// Halted followers this broker rebuilds at once, across every shard it
    /// leads; zero leaves every halt to an operator. A rebuild is a full
    /// transfer, and every follower of a failed broker at once is an outage.
    pub replication_rebuild_max_concurrent: usize,
    /// Bytes per second a rebuilding follower is shipped at; zero is unlimited.
    pub replication_rebuild_bytes_per_sec: u64,
}

/// A margin over the quorum wait, covering the hop from the publish worker back
/// to the waiter. Small: its only job is to let the inner wait finish first.
const ACK_WAIT_OVER_QUORUM_MS: u64 = 500;

impl BrokerConfig {
    /// How long a publish waits for its acknowledgement before the client is
    /// told it timed out.
    ///
    /// Never shorter than the quorum wait it may be sitting on. A publish to a
    /// `Quorum` stream is entitled to `publish_quorum_timeout_ms` to reach a
    /// majority, so a waiter that gives up sooner reports "publish commit
    /// timeout" for a write the broker is still correctly waiting for -- and
    /// replaces the quorum wait's own answer, which says specifically that this
    /// broker cannot vouch for the write, with one that says nothing about why.
    ///
    /// Raising the ceiling does not slow anything down. It is the point at
    /// which a publish that is already stuck is given up on, not a delay any
    /// successful publish pays.
    pub fn ack_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.ack_wait_timeout_ms.max(
                self.publish_quorum_timeout_ms
                    .saturating_add(ACK_WAIT_OVER_QUORUM_MS),
            ),
        )
    }

    /// How long a forwarded publish may spend before it must answer.
    ///
    /// The same relationship as the quorum wait above, from the other side. A
    /// forward is the other inner wait a publish can be sitting on, and it is
    /// the one with no bound of its own that fits: its budget is up to
    /// `MAX_ATTEMPTS` peer requests, each of which may dial first, so its worst
    /// case runs to tens of seconds while the waiter gives up in a few.
    ///
    /// Raising the ceiling to cover that would make a client wait out the whole
    /// thing, so the forward is bounded to fit under the ceiling instead. What
    /// that buys is the same thing the quorum margin buys: the inner wait
    /// finishes first, so the client is told *why* — the owner was unreachable
    /// and nothing was sent, or the batch went out and its fate is unknown —
    /// rather than getting "publish commit timeout", which says nothing and is
    /// the one answer a client cannot act on.
    ///
    /// Derived rather than configured, so the two cannot be tuned apart.
    pub fn forward_budget(&self) -> std::time::Duration {
        self.ack_wait_timeout()
            .saturating_sub(std::time::Duration::from_millis(ACK_WAIT_OVER_QUORUM_MS))
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            quic_bind: SocketAddr::from(([0, 0, 0, 0], 5000)),
            quic_listeners: 1,
            io_runtime_threads: None,
            metrics_bind: SocketAddr::from(([0, 0, 0, 0], 8080)),
            controlplane_url: None,
            controlplane_token: String::new(),
            controlplane_sync_interval_ms: 2000,
            membership: None,
            peer_transport: None,
            ack_on_commit: false,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            publish_queue_wait_timeout_ms: DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS,
            ack_wait_timeout_ms: DEFAULT_ACK_WAIT_TIMEOUT_MS,
            group_visibility_timeout_ms: DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS,
            group_max_attempts: DEFAULT_GROUP_MAX_ATTEMPTS,
            group_max_wait_ms: DEFAULT_GROUP_MAX_WAIT_MS,
            disable_timings: DEFAULT_DISABLE_TIMINGS,
            control_stream_drain_timeout_ms: DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS,
            shutdown_drain_timeout_ms: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
            shutdown_predrain_ms: DEFAULT_SHUTDOWN_PREDRAIN_MS,
            cache_conn_recv_window: DEFAULT_CACHE_CONN_RECV_WINDOW,
            cache_stream_recv_window: DEFAULT_CACHE_STREAM_RECV_WINDOW,
            cache_send_window: DEFAULT_CACHE_SEND_WINDOW,
            event_batch_max_events: 64,
            event_batch_max_bytes: 64 * 1024,
            event_batch_max_delay_us: DEFAULT_EVENT_BATCH_MAX_DELAY_US,
            fanout_batch_size: 64,
            pub_workers_per_conn: DEFAULT_PUB_WORKERS_PER_CONN,
            pub_flush_concurrency: DEFAULT_PUB_FLUSH_CONCURRENCY,
            pub_queue_depth: DEFAULT_PUB_QUEUE_DEPTH,
            pub_inflight_bytes: DEFAULT_PUB_INFLIGHT_BYTES,
            pub_conn_inflight_bytes: DEFAULT_PUB_CONN_INFLIGHT_BYTES,
            pub_ingress_wait: false,
            core_shards: 0,
            subscriber_queue_capacity: DEFAULT_SUBSCRIBER_QUEUE_CAPACITY,
            max_subscriptions_per_conn: DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN,
            subscriber_queue_policy: DEFAULT_SUBSCRIBER_QUEUE_POLICY,
            subscriber_writer_lanes: DEFAULT_SUBSCRIBER_WRITER_LANES,
            subscriber_lane_queue_depth: DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH,
            subscriber_lane_queue_policy: DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY,
            max_subscriber_writer_lanes: DEFAULT_MAX_SUBSCRIBER_WRITER_LANES,
            subscriber_lane_shard: DEFAULT_SUBSCRIBER_LANE_SHARD,
            subscriber_single_writer_per_conn: false,
            subscriber_flush_max_items: DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS,
            subscriber_flush_max_delay_us: DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US,
            subscriber_max_bytes_per_write: DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE,
            sub_streams_per_conn: DEFAULT_SUB_STREAMS_PER_CONN,
            sub_stream_mode: DEFAULT_SUB_STREAM_MODE,
            publish_quorum_timeout_ms: DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS,
            replication_rebuild_max_concurrent: DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT,
            replication_rebuild_bytes_per_sec: DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberLaneShard {
    // Prefer connection-aware routing when a connection id is known, else fallback to subscriber id.
    Auto,
    SubscriberIdHash,
    ConnectionIdHash,
    // Assign once at subscribe time and keep lane pinned (ordering-safe RR variant).
    RoundRobinPin,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubStreamMode {
    PerSubscriber,
    HashedPool,
}

impl SubStreamMode {
    fn parse_env(value: &str) -> Option<Self> {
        match value {
            "per_subscriber" => Some(Self::PerSubscriber),
            "hashed_pool" => Some(Self::HashedPool),
            _ => None,
        }
    }
}

impl SubscriberLaneShard {
    fn parse_env(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(Self::Auto),
            "subscriber_id_hash" => Some(Self::SubscriberIdHash),
            "connection_id_hash" => Some(Self::ConnectionIdHash),
            "round_robin_pin" => Some(Self::RoundRobinPin),
            _ => None,
        }
    }
}

/// How many client-facing QUIC listeners to bind.
///
/// Defaults to 1, which is exactly today's behaviour: one socket, one endpoint
/// driver, one port. Raising it trades a wider port range for a receive path
/// that is no longer one task on one core.
///
/// Refused rather than clamped when the range would wrap past port 65535: a
/// broker that silently bound fewer listeners than asked would read as the
/// feature not working, and the operator has a port number to fix.
fn quic_listeners_from_env(quic_bind: SocketAddr) -> Result<usize> {
    let Some(raw) = std::env::var("FELIX_QUIC_LISTENERS")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(1);
    };
    let count: usize = raw
        .trim()
        .parse()
        .with_context(|| format!("parse FELIX_QUIC_LISTENERS: {raw}"))?;
    if count == 0 {
        anyhow::bail!("FELIX_QUIC_LISTENERS is 0; a broker with no listener serves nothing");
    }
    let last = u32::from(quic_bind.port()) + count as u32 - 1;
    if last > u32::from(u16::MAX) {
        anyhow::bail!(
            "FELIX_QUIC_LISTENERS ({count}) from FELIX_QUIC_BIND port {} runs past 65535",
            quic_bind.port(),
        );
    }
    Ok(count)
}

/// The control-plane credential, from `FELIX_NODE_TOKEN_FILE` or
/// `FELIX_NODE_TOKEN`; empty when neither is set.
///
/// The file form exists so a token can arrive as a mounted secret rather than
/// an environment variable visible in a process listing. Whitespace is trimmed,
/// and a blank value is treated as no credential rather than as an empty one.
fn controlplane_token_from_env() -> std::io::Result<String> {
    match std::env::var("FELIX_NODE_TOKEN_FILE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        Some(path) => Ok(std::fs::read_to_string(&path)
            .map_err(|err| {
                std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("read FELIX_NODE_TOKEN_FILE {path}: {err}"),
                )
            })?
            .trim()
            .to_string()),
        None => Ok(std::env::var("FELIX_NODE_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .unwrap_or_default()),
    }
}

/// Read the cluster identity, or `None` when this broker is not joining one.
///
/// Fails rather than defaults on a half-configured identity. A broker that
/// guessed its own advertised address would register something unreachable, and
/// the failure would surface later as peers unable to connect to a node the
/// catalog says is live.
fn membership_from_env(
    controlplane_url: &Option<String>,
    controlplane_token: &str,
) -> std::io::Result<Option<MembershipConfig>> {
    let Some(node_id) = std::env::var("FELIX_NODE_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    let advertise_addr = std::env::var("FELIX_NODE_ADVERTISE_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "FELIX_NODE_ID is set but FELIX_NODE_ADVERTISE_ADDR is not; \
                 a broker cannot advertise an address it has to guess",
            )
        })?;

    // Parsed here so a malformed address fails at startup rather than as a
    // rejected registration once everything else is already running.
    if advertise_addr.parse::<SocketAddr>().is_err() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("FELIX_NODE_ADVERTISE_ADDR is not a valid host:port address: {advertise_addr}"),
        ));
    }

    if controlplane_url.is_none() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "FELIX_NODE_ID is set but FELIX_CONTROLPLANE_URL is not; \
             there is nowhere to register",
        ));
    }

    if controlplane_token.is_empty() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "FELIX_NODE_ID is set but no node credential was provided; \
             set FELIX_NODE_TOKEN or FELIX_NODE_TOKEN_FILE",
        ));
    }

    // A value, not a path — which cannot work, so say why rather than accept
    // it and lock the broker out at its first restart.
    if std::env::var("FELIX_NODE_REFRESH_TOKEN")
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "FELIX_NODE_REFRESH_TOKEN is set, but a refresh token cannot be \
             passed by value: refreshing spends it and mints a replacement, so \
             the broker has to write that replacement back somewhere. Use \
             FELIX_NODE_REFRESH_TOKEN_FILE and make the path writable.",
        ));
    }
    let refresh_token_file = std::env::var("FELIX_NODE_REFRESH_TOKEN_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from);

    let node_token_file = std::env::var("FELIX_NODE_TOKEN_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from);

    Ok(Some(MembershipConfig {
        node_id,
        refresh_token_file,
        node_token_file,
        advertise_addr,
        client_advertise_addr: std::env::var("FELIX_CLIENT_ADVERTISE_ADDR")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        region: std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string()),
    }))
}

fn parse_sub_queue_policy(value: &str) -> Option<SubQueuePolicy> {
    match value {
        "block" => Some(SubQueuePolicy::Block),
        "drop_new" => Some(SubQueuePolicy::DropNew),
        "drop_old" => Some(SubQueuePolicy::DropOld),
        _ => None,
    }
}

const DEFAULT_BROKER_CONFIG_PATH: &str = "/usr/local/felix/config.yml";
const DEFAULT_EVENT_BATCH_MAX_DELAY_US: u64 = 250;
const DEFAULT_DISABLE_TIMINGS: bool = false;
const DEFAULT_CACHE_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_CACHE_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS: u64 = 2000;
/// How long a publish to a `Quorum` stream waits for a majority.
///
/// Generous next to a healthy replication round trip, because being too short
/// costs a publish the broker cannot vouch for even though the record is on its
/// disk and about to reach a majority.
const DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS: u64 = 5_000;
// One at a time: the cautious reading of "under a policy".
const DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT: usize = 1;
const DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC: u64 = 0;
const DEFAULT_ACK_WAIT_TIMEOUT_MS: u64 = 2000;
/// Thirty seconds. Long enough that ordinary work finishes inside it, short
/// enough that a dead consumer does not hold its records for minutes.
const DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS: u64 = 30_000;

/// Five deliveries. Enough that a transient failure is retried through, few
/// enough that a record that will never succeed is set aside quickly.
const DEFAULT_GROUP_MAX_ATTEMPTS: u32 = 5;

/// Thirty seconds. Long enough that an idle consumer wakes rarely, short enough
/// that a client notices a broker that has stopped answering.
const DEFAULT_GROUP_MAX_WAIT_MS: u64 = 30_000;
const DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS: u64 = 50;
// Total budget for draining in-flight work after a termination signal. Kubernetes
// defaults `terminationGracePeriodSeconds` to 30, and it sends SIGKILL once that
// expires, so the default leaves headroom to finish the drain, log the outcome, and
// exit before being killed. Deployments that raise the grace period should raise
// this to match.
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 25_000;
// Off by default, unlike the control plane's. Kubernetes removes a terminating
// pod from its endpoints without consulting readiness, and the chart's preStop
// sleep covers that; a hold-off here would stack on top of it. Set it for a load
// balancer that learns about draining only by polling `/ready`.
const DEFAULT_SHUTDOWN_PREDRAIN_MS: u64 = 0;
const DEFAULT_PUB_WORKERS_PER_CONN: usize = 4;
// Enough to keep a device flush busy with company without letting a burst put
// unbounded concurrent callers into shared broker state. `sync_batch_appends`
// is the number to watch: the budget in docs/storage-performance.md is >= 8.
const DEFAULT_PUB_FLUSH_CONCURRENCY: usize = 32;
const DEFAULT_PUB_QUEUE_DEPTH: usize = 64;
const DEFAULT_PUB_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PUB_CONN_INFLIGHT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 512;
const DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN: usize = 4096;
const DEFAULT_SUBSCRIBER_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
const DEFAULT_SUBSCRIBER_WRITER_LANES: usize = 4;
const DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH: usize = 64;
const DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
const DEFAULT_MAX_SUBSCRIBER_WRITER_LANES: usize = 8;
const DEFAULT_SUBSCRIBER_LANE_SHARD: SubscriberLaneShard = SubscriberLaneShard::Auto;
const DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS: usize = 16;
const DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US: u64 = 50;
const DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE: usize = 64 * 1024;
const DEFAULT_SUB_STREAMS_PER_CONN: usize = 4;
const DEFAULT_SUB_STREAM_MODE: SubStreamMode = SubStreamMode::PerSubscriber;

/// The settings a config file may override.
///
/// `deny_unknown_fields` because a key nobody reads is a lie: an operator who
/// writes `metrics_bnid` gets the default, no error, and a broker listening
/// somewhere they did not ask for. The same reasoning as the wire protocol's
/// unknown flag bits — a thing not understood is refused, never ignored.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfigOverride {
    quic_bind: Option<String>,
    metrics_bind: Option<String>,
    controlplane_url: Option<String>,
    controlplane_sync_interval_ms: Option<u64>,
    ack_on_commit: Option<bool>,
    max_frame_bytes: Option<usize>,
    publish_queue_wait_timeout_ms: Option<u64>,
    ack_wait_timeout_ms: Option<u64>,
    disable_timings: Option<bool>,
    control_stream_drain_timeout_ms: Option<u64>,
    shutdown_drain_timeout_ms: Option<u64>,
    shutdown_predrain_ms: Option<u64>,
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    event_batch_max_events: Option<usize>,
    event_batch_max_bytes: Option<usize>,
    event_batch_max_delay_us: Option<u64>,
    fanout_batch_size: Option<usize>,
    pub_workers_per_conn: Option<usize>,
    pub_queue_depth: Option<usize>,
    pub_inflight_bytes: Option<usize>,
    pub_conn_inflight_bytes: Option<usize>,
    pub_ingress_wait: Option<bool>,
    core_shards: Option<usize>,
    subscriber_queue_capacity: Option<usize>,
    max_subscriptions_per_conn: Option<usize>,
    subscriber_queue_policy: Option<String>,
    subscriber_writer_lanes: Option<usize>,
    subscriber_lane_queue_depth: Option<usize>,
    subscriber_lane_queue_policy: Option<String>,
    max_subscriber_writer_lanes: Option<usize>,
    subscriber_lane_shard: Option<SubscriberLaneShard>,
    subscriber_single_writer_per_conn: Option<bool>,
    subscriber_flush_max_items: Option<usize>,
    subscriber_flush_max_delay_us: Option<u64>,
    subscriber_max_bytes_per_write: Option<usize>,
    sub_streams_per_conn: Option<usize>,
    sub_stream_mode: Option<SubStreamMode>,
}

impl BrokerConfig {
    /// Every client-facing listener address, in bind order.
    ///
    /// Consecutive ports from `quic_bind`. The first is the one an existing
    /// deployment already knows, so a broker with the default single listener
    /// binds exactly what it always did.
    pub fn quic_binds(&self) -> Vec<SocketAddr> {
        (0..self.quic_listeners)
            .map(|offset| {
                let mut addr = self.quic_bind;
                addr.set_port(self.quic_bind.port() + offset as u16);
                addr
            })
            .collect()
    }

    pub fn from_env() -> Result<Self> {
        // Environment variables provide defaults for local development.
        let metrics_bind = std::env::var("FELIX_BROKER_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_BROKER_METRICS_BIND")?;
        let quic_bind: SocketAddr = std::env::var("FELIX_QUIC_BIND")
            .unwrap_or_else(|_| "0.0.0.0:5000".to_string())
            .parse()
            .with_context(|| "parse FELIX_QUIC_BIND")?;
        let quic_listeners = quic_listeners_from_env(quic_bind)?;
        let io_runtime_threads = std::env::var("FELIX_IO_RUNTIME_THREADS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .trim()
                    .parse::<usize>()
                    .with_context(|| format!("parse FELIX_IO_RUNTIME_THREADS: {value}"))
            })
            .transpose()?;
        let controlplane_url = std::env::var("FELIX_CONTROLPLANE_URL").ok();
        // Poll every 2s by default.
        let controlplane_sync_interval_ms = std::env::var("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2000);
        let controlplane_token = controlplane_token_from_env()?;
        let membership = membership_from_env(&controlplane_url, &controlplane_token)?;
        let peer_transport = match &membership {
            Some(membership) => {
                let peer = crate::peer::PeerTransportConfig::from_env(quic_bind, quic_listeners)?;
                warn_on_unreachable_advertise(membership, &peer);
                // Under mTLS the node id is the name on the certificate, and a
                // dialler verifies a peer's certificate against the node id it
                // means to reach -- so it has to be a name a certificate can
                // carry. Refused here rather than at the first dial.
                if peer.tls.is_some()
                    && rustls::pki_types::DnsName::try_from(membership.node_id.as_str()).is_err()
                {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "FELIX_NODE_ID {:?} is not a valid DNS name; with peer mTLS the node \
                             id is the certificate's DNS name, so no label may start or end \
                             with '-' or be empty",
                            membership.node_id
                        ),
                    )
                    .into());
                }
                Some(peer)
            }
            None => None,
        };
        let ack_on_commit = std::env::var("FELIX_ACK_ON_COMMIT")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let max_frame_bytes = std::env::var("FELIX_MAX_FRAME_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);
        let publish_queue_wait_timeout_ms = std::env::var("FELIX_PUBLISH_QUEUE_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS);
        let ack_wait_timeout_ms = std::env::var("FELIX_ACK_WAIT_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_ACK_WAIT_TIMEOUT_MS);
        let group_visibility_timeout_ms = std::env::var("FELIX_GROUP_VISIBILITY_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_VISIBILITY_TIMEOUT_MS);
        let group_max_attempts = std::env::var("FELIX_GROUP_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_GROUP_MAX_ATTEMPTS);
        let group_max_wait_ms = std::env::var("FELIX_GROUP_MAX_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_GROUP_MAX_WAIT_MS);
        let disable_timings = std::env::var("FELIX_DISABLE_TIMINGS")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_DISABLE_TIMINGS);
        let control_stream_drain_timeout_ms =
            std::env::var("FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS);
        let shutdown_drain_timeout_ms = std::env::var("FELIX_SHUTDOWN_DRAIN_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS);
        // Zero is meaningful here: no hold-off.
        let shutdown_predrain_ms = std::env::var("FELIX_SHUTDOWN_PREDRAIN_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_PREDRAIN_MS);
        let cache_conn_recv_window = std::env::var("FELIX_CACHE_CONN_RECV_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_CONN_RECV_WINDOW);
        let cache_stream_recv_window = std::env::var("FELIX_CACHE_STREAM_RECV_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_STREAM_RECV_WINDOW);
        let cache_send_window = std::env::var("FELIX_CACHE_SEND_WINDOW")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CACHE_SEND_WINDOW);
        let event_batch_max_events = std::env::var("FELIX_EVENT_BATCH_MAX_EVENTS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let event_batch_max_bytes = std::env::var("FELIX_EVENT_BATCH_MAX_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64 * 1024);
        let event_batch_max_delay_us = std::env::var("FELIX_EVENT_BATCH_MAX_DELAY_US")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_EVENT_BATCH_MAX_DELAY_US);
        let fanout_batch_size = std::env::var("FELIX_FANOUT_BATCH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64);
        let pub_workers_per_conn = std::env::var("FELIX_BROKER_PUB_WORKERS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_WORKERS_PER_CONN);
        let pub_flush_concurrency = std::env::var("FELIX_BROKER_PUB_FLUSH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_PUB_FLUSH_CONCURRENCY);
        let pub_queue_depth = std::env::var("FELIX_BROKER_PUB_QUEUE_DEPTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_QUEUE_DEPTH);
        let pub_inflight_bytes = std::env::var("FELIX_BROKER_PUBLISH_INFLIGHT_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_INFLIGHT_BYTES);
        let pub_conn_inflight_bytes = std::env::var("FELIX_BROKER_PUBLISH_CONN_INFLIGHT_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_CONN_INFLIGHT_BYTES);
        let pub_ingress_wait = std::env::var("FELIX_PUB_INGRESS_WAIT")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let core_shards = std::env::var("FELIX_CORE_SHARDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let subscriber_queue_capacity = std::env::var("FELIX_SUBSCRIBER_QUEUE_CAPACITY")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_QUEUE_CAPACITY").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_CAPACITY);
        let max_subscriptions_per_conn = std::env::var("FELIX_MAX_SUBSCRIPTIONS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_SUBSCRIPTIONS_PER_CONN);
        let subscriber_queue_policy = std::env::var("FELIX_SUB_QUEUE_POLICY")
            .ok()
            .and_then(|value| parse_sub_queue_policy(&value))
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_POLICY);
        let subscriber_writer_lanes = std::env::var("FELIX_SUB_EGRESS_LANES")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_WRITER_LANES").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_WRITER_LANES);
        let subscriber_lane_queue_depth = std::env::var("FELIX_SUB_QUEUE_BOUND")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_DEPTH").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH);
        let subscriber_lane_queue_policy = std::env::var("FELIX_SUB_QUEUE_MODE")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_LANE_QUEUE_POLICY").ok())
            .and_then(|value| parse_sub_queue_policy(&value))
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY);
        let max_subscriber_writer_lanes = std::env::var("FELIX_MAX_SUB_WRITER_LANES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_SUBSCRIBER_WRITER_LANES);
        let subscriber_lane_shard = std::env::var("FELIX_SUB_LANE_SHARD")
            .ok()
            .and_then(|value| SubscriberLaneShard::parse_env(value.as_str()))
            .unwrap_or(DEFAULT_SUBSCRIBER_LANE_SHARD);
        let subscriber_single_writer_per_conn = std::env::var("FELIX_SUB_SINGLE_WRITER_PER_CONN")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let subscriber_flush_max_items = std::env::var("FELIX_SUB_FLUSH_MAX_ITEMS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS);
        let subscriber_flush_max_delay_us = std::env::var("FELIX_SUB_FLUSH_MAX_DELAY_US")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US);
        let subscriber_max_bytes_per_write = std::env::var("FELIX_SUB_MAX_BYTES_PER_WRITE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE);
        let sub_streams_per_conn = std::env::var("FELIX_SUB_STREAMS_PER_CONN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUB_STREAMS_PER_CONN);
        let publish_quorum_timeout_ms = std::env::var("FELIX_PUBLISH_QUORUM_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUBLISH_QUORUM_TIMEOUT_MS);
        let sub_stream_mode = std::env::var("FELIX_SUB_STREAM_MODE")
            .ok()
            .and_then(|value| SubStreamMode::parse_env(value.as_str()))
            .unwrap_or(DEFAULT_SUB_STREAM_MODE);
        let replication_rebuild_max_concurrent =
            std::env::var("FELIX_REPLICATION_REBUILD_MAX_CONCURRENT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(DEFAULT_REPLICATION_REBUILD_MAX_CONCURRENT);
        let replication_rebuild_bytes_per_sec =
            std::env::var("FELIX_REPLICATION_REBUILD_BYTES_PER_SEC")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(DEFAULT_REPLICATION_REBUILD_BYTES_PER_SEC);
        Ok(Self {
            quic_bind,
            quic_listeners,
            io_runtime_threads,
            metrics_bind,
            controlplane_url,
            controlplane_token,
            controlplane_sync_interval_ms,
            membership,
            peer_transport,
            ack_on_commit,
            max_frame_bytes,
            publish_queue_wait_timeout_ms,
            ack_wait_timeout_ms,
            group_visibility_timeout_ms,
            group_max_attempts,
            group_max_wait_ms,
            disable_timings,
            control_stream_drain_timeout_ms,
            shutdown_drain_timeout_ms,
            shutdown_predrain_ms,
            cache_conn_recv_window,
            cache_stream_recv_window,
            cache_send_window,
            event_batch_max_events,
            event_batch_max_bytes,
            event_batch_max_delay_us,
            fanout_batch_size,
            pub_workers_per_conn,
            pub_flush_concurrency,
            pub_queue_depth,
            pub_inflight_bytes,
            pub_conn_inflight_bytes,
            pub_ingress_wait,
            core_shards,
            subscriber_queue_capacity,
            max_subscriptions_per_conn,
            subscriber_queue_policy,
            subscriber_writer_lanes,
            subscriber_lane_queue_depth,
            subscriber_lane_queue_policy,
            max_subscriber_writer_lanes,
            subscriber_lane_shard,
            subscriber_single_writer_per_conn,
            subscriber_flush_max_items,
            subscriber_flush_max_delay_us,
            subscriber_max_bytes_per_write,
            sub_streams_per_conn,
            sub_stream_mode,
            publish_quorum_timeout_ms,
            replication_rebuild_max_concurrent,
            replication_rebuild_bytes_per_sec,
        })
    }

    pub fn from_env_or_yaml() -> Result<Self> {
        let mut config = Self::from_env()?;
        let override_path = std::env::var("FELIX_BROKER_CONFIG").ok();
        let config_path = override_path
            .clone()
            .unwrap_or_else(|| DEFAULT_BROKER_CONFIG_PATH.to_string());
        let contents = match fs::read_to_string(&config_path) {
            Ok(contents) => Some(contents),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if override_path.is_some() {
                    return Err(err)
                        .with_context(|| format!("read FELIX_BROKER_CONFIG: {config_path}"));
                }
                None
            }
            Err(err) => {
                if override_path.is_some() {
                    return Err(err)
                        .with_context(|| format!("read FELIX_BROKER_CONFIG: {config_path}"));
                }
                return Err(err).with_context(|| format!("read broker config: {config_path}"));
            }
        };
        if let Some(contents) = contents {
            // YAML overrides allow ops-friendly config files.
            let override_cfg: BrokerConfigOverride =
                serde_yaml_ng::from_str(&contents).with_context(|| "parse broker config yaml")?;
            config.apply(override_cfg)?;
        }
        // After both sources, because a combination is only wrong once it is
        // whole: an override may fix what the environment set, or break what it
        // had right.
        config.validate()?;
        Ok(config)
    }

    /// Settings that are each fine alone and wrong together.
    ///
    /// Every knob validates its own value where it is parsed. Nothing looked at
    /// *pairs*, which is where the confusing failures live — a setting that
    /// never takes effect, or one that produces output the other end will not
    /// accept. Neither shows up as an error at the time; both show up later as
    /// behaviour nobody configured (#416).
    ///
    /// Refusing at startup is the same choice the peer transport already makes
    /// for a shared port: a broker that will not do what its configuration says
    /// should say so while someone is watching.
    pub fn validate(&self) -> Result<()> {
        if self.event_batch_max_bytes > self.max_frame_bytes {
            anyhow::bail!(
                "event_batch_max_bytes ({}) exceeds max_frame_bytes ({}): this \
                 broker would send subscribers frames larger than it will itself \
                 accept, and a client applying the same limit drops them",
                self.event_batch_max_bytes,
                self.max_frame_bytes,
            );
        }
        if self.pub_conn_inflight_bytes > self.pub_inflight_bytes {
            anyhow::bail!(
                "pub_conn_inflight_bytes ({}) exceeds pub_inflight_bytes ({}): \
                 the per-connection limit can never be the one that applies, so \
                 one connection may take the whole broker-wide allowance",
                self.pub_conn_inflight_bytes,
                self.pub_inflight_bytes,
            );
        }
        if self.cache_stream_recv_window > self.cache_conn_recv_window {
            anyhow::bail!(
                "cache_stream_recv_window ({}) exceeds cache_conn_recv_window \
                 ({}): a single stream can never reach its own window, because \
                 the connection's runs out first",
                self.cache_stream_recv_window,
                self.cache_conn_recv_window,
            );
        }
        self.validate_io_runtime_covers_every_listener()?;
        self.validate_credential_can_outlive_itself()?;
        Ok(())
    }

    /// Refuse an I/O runtime pool too small for the listeners that will use it.
    ///
    /// An endpoint's driver is a single task, so two endpoints on one runtime
    /// share its one thread. `io_runtime_index` gives a server endpoint
    /// `sequence % (pool_len - 1)`, which for a pool of 2 is `% 1` -- every
    /// listener on the same thread, which is the single feeder that binding
    /// several listeners exists to escape.
    ///
    /// Each setting is fine alone. `FELIX_IO_RUNTIME_THREADS=1` isolates a
    /// driver; `FELIX_QUIC_LISTENERS=4` asks for four. Together they quietly
    /// collapse the four onto one, and the only symptom is throughput that does
    /// not improve -- which reads as the listeners being pointless rather than
    /// as a misconfiguration.
    ///
    /// Unset is not a conflict: the pool is derived from the listener count, so
    /// there is nothing to disagree with.
    fn validate_io_runtime_covers_every_listener(&self) -> Result<()> {
        let Some(configured) = self.io_runtime_threads else {
            return Ok(());
        };
        // Zero is the documented way to turn the pool off entirely and put
        // drivers back on the app runtime, where tokio spreads them. That is
        // the Linux default and not a conflict.
        if configured == 0 {
            return Ok(());
        }
        let needed = felix_transport::required_io_runtime_threads(self.server_endpoints());
        if configured < needed {
            anyhow::bail!(
                "FELIX_IO_RUNTIME_THREADS ({configured}) is too small for \
                 FELIX_QUIC_LISTENERS ({}): this broker binds {} server \
                 endpoints and every driver past the first would share a thread \
                 with another, which is the single feeder several listeners \
                 exist to escape. Use {needed}, or 0 to put drivers on the app \
                 runtime",
                self.quic_listeners,
                self.server_endpoints(),
            );
        }
        Ok(())
    }

    /// Server endpoints this broker binds: the client listeners, plus the
    /// internal one when it is part of a cluster.
    ///
    /// The internal listener carries replication, so it is a driver like any
    /// other and needs its own runtime rather than sharing a client listener's.
    pub fn server_endpoints(&self) -> usize {
        self.quic_listeners + usize::from(self.peer_transport.is_some())
    }

    /// Refuse a credential that will expire with nothing able to renew it.
    ///
    /// The broker's control-plane calls all read one token, and the heartbeat is
    /// among them -- and the heartbeat *is* the lease renewal. So an expired
    /// credential is not a degraded broker, it is one that stops serving the
    /// shards it led once the lease lapses. That is the correct, safe outcome;
    /// what is not correct is finding out about it an hour into a deployment
    /// nobody touched.
    ///
    /// Two things can keep a token alive: the refresh loop
    /// (`FELIX_NODE_REFRESH_TOKEN_FILE`), or something outside the broker
    /// rewriting `FELIX_NODE_TOKEN_FILE`, which is re-read. With neither, and a
    /// token that says when it expires, the outage is already scheduled.
    ///
    /// A token passed by value is not a seam anything can write, which is why
    /// the file is what counts rather than merely having a token.
    fn validate_credential_can_outlive_itself(&self) -> Result<()> {
        let Some(membership) = self.membership.as_ref() else {
            // Not joining a cluster: no heartbeat, no lease, nothing to lose.
            return Ok(());
        };
        if membership.refresh_token_file.is_some() || membership.node_token_file.is_some() {
            return Ok(());
        }
        // Only a token that says when it expires. One this broker cannot read
        // the claims of is someone else's format, and guessing is worse than
        // letting it run.
        let Some(expires_at) = crate::cluster::credential::read_claims(&self.controlplane_token)
            .map(|claims| claims.exp)
        else {
            return Ok(());
        };
        anyhow::bail!(
            "the node credential expires (exp {expires_at}) and nothing can renew it: \
             FELIX_NODE_TOKEN was passed by value, and neither \
             FELIX_NODE_REFRESH_TOKEN_FILE nor FELIX_NODE_TOKEN_FILE is set. The \
             heartbeat carries this token and the heartbeat is the lease renewal, so \
             when it expires this broker stops serving the shards it leads. Set \
             FELIX_NODE_REFRESH_TOKEN_FILE to refresh it, or write the token to \
             FELIX_NODE_TOKEN_FILE and have whatever mints it rewrite that path -- \
             the broker re-reads it.",
        );
    }

    /// Fold a parsed config file over the values already taken from the
    /// environment.
    ///
    /// Separate from the read so the precedence rules are testable without a
    /// file on disk and the process environment standing in for one.
    fn apply(&mut self, override_cfg: BrokerConfigOverride) -> Result<()> {
        let config = self;
        if let Some(value) = override_cfg.quic_bind {
            config.quic_bind = value.parse().with_context(|| "parse quic_bind")?;
        }
        if let Some(value) = override_cfg.metrics_bind {
            config.metrics_bind = value.parse().with_context(|| "parse metrics_bind")?;
        }
        if let Some(value) = override_cfg.controlplane_url {
            config.controlplane_url = Some(value);
        }
        if let Some(value) = override_cfg.controlplane_sync_interval_ms {
            config.controlplane_sync_interval_ms = value;
        }
        if let Some(value) = override_cfg.ack_on_commit {
            config.ack_on_commit = value;
        }
        if let Some(value) = override_cfg.max_frame_bytes {
            config.max_frame_bytes = value;
        }
        if let Some(value) = override_cfg.publish_queue_wait_timeout_ms {
            config.publish_queue_wait_timeout_ms = value;
        }
        if let Some(value) = override_cfg.ack_wait_timeout_ms {
            config.ack_wait_timeout_ms = value;
        }
        if let Some(value) = override_cfg.disable_timings {
            config.disable_timings = value;
        }
        if let Some(value) = override_cfg.control_stream_drain_timeout_ms {
            config.control_stream_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_drain_timeout_ms
            && value > 0
        {
            config.shutdown_drain_timeout_ms = value;
        }
        if let Some(value) = override_cfg.shutdown_predrain_ms {
            config.shutdown_predrain_ms = value;
        }
        if let Some(value) = override_cfg.cache_conn_recv_window
            && value > 0
        {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = override_cfg.cache_stream_recv_window
            && value > 0
        {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = override_cfg.cache_send_window
            && value > 0
        {
            config.cache_send_window = value;
        }
        if let Some(value) = override_cfg.event_batch_max_events
            && value > 0
        {
            config.event_batch_max_events = value;
        }
        if let Some(value) = override_cfg.event_batch_max_bytes
            && value > 0
        {
            config.event_batch_max_bytes = value;
        }
        if let Some(value) = override_cfg.event_batch_max_delay_us {
            config.event_batch_max_delay_us = value;
        }
        if let Some(value) = override_cfg.fanout_batch_size
            && value > 0
        {
            config.fanout_batch_size = value;
        }
        if let Some(value) = override_cfg.pub_workers_per_conn
            && value > 0
        {
            config.pub_workers_per_conn = value;
        }
        if let Some(value) = override_cfg.pub_queue_depth
            && value > 0
        {
            config.pub_queue_depth = value;
        }
        if let Some(value) = override_cfg.pub_inflight_bytes
            && value > 0
        {
            config.pub_inflight_bytes = value;
        }
        if let Some(value) = override_cfg.pub_conn_inflight_bytes
            && value > 0
        {
            config.pub_conn_inflight_bytes = value;
        }
        if let Some(value) = override_cfg.pub_ingress_wait {
            config.pub_ingress_wait = value;
        }
        if let Some(value) = override_cfg.core_shards {
            config.core_shards = value;
        }
        if let Some(value) = override_cfg.subscriber_queue_capacity
            && value > 0
        {
            config.subscriber_queue_capacity = value;
        }
        if let Some(value) = override_cfg.max_subscriptions_per_conn
            && value > 0
        {
            config.max_subscriptions_per_conn = value;
        }
        if let Some(value) = override_cfg.subscriber_queue_policy
            && let Some(parsed) = parse_sub_queue_policy(&value)
        {
            config.subscriber_queue_policy = parsed;
        }
        if let Some(value) = override_cfg.subscriber_writer_lanes
            && value > 0
        {
            config.subscriber_writer_lanes = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_queue_depth
            && value > 0
        {
            config.subscriber_lane_queue_depth = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_queue_policy
            && let Some(parsed) = parse_sub_queue_policy(&value)
        {
            config.subscriber_lane_queue_policy = parsed;
        }
        if let Some(value) = override_cfg.max_subscriber_writer_lanes
            && value > 0
        {
            config.max_subscriber_writer_lanes = value;
        }
        if let Some(value) = override_cfg.subscriber_lane_shard {
            config.subscriber_lane_shard = value;
        }
        if let Some(value) = override_cfg.subscriber_single_writer_per_conn {
            config.subscriber_single_writer_per_conn = value;
        }
        if let Some(value) = override_cfg.subscriber_flush_max_items
            && value > 0
        {
            config.subscriber_flush_max_items = value;
        }
        if let Some(value) = override_cfg.subscriber_flush_max_delay_us {
            config.subscriber_flush_max_delay_us = value;
        }
        if let Some(value) = override_cfg.subscriber_max_bytes_per_write
            && value > 0
        {
            config.subscriber_max_bytes_per_write = value;
        }
        if let Some(value) = override_cfg.sub_streams_per_conn
            && value > 0
        {
            config.sub_streams_per_conn = value;
        }
        if let Some(value) = override_cfg.sub_stream_mode {
            config.sub_stream_mode = value;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
