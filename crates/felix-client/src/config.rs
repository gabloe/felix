// Client-side defaults and transport configuration helpers.
use anyhow::{Context, Result};
use felix_transport::TransportConfig;
use serde::Deserialize;
use std::fs;
use std::sync::OnceLock;

use crate::client::sharding::PublishSharding;

pub(crate) const PUBLISH_QUEUE_DEPTH: usize = 1024;
pub(crate) const CACHE_WORKER_QUEUE_DEPTH: usize = 1024;
pub(crate) const EVENT_ROUTER_QUEUE_DEPTH: usize = 1024;
pub(crate) const DEFAULT_PUBLISH_CHUNK_BYTES: usize = 16 * 1024;
pub(crate) const DEFAULT_PUB_CONN_POOL: usize = 4;
pub(crate) const DEFAULT_PUB_STREAMS_PER_CONN: usize = 2;
pub(crate) const DEFAULT_EVENT_CONN_POOL: usize = 8;
pub(crate) const DEFAULT_CACHE_CONN_POOL: usize = 8;
pub(crate) const DEFAULT_CACHE_STREAMS_PER_CONN: usize = 4;
pub(crate) const DEFAULT_EVENT_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_EVENT_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_EVENT_SEND_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_CONN_RECV_WINDOW: u64 = 256 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_STREAM_RECV_WINDOW: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_CACHE_SEND_WINDOW: u64 = 256 * 1024 * 1024;
// See also DEFAULT_MAX_FRAME_BYTES and DEFAULT_EVENT_ROUTER_MAX_PENDING below.

/// Hard safety cap for any single felix-wire frame.
///
/// Rationale:
/// - `read_frame_into` and friends allocate a buffer sized by `header.length`.
/// - Without a cap, a malicious / buggy peer can advertise an enormous length and
///   trigger OOM or allocator churn (DoS).
///
/// Override with `FELIX_MAX_FRAME_BYTES`.
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Upper bound on how many pending subscription registrations/streams the event
/// router will hold.
///
/// Rationale:
/// - `pending_waiters` grows when the app registers but the server never opens the uni stream.
/// - `pending_streams` grows when the server opens uni streams for ids the app never registers.
///
/// Either case can happen due to bugs or a malicious peer; we cap memory usage.
/// Override with `FELIX_EVENT_ROUTER_MAX_PENDING`.
pub(crate) const DEFAULT_EVENT_ROUTER_MAX_PENDING: usize = 16 * 1024;

#[derive(Clone)]
pub struct ClientConfig {
    pub quinn: quinn::ClientConfig,
    pub publish_conn_pool: usize,
    pub publish_streams_per_conn: usize,
    pub publish_chunk_bytes: usize,
    pub publish_sharding: PublishSharding,
    pub cache_conn_pool: usize,
    pub cache_streams_per_conn: usize,
    pub event_conn_pool: usize,
    pub event_conn_recv_window: u64,
    pub event_stream_recv_window: u64,
    pub event_send_window: u64,
    pub cache_conn_recv_window: u64,
    pub cache_stream_recv_window: u64,
    pub cache_send_window: u64,
    pub event_router_max_pending: usize,
    pub max_frame_bytes: usize,
    pub bench_embed_ts: bool,
}

#[derive(Clone)]
pub(crate) struct ClientRuntimeConfig {
    pub(crate) event_router_max_pending: usize,
    pub(crate) max_frame_bytes: usize,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    pub(crate) bench_embed_ts: bool,
}

static CLIENT_RUNTIME_CONFIG: OnceLock<ClientRuntimeConfig> = OnceLock::new();

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
struct ClientConfigOverride {
    publish_conn_pool: Option<usize>,
    publish_streams_per_conn: Option<usize>,
    publish_chunk_bytes: Option<usize>,
    publish_sharding: Option<String>,
    cache_conn_pool: Option<usize>,
    cache_streams_per_conn: Option<usize>,
    event_conn_pool: Option<usize>,
    event_conn_recv_window: Option<u64>,
    event_stream_recv_window: Option<u64>,
    event_send_window: Option<u64>,
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    event_router_max_pending: Option<usize>,
    max_frame_bytes: Option<usize>,
    bench_embed_ts: Option<bool>,
}

impl ClientConfig {
    pub fn from_env_or_yaml(quinn: quinn::ClientConfig, config_path: Option<&str>) -> Result<Self> {
        let mut config = Self::from_env(quinn);
        let override_path = config_path
            .map(|value| value.to_string())
            .or_else(|| std::env::var("FELIX_CLIENT_CONFIG").ok());
        let contents = match override_path.as_deref() {
            Some(path) => match fs::read_to_string(path) {
                Ok(contents) => Some(contents),
                Err(err) => {
                    return Err(err).with_context(|| format!("read client config: {path}"));
                }
            },
            None => None,
        };
        if let Some(contents) = contents {
            let override_cfg: ClientConfigOverride =
                serde_yaml::from_str(&contents).context("parse client config yaml")?;
            override_cfg.apply(&mut config);
        }
        Ok(config)
    }

    pub fn optimized_defaults(quinn: quinn::ClientConfig) -> Self {
        Self {
            quinn,
            publish_conn_pool: DEFAULT_PUB_CONN_POOL,
            publish_streams_per_conn: DEFAULT_PUB_STREAMS_PER_CONN,
            publish_chunk_bytes: DEFAULT_PUBLISH_CHUNK_BYTES,
            publish_sharding: PublishSharding::HashStream,
            cache_conn_pool: DEFAULT_CACHE_CONN_POOL,
            cache_streams_per_conn: DEFAULT_CACHE_STREAMS_PER_CONN,
            event_conn_pool: DEFAULT_EVENT_CONN_POOL,
            event_conn_recv_window: DEFAULT_EVENT_CONN_RECV_WINDOW,
            event_stream_recv_window: DEFAULT_EVENT_STREAM_RECV_WINDOW,
            event_send_window: DEFAULT_EVENT_SEND_WINDOW,
            cache_conn_recv_window: DEFAULT_CACHE_CONN_RECV_WINDOW,
            cache_stream_recv_window: DEFAULT_CACHE_STREAM_RECV_WINDOW,
            cache_send_window: DEFAULT_CACHE_SEND_WINDOW,
            event_router_max_pending: DEFAULT_EVENT_ROUTER_MAX_PENDING,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            bench_embed_ts: false,
        }
    }

    fn from_env(quinn: quinn::ClientConfig) -> Self {
        let mut config = Self::optimized_defaults(quinn);
        if let Some(value) = read_usize_env("FELIX_PUB_CONN_POOL") {
            config.publish_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUB_STREAMS_PER_CONN") {
            config.publish_streams_per_conn = value;
        }
        if let Some(value) = read_usize_env("FELIX_PUBLISH_CHUNK_BYTES") {
            config.publish_chunk_bytes = value;
        }
        if let Some(value) = PublishSharding::from_env() {
            config.publish_sharding = value;
        }
        if let Some(value) = read_usize_env("FELIX_CACHE_CONN_POOL") {
            config.cache_conn_pool = value;
        }
        if let Some(value) = read_usize_env("FELIX_CACHE_STREAMS_PER_CONN") {
            config.cache_streams_per_conn = value;
        }
        if let Some(value) = read_usize_env("FELIX_EVENT_CONN_POOL") {
            config.event_conn_pool = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_CONN_RECV_WINDOW") {
            config.event_conn_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_STREAM_RECV_WINDOW") {
            config.event_stream_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_EVENT_SEND_WINDOW") {
            config.event_send_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_CONN_RECV_WINDOW") {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_STREAM_RECV_WINDOW") {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = read_u64_env("FELIX_CACHE_SEND_WINDOW") {
            config.cache_send_window = value;
        }
        if let Some(value) = read_usize_env("FELIX_EVENT_ROUTER_MAX_PENDING") {
            config.event_router_max_pending = value;
        }
        if let Some(value) = read_usize_env("FELIX_MAX_FRAME_BYTES") {
            config.max_frame_bytes = value;
        }
        if let Some(value) = read_bool_env("FELIX_BENCH_EMBED_TS") {
            config.bench_embed_ts = value;
        }
        config
    }

    pub(crate) fn install(&self) {
        let _ = CLIENT_RUNTIME_CONFIG.set(ClientRuntimeConfig {
            event_router_max_pending: self.event_router_max_pending,
            max_frame_bytes: self.max_frame_bytes,
            bench_embed_ts: self.bench_embed_ts,
        });
    }
}

impl ClientConfigOverride {
    fn apply(&self, config: &mut ClientConfig) {
        if let Some(value) = self.publish_conn_pool
            && value > 0
        {
            config.publish_conn_pool = value;
        }
        if let Some(value) = self.publish_streams_per_conn
            && value > 0
        {
            config.publish_streams_per_conn = value;
        }
        if let Some(value) = self.publish_chunk_bytes
            && value > 0
        {
            config.publish_chunk_bytes = value;
        }
        if let Some(value) = &self.publish_sharding
            && let Some(parsed) = parse_sharding(value)
        {
            config.publish_sharding = parsed;
        }
        if let Some(value) = self.cache_conn_pool
            && value > 0
        {
            config.cache_conn_pool = value;
        }
        if let Some(value) = self.cache_streams_per_conn
            && value > 0
        {
            config.cache_streams_per_conn = value;
        }
        if let Some(value) = self.event_conn_pool
            && value > 0
        {
            config.event_conn_pool = value;
        }
        if let Some(value) = self.event_conn_recv_window
            && value > 0
        {
            config.event_conn_recv_window = value;
        }
        if let Some(value) = self.event_stream_recv_window
            && value > 0
        {
            config.event_stream_recv_window = value;
        }
        if let Some(value) = self.event_send_window
            && value > 0
        {
            config.event_send_window = value;
        }
        if let Some(value) = self.cache_conn_recv_window
            && value > 0
        {
            config.cache_conn_recv_window = value;
        }
        if let Some(value) = self.cache_stream_recv_window
            && value > 0
        {
            config.cache_stream_recv_window = value;
        }
        if let Some(value) = self.cache_send_window
            && value > 0
        {
            config.cache_send_window = value;
        }
        if let Some(value) = self.event_router_max_pending
            && value > 0
        {
            config.event_router_max_pending = value;
        }
        if let Some(value) = self.max_frame_bytes
            && value > 0
        {
            config.max_frame_bytes = value;
        }
        if let Some(value) = self.bench_embed_ts {
            config.bench_embed_ts = value;
        }
    }
}

fn parse_sharding(value: &str) -> Option<PublishSharding> {
    match value {
        "rr" => Some(PublishSharding::RoundRobin),
        "hash_stream" => Some(PublishSharding::HashStream),
        _ => None,
    }
}

pub(crate) fn runtime_config() -> &'static ClientRuntimeConfig {
    CLIENT_RUNTIME_CONFIG.get_or_init(|| ClientRuntimeConfig {
        event_router_max_pending: DEFAULT_EVENT_ROUTER_MAX_PENDING,
        max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        bench_embed_ts: false,
    })
}

pub(crate) fn event_transport_config(
    mut base: TransportConfig,
    config: &ClientConfig,
) -> TransportConfig {
    base.receive_window = config.event_conn_recv_window;
    base.stream_receive_window = config.event_stream_recv_window;
    base.send_window = config.event_send_window;
    base
}

pub(crate) fn cache_transport_config(
    mut base: TransportConfig,
    config: &ClientConfig,
) -> TransportConfig {
    base.receive_window = config.cache_conn_recv_window;
    base.stream_receive_window = config.cache_stream_recv_window;
    base.send_window = config.cache_send_window;
    base
}

fn read_u64_env(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn read_usize_env(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn read_bool_env(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}
