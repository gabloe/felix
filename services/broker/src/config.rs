use anyhow::{Context, Result};
use felix_broker::SubQueuePolicy;
use serde::Deserialize;
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;

// Broker service configuration sourced from environment variables.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    // QUIC listener bind address.
    pub quic_bind: SocketAddr,
    // Metrics HTTP listener bind address.
    pub metrics_bind: SocketAddr,
    // Optional control-plane base URL.
    pub controlplane_url: Option<String>,
    // Poll interval for control-plane changes.
    pub controlplane_sync_interval_ms: u64,
    // If true, publish acks are sent after commit.
    pub ack_on_commit: bool,
    // Max frame size accepted on QUIC streams.
    pub max_frame_bytes: usize,
    // Max time to wait when backpressuring publish enqueue.
    pub publish_queue_wait_timeout_ms: u64,
    // Max time to wait for ack-on-commit publish completion.
    pub ack_wait_timeout_ms: u64,
    // Disable timing collection for lower overhead.
    pub disable_timings: bool,
    // Max time to wait for control-stream writer to drain.
    pub control_stream_drain_timeout_ms: u64,
    // Cache connection flow-control window.
    pub cache_conn_recv_window: u64,
    // Cache stream flow-control window.
    pub cache_stream_recv_window: u64,
    // Cache connection send window.
    pub cache_send_window: u64,
    // Max events per batched subscription frame.
    pub event_batch_max_events: usize,
    // Max bytes per batched subscription frame.
    pub event_batch_max_bytes: usize,
    // Max delay before flushing a subscription batch.
    pub event_batch_max_delay_us: u64,
    // Fanout batch size for subscription sending.
    pub fanout_batch_size: usize,
    // Publish worker count per QUIC connection.
    pub pub_workers_per_conn: usize,
    // Per-worker publish queue depth.
    pub pub_queue_depth: usize,
    // Per-subscriber queue capacity in broker core.
    pub subscriber_queue_capacity: usize,
    // Subscriber queue policy for publish->fanout enqueue.
    pub subscriber_queue_policy: SubQueuePolicy,
    // Number of outbound subscriber writer lanes.
    pub subscriber_writer_lanes: usize,
    // Bounded queue depth per writer lane.
    pub subscriber_lane_queue_depth: usize,
    // Queue policy for lane ingress.
    pub subscriber_lane_queue_policy: SubQueuePolicy,
    // Upper bound to prevent over-sharding lane counts that can regress p99/p999 under load.
    pub max_subscriber_writer_lanes: usize,
    // Deterministic policy for assigning subscribers to writer lanes.
    pub subscriber_lane_shard: SubscriberLaneShard,
    // If true, route all subscribers on the same QUIC connection to one writer lane.
    pub subscriber_single_writer_per_conn: bool,
    // Max queued items drained per lane flush.
    pub subscriber_flush_max_items: usize,
    // Max time spent waiting for a lane flush fill.
    pub subscriber_flush_max_delay_us: u64,
    // Upper bound for coalesced bytes per write call.
    pub subscriber_max_bytes_per_write: usize,
    // Number of delivery streams to use per connection in hashed-pool mode.
    pub sub_streams_per_conn: usize,
    // Strategy for mapping subscribers to streams.
    pub sub_stream_mode: SubStreamMode,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberLaneShard {
    // Prefer connection-aware routing when a connection id is known, else fallback to subscriber id.
    Auto,
    SubscriberIdHash,
    ConnectionIdHash,
    // Assign once at subscribe time and keep lane pinned (ordering-safe RR variant).
    RoundRobinPin,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
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
const DEFAULT_ACK_WAIT_TIMEOUT_MS: u64 = 2000;
const DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS: u64 = 50;
const DEFAULT_PUB_WORKERS_PER_CONN: usize = 4;
const DEFAULT_PUB_QUEUE_DEPTH: usize = 1024;
const DEFAULT_SUBSCRIBER_QUEUE_CAPACITY: usize = 4096;
const DEFAULT_SUBSCRIBER_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::DropNew;
const DEFAULT_SUBSCRIBER_WRITER_LANES: usize = 4;
const DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH: usize = 8192;
const DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY: SubQueuePolicy = SubQueuePolicy::Block;
const DEFAULT_MAX_SUBSCRIBER_WRITER_LANES: usize = 8;
const DEFAULT_SUBSCRIBER_LANE_SHARD: SubscriberLaneShard = SubscriberLaneShard::Auto;
const DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS: usize = 64;
const DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US: u64 = 200;
const DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE: usize = 256 * 1024;
const DEFAULT_SUB_STREAMS_PER_CONN: usize = 4;
const DEFAULT_SUB_STREAM_MODE: SubStreamMode = SubStreamMode::PerSubscriber;

#[derive(Debug, Deserialize)]
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
    cache_conn_recv_window: Option<u64>,
    cache_stream_recv_window: Option<u64>,
    cache_send_window: Option<u64>,
    event_batch_max_events: Option<usize>,
    event_batch_max_bytes: Option<usize>,
    event_batch_max_delay_us: Option<u64>,
    fanout_batch_size: Option<usize>,
    pub_workers_per_conn: Option<usize>,
    pub_queue_depth: Option<usize>,
    subscriber_queue_capacity: Option<usize>,
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
    pub fn from_env() -> Result<Self> {
        // Environment variables provide defaults for local development.
        let metrics_bind = std::env::var("FELIX_BROKER_METRICS_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .with_context(|| "parse FELIX_BROKER_METRICS_BIND")?;
        let quic_bind = std::env::var("FELIX_QUIC_BIND")
            .unwrap_or_else(|_| "0.0.0.0:5000".to_string())
            .parse()
            .with_context(|| "parse FELIX_QUIC_BIND")?;
        let controlplane_url = std::env::var("FELIX_CONTROLPLANE_URL").ok();
        // Poll every 2s by default.
        let controlplane_sync_interval_ms = std::env::var("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2000);
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
        let pub_queue_depth = std::env::var("FELIX_BROKER_PUB_QUEUE_DEPTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PUB_QUEUE_DEPTH);
        let subscriber_queue_capacity = std::env::var("FELIX_SUBSCRIBER_QUEUE_CAPACITY")
            .ok()
            .or_else(|| std::env::var("FELIX_SUB_QUEUE_CAPACITY").ok())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_SUBSCRIBER_QUEUE_CAPACITY);
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
        let sub_stream_mode = std::env::var("FELIX_SUB_STREAM_MODE")
            .ok()
            .and_then(|value| SubStreamMode::parse_env(value.as_str()))
            .unwrap_or(DEFAULT_SUB_STREAM_MODE);
        Ok(Self {
            quic_bind,
            metrics_bind,
            controlplane_url,
            controlplane_sync_interval_ms,
            ack_on_commit,
            max_frame_bytes,
            publish_queue_wait_timeout_ms,
            ack_wait_timeout_ms,
            disable_timings,
            control_stream_drain_timeout_ms,
            cache_conn_recv_window,
            cache_stream_recv_window,
            cache_send_window,
            event_batch_max_events,
            event_batch_max_bytes,
            event_batch_max_delay_us,
            fanout_batch_size,
            pub_workers_per_conn,
            pub_queue_depth,
            subscriber_queue_capacity,
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
            if let Some(value) = override_cfg.subscriber_queue_capacity
                && value > 0
            {
                config.subscriber_queue_capacity = value;
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
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    // Helper to clear all Felix env vars
    fn clear_felix_env() {
        for (key, _) in env::vars() {
            if key.starts_with("FELIX_") {
                unsafe {
                    env::remove_var(key);
                }
            }
        }
    }

    #[serial]
    #[test]
    fn from_env_uses_defaults() {
        clear_felix_env();
        let config = BrokerConfig::from_env().expect("from_env");
        assert_eq!(config.quic_bind.to_string(), "0.0.0.0:5000");
        assert_eq!(config.metrics_bind.to_string(), "0.0.0.0:8080");
        assert!(config.controlplane_url.is_none());
        assert_eq!(config.controlplane_sync_interval_ms, 2000);
        assert!(!config.ack_on_commit);
        assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        assert_eq!(
            config.publish_queue_wait_timeout_ms,
            DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS
        );
        assert_eq!(config.ack_wait_timeout_ms, DEFAULT_ACK_WAIT_TIMEOUT_MS);
        assert_eq!(config.disable_timings, DEFAULT_DISABLE_TIMINGS);
        assert_eq!(
            config.control_stream_drain_timeout_ms,
            DEFAULT_CONTROL_STREAM_DRAIN_TIMEOUT_MS
        );
        assert_eq!(
            config.subscriber_queue_capacity,
            DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
        );
        assert_eq!(
            config.subscriber_queue_policy,
            DEFAULT_SUBSCRIBER_QUEUE_POLICY
        );
        assert_eq!(
            config.subscriber_writer_lanes,
            DEFAULT_SUBSCRIBER_WRITER_LANES
        );
        assert_eq!(
            config.subscriber_lane_queue_depth,
            DEFAULT_SUBSCRIBER_LANE_QUEUE_DEPTH
        );
        assert_eq!(
            config.subscriber_lane_queue_policy,
            DEFAULT_SUBSCRIBER_LANE_QUEUE_POLICY
        );
        assert_eq!(
            config.max_subscriber_writer_lanes,
            DEFAULT_MAX_SUBSCRIBER_WRITER_LANES
        );
        assert_eq!(config.subscriber_lane_shard, DEFAULT_SUBSCRIBER_LANE_SHARD);
        assert!(!config.subscriber_single_writer_per_conn);
        assert_eq!(
            config.subscriber_flush_max_items,
            DEFAULT_SUBSCRIBER_FLUSH_MAX_ITEMS
        );
        assert_eq!(
            config.subscriber_flush_max_delay_us,
            DEFAULT_SUBSCRIBER_FLUSH_MAX_DELAY_US
        );
        assert_eq!(
            config.subscriber_max_bytes_per_write,
            DEFAULT_SUBSCRIBER_MAX_BYTES_PER_WRITE
        );
        assert_eq!(config.sub_streams_per_conn, DEFAULT_SUB_STREAMS_PER_CONN);
        assert_eq!(config.sub_stream_mode, DEFAULT_SUB_STREAM_MODE);
    }

    #[serial]
    #[test]
    fn from_env_respects_env_vars() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_QUIC_BIND", "127.0.0.1:6000");
            env::set_var("FELIX_BROKER_METRICS_BIND", "127.0.0.1:9000");
            env::set_var(
                "FELIX_CONTROLPLANE_URL",
                "http://controlplane.example.com:8443",
            );
            env::set_var("FELIX_CONTROLPLANE_SYNC_INTERVAL_MS", "5000");
            env::set_var("FELIX_ACK_ON_COMMIT", "true");
            env::set_var("FELIX_MAX_FRAME_BYTES", "32000000");
            env::set_var("FELIX_PUBLISH_QUEUE_WAIT_MS", "3000");
            env::set_var("FELIX_ACK_WAIT_TIMEOUT_MS", "4000");
            env::set_var("FELIX_DISABLE_TIMINGS", "yes");
            env::set_var("FELIX_CONTROL_STREAM_DRAIN_TIMEOUT_MS", "100");
            env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "128");
            env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "512000");
            env::set_var("FELIX_FANOUT_BATCH", "256");
            env::set_var("FELIX_SUB_WRITER_LANES", "8");
            env::set_var("FELIX_SUB_LANE_QUEUE_DEPTH", "4096");
            env::set_var("FELIX_SUB_QUEUE_MODE", "drop_old");
            env::set_var("FELIX_MAX_SUB_WRITER_LANES", "16");
            env::set_var("FELIX_SUB_LANE_SHARD", "connection_id_hash");
            env::set_var("FELIX_SUB_QUEUE_POLICY", "block");
            env::set_var("FELIX_SUB_SINGLE_WRITER_PER_CONN", "false");
            env::set_var("FELIX_SUB_FLUSH_MAX_ITEMS", "32");
            env::set_var("FELIX_SUB_FLUSH_MAX_DELAY_US", "150");
            env::set_var("FELIX_SUB_MAX_BYTES_PER_WRITE", "131072");
            env::set_var("FELIX_SUB_STREAMS_PER_CONN", "8");
            env::set_var("FELIX_SUB_STREAM_MODE", "hashed_pool");
        }

        let config = BrokerConfig::from_env().expect("from_env");
        assert_eq!(config.quic_bind.to_string(), "127.0.0.1:6000");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9000");
        assert_eq!(
            config.controlplane_url,
            Some("http://controlplane.example.com:8443".to_string())
        );
        assert_eq!(config.controlplane_sync_interval_ms, 5000);
        assert!(config.ack_on_commit);
        assert_eq!(config.max_frame_bytes, 32000000);
        assert_eq!(config.publish_queue_wait_timeout_ms, 3000);
        assert_eq!(config.ack_wait_timeout_ms, 4000);
        assert!(config.disable_timings);
        assert_eq!(config.control_stream_drain_timeout_ms, 100);
        assert_eq!(config.event_batch_max_events, 128);
        assert_eq!(config.event_batch_max_bytes, 512000);
        assert_eq!(config.fanout_batch_size, 256);
        assert_eq!(config.subscriber_writer_lanes, 8);
        assert_eq!(config.subscriber_lane_queue_depth, 4096);
        assert_eq!(config.subscriber_lane_queue_policy, SubQueuePolicy::DropOld);
        assert_eq!(config.max_subscriber_writer_lanes, 16);
        assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::Block);
        assert_eq!(
            config.subscriber_lane_shard,
            SubscriberLaneShard::ConnectionIdHash
        );
        assert!(!config.subscriber_single_writer_per_conn);
        assert_eq!(config.subscriber_flush_max_items, 32);
        assert_eq!(config.subscriber_flush_max_delay_us, 150);
        assert_eq!(config.subscriber_max_bytes_per_write, 131072);
        assert_eq!(config.sub_streams_per_conn, 8);
        assert_eq!(config.sub_stream_mode, SubStreamMode::HashedPool);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_ack_on_commit_variations() {
        clear_felix_env();
        for val in &["1", "true", "yes"] {
            unsafe {
                env::set_var("FELIX_ACK_ON_COMMIT", val);
            }
            let config = BrokerConfig::from_env().expect("from_env");
            assert!(config.ack_on_commit, "expected true for {}", val);
        }
        for val in &["0", "false", "no", "anything"] {
            unsafe {
                env::set_var("FELIX_ACK_ON_COMMIT", val);
            }
            let config = BrokerConfig::from_env().expect("from_env");
            assert!(!config.ack_on_commit, "expected false for {}", val);
        }
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_filters_zero_values() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_MAX_FRAME_BYTES", "0");
            env::set_var("FELIX_PUBLISH_QUEUE_WAIT_MS", "0");
            env::set_var("FELIX_FANOUT_BATCH", "0");
        }

        let config = BrokerConfig::from_env().expect("from_env");
        // Should use defaults when 0 is provided
        assert_eq!(config.max_frame_bytes, DEFAULT_MAX_FRAME_BYTES);
        assert_eq!(
            config.publish_queue_wait_timeout_ms,
            DEFAULT_PUBLISH_QUEUE_WAIT_TIMEOUT_MS
        );
        assert_eq!(config.fanout_batch_size, 64);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_rejects_invalid_socket_addr() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_QUIC_BIND", "not-a-valid-address");
        }
        let result = BrokerConfig::from_env();
        assert!(result.is_err());
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_no_file_uses_defaults() {
        clear_felix_env();
        let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.quic_bind.to_string(), "0.0.0.0:5000");
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_file_not_found_with_explicit_path_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let nonexistent = tmpdir.path().join("nonexistent.yml");
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", nonexistent.to_str().unwrap());
        }
        let result = BrokerConfig::from_env_or_yaml();
        assert!(result.is_err());
        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_overrides_with_valid_yaml() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(
            &config_path,
            r#"
quic_bind: "127.0.0.1:5555"
metrics_bind: "127.0.0.1:9999"
controlplane_url: "http://test-cp:8443"
controlplane_sync_interval_ms: 5000
ack_on_commit: true
max_frame_bytes: 32000000
event_batch_max_events: 128
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        }

        let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.quic_bind.to_string(), "127.0.0.1:5555");
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9999");
        assert_eq!(
            config.controlplane_url,
            Some("http://test-cp:8443".to_string())
        );
        assert_eq!(config.controlplane_sync_interval_ms, 5000);
        assert!(config.ack_on_commit);
        assert_eq!(config.max_frame_bytes, 32000000);
        assert_eq!(config.event_batch_max_events, 128);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_invalid_yaml_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("bad.yml");
        fs::write(&config_path, "this is not: valid: yaml:").unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        }

        let result = BrokerConfig::from_env_or_yaml();
        assert!(result.is_err());

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_invalid_socket_in_yaml_fails() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(&config_path, "quic_bind: \"not-a-socket\"").unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        }

        let result = BrokerConfig::from_env_or_yaml();
        assert!(result.is_err());

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_filters_zero_values_in_yaml() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(
            &config_path,
            r#"
cache_conn_recv_window: 0
event_batch_max_events: 0
fanout_batch_size: 0
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        }

        let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
        // Should keep env defaults when yaml has 0
        assert_eq!(
            config.cache_conn_recv_window,
            DEFAULT_CACHE_CONN_RECV_WINDOW
        );
        assert_eq!(config.event_batch_max_events, 64);
        assert_eq!(config.fanout_batch_size, 64);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_partial_override() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(
            &config_path,
            r#"
quic_bind: "127.0.0.1:7777"
max_frame_bytes: 8000000
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
            env::set_var("FELIX_BROKER_METRICS_BIND", "127.0.0.1:9090");
        }

        let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
        // YAML override
        assert_eq!(config.quic_bind.to_string(), "127.0.0.1:7777");
        assert_eq!(config.max_frame_bytes, 8000000);
        // Env var
        assert_eq!(config.metrics_bind.to_string(), "127.0.0.1:9090");
        // Default
        assert_eq!(config.controlplane_sync_interval_ms, 2000);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_respects_all_cache_window_settings() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_CACHE_CONN_RECV_WINDOW", "512000000");
            env::set_var("FELIX_CACHE_STREAM_RECV_WINDOW", "128000000");
            env::set_var("FELIX_CACHE_SEND_WINDOW", "512000000");
        }

        let config = BrokerConfig::from_env().expect("from_env");
        assert_eq!(config.cache_conn_recv_window, 512000000);
        assert_eq!(config.cache_stream_recv_window, 128000000);
        assert_eq!(config.cache_send_window, 512000000);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_respects_worker_and_queue_settings() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_BROKER_PUB_WORKERS_PER_CONN", "8");
            env::set_var("FELIX_BROKER_PUB_QUEUE_DEPTH", "2048");
            env::set_var("FELIX_SUBSCRIBER_QUEUE_CAPACITY", "256");
            env::set_var("FELIX_SUB_QUEUE_POLICY", "drop_old");
        }

        let config = BrokerConfig::from_env().expect("from_env");
        assert_eq!(config.pub_workers_per_conn, 8);
        assert_eq!(config.pub_queue_depth, 2048);
        assert_eq!(config.subscriber_queue_capacity, 256);
        assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_respects_batch_settings() {
        clear_felix_env();
        unsafe {
            env::set_var("FELIX_EVENT_BATCH_MAX_EVENTS", "256");
            env::set_var("FELIX_EVENT_BATCH_MAX_BYTES", "1048576");
            env::set_var("FELIX_EVENT_BATCH_MAX_DELAY_US", "500");
        }

        let config = BrokerConfig::from_env().expect("from_env");
        assert_eq!(config.event_batch_max_events, 256);
        assert_eq!(config.event_batch_max_bytes, 1048576);
        assert_eq!(config.event_batch_max_delay_us, 500);

        clear_felix_env();
    }

    #[serial]
    #[test]
    fn from_env_or_yaml_all_window_overrides() {
        clear_felix_env();
        let tmpdir = TempDir::new().unwrap();
        let config_path = tmpdir.path().join("config.yml");
        fs::write(
            &config_path,
            r#"
cache_conn_recv_window: 128000000
cache_stream_recv_window: 32000000
cache_send_window: 128000000
pub_workers_per_conn: 16
pub_queue_depth: 4096
subscriber_queue_capacity: 96
subscriber_queue_policy: block
subscriber_single_writer_per_conn: false
"#,
        )
        .unwrap();
        unsafe {
            env::set_var("FELIX_BROKER_CONFIG", config_path.to_str().unwrap());
        }

        let config = BrokerConfig::from_env_or_yaml().expect("from_env_or_yaml");
        assert_eq!(config.cache_conn_recv_window, 128000000);
        assert_eq!(config.cache_stream_recv_window, 32000000);
        assert_eq!(config.cache_send_window, 128000000);
        assert_eq!(config.pub_workers_per_conn, 16);
        assert_eq!(config.pub_queue_depth, 4096);
        assert_eq!(config.subscriber_queue_capacity, 96);
        assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::Block);
        assert!(!config.subscriber_single_writer_per_conn);

        clear_felix_env();
    }
}
