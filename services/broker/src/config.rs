use anyhow::{Context, Result};
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
    // Subscription event queue depth.
    pub event_queue_depth: usize,
    // Use binary encoding for single events when enabled.
    pub event_single_binary_enabled: bool,
    // Min payload size to use binary encoding for single events.
    pub event_single_binary_min_bytes: usize,
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
const DEFAULT_EVENT_QUEUE_DEPTH: usize = 1024;
const DEFAULT_EVENT_SINGLE_BINARY_ENABLED: bool = false;
const DEFAULT_EVENT_SINGLE_BINARY_MIN_BYTES: usize = 512;

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
    event_queue_depth: Option<usize>,
    event_single_binary_enabled: Option<bool>,
    event_single_binary_min_bytes: Option<usize>,
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
        let controlplane_url = std::env::var("FELIX_CP_URL").ok();
        // Poll every 2s by default.
        let controlplane_sync_interval_ms = std::env::var("FELIX_CP_SYNC_INTERVAL_MS")
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
            .unwrap_or(256 * 1024);
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
        let event_queue_depth = std::env::var("FELIX_EVENT_QUEUE_DEPTH")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_EVENT_QUEUE_DEPTH);
        let event_single_binary_enabled = std::env::var("FELIX_BINARY_SINGLE_EVENT")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "TRUE" | "YES"))
            .unwrap_or(DEFAULT_EVENT_SINGLE_BINARY_ENABLED);
        let event_single_binary_min_bytes = std::env::var("FELIX_BINARY_SINGLE_EVENT_MIN_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_EVENT_SINGLE_BINARY_MIN_BYTES);
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
            event_queue_depth,
            event_single_binary_enabled,
            event_single_binary_min_bytes,
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
                serde_yaml::from_str(&contents).with_context(|| "parse broker config yaml")?;
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
            if let Some(value) = override_cfg.event_queue_depth
                && value > 0
            {
                config.event_queue_depth = value;
            }
            if let Some(value) = override_cfg.event_single_binary_enabled {
                config.event_single_binary_enabled = value;
            }
            if let Some(value) = override_cfg.event_single_binary_min_bytes
                && value > 0
            {
                config.event_single_binary_min_bytes = value;
            }
        }
        Ok(config)
    }
}
