//! Client configuration: pool sizes, stream windows, queue limits and
//! credentials.
//!
//! [`ClientConfig::optimized_defaults`] is the starting point;
//! [`ClientConfig::from_env_or_yaml`] layers `FELIX_*` environment variables
//! and then a YAML file over it. Defaults favor throughput for publish and low
//! latency for cache and event streams, with hard caps against oversized
//! frames.

mod defaults;
mod env;
mod yaml;

pub(crate) use defaults::*;

use std::fs;
use std::sync::Arc;

use anyhow::{Context, Result};
use felix_transport::TransportConfig;

use crate::auth::{StaticToken, TokenProvider};
use crate::publish::PublishSharding;
use yaml::ClientConfigOverride;

/// Everything a [`crate::Client`] is built from.
///
/// Build one with [`ClientConfig::optimized_defaults`] or
/// [`ClientConfig::from_env_or_yaml`] and adjust fields from there.
#[derive(Clone)]
pub struct ClientConfig {
    pub quinn: quinn::ClientConfig,
    pub publish_conn_pool: usize,
    pub publish_streams_per_conn: usize,
    pub publish_chunk_bytes: usize,
    pub publish_queue_depth: usize,
    pub publish_inflight_bytes: usize,
    pub publish_sharding: PublishSharding,
    pub auth_tenant_id: Option<String>,
    /// A fixed token for every stream. Clients that run longer than the
    /// token lasts should use `token_provider`.
    pub auth_token: Option<String>,
    /// Supplies the token each time a stream authenticates. Overrides
    /// `auth_token`. See [`crate::RefreshingToken`].
    pub token_provider: Option<Arc<dyn TokenProvider>>,
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
    pub client_sub_queue_capacity: usize,
    pub client_sub_queue_policy: ClientSubQueuePolicy,
    pub max_frame_bytes: usize,
    pub bench_embed_ts: bool,
}

impl ClientConfig {
    pub fn optimized_defaults(quinn: quinn::ClientConfig) -> Self {
        Self {
            quinn,
            publish_conn_pool: DEFAULT_PUB_CONN_POOL,
            publish_streams_per_conn: DEFAULT_PUB_STREAMS_PER_CONN,
            publish_chunk_bytes: DEFAULT_PUBLISH_CHUNK_BYTES,
            publish_queue_depth: DEFAULT_PUBLISH_QUEUE_DEPTH,
            publish_inflight_bytes: DEFAULT_PUBLISH_INFLIGHT_BYTES,
            // Hash, and measurement says leave it there. Round-robin was
            // re-tested on a generator that actually reads the setting --
            // verified by the binary reporting `sharding=RoundRobin` rather
            // than assumed -- and landed at 912.6 MB/s, inside the band every
            // other valid run occupied. The earlier run that appeared to favour
            // it never applied the override at all (#553).
            publish_sharding: PublishSharding::HashStream,
            auth_tenant_id: None,
            auth_token: None,
            token_provider: None,
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
            client_sub_queue_capacity: DEFAULT_CLIENT_SUB_QUEUE_CAPACITY,
            client_sub_queue_policy: ClientSubQueuePolicy::DropNew,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            bench_embed_ts: false,
        }
    }

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
                serde_yaml_ng::from_str(&contents).context("parse client config yaml")?;
            override_cfg.apply(&mut config);
        }
        Ok(config)
    }

    /// The token source streams authenticate with.
    pub(crate) fn tokens(&self) -> Result<Arc<dyn TokenProvider>> {
        if let Some(provider) = &self.token_provider {
            return Ok(Arc::clone(provider));
        }
        let token = self
            .auth_token
            .clone()
            .context("FELIX_AUTH_TOKEN must be set, or a token provider configured")?;
        Ok(Arc::new(StaticToken(token)))
    }

    pub(crate) fn runtime_config(&self) -> ClientRuntimeConfig {
        ClientRuntimeConfig {
            event_router_max_pending: self.event_router_max_pending,
            max_frame_bytes: self.max_frame_bytes,
            client_sub_queue_capacity: self.client_sub_queue_capacity,
            client_sub_queue_policy: self.client_sub_queue_policy,
            bench_embed_ts: self.bench_embed_ts,
        }
    }
}

/// What a subscription does when its bounded queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientSubQueuePolicy {
    Block,
    DropNew,
    DropOld,
}

impl ClientSubQueuePolicy {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "block" => Some(Self::Block),
            "drop_new" => Some(Self::DropNew),
            "drop_old" => Some(Self::DropOld),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ClientRuntimeConfig {
    pub(crate) event_router_max_pending: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) client_sub_queue_capacity: usize,
    pub(crate) client_sub_queue_policy: ClientSubQueuePolicy,
    pub(crate) bench_embed_ts: bool,
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

#[cfg(test)]
mod tests;
