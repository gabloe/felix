//! How quickly a silent broker is declared down, and how often the
//! timer-driven loops run.
use anyhow::{Result, anyhow};

use super::{
    DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS, DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
    DEFAULT_NODE_HEARTBEAT_INTERVAL_MS, DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
};

/// Timings for broker liveness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLivenessConfig {
    /// Advertised to brokers in every heartbeat response.
    pub heartbeat_interval_ms: u64,
    /// Silence beyond this marks a node down.
    pub expiry_timeout_ms: u64,
    /// How often the sweep looks for expired nodes.
    pub sweep_interval_ms: u64,
    /// How often unplaced shards are assigned to live brokers.
    pub shard_reconcile_interval_ms: u64,
}

impl Default for NodeLivenessConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: DEFAULT_NODE_HEARTBEAT_INTERVAL_MS,
            expiry_timeout_ms: DEFAULT_NODE_EXPIRY_TIMEOUT_MS,
            sweep_interval_ms: DEFAULT_NODE_EXPIRY_SWEEP_INTERVAL_MS,
            shard_reconcile_interval_ms: DEFAULT_SHARD_RECONCILE_INTERVAL_MS,
        }
    }
}

impl NodeLivenessConfig {
    pub(super) fn validate(&self) -> Result<()> {
        if self.heartbeat_interval_ms == 0 {
            return Err(anyhow!(
                "node heartbeat_interval_ms must be greater than zero"
            ));
        }
        if self.sweep_interval_ms == 0 {
            return Err(anyhow!("node sweep_interval_ms must be greater than zero"));
        }
        if self.shard_reconcile_interval_ms == 0 {
            return Err(anyhow!(
                "shard_reconcile_interval_ms must be greater than zero"
            ));
        }
        // A timeout at or below the interval expires brokers that are heartbeating
        // exactly as told to, which takes down a healthy cluster.
        if self.expiry_timeout_ms <= self.heartbeat_interval_ms {
            return Err(anyhow!(
                "node expiry_timeout_ms ({}) must exceed heartbeat_interval_ms ({})",
                self.expiry_timeout_ms,
                self.heartbeat_interval_ms
            ));
        }
        Ok(())
    }
}
