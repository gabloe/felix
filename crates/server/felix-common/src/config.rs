//! Per-node settings: identity, listen address, data directory and limits.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::RegionId;

/// Node configuration values shared across components.
///
/// ```
/// use felix_common::{ids::RegionId, NodeConfig};
///
/// let region = RegionId::new();
/// let config = NodeConfig::new(region, "127.0.0.1:9000", "/tmp/felix");
/// assert_eq!(config.listen_addr, "127.0.0.1:9000");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: Uuid,
    pub region: RegionId,
    pub listen_addr: String,
    pub data_dir: String,
    pub limits: LimitsConfig,
}

impl NodeConfig {
    pub fn new(
        region: RegionId,
        listen_addr: impl Into<String>,
        data_dir: impl Into<String>,
    ) -> Self {
        // Use a new node ID so multiple nodes can run on one machine.
        Self {
            node_id: Uuid::new_v4(),
            region,
            listen_addr: listen_addr.into(),
            data_dir: data_dir.into(),
            limits: LimitsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    pub max_message_bytes: usize,
    pub max_inflight: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        // Defaults are conservative for local/dev usage.
        Self {
            max_message_bytes: 1024 * 1024,
            max_inflight: 10_000,
        }
    }
}

#[cfg(test)]
mod tests;
