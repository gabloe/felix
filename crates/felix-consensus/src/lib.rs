/// Consensus configuration values for a simple local Raft setup.
///
/// ```
/// use felix_consensus::RaftConfig;
///
/// let config = RaftConfig::default();
/// assert!(config.election_timeout_ms > config.heartbeat_interval_ms);
/// ```
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        // Defaults are tuned for local development over correctness.
        Self {
            cluster_id: "felix-local".into(),
            node_id: "node-1".into(),
            election_timeout_ms: 1500,
            heartbeat_interval_ms: 300,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        // Ensure the defaults stay consistent and sensible.
        let config = RaftConfig::default();
        assert_eq!(config.cluster_id, "felix-local");
        assert_eq!(config.node_id, "node-1");
        assert!(config.election_timeout_ms > config.heartbeat_interval_ms);
    }

    #[test]
    fn default_timeouts_are_nonzero() {
        let config = RaftConfig::default();
        assert!(config.election_timeout_ms > 0);
        assert!(config.heartbeat_interval_ms > 0);
    }
}
