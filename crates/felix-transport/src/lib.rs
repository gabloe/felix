// Transport-layer configuration and identifiers.
/// Transport-layer configuration defaults.
///
/// ```
/// use felix_transport::TransportConfig;
///
/// let config = TransportConfig::default();
/// assert!(config.max_frame_bytes > 0);
/// ```
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub max_frame_bytes: usize,
    pub max_streams: u16,
}

impl Default for TransportConfig {
    fn default() -> Self {
        // Keep defaults large enough for most dev/test workloads.
        Self {
            max_frame_bytes: 4 * 1024 * 1024,
            max_streams: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub id: ConnectionId,
    pub peer_addr: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_transport_config() {
        // Basic sanity checks on defaults.
        let config = TransportConfig::default();
        assert!(config.max_frame_bytes > 0);
        assert!(config.max_streams > 0);
    }

    #[test]
    fn connection_info_holds_fields() {
        let info = ConnectionInfo {
            id: ConnectionId(42),
            peer_addr: "127.0.0.1:1234".into(),
        };
        assert_eq!(info.id, ConnectionId(42));
        assert_eq!(info.peer_addr, "127.0.0.1:1234");
    }
}
