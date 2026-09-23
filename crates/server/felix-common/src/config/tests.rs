use super::{LimitsConfig, NodeConfig};
use crate::ids::RegionId;

#[test]
fn node_config_new_sets_fields() {
    let region = RegionId::new();
    let config = NodeConfig::new(region, "127.0.0.1:9000", "/tmp/felix");
    assert_eq!(config.region, region);
    assert_eq!(config.listen_addr, "127.0.0.1:9000");
    assert_eq!(config.data_dir, "/tmp/felix");
    assert!(config.limits.max_message_bytes > 0);
    assert!(config.limits.max_inflight > 0);
}

#[test]
fn limits_config_default_values() {
    let limits = LimitsConfig::default();
    assert_eq!(limits.max_message_bytes, 1024 * 1024);
    assert_eq!(limits.max_inflight, 10_000);
}

#[test]
fn limits_config_custom_values() {
    let limits = LimitsConfig {
        max_message_bytes: 2048,
        max_inflight: 5000,
    };
    assert_eq!(limits.max_message_bytes, 2048);
    assert_eq!(limits.max_inflight, 5000);
}

#[test]
fn node_config_preserves_limits() {
    let region = RegionId::new();
    let mut config = NodeConfig::new(region, "0.0.0.0:8000", "/data");
    config.limits.max_message_bytes = 9999;
    assert_eq!(config.limits.max_message_bytes, 9999);
}
