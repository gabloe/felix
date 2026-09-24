use super::*;

#[test]
fn default_log_config_values() {
    let config = LogConfig::default();
    assert_eq!(config.segment_size_bytes, 256 * 1024 * 1024);
    assert_eq!(config.index_spacing_bytes, 4 * 1024);
    assert_eq!(
        config.fsync_mode,
        FsyncMode::Periodic {
            interval: Duration::from_millis(250)
        }
    );
}
