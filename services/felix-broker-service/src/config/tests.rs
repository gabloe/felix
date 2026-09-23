use super::file::BrokerConfigOverride;
use super::*;
use felix_broker::SubQueuePolicy;
use serial_test::serial;
use std::env;
use std::fs;
use tempfile::TempDir;

/// A broker with no identity is a single node, and registering one would
/// put a node in the catalog placement would then try to use.
#[serial]
#[test]
fn membership_is_off_without_a_node_id() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    assert!(
        BrokerConfig::from_env()
            .expect("config")
            .membership
            .is_none()
    );
}

/// A broker that guessed its advertised address would register something
/// unreachable, and the failure would surface later as peers unable to
/// connect to a node the catalog says is live.
#[serial]
#[test]
fn a_node_id_without_an_advertise_address_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(
        err.to_string().contains("FELIX_NODE_ADVERTISE_ADDR"),
        "{err}"
    );
}

/// Rejected at startup rather than as a failed registration once everything
/// else is already running.
#[serial]
#[test]
fn a_malformed_advertise_address_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "not-an-address");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("valid host:port"), "{err}");
}

#[serial]
#[test]
fn a_node_id_without_a_control_plane_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("FELIX_CONTROLPLANE_URL"), "{err}");
}

#[serial]
#[test]
fn a_complete_identity_is_accepted() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_REGION_ID", "eu-central-1");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
    }
    let membership = BrokerConfig::from_env()
        .expect("config")
        .membership
        .expect("membership");
    assert_eq!(membership.node_id, "broker-a");
    assert_eq!(membership.advertise_addr, "10.0.0.4:7000");
    assert_eq!(membership.region, "eu-central-1");
}

/// A broker with an identity and no credential cannot register. Starting it
/// to fail every control-plane call on a loop is worse than refusing.
#[serial]
#[test]
fn an_identity_without_a_credential_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("FELIX_NODE_TOKEN"), "{err}");
}

/// A broker in a cluster listens for peers; one on its own has no peers to
/// listen for, so it binds nothing.
#[serial]
#[test]
fn the_internal_listener_is_configured_only_for_a_cluster_member() {
    clear_felix_env();
    assert!(
        BrokerConfig::from_env()
            .expect("config")
            .peer_transport
            .is_none(),
        "a standalone broker must not bind an internal listener",
    );

    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5001");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5001");
    }
    let peer = BrokerConfig::from_env()
        .expect("config")
        .peer_transport
        .expect("peer transport");
    assert_eq!(peer.bind.to_string(), "0.0.0.0:5001");
}

/// The two roles must not be reachable at the same place. Sharing a port
/// would put client traffic and peer traffic on one listener, which is the
/// separation the internal protocol exists to keep.
#[serial]
#[test]
fn an_internal_listener_sharing_the_client_port_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5000");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("share a port"), "{err}");
}

/// A listener range must clear the internal port too. The collision is
/// easier to hit than the single-port one -- the port that clashes is one
/// nobody wrote down, it is merely `quic_bind + n`.
#[serial]
#[test]
fn an_internal_listener_inside_the_client_range_fails_startup() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5010");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
        // Inside 5000-5003, but not equal to 5000, so only the range check
        // catches it.
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5002");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("5000-5003"), "{err}");
    clear_felix_env();
}

/// Just past the range is fine -- the check must not be off by one.
#[serial]
#[test]
fn an_internal_listener_just_past_the_client_range_is_accepted() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:5004");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:5000");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
        env::set_var("FELIX_INTERNAL_BIND", "0.0.0.0:5004");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(config.quic_listeners, 4);
    clear_felix_env();
}

/// The default is one listener bound at exactly the configured address, so
/// a deployment that has not asked for more is unchanged.
#[serial]
#[test]
fn one_listener_by_default_binds_only_the_configured_address() {
    clear_felix_env();
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(config.quic_listeners, 1);
    assert_eq!(
        config
            .quic_binds()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["0.0.0.0:5000"],
    );
}

#[serial]
#[test]
fn listeners_occupy_consecutive_ports_from_the_bind_address() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "127.0.0.1:7000");
        env::set_var("FELIX_QUIC_LISTENERS", "3");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert_eq!(
        config
            .quic_binds()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"],
    );
    clear_felix_env();
}

/// Refused rather than clamped: a broker that silently bound fewer
/// listeners than asked reads as the feature not working.
#[serial]
#[test]
fn a_listener_range_past_the_last_port_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_BIND", "0.0.0.0:65534");
        env::set_var("FELIX_QUIC_LISTENERS", "4");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("65535"), "{err}");
    clear_felix_env();
}

#[serial]
#[test]
fn zero_listeners_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_QUIC_LISTENERS", "0");
    }
    let err = BrokerConfig::from_env().expect_err("should fail");
    assert!(err.to_string().contains("serves nothing"), "{err}");
    clear_felix_env();
}

/// A token can arrive as a mounted secret rather than an environment
/// variable visible in a process listing.
#[serial]
#[test]
fn a_credential_can_come_from_a_file() {
    let dir = TempDir::new().expect("dir");
    let path = dir.path().join("node.token");
    fs::write(&path, "  file-token\n").expect("write");

    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN_FILE", path.to_str().expect("path"));
    }
    let config = BrokerConfig::from_env().expect("config");
    assert!(config.membership.is_some(), "membership");
    assert_eq!(
        config.controlplane_token, "file-token",
        "surrounding whitespace is trimmed"
    );
}

/// A standalone broker can still carry a credential: the metadata feeds
/// it syncs from require one, whether or not it joins a cluster.
#[serial]
#[test]
fn a_credential_without_a_node_id_is_kept_for_the_sync() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "sync-token");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert!(config.membership.is_none());
    assert_eq!(config.controlplane_token, "sync-token");
}

/// Peer mTLS is all three variables or none: one or two would look
/// secured while either presenting nothing or verifying nothing.
#[serial]
#[test]
fn a_partly_configured_peer_mtls_is_refused() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
    }
    let err = BrokerConfig::from_env().expect_err("two of three accepted");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("FELIX_INTERNAL_TLS_CA not set"),
        "{rendered}"
    );
}

/// With peer mTLS the node id is the certificate's DNS name, so a node id
/// no certificate can carry -- here a label ending in a hyphen -- is
/// refused at startup, not at the first dial.
#[serial]
#[test]
fn a_node_id_that_is_not_a_dns_name_is_refused_under_peer_mtls() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
        env::set_var("FELIX_INTERNAL_TLS_CA", "/etc/felix/peer/ca.crt");
    }
    let err = BrokerConfig::from_env().expect_err("a trailing hyphen is not a DNS name");
    assert!(
        format!("{err:#}").contains("not a valid DNS name"),
        "{err:#}"
    );

    // The same id is fine without mTLS, where nothing names it.
    unsafe {
        env::remove_var("FELIX_INTERNAL_TLS_CERT");
        env::remove_var("FELIX_INTERNAL_TLS_KEY");
        env::remove_var("FELIX_INTERNAL_TLS_CA");
    }
    let config = BrokerConfig::from_env().expect("config");
    assert!(config.peer_transport.expect("peer").tls.is_none());
}

#[serial]
#[test]
fn the_three_peer_mtls_paths_reach_the_config() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "a-node-token");
        env::set_var("FELIX_INTERNAL_TLS_CERT", "/etc/felix/peer/tls.crt");
        env::set_var("FELIX_INTERNAL_TLS_KEY", "/etc/felix/peer/tls.key");
        env::set_var("FELIX_INTERNAL_TLS_CA", "/etc/felix/peer/ca.crt");
    }
    let config = BrokerConfig::from_env().expect("config");
    let tls = config.peer_transport.expect("peer").tls.expect("tls");
    assert_eq!(tls.cert_path, "/etc/felix/peer/tls.crt");
    assert_eq!(tls.key_path, "/etc/felix/peer/tls.key");
    assert_eq!(tls.ca_path, "/etc/felix/peer/ca.crt");
}

#[serial]
#[test]
fn a_blank_credential_is_no_credential() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_NODE_ID", "broker-a");
        env::set_var("FELIX_NODE_ADVERTISE_ADDR", "10.0.0.4:7000");
        env::set_var("FELIX_CONTROLPLANE_URL", "http://localhost:8443");
        env::set_var("FELIX_NODE_TOKEN", "   ");
    }
    assert!(BrokerConfig::from_env().is_err());
}

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
    assert_eq!(config.pub_inflight_bytes, DEFAULT_PUB_INFLIGHT_BYTES);
    assert_eq!(config.core_shards, 0);
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

/// **The ack waiter outlasts the quorum wait it may be sitting on.** Giving
/// up first reports a timeout for a publish the broker is still correctly
/// waiting for, and throws away the quorum wait's more specific answer.
#[test]
fn the_ack_wait_outlasts_the_quorum_wait() {
    let config = BrokerConfig::default();
    assert!(
        config.ack_wait_timeout().as_millis() as u64 > config.publish_quorum_timeout_ms,
        "ack wait {:?} does not outlast the quorum wait {}ms",
        config.ack_wait_timeout(),
        config.publish_quorum_timeout_ms,
    );
}

/// A configured ack wait longer than the quorum wait is kept as configured.
#[test]
fn a_longer_configured_ack_wait_is_left_alone() {
    let config = BrokerConfig {
        ack_wait_timeout_ms: 30_000,
        publish_quorum_timeout_ms: 5_000,
        ..BrokerConfig::default()
    };
    assert_eq!(config.ack_wait_timeout().as_millis() as u64, 30_000);
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
fn from_env_respects_core_shards() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_CORE_SHARDS", "4");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.core_shards, 4);

    clear_felix_env();
}

#[serial]
#[test]
fn from_env_respects_pub_inflight_bytes() {
    clear_felix_env();
    unsafe {
        env::set_var("FELIX_BROKER_PUBLISH_INFLIGHT_BYTES", "8388608");
    }

    let config = BrokerConfig::from_env().expect("from_env");
    assert_eq!(config.pub_inflight_bytes, 8388608);

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
pub_inflight_bytes: 134217728
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
    assert_eq!(config.pub_inflight_bytes, 134217728);
    assert_eq!(config.subscriber_queue_capacity, 96);
    assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::Block);
    assert!(!config.subscriber_single_writer_per_conn);

    clear_felix_env();
}

/// Pairs that are each fine alone and wrong together.
///
/// The defaults are all correctly ordered, so these only fire for someone
/// who inverted one — which is exactly the case that produced behaviour
/// nobody configured and no error to explain it.
mod cross_field {
    use super::*;

    #[test]
    fn the_defaults_are_valid() {
        // If this ever fails, a default was changed into a contradiction
        // and every broker would refuse to start.
        BrokerConfig::default().validate().expect("defaults");
    }

    /// The pair this module exists for, in its newest form: each setting
    /// is fine alone, and together they put four listeners on one thread.
    #[test]
    fn an_io_runtime_pool_too_small_for_the_listeners_is_refused() {
        let config = BrokerConfig {
            quic_listeners: 4,
            io_runtime_threads: Some(1),
            ..BrokerConfig::default()
        };
        let err = config
            .validate()
            .expect_err("1 runtime cannot serve 4 listeners");
        let message = format!("{err:#}");
        assert!(message.contains("FELIX_IO_RUNTIME_THREADS"), "{message}");
        assert!(message.contains("FELIX_QUIC_LISTENERS"), "{message}");
        // Says what to use instead, rather than only what is wrong.
        assert!(message.contains("Use 5"), "{message}");
    }

    /// Zero is the documented way to turn the pool off and put drivers back
    /// on the app runtime, where tokio spreads them. Not a conflict.
    #[test]
    fn turning_the_pool_off_is_not_a_conflict() {
        BrokerConfig {
            quic_listeners: 8,
            io_runtime_threads: Some(0),
            ..BrokerConfig::default()
        }
        .validate()
        .expect("0 disables the pool");
    }

    /// Unset is the ordinary case: nothing to disagree with, because the
    /// pool is derived from the listener count.
    #[test]
    fn an_underived_pool_is_not_a_conflict() {
        BrokerConfig {
            quic_listeners: 8,
            io_runtime_threads: None,
            ..BrokerConfig::default()
        }
        .validate()
        .expect("unset derives");
    }

    /// A pool sized exactly right is accepted, so the error names a number
    /// that actually works.
    #[test]
    fn a_pool_sized_for_the_listeners_is_accepted() {
        let config = BrokerConfig {
            quic_listeners: 4,
            io_runtime_threads: Some(5),
            ..BrokerConfig::default()
        };
        assert_eq!(config.server_endpoints(), 4);
        config.validate().expect("5 runtimes serve 4 listeners");
    }

    #[test]
    fn a_batch_larger_than_a_frame_is_refused() {
        let config = BrokerConfig {
            max_frame_bytes: 64 * 1024,
            event_batch_max_bytes: 128 * 1024,
            ..BrokerConfig::default()
        };
        let err = config
            .validate()
            .expect_err("a batch cannot exceed a frame");
        let message = format!("{err:#}");
        assert!(message.contains("event_batch_max_bytes"), "{message}");
        assert!(message.contains("max_frame_bytes"), "{message}");
    }

    #[test]
    fn a_per_connection_limit_above_the_broker_wide_one_is_refused() {
        let config = BrokerConfig {
            pub_inflight_bytes: 1024,
            pub_conn_inflight_bytes: 2048,
            ..BrokerConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_stream_window_above_the_connection_window_is_refused() {
        let config = BrokerConfig {
            cache_conn_recv_window: 1024,
            cache_stream_recv_window: 2048,
            ..BrokerConfig::default()
        };
        assert!(config.validate().is_err());
    }

    /// Equal is fine everywhere. The limits bound each other; they do not
    /// have to differ, and refusing equality would fail a configuration
    /// that behaves exactly as written.
    use base64::Engine;

    fn expiring_token(exp: i64) -> String {
        let encode = |value: &serde_json::Value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
        };
        format!(
            "{}.{}.{}",
            encode(&serde_json::json!({"alg": "EdDSA", "typ": "JWT"})),
            encode(&serde_json::json!({"tid": "acme", "exp": exp, "sub": "node-1"})),
            "not-a-real-signature",
        )
    }

    fn joining_with(token: &str, refresh: bool, token_file: bool) -> BrokerConfig {
        BrokerConfig {
            controlplane_token: token.to_string(),
            membership: Some(MembershipConfig {
                node_id: "broker-a".to_string(),
                advertise_addr: "10.0.0.1:5000".to_string(),
                client_advertise_addr: None,
                refresh_token_file: refresh.then(|| "/run/felix/refresh".into()),
                node_token_file: token_file.then(|| "/run/felix/node.token".into()),
                region: "us-west-2".to_string(),
            }),
            ..BrokerConfig::default()
        }
    }

    /// The outage this check exists to move forward in time.
    ///
    /// The heartbeat carries this token and the heartbeat is the lease
    /// renewal, so an expiring credential nothing can renew is a broker
    /// that stops serving its shards at a time already determined. Saying
    /// so at startup costs a failed rollout; not saying so costs an
    /// incident an hour later with no change to blame.
    #[test]
    fn an_expiring_credential_with_no_way_to_renew_it_is_refused() {
        let config = joining_with(&expiring_token(1_700_000_900), false, false);
        let err = config.validate().expect_err("should refuse");
        let message = err.to_string();
        assert!(message.contains("nothing can renew it"), "{message}");
        // Names both ways out, because the error is the only place an
        // operator meets this.
        assert!(
            message.contains("FELIX_NODE_REFRESH_TOKEN_FILE"),
            "{message}"
        );
        assert!(message.contains("FELIX_NODE_TOKEN_FILE"), "{message}");
    }

    #[test]
    fn a_refresh_file_makes_an_expiring_credential_fine() {
        joining_with(&expiring_token(1_700_000_900), true, false)
            .validate()
            .expect("refresh renews it");
    }

    /// A file is a seam something else can write; a value is not. That is
    /// the whole distinction the check turns on, so it is asserted rather
    /// than implied.
    #[test]
    fn a_token_file_makes_an_expiring_credential_fine() {
        joining_with(&expiring_token(1_700_000_900), false, true)
            .validate()
            .expect("an external rotator can renew it");
    }

    #[test]
    fn a_credential_that_never_expires_is_left_alone() {
        // Not a Felix token, so there is no `exp` to act on. Guessing would
        // refuse a deployment whose credential this broker cannot read and
        // has no business judging.
        joining_with("opaque-token", false, false)
            .validate()
            .expect("nothing to schedule against");
    }

    #[test]
    fn a_broker_not_joining_a_cluster_is_left_alone() {
        // No membership means no heartbeat and no lease, so an expiring
        // credential costs it nothing.
        let config = BrokerConfig {
            controlplane_token: expiring_token(1_700_000_900),
            membership: None,
            ..BrokerConfig::default()
        };
        config.validate().expect("no cluster to fall out of");
    }

    #[test]
    fn equal_limits_are_allowed() {
        let config = BrokerConfig {
            max_frame_bytes: 64 * 1024,
            event_batch_max_bytes: 64 * 1024,
            pub_inflight_bytes: 4096,
            pub_conn_inflight_bytes: 4096,
            cache_conn_recv_window: 8192,
            cache_stream_recv_window: 8192,
            ..BrokerConfig::default()
        };
        config.validate().expect("equal limits");
    }
}

/// What `--print-config` renders, and the one thing it must never render.
mod printing {
    use super::*;

    fn with_membership(token: &str) -> BrokerConfig {
        BrokerConfig {
            controlplane_token: token.to_string(),
            membership: Some(MembershipConfig {
                node_id: "broker-a".to_string(),
                advertise_addr: "10.0.0.1:5000".to_string(),
                client_advertise_addr: None,
                refresh_token_file: None,
                node_token_file: None,
                region: "us-west-2".to_string(),
            }),
            ..BrokerConfig::default()
        }
    }

    /// **The credential never appears.**
    ///
    /// `--print-config` exists to be pasted into an issue, so a token that
    /// reaches the output has been published. This is the assertion that
    /// has to hold even if every other field's rendering changes.
    #[test]
    fn the_credential_is_never_printed() {
        let rendered =
            serde_yaml_ng::to_string(&with_membership("super-secret-value")).expect("render");
        assert!(
            !rendered.contains("super-secret-value"),
            "the credential reached the output:\n{rendered}",
        );
        assert!(rendered.contains("controlplane_token: <redacted>"));
    }

    /// Redacted, not omitted: whether a token is set at all is exactly what
    /// someone debugging a registration failure needs to see.
    #[test]
    fn an_absent_credential_says_so_rather_than_vanishing() {
        let rendered = serde_yaml_ng::to_string(&with_membership("")).expect("render");
        assert!(
            rendered.contains("controlplane_token: <unset>"),
            "{rendered}"
        );
    }

    /// Durations come out in the unit their variables are named for.
    /// Serde's default for `Duration` is `{ secs, nanos }`, which is
    /// unreadable beside `FELIX_PEER_REQUEST_TIMEOUT_MS`.
    #[test]
    fn peer_timeouts_are_printed_as_milliseconds() {
        let config = BrokerConfig {
            peer_transport: Some(crate::peer::PeerTransportConfig {
                request_timeout: std::time::Duration::from_millis(2500),
                ..crate::peer::PeerTransportConfig::default()
            }),
            ..BrokerConfig::default()
        };
        let rendered = serde_yaml_ng::to_string(&config).expect("render");
        assert!(rendered.contains("request_timeout: 2500"), "{rendered}");
    }
}

/// The YAML config file, folded over what the environment already gave.
///
/// Driven through `apply` rather than a file so the whole precedence table
/// is one test rather than one per key — an override that is parsed but
/// never assigned is otherwise invisible until an operator sets it.
mod yaml_overrides {
    use super::*;

    fn parse(yaml: &str) -> BrokerConfigOverride {
        serde_yaml_ng::from_str(yaml).expect("parse the override")
    }

    /// **A key nobody reads is refused, not ignored.**
    ///
    /// The test above proves every key that parses reaches the config. This
    /// is the other half: a key that does *not* parse must say so. Without
    /// it an operator who writes `metrics_bnid` gets the default, no error,
    /// and a broker listening somewhere they did not ask for — and the file
    /// they are looking at says otherwise.
    #[test]
    fn a_key_the_broker_does_not_know_is_refused() {
        let err = serde_yaml_ng::from_str::<BrokerConfigOverride>("metrics_bnid: \"0.0.0.0:1\"")
            .expect_err("a misspelled key must not be accepted");
        assert!(
            err.to_string().contains("metrics_bnid"),
            "the error has to name the key, or it cannot be acted on: {err}",
        );
    }

    /// **Every key in the file has to reach the config.** A key that
    /// deserializes and is then never assigned looks like a setting that
    /// silently does nothing.
    #[test]
    fn every_key_in_the_file_reaches_the_config() {
        // The flags start false so the assertions below prove an
        // assignment rather than agreeing with a default that is already
        // true.
        let mut config = BrokerConfig {
            ack_on_commit: false,
            disable_timings: false,
            pub_ingress_wait: false,
            subscriber_single_writer_per_conn: false,
            ..Default::default()
        };
        config
            .apply(parse(
                r#"
            quic_bind: "127.0.0.1:5999"
            metrics_bind: "127.0.0.1:8999"
            controlplane_url: "http://cp.example:8443"
            controlplane_sync_interval_ms: 1004
            ack_on_commit: true
            max_frame_bytes: 1006
            publish_queue_wait_timeout_ms: 1007
            ack_wait_timeout_ms: 1008
            disable_timings: true
            control_stream_drain_timeout_ms: 1010
            shutdown_drain_timeout_ms: 1011
            cache_conn_recv_window: 1012
            cache_stream_recv_window: 1013
            cache_send_window: 1014
            event_batch_max_events: 1015
            event_batch_max_bytes: 1016
            event_batch_max_delay_us: 1017
            fanout_batch_size: 1018
            pub_workers_per_conn: 1019
            pub_queue_depth: 1020
            pub_inflight_bytes: 1021
            pub_conn_inflight_bytes: 1022
            pub_ingress_wait: true
            core_shards: 1024
            subscriber_queue_capacity: 1025
            max_subscriptions_per_conn: 1026
            subscriber_queue_policy: "drop_old"
            subscriber_writer_lanes: 1028
            subscriber_lane_queue_depth: 1029
            subscriber_lane_queue_policy: "block"
            max_subscriber_writer_lanes: 1031
            subscriber_lane_shard: "round_robin_pin"
            subscriber_single_writer_per_conn: true
            subscriber_flush_max_items: 1034
            subscriber_flush_max_delay_us: 1035
            subscriber_max_bytes_per_write: 1036
            sub_streams_per_conn: 1037
            sub_stream_mode: "hashed_pool"
"#,
            ))
            .expect("apply");

        assert_eq!(config.quic_bind, "127.0.0.1:5999".parse().unwrap());
        assert_eq!(config.metrics_bind, "127.0.0.1:8999".parse().unwrap());
        assert_eq!(
            config.controlplane_url.as_deref(),
            Some("http://cp.example:8443")
        );
        assert_eq!(config.controlplane_sync_interval_ms, 1004);
        assert!(config.ack_on_commit);
        assert_eq!(config.max_frame_bytes, 1006);
        assert_eq!(config.publish_queue_wait_timeout_ms, 1007);
        assert_eq!(config.ack_wait_timeout_ms, 1008);
        assert!(config.disable_timings);
        assert_eq!(config.control_stream_drain_timeout_ms, 1010);
        assert_eq!(config.shutdown_drain_timeout_ms, 1011);
        assert_eq!(config.cache_conn_recv_window, 1012);
        assert_eq!(config.cache_stream_recv_window, 1013);
        assert_eq!(config.cache_send_window, 1014);
        assert_eq!(config.event_batch_max_events, 1015);
        assert_eq!(config.event_batch_max_bytes, 1016);
        assert_eq!(config.event_batch_max_delay_us, 1017);
        assert_eq!(config.fanout_batch_size, 1018);
        assert_eq!(config.pub_workers_per_conn, 1019);
        assert_eq!(config.pub_queue_depth, 1020);
        assert_eq!(config.pub_inflight_bytes, 1021);
        assert_eq!(config.pub_conn_inflight_bytes, 1022);
        assert!(config.pub_ingress_wait);
        assert_eq!(config.core_shards, 1024);
        assert_eq!(config.subscriber_queue_capacity, 1025);
        assert_eq!(config.max_subscriptions_per_conn, 1026);
        assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);
        assert_eq!(config.subscriber_writer_lanes, 1028);
        assert_eq!(config.subscriber_lane_queue_depth, 1029);
        assert_eq!(config.subscriber_lane_queue_policy, SubQueuePolicy::Block);
        assert_eq!(config.max_subscriber_writer_lanes, 1031);
        assert_eq!(
            config.subscriber_lane_shard,
            SubscriberLaneShard::RoundRobinPin
        );
        assert!(config.subscriber_single_writer_per_conn);
        assert_eq!(config.subscriber_flush_max_items, 1034);
        assert_eq!(config.subscriber_flush_max_delay_us, 1035);
        assert_eq!(config.subscriber_max_bytes_per_write, 1036);
        assert_eq!(config.sub_streams_per_conn, 1037);
        assert_eq!(config.sub_stream_mode, SubStreamMode::HashedPool);
    }

    /// A file that names no keys changes nothing. Absent is not zero: the
    /// environment's value has to survive a config file that says nothing
    /// about it.
    #[test]
    fn an_empty_file_leaves_the_config_alone() {
        let mut config = BrokerConfig {
            controlplane_url: Some("http://from-the-environment".to_string()),
            core_shards: 7,
            ..Default::default()
        };

        config.apply(parse("{}")).expect("apply");

        assert_eq!(
            config.controlplane_url.as_deref(),
            Some("http://from-the-environment")
        );
        assert_eq!(config.core_shards, 7);
    }

    /// **A zero does not disable a subsystem.** These are sizes and lane
    /// counts: zero lanes or a zero-capacity queue is not a smaller
    /// configuration, it is a broker that delivers nothing, so a file
    /// asking for one is ignored rather than obeyed.
    #[test]
    fn a_zero_is_ignored_where_zero_would_mean_off() {
        let defaults = BrokerConfig::default();
        let mut config = BrokerConfig::default();

        config
            .apply(parse(
                r#"
            cache_conn_recv_window: 0
            cache_send_window: 0
            cache_stream_recv_window: 0
            event_batch_max_bytes: 0
            event_batch_max_events: 0
            fanout_batch_size: 0
            max_subscriber_writer_lanes: 0
            max_subscriptions_per_conn: 0
            pub_conn_inflight_bytes: 0
            pub_inflight_bytes: 0
            pub_queue_depth: 0
            pub_workers_per_conn: 0
            shutdown_drain_timeout_ms: 0
            sub_streams_per_conn: 0
            subscriber_flush_max_items: 0
            subscriber_lane_queue_depth: 0
            subscriber_max_bytes_per_write: 0
            subscriber_queue_capacity: 0
            subscriber_writer_lanes: 0
"#,
            ))
            .expect("apply");

        assert_eq!(
            config.cache_conn_recv_window, defaults.cache_conn_recv_window,
            "cache_conn_recv_window was disabled by a zero"
        );
        assert_eq!(
            config.cache_send_window, defaults.cache_send_window,
            "cache_send_window was disabled by a zero"
        );
        assert_eq!(
            config.cache_stream_recv_window, defaults.cache_stream_recv_window,
            "cache_stream_recv_window was disabled by a zero"
        );
        assert_eq!(
            config.event_batch_max_bytes, defaults.event_batch_max_bytes,
            "event_batch_max_bytes was disabled by a zero"
        );
        assert_eq!(
            config.event_batch_max_events, defaults.event_batch_max_events,
            "event_batch_max_events was disabled by a zero"
        );
        assert_eq!(
            config.fanout_batch_size, defaults.fanout_batch_size,
            "fanout_batch_size was disabled by a zero"
        );
        assert_eq!(
            config.max_subscriber_writer_lanes, defaults.max_subscriber_writer_lanes,
            "max_subscriber_writer_lanes was disabled by a zero"
        );
        assert_eq!(
            config.max_subscriptions_per_conn, defaults.max_subscriptions_per_conn,
            "max_subscriptions_per_conn was disabled by a zero"
        );
        assert_eq!(
            config.pub_conn_inflight_bytes, defaults.pub_conn_inflight_bytes,
            "pub_conn_inflight_bytes was disabled by a zero"
        );
        assert_eq!(
            config.pub_inflight_bytes, defaults.pub_inflight_bytes,
            "pub_inflight_bytes was disabled by a zero"
        );
        assert_eq!(
            config.pub_queue_depth, defaults.pub_queue_depth,
            "pub_queue_depth was disabled by a zero"
        );
        assert_eq!(
            config.pub_workers_per_conn, defaults.pub_workers_per_conn,
            "pub_workers_per_conn was disabled by a zero"
        );
        assert_eq!(
            config.shutdown_drain_timeout_ms, defaults.shutdown_drain_timeout_ms,
            "shutdown_drain_timeout_ms was disabled by a zero"
        );
        assert_eq!(
            config.sub_streams_per_conn, defaults.sub_streams_per_conn,
            "sub_streams_per_conn was disabled by a zero"
        );
        assert_eq!(
            config.subscriber_flush_max_items, defaults.subscriber_flush_max_items,
            "subscriber_flush_max_items was disabled by a zero"
        );
        assert_eq!(
            config.subscriber_lane_queue_depth, defaults.subscriber_lane_queue_depth,
            "subscriber_lane_queue_depth was disabled by a zero"
        );
        assert_eq!(
            config.subscriber_max_bytes_per_write, defaults.subscriber_max_bytes_per_write,
            "subscriber_max_bytes_per_write was disabled by a zero"
        );
        assert_eq!(
            config.subscriber_queue_capacity, defaults.subscriber_queue_capacity,
            "subscriber_queue_capacity was disabled by a zero"
        );
        assert_eq!(
            config.subscriber_writer_lanes, defaults.subscriber_writer_lanes,
            "subscriber_writer_lanes was disabled by a zero"
        );
    }

    /// An address that does not parse fails the load and says which key,
    /// rather than leaving the broker bound somewhere the operator did not
    /// ask for.
    #[test]
    fn an_unparseable_address_names_the_key_it_came_from() {
        let mut config = BrokerConfig::default();
        let err = config
            .apply(parse("quic_bind: not-an-address"))
            .expect_err("an unparseable address should fail");
        assert!(err.to_string().contains("quic_bind"), "{err}");

        let mut config = BrokerConfig::default();
        let err = config
            .apply(parse("metrics_bind: also-not-an-address"))
            .expect_err("an unparseable address should fail");
        assert!(err.to_string().contains("metrics_bind"), "{err}");
    }

    /// An unknown queue policy leaves the configured one in place. The
    /// alternative is a typo silently switching a broker's overflow
    /// behaviour to something the operator did not choose.
    #[test]
    fn an_unknown_queue_policy_leaves_the_configured_one() {
        let mut config = BrokerConfig {
            subscriber_queue_policy: SubQueuePolicy::DropOld,
            ..Default::default()
        };

        config
            .apply(parse("subscriber_queue_policy: drop_everything"))
            .expect("apply");

        assert_eq!(config.subscriber_queue_policy, SubQueuePolicy::DropOld);
    }
}
