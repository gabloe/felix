//! The internal listener's address and the peer mTLS settings.

use super::*;

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
