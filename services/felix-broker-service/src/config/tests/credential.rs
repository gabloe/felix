//! Where the node credential comes from.

use super::*;

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
