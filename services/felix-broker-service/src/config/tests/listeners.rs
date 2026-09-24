//! How many client listeners bind, and on which ports.

use super::*;

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
