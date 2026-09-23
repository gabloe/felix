//! The client config has to honour the `FELIX_PUB_*` knobs the instrument
//! documents.

use super::client_config;

/// **The instrument must respond to the knobs it documents.**
///
/// `optimized_defaults` ignores the environment, so every `FELIX_PUB_*`
/// variable was inert here and a run that set one measured the default
/// instead -- silently, which is how a perf session drew two conclusions
/// from configurations it had never actually run (#553).
#[test]
fn the_client_config_reads_the_environment() {
    // Serialised by the env mutation, so both assertions live in one test.
    unsafe { std::env::remove_var("FELIX_PUB_CONN_POOL") };
    let default_pool = client_config("t1", "token")
        .expect("config")
        .publish_conn_pool;

    unsafe { std::env::set_var("FELIX_PUB_CONN_POOL", "17") };
    let configured = client_config("t1", "token").expect("config");
    unsafe { std::env::remove_var("FELIX_PUB_CONN_POOL") };

    assert_ne!(
        default_pool, 17,
        "pick a probe value the default is not, or this proves nothing"
    );
    assert_eq!(
        configured.publish_conn_pool, 17,
        "FELIX_PUB_CONN_POOL did not reach the client"
    );
    // An unset environment must still measure a default client, so every
    // number taken before this change stays comparable.
    assert_eq!(
        client_config("t1", "token")
            .expect("config")
            .publish_conn_pool,
        default_pool
    );
}
