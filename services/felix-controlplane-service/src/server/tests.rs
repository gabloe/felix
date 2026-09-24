use serial_test::serial;

use super::*;
use crate::config::{
    BootstrapConfig, DEFAULT_READINESS_CACHE_TTL_MS, DEFAULT_READINESS_TIMEOUT_MS,
    NodeLivenessConfig, PostgresConfig, StorageBackend,
};

/// Memory-backed, bootstrap off, every port ephemeral.
fn config() -> ControlPlaneConfig {
    ControlPlaneConfig {
        bind_addr: "127.0.0.1:0".parse().expect("bind"),
        metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
        region_id: "local".to_string(),
        storage: StorageBackend::Memory,
        postgres: None,
        raft: None,
        changes_limit: 10,
        change_retention_max_rows: Some(20),
        oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
        bootstrap: BootstrapConfig {
            enabled: false,
            bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
            token: None,
            previous_token: None,
            tls: None,
        },
        node_liveness: NodeLivenessConfig::default(),
        shard_moves: placement::MovePolicy::default(),
        shutdown_drain_timeout_ms: 25_000,
        shutdown_predrain_ms: 0,
        readiness_timeout_ms: DEFAULT_READINESS_TIMEOUT_MS,
        readiness_cache_ttl_ms: DEFAULT_READINESS_CACHE_TTL_MS,
    }
}

fn with_bootstrap(mut config: ControlPlaneConfig) -> ControlPlaneConfig {
    config.bootstrap.enabled = true;
    config.bootstrap.token = Some("bootstrap-token".to_string());
    config
}

#[tokio::test]
async fn build_state_memory_backend() {
    let (state, _raft) = build_state(config(), Readiness::ready(), &Default::default())
        .await
        .expect("state");
    assert_eq!(state.region.region_id, "local");
    assert!(!state.features.durable_storage);
}

#[tokio::test]
async fn build_state_postgres_requires_config() {
    let config = ControlPlaneConfig {
        storage: StorageBackend::Postgres,
        ..config()
    };
    let err = build_state(config, Readiness::ready(), &Default::default())
        .await
        .err()
        .expect("missing postgres");
    assert!(err.to_string().contains("postgres configuration missing"));
}

#[tokio::test]
async fn build_state_postgres_attempts_connection_when_config_present() {
    let config = with_bootstrap(ControlPlaneConfig {
        storage: StorageBackend::Postgres,
        postgres: Some(PostgresConfig {
            url: "postgres://postgres:postgres@127.0.0.1:1/postgres".to_string(),
            max_connections: 1,
            connect_timeout_ms: 500,
            acquire_timeout_ms: 500,
        }),
        ..config()
    });
    let err = build_state(config, Readiness::ready(), &Default::default())
        .await
        .err()
        .expect("connect should fail");
    let text = err.to_string();
    assert!(text.contains("pool") || text.contains("connect") || text.contains("Connection"));
}

#[tokio::test]
#[serial]
async fn run_with_shutdown_starts_and_stops_without_bootstrap() {
    run(config(), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    })
    .await
    .expect("run should stop cleanly");
}

#[tokio::test]
#[serial]
async fn run_with_shutdown_starts_and_stops_with_bootstrap() {
    run(with_bootstrap(config()), async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    })
    .await
    .expect("run should stop cleanly");
}
