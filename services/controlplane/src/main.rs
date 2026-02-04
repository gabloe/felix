//! Felix control-plane HTTP service entry point.
//!
//! # Purpose
//! Wires configuration, storage, auth validators, and HTTP routers, then starts
//! the main API server and (optionally) the bootstrap server.
//!
//! # Notes
//! The `build_state` helper keeps wiring testable and minimizes main setup logic.
mod api;
mod app;
mod auth;
mod config;
mod model;
mod observability;
mod store;

use anyhow::Context;
use api::types::{FeatureFlags, Region};
use app::{AppState, build_bootstrap_router, build_router};
use auth::oidc::UpstreamOidcValidator;
use std::future::Future;
use std::sync::Arc;
use store::{ControlPlaneAuthStore, StoreConfig, memory::InMemoryStore, postgres::PostgresStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    run_with_shutdown(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

async fn run_with_shutdown<F>(config: config::ControlPlaneConfig, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let metrics_handle = observability::init_observability("felix-controlplane");
    let state = build_state(config.clone()).await?;
    let _backend_name = state.store.backend_name();
    let metrics_task = tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

    let app = build_router(state.clone());

    let bootstrap_task = if config.bootstrap.enabled {
        let bootstrap_addr = config.bootstrap.bind_addr;
        let bootstrap_app = build_bootstrap_router(state.clone());
        let bootstrap_enabled = state.bootstrap_enabled;
        Some(tokio::spawn(async move {
            tracing::info!(
                %bootstrap_addr,
                enabled = bootstrap_enabled,
                "bootstrap control plane listening"
            );
            match tokio::net::TcpListener::bind(bootstrap_addr).await {
                Ok(listener) => {
                    let _ = axum::serve(listener, bootstrap_app.into_make_service()).await;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to bind bootstrap listener");
                }
            }
        }))
    } else {
        None
    };

    let addr = config.bind_addr;
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tokio::pin!(shutdown);
    tokio::select! {
        result = axum::serve(listener, app.into_make_service()) => {
            result?;
        }
        _ = &mut shutdown => {}
    }

    metrics_task.abort();
    if let Some(task) = &bootstrap_task {
        task.abort();
    }
    let _ = metrics_task.await;
    if let Some(task) = bootstrap_task {
        let _ = task.await;
    }
    Ok(())
}

async fn build_state(config: config::ControlPlaneConfig) -> anyhow::Result<AppState> {
    let store_config = StoreConfig {
        changes_limit: config.changes_limit,
        change_retention_max_rows: config.change_retention_max_rows,
    };
    let store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = match config.storage {
        config::StorageBackend::Memory => Arc::new(InMemoryStore::new(store_config)),
        config::StorageBackend::Postgres => {
            let pg = config
                .postgres
                .as_ref()
                .context("postgres configuration missing")?;
            Arc::new(PostgresStore::connect(pg, store_config).await?)
        }
    };

    Ok(AppState {
        region: Region {
            region_id: config.region_id,
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store,
        oidc_validator: UpstreamOidcValidator::new_with_allowed_algorithms(
            std::time::Duration::from_secs(3600),
            std::time::Duration::from_secs(3600),
            60,
            config.oidc_allowed_algorithms,
        ),
        bootstrap_enabled: config.bootstrap.enabled,
        bootstrap_token: config.bootstrap.token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    async fn build_state_memory_backend() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
            },
        };
        let state = build_state(config).await.expect("state");
        assert_eq!(state.region.region_id, "local");
        assert!(!state.features.durable_storage);
    }

    #[tokio::test]
    async fn build_state_postgres_requires_config() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Postgres,
            postgres: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
            },
        };
        let err = build_state(config).await.err().expect("missing postgres");
        assert!(err.to_string().contains("postgres configuration missing"));
    }

    #[tokio::test]
    async fn build_state_postgres_attempts_connection_when_config_present() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Postgres,
            postgres: Some(config::PostgresConfig {
                url: "postgres://postgres:postgres@127.0.0.1:1/postgres".to_string(),
                max_connections: 1,
                connect_timeout_ms: 500,
                acquire_timeout_ms: 500,
            }),
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: true,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: Some("bootstrap-token".to_string()),
            },
        };
        let err = build_state(config)
            .await
            .err()
            .expect("connect should fail");
        let text = err.to_string();
        assert!(text.contains("pool") || text.contains("connect") || text.contains("Connection"));
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_starts_and_stops_without_bootstrap() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
            },
        };
        run_with_shutdown(config, async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        })
        .await
        .expect("run should stop cleanly");
    }

    #[tokio::test]
    #[serial]
    async fn run_with_shutdown_starts_and_stops_with_bootstrap() {
        let config = config::ControlPlaneConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind"),
            metrics_bind: "127.0.0.1:0".parse().expect("metrics"),
            region_id: "local".to_string(),
            storage: config::StorageBackend::Memory,
            postgres: None,
            changes_limit: 10,
            change_retention_max_rows: Some(20),
            oidc_allowed_algorithms: vec![jsonwebtoken::Algorithm::ES256],
            bootstrap: config::BootstrapConfig {
                enabled: true,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: Some("bootstrap-token".to_string()),
            },
        };
        run_with_shutdown(config, async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        })
        .await
        .expect("run should stop cleanly");
    }
}
