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
use auth::oidc::OidcValidator;
use std::sync::Arc;
use store::{ControlPlaneAuthStore, StoreConfig, memory::InMemoryStore, postgres::PostgresStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let metrics_handle = observability::init_observability("felix-controlplane");

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    let state = build_state(config.clone()).await?;

    tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

    let app = build_router(state.clone());

    if config.bootstrap.enabled {
        let bootstrap_addr = config.bootstrap.bind_addr;
        let bootstrap_app = build_bootstrap_router(state.clone());
        let bootstrap_enabled = state.bootstrap_enabled;
        tokio::spawn(async move {
            tracing::info!(
                %bootstrap_addr,
                enabled = bootstrap_enabled,
                "bootstrap control plane listening"
            );
            let listener = tokio::net::TcpListener::bind(bootstrap_addr).await?;
            axum::serve(listener, bootstrap_app.into_make_service()).await?;
            Ok::<(), anyhow::Error>(())
        });
    }

    let addr = config.bind_addr;
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
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

    tracing::info!(
        backend = store.backend_name(),
        durable = store.is_durable(),
        "control plane store ready"
    );

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
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: config.bootstrap.enabled,
        bootstrap_token: config.bootstrap.token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            bootstrap: config::BootstrapConfig {
                enabled: false,
                bind_addr: "127.0.0.1:0".parse().expect("bootstrap"),
                token: None,
            },
        };
        let err = build_state(config).await.err().expect("missing postgres");
        assert!(err.to_string().contains("postgres configuration missing"));
    }
}
