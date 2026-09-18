//! Control-plane HTTP handlers plus the shared existence checks that run
//! before any store mutation.
pub mod bootstrap;
pub mod caches;
pub mod error;
pub mod namespaces;
pub mod nodes;
pub mod openapi;
pub mod regions;
pub mod streams;
pub mod system;
pub mod tenants;
pub mod types;

use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::app::AppState;
use crate::model::NamespaceKey;

/// 404 unless both the tenant and the namespace exist. The tenant is checked
/// first so a caller can't learn about namespaces in tenants it can't see.
pub(crate) async fn ensure_tenant_namespace(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
) -> Result<(), ApiError> {
    ensure_tenant_exists(state, tenant_id).await?;
    let namespace_key = NamespaceKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
    };
    let exists = state
        .store
        .namespace_exists(&namespace_key)
        .await
        .map_err(|err| api_internal("failed to check namespace existence", &err))?;
    if !exists {
        return Err(api_not_found("namespace not found"));
    }
    Ok(())
}

/// 404 unless the tenant exists.
pub(crate) async fn ensure_tenant_exists(
    state: &AppState,
    tenant_id: &str,
) -> Result<(), ApiError> {
    let exists = state
        .store
        .tenant_exists(tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant existence", &err))?;
    if !exists {
        return Err(api_not_found("tenant not found"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::types::{FeatureFlags, Region};
    use crate::store::memory::InMemoryStore;
    use crate::store::{ControlPlaneStore, StoreConfig};
    use std::sync::Arc;

    fn test_state() -> AppState {
        let store = InMemoryStore::new(StoreConfig {
            changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
        });
        AppState {
            region: Region {
                region_id: "local".to_string(),
                display_name: "Local".to_string(),
            },
            api_version: "v1".to_string(),
            features: FeatureFlags {
                durable_storage: store.is_durable(),
                tiered_storage: false,
                bridges: false,
            },
            store: Arc::new(store),
            oidc_validator: crate::auth::oidc::UpstreamOidcValidator::default(),
            bootstrap_enabled: false,
            bootstrap_tokens: Vec::new(),
            node_liveness: Default::default(),
            readiness: std::sync::Arc::new(crate::readiness::Readiness::new(std::sync::Arc::new(
                crate::readiness::AlwaysReady,
            ))),
            in_flight: Default::default(),
        }
    }

    #[tokio::test]
    async fn ensure_tenant_exists_returns_not_found() {
        let state = test_state();
        let err = ensure_tenant_exists(&state, "missing").await.unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ensure_tenant_namespace_checks_both() {
        let state = test_state();
        let err = ensure_tenant_namespace(&state, "missing", "ns")
            .await
            .unwrap_err();
        assert_eq!(err.status, axum::http::StatusCode::NOT_FOUND);

        // Insert a tenant and namespace, then validate success.
        state
            .store
            .create_tenant(crate::model::Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant".to_string(),
            })
            .await
            .expect("tenant");
        state
            .store
            .create_namespace(crate::model::Namespace {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                display_name: "Default".to_string(),
            })
            .await
            .expect("namespace");

        ensure_tenant_namespace(&state, "t1", "default")
            .await
            .expect("exists");
    }
}
