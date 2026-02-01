//! Control-plane HTTP API module.
//!
//! # Purpose and responsibility
//! Exposes route handler modules and shared helper functions used by the REST
//! API layer.
//!
//! # Where it fits in Felix
//! This module is the entry point for HTTP handlers and shared validation
//! helpers used across tenant/namespace/stream/cache endpoints.
//!
//! # Key invariants and assumptions
//! - Tenant and namespace existence checks must happen before store mutations.
//! - Error shapes are standardized via `api::error`.
//!
//! # Security considerations
//! - Existence checks should not leak data beyond tenant scope.
pub mod bootstrap;
pub mod caches;
pub mod error;
pub mod namespaces;
pub mod openapi;
pub mod regions;
pub mod streams;
pub mod system;
pub mod tenants;
pub mod types;

use crate::api::error::{ApiError, api_internal, api_not_found};
use crate::app::AppState;
use crate::model::NamespaceKey;

/// Ensure a tenant and namespace exist before performing an operation.
///
/// # What it does
/// Checks tenant existence, then namespace existence, and returns a 404 error
/// if either is missing.
///
/// # Why it exists
/// Centralizes resource validation and keeps error messages consistent.
///
/// # Errors
/// - Returns `ApiError` with 404 when tenant/namespace is missing.
/// - Returns 500 when the store fails.
pub(crate) async fn ensure_tenant_namespace(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
) -> Result<(), ApiError> {
    // Validate the tenant first to avoid leaking namespace existence.
    ensure_tenant_exists(state, tenant_id).await?;
    let namespace_key = NamespaceKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
    };
    // Check namespace existence within the tenant scope.
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

/// Ensure a tenant exists before performing an operation.
///
/// # What it does
/// Checks tenant existence and returns a 404 error if missing.
///
/// # Why it exists
/// Centralizes tenant validation to keep handlers consistent.
///
/// # Errors
/// - Returns `ApiError` with 404 when tenant is missing.
/// - Returns 500 when the store fails.
pub(crate) async fn ensure_tenant_exists(
    state: &AppState,
    tenant_id: &str,
) -> Result<(), ApiError> {
    // Check tenant existence via the store.
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
            bootstrap_token: None,
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
