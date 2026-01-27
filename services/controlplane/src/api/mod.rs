//! Control-plane HTTP API module.
//!
//! # Purpose
//! Exposes route handler modules and shared helper functions for validating
//! tenant/namespace existence.
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
