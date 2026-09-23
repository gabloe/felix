use super::*;
use crate::store::StoreConfig;
use crate::store::memory::InMemoryStore;
use std::sync::Arc;

fn test_state() -> AppState {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    crate::test_support::app_state_ready(Arc::new(store))
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
