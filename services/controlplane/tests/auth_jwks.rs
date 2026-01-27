mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::keys::generate_signing_keys;
use controlplane::auth::oidc::OidcValidator;
use controlplane::store::{AuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore};
use tower::ServiceExt;

#[tokio::test]
async fn jwks_endpoint_returns_keys_for_tenant() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    store
        .create_tenant(controlplane::model::Tenant {
            tenant_id: "t1".to_string(),
            display_name: "Tenant One".to_string(),
        })
        .await
        .expect("tenant");
    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys)
        .await
        .expect("set keys");

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: std::sync::Arc::new(store),
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app = build_router(state).into_service();

    let req = Request::builder()
        .uri("/v1/tenants/t1/.well-known/jwks.json")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(req).await.expect("jwks");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(!payload["keys"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn jwks_endpoint_missing_tenant_returns_404() {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });

    let state = AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: std::sync::Arc::new(store),
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app = build_router(state).into_service();

    let req = Request::builder()
        .uri("/v1/tenants/missing/.well-known/jwks.json")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(req).await.expect("jwks");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
