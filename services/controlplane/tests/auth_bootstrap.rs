mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::api::bootstrap::BootstrapInitializeRequest;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_bootstrap_router};
use controlplane::auth::oidc::OidcValidator;
use controlplane::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore,
};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

fn bootstrap_state(enabled: bool, token: Option<String>) -> (Arc<InMemoryStore>, AppState) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    }));
    let state_store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = store.clone();
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
        store: state_store,
        oidc_validator: OidcValidator::default(),
        bootstrap_enabled: enabled,
        bootstrap_token: token,
    };
    (store, state)
}

#[tokio::test]
async fn bootstrap_disabled_returns_404() {
    let (_store, state) = bootstrap_state(false, None);
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&BootstrapInitializeRequest {
                display_name: "Tenant One".to_string(),
                idp_issuers: Vec::new(),
                initial_admin_principals: vec!["p:admin".to_string()],
                policies: Vec::new(),
                groupings: Vec::new(),
            })
            .unwrap(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "not_enabled");
}

#[tokio::test]
async fn bootstrap_requires_token() {
    let (_store, state) = bootstrap_state(true, Some("secret".to_string()));
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_string(&BootstrapInitializeRequest {
                display_name: "Tenant One".to_string(),
                idp_issuers: Vec::new(),
                initial_admin_principals: vec!["p:admin".to_string()],
                policies: Vec::new(),
                groupings: Vec::new(),
            })
            .unwrap(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bootstrap_initializes_tenant_and_auth() {
    let (store, state) = bootstrap_state(true, Some("secret".to_string()));
    let app = build_bootstrap_router(state).into_service();

    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [{
                    "issuer": "https://issuer.example.com",
                    "audiences": ["felix-controlplane"],
                    "discovery_url": null,
                    "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
                    "claim_mappings": {
                        "subject_claim": "sub",
                        "groups_claim": "groups"
                    }
                }],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["status"], "initialized");

    let issuers = store.list_idp_issuers("t1").await.expect("issuers");
    assert_eq!(issuers.len(), 1);
    let policies = store.list_rbac_policies("t1").await.expect("policies");
    assert!(
        policies
            .iter()
            .any(|policy| policy.action == "tenant.admin")
    );
    let groupings = store.list_rbac_groupings("t1").await.expect("groupings");
    assert!(groupings.iter().any(|g| g.user == "p:admin"));
    let keys = store.get_tenant_signing_keys("t1").await.expect("keys");
    assert!(!keys.current.kid.is_empty());
    let bootstrapped = store
        .tenant_auth_is_bootstrapped("t1")
        .await
        .expect("bootstrapped");
    assert!(bootstrapped);

    let second = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(second).await.expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "already_initialized");
}
