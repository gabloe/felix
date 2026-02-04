mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::api::bootstrap::BootstrapInitializeRequest;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_bootstrap_router};
use controlplane::auth::oidc::UpstreamOidcValidator;
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
        oidc_validator: UpstreamOidcValidator::default(),
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
            .any(|policy| policy.action == "tenant.manage")
    );
    assert!(
        policies
            .iter()
            .any(|policy| policy.action == "rbac.policy.manage")
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

#[tokio::test]
async fn bootstrap_rejects_invalid_token_and_validation_errors() {
    let (_store, state) = bootstrap_state(true, Some("secret".to_string()));
    let app = build_bootstrap_router(state).into_service();

    let invalid_token = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "wrong")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(invalid_token).await.expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let empty_name = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "   ",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(empty_name).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let empty_admins = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [],
                "initial_admin_principals": []
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.clone().oneshot(empty_admins).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let empty_issuer = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t1/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant One",
                "idp_issuers": [{
                    "issuer": "   ",
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
    let response = app.oneshot(empty_issuer).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bootstrap_missing_configured_token_returns_internal_error() {
    let (_store, state) = bootstrap_state(true, None);
    let app = build_bootstrap_router(state).into_service();

    let request = Request::builder()
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
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn bootstrap_existing_unbootstrapped_tenant_initializes_without_conflict() {
    let (store, state) = bootstrap_state(true, Some("secret".to_string()));
    store
        .create_tenant(controlplane::model::Tenant {
            tenant_id: "t-existing".to_string(),
            display_name: "Existing".to_string(),
        })
        .await
        .expect("seed tenant");

    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t-existing/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Existing",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn bootstrap_respects_preseeded_admin_policies_matching_required_scopes() {
    let (store, state) = bootstrap_state(true, Some("secret".to_string()));
    let app = build_bootstrap_router(state).into_service();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/bootstrap/tenants/t2/initialize")
        .header("content-type", "application/json")
        .header("X-Felix-Bootstrap-Token", "secret")
        .body(Body::from(
            json!({
                "display_name": "Tenant Two",
                "idp_issuers": [],
                "initial_admin_principals": ["p:admin"],
                "policies": [
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "tenant.manage" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.view" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.policy.manage" },
                    { "subject": "role:tenant-admin", "object": "tenant:t2", "action": "rbac.assignment.manage" },
                    { "subject": "role:tenant-admin", "object": "namespace:t2/*", "action": "ns.manage" }
                ]
            })
            .to_string(),
        ))
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let policies = store.list_rbac_policies("t2").await.expect("policies");
    let tenant_manage = policies
        .iter()
        .filter(|policy| {
            policy.subject == "role:tenant-admin"
                && policy.object == "tenant:t2"
                && policy.action == "tenant.manage"
        })
        .count();
    assert_eq!(tenant_manage, 1);

    let ns_manage = policies
        .iter()
        .filter(|policy| {
            policy.subject == "role:tenant-admin"
                && policy.object == "namespace:t2/*"
                && policy.action == "ns.manage"
        })
        .count();
    assert_eq!(ns_manage, 1);
}
