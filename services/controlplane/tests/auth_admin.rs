mod common;
mod http_helpers;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use common::read_json;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::mint_token;
use controlplane::auth::keys::generate_signing_keys;
use controlplane::auth::oidc::OidcValidator;
use controlplane::store::{
    AuthStore, ControlPlaneAuthStore, ControlPlaneStore, StoreConfig, memory::InMemoryStore,
};
use http_helpers::json_request;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

#[tokio::test]
async fn idp_issuer_and_rbac_admin_endpoints() {
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
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app = build_router(state).into_service();

    let create_tenant = json_request(
        "POST",
        "/v1/tenants",
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(create_tenant).await.expect("tenant");
    assert_eq!(response.status(), StatusCode::CREATED);

    let keys = generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("set keys");
    let token = mint_token(
        &keys,
        "t1",
        "p:admin",
        vec!["tenant.admin:tenant:*".to_string()],
        Duration::from_secs(900),
    )
    .expect("token");

    let upsert_issuer = json_request(
        "POST",
        "/v1/tenants/t1/idp-issuers",
        serde_json::json!({
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": "groups"
            }
        }),
    );
    let upsert_issuer = add_auth(upsert_issuer, &token);
    let response = app.clone().oneshot(upsert_issuer).await.expect("issuer");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let add_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:admin",
            "object": "tenant:*",
            "action": "tenant.admin"
        }),
    );
    let add_policy = add_auth(add_policy, &token);
    let response = app.clone().oneshot(add_policy).await.expect("policy");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let add_grouping = json_request(
        "POST",
        "/v1/tenants/t1/rbac/groupings",
        serde_json::json!({
            "user": "p:alice",
            "role": "role:admin"
        }),
    );
    let add_grouping = add_auth(add_grouping, &token);
    let response = app.clone().oneshot(add_grouping).await.expect("grouping");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_issuer = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/idp-issuers/https%3A%2F%2Fissuer.example.com")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .expect("delete issuer");
    let response = app
        .clone()
        .oneshot(delete_issuer)
        .await
        .expect("delete issuer");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn idp_admin_missing_tenant_returns_404() {
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
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app = build_router(state).into_service();
    let upsert_issuer = json_request(
        "POST",
        "/v1/tenants/missing/idp-issuers",
        serde_json::json!({
            "issuer": "https://issuer.example.com",
            "audiences": ["felix-controlplane"],
            "discovery_url": null,
            "jwks_url": "https://issuer.example.com/.well-known/jwks.json",
            "claim_mappings": {
                "subject_claim": "sub",
                "groups_claim": null
            }
        }),
    );
    let upsert_issuer = add_auth(upsert_issuer, "invalid");
    let response = app.clone().oneshot(upsert_issuer).await.expect("issuer");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "not_found");
}

fn add_auth(mut request: Request<Body>, token: &str) -> Request<Body> {
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("auth header"),
    );
    request
}
