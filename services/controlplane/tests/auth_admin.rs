mod common;
mod http_helpers;

use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use common::read_json;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::{TenantSigningKeys, mint_token};
use controlplane::auth::keys::generate_signing_keys;
use controlplane::auth::oidc::UpstreamOidcValidator;
use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use controlplane::store::{AuthStore, ControlPlaneAuthStore, StoreConfig, memory::InMemoryStore};
use http_helpers::json_request;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

fn build_state(store: Arc<InMemoryStore>) -> AppState {
    let state_store: Arc<dyn ControlPlaneAuthStore + Send + Sync> = store;
    AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        store: state_store,
        oidc_validator: UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    }
}

async fn setup() -> (
    axum::routing::RouterIntoService<Body, ()>,
    Arc<InMemoryStore>,
    TenantSigningKeys,
) {
    let store = Arc::new(InMemoryStore::new(StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    }));
    let app = build_router(build_state(store.clone())).into_service();

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

    (app, store, keys)
}

fn token(keys: &TenantSigningKeys, perms: Vec<&str>) -> String {
    mint_token(
        keys,
        "t1",
        "p:admin",
        perms.into_iter().map(|value| value.to_string()).collect(),
        Duration::from_secs(900),
    )
    .expect("token")
}

#[tokio::test]
async fn ns_manage_cannot_modify_rbac() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["ns.manage:namespace:t1/payments"]);

    let add_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/orders",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(add_policy, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rbac_view_can_list_but_cannot_mutate() {
    let (app, store, keys) = setup().await;
    store
        .add_rbac_policy(
            "t1",
            PolicyRule {
                subject: "role:reader".to_string(),
                object: "stream:t1/payments/*".to_string(),
                action: "stream.subscribe".to_string(),
            },
        )
        .await
        .expect("policy");
    store
        .add_rbac_grouping(
            "t1",
            GroupingRule {
                user: "group:g1".to_string(),
                role: "role:reader".to_string(),
            },
        )
        .await
        .expect("grouping");

    let t = token(&keys, vec!["rbac.view:tenant:t1"]);

    let policies = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/rbac/policies")
        .header("authorization", format!("Bearer {t}"))
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(policies).await.expect("policies");
    assert_eq!(response.status(), StatusCode::OK);

    let groupings = Request::builder()
        .method("GET")
        .uri("/v1/tenants/t1/rbac/groupings")
        .header("authorization", format!("Bearer {t}"))
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(groupings).await.expect("groupings");
    assert_eq!(response.status(), StatusCode::OK);

    let add_policy = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:writer",
            "object": "stream:t1/payments/orders",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(add_policy, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn namespace_rbac_admin_scope_is_enforced() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:namespace:t1/payments"]);

    let in_scope = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/*",
            "action": "stream.publish"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(in_scope, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let other_ns = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:orders-publisher",
            "object": "stream:t1/orders/*",
            "action": "stream.publish"
        }),
    );
    let response = app.oneshot(add_auth(other_ns, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn stream_rbac_admin_cannot_grant_namespace_wide_rules() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:stream:t1/payments/orders"]);

    let grant_namespace = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:payments-publisher",
            "object": "stream:t1/payments/*",
            "action": "stream.publish"
        }),
    );
    let response = app
        .oneshot(add_auth(grant_namespace, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn tenant_rbac_admin_can_grant_tenant_rules_but_tenant_star_is_rejected() {
    let (app, _store, keys) = setup().await;
    let t = token(&keys, vec!["rbac.policy.manage:tenant:t1"]);

    let valid = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:cache-reader",
            "object": "cache:t1/payments/*",
            "action": "cache.read"
        }),
    );
    let response = app
        .clone()
        .oneshot(add_auth(valid, &t))
        .await
        .expect("policy");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let invalid = json_request(
        "POST",
        "/v1/tenants/t1/rbac/policies",
        serde_json::json!({
            "subject": "role:admin",
            "object": "tenant:*",
            "action": "tenant.manage"
        }),
    );
    let response = app.oneshot(add_auth(invalid, &t)).await.expect("policy");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = read_json(response).await;
    assert_eq!(payload["code"], "validation_error");
}

fn add_auth(mut request: Request<Body>, token: &str) -> Request<Body> {
    request.headers_mut().insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().expect("auth header"),
    );
    request
}
