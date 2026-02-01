mod common;
mod http_helpers;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::api::types::FeatureFlags;
use controlplane::app::{AppState, build_router};
use controlplane::model::{RetentionPolicy, StreamKind};
use controlplane::store::{ControlPlaneStore, StoreConfig};
use http_helpers::json_request;
use std::sync::Arc;
use tower::ServiceExt;

fn app_with_state() -> axum::routing::RouterIntoService<Body, ()> {
    let store = controlplane::store::memory::InMemoryStore::new(StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    let state = AppState {
        region: controlplane::api::types::Region {
            region_id: "local".to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
        oidc_validator: controlplane::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    build_router(state).into_service()
}

async fn create_tenant(app: &axum::routing::RouterIntoService<Body, ()>) {
    let req = json_request(
        "POST",
        "/v1/tenants",
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(req).await.expect("tenant");
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn create_namespace(app: &axum::routing::RouterIntoService<Body, ()>) {
    let req = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app.clone().oneshot(req).await.expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn tenants_conflict_and_delete_not_found() {
    let app = app_with_state();

    // First create should succeed.
    create_tenant(&app).await;

    // Second create should conflict.
    let req = json_request(
        "POST",
        "/v1/tenants",
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One"
        }),
    );
    let response = app.clone().oneshot(req).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Delete existing tenant succeeds, second delete returns not found.
    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_again = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_again).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn namespaces_missing_tenant_and_conflict() {
    let app = app_with_state();

    let list_missing = Request::builder()
        .uri("/v1/tenants/missing/namespaces")
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list_missing).await.expect("list");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    create_tenant(&app).await;

    create_namespace(&app).await;

    let create_conflict = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_conflict)
        .await
        .expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/other")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn caches_error_paths_and_changes() {
    let app = app_with_state();
    create_tenant(&app).await;
    create_namespace(&app).await;

    let create = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary Cache"
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary Cache"
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let get_missing = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get_missing).await.expect("get");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_missing = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/missing",
        serde_json::json!({
            "display_name": "Missing"
        }),
    );
    let response = app.clone().oneshot(patch_missing).await.expect("patch");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let changes = Request::builder()
        .uri("/v1/caches/changes?since=0")
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(payload["items"].as_array().is_some());
}

#[tokio::test]
async fn streams_error_paths_and_changes() {
    let app = app_with_state();
    create_tenant(&app).await;
    create_namespace(&app).await;

    let create = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": StreamKind::Stream,
            "shards": 1,
            "retention": RetentionPolicy { max_age_seconds: Some(3600), max_size_bytes: None },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": StreamKind::Stream,
            "shards": 1,
            "retention": RetentionPolicy { max_age_seconds: Some(3600), max_size_bytes: None },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let get_missing = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get_missing).await.expect("get");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_missing = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/missing",
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": null }
        }),
    );
    let response = app.clone().oneshot(patch_missing).await.expect("patch");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete_missing).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let changes = Request::builder()
        .uri("/v1/streams/changes?since=0")
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(payload["items"].as_array().is_some());
}
