mod common;
mod http_helpers;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::read_json;
use controlplane::api::types::{FeatureFlags, Region};
use controlplane::app::{AppState, build_router};
use controlplane::auth::felix_token::TenantSigningKeys;
use controlplane::auth::idp_registry::IdpIssuerConfig;
use controlplane::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use controlplane::model::{
    Cache, CacheChange, CacheKey, CachePatchRequest, Namespace, NamespaceChange, NamespaceKey,
    Stream, StreamChange, StreamKey, StreamPatchRequest, Tenant, TenantChange,
};
use controlplane::store::{
    AuthStore, ChangeSet, ControlPlaneStore, Snapshot, StoreError, StoreResult,
};
use http_helpers::json_request;
use std::sync::Arc;
use tower::ServiceExt;

fn app_with_region_id(region_id: &str) -> axum::routing::RouterIntoService<axum::body::Body, ()> {
    let store = controlplane::store::memory::InMemoryStore::new(controlplane::store::StoreConfig {
        changes_limit: controlplane::config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(controlplane::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    let state = AppState {
        region: Region {
            region_id: region_id.to_string(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
        oidc_validator: controlplane::auth::oidc::OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    build_router(state).into_service()
}

#[tokio::test]
async fn streams_crud_and_changes_smoke() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create).await.expect("create");
    assert_eq!(response.status(), StatusCode::CREATED);

    let list = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams")
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list).await.expect("list");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get).await.expect("get");
    assert_eq!(response.status(), StatusCode::OK);

    let patch = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/orders",
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": null }
        }),
    );
    let response = app.clone().oneshot(patch).await.expect("patch");
    assert_eq!(response.status(), StatusCode::OK);

    let changes = Request::builder()
        .uri("/v1/streams/changes?since=0")
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(!payload["items"].as_array().unwrap().is_empty());

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/orders")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn caches_crud_and_changes_smoke() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

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

    let list = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches")
        .body(Body::empty())
        .expect("list");
    let response = app.clone().oneshot(list).await.expect("list");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .body(Body::empty())
        .expect("get");
    let response = app.clone().oneshot(get).await.expect("get");
    assert_eq!(response.status(), StatusCode::OK);

    let patch = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/primary",
        serde_json::json!({
            "display_name": "Primary Cache Updated"
        }),
    );
    let response = app.clone().oneshot(patch).await.expect("patch");
    assert_eq!(response.status(), StatusCode::OK);

    let changes = Request::builder()
        .uri("/v1/caches/changes?since=0")
        .body(Body::empty())
        .expect("changes");
    let response = app.clone().oneshot(changes).await.expect("changes");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert!(!payload["items"].as_array().unwrap().is_empty());

    let delete = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/primary")
        .body(Body::empty())
        .expect("delete");
    let response = app.clone().oneshot(delete).await.expect("delete");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn system_and_region_endpoints() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

    let info = Request::builder()
        .uri("/v1/system/info")
        .body(Body::empty())
        .expect("info");
    let response = app.clone().oneshot(info).await.expect("info");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["region_id"], "local");
    assert_eq!(payload["api_version"], "v1");

    let health = Request::builder()
        .uri("/v1/system/health")
        .body(Body::empty())
        .expect("health");
    let response = app.clone().oneshot(health).await.expect("health");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["status"], "ok");

    let regions = Request::builder()
        .uri("/v1/regions")
        .body(Body::empty())
        .expect("regions");
    let response = app.clone().oneshot(regions).await.expect("regions");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = read_json(response).await;
    assert_eq!(payload["items"].as_array().unwrap().len(), 1);

    let get_region = Request::builder()
        .uri("/v1/regions/local")
        .body(Body::empty())
        .expect("get region");
    let response = app.clone().oneshot(get_region).await.expect("get region");
    assert_eq!(response.status(), StatusCode::OK);

    let missing_region = Request::builder()
        .uri("/v1/regions/missing")
        .body(Body::empty())
        .expect("missing region");
    let response = app
        .clone()
        .oneshot(missing_region)
        .await
        .expect("missing region");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn tenant_and_namespace_errors() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

    let list = Request::builder()
        .uri("/v1/tenants")
        .body(Body::empty())
        .expect("list tenants");
    let response = app.clone().oneshot(list).await.expect("list tenants");
    assert_eq!(response.status(), StatusCode::OK);

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

    let conflict = json_request(
        "POST",
        "/v1/tenants",
        serde_json::json!({
            "tenant_id": "t1",
            "display_name": "Tenant One Again"
        }),
    );
    let response = app.clone().oneshot(conflict).await.expect("conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let list_missing_ns = Request::builder()
        .uri("/v1/tenants/missing/namespaces")
        .body(Body::empty())
        .expect("list missing ns");
    let response = app
        .clone()
        .oneshot(list_missing_ns)
        .await
        .expect("list missing ns");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let create_missing_ns = json_request(
        "POST",
        "/v1/tenants/missing/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_missing_ns)
        .await
        .expect("missing ns");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let delete_namespace = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default")
        .body(Body::empty())
        .expect("delete namespace");
    let response = app
        .clone()
        .oneshot(delete_namespace)
        .await
        .expect("delete namespace");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_namespace_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default")
        .body(Body::empty())
        .expect("delete namespace missing");
    let response = app
        .clone()
        .oneshot(delete_namespace_missing)
        .await
        .expect("delete namespace missing");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_tenant = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .body(Body::empty())
        .expect("delete tenant");
    let response = app
        .clone()
        .oneshot(delete_tenant)
        .await
        .expect("delete tenant");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let delete_tenant_missing = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1")
        .body(Body::empty())
        .expect("delete tenant missing");
    let response = app
        .clone()
        .oneshot(delete_tenant_missing)
        .await
        .expect("delete tenant missing");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn stream_and_cache_not_found_paths() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let get_stream = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .body(Body::empty())
        .expect("get stream");
    let response = app.clone().oneshot(get_stream).await.expect("get stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_stream = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/missing",
        serde_json::json!({
            "durable": true
        }),
    );
    let response = app
        .clone()
        .oneshot(patch_stream)
        .await
        .expect("patch stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let get_cache = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .body(Body::empty())
        .expect("get cache");
    let response = app.clone().oneshot(get_cache).await.expect("get cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let patch_cache = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/missing",
        serde_json::json!({
            "display_name": "Updated"
        }),
    );
    let response = app.clone().oneshot(patch_cache).await.expect("patch cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list_missing_namespace_streams = Request::builder()
        .uri("/v1/tenants/t1/namespaces/missing/streams")
        .body(Body::empty())
        .expect("list missing streams");
    let response = app
        .clone()
        .oneshot(list_missing_namespace_streams)
        .await
        .expect("list missing streams");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let list_missing_namespace_caches = Request::builder()
        .uri("/v1/tenants/t1/namespaces/missing/caches")
        .body(Body::empty())
        .expect("list missing caches");
    let response = app
        .clone()
        .oneshot(list_missing_namespace_caches)
        .await
        .expect("list missing caches");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn snapshots_and_changes_endpoints() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_cache = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    for path in [
        "/v1/tenants/snapshot",
        "/v1/tenants/changes?since=0",
        "/v1/namespaces/snapshot",
        "/v1/namespaces/changes?since=0",
        "/v1/streams/snapshot",
        "/v1/streams/changes?since=0",
        "/v1/caches/snapshot",
        "/v1/caches/changes?since=0",
    ] {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let response = app.clone().oneshot(req).await.expect("snapshot/changes");
        assert_eq!(response.status(), StatusCode::OK);
        let payload = read_json(response).await;
        assert!(!payload["items"].as_array().unwrap().is_empty());
    }
}

#[tokio::test]
async fn stream_and_cache_conflict_and_delete_errors() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict_stream = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app
        .clone()
        .oneshot(conflict_stream)
        .await
        .expect("stream conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let patch_stream = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/streams/orders",
        serde_json::json!({
            "retention": { "max_age_seconds": 7200, "max_size_bytes": 1024 },
            "consistency": "Quorum",
            "delivery": "AtMostOnce",
            "durable": true
        }),
    );
    let response = app
        .clone()
        .oneshot(patch_stream)
        .await
        .expect("patch stream");
    assert_eq!(response.status(), StatusCode::OK);

    let create_cache = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    let conflict_cache = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app
        .clone()
        .oneshot(conflict_cache)
        .await
        .expect("cache conflict");
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let patch_cache = json_request(
        "PATCH",
        "/v1/tenants/t1/namespaces/default/caches/primary",
        serde_json::json!({ "display_name": "Primary Updated" }),
    );
    let response = app.clone().oneshot(patch_cache).await.expect("patch cache");
    assert_eq!(response.status(), StatusCode::OK);

    let delete_stream = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/streams/missing")
        .body(Body::empty())
        .expect("delete stream");
    let response = app
        .clone()
        .oneshot(delete_stream)
        .await
        .expect("delete stream");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let delete_cache = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/t1/namespaces/default/caches/missing")
        .body(Body::empty())
        .expect("delete cache");
    let response = app
        .clone()
        .oneshot(delete_cache)
        .await
        .expect("delete cache");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_endpoints_return_items() {
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> = app_with_region_id("local");

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

    let create_namespace = json_request(
        "POST",
        "/v1/tenants/t1/namespaces",
        serde_json::json!({
            "namespace": "default",
            "display_name": "Default"
        }),
    );
    let response = app
        .clone()
        .oneshot(create_namespace)
        .await
        .expect("namespace");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_stream = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/streams",
        serde_json::json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "retention": { "max_age_seconds": 3600, "max_size_bytes": null },
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": false
        }),
    );
    let response = app.clone().oneshot(create_stream).await.expect("stream");
    assert_eq!(response.status(), StatusCode::CREATED);

    let create_cache = json_request(
        "POST",
        "/v1/tenants/t1/namespaces/default/caches",
        serde_json::json!({
            "cache": "primary",
            "display_name": "Primary"
        }),
    );
    let response = app.clone().oneshot(create_cache).await.expect("cache");
    assert_eq!(response.status(), StatusCode::CREATED);

    let list_tenants = Request::builder()
        .uri("/v1/tenants")
        .body(Body::empty())
        .expect("list tenants");
    let response = app
        .clone()
        .oneshot(list_tenants)
        .await
        .expect("list tenants");
    assert_eq!(response.status(), StatusCode::OK);

    let list_namespaces = Request::builder()
        .uri("/v1/tenants/t1/namespaces")
        .body(Body::empty())
        .expect("list namespaces");
    let response = app
        .clone()
        .oneshot(list_namespaces)
        .await
        .expect("list namespaces");
    assert_eq!(response.status(), StatusCode::OK);

    let list_streams = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/streams")
        .body(Body::empty())
        .expect("list streams");
    let response = app
        .clone()
        .oneshot(list_streams)
        .await
        .expect("list streams");
    assert_eq!(response.status(), StatusCode::OK);

    let list_caches = Request::builder()
        .uri("/v1/tenants/t1/namespaces/default/caches")
        .body(Body::empty())
        .expect("list caches");
    let response = app.clone().oneshot(list_caches).await.expect("list caches");
    assert_eq!(response.status(), StatusCode::OK);
}

struct FailingStore;

#[async_trait]
impl ControlPlaneStore for FailingStore {
    async fn list_tenants(&self) -> StoreResult<Vec<Tenant>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_tenant(&self, _tenant: Tenant) -> StoreResult<Tenant> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_tenant(&self, _tenant_id: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_snapshot(&self) -> StoreResult<Snapshot<Tenant>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_changes(&self, _since: u64) -> StoreResult<ChangeSet<TenantChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_namespaces(&self, _tenant_id: &str) -> StoreResult<Vec<Namespace>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_namespace(&self, _namespace: Namespace) -> StoreResult<Namespace> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_namespace(&self, _key: &NamespaceKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn namespace_snapshot(&self) -> StoreResult<Snapshot<Namespace>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn namespace_changes(&self, _since: u64) -> StoreResult<ChangeSet<NamespaceChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_streams(&self, _tenant_id: &str, _namespace: &str) -> StoreResult<Vec<Stream>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_stream(&self, _key: &StreamKey) -> StoreResult<Stream> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_stream(&self, _stream: Stream) -> StoreResult<Stream> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn patch_stream(
        &self,
        _key: &StreamKey,
        _patch: StreamPatchRequest,
    ) -> StoreResult<Stream> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_stream(&self, _key: &StreamKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn stream_snapshot(&self) -> StoreResult<Snapshot<Stream>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn stream_changes(&self, _since: u64) -> StoreResult<ChangeSet<StreamChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_caches(&self, _tenant_id: &str, _namespace: &str) -> StoreResult<Vec<Cache>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_cache(&self, _key: &CacheKey) -> StoreResult<Cache> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn create_cache(&self, _cache: Cache) -> StoreResult<Cache> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn patch_cache(&self, _key: &CacheKey, _patch: CachePatchRequest) -> StoreResult<Cache> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_cache(&self, _key: &CacheKey) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn cache_snapshot(&self) -> StoreResult<Snapshot<Cache>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn cache_changes(&self, _since: u64) -> StoreResult<ChangeSet<CacheChange>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_exists(&self, _tenant_id: &str) -> StoreResult<bool> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn namespace_exists(&self, _key: &NamespaceKey) -> StoreResult<bool> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn health_check(&self) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    fn is_durable(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "fail"
    }
}

#[async_trait]
impl AuthStore for FailingStore {
    async fn list_idp_issuers(&self, _tenant_id: &str) -> StoreResult<Vec<IdpIssuerConfig>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn upsert_idp_issuer(
        &self,
        _tenant_id: &str,
        _issuer: IdpIssuerConfig,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn delete_idp_issuer(&self, _tenant_id: &str, _issuer: &str) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_policies(&self, _tenant_id: &str) -> StoreResult<Vec<PolicyRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn list_rbac_groupings(&self, _tenant_id: &str) -> StoreResult<Vec<GroupingRule>> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn add_rbac_policy(&self, _tenant_id: &str, _policy: PolicyRule) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn add_rbac_grouping(
        &self,
        _tenant_id: &str,
        _grouping: GroupingRule,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn get_tenant_signing_keys(&self, _tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_tenant_signing_keys(
        &self,
        _tenant_id: &str,
        _keys: TenantSigningKeys,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn tenant_auth_is_bootstrapped(&self, _tenant_id: &str) -> StoreResult<bool> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn set_tenant_auth_bootstrapped(
        &self,
        _tenant_id: &str,
        _bootstrapped: bool,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn ensure_signing_key_current(&self, _tenant_id: &str) -> StoreResult<TenantSigningKeys> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }

    async fn seed_rbac_policies_and_groupings(
        &self,
        _tenant_id: &str,
        _policies: Vec<PolicyRule>,
        _groupings: Vec<GroupingRule>,
    ) -> StoreResult<()> {
        Err(StoreError::Unexpected(anyhow::anyhow!("fail")))
    }
}

#[tokio::test]
async fn system_health_reports_internal_error_on_store_failure() {
    let state = AppState {
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
        store: Arc::new(FailingStore),
        oidc_validator: controlplane::auth::oidc::OidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_token: None,
    };
    let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
        build_router(state).into_service();

    let health = Request::builder()
        .uri("/v1/system/health")
        .body(Body::empty())
        .expect("health");
    let response = app.clone().oneshot(health).await.expect("health");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
