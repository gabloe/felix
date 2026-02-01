//! Cache API handlers.
//!
//! # Purpose
//! Implements cache CRUD, patching, snapshot, and changefeed endpoints with
//! tenant/namespace validation.
use crate::api::ensure_tenant_namespace;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{
    CacheChangesResponse, CacheCreateRequest, CacheListResponse, CacheSnapshotResponse,
};
use crate::app::AppState;
use crate::model::{Cache, CacheKey, CachePatchRequest};
use crate::store::StoreError;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::HashMap;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    responses(
        (status = 200, description = "List caches", body = CacheListResponse),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_caches(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<CacheListResponse>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let items = state
        .store
        .list_caches(&tenant_id, &namespace)
        .await
        .map_err(|err| api_internal("failed to list caches", &err))?;
    Ok(Json(CacheListResponse { items }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    request_body = CacheCreateRequest,
    responses(
        (status = 201, description = "Cache created", body = Cache),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Cache already exists", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_cache(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<CacheCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let cache = Cache {
        tenant_id,
        namespace,
        cache: body.cache,
        display_name: body.display_name,
    };
    match state.store.create_cache(cache.clone()).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StoreError::Conflict(_)) => Err(api_conflict("conflict", "cache already exists")),
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to create cache", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    responses(
        (status = 200, description = "Fetch cache", body = Cache),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn get_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Cache>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache,
    };
    match state.store.get_cache(&key).await {
        Ok(cache) => Ok(Json(cache)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to fetch cache", &err)),
    }
}

#[utoipa::path(
    patch,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    request_body = CachePatchRequest,
    responses(
        (status = 200, description = "Cache updated", body = Cache),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn patch_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(body): Json<CachePatchRequest>,
) -> Result<Json<Cache>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache,
    };
    match state.store.patch_cache(&key, body).await {
        Ok(updated) => Ok(Json(updated)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to update cache", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("cache" = String, Path, description = "Cache identifier")
    ),
    responses(
        (status = 204, description = "Cache deleted"),
        (status = 404, description = "Cache or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_cache(
    Path((tenant_id, namespace, cache)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id,
        namespace,
        cache,
    };
    match state.store.delete_cache(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("cache not found")),
        Err(err) => Err(api_internal("failed to delete cache", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/caches/snapshot",
    tag = "caches",
    responses(
        (status = 200, description = "Full cache snapshot", body = CacheSnapshotResponse)
    )
)]
pub(crate) async fn cache_snapshot(
    State(state): State<AppState>,
) -> Result<Json<CacheSnapshotResponse>, ApiError> {
    let snapshot = state
        .store
        .cache_snapshot()
        .await
        .map_err(|err| api_internal("failed to load cache snapshot", &err))?;
    Ok(Json(CacheSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/caches/changes",
    tag = "caches",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Cache change list", body = CacheChangesResponse)
    )
)]
pub(crate) async fn cache_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<CacheChangesResponse>, ApiError> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .cache_changes(since)
        .await
        .map_err(|err| api_internal("failed to load cache changes", &err))?;
    Ok(Json(CacheChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}
