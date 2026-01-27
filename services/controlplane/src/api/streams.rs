//! Stream API handlers.
//!
//! # Purpose
//! Implements stream CRUD, patching, snapshot, and changefeed endpoints with
//! tenant/namespace validation.
use crate::api::ensure_tenant_namespace;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{
    StreamChangesResponse, StreamCreateRequest, StreamListResponse, StreamSnapshotResponse,
};
use crate::app::AppState;
use crate::model::{Stream, StreamKey, StreamPatchRequest};
use crate::store::StoreError;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::HashMap;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    responses(
        (status = 200, description = "List streams", body = StreamListResponse),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_streams(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<StreamListResponse>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let items = state
        .store
        .list_streams(&tenant_id, &namespace)
        .await
        .map_err(|err| api_internal("failed to list streams", &err))?;
    Ok(Json(StreamListResponse { items }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    request_body = StreamCreateRequest,
    responses(
        (status = 201, description = "Stream created", body = Stream),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Stream already exists", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_stream(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<StreamCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let stream = Stream {
        tenant_id,
        namespace,
        stream: body.stream,
        kind: body.kind,
        shards: body.shards,
        retention: body.retention,
        consistency: body.consistency,
        delivery: body.delivery,
        durable: body.durable,
    };
    match state.store.create_stream(stream.clone()).await {
        Ok(created) => Ok((StatusCode::CREATED, Json(created))),
        Err(StoreError::Conflict(_)) => Err(api_conflict("conflict", "stream already exists")),
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to create stream", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    responses(
        (status = 200, description = "Fetch stream", body = Stream),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn get_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Stream>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream,
    };
    match state.store.get_stream(&key).await {
        Ok(stream) => Ok(Json(stream)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to fetch stream", &err)),
    }
}

#[utoipa::path(
    patch,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    request_body = StreamPatchRequest,
    responses(
        (status = 200, description = "Stream updated", body = Stream),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn patch_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(body): Json<StreamPatchRequest>,
) -> Result<Json<Stream>, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream,
    };
    match state.store.patch_stream(&key, body).await {
        Ok(updated) => Ok(Json(updated)),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to update stream", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
    tag = "streams",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier"),
        ("stream" = String, Path, description = "Stream identifier")
    ),
    responses(
        (status = 204, description = "Stream deleted"),
        (status = 404, description = "Stream or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = StreamKey {
        tenant_id,
        namespace,
        stream,
    };
    match state.store.delete_stream(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("stream not found")),
        Err(err) => Err(api_internal("failed to delete stream", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/streams/snapshot",
    tag = "streams",
    responses(
        (status = 200, description = "Full stream snapshot", body = StreamSnapshotResponse)
    )
)]
pub(crate) async fn stream_snapshot(
    State(state): State<AppState>,
) -> Result<Json<StreamSnapshotResponse>, ApiError> {
    let snapshot = state
        .store
        .stream_snapshot()
        .await
        .map_err(|err| api_internal("failed to load stream snapshot", &err))?;
    Ok(Json(StreamSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/streams/changes",
    tag = "streams",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Stream change list", body = StreamChangesResponse)
    )
)]
pub(crate) async fn stream_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<StreamChangesResponse>, ApiError> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .stream_changes(since)
        .await
        .map_err(|err| api_internal("failed to load stream changes", &err))?;
    Ok(Json(StreamChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}
