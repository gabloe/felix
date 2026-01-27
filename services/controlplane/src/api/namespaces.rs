//! Namespace API handlers.
//!
//! # Purpose
//! Implements CRUD, snapshot, and changefeed endpoints for namespaces, including
//! tenant existence checks and error mapping.
use crate::api::ensure_tenant_exists;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{
    NamespaceChangesResponse, NamespaceCreateRequest, NamespaceListResponse,
    NamespaceSnapshotResponse,
};
use crate::app::AppState;
use crate::model::{Namespace, NamespaceKey};
use crate::store::StoreError;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::collections::HashMap;

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 200, description = "List namespaces", body = NamespaceListResponse),
        (status = 404, description = "Tenant not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn list_namespaces(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<NamespaceListResponse>, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    let items = state
        .store
        .list_namespaces(&tenant_id)
        .await
        .map_err(|err| api_internal("failed to list namespaces", &err))?;
    Ok(Json(NamespaceListResponse { items }))
}

#[utoipa::path(
    post,
    path = "/v1/tenants/{tenant_id}/namespaces",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    request_body = NamespaceCreateRequest,
    responses(
        (status = 201, description = "Namespace created", body = Namespace),
        (status = 404, description = "Tenant not found", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Namespace already exists", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn create_namespace(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    Json(body): Json<NamespaceCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    let namespace = Namespace {
        tenant_id,
        namespace: body.namespace,
        display_name: body.display_name,
    };
    match state.store.create_namespace(namespace.clone()).await {
        Ok(ns) => Ok((StatusCode::CREATED, Json(ns))),
        Err(StoreError::Conflict(_)) => {
            Err(api_conflict("already_exists", "namespace already exists"))
        }
        Err(StoreError::NotFound(_)) => Err(api_not_found("tenant not found")),
        Err(err) => Err(api_internal("failed to create namespace", &err)),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    responses(
        (status = 204, description = "Namespace deleted"),
        (status = 404, description = "Tenant or namespace not found", body = crate::api::types::ErrorResponse)
    )
)]
pub(crate) async fn delete_namespace(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    ensure_tenant_exists(&state, &tenant_id).await?;
    let key = NamespaceKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
    };
    match state.store.delete_namespace(&key).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("namespace not found")),
        Err(err) => Err(api_internal("failed to delete namespace", &err)),
    }
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/snapshot",
    tag = "namespaces",
    responses(
        (status = 200, description = "Full namespace snapshot", body = NamespaceSnapshotResponse)
    )
)]
pub(crate) async fn namespace_snapshot(
    State(state): State<AppState>,
) -> Result<Json<NamespaceSnapshotResponse>, ApiError> {
    let snapshot = state
        .store
        .namespace_snapshot()
        .await
        .map_err(|err| api_internal("failed to load namespace snapshot", &err))?;
    Ok(Json(NamespaceSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/changes",
    tag = "namespaces",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Namespace change list", body = NamespaceChangesResponse)
    )
)]
pub(crate) async fn namespace_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<NamespaceChangesResponse>, ApiError> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .namespace_changes(since)
        .await
        .map_err(|err| api_internal("failed to load namespace changes", &err))?;
    Ok(Json(NamespaceChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}
