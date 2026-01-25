// Felix Control Plane (HTTP)
// --------------------------
// This binary is the control-plane metadata service for Felix. It exposes a JSON/HTTP API
// (Axum) for managing and discovering *configuration metadata* that brokers and clients
// need in order to operate: tenants, namespaces, streams, and caches.
//
// Storage model (2026):
// - Pluggable `ControlPlaneStore`: in-memory (default) or Postgres when
//   FELIX_CONTROLPLANE_POSTGRES_URL / DATABASE_URL / storage.backend=postgres is set.
// - Postgres uses canonical tables + append-only change tables with BIGSERIAL seq to keep
//   `next_seq` durable and monotonic across restarts; migrations run at startup.
// - Memory store preserves legacy dev behavior (HashMaps + VecDeque change logs) and resets
//   on process restart.
//
// How brokers consume this API:
// - Brokers treat the control plane as the *authoritative source* for metadata.
// - On cold start (or when they fall behind), brokers call the `*/snapshot` endpoints to
//   fetch the full current set of metadata for each resource type.
// - During steady state, brokers poll the `*/changes?since=<seq>` endpoints to fetch an
//   incremental change feed (rolling window) and keep local caches up to date.
//
// Change feed semantics:
// - Per-resource monotonic `seq` cursor; durable in Postgres (BIGSERIAL), process-local in
//   memory mode.
// - Change feeds are rolling windows: DB retention (row limit) in Postgres; capped VecDeque
//   in memory. Brokers that fall behind must resnapshot to catch up.
mod config;
mod observability;
mod store;

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use store::{
    ControlPlaneStore, StoreConfig, StoreError, memory::InMemoryStore, postgres::PostgresStore,
};
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utoipa::{OpenApi, ToSchema};

#[derive(Clone)]
struct AppState {
    region: Region,
    api_version: String,
    features: FeatureFlags,
    store: Arc<dyn ControlPlaneStore + Send + Sync>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct FeatureFlags {
    durable_storage: bool,
    tiered_storage: bool,
    bridges: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct SystemInfo {
    region_id: String,
    api_version: String,
    features: FeatureFlags,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct HealthStatus {
    status: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct Region {
    region_id: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct Tenant {
    tenant_id: String,
    display_name: String,
}

// Stable identifier for a namespace, used as a HashMap key and as the identity
// carried in change feed events. This is intentionally separate from `Namespace`
// so we can use it in maps and logs even when the full object is absent (e.g. deletions).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct Namespace {
    tenant_id: String,
    namespace: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct TenantCreateRequest {
    tenant_id: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct NamespaceCreateRequest {
    namespace: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct TenantListResponse {
    items: Vec<Tenant>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct TenantSnapshotResponse {
    items: Vec<Tenant>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct TenantChange {
    seq: u64,
    op: TenantChangeOp,
    tenant_id: String,
    tenant: Option<Tenant>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct TenantChangesResponse {
    items: Vec<TenantChange>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) enum TenantChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct NamespaceListResponse {
    items: Vec<Namespace>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct NamespaceSnapshotResponse {
    items: Vec<Namespace>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct NamespaceChange {
    seq: u64,
    op: NamespaceChangeOp,
    key: NamespaceKey,
    namespace: Option<Namespace>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct NamespaceChangesResponse {
    items: Vec<NamespaceChange>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) enum NamespaceChangeOp {
    Created,
    Deleted,
}

// Stable identifier for a stream, used as a HashMap key and in change feed events.
// Keys are fully-qualified by tenant + namespace to avoid ambiguity.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

// Stream metadata describes how brokers should treat a named stream/queue.
// These fields are *control-plane intent*; the broker/data-plane may enforce
// constraints and implement behavior based on them.
//
// - kind: logical model (stream vs queue vs cache-like stream semantics)
// - shards: partitioning count (implementation-defined; may map to internal shards)
// - retention: maximum age/size controls for durable/tiered retention (future)
// - consistency: read/write consistency expectations (leader/quorum semantics; future)
// - delivery: delivery guarantee intent (at-most-once vs at-least-once; future)
// - durable: whether this stream is expected to be persisted durably (future)
//   (in the current MVP, this mostly advertises intent rather than enforcing storage).
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct Stream {
    tenant_id: String,
    namespace: String,
    stream: String,
    kind: StreamKind,
    shards: u32,
    retention: RetentionPolicy,
    consistency: ConsistencyLevel,
    delivery: DeliveryGuarantee,
    durable: bool,
}

// Stable identifier for a cache, used as a HashMap key and in change feed events.
// Keys are fully-qualified by tenant + namespace to avoid ambiguity.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct Cache {
    tenant_id: String,
    namespace: String,
    cache: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct CacheCreateRequest {
    cache: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct CachePatchRequest {
    display_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct CacheListResponse {
    items: Vec<Cache>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct CacheSnapshotResponse {
    items: Vec<Cache>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct CacheChange {
    seq: u64,
    op: CacheChangeOp,
    key: CacheKey,
    cache: Option<Cache>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct CacheChangesResponse {
    items: Vec<CacheChange>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct StreamCreateRequest {
    stream: String,
    kind: StreamKind,
    shards: u32,
    retention: RetentionPolicy,
    consistency: ConsistencyLevel,
    delivery: DeliveryGuarantee,
    durable: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct StreamListResponse {
    items: Vec<Stream>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct StreamSnapshotResponse {
    items: Vec<Stream>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct StreamChange {
    seq: u64,
    op: StreamChangeOp,
    key: StreamKey,
    stream: Option<Stream>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct StreamChangesResponse {
    items: Vec<StreamChange>,
    next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct StreamPatchRequest {
    retention: Option<RetentionPolicy>,
    consistency: Option<ConsistencyLevel>,
    delivery: Option<DeliveryGuarantee>,
    durable: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) struct RetentionPolicy {
    max_age_seconds: Option<u64>,
    max_size_bytes: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) enum StreamKind {
    Stream,
    Queue,
    Cache,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) enum ConsistencyLevel {
    Leader,
    Quorum,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub(crate) enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
struct ListRegionsResponse {
    items: Vec<Region>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
struct ErrorResponse {
    code: String,
    message: String,
    request_id: Option<String>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ErrorResponse,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(self.body)).into_response()
    }
}

fn api_not_found(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

fn api_conflict(code: &str, message: &str) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        body: ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

fn api_internal(message: &str, err: &StoreError) -> ApiError {
    tracing::error!(error = ?err, "controlplane storage error");
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: ErrorResponse {
            code: "internal".to_string(),
            message: message.to_string(),
            request_id: None,
        },
    }
}

// Cluster identity + capability discovery. Brokers/clients can use this to
// learn region identity, API versioning, and feature flags.
#[utoipa::path(
    get,
    path = "/v1/system/info",
    tag = "system",
    responses(
        (status = 200, description = "Cluster identity and capabilities", body = SystemInfo)
    )
)]
async fn system_info(State(state): State<AppState>) -> Json<SystemInfo> {
    Json(SystemInfo {
        region_id: state.region.region_id.clone(),
        api_version: state.api_version.clone(),
        features: state.features.clone(),
    })
}

// Basic liveness endpoint. Does not currently validate backend dependencies
// because the MVP backend is purely in-memory.
#[utoipa::path(
    get,
    path = "/v1/system/health",
    tag = "system",
    responses(
        (status = 200, description = "Control plane health", body = HealthStatus)
    )
)]
async fn system_health(State(state): State<AppState>) -> Result<Json<HealthStatus>, ApiError> {
    if let Err(err) = state.store.health_check().await {
        return Err(api_internal("storage unavailable", &err));
    }
    Ok(Json(HealthStatus {
        status: "ok".to_string(),
    }))
}

// Region listing for discovery. MVP is single-region and returns exactly one
// region entry; multi-region control plane would expand this.
#[utoipa::path(
    get,
    path = "/v1/regions",
    tag = "regions",
    responses(
        (status = 200, description = "List regions", body = ListRegionsResponse)
    )
)]
async fn list_regions(State(state): State<AppState>) -> Json<ListRegionsResponse> {
    Json(ListRegionsResponse {
        items: vec![state.region.clone()],
    })
}

// Fetch a single region by id. In MVP, only the locally configured region exists.
#[utoipa::path(
    get,
    path = "/v1/regions/{region_id}",
    tag = "regions",
    params(
        ("region_id" = String, Path, description = "Region identifier")
    ),
    responses(
        (status = 200, description = "Fetch region", body = Region),
        (status = 404, description = "Region not found", body = ErrorResponse)
    )
)]
async fn get_region(
    Path(region_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Region>, ApiError> {
    if state.region.region_id != region_id {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "region not found".to_string(),
                request_id: None,
            },
        });
    }
    Ok(Json(state.region))
}

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
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse)
    )
)]
async fn list_streams(
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
    get,
    path = "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
    tag = "caches",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier"),
        ("namespace" = String, Path, description = "Namespace identifier")
    ),
    responses(
        (status = 200, description = "List caches", body = CacheListResponse),
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse)
    )
)]
async fn list_caches(
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
    get,
    path = "/v1/tenants",
    tag = "tenants",
    responses(
        (status = 200, description = "List tenants", body = TenantListResponse)
    )
)]
async fn list_tenants(State(state): State<AppState>) -> Result<Json<TenantListResponse>, ApiError> {
    let items = state
        .store
        .list_tenants()
        .await
        .map_err(|err| api_internal("failed to list tenants", &err))?;
    Ok(Json(TenantListResponse { items }))
}

// Create a tenant. This establishes the top-level scope for all other resources.
// Emits a change log entry so brokers can refresh their local caches.
#[utoipa::path(
    post,
    path = "/v1/tenants",
    tag = "tenants",
    request_body = TenantCreateRequest,
    responses(
        (status = 201, description = "Tenant created", body = Tenant),
        (status = 409, description = "Tenant already exists", body = ErrorResponse)
    )
)]
async fn create_tenant(
    State(state): State<AppState>,
    Json(body): Json<TenantCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = Tenant {
        tenant_id: body.tenant_id,
        display_name: body.display_name,
    };
    match state.store.create_tenant(tenant.clone()).await {
        Ok(_) => Ok((StatusCode::CREATED, Json(tenant))),
        Err(StoreError::Conflict(_)) => {
            Err(api_conflict("already_exists", "tenant already exists"))
        }
        Err(err) => Err(api_internal("failed to create tenant", &err)),
    }
}

// Delete a tenant and cascade-delete all nested namespaces, streams, and caches.
// Emits change events for each removed object type so brokers converge correctly.
#[utoipa::path(
    delete,
    path = "/v1/tenants/{tenant_id}",
    tag = "tenants",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 204, description = "Tenant deleted"),
        (status = 404, description = "Tenant not found", body = ErrorResponse)
    )
)]
async fn delete_tenant(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
    match state.store.delete_tenant(&tenant_id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(StoreError::NotFound(_)) => Err(api_not_found("tenant not found")),
        Err(err) => Err(api_internal("failed to delete tenant", &err)),
    }
}

// Full snapshot endpoint for cold start / resync.
// The snapshot is authoritative current state, and `next_seq` indicates where a
// consumer should start polling incremental changes.
#[utoipa::path(
    get,
    path = "/v1/tenants/snapshot",
    tag = "tenants",
    responses(
        (status = 200, description = "Full tenant snapshot", body = TenantSnapshotResponse)
    )
)]
async fn tenant_snapshot(
    State(state): State<AppState>,
) -> Result<Json<TenantSnapshotResponse>, ApiError> {
    let snapshot = state
        .store
        .tenant_snapshot()
        .await
        .map_err(|err| api_internal("failed to load tenant snapshot", &err))?;
    Ok(Json(TenantSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

// Incremental change feed for steady-state polling.
// Clients pass `since=<last_seen_seq>` and receive all changes with seq >= since,
// along with `next_seq` which should be stored as the new cursor.
// If the consumer falls behind beyond the rolling window, it must resync via snapshot.
#[utoipa::path(
    get,
    path = "/v1/tenants/changes",
    tag = "tenants",
    params(
        ("since" = Option<u64>, Query, description = "Last seen sequence")
    ),
    responses(
        (status = 200, description = "Tenant change list", body = TenantChangesResponse)
    )
)]
async fn tenant_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<TenantChangesResponse>, ApiError> {
    // Incremental change feed used by brokers between snapshots.
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .tenant_changes(since)
        .await
        .map_err(|err| api_internal("failed to load tenant changes", &err))?;
    Ok(Json(TenantChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/tenants/{tenant_id}/namespaces",
    tag = "namespaces",
    params(
        ("tenant_id" = String, Path, description = "Tenant identifier")
    ),
    responses(
        (status = 200, description = "List namespaces", body = NamespaceListResponse),
        (status = 404, description = "Tenant not found", body = ErrorResponse)
    )
)]
async fn list_namespaces(
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

// Create a namespace under an existing tenant. Namespaces are tenant-scoped.
// Emits a change log entry for broker cache refresh.
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
        (status = 404, description = "Tenant not found", body = ErrorResponse),
        (status = 409, description = "Namespace already exists", body = ErrorResponse)
    )
)]
async fn create_namespace(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    Json(body): Json<NamespaceCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Require a valid tenant before allowing namespace creation.
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

// Delete a namespace and cascade-delete streams/caches within it.
// Emits change events so brokers can remove or invalidate local metadata.
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
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse)
    )
)]
async fn delete_namespace(
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

// Full snapshot endpoint for cold start / resync.
// The snapshot is authoritative current state, and `next_seq` indicates where a
// consumer should start polling incremental changes.
#[utoipa::path(
    get,
    path = "/v1/namespaces/snapshot",
    tag = "namespaces",
    responses(
        (status = 200, description = "Full namespace snapshot", body = NamespaceSnapshotResponse)
    )
)]
async fn namespace_snapshot(
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

// Incremental change feed for steady-state polling.
// Clients pass `since=<last_seen_seq>` and receive all changes with seq >= since,
// along with `next_seq` which should be stored as the new cursor.
// If the consumer falls behind beyond the rolling window, it must resync via snapshot.
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
async fn namespace_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<NamespaceChangesResponse>, ApiError> {
    // Incremental change feed used by brokers between snapshots.
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

// Full snapshot endpoint for cold start / resync.
// The snapshot is authoritative current state, and `next_seq` indicates where a
// consumer should start polling incremental changes.
#[utoipa::path(
    get,
    path = "/v1/streams/snapshot",
    tag = "streams",
    responses(
        (status = 200, description = "Full stream snapshot", body = StreamSnapshotResponse)
    )
)]
async fn stream_snapshot(
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

// Incremental change feed for steady-state polling.
// Clients pass `since=<last_seen_seq>` and receive all changes with seq >= since,
// along with `next_seq` which should be stored as the new cursor.
// If the consumer falls behind beyond the rolling window, it must resync via snapshot.
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
async fn stream_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<StreamChangesResponse>, ApiError> {
    // Incremental change feed used by brokers between snapshots.
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

// Full snapshot endpoint for cold start / resync.
// The snapshot is authoritative current state, and `next_seq` indicates where a
// consumer should start polling incremental changes.
#[utoipa::path(
    get,
    path = "/v1/caches/snapshot",
    tag = "caches",
    responses(
        (status = 200, description = "Full cache snapshot", body = CacheSnapshotResponse)
    )
)]
async fn cache_snapshot(
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

// Incremental change feed for steady-state polling.
// Clients pass `since=<last_seen_seq>` and receive all changes with seq >= since,
// along with `next_seq` which should be stored as the new cursor.
// If the consumer falls behind beyond the rolling window, it must resync via snapshot.
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
async fn cache_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Json<CacheChangesResponse>, ApiError> {
    // Incremental change feed used by brokers between snapshots.
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

// Create a stream in a tenant/namespace. Validates tenant+namespace existence.
// Emits a stream change event; metrics track stream counts and change totals.
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
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse),
        (status = 409, description = "Stream already exists", body = ErrorResponse)
    )
)]
async fn create_stream(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<StreamCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Streams are scoped to tenant + namespace, so validate both first.
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
        (status = 404, description = "Stream or namespace not found", body = ErrorResponse)
    )
)]
async fn get_stream(
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

// Patch selected stream policy fields (retention/consistency/delivery/durable).
// This is a read-modify-write update under a write lock and emits an Updated change.
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
        (status = 404, description = "Stream or namespace not found", body = ErrorResponse)
    )
)]
async fn patch_stream(
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

// Delete a stream and emit a Deleted change so brokers stop serving it.
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
        (status = 404, description = "Stream or namespace not found", body = ErrorResponse)
    )
)]
async fn delete_stream(
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

// Create a cache in a tenant/namespace. Emits a Created change and updates metrics.
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
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse),
        (status = 409, description = "Cache already exists", body = ErrorResponse)
    )
)]
async fn create_cache(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<CacheCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Cache is scoped to tenant + namespace.
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
        (status = 404, description = "Cache or namespace not found", body = ErrorResponse)
    )
)]
async fn get_cache(
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

// Patch mutable cache fields (currently display_name only). Emits an Updated change.
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
        (status = 404, description = "Cache or namespace not found", body = ErrorResponse)
    )
)]
async fn patch_cache(
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

// Delete a cache and emit a Deleted change so brokers remove/invalidate local metadata.
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
        (status = 404, description = "Cache or namespace not found", body = ErrorResponse)
    )
)]
async fn delete_cache(
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

#[derive(OpenApi)]
#[openapi(
    info(
        title = "felix-controlplane",
        version = "v1",
        description = "Felix control plane HTTP API"
    ),
    paths(
        system_info,
        system_health,
        list_regions,
        get_region,
        list_tenants,
        create_tenant,
        delete_tenant,
        tenant_snapshot,
        tenant_changes,
        list_namespaces,
        create_namespace,
        delete_namespace,
        namespace_snapshot,
        namespace_changes,
        cache_snapshot,
        cache_changes,
        stream_snapshot,
        stream_changes,
        list_streams,
        list_caches,
        create_stream,
        get_stream,
        patch_stream,
        delete_stream,
        create_cache,
        get_cache,
        patch_cache,
        delete_cache
    ),
    components(schemas(
        FeatureFlags,
        SystemInfo,
        HealthStatus,
        Region,
        ListRegionsResponse,
        ErrorResponse,
        Tenant,
        TenantCreateRequest,
        TenantListResponse,
        TenantSnapshotResponse,
        TenantChange,
        TenantChangesResponse,
        TenantChangeOp,
        Namespace,
        NamespaceCreateRequest,
        NamespaceListResponse,
        NamespaceSnapshotResponse,
        NamespaceChange,
        NamespaceChangesResponse,
        NamespaceChangeOp,
        Cache,
        CacheKey,
        CacheCreateRequest,
        CachePatchRequest,
        CacheListResponse,
        CacheSnapshotResponse,
        CacheChange,
        CacheChangesResponse,
        CacheChangeOp,
        Stream,
        StreamKind,
        StreamCreateRequest,
        StreamListResponse,
        StreamSnapshotResponse,
        StreamChange,
        StreamChangesResponse,
        StreamChangeOp,
        StreamKey,
        StreamPatchRequest,
        RetentionPolicy,
        ConsistencyLevel,
        DeliveryGuarantee
    )),
    tags(
        (name = "system", description = "System and discovery endpoints"),
        (name = "regions", description = "Region metadata"),
        (name = "tenants", description = "Tenant management"),
        (name = "namespaces", description = "Namespace management"),
        (name = "streams", description = "Stream management"),
        (name = "caches", description = "Cache management")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let metrics_handle = observability::init_observability("felix-controlplane");

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    let store_config = StoreConfig {
        changes_limit: config.changes_limit,
        change_retention_max_rows: config.change_retention_max_rows,
    };
    let store: Arc<dyn ControlPlaneStore + Send + Sync> = match config.storage {
        config::StorageBackend::Memory => Arc::new(InMemoryStore::new(store_config)),
        config::StorageBackend::Postgres => {
            let pg = config
                .postgres
                .as_ref()
                .context("postgres configuration missing")?;
            Arc::new(PostgresStore::connect(pg, store_config).await?)
        }
    };

    tracing::info!(
        backend = store.backend_name(),
        durable = store.is_durable(),
        "control plane store ready"
    );

    let state = AppState {
        region: Region {
            region_id: config.region_id.clone(),
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store,
    };

    tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

    let app = build_app(state);

    let addr = config.bind_addr;
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

// Constructs the default in-memory state for local development/tests.
#[cfg(test)]
fn default_state_with_region_id(region_id: String) -> AppState {
    let store = InMemoryStore::new(StoreConfig {
        changes_limit: config::DEFAULT_CHANGES_LIMIT,
        change_retention_max_rows: Some(config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
    });
    AppState {
        region: Region {
            region_id,
            display_name: "Local Region".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store: Arc::new(store),
    }
}

// Builds the Axum router, including:
// - route wiring for discovery, snapshots, change feeds, and CRUD
// - Swagger/OpenAPI docs
// - tracing middleware that extracts parent context from headers (if present)
// - shared application state injection
fn build_app(state: AppState) -> Router {
    // API surface: system/regions, change feeds, CRUD for tenants/namespaces/streams/caches.
    let trace_layer =
        TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
            let parent = observability::trace_context_from_headers(request.headers());
            let span = tracing::info_span!(
                "http.request",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version()
            );
            span.set_parent(parent);
            span
        });

    Router::new()
        .route("/v1/system/info", get(system_info))
        .route("/v1/system/health", get(system_health))
        .route("/v1/regions", get(list_regions))
        .route("/v1/regions/:region_id", get(get_region))
        .route("/v1/tenants/snapshot", get(tenant_snapshot))
        .route("/v1/tenants/changes", get(tenant_changes))
        .route("/v1/namespaces/snapshot", get(namespace_snapshot))
        .route("/v1/namespaces/changes", get(namespace_changes))
        .route("/v1/caches/snapshot", get(cache_snapshot))
        .route("/v1/caches/changes", get(cache_changes))
        .route("/v1/streams/snapshot", get(stream_snapshot))
        .route("/v1/streams/changes", get(stream_changes))
        .route("/v1/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/v1/tenants/:tenant_id",
            axum::routing::delete(delete_tenant),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces",
            get(list_namespaces).post(create_namespace),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace",
            axum::routing::delete(delete_namespace),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams",
            get(list_streams).post(create_stream),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams/:stream",
            get(get_stream).patch(patch_stream).delete(delete_stream),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/caches",
            get(list_caches).post(create_cache),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/caches/:cache",
            get(get_cache).patch(patch_cache).delete(delete_cache),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs").url("/v1/openapi.json", ApiDoc::openapi()),
        )
        .layer(trace_layer)
        .with_state(state)
}

// Shared guard that ensures both tenant and namespace exist.
// Centralizing this keeps 404 behavior consistent across endpoints.
async fn ensure_tenant_namespace(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
) -> Result<(), ApiError> {
    ensure_tenant_exists(state, tenant_id).await?;
    let namespace_key = NamespaceKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
    };
    let exists = state
        .store
        .namespace_exists(&namespace_key)
        .await
        .map_err(|err| api_internal("failed to check namespace existence", &err))?;
    if !exists {
        return Err(api_not_found("namespace not found"));
    }
    Ok(())
}

// Shared guard that ensures a tenant exists, returning a consistent 404 error shape.
async fn ensure_tenant_exists(state: &AppState, tenant_id: &str) -> Result<(), ApiError> {
    let exists = state
        .store
        .tenant_exists(tenant_id)
        .await
        .map_err(|err| api_internal("failed to check tenant existence", &err))?;
    if !exists {
        return Err(api_not_found("tenant not found"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ChangeSet, Snapshot, StoreResult};
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[cfg(feature = "pg-tests")]
    use sqlx::postgres::PgPoolOptions;

    fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    async fn read_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }

    #[tokio::test]
    async fn streams_crud_and_changes_smoke() {
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(default_state_with_region_id("local".to_string())).into_service();

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

        async fn list_streams(
            &self,
            _tenant_id: &str,
            _namespace: &str,
        ) -> StoreResult<Vec<Stream>> {
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

        async fn patch_cache(
            &self,
            _key: &CacheKey,
            _patch: CachePatchRequest,
        ) -> StoreResult<Cache> {
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
        };
        let app: axum::routing::RouterIntoService<axum::body::Body, ()> =
            build_app(state).into_service();

        let health = Request::builder()
            .uri("/v1/system/health")
            .body(Body::empty())
            .expect("health");
        let response = app.clone().oneshot(health).await.expect("health");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[cfg(feature = "pg-tests")]
    async fn reset_postgres(url: &str) {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .expect("pg pool");
        sqlx::query(
            "TRUNCATE tenant_changes, namespace_changes, stream_changes, cache_changes, streams, caches, namespaces, tenants RESTART IDENTITY",
        )
        .execute(&pool)
        .await
        .expect("truncate tables");
    }

    #[cfg(feature = "pg-tests")]
    async fn pg_store() -> Option<store::postgres::PostgresStore> {
        let url = match std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
        {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping pg-tests: set FELIX_CONTROLPLANE_POSTGRES_URL or DATABASE_URL");
                return None;
            }
        };
        reset_postgres(&url).await;
        let pg_cfg = config::PostgresConfig {
            url,
            max_connections: 5,
            connect_timeout_ms: 5_000,
            acquire_timeout_ms: 5_000,
        };
        Some(
            store::postgres::PostgresStore::connect(
                &pg_cfg,
                StoreConfig {
                    changes_limit: config::DEFAULT_CHANGES_LIMIT,
                    change_retention_max_rows: Some(config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
                },
            )
            .await
            .expect("connect postgres store"),
        )
    }

    #[cfg(feature = "pg-tests")]
    #[tokio::test]
    async fn pg_stream_sequences_monotonic() {
        let Some(store) = pg_store().await else {
            return;
        };

        store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            })
            .await
            .expect("tenant");
        store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                display_name: "Default".to_string(),
            })
            .await
            .expect("namespace");

        let mut stream = Stream {
            tenant_id: "t1".to_string(),
            namespace: "default".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            retention: RetentionPolicy {
                max_age_seconds: Some(3600),
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: false,
        };
        store
            .create_stream(stream.clone())
            .await
            .expect("create stream");

        stream.retention.max_age_seconds = Some(7200);
        store
            .patch_stream(
                &StreamKey {
                    tenant_id: "t1".to_string(),
                    namespace: "default".to_string(),
                    stream: "orders".to_string(),
                },
                StreamPatchRequest {
                    retention: Some(stream.retention.clone()),
                    consistency: None,
                    delivery: None,
                    durable: None,
                },
            )
            .await
            .expect("patch stream");

        store
            .delete_stream(&StreamKey {
                tenant_id: "t1".to_string(),
                namespace: "default".to_string(),
                stream: "orders".to_string(),
            })
            .await
            .expect("delete stream");

        let changes = store.stream_changes(0).await.expect("stream changes");
        let seqs: Vec<u64> = changes.items.iter().map(|c| c.seq).collect();
        assert!(seqs.windows(2).all(|w| w[1] > w[0]));
        if let Some(last) = seqs.last() {
            assert_eq!(changes.next_seq, last + 1);
        }
        let snapshot = store.stream_snapshot().await.expect("snapshot");
        assert_eq!(snapshot.next_seq, changes.next_seq);
    }

    #[cfg(feature = "pg-tests")]
    #[tokio::test]
    async fn pg_delete_namespace_emits_cascades() {
        let Some(store) = pg_store().await else {
            return;
        };

        store
            .create_tenant(Tenant {
                tenant_id: "t1".to_string(),
                display_name: "Tenant One".to_string(),
            })
            .await
            .expect("tenant");
        store
            .create_namespace(Namespace {
                tenant_id: "t1".to_string(),
                namespace: "ns1".to_string(),
                display_name: "NS1".to_string(),
            })
            .await
            .expect("namespace");
        store
            .create_stream(Stream {
                tenant_id: "t1".to_string(),
                namespace: "ns1".to_string(),
                stream: "orders".to_string(),
                kind: StreamKind::Stream,
                shards: 1,
                retention: RetentionPolicy {
                    max_age_seconds: None,
                    max_size_bytes: None,
                },
                consistency: ConsistencyLevel::Leader,
                delivery: DeliveryGuarantee::AtLeastOnce,
                durable: false,
            })
            .await
            .expect("stream");
        store
            .create_cache(Cache {
                tenant_id: "t1".to_string(),
                namespace: "ns1".to_string(),
                cache: "primary".to_string(),
                display_name: "Primary".to_string(),
            })
            .await
            .expect("cache");

        store
            .delete_namespace(&NamespaceKey {
                tenant_id: "t1".to_string(),
                namespace: "ns1".to_string(),
            })
            .await
            .expect("delete namespace");

        let stream_changes = store.stream_changes(0).await.expect("stream changes");
        assert!(
            stream_changes
                .items
                .iter()
                .any(|c| matches!(c.op, StreamChangeOp::Deleted))
        );

        let cache_changes = store.cache_changes(0).await.expect("cache changes");
        assert!(
            cache_changes
                .items
                .iter()
                .any(|c| matches!(c.op, CacheChangeOp::Deleted))
        );

        let ns_changes = store.namespace_changes(0).await.expect("ns changes");
        assert!(
            ns_changes
                .items
                .iter()
                .any(|c| matches!(c.op, NamespaceChangeOp::Deleted))
        );
    }
}
