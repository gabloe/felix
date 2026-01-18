// Control plane HTTP API for metadata management.
// Maintains in-memory registries for tenants, namespaces, streams, and caches, plus
// change logs that brokers poll to keep their local caches up to date.
mod config;
mod observability;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utoipa::{OpenApi, ToSchema};

#[derive(Clone, Debug)]
struct AppState {
    region: Region,
    api_version: String,
    features: FeatureFlags,
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    namespaces: Arc<RwLock<HashMap<NamespaceKey, Namespace>>>,
    tenant_changes: Arc<RwLock<TenantChangeLog>>,
    namespace_changes: Arc<RwLock<NamespaceChangeLog>>,
    streams: Arc<RwLock<HashMap<StreamKey, Stream>>>,
    stream_changes: Arc<RwLock<StreamChangeLog>>,
    caches: Arc<RwLock<HashMap<CacheKey, Cache>>>,
    cache_changes: Arc<RwLock<CacheChangeLog>>,
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
struct Tenant {
    tenant_id: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
struct NamespaceKey {
    tenant_id: String,
    namespace: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct Namespace {
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
struct TenantChange {
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
enum TenantChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Default)]
struct TenantChangeLog {
    next_seq: u64,
    items: VecDeque<TenantChange>,
}

impl TenantChangeLog {
    fn record(&mut self, op: TenantChangeOp, tenant_id: String, tenant: Option<Tenant>) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(TenantChange {
            seq,
            op,
            tenant_id,
            tenant,
        });
        const MAX_CHANGES: usize = 1024;
        while self.items.len() > MAX_CHANGES {
            self.items.pop_front();
        }
        seq
    }
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
struct NamespaceChange {
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
enum NamespaceChangeOp {
    Created,
    Deleted,
}

#[derive(Debug, Default)]
struct NamespaceChangeLog {
    next_seq: u64,
    items: VecDeque<NamespaceChange>,
}

impl NamespaceChangeLog {
    fn record(
        &mut self,
        op: NamespaceChangeOp,
        key: NamespaceKey,
        namespace: Option<Namespace>,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(NamespaceChange {
            seq,
            op,
            key,
            namespace,
        });
        const MAX_CHANGES: usize = 1024;
        while self.items.len() > MAX_CHANGES {
            self.items.pop_front();
        }
        seq
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    tenant_id: String,
    namespace: String,
    stream: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct Stream {
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

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    tenant_id: String,
    namespace: String,
    cache: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct Cache {
    tenant_id: String,
    namespace: String,
    cache: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct CacheCreateRequest {
    cache: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct CachePatchRequest {
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
struct CacheChange {
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
enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Default)]
struct CacheChangeLog {
    next_seq: u64,
    items: VecDeque<CacheChange>,
}

impl CacheChangeLog {
    fn record(&mut self, op: CacheChangeOp, key: CacheKey, cache: Option<Cache>) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(CacheChange {
            seq,
            op,
            key,
            cache,
        });
        const MAX_CHANGES: usize = 1024;
        while self.items.len() > MAX_CHANGES {
            self.items.pop_front();
        }
        seq
    }
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
struct StreamChange {
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
enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct StreamPatchRequest {
    retention: Option<RetentionPolicy>,
    consistency: Option<ConsistencyLevel>,
    delivery: Option<DeliveryGuarantee>,
    durable: Option<bool>,
}

#[derive(Debug, Default)]
struct StreamChangeLog {
    next_seq: u64,
    items: VecDeque<StreamChange>,
}

impl StreamChangeLog {
    fn record(&mut self, op: StreamChangeOp, key: StreamKey, stream: Option<Stream>) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.items.push_back(StreamChange {
            seq,
            op,
            key,
            stream,
        });
        const MAX_CHANGES: usize = 1024;
        while self.items.len() > MAX_CHANGES {
            self.items.pop_front();
        }
        seq
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
struct RetentionPolicy {
    max_age_seconds: Option<u64>,
    max_size_bytes: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
enum StreamKind {
    Stream,
    Queue,
    Cache,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
enum ConsistencyLevel {
    Leader,
    Quorum,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
enum DeliveryGuarantee {
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

#[utoipa::path(
    get,
    path = "/v1/system/health",
    tag = "system",
    responses(
        (status = 200, description = "Control plane health", body = HealthStatus)
    )
)]
async fn system_health() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok".to_string(),
    })
}

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
    let guard = state.streams.read().await;
    let items = guard
        .values()
        .filter(|stream| stream.tenant_id == tenant_id && stream.namespace == namespace)
        .cloned()
        .collect();
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
    let guard = state.caches.read().await;
    let items = guard
        .values()
        .filter(|cache| cache.tenant_id == tenant_id && cache.namespace == namespace)
        .cloned()
        .collect();
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
async fn list_tenants(State(state): State<AppState>) -> Json<TenantListResponse> {
    let guard = state.tenants.read().await;
    Json(TenantListResponse {
        items: guard.values().cloned().collect(),
    })
}

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
    // Create a tenant and emit a change for broker cache refresh.
    let mut guard = state.tenants.write().await;
    if guard.contains_key(&body.tenant_id) {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            body: ErrorResponse {
                code: "already_exists".to_string(),
                message: "tenant already exists".to_string(),
                request_id: None,
            },
        });
    }
    let tenant = Tenant {
        tenant_id: body.tenant_id,
        display_name: body.display_name,
    };
    guard.insert(tenant.tenant_id.clone(), tenant.clone());
    state.tenant_changes.write().await.record(
        TenantChangeOp::Created,
        tenant.tenant_id.clone(),
        Some(tenant.clone()),
    );
    Ok((StatusCode::CREATED, Json(tenant)))
}

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
    let mut tenants = state.tenants.write().await;
    if tenants.remove(&tenant_id).is_none() {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "tenant not found".to_string(),
                request_id: None,
            },
        });
    }
    drop(tenants);

    let mut namespaces = state.namespaces.write().await;
    let namespace_keys = namespaces
        .keys()
        .filter(|key| key.tenant_id == tenant_id)
        .cloned()
        .collect::<Vec<_>>();
    for key in &namespace_keys {
        if let Some(namespace) = namespaces.remove(key) {
            state.namespace_changes.write().await.record(
                NamespaceChangeOp::Deleted,
                key.clone(),
                Some(namespace),
            );
        }
    }
    drop(namespaces);

    let mut streams = state.streams.write().await;
    let stream_keys = streams
        .keys()
        .filter(|key| key.tenant_id == tenant_id)
        .cloned()
        .collect::<Vec<_>>();
    for key in &stream_keys {
        if let Some(stream) = streams.remove(key) {
            state.stream_changes.write().await.record(
                StreamChangeOp::Deleted,
                key.clone(),
                Some(stream),
            );
        }
    }
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);

    let mut caches = state.caches.write().await;
    let cache_keys = caches
        .keys()
        .filter(|key| key.tenant_id == tenant_id)
        .cloned()
        .collect::<Vec<_>>();
    for key in &cache_keys {
        if let Some(cache) = caches.remove(key) {
            state.cache_changes.write().await.record(
                CacheChangeOp::Deleted,
                key.clone(),
                Some(cache),
            );
        }
    }
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);
    state
        .tenant_changes
        .write()
        .await
        .record(TenantChangeOp::Deleted, tenant_id, None);
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/tenants/snapshot",
    tag = "tenants",
    responses(
        (status = 200, description = "Full tenant snapshot", body = TenantSnapshotResponse)
    )
)]
async fn tenant_snapshot(State(state): State<AppState>) -> Json<TenantSnapshotResponse> {
    let guard = state.tenants.read().await;
    let items = guard.values().cloned().collect::<Vec<_>>();
    let next_seq = state.tenant_changes.read().await.next_seq;
    Json(TenantSnapshotResponse { items, next_seq })
}

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
) -> Json<TenantChangesResponse> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let guard = state.tenant_changes.read().await;
    let items = guard
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .cloned()
        .collect();
    Json(TenantChangesResponse {
        items,
        next_seq: guard.next_seq,
    })
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
    let guard = state.namespaces.read().await;
    Ok(Json(NamespaceListResponse {
        items: guard
            .values()
            .filter(|namespace| namespace.tenant_id == tenant_id)
            .cloned()
            .collect(),
    }))
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
    let key = NamespaceKey {
        tenant_id: tenant_id.clone(),
        namespace: body.namespace.clone(),
    };
    let mut guard = state.namespaces.write().await;
    if guard.contains_key(&key) {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            body: ErrorResponse {
                code: "already_exists".to_string(),
                message: "namespace already exists".to_string(),
                request_id: None,
            },
        });
    }
    let namespace = Namespace {
        tenant_id,
        namespace: body.namespace,
        display_name: body.display_name,
    };
    guard.insert(key.clone(), namespace.clone());
    state.namespace_changes.write().await.record(
        NamespaceChangeOp::Created,
        key,
        Some(namespace.clone()),
    );
    Ok((StatusCode::CREATED, Json(namespace)))
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
    let mut namespaces = state.namespaces.write().await;
    let removed = namespaces.remove(&key);
    drop(namespaces);
    if removed.is_none() {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "namespace not found".to_string(),
                request_id: None,
            },
        });
    }
    state
        .namespace_changes
        .write()
        .await
        .record(NamespaceChangeOp::Deleted, key.clone(), None);

    let mut streams = state.streams.write().await;
    let stream_keys = streams
        .keys()
        .filter(|stream_key| stream_key.tenant_id == tenant_id && stream_key.namespace == namespace)
        .cloned()
        .collect::<Vec<_>>();
    for stream_key in &stream_keys {
        if let Some(stream) = streams.remove(stream_key) {
            state.stream_changes.write().await.record(
                StreamChangeOp::Deleted,
                stream_key.clone(),
                Some(stream),
            );
        }
    }
    metrics::gauge!("felix_streams_total").set(streams.len() as f64);

    let mut caches = state.caches.write().await;
    let cache_keys = caches
        .keys()
        .filter(|cache_key| cache_key.tenant_id == tenant_id && cache_key.namespace == namespace)
        .cloned()
        .collect::<Vec<_>>();
    for cache_key in &cache_keys {
        if let Some(cache) = caches.remove(cache_key) {
            state.cache_changes.write().await.record(
                CacheChangeOp::Deleted,
                cache_key.clone(),
                Some(cache),
            );
        }
    }
    metrics::gauge!("felix_caches_total").set(caches.len() as f64);
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/snapshot",
    tag = "namespaces",
    responses(
        (status = 200, description = "Full namespace snapshot", body = NamespaceSnapshotResponse)
    )
)]
async fn namespace_snapshot(State(state): State<AppState>) -> Json<NamespaceSnapshotResponse> {
    let guard = state.namespaces.read().await;
    let items = guard.values().cloned().collect::<Vec<_>>();
    let next_seq = state.namespace_changes.read().await.next_seq;
    Json(NamespaceSnapshotResponse { items, next_seq })
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
async fn namespace_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<NamespaceChangesResponse> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let guard = state.namespace_changes.read().await;
    let items = guard
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .cloned()
        .collect();
    Json(NamespaceChangesResponse {
        items,
        next_seq: guard.next_seq,
    })
}

#[utoipa::path(
    get,
    path = "/v1/streams/snapshot",
    tag = "streams",
    responses(
        (status = 200, description = "Full stream snapshot", body = StreamSnapshotResponse)
    )
)]
async fn stream_snapshot(State(state): State<AppState>) -> Json<StreamSnapshotResponse> {
    let guard = state.streams.read().await;
    let items = guard.values().cloned().collect::<Vec<_>>();
    let next_seq = state.stream_changes.read().await.next_seq;
    Json(StreamSnapshotResponse { items, next_seq })
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
async fn stream_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<StreamChangesResponse> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let guard = state.stream_changes.read().await;
    let items = guard
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .cloned()
        .collect();
    Json(StreamChangesResponse {
        items,
        next_seq: guard.next_seq,
    })
}

#[utoipa::path(
    get,
    path = "/v1/caches/snapshot",
    tag = "caches",
    responses(
        (status = 200, description = "Full cache snapshot", body = CacheSnapshotResponse)
    )
)]
async fn cache_snapshot(State(state): State<AppState>) -> Json<CacheSnapshotResponse> {
    let guard = state.caches.read().await;
    let items = guard.values().cloned().collect::<Vec<_>>();
    let next_seq = state.cache_changes.read().await.next_seq;
    Json(CacheSnapshotResponse { items, next_seq })
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
async fn cache_changes(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<CacheChangesResponse> {
    let since = params
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let guard = state.cache_changes.read().await;
    let items = guard
        .items
        .iter()
        .filter(|item| item.seq >= since)
        .cloned()
        .collect();
    Json(CacheChangesResponse {
        items,
        next_seq: guard.next_seq,
    })
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
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream: body.stream.clone(),
    };
    let mut guard = state.streams.write().await;
    if guard.contains_key(&key) {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            body: ErrorResponse {
                code: "conflict".to_string(),
                message: "stream already exists".to_string(),
                request_id: None,
            },
        });
    }
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
    guard.insert(key.clone(), stream.clone());
    state
        .stream_changes
        .write()
        .await
        .record(StreamChangeOp::Created, key, Some(stream.clone()));
    metrics::counter!("felix_stream_changes_total", "op" => "created").increment(1);
    metrics::gauge!("felix_streams_total").set(guard.len() as f64);
    Ok((StatusCode::CREATED, Json(stream)))
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
    let guard = state.streams.read().await;
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream,
    };
    guard.get(&key).cloned().map(Json).ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: "stream not found".to_string(),
            request_id: None,
        },
    })
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
    let mut guard = state.streams.write().await;
    let stream = guard.get_mut(&key).ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: "stream not found".to_string(),
            request_id: None,
        },
    })?;
    if let Some(retention) = body.retention {
        stream.retention = retention;
    }
    if let Some(consistency) = body.consistency {
        stream.consistency = consistency;
    }
    if let Some(delivery) = body.delivery {
        stream.delivery = delivery;
    }
    if let Some(durable) = body.durable {
        stream.durable = durable;
    }
    let stream_id = stream.stream.clone();
    let key = StreamKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        stream: stream_id,
    };
    state
        .stream_changes
        .write()
        .await
        .record(StreamChangeOp::Updated, key, Some(stream.clone()));
    metrics::counter!("felix_stream_changes_total", "op" => "updated").increment(1);
    Ok(Json(stream.clone()))
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
    let mut guard = state.streams.write().await;
    if guard.remove(&key).is_none() {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "stream not found".to_string(),
                request_id: None,
            },
        });
    }
    state
        .stream_changes
        .write()
        .await
        .record(StreamChangeOp::Deleted, key, None);
    metrics::counter!("felix_stream_changes_total", "op" => "deleted").increment(1);
    metrics::gauge!("felix_streams_total").set(guard.len() as f64);
    Ok(StatusCode::NO_CONTENT)
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
        (status = 404, description = "Tenant or namespace not found", body = ErrorResponse),
        (status = 409, description = "Cache already exists", body = ErrorResponse)
    )
)]
async fn create_cache(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<CacheCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    ensure_tenant_namespace(&state, &tenant_id, &namespace).await?;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache: body.cache.clone(),
    };
    let mut guard = state.caches.write().await;
    if guard.contains_key(&key) {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            body: ErrorResponse {
                code: "conflict".to_string(),
                message: "cache already exists".to_string(),
                request_id: None,
            },
        });
    }
    let cache = Cache {
        tenant_id,
        namespace,
        cache: body.cache,
        display_name: body.display_name,
    };
    guard.insert(key.clone(), cache.clone());
    state
        .cache_changes
        .write()
        .await
        .record(CacheChangeOp::Created, key, Some(cache.clone()));
    metrics::counter!("felix_cache_changes_total", "op" => "created").increment(1);
    metrics::gauge!("felix_caches_total").set(guard.len() as f64);
    Ok((StatusCode::CREATED, Json(cache)))
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
    let guard = state.caches.read().await;
    let key = CacheKey {
        tenant_id: tenant_id.clone(),
        namespace: namespace.clone(),
        cache,
    };
    guard.get(&key).cloned().map(Json).ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: "cache not found".to_string(),
            request_id: None,
        },
    })
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
    let mut guard = state.caches.write().await;
    let cache = guard.get_mut(&key).ok_or(ApiError {
        status: StatusCode::NOT_FOUND,
        body: ErrorResponse {
            code: "not_found".to_string(),
            message: "cache not found".to_string(),
            request_id: None,
        },
    })?;
    if let Some(display_name) = body.display_name {
        cache.display_name = display_name;
    }
    state
        .cache_changes
        .write()
        .await
        .record(CacheChangeOp::Updated, key, Some(cache.clone()));
    metrics::counter!("felix_cache_changes_total", "op" => "updated").increment(1);
    Ok(Json(cache.clone()))
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
    let mut guard = state.caches.write().await;
    if guard.remove(&key).is_none() {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "cache not found".to_string(),
                request_id: None,
            },
        });
    }
    state
        .cache_changes
        .write()
        .await
        .record(CacheChangeOp::Deleted, key, None);
    metrics::counter!("felix_cache_changes_total", "op" => "deleted").increment(1);
    metrics::gauge!("felix_caches_total").set(guard.len() as f64);
    Ok(StatusCode::NO_CONTENT)
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
async fn main() -> Result<(), std::io::Error> {
    let metrics_handle = observability::init_observability("felix-controlplane");

    let config = config::ControlPlaneConfig::from_env_or_yaml().expect("control plane config");
    let state = default_state_with_region_id(config.region_id.clone());
    tokio::spawn(observability::serve_metrics(
        metrics_handle,
        config.metrics_bind,
    ));

    let app = build_app(state);

    let addr = config.bind_addr;
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await
}

fn default_state_with_region_id(region_id: String) -> AppState {
    // In-memory state for local development; durable metadata comes later.
    let region = Region {
        region_id,
        display_name: "Local Region".to_string(),
    };
    AppState {
        region,
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        tenants: Arc::new(RwLock::new(HashMap::new())),
        namespaces: Arc::new(RwLock::new(HashMap::new())),
        tenant_changes: Arc::new(RwLock::new(TenantChangeLog::default())),
        namespace_changes: Arc::new(RwLock::new(NamespaceChangeLog::default())),
        streams: Arc::new(RwLock::new(HashMap::new())),
        stream_changes: Arc::new(RwLock::new(StreamChangeLog::default())),
        caches: Arc::new(RwLock::new(HashMap::new())),
        cache_changes: Arc::new(RwLock::new(CacheChangeLog::default())),
    }
}

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

async fn ensure_tenant_namespace(
    state: &AppState,
    tenant_id: &str,
    namespace: &str,
) -> Result<(), ApiError> {
    // Centralized guard so stream routes return consistent 404s.
    ensure_tenant_exists(state, tenant_id).await?;
    let namespace_key = NamespaceKey {
        tenant_id: tenant_id.to_string(),
        namespace: namespace.to_string(),
    };
    if !state.namespaces.read().await.contains_key(&namespace_key) {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "namespace not found".to_string(),
                request_id: None,
            },
        });
    }
    Ok(())
}

async fn ensure_tenant_exists(state: &AppState, tenant_id: &str) -> Result<(), ApiError> {
    if !state.tenants.read().await.contains_key(tenant_id) {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse {
                code: "not_found".to_string(),
                message: "tenant not found".to_string(),
                request_id: None,
            },
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

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
}
