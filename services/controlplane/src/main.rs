use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;
use utoipa::{OpenApi, ToSchema};

#[derive(Clone, Debug)]
struct AppState {
    region: Region,
    api_version: String,
    features: FeatureFlags,
    streams: Arc<RwLock<HashMap<StreamKey, Stream>>>,
    stream_changes: Arc<RwLock<StreamChangeLog>>,
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
        (status = 200, description = "List streams", body = StreamListResponse)
    )
)]
async fn list_streams(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Json<StreamListResponse> {
    let guard = state.streams.read().await;
    let items = guard
        .values()
        .filter(|stream| stream.tenant_id == tenant_id && stream.namespace == namespace)
        .cloned()
        .collect();
    Json(StreamListResponse { items })
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
        (status = 409, description = "Stream already exists", body = ErrorResponse)
    )
)]
async fn create_stream(
    Path((tenant_id, namespace)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(body): Json<StreamCreateRequest>,
) -> Result<impl IntoResponse, ApiError> {
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
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
async fn get_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Stream>, ApiError> {
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
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
async fn patch_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(body): Json<StreamPatchRequest>,
) -> Result<Json<Stream>, ApiError> {
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
        (status = 404, description = "Stream not found", body = ErrorResponse)
    )
)]
async fn delete_stream(
    Path((tenant_id, namespace, stream)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<StatusCode, ApiError> {
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
        stream_snapshot,
        stream_changes,
        list_streams,
        create_stream,
        get_stream,
        patch_stream,
        delete_stream
    ),
    components(schemas(
        FeatureFlags,
        SystemInfo,
        HealthStatus,
        Region,
        ListRegionsResponse,
        ErrorResponse,
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
        (name = "streams", description = "Stream management")
    )
)]
struct ApiDoc;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let region = Region {
        region_id: std::env::var("FELIX_REGION_ID").unwrap_or_else(|_| "local".to_string()),
        display_name: "Local Region".to_string(),
    };
    let state = AppState {
        region,
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: false,
            tiered_storage: false,
            bridges: false,
        },
        streams: Arc::new(RwLock::new(HashMap::new())),
        stream_changes: Arc::new(RwLock::new(StreamChangeLog::default())),
    };

    let app = Router::new()
        .route("/v1/system/info", get(system_info))
        .route("/v1/system/health", get(system_health))
        .route("/v1/regions", get(list_regions))
        .route("/v1/regions/:region_id", get(get_region))
        .route("/v1/streams/snapshot", get(stream_snapshot))
        .route("/v1/streams/changes", get(stream_changes))
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams",
            get(list_streams).post(create_stream),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams/:stream",
            get(get_stream).patch(patch_stream).delete(delete_stream),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs").url("/v1/openapi.json", ApiDoc::openapi()),
        )
        .with_state(state);

    let addr: SocketAddr = std::env::var("FELIX_CP_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
        .parse()
        .expect("FELIX_CP_BIND");
    tracing::info!(%addr, "control plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await
}
