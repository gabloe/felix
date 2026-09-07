//! HTTP API request/response types.
//!
//! Defines shared payload shapes for the control-plane REST API and OpenAPI
//! schema generation.
use crate::model::{
    Cache, CacheChange, Namespace, NamespaceChange, Stream, StreamChange, Tenant, TenantChange,
};
use crate::model::{
    ConsistencyLevel, DeliveryGuarantee, NodeLifecycle, RetentionPolicy, StreamKind,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FeatureFlags {
    pub durable_storage: bool,
    pub tiered_storage: bool,
    pub bridges: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct SystemInfo {
    pub region_id: String,
    pub api_version: String,
    pub features: FeatureFlags,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct HealthStatus {
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Region {
    pub region_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListRegionsResponse {
    pub items: Vec<Region>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantCreateRequest {
    pub tenant_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceCreateRequest {
    pub namespace: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamCreateRequest {
    pub stream: String,
    pub kind: StreamKind,
    pub shards: u32,
    pub retention: RetentionPolicy,
    pub consistency: ConsistencyLevel,
    pub delivery: DeliveryGuarantee,
    pub durable: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheCreateRequest {
    pub cache: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantListResponse {
    pub items: Vec<Tenant>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantSnapshotResponse {
    pub items: Vec<Tenant>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantChangesResponse {
    pub items: Vec<TenantChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceListResponse {
    pub items: Vec<Namespace>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceSnapshotResponse {
    pub items: Vec<Namespace>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceChangesResponse {
    pub items: Vec<NamespaceChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamListResponse {
    pub items: Vec<Stream>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamSnapshotResponse {
    pub items: Vec<Stream>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamChangesResponse {
    pub items: Vec<StreamChange>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheListResponse {
    pub items: Vec<Cache>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheSnapshotResponse {
    pub items: Vec<Cache>,
    pub next_seq: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheChangesResponse {
    pub items: Vec<CacheChange>,
    pub next_seq: u64,
}

/// A broker's report that it is alive.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeHeartbeatRequest {
    /// The reporting process's own incarnation, from its last registration.
    ///
    /// Carried so a heartbeat that was delayed past a restart is rejected
    /// rather than counted for the process that replaced it.
    pub incarnation: u64,
}

/// What the control plane tells a broker in return.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeHeartbeatResponse {
    pub node_id: String,
    /// The node's lifecycle as the cluster sees it. A broker that reads `down`
    /// here has been expired and must register again.
    pub lifecycle: NodeLifecycle,
    /// How soon the next heartbeat is expected, so the interval is configured
    /// in one place rather than on every broker.
    pub heartbeat_interval_ms: u64,
    /// Silence beyond this marks the node down.
    pub expiry_timeout_ms: u64,
}

/// A broker claiming its identity on boot.
///
/// Carries only spec fields. Observed status is the control plane's to set: a
/// broker that could declare itself live could outlive its own expiry.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeRegistrationRequest {
    pub node_id: String,
    /// `host:port` the broker-internal QUIC listener is reachable on.
    pub advertise_addr: String,
    pub region: String,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub capacity: crate::model::NodeCapacity,
}

/// What a broker learns from registering.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeRegistrationResponse {
    pub node: crate::model::Node,
    /// The cadence expected of this broker, so it is configured in one place.
    pub heartbeat_interval_ms: u64,
    pub expiry_timeout_ms: u64,
}

/// Why a node is or is not a placement candidate.
///
/// The point of the endpoint: "this broker is registered but shards are not
/// landing on it" is otherwise answered by reading a lifecycle string and doing
/// heartbeat arithmetic by hand.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodePlacement {
    pub eligible: bool,
    /// Empty when eligible. One entry per reason it is not.
    pub reasons: Vec<String>,
    /// How long since the last accepted heartbeat, against the control plane's
    /// clock at the time of the request.
    pub heartbeat_age_ms: u64,
}

/// A node as an operator sees it: the record, plus what it means.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeView {
    pub node: crate::model::Node,
    pub placement: NodePlacement,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NodeListResponse {
    pub items: Vec<NodeView>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShardAssignmentListResponse {
    pub items: Vec<crate::model::ShardAssignment>,
}
