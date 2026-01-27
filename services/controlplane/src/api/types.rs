//! HTTP API request/response types.
//!
//! # Purpose
//! Defines shared payload shapes for the control-plane REST API and OpenAPI
//! schema generation.
use crate::model::{
    Cache, CacheChange, Namespace, NamespaceChange, Stream, StreamChange, Tenant, TenantChange,
};
use crate::model::{ConsistencyLevel, DeliveryGuarantee, RetentionPolicy, StreamKind};
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
