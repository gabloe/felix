//! OpenAPI schema aggregation for the control-plane API.
//!
//! # Purpose
//! Collects all routes and schema types into a single OpenAPI document for docs
//! and client generation.
use crate::api::{
    caches, namespaces, regions, streams, system, tenants,
    types::{
        CacheChangesResponse, CacheCreateRequest, CacheListResponse, CacheSnapshotResponse,
        ErrorResponse, FeatureFlags, HealthStatus, ListRegionsResponse, NamespaceChangesResponse,
        NamespaceCreateRequest, NamespaceListResponse, NamespaceSnapshotResponse, Region,
        StreamChangesResponse, StreamCreateRequest, StreamListResponse, StreamSnapshotResponse,
        SystemInfo, TenantChangesResponse, TenantCreateRequest, TenantListResponse,
        TenantSnapshotResponse,
    },
};
use crate::auth::admin;
use crate::auth::admin::{GroupingRequest, PolicyRequest};
use crate::auth::exchange::{self, TokenExchangeRequest, TokenExchangeResponse};
use crate::auth::idp_registry::IdpIssuerConfig;
use crate::auth::jwks::{self, JwksResponse};
use crate::auth::rbac::policy_store::{GroupingRule, PolicyRule};
use crate::model::{
    Cache, CacheChange, CacheChangeOp, CacheKey, CachePatchRequest, ConsistencyLevel,
    DeliveryGuarantee, Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey,
    RetentionPolicy, Stream, StreamChange, StreamChangeOp, StreamKey, StreamKind,
    StreamPatchRequest, Tenant, TenantChange, TenantChangeOp,
};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "felix-controlplane",
        version = "v1",
        description = "Felix control plane HTTP API"
    ),
    paths(
        system::system_info,
        system::system_health,
        regions::list_regions,
        regions::get_region,
        exchange::exchange_token,
        jwks::tenant_jwks,
        admin::upsert_idp_issuer,
        admin::delete_idp_issuer,
        admin::add_policy,
        admin::add_grouping,
        tenants::list_tenants,
        tenants::create_tenant,
        tenants::delete_tenant,
        tenants::tenant_snapshot,
        tenants::tenant_changes,
        namespaces::list_namespaces,
        namespaces::create_namespace,
        namespaces::delete_namespace,
        namespaces::namespace_snapshot,
        namespaces::namespace_changes,
        caches::cache_snapshot,
        caches::cache_changes,
        streams::stream_snapshot,
        streams::stream_changes,
        streams::list_streams,
        caches::list_caches,
        streams::create_stream,
        streams::get_stream,
        streams::patch_stream,
        streams::delete_stream,
        caches::create_cache,
        caches::get_cache,
        caches::patch_cache,
        caches::delete_cache
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
        NamespaceKey,
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
        DeliveryGuarantee,
        TokenExchangeRequest,
        TokenExchangeResponse,
        IdpIssuerConfig,
        PolicyRequest,
        GroupingRequest,
        PolicyRule,
        GroupingRule,
        JwksResponse
    )),
    tags(
        (name = "system", description = "System and discovery endpoints"),
        (name = "regions", description = "Region metadata"),
        (name = "auth", description = "Authentication and token exchange"),
        (name = "tenants", description = "Tenant management"),
        (name = "namespaces", description = "Namespace management"),
        (name = "streams", description = "Stream management"),
        (name = "caches", description = "Cache management")
    )
)]
pub struct ApiDoc;
