//! OpenAPI schema aggregation for the control-plane API.
//!
//! # Purpose and responsibility
//! Collects all routes and schema types into a single OpenAPI document for docs
//! and client generation.
//!
//! # Where it fits in Felix
//! Used by tooling and documentation systems to expose a machine-readable API
//! description of the control-plane.
//!
//! # Key invariants and assumptions
//! - All public API endpoints must be registered here for full coverage.
//! - Schema types must match runtime payloads.
//!
//! # Security considerations
//! - Avoid exposing internal-only endpoints in the public schema.
use crate::api::{
    caches, namespaces, nodes, regions, streams, system, tenants,
    types::{
        CacheChangesResponse, CacheCreateRequest, CacheListResponse, CacheSnapshotResponse,
        ErrorResponse, FeatureFlags, HealthStatus, ListRegionsResponse, NamespaceChangesResponse,
        NamespaceCreateRequest, NamespaceListResponse, NamespaceSnapshotResponse,
        NodeHeartbeatRequest, NodeHeartbeatResponse, NodeListResponse, NodePlacement,
        NodeRegistrationRequest, NodeRegistrationResponse, NodeView, Region,
        ShardAssignmentChangesResponse, ShardAssignmentListResponse,
        ShardAssignmentSnapshotResponse, StreamChangesResponse, StreamCreateRequest,
        StreamListResponse, StreamSnapshotResponse, SystemInfo, TenantChangesResponse,
        TenantCreateRequest, TenantListResponse, TenantSnapshotResponse,
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
    DeliveryGuarantee, Namespace, NamespaceChange, NamespaceChangeOp, NamespaceKey, Node,
    NodeCapacity, NodeChange, NodeChangeOp, NodeLifecycle, NodePatchRequest, NodeSpec, NodeStatus,
    RetentionPolicy, ShardAssignment, ShardAssignmentChange, ShardAssignmentChangeOp, ShardKey,
    ShardState, Stream, StreamChange, StreamChangeOp, StreamKey, StreamKind, StreamPatchRequest,
    Tenant, TenantChange, TenantChangeOp,
};
use utoipa::OpenApi;

/// OpenAPI document for the control-plane API.
///
/// Aggregates paths, schemas, and tags into a single `OpenApi` document.
///
/// Enables consistent documentation and client generation.
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
        caches::delete_cache,
        nodes::report_health,
        nodes::register_node,
        nodes::drain_node,
        nodes::deregister_node,
        nodes::list_nodes,
        nodes::get_node,
        nodes::list_shard_assignments,
        nodes::shard_assignment_snapshot,
        nodes::shard_assignment_changes
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
        Node,
        NodeSpec,
        NodeStatus,
        NodeCapacity,
        NodeLifecycle,
        NodePatchRequest,
        NodeChange,
        NodeChangeOp,
        NodeHeartbeatRequest,
        NodeHeartbeatResponse,
        NodeRegistrationRequest,
        NodeRegistrationResponse,
        NodeView,
        NodePlacement,
        NodeListResponse,
        ShardAssignment,
        ShardKey,
        ShardState,
        ShardAssignmentListResponse,
        ShardAssignmentSnapshotResponse,
        ShardAssignmentChangesResponse,
        ShardAssignmentChange,
        ShardAssignmentChangeOp,
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
        (name = "caches", description = "Cache management"),
        (name = "nodes", description = "Broker membership")
    )
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;

    /// A model that is not registered here is absent from the published schema
    /// and from every generated client, and nothing else would catch it.
    #[test]
    fn the_node_model_reaches_the_generated_schema() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
        let schemas = doc["components"]["schemas"]
            .as_object()
            .expect("schemas object");

        for name in [
            "Node",
            "NodeSpec",
            "NodeStatus",
            "NodeCapacity",
            "NodeLifecycle",
            "NodePatchRequest",
            "NodeChange",
            "NodeChangeOp",
            "ShardAssignment",
            "ShardKey",
            "ShardState",
            "ShardAssignmentListResponse",
            "ShardAssignmentSnapshotResponse",
            "ShardAssignmentChangesResponse",
            "ShardAssignmentChange",
            "NodeView",
            "NodePlacement",
            "NodeListResponse",
            "NodeRegistrationRequest",
            "NodeRegistrationResponse",
            "NodeHeartbeatRequest",
            "NodeHeartbeatResponse",
        ] {
            assert!(
                schemas.contains_key(name),
                "{name} is missing from the schema"
            );
        }
    }

    /// A route the document does not describe is a route no generated client
    /// can call, and nothing else in the build would notice.
    #[test]
    fn every_node_route_is_described() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
        let paths = doc["paths"].as_object().expect("paths object");

        for (path, method) in [
            ("/v1/nodes", "get"),
            ("/v1/nodes", "post"),
            ("/v1/nodes/{node_id}", "get"),
            ("/v1/nodes/{node_id}/heartbeat", "post"),
            ("/v1/nodes/{node_id}/drain", "post"),
            ("/v1/nodes/{node_id}/deregister", "post"),
            ("/v1/shard-assignments", "get"),
            ("/v1/shard-assignments/snapshot", "get"),
            ("/v1/shard-assignments/changes", "get"),
        ] {
            let described = paths
                .get(path)
                .and_then(|entry| entry.get(method))
                .is_some();
            assert!(
                described,
                "{method} {path} is missing from the OpenAPI document"
            );
        }
    }

    /// The split only holds on the wire if the patch schema has no observed
    /// field to send.
    #[test]
    fn the_node_patch_schema_exposes_no_observed_field() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("serialize openapi");
        let properties = doc["components"]["schemas"]["NodePatchRequest"]["properties"]
            .as_object()
            .expect("patch properties");

        // Guards against the negative assertions below passing on an empty map.
        for settable in ["region", "labels", "capacity", "lifecycle"] {
            assert!(
                properties.contains_key(settable),
                "{settable} should be patchable",
            );
        }
        for observed in [
            "last_heartbeat_at_millis",
            "registered_at_millis",
            "incarnation",
        ] {
            assert!(
                !properties.contains_key(observed),
                "{observed} must not be patchable",
            );
        }
    }
}
