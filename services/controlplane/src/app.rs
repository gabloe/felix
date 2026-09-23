//! Control-plane HTTP application wiring.
//!
//! Builds the Axum router, configures middleware, and defines the shared
//! application state injected into handlers.
//!
//! This module centralizes route composition to keep `main` small and testable.
use crate::api;
use crate::api::openapi::ApiDoc;
use crate::api::types::{FeatureFlags, Region};
use crate::auth;
use crate::auth::oidc::UpstreamOidcValidator;
use crate::config::NodeLivenessConfig;
use crate::observability;
use crate::store::ControlPlaneAuthStore;
use axum::Router;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utoipa::OpenApi;

#[derive(Clone)]
pub struct AppState {
    pub region: Region,
    pub api_version: String,
    pub features: FeatureFlags,
    pub store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    pub oidc_validator: UpstreamOidcValidator,
    pub bootstrap_enabled: bool,
    /// Accepted bootstrap tokens, current first. More than one only during a
    /// rotation, so replacing the token is a rolling deploy rather than an
    /// outage — see [`crate::api::bootstrap::initialize`].
    pub bootstrap_tokens: Vec<String>,
    pub node_liveness: NodeLivenessConfig,
    /// Which replicas their leaders last reported as holding each shard's log.
    ///
    /// In memory: these change constantly, are advisory, and expire in about a
    /// second. Persisting them would cost a write per report for data that is
    /// worthless by the time it could be read back. The consequence is that a
    /// second control-plane instance starts knowing nothing and cannot promote
    /// until leaders have reported to *it* — see `docs/replication-design.md`.
    /// Whether this instance can serve, bounded and cached.
    pub readiness: Arc<crate::readiness::Readiness>,
    /// Requests currently being served, so a drain can say what it waited for.
    pub in_flight: felix_common::lifecycle::InFlight,
}

/// Count a request for the whole time it is being served.
///
/// Placed outside the handlers so it cannot be forgotten on a new route, and so
/// it counts time spent in every other layer too — a request stuck in the trace
/// layer is still a request this instance owes an answer for.
async fn count_in_flight(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _guard = state.in_flight.enter();
    next.run(request).await
}

pub fn build_router(state: AppState) -> Router {
    let trace_layer =
        TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
            let parent = observability::trace_context_from_headers(request.headers());
            let span = tracing::info_span!(
                "http.request",
                method = %request.method(),
                uri = %request.uri(),
                version = ?request.version()
            );
            let _ = span.set_parent(parent);
            span
        });

    Router::new()
        .route(
            "/v1/system/info",
            axum::routing::get(api::system::system_info),
        )
        .route(
            "/v1/system/health",
            axum::routing::get(api::system::system_health),
        )
        // Two probes, because they drive different actions: `live` decides
        // whether to restart the process, `ready` whether to send it traffic.
        // See `docs/control-plane.md` for the intervals these expect.
        .route(
            "/v1/system/live",
            axum::routing::get(api::system::system_live),
        )
        .route(
            "/v1/system/ready",
            axum::routing::get(api::system::system_ready),
        )
        .route(
            "/v1/regions",
            axum::routing::get(api::regions::list_regions),
        )
        .route(
            "/v1/regions/{region_id}",
            axum::routing::get(api::regions::get_region),
        )
        .route(
            "/v1/tenants/snapshot",
            axum::routing::get(api::tenants::tenant_snapshot),
        )
        .route(
            "/v1/tenants/changes",
            axum::routing::get(api::tenants::tenant_changes),
        )
        .route(
            "/v1/namespaces/snapshot",
            axum::routing::get(api::namespaces::namespace_snapshot),
        )
        .route(
            "/v1/namespaces/changes",
            axum::routing::get(api::namespaces::namespace_changes),
        )
        .route(
            "/v1/caches/snapshot",
            axum::routing::get(api::caches::cache_snapshot),
        )
        .route(
            "/v1/caches/changes",
            axum::routing::get(api::caches::cache_changes),
        )
        .route(
            "/v1/streams/snapshot",
            axum::routing::get(api::streams::stream_snapshot),
        )
        .route(
            "/v1/streams/changes",
            axum::routing::get(api::streams::stream_changes),
        )
        .route(
            "/v1/tenants",
            axum::routing::get(api::tenants::list_tenants).post(api::tenants::create_tenant),
        )
        .route(
            "/v1/tenants/{tenant_id}",
            axum::routing::delete(api::tenants::delete_tenant),
        )
        .route(
            "/v1/nodes",
            axum::routing::get(api::nodes::list_nodes).post(api::nodes::register_node),
        )
        .route(
            "/v1/nodes/{node_id}",
            axum::routing::get(api::nodes::get_node)
                .patch(api::nodes::patch_node)
                .delete(api::nodes::delete_node),
        )
        .route(
            "/v1/shard-assignments",
            axum::routing::get(api::nodes::list_shard_assignments),
        )
        .route(
            "/v1/shard-assignments/snapshot",
            axum::routing::get(api::nodes::shard_assignment_snapshot),
        )
        .route(
            "/v1/shard-assignments/changes",
            axum::routing::get(api::nodes::shard_assignment_changes),
        )
        .route(
            "/v1/nodes/{node_id}/heartbeat",
            axum::routing::post(api::nodes::report_health),
        )
        .route(
            "/v1/nodes/{node_id}/replica-status",
            axum::routing::post(api::nodes::report_replica_status),
        )
        .route(
            "/v1/nodes/{node_id}/drain",
            axum::routing::post(api::nodes::drain_node),
        )
        .route(
            "/v1/nodes/{node_id}/deregister",
            axum::routing::post(api::nodes::deregister_node),
        )
        .route(
            "/v1/tenants/{tenant_id}/token/exchange",
            axum::routing::post(auth::exchange::exchange_token),
        )
        .route(
            "/v1/tenants/{tenant_id}/token/refresh",
            axum::routing::post(auth::refresh::refresh_token_handler),
        )
        .route(
            "/v1/tenants/{tenant_id}/.well-known/jwks.json",
            axum::routing::get(auth::jwks::tenant_jwks),
        )
        .route(
            "/v1/tenants/{tenant_id}/idp-issuers",
            axum::routing::post(auth::admin::upsert_idp_issuer),
        )
        .route(
            "/v1/tenants/{tenant_id}/idp-issuers/{issuer}",
            axum::routing::delete(auth::admin::delete_idp_issuer),
        )
        .route(
            "/v1/tenants/{tenant_id}/rbac/policies",
            axum::routing::get(auth::admin::list_policies).post(auth::admin::add_policy),
        )
        .route(
            "/v1/tenants/{tenant_id}/rbac/groupings",
            axum::routing::get(auth::admin::list_groupings).post(auth::admin::add_grouping),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces",
            axum::routing::get(api::namespaces::list_namespaces)
                .post(api::namespaces::create_namespace),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}",
            axum::routing::delete(api::namespaces::delete_namespace),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams",
            axum::routing::get(api::streams::list_streams).post(api::streams::create_stream),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/streams/{stream}",
            axum::routing::get(api::streams::get_stream)
                .patch(api::streams::patch_stream)
                .delete(api::streams::delete_stream),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches",
            axum::routing::get(api::caches::list_caches).post(api::caches::create_cache),
        )
        .route(
            "/v1/tenants/{tenant_id}/namespaces/{namespace}/caches/{cache}",
            axum::routing::get(api::caches::get_cache)
                .patch(api::caches::patch_cache)
                .delete(api::caches::delete_cache),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs").url("/v1/openapi.json", ApiDoc::openapi()),
        )
        .layer(trace_layer)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            count_in_flight,
        ))
        .with_state(state)
}

pub fn build_bootstrap_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/internal/bootstrap/tenants/{tenant_id}/initialize",
            axum::routing::post(api::bootstrap::initialize),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
