//! Control-plane HTTP application wiring.
//!
//! # Purpose
//! Builds the Axum router, configures middleware, and defines the shared
//! application state injected into handlers.
//!
//! # Notes
//! This module centralizes route composition to keep `main` small and testable.
use crate::api;
use crate::api::openapi::ApiDoc;
use crate::api::types::{FeatureFlags, Region};
use crate::auth;
use crate::auth::oidc::UpstreamOidcValidator;
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
    pub bootstrap_token: Option<String>,
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
            span.set_parent(parent);
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
        .route(
            "/v1/regions",
            axum::routing::get(api::regions::list_regions),
        )
        .route(
            "/v1/regions/:region_id",
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
            "/v1/tenants/:tenant_id",
            axum::routing::delete(api::tenants::delete_tenant),
        )
        .route(
            "/v1/tenants/:tenant_id/token/exchange",
            axum::routing::post(auth::exchange::exchange_token),
        )
        .route(
            "/v1/tenants/:tenant_id/.well-known/jwks.json",
            axum::routing::get(auth::jwks::tenant_jwks),
        )
        .route(
            "/v1/tenants/:tenant_id/idp-issuers",
            axum::routing::post(auth::admin::upsert_idp_issuer),
        )
        .route(
            "/v1/tenants/:tenant_id/idp-issuers/:issuer",
            axum::routing::delete(auth::admin::delete_idp_issuer),
        )
        .route(
            "/v1/tenants/:tenant_id/rbac/policies",
            axum::routing::get(auth::admin::list_policies).post(auth::admin::add_policy),
        )
        .route(
            "/v1/tenants/:tenant_id/rbac/groupings",
            axum::routing::get(auth::admin::list_groupings).post(auth::admin::add_grouping),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces",
            axum::routing::get(api::namespaces::list_namespaces)
                .post(api::namespaces::create_namespace),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace",
            axum::routing::delete(api::namespaces::delete_namespace),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams",
            axum::routing::get(api::streams::list_streams).post(api::streams::create_stream),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/streams/:stream",
            axum::routing::get(api::streams::get_stream)
                .patch(api::streams::patch_stream)
                .delete(api::streams::delete_stream),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/caches",
            axum::routing::get(api::caches::list_caches).post(api::caches::create_cache),
        )
        .route(
            "/v1/tenants/:tenant_id/namespaces/:namespace/caches/:cache",
            axum::routing::get(api::caches::get_cache)
                .patch(api::caches::patch_cache)
                .delete(api::caches::delete_cache),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs").url("/v1/openapi.json", ApiDoc::openapi()),
        )
        .layer(trace_layer)
        .with_state(state)
}

pub fn build_bootstrap_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/internal/bootstrap/tenants/:tenant_id/initialize",
            axum::routing::post(api::bootstrap::initialize),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
