//! System/health API handlers.
//!
//! # Purpose and responsibility
//! Provides lightweight endpoints for service metadata and health checks.
//!
//! # Where it fits in Felix
//! Used by operators, probes, and automation to validate control-plane health
//! and discover cluster capabilities.
//!
//! # Key invariants and assumptions
//! - Health checks must be fast and side-effect free.
//! - System info is derived from in-memory configuration.
//!
//! # Security considerations
//! - These endpoints are read-only but still reveal deployment metadata.
use crate::api::error::ApiError;
use crate::api::types::{HealthStatus, SystemInfo};
use crate::app::AppState;
use axum::Json;
use axum::extract::State;

#[utoipa::path(
    get,
    path = "/v1/system/info",
    tag = "system",
    responses(
        (status = 200, description = "Cluster identity and capabilities", body = SystemInfo)
    )
)]
/// Return control-plane identity and feature flags.
///
/// Exposes region ID, API version, and feature toggles.
///
/// Enables clients and operators to discover capabilities at runtime.
///
/// # Errors
/// - Does not return errors.
pub(crate) async fn system_info(State(state): State<AppState>) -> Json<SystemInfo> {
    // Build the response from in-memory configuration (no I/O).
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
/// Return control-plane health status.
///
/// Probes the backing store and returns `ok` if healthy.
///
/// Supports readiness/liveness checks and operational monitoring.
///
/// # Errors
/// - Returns 500 if storage health check fails.
pub(crate) async fn system_health(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    // The original probe path, kept because deployments already point at it.
    // It answers the readiness question, which is what a caller polling
    // "health" almost always wants: whether to send this instance traffic.
    system_ready(State(state)).await
}

#[utoipa::path(
    get,
    path = "/v1/system/live",
    tag = "system",
    responses((status = 200, description = "The process is running", body = HealthStatus))
)]
/// Whether this process should be left alone.
///
/// Deliberately answers without touching the database or anything else outside
/// the process. A liveness probe drives *restarts*, and restarting every
/// instance because Postgres is down turns one external outage into a
/// cluster-wide restart loop that cannot fix it.
///
/// It fails when the process cannot answer at all — wedged, out of file
/// descriptors, or its runtime blocked — which is the one condition a restart
/// does address.
pub(crate) async fn system_live() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok".to_string(),
    })
}

#[utoipa::path(
    get,
    path = "/v1/system/ready",
    tag = "system",
    responses(
        (status = 200, description = "Ready to serve metadata", body = HealthStatus),
        (status = 503, description = "Not ready", body = crate::api::types::ErrorResponse)
    )
)]
/// Whether this instance should be sent traffic.
///
/// Checks the store, bounded and cached — see [`crate::readiness`]. Answers 503
/// rather than 500: this is a statement about *this instance right now*, and a
/// load balancer removing it is the correct response, not an error to alert on.
pub(crate) async fn system_ready(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    match state.readiness.check().await {
        Ok(()) => Ok(Json(HealthStatus {
            status: "ok".to_string(),
        })),
        Err(reason) => Err(crate::api::error::api_error(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            &reason.to_string(),
        )),
    }
}
