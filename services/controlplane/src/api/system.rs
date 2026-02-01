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
use crate::api::error::{ApiError, api_internal};
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
/// # What it does
/// Exposes region ID, API version, and feature toggles.
///
/// # Why it exists
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
/// # What it does
/// Probes the backing store and returns `ok` if healthy.
///
/// # Why it exists
/// Supports readiness/liveness checks and operational monitoring.
///
/// # Errors
/// - Returns 500 if storage health check fails.
pub(crate) async fn system_health(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    // Check backing store health to surface dependency availability.
    if let Err(err) = state.store.health_check().await {
        return Err(api_internal("storage unavailable", &err));
    }
    // Return a simple ok status for probes.
    Ok(Json(HealthStatus {
        status: "ok".to_string(),
    }))
}
