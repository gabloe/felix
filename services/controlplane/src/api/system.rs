//! System/health API handlers.
//!
//! # Purpose
//! Provides lightweight endpoints for service metadata and health checks.
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
pub(crate) async fn system_info(State(state): State<AppState>) -> Json<SystemInfo> {
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
pub(crate) async fn system_health(
    State(state): State<AppState>,
) -> Result<Json<HealthStatus>, ApiError> {
    if let Err(err) = state.store.health_check().await {
        return Err(api_internal("storage unavailable", &err));
    }
    Ok(Json(HealthStatus {
        status: "ok".to_string(),
    }))
}
