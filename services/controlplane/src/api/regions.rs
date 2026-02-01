//! Region API handlers.
//!
//! # Purpose and responsibility
//! Exposes configured region metadata for control-plane clients.
//!
//! # Where it fits in Felix
//! Used by clients to discover which region the control-plane represents.
//!
//! # Key invariants and assumptions
//! - The control-plane currently exposes a single configured region.
//!
//! # Security considerations
//! - Region metadata is public but should avoid sensitive details.
use crate::api::error::{ApiError, api_not_found};
use crate::api::types::{ListRegionsResponse, Region};
use crate::app::AppState;
use axum::Json;
use axum::extract::{Path, State};

#[utoipa::path(
    get,
    path = "/v1/regions",
    tag = "regions",
    responses(
        (status = 200, description = "List regions", body = ListRegionsResponse)
    )
)]
/// List configured regions.
///
/// # What it does
/// Returns the single region configured for this control-plane instance.
///
/// # Why it exists
/// Allows clients to discover region metadata for routing or display.
///
/// # Errors
/// - Does not return errors.
pub(crate) async fn list_regions(State(state): State<AppState>) -> Json<ListRegionsResponse> {
    // Return the configured region as the only entry.
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
        (status = 404, description = "Region not found", body = crate::api::types::ErrorResponse)
    )
)]
/// Fetch region metadata by ID.
///
/// # What it does
/// Returns the configured region when the ID matches.
///
/// # Why it exists
/// Provides a stable per-region lookup for clients.
///
/// # Errors
/// - Returns 404 if the region ID does not match the configured one.
pub(crate) async fn get_region(
    Path(region_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Region>, ApiError> {
    // Return 404 if the requested region does not match the configured one.
    if state.region.region_id != region_id {
        return Err(api_not_found("region not found"));
    }
    // Return the configured region.
    Ok(Json(state.region))
}
