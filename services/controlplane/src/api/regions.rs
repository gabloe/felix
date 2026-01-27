//! Region API handlers.
//!
//! # Purpose
//! Exposes configured region metadata for control-plane clients.
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
pub(crate) async fn list_regions(State(state): State<AppState>) -> Json<ListRegionsResponse> {
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
pub(crate) async fn get_region(
    Path(region_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Region>, ApiError> {
    if state.region.region_id != region_id {
        return Err(api_not_found("region not found"));
    }
    Ok(Json(state.region))
}
