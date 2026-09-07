//! Broker membership API handlers.
//!
//! # Purpose and responsibility
//! Accepts health reports from brokers and keeps their observed liveness
//! current.
//!
//! # Where it fits in Felix
//! A broker registers on boot and then reports health on an interval. The
//! control plane records the last report; a sweep marks silent nodes down.
//!
//! # Key invariants and assumptions
//! - A heartbeat records liveness. It never revives a node the cluster already
//!   marked down, and never resurrects a superseded incarnation.
//! - The clock is the control plane's, not the caller's, so a broker cannot
//!   claim a future heartbeat and outlive its timeout.
//!
//! # Security considerations
//! - **This endpoint is not yet authenticated.** Any caller that can reach it
//!   can report health for any node_id. Authenticating broker identity is
//!   tracked in #126; until then this is only safe on a trusted network.
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{NodeHeartbeatRequest, NodeHeartbeatResponse};
use crate::app::AppState;
use crate::store::StoreError;
use axum::Json;
use axum::extract::{Path, State};

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/heartbeat",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = NodeHeartbeatRequest,
    responses(
        (status = 200, description = "Heartbeat recorded", body = NodeHeartbeatResponse),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Heartbeat is for a superseded incarnation", body = crate::api::types::ErrorResponse)
    )
)]
/// Record that a broker is alive.
///
/// Returns the node's current lifecycle and how soon the next heartbeat is
/// expected. A broker that finds itself `down` here has been expired and must
/// register again before it is eligible for placement.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the reported incarnation is older than the recorded one.
pub(crate) async fn report_health(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(request): Json<NodeHeartbeatRequest>,
) -> Result<Json<NodeHeartbeatResponse>, ApiError> {
    // The control plane's clock, deliberately: expiry is judged against it, so
    // letting a caller supply the time would let it postpone its own timeout.
    let now = now_millis();

    let node = state
        .store
        .record_node_heartbeat(&node_id, request.incarnation, now)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("record node heartbeat", other),
        })?;

    Ok(Json(NodeHeartbeatResponse {
        node_id: node.node_id,
        lifecycle: node.status.lifecycle,
        heartbeat_interval_ms: state.node_liveness.heartbeat_interval_ms,
        expiry_timeout_ms: state.node_liveness.expiry_timeout_ms,
    }))
}

/// Wall-clock milliseconds since the Unix epoch.
///
/// Clamped at zero so a clock behind the epoch cannot panic the handler.
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
