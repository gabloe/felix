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
use crate::api::types::{
    NodeHeartbeatRequest, NodeHeartbeatResponse, NodeRegistrationRequest, NodeRegistrationResponse,
};
use crate::app::AppState;
use crate::model::{Node, NodeLifecycle, NodeSpec, NodeStatus};
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

#[utoipa::path(
    post,
    path = "/v1/nodes",
    tag = "nodes",
    request_body = NodeRegistrationRequest,
    responses(
        (status = 200, description = "Node registered", body = NodeRegistrationResponse),
        (status = 409, description = "Identity or address rejected", body = crate::api::types::ErrorResponse)
    )
)]
/// Claim a broker identity.
///
/// Idempotent per `node_id`: a broker that restarts re-registers under the same
/// identity, keeping its original registration time and taking the next
/// incarnation. That is what lets a restart be told apart from a new node.
///
/// # Errors
/// - 409 when the identity or advertised address is invalid, or the address
///   already belongs to another node.
pub(crate) async fn register_node(
    State(state): State<AppState>,
    Json(request): Json<NodeRegistrationRequest>,
) -> Result<Json<NodeRegistrationResponse>, ApiError> {
    let now = now_millis();
    let node = Node {
        node_id: request.node_id,
        spec: NodeSpec {
            advertise_addr: request.advertise_addr,
            region: request.region,
            labels: request.labels,
            capacity: request.capacity,
        },
        // The control plane's to set, not the caller's. `register_node`
        // preserves the original `registered_at_millis` and owns `incarnation`.
        status: NodeStatus {
            lifecycle: NodeLifecycle::Live,
            last_heartbeat_at_millis: now,
            registered_at_millis: now,
            incarnation: 0,
        },
    };

    let node = state
        .store
        .register_node(node)
        .await
        .map_err(|err| match err {
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("register node", other),
        })?;

    Ok(Json(NodeRegistrationResponse {
        node,
        heartbeat_interval_ms: state.node_liveness.heartbeat_interval_ms,
        expiry_timeout_ms: state.node_liveness.expiry_timeout_ms,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/drain",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node is draining", body = crate::model::Node),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse),
        (status = 409, description = "Node cannot drain from its current lifecycle", body = crate::api::types::ErrorResponse)
    )
)]
/// Stop new placement without stopping service.
///
/// Called by a broker at the start of its own shutdown, so nothing new is
/// assigned to it while it finishes what it has.
///
/// # Errors
/// - 404 when the node is not registered.
/// - 409 when the node is not currently serving, since there is nothing to drain.
pub(crate) async fn drain_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    set_lifecycle(&state, &node_id, NodeLifecycle::Draining).await
}

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/deregister",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node has left", body = crate::model::Node),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse)
    )
)]
/// Leave the cluster on purpose.
///
/// This is what distinguishes a graceful shutdown from a crash: a node that
/// deregisters is `left`, while one that simply stops is found `down` by expiry.
/// The record is kept either way, so the identity and its incarnation survive
/// for the next boot.
///
/// # Errors
/// - 404 when the node is not registered.
pub(crate) async fn deregister_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    set_lifecycle(&state, &node_id, NodeLifecycle::Left).await
}

/// Drive a lifecycle move, treating "already there" as success.
///
/// A retried drain or deregistration must not fail: a broker that shuts down
/// twice for the same reason is not an error condition.
async fn set_lifecycle(
    state: &AppState,
    node_id: &str,
    lifecycle: NodeLifecycle,
) -> Result<Json<Node>, ApiError> {
    let moved = state
        .store
        .set_node_lifecycle(node_id, lifecycle)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            StoreError::Conflict(ref message) => api_conflict("conflict", message),
            ref other => api_internal("set node lifecycle", other),
        })?;

    match moved {
        Some(node) => Ok(Json(node)),
        None => state
            .store
            .get_node(node_id)
            .await
            .map(Json)
            .map_err(|err| match err {
                StoreError::NotFound(_) => api_not_found("node is not registered"),
                ref other => api_internal("get node", other),
            }),
    }
}
