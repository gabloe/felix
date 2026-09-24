//! What brokers report about themselves: liveness, and their replicas'
//! positions.
use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use super::require_node_manage;
use crate::api::AppState;
use crate::api::error::{ApiError, api_conflict, api_internal, api_not_found};
use crate::api::types::{NodeHeartbeatRequest, NodeHeartbeatResponse, ReplicaStatusRequest};
use crate::store::StoreError;

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
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<NodeHeartbeatRequest>,
) -> Result<Json<NodeHeartbeatResponse>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    // The *store's* clock, not this instance's. Expiry is judged against the
    // same one, and with several instances over one database those are
    // different processes — see `ControlPlaneStore::now_millis`. Still not the
    // caller's, which would let a broker postpone its own timeout.
    let now = state
        .store
        .now_millis()
        .await
        .map_err(|ref err| api_internal("read the store clock", err))?;

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

#[utoipa::path(
    post,
    path = "/v1/nodes/{node_id}/replica-status",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    request_body = ReplicaStatusRequest,
    responses(
        (status = 204, description = "Report recorded"),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse)
    )
)]
/// Record which replicas a leader believes hold each of its shards.
///
/// Promotion is gated on this. Without it a lost leader cannot be replaced at
/// all, and with a stale answer it can be replaced by a broker holding less
/// than it claims — so reports expire, and the expiry is derived from the
/// liveness settings rather than trusted from the caller.
///
/// Two checks, not one. A broker may speak for itself and no one else, as for
/// a heartbeat — and it may only report on shards it currently leads. The
/// second matters just as much: speaking for yourself about someone else's
/// shard is still nominating yourself for promotion.
///
/// # Errors
/// - 404 when the node is not registered.
pub(crate) async fn report_replica_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
    Json(request): Json<ReplicaStatusRequest>,
) -> Result<axum::http::StatusCode, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
    // The store's clock, like a heartbeat, and for the same reason: the
    // report is judged by the placement pass, which may run on a different
    // instance, so the two sides need a clock they share. Not the caller's,
    // which would let a broker keep its own report alive.
    let now = state
        .store
        .now_millis()
        .await
        .map_err(|ref err| api_internal("read the store clock", err))?;
    // Not enforced: brokers still send 0 here, because the driver that reports
    // is spawned before registration returns an incarnation. The leadership
    // check below is the stronger one anyway — it bounds *which* shards a
    // broker can speak about, where the incarnation would only catch a stale
    // report from the same broker's previous life.
    let _ = request.incarnation;

    for shard in request.shards {
        let key = crate::model::ShardKey {
            tenant_id: shard.tenant_id,
            namespace: shard.namespace,
            stream: shard.stream,
            shard: shard.shard,
            kind: shard.kind.into(),
        };

        // Being authorised to speak for yourself is not the same as leading
        // this shard. Without this, any node credential can list itself as
        // caught up for any shard and nominate itself for promotion.
        let assignment = match state.store.get_shard_assignment(&key).await {
            Ok(assignment) => assignment,
            // An unplaced shard has no leader, so nobody can report on it.
            Err(StoreError::NotFound(_)) => continue,
            Err(ref other) => return Err(api_internal("read shard assignment", other)),
        };
        if assignment.leader != node_id {
            tracing::warn!(
                node_id = %node_id,
                leader = %assignment.leader,
                stream = %key.stream,
                shard = key.shard,
                "a broker reported replica positions for a shard it does not lead",
            );
            metrics::counter!("felix_replica_status_rejected_total", "reason" => "not_leader")
                .increment(1);
            continue;
        }
        // A generation past the assignment's cannot be one the broker read, and
        // accepting it would wedge the shard: `record` drops everything older,
        // so one report claiming u64::MAX blocks every real one after it.
        if shard.generation > assignment.generation {
            tracing::warn!(
                node_id = %node_id,
                reported = shard.generation,
                assigned = assignment.generation,
                stream = %key.stream,
                shard = key.shard,
                "a broker reported a generation ahead of the assignment",
            );
            metrics::counter!("felix_replica_status_rejected_total", "reason" => "future_generation")
                .increment(1);
            continue;
        }

        let advances = advances_move(
            &assignment,
            shard.generation,
            shard.drained,
            &shard.caught_up,
        );
        match state
            .store
            .record_replica_report(crate::model::ReplicaReport {
                key,
                generation: shard.generation,
                caught_up: shard.caught_up.into_iter().collect(),
                drained: shard.drained,
                offsets: shard
                    .replica_offsets
                    .into_iter()
                    .map(|replica| (replica.node_id, replica.durable_offset))
                    .collect(),
                reported_at_millis: now,
                leader_offset: shard.leader_offset,
            })
            .await
        {
            // The next step of a move waits on exactly this report, so
            // placement runs now rather than at its next tick.
            Ok(()) if advances => state.placement_wakes.request_pass(),
            Ok(()) => {}
            // The assignment went between the read above and the write: the
            // shard is nobody's to report on any more.
            Err(StoreError::NotFound(_)) => continue,
            Err(ref err) => return Err(api_internal("record replica report", err)),
        }
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// Whether a report is the one a move in progress is waiting for: a caught-up
/// successor lets it fence, a drained leader lets it cut over.
///
/// Only a hint for when placement runs. The pass judges the report itself,
/// so a wrong answer here costs a pass or some latency, never a decision.
fn advances_move(
    assignment: &crate::model::ShardAssignment,
    generation: u64,
    drained: bool,
    caught_up: &[String],
) -> bool {
    if generation != assignment.generation {
        return false;
    }
    match (&assignment.state, &assignment.successor) {
        (crate::model::ShardState::Draining, _) => drained,
        (_, Some(successor)) => caught_up.contains(successor),
        (_, None) => false,
    }
}

#[cfg(test)]
mod tests;
