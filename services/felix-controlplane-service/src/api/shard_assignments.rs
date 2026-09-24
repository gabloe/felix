//! Which broker leads each shard, as operators and brokers read it.
use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::HeaderMap;

use crate::api::AppState;
use crate::api::error::{ApiError, api_internal};
use crate::api::nodes::require_cluster_node_view;
use crate::api::types::{
    ShardAssignmentChangesResponse, ShardAssignmentListResponse, ShardAssignmentSnapshotResponse,
};

#[utoipa::path(
    get,
    path = "/v1/shard-assignments",
    tag = "nodes",
    params(("leader" = Option<String>, Query, description = "Only shards this node leads")),
    responses((status = 200, description = "List shard assignments", body = ShardAssignmentListResponse))
)]
/// List shard ownership.
///
/// Answers the two questions an operator has when placement looks wrong: who
/// owns this shard, and what does this broker own. Requires the same
/// `node.view:cluster:*` as the node listing, because ownership and membership
/// are the same view of the cluster.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn list_shard_assignments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<ShardAssignmentListResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;

    let items = match query.get("leader") {
        Some(leader) => state.store.list_shard_assignments_for_node(leader).await,
        None => state.store.list_shard_assignments().await,
    }
    .map_err(|ref err| api_internal("failed to list shard assignments", err))?;

    Ok(Json(ShardAssignmentListResponse { items }))
}

#[utoipa::path(
    get,
    path = "/v1/shard-assignments/snapshot",
    tag = "nodes",
    responses((status = 200, description = "Full assignment snapshot", body = ShardAssignmentSnapshotResponse))
)]
/// Every current assignment, plus where to start polling.
///
/// A broker applies this and then polls `changes` from `next_seq`. The two
/// together describe every committed change exactly once — the snapshot is read
/// at a consistent point and `next_seq` is the log position at that same point.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn shard_assignment_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ShardAssignmentSnapshotResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let snapshot = state
        .store
        .shard_assignment_snapshot()
        .await
        .map_err(|ref err| api_internal("failed to snapshot shard assignments", err))?;
    Ok(Json(ShardAssignmentSnapshotResponse {
        items: snapshot.items,
        next_seq: snapshot.next_seq,
    }))
}

#[utoipa::path(
    get,
    path = "/v1/shard-assignments/changes",
    tag = "nodes",
    params(("since" = Option<u64>, Query, description = "Last seen sequence")),
    responses((status = 200, description = "Assignment changes", body = ShardAssignmentChangesResponse))
)]
/// Assignment changes at or after `since`.
///
/// A caller that finds the first returned `seq` above its `since`, or an empty
/// page with `next_seq` above it, has fallen outside the retention window and
/// must re-snapshot. `next_seq` below `since` means the sequence was reset —
/// a control plane restarted onto a store that does not persist it.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn shard_assignment_changes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<ShardAssignmentChangesResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let since = query
        .get("since")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let changes = state
        .store
        .shard_assignment_changes(since)
        .await
        .map_err(|ref err| api_internal("failed to load shard assignment changes", err))?;
    Ok(Json(ShardAssignmentChangesResponse {
        items: changes.items,
        next_seq: changes.next_seq,
    }))
}
