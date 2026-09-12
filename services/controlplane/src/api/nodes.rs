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
//! - Every write requires `node.manage` over the node being changed. A broker's
//!   credential is scoped to `node:{its own id}`, so it cannot register, drain,
//!   deregister, or report health for another broker; an operator holding
//!   `cluster:*` can manage the whole fleet.
//! - Reads require `node.view:cluster:*`. The listing exposes advertised
//!   internal addresses, which is the cluster's network layout.
use crate::api::error::{
    ApiError, api_conflict, api_forbidden, api_internal, api_not_found, api_unauthorized,
};
use crate::api::types::{
    NodeHeartbeatRequest, NodeHeartbeatResponse, NodeListResponse, NodePlacement,
    NodeRegistrationRequest, NodeRegistrationResponse, NodeView, ReplicaStatusRequest,
    ShardAssignmentChangesResponse, ShardAssignmentListResponse, ShardAssignmentSnapshotResponse,
};
use crate::app::AppState;
use crate::auth::felix_token::verify_token;
use crate::auth::rbac::authorize::{
    ACTION_NODE_MANAGE, ACTION_NODE_VIEW, ParsedObject, object_within_scope, parse_permission,
};
use crate::model::{Node, NodeLifecycle, NodeSpec, NodeStatus};
use crate::store::StoreError;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use std::collections::HashMap;

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
/// Authorised exactly as a heartbeat is: a broker may speak for itself and no
/// one else. A broker that could report on another's behalf could nominate
/// itself for promotion.
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
    // The control plane's clock, deliberately, exactly as for a heartbeat:
    // letting a caller supply the time would let it keep a stale report alive.
    let now = now_millis();
    let _ = request.incarnation;

    for shard in request.shards {
        state.replica_positions.record(
            crate::model::ShardKey {
                tenant_id: shard.tenant_id,
                namespace: shard.namespace,
                stream: shard.stream,
                shard: shard.shard,
                kind: shard.kind,
            },
            shard.generation,
            shard.caught_up.into_iter().collect(),
            shard
                .replica_offsets
                .into_iter()
                .map(|replica| (replica.node_id, replica.durable_offset))
                .collect(),
            now,
        );
    }
    Ok(axum::http::StatusCode::NO_CONTENT)
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
    headers: HeaderMap,
    Json(request): Json<NodeRegistrationRequest>,
) -> Result<Json<NodeRegistrationResponse>, ApiError> {
    // The identity being claimed comes from the body, so that is what the
    // token has to be authorised for. A broker cannot register as someone else
    // by asking to.
    require_node_manage(&state, &headers, &request.node_id).await?;
    let now = now_millis();
    let node = Node {
        node_id: request.node_id,
        spec: NodeSpec {
            advertise_addr: request.advertise_addr,
            client_addr: request.client_addr,
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
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
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
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<Node>, ApiError> {
    require_node_manage(&state, &headers, &node_id).await?;
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

/// Filters accepted by the node listing.
///
/// Every filter is an intersection, and an absent filter matches everything.
#[derive(Debug, Default)]
struct NodeFilters {
    lifecycle: Option<String>,
    region: Option<String>,
    /// `key=value` pairs a node must carry all of.
    labels: Vec<(String, String)>,
}

impl NodeFilters {
    /// `?lifecycle=live&region=us-west-2&label=rack%3Da1&label=tier%3Dhot`
    ///
    /// Repeating `label` intersects rather than replaces, which is what makes
    /// "the hot racks in this region" expressible.
    fn from_query(query: &HashMap<String, String>, raw: &str) -> Self {
        let labels = raw
            .split('&')
            .filter_map(|pair| pair.strip_prefix("label="))
            .filter_map(|value| {
                let decoded = percent_decode(value);
                decoded
                    .split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect();
        Self {
            lifecycle: query.get("lifecycle").cloned(),
            region: query.get("region").cloned(),
            labels,
        }
    }

    fn matches(&self, node: &crate::model::Node) -> bool {
        if let Some(lifecycle) = &self.lifecycle
            && !lifecycle_matches(node.status.lifecycle, lifecycle)
        {
            return false;
        }
        if let Some(region) = &self.region
            && &node.spec.region != region
        {
            return false;
        }
        self.labels
            .iter()
            .all(|(key, value)| node.spec.labels.get(key) == Some(value))
    }
}

/// Minimal `%XX` decoding for label values, which routinely contain `=` and `/`.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&value[i + 1..i + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                }
                Err(_) => {
                    out.push(bytes[i]);
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

fn lifecycle_matches(lifecycle: NodeLifecycle, wanted: &str) -> bool {
    let name = match lifecycle {
        NodeLifecycle::Live => "live",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Down => "down",
        NodeLifecycle::Left => "left",
    };
    name.eq_ignore_ascii_case(wanted)
}

/// Explain this node's placement standing at `now`.
fn placement_for(node: &Node, now_millis: u64, expiry_timeout_ms: u64) -> NodePlacement {
    // Saturating: a heartbeat recorded a moment ahead of this read (two control
    // plane instances, slightly different clocks) is an age of zero, not a
    // wrapped enormous one.
    let heartbeat_age_ms = now_millis.saturating_sub(node.status.last_heartbeat_at_millis);
    let mut reasons = Vec::new();

    match node.status.lifecycle {
        NodeLifecycle::Live => {}
        NodeLifecycle::Draining => {
            reasons.push("node is draining and takes no new placement".into())
        }
        NodeLifecycle::Down => reasons.push("node missed its heartbeat window".into()),
        NodeLifecycle::Left => reasons.push("node deregistered and left the cluster".into()),
    }

    // Reported separately from the lifecycle: between a heartbeat lapsing and
    // the sweep noticing, a node still reads `live` while already being past
    // its window, and that gap is exactly what an operator is trying to see.
    if heartbeat_age_ms > expiry_timeout_ms
        && matches!(
            node.status.lifecycle,
            NodeLifecycle::Live | NodeLifecycle::Draining
        )
    {
        reasons.push(format!(
            "last heartbeat was {heartbeat_age_ms}ms ago, past the {expiry_timeout_ms}ms timeout; expiry has not run yet"
        ));
    }

    if node.spec.capacity.max_shards == Some(0) {
        reasons.push("capacity hint max_shards is zero".into());
    }
    if node.spec.capacity.weight == 0 {
        reasons.push("capacity hint weight is zero, so placement never selects it".into());
    }

    NodePlacement {
        eligible: reasons.is_empty(),
        reasons,
        heartbeat_age_ms,
    }
}

#[utoipa::path(
    get,
    path = "/v1/nodes",
    tag = "nodes",
    params(
        ("lifecycle" = Option<String>, Query, description = "live, draining, down, or left"),
        ("region" = Option<String>, Query, description = "Exact region match"),
        ("label" = Option<String>, Query, description = "key=value; repeat to require several")
    ),
    responses((status = 200, description = "List registered nodes", body = NodeListResponse))
)]
/// List every registered broker and why each is, or is not, placeable.
///
/// Unpaginated, like the other listings in this API: a cluster has brokers in
/// the tens, and a cursor no caller needs is a cursor every caller has to handle.
///
/// # Errors
/// - 500 when the store cannot be read.
pub(crate) async fn list_nodes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    raw_query: axum::extract::RawQuery,
) -> Result<Json<NodeListResponse>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let filters = NodeFilters::from_query(&query, raw_query.0.as_deref().unwrap_or_default());
    let now = now_millis();
    let expiry = state.node_liveness.expiry_timeout_ms;

    let items = state
        .store
        .list_nodes()
        .await
        .map_err(|ref err| api_internal("failed to list nodes", err))?
        .into_iter()
        .filter(|node| filters.matches(node))
        .map(|node| NodeView {
            placement: placement_for(&node, now, expiry),
            node,
        })
        .collect();

    Ok(Json(NodeListResponse { items }))
}

#[utoipa::path(
    get,
    path = "/v1/nodes/{node_id}",
    tag = "nodes",
    params(("node_id" = String, Path, description = "Broker node identifier")),
    responses(
        (status = 200, description = "Node detail", body = NodeView),
        (status = 404, description = "Node is not registered", body = crate::api::types::ErrorResponse)
    )
)]
/// Fetch one broker, with the same placement explanation the listing gives.
///
/// # Errors
/// - 404 when the node is not registered.
pub(crate) async fn get_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Result<Json<NodeView>, ApiError> {
    require_cluster_node_view(&state, &headers).await?;
    let node = state
        .store
        .get_node(&node_id)
        .await
        .map_err(|err| match err {
            StoreError::NotFound(_) => api_not_found("node is not registered"),
            ref other => api_internal("get node", other),
        })?;

    let placement = placement_for(&node, now_millis(), state.node_liveness.expiry_timeout_ms);
    Ok(Json(NodeView { node, placement }))
}

/// Require cluster-scoped `node.view` from a Felix bearer token.
///
/// The tenant comes from the token's own `tid` claim rather than the path,
/// because `/v1/nodes` is not a tenant resource. That claim only selects which
/// tenant's signing keys to check against — the signature is what grants trust,
/// exactly as `kid` selects a key without conferring one.
///
/// Naming a tenant confers nothing here: the permission this requires is
/// `node.view:cluster:*`, and no tenant scope contains a cluster object, so a
/// tenant admin cannot write that rule for themselves.
async fn require_cluster_node_view(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let (tenant_id, claims) = verified_claims(state, headers).await?;

    let allowed = claims.perms.iter().any(|perm| {
        // Parsed against the token's own tenant; a cluster object ignores it.
        // Unparsable entries are skipped rather than trusted, so a malformed
        // permission can never widen access.
        matches!(
            parse_permission(perm, &tenant_id),
            Ok(parsed) if parsed.action == ACTION_NODE_VIEW && parsed.object == ParsedObject::Cluster
        )
    });

    if allowed {
        Ok(())
    } else {
        Err(api_forbidden("missing node.view:cluster:* permission"))
    }
}

/// Require permission to change one node's membership.
///
/// The node id comes from the request -- a path segment, or the body on
/// registration -- and the token has to carry `node.manage` over a scope that
/// contains it. A broker's credential is scoped to `node:{its own id}`, so
/// presenting it for another node fails here rather than being trusted because
/// the request said so. An operator holding `cluster:*` covers every node.
///
/// This is what closes the hole where any caller that could reach the control
/// plane could register, drain, or deregister any broker, or keep a dead one
/// looking alive.
async fn require_node_manage(
    state: &AppState,
    headers: &HeaderMap,
    node_id: &str,
) -> Result<(), ApiError> {
    let (tenant_id, claims) = verified_claims(state, headers).await?;
    let target = ParsedObject::Node {
        node_id: node_id.to_string(),
    };

    let allowed = claims.perms.iter().any(|perm| {
        // Unparsable entries are skipped rather than trusted, so a malformed
        // permission can never widen access.
        matches!(
            parse_permission(perm, &tenant_id),
            Ok(parsed) if parsed.action == ACTION_NODE_MANAGE
                && object_within_scope(&parsed.object, &target)
        )
    });

    if allowed {
        Ok(())
    } else {
        Err(api_forbidden(&format!(
            "missing node.manage on node:{node_id} or cluster:*"
        )))
    }
}

/// Verify a bearer token and return the tenant whose keys signed it.
///
/// Shared by the read and write guards so there is one verification path, not
/// two that can drift.
async fn verified_claims(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, crate::auth::felix_token::FelixClaims), ApiError> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| api_unauthorized("missing bearer token"))?;

    let tenant_id = unverified_tenant(bearer)?;
    let keys = state
        .store
        .get_tenant_signing_keys(&tenant_id)
        .await
        .map_err(|ref err| api_internal("failed to load signing keys", err))?;
    let claims = verify_token(&keys, &tenant_id, bearer, 5)
        .map_err(|_| api_unauthorized("invalid token"))?;
    Ok((tenant_id, claims))
}

/// Read `tid` from an unverified token, only to choose a verification key.
fn unverified_tenant(token: &str) -> Result<String, ApiError> {
    use base64::Engine as _;

    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| api_unauthorized("malformed token"))?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| api_unauthorized("malformed token"))?;
    let claims: serde_json::Value =
        serde_json::from_slice(&decoded).map_err(|_| api_unauthorized("malformed token"))?;

    claims
        .get("tid")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| api_unauthorized("token has no tenant claim"))
}

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
