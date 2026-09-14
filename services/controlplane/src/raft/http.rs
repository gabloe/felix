//! The server half of the Raft transport: three POST routes handing each
//! RPC to the local node and shipping its whole `Result` back as JSON —
//! the shape [`super::network`] expects on the other side.
//!
//! Mounted on the internal listener, deliberately not a new port: the group
//! is small, elections are rare, and one less listener is one less surface
//! for M8 to secure.
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::types::{Raft, TypeConfig};

pub(super) fn router(raft: Raft) -> Router {
    Router::new()
        .route("/internal/raft/append-entries", post(append_entries))
        .route("/internal/raft/vote", post(vote))
        .route("/internal/raft/install-snapshot", post(install_snapshot))
        .route("/internal/raft/propose", post(propose))
        // Axum's default body limit (2MB) is below openraft's default
        // snapshot chunk (3MB); a snapshot install crossing that line would
        // be refused with a 413 the sender reads as a network fault. Sized
        // to the chunk plus JSON's expansion of binary data.
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(raft)
}

/// A follower's forwarded proposal: opaque command bytes in, the state
/// machine's response bytes out. 503 when this node cannot commit it —
/// including "not the leader any more", which the forwarding side treats as
/// a retryable answer, not a network fault.
async fn propose(State(raft): State<Raft>, body: axum::body::Bytes) -> axum::response::Response {
    use axum::response::IntoResponse;
    match raft.client_write(body.to_vec()).await {
        Ok(response) => (axum::http::StatusCode::OK, response.data).into_response(),
        Err(err) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}

async fn append_entries(
    State(raft): State<Raft>,
    Json(rpc): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<u64>, RaftError<u64>>> {
    Json(raft.append_entries(rpc).await)
}

async fn vote(
    State(raft): State<Raft>,
    Json(rpc): Json<VoteRequest<u64>>,
) -> Json<Result<VoteResponse<u64>, RaftError<u64>>> {
    Json(raft.vote(rpc).await)
}

async fn install_snapshot(
    State(raft): State<Raft>,
    Json(rpc): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>> {
    Json(raft.install_snapshot(rpc).await)
}
