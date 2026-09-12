//! Shard assignment: which broker leads a stream shard, and who replicates it.
//!
//! One assignment per `(stream, shard)`, and it is the authority on ownership.
//! Placement writes it; brokers read it and take or release shards to match.
//!
//! `generation` is what makes that safe. A broker reporting on an assignment it
//! read some time ago carries the generation it saw, and the control plane
//! rejects it if placement has moved on. Without that, a slow broker's status
//! could resurrect an ownership decision that was already replaced.
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::model::node::validate_node_id;
use crate::model::{NodeValidationError, StreamKey};

/// Why a shard assignment was rejected.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShardValidationError {
    #[error("shard {shard} is out of bounds for a stream with {shards} shards")]
    ShardOutOfBounds { shard: u32, shards: u32 },
    #[error("leader node is invalid: {0}")]
    InvalidLeader(NodeValidationError),
    #[error("replica node is invalid: {0}")]
    InvalidReplica(NodeValidationError),
    #[error("node {0:?} is both the leader and a replica")]
    LeaderIsAlsoReplica(String),
    #[error("node {0:?} appears twice in the replica set")]
    DuplicateReplica(String),
    #[error("cannot move a shard from {from:?} to {to:?}")]
    UnsupportedTransition { from: ShardState, to: ShardState },
}

/// Where an assignment is between being decided and being served.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ShardState {
    /// Placement has decided, and the leader has not confirmed it is serving.
    Assigning,
    /// The leader is serving this shard.
    Active,
    /// Ownership is moving elsewhere. The leader still serves until it stops.
    Draining,
}

impl ShardState {
    /// Whether `self` may move to `next`.
    ///
    /// Draining is only meaningful for a shard someone is actually serving, and
    /// a drained shard does not return to the same leader — placement writes a
    /// new assignment, at a new generation, instead.
    pub fn can_transition_to(self, next: ShardState) -> bool {
        use ShardState::*;
        matches!(
            (self, next),
            (Assigning, Assigning | Active) | (Active, Active | Draining) | (Draining, Draining)
        )
    }
}

/// Whether a shard belongs to a stream or to a cache.
///
/// Both are placed by the same algorithm over the same kind of log — that is
/// the point of "one core log, many semantics". They are *not* the same
/// namespace: a cache and a stream may share a name within one namespace, and
/// when they do their shards are unrelated. Anything keyed by `ShardKey` must
/// therefore carry the kind, or the two silently collide on ownership.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    ToSchema,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "camelCase")]
pub enum ShardKind {
    /// A stream's shard. The default, so an assignment persisted before caches
    /// were placed reads back as exactly what it was.
    #[default]
    Stream,
    /// A cache's shard.
    Cache,
}

impl ShardKind {
    /// The name this kind is stored and logged under.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Cache => "cache",
        }
    }
}

impl std::fmt::Display for ShardKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Identifies one shard of one stream or cache.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub tenant_id: String,
    pub namespace: String,
    /// The stream or cache name, disambiguated by `kind`.
    pub stream: String,
    pub shard: u32,
    /// Absent on the wire means `Stream`, which is what every assignment
    /// written before caches were placed is.
    #[serde(default)]
    pub kind: ShardKind,
}

impl ShardKey {
    /// The stream this shard belongs to, or `None` if it belongs to a cache.
    pub fn stream_key(&self) -> Option<StreamKey> {
        (self.kind == ShardKind::Stream).then(|| StreamKey {
            tenant_id: self.tenant_id.clone(),
            namespace: self.namespace.clone(),
            stream: self.stream.clone(),
        })
    }
}

/// Who owns a shard right now.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardAssignment {
    #[serde(flatten)]
    pub key: ShardKey,
    /// The node that serves reads and writes for this shard.
    pub leader: String,
    /// Nodes holding a copy. Never contains the leader, never repeats.
    ///
    /// Empty when the replication factor is 1, which is the default.
    #[serde(default)]
    pub replicas: Vec<String>,
    /// Increments on every change to this assignment.
    ///
    /// A broker reports status against the generation it read; the control plane
    /// rejects anything older, so a slow broker cannot resurrect an ownership
    /// decision that placement has already replaced.
    pub generation: u64,
    pub state: ShardState,
}

impl ShardAssignment {
    /// Check node identities and replica-set shape.
    ///
    /// Does not check the shard bound, which needs the stream's shard count, or
    /// that the nodes exist, which needs the catalog. Both are the store's.
    pub fn validate(&self) -> Result<(), ShardValidationError> {
        validate_node_id(&self.leader).map_err(ShardValidationError::InvalidLeader)?;

        let mut seen = std::collections::BTreeSet::new();
        for replica in &self.replicas {
            validate_node_id(replica).map_err(ShardValidationError::InvalidReplica)?;
            if replica == &self.leader {
                return Err(ShardValidationError::LeaderIsAlsoReplica(replica.clone()));
            }
            if !seen.insert(replica) {
                return Err(ShardValidationError::DuplicateReplica(replica.clone()));
            }
        }
        Ok(())
    }

    /// Check this assignment's shard number against the stream that owns it.
    pub fn validate_within(&self, shards: u32) -> Result<(), ShardValidationError> {
        if self.key.shard >= shards {
            return Err(ShardValidationError::ShardOutOfBounds {
                shard: self.key.shard,
                shards,
            });
        }
        self.validate()
    }

    /// Every node this assignment refers to, leader first.
    pub fn nodes(&self) -> impl Iterator<Item = &String> {
        std::iter::once(&self.leader).chain(self.replicas.iter())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct ShardAssignmentChange {
    pub seq: u64,
    pub op: ShardAssignmentChangeOp,
    pub key: ShardKey,
    pub assignment: Option<ShardAssignment>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ShardAssignmentChangeOp {
    Assigned,
    Updated,
    Unassigned,
}

#[cfg(test)]
#[path = "shard_tests.rs"]
mod tests;
