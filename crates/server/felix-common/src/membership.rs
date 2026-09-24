//! The broker-to-control-plane membership shapes, defined once.
//!
//! These cross a process boundary as JSON, so a field renamed on one side and
//! not the other is a silent mismatch — the receiver sees a missing field and
//! the sender never hears about it. Sharing the types puts that back in the
//! compiler's hands.
//!
//! The `openapi` feature adds `ToSchema` so the control plane can keep
//! publishing them, without making every consumer take a utoipa dependency.

use serde::{Deserialize, Serialize};

#[cfg(feature = "openapi")]
use utoipa::ToSchema;

/// What the cluster currently thinks of a node.
///
/// `Unknown` is not a state the control plane sends. It is what a broker reads
/// when a newer control plane sends one this build has never heard of — and
/// treating that as "not serving" is deliberate: a broker that cannot
/// understand its own status must not assume it may keep leading shards. That
/// matches what string comparison already did, so nothing changes behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum NodeLifecycle {
    /// Registered and heartbeating. Eligible for new shard placement.
    #[default]
    Live,
    /// Still serving, but should not receive new placement.
    Draining,
    /// Heartbeat expired. The process may or may not still be running.
    Down,
    /// Deregistered through a graceful shutdown.
    Left,
    #[serde(other)]
    Unknown,
}

impl NodeLifecycle {
    /// Whether the cluster will place shards on a node in this state, and so
    /// whether it may keep serving the ones it has.
    pub fn is_placeable(self) -> bool {
        matches!(self, Self::Live | Self::Draining)
    }
}

impl std::fmt::Display for NodeLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Live => "live",
            Self::Draining => "draining",
            Self::Down => "down",
            Self::Left => "left",
            Self::Unknown => "unknown",
        })
    }
}

/// Whether a shard belongs to a stream or a cache.
///
/// Absent means stream, which is what every broker predating cache placement
/// reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum ShardKind {
    #[default]
    Stream,
    Cache,
}

/// How far one replica has got with a shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
pub struct ReplicaOffset {
    pub node_id: String,
    pub durable_offset: u64,
}

/// One shard's replica positions, as its leader last saw them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
pub struct ShardReplicaStatus {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub shard: u32,
    #[serde(default)]
    pub kind: ShardKind,
    /// The assignment generation the reporting broker held.
    pub generation: u64,
    /// Replicas within the catch-up bound.
    pub caught_up: Vec<String>,
    #[serde(default)]
    pub replica_offsets: Vec<ReplicaOffset>,
    /// The leader has stopped serving at `generation` and `caught_up` was
    /// measured against its final tail. Omitted when false.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub drained: bool,
    /// The leader's own tail when it reported, which `replica_offsets` are
    /// measured against. Omitted by brokers that predate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_offset: Option<u64>,
}

/// What a leader tells the control plane about the shards it leads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
pub struct ReplicaStatusRequest {
    /// The reporting process's own incarnation, from its last registration.
    pub incarnation: u64,
    pub shards: Vec<ShardReplicaStatus>,
}

#[cfg(test)]
mod tests;
