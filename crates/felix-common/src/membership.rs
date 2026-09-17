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
mod tests {
    use super::*;

    #[test]
    fn a_lifecycle_this_build_does_not_know_reads_as_not_placeable() {
        // A newer control plane adding a state must not leave an older broker
        // guessing that it may keep serving.
        let parsed: NodeLifecycle = serde_json::from_str("\"quiescing\"").expect("parse");
        assert_eq!(parsed, NodeLifecycle::Unknown);
        assert!(!parsed.is_placeable());
    }

    #[test]
    fn the_known_states_round_trip_as_the_wire_spells_them() {
        for (text, value) in [
            ("\"live\"", NodeLifecycle::Live),
            ("\"draining\"", NodeLifecycle::Draining),
            ("\"down\"", NodeLifecycle::Down),
            ("\"left\"", NodeLifecycle::Left),
        ] {
            assert_eq!(
                serde_json::from_str::<NodeLifecycle>(text).expect("parse"),
                value,
            );
            assert_eq!(serde_json::to_string(&value).expect("write"), text);
        }
    }

    #[test]
    fn only_live_and_draining_are_placeable() {
        assert!(NodeLifecycle::Live.is_placeable());
        assert!(NodeLifecycle::Draining.is_placeable());
        assert!(!NodeLifecycle::Down.is_placeable());
        assert!(!NodeLifecycle::Left.is_placeable());
    }

    #[test]
    fn a_report_round_trips() {
        let report = ReplicaStatusRequest {
            incarnation: 3,
            shards: vec![ShardReplicaStatus {
                tenant_id: "t1".into(),
                namespace: "ns".into(),
                stream: "orders".into(),
                shard: 0,
                kind: ShardKind::Cache,
                generation: 7,
                caught_up: vec!["broker-b".into()],
                replica_offsets: vec![ReplicaOffset {
                    node_id: "broker-b".into(),
                    durable_offset: 42,
                }],
            }],
        };
        let json = serde_json::to_string(&report).expect("write");
        assert_eq!(
            serde_json::from_str::<ReplicaStatusRequest>(&json).expect("read"),
            report,
        );
    }

    /// A report from a broker predating cache placement omits `kind`, and must
    /// still be read as a stream rather than rejected.
    #[test]
    fn an_older_report_without_a_kind_reads_as_a_stream() {
        let older = r#"{"incarnation":0,"shards":[{"tenant_id":"t1","namespace":"ns",
            "stream":"orders","shard":0,"generation":1,"caught_up":[]}]}"#;
        let parsed: ReplicaStatusRequest = serde_json::from_str(older).expect("parse");
        assert_eq!(parsed.shards[0].kind, ShardKind::Stream);
        assert!(parsed.shards[0].replica_offsets.is_empty());
    }
}
