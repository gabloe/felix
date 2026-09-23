//! Stream model definitions and patch/change payloads.
//!
//! Defines stream identifiers, configuration fields, and change-log payloads
//! used by the control-plane store and API handlers.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Stream {
    pub tenant_id: String,
    pub namespace: String,
    pub stream: String,
    pub kind: StreamKind,
    pub shards: u32,
    /// How many brokers hold a copy of each shard, leader included.
    ///
    /// `1` is leader-only and is the default, so a stream created before this
    /// existed — or by a caller that does not set it — behaves exactly as it did.
    /// A `Quorum` stream needs at least 3 for a majority to mean anything.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub consistency: ConsistencyLevel,
    pub delivery: DeliveryGuarantee,
    pub durable: bool,
}

/// Leader-only. The value a stream has unless it asks for more, and the value
/// every stream written before replication existed reads back as.
pub(crate) fn default_replication_factor() -> u32 {
    1
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamChange {
    pub seq: u64,
    pub op: StreamChangeOp,
    pub key: StreamKey,
    pub stream: Option<Stream>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum StreamChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct StreamPatchRequest {
    pub retention: Option<RetentionPolicy>,
    pub consistency: Option<ConsistencyLevel>,
    pub delivery: Option<DeliveryGuarantee>,
    pub durable: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RetentionPolicy {
    pub max_age_seconds: Option<u64>,
    pub max_size_bytes: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum StreamKind {
    Stream,
    Queue,
    Cache,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum ConsistencyLevel {
    Leader,
    Quorum,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
}

#[cfg(test)]
mod consistency_wire_tests {
    use super::*;

    /// The names these serialize under are what a broker parses. Pinned here as
    /// well as in the broker, because the two are compiled separately and a
    /// rename on this side would otherwise be found by a cluster rather than by
    /// a test.
    #[test]
    fn the_levels_serialize_under_the_names_brokers_read() {
        assert_eq!(
            serde_json::to_string(&ConsistencyLevel::Leader).expect("serialize"),
            "\"Leader\"",
        );
        assert_eq!(
            serde_json::to_string(&ConsistencyLevel::Quorum).expect("serialize"),
            "\"Quorum\"",
        );
    }

    /// A stream serializes its level as a plain field, which is what lets a
    /// broker read it without understanding the rest of the model.
    #[test]
    fn a_stream_carries_its_level_as_a_field() {
        let stream = Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 3,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Quorum,
            delivery: DeliveryGuarantee::AtLeastOnce,
            durable: true,
        };
        let json: serde_json::Value = serde_json::to_value(&stream).expect("serialize");
        assert_eq!(json["consistency"], "Quorum");
    }
}
