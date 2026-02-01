//! Stream model definitions and patch/change payloads.
//!
//! # Purpose
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
    pub retention: RetentionPolicy,
    pub consistency: ConsistencyLevel,
    pub delivery: DeliveryGuarantee,
    pub durable: bool,
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
