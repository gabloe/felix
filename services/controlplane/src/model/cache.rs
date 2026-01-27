//! Cache model definitions and change-log payloads.
//!
//! # Purpose
//! Defines cache identifiers, records, and change events used by the store and API.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Cache {
    pub tenant_id: String,
    pub namespace: String,
    pub cache: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheChange {
    pub seq: u64,
    pub op: CacheChangeOp,
    pub key: CacheKey,
    pub cache: Option<Cache>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CacheChangeOp {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CachePatchRequest {
    pub display_name: Option<String>,
}
