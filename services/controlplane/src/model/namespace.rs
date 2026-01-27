//! Namespace model definitions and change-log payloads.
//!
//! # Purpose
//! Defines namespace identifiers, records, and change events used by the store
//! and HTTP API.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq, Hash)]
pub struct NamespaceKey {
    pub tenant_id: String,
    pub namespace: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Namespace {
    pub tenant_id: String,
    pub namespace: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NamespaceChange {
    pub seq: u64,
    pub op: NamespaceChangeOp,
    pub key: NamespaceKey,
    pub namespace: Option<Namespace>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum NamespaceChangeOp {
    Created,
    Deleted,
}
