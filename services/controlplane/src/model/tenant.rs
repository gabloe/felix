//! Tenant model definitions and change-log payloads.
//!
//! # Purpose
//! Defines tenant records and change events used by the store and HTTP API.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Tenant {
    pub tenant_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct TenantChange {
    pub seq: u64,
    pub op: TenantChangeOp,
    pub tenant_id: String,
    pub tenant: Option<Tenant>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TenantChangeOp {
    Created,
    Deleted,
}
