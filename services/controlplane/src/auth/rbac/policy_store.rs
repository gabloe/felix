//! RBAC policy/grouping models.
//!
//! # Purpose
//! Defines the policy and grouping record shapes shared across storage and API.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyRule {
    pub subject: String,
    pub object: String,
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GroupingRule {
    pub user: String,
    pub role: String,
}
