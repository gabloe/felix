//! RBAC policy and grouping models.
//!
//! # Purpose and responsibility
//! Defines the policy and grouping record shapes shared across storage, API
//! handlers, and Casbin enforcement.
//!
//! # Where it fits in Felix
//! These types are persisted in the control-plane store and loaded into the
//! RBAC enforcer during token exchange and admin operations.
//!
//! # Key invariants and assumptions
//! - `subject`, `object`, and `action` must align with the Casbin model.
//! - Grouping rules associate a user/subject with a role.
//!
//! # Security considerations
//! - These records directly influence authorization; treat them as privileged.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// A single RBAC policy rule.
///
/// # What it does
/// Represents the `(subject, object, action)` tuple used by Casbin.
///
/// # Why it exists
/// Provides a stable, serializable shape for storing and transporting policy.
///
/// # Invariants
/// - `action` must match the Felix action vocabulary.
/// - `object` must be compatible with `keyMatch2` pattern matching.
///
/// # Example
/// ```rust
/// use controlplane::auth::rbac::policy_store::PolicyRule;
///
/// let rule = PolicyRule {
///     subject: "role:admin".to_string(),
///     object: "tenant:t1".to_string(),
///     action: "tenant.manage".to_string(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyRule {
    pub subject: String,
    pub object: String,
    pub action: String,
}

/// A single RBAC grouping rule.
///
/// # What it does
/// Binds a user/subject to a role for role-based access.
///
/// # Why it exists
/// Separates role assignment from policy definition for clarity and reuse.
///
/// # Invariants
/// - `user` and `role` must be non-empty identifiers.
///
/// # Example
/// ```rust
/// use controlplane::auth::rbac::policy_store::GroupingRule;
///
/// let rule = GroupingRule {
///     user: "user:alice".to_string(),
///     role: "role:tenant_admin".to_string(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GroupingRule {
    pub user: String,
    pub role: String,
}
