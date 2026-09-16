//! Policy and grouping record shapes shared by the store, the admin API, and
//! the Casbin enforcer. These rows feed authorization directly.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// One Casbin policy rule: `(subject, object, action)`.
///
/// `action` comes from the Felix action vocabulary; `object` must work with
/// `keyMatch2` patterns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PolicyRule {
    pub subject: String,
    pub object: String,
    pub action: String,
}

/// One Casbin grouping rule: binds a user/subject to a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GroupingRule {
    pub user: String,
    pub role: String,
}
