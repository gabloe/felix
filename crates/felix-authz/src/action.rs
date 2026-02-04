//! Canonical action identifiers used in Felix authorization rules.
//!
//! # Purpose
//! Defines the stable, serialized action names that appear in permissions,
//! policies, and API responses.
//!
//! # How it fits
//! Action values are produced by request handlers and compared by the
//! authorization layer (Casbin matcher and permission evaluation).
//!
//! # Key invariants
//! - Each enum variant maps to a single snake-cased permission string.
//! - String representations are stable for policy persistence.
//!
//! # Important configuration
//! - None (this module is pure data mapping).
//!
//! # Examples
//! ```rust
//! use felix_authz::Action;
//!
//! let action = Action::StreamPublish;
//! assert_eq!(action.as_str(), "stream.publish");
//! ```
//!
//! # Common pitfalls
//! - Renaming or reformatting the strings breaks stored policies and tests.
//! - Adding new variants without updating `from_str` causes parse failures.
//!
//! # Future work
//! - Generate action lists from a single registry to avoid drift across crates.
use serde::{Deserialize, Serialize};

/// Authorization actions used in policy and permission checks.
///
/// # Summary
/// Enumerates the set of allowed action identifiers.
///
/// # Invariants
/// - The serialized form is snake_case and stable for policy storage.
///
/// # Performance
/// - Matching is a simple string or enum match; no allocations after deserialization.
///
/// # Example
/// ```rust
/// use felix_authz::Action;
///
/// assert_eq!(Action::CacheRead.as_str(), "cache.read");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    RbacView,
    RbacPolicyManage,
    RbacAssignmentManage,
    TenantManage,
    NamespaceManage,
    StreamManage,
    CacheManage,
    StreamPublish,
    StreamSubscribe,
    CacheRead,
    CacheWrite,
}

impl Action {
    /// Return the canonical string identifier for an action.
    ///
    /// # Parameters
    /// - `self`: the action variant.
    ///
    /// # Returns
    /// - The stable permission string (e.g. `"stream.publish"`).
    ///
    /// # Invariants
    /// - Returned strings must stay in sync with `FromStr` and stored policies.
    ///
    /// # Example
    /// ```rust
    /// use felix_authz::Action;
    ///
    /// assert_eq!(Action::TenantManage.as_str(), "tenant.manage");
    /// ```
    pub fn as_str(self) -> &'static str {
        // Match each action to its stable, persisted identifier.
        match self {
            Action::RbacView => "rbac.view",
            Action::RbacPolicyManage => "rbac.policy.manage",
            Action::RbacAssignmentManage => "rbac.assignment.manage",
            Action::TenantManage => "tenant.manage",
            Action::NamespaceManage => "ns.manage",
            Action::StreamManage => "stream.manage",
            Action::CacheManage => "cache.manage",
            Action::StreamPublish => "stream.publish",
            Action::StreamSubscribe => "stream.subscribe",
            Action::CacheRead => "cache.read",
            Action::CacheWrite => "cache.write",
        }
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Action {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Keep this mapping in lockstep with `as_str` and policy storage.
        match value {
            "rbac.view" => Ok(Action::RbacView),
            "rbac.policy.manage" => Ok(Action::RbacPolicyManage),
            "rbac.assignment.manage" => Ok(Action::RbacAssignmentManage),
            "tenant.manage" => Ok(Action::TenantManage),
            "ns.manage" => Ok(Action::NamespaceManage),
            "stream.manage" => Ok(Action::StreamManage),
            "cache.manage" => Ok(Action::CacheManage),
            "stream.publish" => Ok(Action::StreamPublish),
            "stream.subscribe" => Ok(Action::StreamSubscribe),
            "cache.read" => Ok(Action::CacheRead),
            "cache.write" => Ok(Action::CacheWrite),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Action;

    #[test]
    fn action_string_roundtrip() {
        let actions = [
            Action::RbacView,
            Action::RbacPolicyManage,
            Action::RbacAssignmentManage,
            Action::TenantManage,
            Action::NamespaceManage,
            Action::StreamManage,
            Action::CacheManage,
            Action::StreamPublish,
            Action::StreamSubscribe,
            Action::CacheRead,
            Action::CacheWrite,
        ];

        for action in actions {
            let as_str = action.as_str();
            assert_eq!(
                <Action as std::str::FromStr>::from_str(as_str).ok(),
                Some(action)
            );
            assert_eq!(action.to_string(), as_str);
        }
    }

    #[test]
    fn action_from_str_invalid() {
        assert!(<Action as std::str::FromStr>::from_str("tenant.write").is_err());
    }
}
