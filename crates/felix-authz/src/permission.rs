//! Permission and permission-pattern primitives.
//!
//! # Purpose
//! Defines strongly typed permission values and parseable permission patterns.
//!
//! # How it fits
//! Request handlers and policy loaders produce `PermissionPattern` values which
//! are later evaluated by the permission matcher.
//!
//! # Key invariants
//! - Permission strings are `action:resource`.
//! - Action names must match the canonical [`Action`] strings.
//!
//! # Important configuration
//! - None; permissions are pure data structures.
//!
//! # Examples
//! ```rust
//! use felix_authz::{Action, Permission};
//!
//! let permission = Permission::new(Action::StreamPublish, "stream:payments/*");
//! assert!(permission.as_string().starts_with("stream.publish:"));
//! ```
//!
//! # Common pitfalls
//! - Forgetting the resource portion (`action:resource`) during parsing.
//! - Passing unvalidated action strings into `PermissionPattern::parse`.
//!
//! # Future work
//! - Add typed resource wrappers to reduce stringly-typed permissions.
use crate::{Action, AuthzError, AuthzResult};
use serde::{Deserialize, Serialize};

/// Concrete permission granting an action on a specific resource.
///
/// # Summary
/// Holds the canonical action and resource identifier.
///
/// # Invariants
/// - `resource` should use the same namespace format as policy rules.
///
/// # Example
/// ```rust
/// use felix_authz::{Action, Permission};
///
/// let permission = Permission::new(Action::CacheRead, "cache:payments/session/1");
/// assert_eq!(permission.action, Action::CacheRead);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permission {
    pub action: Action,
    pub resource: String,
}

impl Permission {
    /// Create a new permission from an action and resource string.
    ///
    /// # Parameters
    /// - `action`: the action to allow.
    /// - `resource`: resource identifier string.
    ///
    /// # Returns
    /// - A new [`Permission`].
    pub fn new(action: Action, resource: impl Into<String>) -> Self {
        Self {
            action,
            resource: resource.into(),
        }
    }

    /// Render the permission as a `action:resource` string.
    ///
    /// # Returns
    /// - A newly allocated string representation.
    ///
    /// # Performance
    /// - Allocates a new `String` each call.
    pub fn as_string(&self) -> String {
        // Use the canonical action string to keep policies stable.
        format!("{}:{}", self.action.as_str(), self.resource)
    }
}

/// Permission pattern that allows wildcards in the resource.
///
/// # Summary
/// Holds an action plus a resource pattern used for wildcard matching.
///
/// # Invariants
/// - `resource_pattern` may contain `*` as a glob.
///
/// # Example
/// ```rust
/// use felix_authz::{Action, PermissionPattern};
///
/// let pattern = PermissionPattern::new(Action::StreamSubscribe, "stream:payments/*");
/// assert_eq!(pattern.action, Action::StreamSubscribe);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermissionPattern {
    pub action: Action,
    pub resource_pattern: String,
}

impl PermissionPattern {
    /// Create a new permission pattern from an action and resource pattern.
    ///
    /// # Parameters
    /// - `action`: the action to allow.
    /// - `resource_pattern`: resource pattern with optional wildcards.
    ///
    /// # Returns
    /// - A new [`PermissionPattern`].
    pub fn new(action: Action, resource_pattern: impl Into<String>) -> Self {
        Self {
            action,
            resource_pattern: resource_pattern.into(),
        }
    }

    /// Render the permission pattern as a string.
    ///
    /// # Returns
    /// - `action:resource_pattern` string.
    ///
    /// # Performance
    /// - Allocates a new `String`.
    pub fn as_string(&self) -> String {
        format!("{}:{}", self.action.as_str(), self.resource_pattern)
    }
}

impl std::str::FromStr for PermissionPattern {
    type Err = AuthzError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Split on the first colon to recover action and resource pattern.
        let (action, resource) = value
            .split_once(':')
            .ok_or_else(|| AuthzError::InvalidPermission(value.to_string()))?;
        // Parse the action string to an enum variant.
        let action = <Action as std::str::FromStr>::from_str(action)
            .map_err(|_| AuthzError::InvalidAction(action.to_string()))?;
        Ok(Self::new(action, resource))
    }
}

impl PermissionPattern {
    /// Parse a permission pattern from a string.
    ///
    /// # Parameters
    /// - `value`: a `action:resource_pattern` string.
    ///
    /// # Returns
    /// - Parsed [`PermissionPattern`].
    ///
    /// # Errors
    /// - [`AuthzError::InvalidPermission`] if the string is missing a colon.
    /// - [`AuthzError::InvalidAction`] if the action is unknown.
    pub fn parse(value: &str) -> AuthzResult<Self> {
        value.parse()
    }
}

impl std::fmt::Display for PermissionPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Action;

    #[test]
    fn permission_string_rendering() {
        let permission = Permission::new(Action::StreamPublish, "stream:payments/orders.*");
        assert_eq!(
            permission.as_string(),
            "stream.publish:stream:payments/orders.*"
        );
    }

    #[test]
    fn permission_pattern_parse_roundtrip() {
        let parsed = PermissionPattern::parse("stream.subscribe:stream:payments/orders.*")
            .expect("parse permission");
        assert_eq!(parsed.action, Action::StreamSubscribe);
        assert_eq!(parsed.resource_pattern, "stream:payments/orders.*");
        assert_eq!(
            parsed.to_string(),
            "stream.subscribe:stream:payments/orders.*"
        );
    }

    #[test]
    fn permission_pattern_parse_invalid_format() {
        let err = PermissionPattern::parse("stream.publish").expect_err("missing resource");
        assert!(matches!(err, AuthzError::InvalidPermission(_)));
    }

    #[test]
    fn permission_pattern_parse_invalid_action() {
        let err = PermissionPattern::parse("stream.write:stream:payments/orders")
            .expect_err("bad action");
        assert!(matches!(err, AuthzError::InvalidAction(_)));
    }
}
