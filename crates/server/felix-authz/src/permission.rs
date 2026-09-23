//! Typed permissions and permission patterns. The string form is
//! `action:resource`, where the action must be one of the canonical
//! [`Action`] strings.
use serde::{Deserialize, Serialize};

use crate::{Action, AuthzError, AuthzResult};

/// A grant of one action on one concrete resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Permission {
    pub action: Action,
    pub resource: String,
}

impl Permission {
    pub fn new(action: Action, resource: impl Into<String>) -> Self {
        Self {
            action,
            resource: resource.into(),
        }
    }

    /// The `action:resource` string form.
    pub fn as_string(&self) -> String {
        format!("{}:{}", self.action.as_str(), self.resource)
    }
}

/// A grant whose resource may contain `*` wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermissionPattern {
    pub action: Action,
    pub resource_pattern: String,
}

impl PermissionPattern {
    pub fn new(action: Action, resource_pattern: impl Into<String>) -> Self {
        Self {
            action,
            resource_pattern: resource_pattern.into(),
        }
    }

    /// Parse an `action:resource_pattern` string.
    ///
    /// # Errors
    /// [`AuthzError::InvalidPermission`] when the colon is missing,
    /// [`AuthzError::InvalidAction`] when the action is unknown.
    pub fn parse(value: &str) -> AuthzResult<Self> {
        value.parse()
    }

    /// The `action:resource_pattern` string form.
    pub fn as_string(&self) -> String {
        format!("{}:{}", self.action.as_str(), self.resource_pattern)
    }
}

impl std::fmt::Display for PermissionPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}

impl std::str::FromStr for PermissionPattern {
    type Err = AuthzError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // Split on the first colon only — resources contain colons too
        // ("stream.publish:stream:tenant-a/...").
        let (action, resource) = value
            .split_once(':')
            .ok_or_else(|| AuthzError::InvalidPermission(value.to_string()))?;
        let action = <Action as std::str::FromStr>::from_str(action)
            .map_err(|_| AuthzError::InvalidAction(action.to_string()))?;
        Ok(Self::new(action, resource))
    }
}

#[cfg(test)]
mod tests;
