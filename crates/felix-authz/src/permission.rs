use crate::{Action, AuthzError, AuthzResult};
use serde::{Deserialize, Serialize};

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

    pub fn as_string(&self) -> String {
        format!("{}:{}", self.action.as_str(), self.resource)
    }
}

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

    pub fn as_string(&self) -> String {
        format!("{}:{}", self.action.as_str(), self.resource_pattern)
    }
}

impl std::str::FromStr for PermissionPattern {
    type Err = AuthzError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (action, resource) = value
            .split_once(':')
            .ok_or_else(|| AuthzError::InvalidPermission(value.to_string()))?;
        let action = <Action as std::str::FromStr>::from_str(action)
            .map_err(|_| AuthzError::InvalidAction(action.to_string()))?;
        Ok(Self::new(action, resource))
    }
}

impl PermissionPattern {
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
