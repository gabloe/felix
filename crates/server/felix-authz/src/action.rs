//! The action vocabulary. These strings are persisted in policies, so they
//! are frozen: renaming one breaks every stored rule that uses it, and a new
//! variant is invisible until `FromStr` learns it too.
use serde::{Deserialize, Serialize};

/// Authorization actions used in policy and permission checks. The serialized
/// form is snake_case and stable.
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
    /// The persisted identifier, e.g. `"stream.publish"`. Must stay in
    /// lockstep with `FromStr`.
    pub fn as_str(self) -> &'static str {
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
