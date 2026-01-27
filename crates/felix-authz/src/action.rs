use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    TenantAdmin,
    TenantObserve,
    NamespaceManage,
    StreamPublish,
    StreamSubscribe,
    CacheRead,
    CacheWrite,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::TenantAdmin => "tenant.admin",
            Action::TenantObserve => "tenant.observe",
            Action::NamespaceManage => "ns.manage",
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
            "tenant.admin" => Ok(Action::TenantAdmin),
            "tenant.observe" => Ok(Action::TenantObserve),
            "ns.manage" => Ok(Action::NamespaceManage),
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
            Action::TenantAdmin,
            Action::TenantObserve,
            Action::NamespaceManage,
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
