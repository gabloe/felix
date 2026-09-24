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
