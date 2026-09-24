use super::*;
use crate::Action;

#[test]
fn permission_string_rendering() {
    let permission = Permission::new(Action::StreamPublish, "stream:tenant-a/payments/orders.*");
    assert_eq!(
        permission.as_string(),
        "stream.publish:stream:tenant-a/payments/orders.*"
    );
}

#[test]
fn permission_pattern_parse_roundtrip() {
    let parsed = PermissionPattern::parse("stream.subscribe:stream:tenant-a/payments/orders.*")
        .expect("parse permission");
    assert_eq!(parsed.action, Action::StreamSubscribe);
    assert_eq!(parsed.resource_pattern, "stream:tenant-a/payments/orders.*");
    assert_eq!(
        parsed.to_string(),
        "stream.subscribe:stream:tenant-a/payments/orders.*"
    );
}

#[test]
fn permission_pattern_parse_invalid_format() {
    let err = PermissionPattern::parse("stream.publish").expect_err("missing resource");
    assert!(matches!(err, AuthzError::InvalidPermission(_)));
}

#[test]
fn permission_pattern_parse_invalid_action() {
    let err = PermissionPattern::parse("stream.write:stream:tenant-a/payments/orders")
        .expect_err("bad action");
    assert!(matches!(err, AuthzError::InvalidAction(_)));
}
