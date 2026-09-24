use super::*;
use crate::Action;

#[test]
fn wildcard_match_exact() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/orders",
        "stream:tenant-a/payments/orders"
    ));
    assert!(!wildcard_match(
        "stream:tenant-a/payments/orders",
        "stream:tenant-a/payments/orders.v2"
    ));
}

#[test]
fn wildcard_match_suffix() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/orders"
    ));
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/orders.v2"
    ));
    assert!(!wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:accounts/orders"
    ));
}

#[test]
fn wildcard_match_any() {
    assert!(wildcard_match("*", "anything"));
}

#[test]
fn wildcard_match_backtrack() {
    assert!(wildcard_match(
        "cache:*:read",
        "cache:tenant-a/payments:read"
    ));
    assert!(!wildcard_match(
        "cache:*:read",
        "cache:tenant-a/payments:write"
    ));
}

#[test]
fn wildcard_match_trailing_star() {
    assert!(wildcard_match(
        "stream:tenant-a/payments/*",
        "stream:tenant-a/payments/"
    ));
}

#[test]
fn matcher_allows() {
    let matcher = PermissionMatcher::new(vec![PermissionPattern::new(
        Action::StreamPublish,
        "stream:tenant-a/payments/orders.*",
    )]);
    assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v2"));
    assert!(!matcher.allows(
        Action::StreamSubscribe,
        "stream:tenant-a/payments/orders.v2"
    ));
}

#[test]
fn matcher_from_strings_and_patterns() {
    let patterns = vec![
        "stream.publish:stream:tenant-a/payments/orders.*".to_string(),
        "cache.read:cache:tenant-a/payments/session/*".to_string(),
    ];
    let matcher = PermissionMatcher::from_strings(&patterns).expect("parse patterns");
    assert_eq!(matcher.patterns().len(), 2);
    assert!(matcher.allows(Action::StreamPublish, "stream:tenant-a/payments/orders.v1"));
    assert!(matcher.allows(Action::CacheRead, "cache:tenant-a/payments/session/abc"));
}
